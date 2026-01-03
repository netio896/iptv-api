# -*- coding: utf-8 -*-
"""
EPG_M3U_智能匹配工具v1.0.5
作者: Daixiaobai
修复了启动时 thread_count 未初始化的错误
新增功能: 停止匹配、重新匹配
修复: 保留所有播放数据和其他行
新增: 预计完成时间显示，匹配百分比显示
优化: 大幅提升匹配速度
"""

import os, re, gzip, shutil, threading, tkinter as tk, time
from tkinter import filedialog, scrolledtext, ttk, messagebox, Listbox, MULTIPLE
from datetime import datetime, timedelta
import requests, xml.etree.ElementTree as ET
from difflib import SequenceMatcher
import queue, csv, psutil
from concurrent.futures import ThreadPoolExecutor, as_completed, Future

# -------------------- 工具函数 --------------------
def download_file(url, cache_dir="cache", max_cache_age_hours=4):
    """下载文件到缓存目录，支持缓存过期管理"""
    os.makedirs(cache_dir, exist_ok=True)
    filename = url.split("/")[-1].split("?")[0]
    local_name = os.path.join(cache_dir, filename)
    
    # 检查缓存是否过期（4小时）
    if os.path.exists(local_name):
        file_age = time.time() - os.path.getmtime(local_name)
        if file_age < max_cache_age_hours * 3600:
            file_size = os.path.getsize(local_name)
            return local_name, True, file_size
        else:
            # 缓存过期，删除文件
            os.remove(local_name)
    
    # 检查缓存总大小
    cache_size_mb = get_cache_size_mb(cache_dir)
    if cache_size_mb > 4 * 1024:  # 4GB限制
        cleanup_cache(cache_dir, 3 * 1024)  # 清理到3GB
    
    try:
        r = requests.get(url, stream=True, timeout=15)
        r.raise_for_status()
        total_size = int(r.headers.get('content-length', 0))
        
        with open(local_name, "wb") as f:
            downloaded = 0
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
        
        return local_name, False, total_size
    except Exception as e:
        return None, False, str(e)

def get_cache_size_mb(cache_dir="cache"):
    """获取缓存目录大小（MB）"""
    if not os.path.exists(cache_dir):
        return 0
    
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(cache_dir):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    
    return total_size / (1024 * 1024)  # 转换为MB

def cleanup_cache(cache_dir="cache", target_size_mb=3072):
    """清理缓存到目标大小，按修改时间删除最旧的文件"""
    if not os.path.exists(cache_dir):
        return
    
    # 获取所有文件及其修改时间
    files = []
    for dirpath, dirnames, filenames in os.walk(cache_dir):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            mtime = os.path.getmtime(fp)
            files.append((fp, mtime, os.path.getsize(fp)))
    
    # 按修改时间排序（最旧的在前）
    files.sort(key=lambda x: x[1])
    
    current_size_mb = sum(f[2] for f in files) / (1024 * 1024)
    deleted_size_mb = 0
    
    # 删除最旧的文件直到达到目标大小
    for filepath, _, size in files:
        if current_size_mb - deleted_size_mb <= target_size_mb:
            break
        
        try:
            os.remove(filepath)
            deleted_size_mb += size / (1024 * 1024)
        except:
            pass
    
    return deleted_size_mb

def load_epg_file(path_or_url):
    """加载EPG文件（支持.gz压缩格式）"""
    if path_or_url.startswith("http"):
        local_path, cached, info = download_file(path_or_url)
        if not local_path:
            return f"ERROR: 下载失败 - {info}"
    else:
        local_path = path_or_url
        cached = True
    
    try:
        if local_path.endswith(".gz"):
            with gzip.open(local_path, "rb") as f:
                content = f.read()
            return ET.fromstring(content), cached
        else:
            tree = ET.parse(local_path)
            return tree.getroot(), cached
    except Exception as e:
        return f"ERROR: 解析失败 - {e}", False

def standardize_name(name):
    """标准化频道名称用于匹配"""
    if not name: 
        return ""
    
    # 移除括号内容
    name = re.sub(r"[\[\【\(].*?[\]\】\)]", "", name)
    # 移除质量标识
    name = re.sub(r"(HD|1080p|4K|超清|高清|直播|卫视|电视台|CCTV)", "", name, flags=re.I)
    # 统一处理空格和大小写
    name = name.replace(" ", "").lower()
    # 中文数字转阿拉伯数字
    name = name.replace("一", "1").replace("壹", "1").replace("二", "2").replace("三", "3").replace("四", "4")
    name = name.replace("五", "5").replace("六", "6").replace("七", "7").replace("八", "8").replace("九", "9").replace("零", "0")
    
    # 繁简转换（可选）
    try:
        from opencc import OpenCC
        cc = OpenCC('t2s')
        name = cc.convert(name)
    except:
        pass
    
    return name.strip()

def similar(a, b):
    """计算字符串相似度"""
    if not a or not b:
        return 0
    
    # 快速检查：如果完全相同
    if a == b:
        return 1.0
    
    # 快速检查：如果长度差异太大
    len_a, len_b = len(a), len(b)
    if len_a == 0 or len_b == 0:
        return 0
    
    max_len = max(len_a, len_b)
    min_len = min(len_a, len_b)
    
    # 长度差异过大时快速返回低相似度
    if max_len > 3 * min_len:
        return 0
    
    # 使用更快的算法（对于短文本，SequenceMatcher可以接受）
    if len_a <= 20 and len_b <= 20:
        return SequenceMatcher(None, a, b).ratio()
    else:
        # 对于长文本，使用简单的字符重叠度计算
        set_a = set(a)
        set_b = set(b)
        
        if not set_a or not set_b:
            return 0
        
        intersection = len(set_a & set_b)
        union = len(set_a | set_b)
        
        if union == 0:
            return 0
        
        return intersection / union

def format_remaining_time(seconds):
    """格式化剩余时间显示"""
    if seconds < 60:
        return f"{int(seconds)}秒"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}分{secs}秒"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}小时{minutes}分"

def build_epg_index(epg_channels):
    """构建EPG索引，加速匹配查找"""
    index = {
        'tvg_id': {},
        'tvg_name': {},
        'display_name': {},
        'optimized': {},
        'all_channels': list(epg_channels.values()),  # 用于模糊匹配
    }
    
    # 预计算标准化名称
    for ch_id, ch_info in epg_channels.items():
        ch_display = ch_info["display-name"]
        ch_normalized = standardize_name(ch_display)
        
        # 构建各种索引
        if ch_id:
            index['tvg_id'][ch_id] = ch_info
            
        if ch_id:  # tvg-name也使用ch_id
            index['tvg_name'][ch_id] = ch_info
            
        if ch_display:
            index['display_name'][ch_display] = ch_info
            
        # 标准化名称索引
        if ch_normalized:
            index['optimized'][ch_normalized] = ch_info
    
    return index

# -------------------- 现代化GUI界面 --------------------
class ModernButton(tk.Button):
    """现代化按钮样式"""
    def __init__(self, master=None, **kwargs):
        super().__init__(master, **kwargs)
        self.config(
            bg="#007ACC",
            fg="white",
            font=("Segoe UI", 10),
            relief="flat",
            padx=15,
            pady=5,
            cursor="hand2"
        )
        self.bind("<Enter>", self.on_enter)
        self.bind("<Leave>", self.on_leave)
    
    def on_enter(self, e):
        self.config(bg="#005A9E")
    
    def on_leave(self, e):
        self.config(bg="#007ACC")

class EPGMatcherGUI:
    def __init__(self, master):
        self.master = master
        master.title("🎬戴小白_EPG_M3U_智能匹配工具_V1.0.5")
        master.geometry("1100x800")  # 稍微增加窗口大小
        master.configure(bg="#f0f0f0")
        
        # 设置图标（如果有）
        try:
            master.iconbitmap("icon.ico")
        except:
            pass
        
        # 日志队列用于线程安全
        self.log_queue = queue.Queue()
        
        # 初始化变量（先初始化所有变量，再创建界面）
        self.m3u_lines = []
        self.epg_files = []
        self.cache_dir = "cache"
        self.m3u_file = ""
        self.total_channels = 0
        self.matched_channels = 0
        self.start_time = None
        
        # 进度相关变量
        self.last_progress_time = None
        self.last_progress_count = 0
        self.estimated_remaining_time = "计算中..."
        
        # 线程控制变量
        self.stop_requested = False
        self.executor = None
        self.matching_thread = None
        self.is_matching = False
        
        # 多线程配置（先初始化这些变量）
        self.cpu_count = psutil.cpu_count(logical=False) or 8
        self.thread_count = min(self.cpu_count * 2, 16)  # 不超过16线程
        
        # 匹配优先级配置
        self.match_priority = ["tvg-id", "tvg-name", "display-name", "optimized", "fuzzy"]
        self.enabled_match_types = {
            "tvg-id": True,
            "tvg-name": True,
            "display-name": True,
            "optimized": True,
            "fuzzy": True
        }
        
        # 创建必要目录
        os.makedirs("m3u", exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # 创建主容器
        self.create_widgets()
        
        # 开始检查日志队列
        self.check_log_queue()
        
        # 显示系统信息
        self.log("INFO", f"💻 检测到 {self.cpu_count} 个物理核心，使用 {self.thread_count} 个线程")
        self.log("INFO", f"💾 缓存上限: 4GB，过期时间: 4小时")
        self.log("INFO", "🚀 已启用优化速度版，匹配速度提升2-5倍")
    
    def create_widgets(self):
        """创建所有GUI组件"""
        # 标题
        title_frame = tk.Frame(self.master, bg="#2c3e50", height=60)
        title_frame.pack(fill=tk.X, pady=(0, 10))
        
        title_label = tk.Label(
            title_frame,
            text="🎬戴小白_EPG_M3U_智能匹配工具_V1.0.5",
            font=("Segoe UI", 18, "bold"),
            fg="white",
            bg="#2c3e50"
        )
        title_label.pack(pady=15)
        
        # 主内容区域
        main_container = tk.Frame(self.master, bg="#f0f0f0")
        main_container.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        # 左侧配置面板
        left_panel = tk.Frame(main_container, bg="white", relief="solid", bd=1)
        left_panel.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        
        # 右侧日志面板
        right_panel = tk.Frame(main_container, bg="white", relief="solid", bd=1)
        right_panel.grid(row=0, column=1, sticky="nsew")
        
        # 配置网格权重
        main_container.grid_columnconfigure(0, weight=1)
        main_container.grid_columnconfigure(1, weight=1)
        main_container.grid_rowconfigure(0, weight=1)
        
        # 创建左侧面板内容
        self.create_left_panel(left_panel)
        
        # 创建右侧面板内容
        self.create_right_panel(right_panel)
        
        # 底部状态栏
        self.create_status_bar()
    
    def create_left_panel(self, parent):
        """创建左侧配置面板"""
        # 1. M3U文件配置
        m3u_group = tk.LabelFrame(
            parent,
            text="📁 M3U 源配置",
            font=("Segoe UI", 11, "bold"),
            bg="white",
            padx=10,
            pady=10
        )
        m3u_group.pack(fill=tk.X, pady=(0, 10))
        
        # 远程URL输入
        tk.Label(m3u_group, text="远程M3U URL:", bg="white", font=("Segoe UI", 9)).pack(anchor="w", pady=(0, 5))
        self.m3u_url_entry = tk.Entry(m3u_group, font=("Segoe UI", 10), width=40)
        self.m3u_url_entry.pack(fill=tk.X, pady=(0, 10))
        
        # 本地文件选择
        tk.Label(m3u_group, text="本地M3U文件:", bg="white", font=("Segoe UI", 9)).pack(anchor="w", pady=(0, 5))
        file_frame = tk.Frame(m3u_group, bg="white")
        file_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.m3u_file_entry = tk.Entry(file_frame, font=("Segoe UI", 10))
        self.m3u_file_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        ModernButton(file_frame, text="📂 选择文件", command=self.select_m3u).pack(side=tk.LEFT, padx=(5, 0))
        
        # 2. EPG源配置
        epg_group = tk.LabelFrame(
            parent,
            text="📡 EPG 源配置",
            font=("Segoe UI", 11, "bold"),
            bg="white",
            padx=10,
            pady=10
        )
        epg_group.pack(fill=tk.X, pady=(0, 10))
        
        # EPG URL列表
        tk.Label(epg_group, text="EPG URL列表 (每行一个):", bg="white", font=("Segoe UI", 9)).pack(anchor="w", pady=(0, 5))
        
        epg_text_frame = tk.Frame(epg_group, bg="white")
        epg_text_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.epg_text = scrolledtext.ScrolledText(epg_text_frame, height=4, font=("Consolas", 9), wrap=tk.WORD)
        self.epg_text.pack(fill=tk.BOTH, expand=True)
        
        # 本地EPG文件
        tk.Label(epg_group, text="本地EPG文件:", bg="white", font=("Segoe UI", 9)).pack(anchor="w", pady=(0, 5))
        epg_file_frame = tk.Frame(epg_group, bg="white")
        epg_file_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.epg_file_entry = tk.Entry(epg_file_frame, font=("Segoe UI", 10))
        self.epg_file_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        ModernButton(epg_file_frame, text="📂 选择文件", command=self.select_epg).pack(side=tk.LEFT, padx=(5, 0))
        
        # 3. 匹配设置
        settings_group = tk.LabelFrame(
            parent,
            text="⚙️ 匹配设置",
            font=("Segoe UI", 11, "bold"),
            bg="white",
            padx=10,
            pady=10
        )
        settings_group.pack(fill=tk.X, pady=(0, 10))
        
        # 匹配优先级设置
        tk.Label(settings_group, text="匹配类型 (勾选启用):", bg="white", font=("Segoe UI", 9)).pack(anchor="w", pady=(0, 5))
        
        match_frame = tk.Frame(settings_group, bg="white")
        match_frame.pack(fill=tk.X, pady=(0, 10))
        
        # 创建复选框
        self.match_vars = {}
        match_types = [
            ("tvg-id", "tvg-id匹配"),
            ("tvg-name", "tvg-name匹配"),
            ("display-name", "display-name匹配"),
            ("optimized", "优化匹配"),
            ("fuzzy", "模糊匹配")
        ]
        
        for i, (key, label) in enumerate(match_types):
            var = tk.BooleanVar(value=True)
            self.match_vars[key] = var
            cb = tk.Checkbutton(
                match_frame,
                text=label,
                variable=var,
                bg="white",
                font=("Segoe UI", 9)
            )
            cb.grid(row=0, column=i, padx=5, sticky="w")
        
        # 线程数设置
        tk.Label(settings_group, text="线程数:", bg="white", font=("Segoe UI", 9)).pack(anchor="w", pady=(0, 5))
        thread_frame = tk.Frame(settings_group, bg="white")
        thread_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.thread_var = tk.IntVar(value=self.thread_count)  # 这里使用已经初始化的 thread_count
        thread_scale = tk.Scale(
            thread_frame,
            from_=1,
            to=32,
            resolution=1,
            orient=tk.HORIZONTAL,
            variable=self.thread_var,
            bg="white",
            font=("Segoe UI", 9),
            length=200
        )
        thread_scale.pack(side=tk.LEFT, padx=(0, 10))
        
        tk.Label(
            thread_frame,
            text=f"推荐: {self.cpu_count}核={self.thread_count}线程",
            bg="white",
            font=("Segoe UI", 9),
            fg="#666"
        ).pack(side=tk.LEFT)
        
        # 相似度阈值
        tk.Label(settings_group, text="模糊匹配阈值:", bg="white", font=("Segoe UI", 9)).pack(anchor="w", pady=(0, 5))
        self.similarity_var = tk.DoubleVar(value=0.8)
        similarity_scale = tk.Scale(
            settings_group,
            from_=0.5,
            to=1.0,
            resolution=0.05,
            orient=tk.HORIZONTAL,
            variable=self.similarity_var,
            bg="white",
            font=("Segoe UI", 9)
        )
        similarity_scale.pack(fill=tk.X, pady=(0, 10))
        
        # 4. 操作按钮
        button_frame = tk.Frame(parent, bg="white")
        button_frame.pack(fill=tk.X, pady=(10, 0))
        
        # 创建按钮组
        self.start_button = ModernButton(button_frame, text="🚀 开始匹配", command=self.start_matching)
        self.start_button.pack(side=tk.LEFT, padx=(0, 5))
        
        self.stop_button = ModernButton(button_frame, text="⏹️ 停止匹配", command=self.stop_matching)
        self.stop_button.pack(side=tk.LEFT, padx=(0, 5))
        self.stop_button.config(state="disabled", bg="#666666")
        
        self.reset_button = ModernButton(button_frame, text="🔄 重新匹配", command=self.reset_matching)
        self.reset_button.pack(side=tk.LEFT, padx=(0, 5))
        self.reset_button.config(state="disabled", bg="#666666")
        
        ModernButton(button_frame, text="🧹 清理缓存", command=self.clear_cache).pack(side=tk.LEFT, padx=(0, 5))
        ModernButton(button_frame, text="📊 查看缓存", command=self.show_cache_info).pack(side=tk.LEFT, padx=(0, 5))
        ModernButton(button_frame, text="📂 打开输出文件夹", command=self.open_output_folder).pack(side=tk.LEFT)
        
        # 进度条
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(
            parent,
            variable=self.progress_var,
            maximum=100,
            mode='determinate',
            length=200
        )
        self.progress_bar.pack(fill=tk.X, pady=(15, 0))
        
        self.progress_label = tk.Label(
            parent,
            text="就绪",
            font=("Segoe UI", 9),
            bg="white",
            fg="#666"
        )
        self.progress_label.pack(pady=(5, 0))
        
        # 预计完成时间标签
        self.eta_label = tk.Label(
            parent,
            text="预计完成时间: -",
            font=("Segoe UI", 9),
            bg="white",
            fg="#666"
        )
        self.eta_label.pack(pady=(0, 5))
    
    def create_right_panel(self, parent):
        """创建右侧日志面板"""
        # 日志标题
        log_header = tk.Frame(parent, bg="#2c3e50")
        log_header.pack(fill=tk.X)
        
        tk.Label(
            log_header,
            text="📋 匹配日志",
            font=("Segoe UI", 12, "bold"),
            fg="white",
            bg="#2c3e50"
        ).pack(pady=5)
        
        # 统计信息
        stats_frame = tk.Frame(parent, bg="white")
        stats_frame.pack(fill=tk.X, padx=10, pady=5)
        
        self.total_label = tk.Label(
            stats_frame,
            text="频道总数: 0",
            font=("Segoe UI", 9),
            bg="white"
        )
        self.total_label.pack(side=tk.LEFT, padx=(0, 15))
        
        self.matched_label = tk.Label(
            stats_frame,
            text="已匹配: 0",
            font=("Segoe UI", 9),
            bg="white"
        )
        self.matched_label.pack(side=tk.LEFT, padx=(0, 15))
        
        self.time_label = tk.Label(
            stats_frame,
            text="耗时: 0s",
            font=("Segoe UI", 9),
            bg="white"
        )
        self.time_label.pack(side=tk.LEFT)
        
        # 匹配率标签
        self.match_rate_label = tk.Label(
            stats_frame,
            text="匹配率: 0%",
            font=("Segoe UI", 9),
            bg="white",
            fg="#0066CC"
        )
        self.match_rate_label.pack(side=tk.LEFT, padx=(15, 0))
        
        # 缓存信息标签
        self.cache_label = tk.Label(
            stats_frame,
            text="缓存: 0MB",
            font=("Segoe UI", 9),
            bg="white",
            fg="#666"
        )
        self.cache_label.pack(side=tk.RIGHT, padx=(0, 10))
        
        # 日志文本框
        log_frame = tk.Frame(parent, bg="white")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        
        # 创建带滚动条的文本区域
        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            wrap=tk.WORD,
            font=("Consolas", 9),
            bg="#f8f9fa",
            fg="#333",
            height=20
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # 配置标签颜色
        self.log_text.tag_config("SUCCESS", foreground="green")
        self.log_text.tag_config("ERROR", foreground="red")
        self.log_text.tag_config("WARNING", foreground="orange")
        self.log_text.tag_config("INFO", foreground="blue")
        self.log_text.tag_config("MATCH", foreground="#0066CC")
        self.log_text.tag_config("UNMATCHED", foreground="#999999")
        self.log_text.tag_config("STOP", foreground="#FF6600")
        
        # 日志操作按钮
        log_buttons = tk.Frame(log_frame, bg="white")
        log_buttons.pack(fill=tk.X, pady=(5, 0))
        
        ModernButton(log_buttons, text="📋 清空日志", command=self.clear_log).pack(side=tk.LEFT, padx=(0, 5))
        ModernButton(log_buttons, text="💾 保存日志", command=self.save_log).pack(side=tk.LEFT)
    
    def create_status_bar(self):
        """创建底部状态栏"""
        status_bar = tk.Frame(self.master, bg="#2c3e50", height=25)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
        self.status_label = tk.Label(
            status_bar,
            text="就绪",
            font=("Segoe UI", 9),
            fg="white",
            bg="#2c3e50"
        )
        self.status_label.pack(side=tk.LEFT, padx=10)
        
        # 版本信息
        tk.Label(
            status_bar,
            text="🎬戴小白_EPG_M3U_智能匹配工具_V1.0.5 © 2025-12-05 Daixiaobai",
            font=("Segoe UI", 9),
            fg="#aaa",
            bg="#2c3e50"
        ).pack(side=tk.RIGHT, padx=10)
    
    def update_button_states(self, matching=False):
        """更新按钮状态"""
        if matching:
            # 匹配进行中
            self.start_button.config(state="disabled", bg="#666666")
            self.stop_button.config(state="normal", bg="#FF3333")
            self.reset_button.config(state="disabled", bg="#666666")
        else:
            # 匹配未进行
            self.start_button.config(state="normal", bg="#007ACC")
            self.stop_button.config(state="disabled", bg="#666666")
            if self.total_channels > 0:
                self.reset_button.config(state="normal", bg="#28A745")
            else:
                self.reset_button.config(state="disabled", bg="#666666")
    
    def select_m3u(self):
        """选择M3U文件"""
        path = filedialog.askopenfilename(
            title="选择M3U文件",
            filetypes=[("M3U文件", "*.m3u *.m3u8"), ("所有文件", "*.*")]
        )
        if path:
            self.m3u_file_entry.delete(0, tk.END)
            self.m3u_file_entry.insert(0, path)
            self.load_m3u_file(path)
    
    def load_m3u_file(self, path):
        """加载M3U文件内容"""
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                self.m3u_lines = f.readlines()
            
            # 统计频道数量
            channel_count = sum(1 for line in self.m3u_lines if line.startswith("#EXTINF"))
            self.total_channels = channel_count
            
            # 更新界面
            self.total_label.config(text=f"频道总数: {channel_count}")
            self.log("SUCCESS", f"✅ 成功加载 M3U 文件: {os.path.basename(path)}")
            self.log("INFO", f"📊 检测到 {channel_count} 个频道")
            
            # 显示文件信息
            file_size = os.path.getsize(path) / 1024
            self.log("INFO", f"📁 文件大小: {file_size:.1f} KB")
            self.update_status(f"已加载 M3U 文件: {os.path.basename(path)}")
            
            self.m3u_file = path
            # 更新按钮状态
            self.update_button_states(False)
        except Exception as e:
            self.log("ERROR", f"❌ 加载文件失败: {str(e)}")
    
    def select_epg(self):
        """选择EPG文件"""
        path = filedialog.askopenfilename(
            title="选择EPG文件",
            filetypes=[("EPG文件", "*.xml *.xml.gz"), ("所有文件", "*.*")]
        )
        if path:
            self.epg_file_entry.delete(0, tk.END)
            self.epg_file_entry.insert(0, path)
            self.log("SUCCESS", f"✅ 已选择 EPG 文件: {os.path.basename(path)}")
    
    def clear_cache(self):
        """清理缓存目录"""
        if os.path.exists(self.cache_dir):
            try:
                # 计算缓存大小
                cache_size = get_cache_size_mb(self.cache_dir)
                
                shutil.rmtree(self.cache_dir)
                os.makedirs(self.cache_dir, exist_ok=True)
                
                self.log("SUCCESS", f"🧹 缓存清理完成，释放 {cache_size:.2f} MB 空间")
                self.update_status("缓存已清理")
                messagebox.showinfo("清理完成", f"成功清理缓存，释放 {cache_size:.2f} MB 空间")
                self.update_cache_info()
            except Exception as e:
                self.log("ERROR", f"❌ 清理缓存失败: {str(e)}")
        else:
            self.log("INFO", "📭 缓存目录为空")
    
    def show_cache_info(self):
        """显示缓存详细信息"""
        if os.path.exists(self.cache_dir):
            cache_size_mb = get_cache_size_mb(self.cache_dir)
            
            # 获取文件列表和过期状态
            files = []
            for dirpath, dirnames, filenames in os.walk(self.cache_dir):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    mtime = os.path.getmtime(fp)
                    age_hours = (time.time() - mtime) / 3600
                    size_mb = os.path.getsize(fp) / (1024 * 1024)
                    expired = age_hours > 4
                    files.append((fp, size_mb, age_hours, expired))
            
            if files:
                # 创建缓存信息窗口
                info_window = tk.Toplevel(self.master)
                info_window.title("📊 缓存信息")
                info_window.geometry("600x400")
                info_window.configure(bg="white")
                
                # 标题
                title_frame = tk.Frame(info_window, bg="#2c3e50", height=50)
                title_frame.pack(fill=tk.X, pady=(0, 10))
                
                tk.Label(
                    title_frame,
                    text=f"缓存目录: {self.cache_dir}",
                    font=("Segoe UI", 12, "bold"),
                    fg="white",
                    bg="#2c3e50"
                ).pack(pady=15)
                
                # 统计信息
                stats_text = f"总大小: {cache_size_mb:.2f} MB\n"
                stats_text += f"文件数量: {len(files)}\n"
                stats_text += f"过期文件: {sum(1 for f in files if f[3])}\n"
                stats_text += f"配置限制: 4GB, 4小时过期"
                
                tk.Label(
                    info_window,
                    text=stats_text,
                    font=("Segoe UI", 10),
                    bg="white",
                    justify="left"
                ).pack(pady=10)
                
                # 文件列表
                list_frame = tk.Frame(info_window, bg="white")
                list_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
                
                listbox = tk.Listbox(
                    list_frame,
                    font=("Consolas", 9),
                    bg="#f8f9fa"
                )
                scrollbar = tk.Scrollbar(list_frame, orient="vertical", command=listbox.yview)
                listbox.configure(yscrollcommand=scrollbar.set)
                
                listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
                scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
                
                for filepath, size, age, expired in files:
                    filename = os.path.basename(filepath)
                    status = " [已过期]" if expired else " [有效]"
                    item = f"{filename[:30]:30} {size:6.2f}MB {age:5.1f}h{status}"
                    listbox.insert(tk.END, item)
                
                # 操作按钮
                btn_frame = tk.Frame(info_window, bg="white")
                btn_frame.pack(pady=10)
                
                ModernButton(btn_frame, text="🗑️ 删除过期文件", 
                           command=lambda: self.delete_expired_files(files, info_window)).pack(side=tk.LEFT, padx=5)
                ModernButton(btn_frame, text="关闭", 
                           command=info_window.destroy).pack(side=tk.LEFT, padx=5)
            else:
                messagebox.showinfo("缓存信息", "缓存目录为空")
        else:
            messagebox.showinfo("缓存信息", "缓存目录不存在")
    
    def delete_expired_files(self, files, window):
        """删除过期文件"""
        expired_files = [f[0] for f in files if f[3]]
        if not expired_files:
            messagebox.showinfo("提示", "没有过期文件")
            return
        
        try:
            deleted_size = 0
            for filepath in expired_files:
                size = os.path.getsize(filepath)
                os.remove(filepath)
                deleted_size += size
            
            self.log("SUCCESS", f"🗑️ 已删除 {len(expired_files)} 个过期文件，释放 {deleted_size/(1024*1024):.2f} MB")
            window.destroy()
            self.update_cache_info()
            messagebox.showinfo("完成", f"已删除 {len(expired_files)} 个过期文件")
        except Exception as e:
            self.log("ERROR", f"❌ 删除过期文件失败: {str(e)}")
    
    def update_cache_info(self):
        """更新缓存信息显示"""
        if os.path.exists(self.cache_dir):
            cache_size_mb = get_cache_size_mb(self.cache_dir)
            self.cache_label.config(text=f"缓存: {cache_size_mb:.1f}MB")
    
    def open_output_folder(self):
        """打开输出文件夹"""
        output_dir = "m3u"
        if os.path.exists(output_dir):
            os.startfile(output_dir)
        else:
            os.makedirs(output_dir, exist_ok=True)
            os.startfile(output_dir)
    
    def clear_log(self):
        """清空日志"""
        self.log_text.delete("1.0", tk.END)
    
    def save_log(self):
        """保存日志到文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"匹配日志_{timestamp}.txt"
        log_content = self.log_text.get("1.0", tk.END)
        
        with open(filename, "w", encoding="utf-8") as f:
            f.write(log_content)
        
        self.log("SUCCESS", f"💾 日志已保存到: {filename}")
    
    def log(self, level, message):
        """线程安全的日志记录"""
        self.log_queue.put((level, message, datetime.now()))
    
    def check_log_queue(self):
        """定期检查并处理日志队列"""
        try:
            while True:
                level, message, timestamp = self.log_queue.get_nowait()
                time_str = timestamp.strftime("%H:%M:%S")
                formatted_message = f"[{time_str}] {message}\n"
                
                self.log_text.insert(tk.END, formatted_message, level)
                self.log_text.see(tk.END)
                
                # 如果是缓存相关的消息，更新缓存信息
                if "缓存" in message or "下载" in message:
                    self.master.after(1000, self.update_cache_info)
        except queue.Empty:
            pass
        finally:
            self.master.after(100, self.check_log_queue)
    
    def update_status(self, message):
        """更新状态栏"""
        self.status_label.config(text=message)
    
    def update_progress(self, current, total, message=""):
        """更新进度条和预计完成时间"""
        if total > 0:
            percentage = (current / total) * 100
            self.progress_var.set(percentage)
            
            # 更新匹配率标签
            if self.total_channels > 0:
                match_percentage = (self.matched_channels / self.total_channels * 100)
                self.match_rate_label.config(text=f"匹配率: {match_percentage:.1f}%")
            
            # 计算预计完成时间
            current_time = time.time()
            
            # 初始化或重置计时器
            if self.last_progress_time is None:
                self.last_progress_time = current_time
                self.last_progress_count = current
                self.estimated_remaining_time = "计算中..."
            else:
                # 计算每秒处理速度
                time_diff = current_time - self.last_progress_time
                count_diff = current - self.last_progress_count
                
                if count_diff > 0 and time_diff > 0:
                    speed = count_diff / time_diff  # 每秒处理的频道数
                    
                    # 计算剩余时间和预计完成时间
                    remaining_count = total - current
                    if speed > 0:
                        remaining_seconds = remaining_count / speed
                        
                        # 格式化剩余时间
                        self.estimated_remaining_time = format_remaining_time(remaining_seconds)
                        
                        # 计算预计完成时间（具体时间点）
                        estimated_completion_time = datetime.now() + timedelta(seconds=remaining_seconds)
                        time_str = estimated_completion_time.strftime("%H:%M:%S")
                        
                        # 更新ETA标签
                        self.eta_label.config(text=f"预计完成: {time_str} (剩余: {self.estimated_remaining_time})")
                    else:
                        self.eta_label.config(text="预计完成: 计算中...")
                
                # 更新最后记录的时间和计数
                if count_diff > 10 or current == total:  # 每处理10个频道更新一次计时器，或者完成后重置
                    self.last_progress_time = current_time
                    self.last_progress_count = current
            
            # 更新进度标签
            self.progress_label.config(text=f"{message} {percentage:.1f}% ({current}/{total})")
            self.matched_label.config(text=f"已匹配: {self.matched_channels}")
    
    def match_single_channel_fast(self, channel_data, epg_index, similarity_threshold):
        """优化版单频道匹配函数"""
        if self.stop_requested:
            return None
        
        line, display_name, tvg_id, tvg_name, line_index = channel_data
        
        # 更新启用的匹配类型
        enabled_types = {}
        for key, var in self.match_vars.items():
            enabled_types[key] = var.get()
        
        # 快速路径：如果所有匹配类型都禁用
        if not any(enabled_types.values()):
            return self.create_match_result(line, display_name, tvg_id, line_index, False, "未匹配", None, "")
        
        # 1. tvg-id匹配（字典查找 O(1)）
        if enabled_types["tvg-id"] and tvg_id:
            ch_info = epg_index['tvg_id'].get(tvg_id)
            if ch_info:
                return self.create_match_result(
                    line, display_name, tvg_id, line_index, 
                    True, "tvg-id匹配", ch_info["display-name"], 
                    os.path.basename(ch_info["epg_file"])
                )
        
        # 2. tvg-name匹配（字典查找 O(1)）
        if enabled_types["tvg-name"] and tvg_name:
            ch_info = epg_index['tvg_name'].get(tvg_name)
            if ch_info:
                return self.create_match_result(
                    line, display_name, tvg_id, line_index,
                    True, "tvg-name匹配", ch_info["display-name"],
                    os.path.basename(ch_info["epg_file"])
                )
        
        # 3. display-name精确匹配（字典查找 O(1)）
        if enabled_types["display-name"] and display_name:
            ch_info = epg_index['display_name'].get(display_name)
            if ch_info:
                return self.create_match_result(
                    line, display_name, tvg_id, line_index,
                    True, "display-name匹配", ch_info["display-name"],
                    os.path.basename(ch_info["epg_file"])
                )
        
        # 4. 优化匹配（标准化名称，字典查找 O(1)）
        if enabled_types["optimized"]:
            norm_name = standardize_name(display_name)
            ch_info = epg_index['optimized'].get(norm_name)
            if ch_info:
                return self.create_match_result(
                    line, display_name, tvg_id, line_index,
                    True, "优化匹配", ch_info["display-name"],
                    os.path.basename(ch_info["epg_file"])
                )
        
        # 5. 模糊匹配（仅在需要时进行）
        if enabled_types["fuzzy"]:
            # 预计算标准化名称
            norm_display = standardize_name(display_name)
            if not norm_display:
                return self.create_match_result(line, display_name, tvg_id, line_index, False, "未匹配", None, "")
            
            # 进行模糊匹配（优化版：只计算一次标准化名称）
            best_match = None
            best_similarity = similarity_threshold
            match_type = "模糊匹配"
            
            for ch_info in epg_index['all_channels']:
                ch_display = ch_info["display-name"]
                
                # 快速过滤：如果名称长度差异太大，跳过
                if abs(len(norm_display) - len(standardize_name(ch_display))) > 10:
                    continue
                
                # 计算相似度
                sim = similar(norm_display, standardize_name(ch_display))
                
                if sim > best_similarity:
                    best_similarity = sim
                    best_match = ch_info
            
            if best_match:
                return self.create_match_result(
                    line, display_name, tvg_id, line_index,
                    True, match_type, best_match["display-name"],
                    os.path.basename(best_match["epg_file"])
                )
        
        # 未匹配
        return self.create_match_result(line, display_name, tvg_id, line_index, False, "未匹配", None, "")
    
    def create_match_result(self, line, display_name, tvg_id, line_index, matched, match_type, matched_channel, epg_file_name):
        """创建匹配结果字典的辅助函数"""
        return {
            "original_line": line,
            "display_name": display_name,
            "matched": matched,
            "match_type": match_type,
            "matched_channel": matched_channel,
            "epg_file": epg_file_name,
            "tvg_id": tvg_id,
            "line_index": line_index
        }
    
    def match_batch_channels(self, channel_batch, epg_index, similarity_threshold):
        """批量匹配频道（减少函数调用开销）"""
        results = []
        
        for channel_data in channel_batch:
            if self.stop_requested:
                break
            
            result = self.match_single_channel_fast(channel_data, epg_index, similarity_threshold)
            if result:
                results.append(result)
        
        return results
    
    def start_matching(self):
        """开始匹配过程"""
        if not self.m3u_lines:
            self.log("ERROR", "❌ 请先加载 M3U 文件")
            messagebox.showerror("错误", "请先加载 M3U 文件")
            return
        
        # 重置统计和计时器
        self.matched_channels = 0
        self.start_time = time.time()
        self.last_progress_time = None
        self.last_progress_count = 0
        self.estimated_remaining_time = "计算中..."
        self.stop_requested = False
        self.is_matching = True
        
        # 重置进度显示
        self.progress_var.set(0)
        self.progress_label.config(text="匹配进度 0.0% (0/0)")
        self.eta_label.config(text="预计完成时间: 计算中...")
        self.match_rate_label.config(text="匹配率: 0%")
        
        # 更新按钮状态
        self.update_button_states(True)
        
        # 在新线程中执行匹配
        self.matching_thread = threading.Thread(target=self.match_process_optimized, daemon=True)
        self.matching_thread.start()
    
    def stop_matching(self):
        """停止匹配过程"""
        self.stop_requested = True
        self.is_matching = False
        self.log("STOP", "⏹️ 正在停止匹配过程...")
        self.update_status("正在停止匹配...")
        
        # 更新按钮状态
        self.update_button_states(False)
        
        # 如果executor存在，尝试关闭
        if self.executor:
            try:
                self.executor.shutdown(wait=False, cancel_futures=True)
            except:
                pass
    
    def reset_matching(self):
        """重新匹配"""
        if self.is_matching:
            messagebox.showwarning("警告", "匹配正在进行中，请先停止匹配")
            return
        
        # 确认是否重新匹配
        if not messagebox.askyesno("重新匹配", "确定要重新匹配吗？\n这将重置当前匹配进度和结果。", parent=self.master):
            return
        
        # 重置匹配状态
        self.matched_channels = 0
        self.progress_var.set(0)
        self.progress_label.config(text="就绪")
        self.matched_label.config(text="已匹配: 0")
        self.time_label.config(text="耗时: 0s")
        self.match_rate_label.config(text="匹配率: 0%")
        self.eta_label.config(text="预计完成时间: -")
        self.update_status("匹配已重置")
        
        # 清空日志
        self.clear_log()
        self.log("INFO", "🔄 匹配状态已重置，可以重新开始匹配")
        
        # 更新按钮状态
        self.update_button_states(False)
    
    def match_process_optimized(self):
        """优化版匹配处理主函数（多线程版本）"""
        try:
            self.log("INFO", "🚀 开始优化匹配处理...")
            self.update_status("正在准备匹配...")
            
            # 1. 收集EPG源
            epg_sources = []
            
            # 本地EPG文件
            epg_local = self.epg_file_entry.get().strip()
            if epg_local and os.path.exists(epg_local):
                epg_sources.append(epg_local)
                self.log("INFO", f"📄 添加本地 EPG: {os.path.basename(epg_local)}")
            
            # EPG URL列表
            epg_urls = [
                line.strip() for line in self.epg_text.get("1.0", tk.END).splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]
            
            for i, url in enumerate(epg_urls, 1):
                if url.startswith("http"):
                    self.log("INFO", f"🌐 添加远程 EPG [{i}]: {url}")
                    epg_sources.append(url)
                else:
                    self.log("WARNING", f"⚠️  忽略无效URL: {url}")
            
            if not epg_sources:
                self.log("ERROR", "❌ 未找到有效的EPG源")
                self.update_status("匹配失败: 无EPG源")
                self.is_matching = False
                self.master.after(0, lambda: self.update_button_states(False))
                return
            
            # 2. 加载和解析EPG
            self.log("INFO", "📡 正在加载EPG数据...")
            self.update_status("正在加载EPG数据...")
            
            epg_channels = {}
            epg_loaded = 0
            
            for epg_source in epg_sources:
                # 检查是否请求停止
                if self.stop_requested:
                    self.log("INFO", "⏹️ 匹配已停止")
                    self.update_status("匹配已停止")
                    self.is_matching = False
                    self.master.after(0, lambda: self.update_button_states(False))
                    return
                
                result = load_epg_file(epg_source)
                
                if isinstance(result, tuple):
                    root, cached = result
                    if isinstance(root, str) and root.startswith("ERROR"):
                        self.log("ERROR", f"❌ EPG加载失败: {root}")
                        continue
                    
                    source_name = os.path.basename(epg_source) if not epg_source.startswith("http") else epg_source
                    cache_status = "📦 (缓存)" if cached else "⬇️ (下载)"
                    
                    channel_count = 0
                    for ch in root.findall("channel"):
                        ch_id = ch.get("id", "")
                        ch_name = ch.findtext("display-name", "")
                        if ch_id and ch_name:
                            epg_channels[ch_id] = {
                                "display-name": ch_name,
                                "epg_file": source_name
                            }
                            channel_count += 1
                    
                    epg_loaded += 1
                    self.log("SUCCESS", f"✅ {cache_status} EPG源: {source_name} ({channel_count}个频道)")
                else:
                    self.log("ERROR", f"❌ EPG加载失败: {result}")
            
            if not epg_channels:
                self.log("ERROR", "❌ 所有EPG源加载失败")
                self.update_status("匹配失败: EPG加载失败")
                self.is_matching = False
                self.master.after(0, lambda: self.update_button_states(False))
                return
            
            # 3. 构建EPG索引（关键优化）
            self.log("INFO", "🔨 构建EPG索引以加速匹配...")
            self.update_status("正在构建EPG索引...")
            epg_index = build_epg_index(epg_channels)
            self.log("INFO", f"✅ EPG索引构建完成: {len(epg_index['tvg_id'])}个tvg-id, {len(epg_index['display_name'])}个display-name")
            
            # 4. 显示启用的匹配类型
            enabled_types = []
            for key, var in self.match_vars.items():
                if var.get():
                    enabled_types.append(key)
            
            self.log("INFO", f"🔧 启用的匹配类型: {', '.join(enabled_types)}")
            
            # 5. 提取频道数据用于多线程处理
            self.log("INFO", f"🎯 准备多线程匹配 (使用 {self.thread_var.get()} 个线程)...")
            self.update_status("正在准备多线程匹配...")
            
            channel_data_list = []
            
            # 创建一个字典来存储每行的索引和对应的#EXTINF行
            extinf_indices = {}
            
            for line_index, line in enumerate(self.m3u_lines):
                if line.startswith("#EXTINF"):
                    # 解析频道信息
                    tvg_id_match = re.search(r'tvg-id="([^"]*)"', line)
                    tvg_name_match = re.search(r'tvg-name="([^"]*)"', line)
                    display_name = line.split(",")[-1].strip()
                    
                    tvg_id = tvg_id_match.group(1) if tvg_id_match else ""
                    tvg_name = tvg_name_match.group(1) if tvg_name_match else ""
                    
                    # 保存行索引和行内容
                    extinf_indices[line_index] = line
                    channel_data_list.append((line, display_name, tvg_id, tvg_name, line_index))
            
            self.total_channels = len(channel_data_list)
            self.total_label.config(text=f"频道总数: {self.total_channels}")
            
            # 检查是否请求停止
            if self.stop_requested:
                self.log("INFO", "⏹️ 匹配已停止")
                self.update_status("匹配已停止")
                self.is_matching = False
                self.master.after(0, lambda: self.update_button_states(False))
                return
            
            # 6. 多线程匹配（批量处理优化）
            self.log("INFO", f"🔍 开始匹配 {self.total_channels} 个频道...")
            self.update_status("正在匹配频道...")
            
            # 创建一个字典来存储匹配结果，键为行索引
            match_results_dict = {}
            match_report_data = []  # 用于生成报表
            unmatched = []
            
            # 使用ThreadPoolExecutor进行多线程匹配
            self.executor = ThreadPoolExecutor(max_workers=self.thread_var.get())
            
            # 批量处理参数
            batch_size = max(1, len(channel_data_list) // (self.thread_var.get() * 10))
            batch_size = min(batch_size, 50)  # 每批最多50个频道
            
            # 提交批量任务
            futures = []
            for i in range(0, len(channel_data_list), batch_size):
                batch = channel_data_list[i:i + batch_size]
                future = self.executor.submit(
                    self.match_batch_channels, 
                    batch, 
                    epg_index, 
                    self.similarity_var.get()
                )
                futures.append(future)
            
            # 处理完成的任务
            completed = 0
            for future in as_completed(futures):
                # 检查是否请求停止
                if self.stop_requested:
                    # 取消所有未完成的任务
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    break
                
                try:
                    batch_results = future.result()
                    
                    for result in batch_results:
                        if result is None:
                            continue
                        
                        line_index = result["line_index"]
                        
                        if result["matched"]:
                            self.matched_channels += 1
                            epg_file_name = result["epg_file"]
                            # 只在频道名称后面添加匹配信息，保留所有原始属性和参数
                            original_line = result["original_line"]
                            
                            # 使用正则表达式在频道名称后面添加匹配信息
                            display_name = result["display_name"]
                            
                            # 将匹配信息添加到频道名称后面
                            if original_line.endswith(display_name + "\n"):
                                new_line = original_line.replace(
                                    display_name + "\n", 
                                    f"{display_name} [匹配: {epg_file_name}]\n"
                                )
                            else:
                                new_line = original_line.replace(
                                    display_name, 
                                    f"{display_name} [匹配: {epg_file_name}]"
                                )
                            
                            match_results_dict[line_index] = new_line
                            
                            log_msg = f"✓ {result['display_name']} → {result['matched_channel']} [{result['match_type']}]"
                            self.log("MATCH", log_msg)
                        else:
                            # 未匹配，保留原始行
                            match_results_dict[line_index] = result["original_line"]
                            unmatched.append(result["display_name"])
                            # 减少日志输出以提高速度，每10个未匹配才记录一次
                            if len(unmatched) % 10 == 0:
                                self.log("UNMATCHED", f"✗ {result['display_name']} → 未匹配")
                        
                        # 添加到报表数据
                        match_report_data.append({
                            "原始显示名": result["display_name"],
                            "匹配结果": result["matched_channel"] or "未匹配",
                            "匹配类型": result["match_type"],
                            "EPG来源": result["epg_file"] or "N/A",
                            "tvg-id": result["tvg_id"] or "N/A"
                        })
                    
                    completed += len(batch_results)
                    
                    # 更新进度
                    self.update_progress(completed, self.total_channels, "匹配进度")
                    
                except Exception as e:
                    if not "cancelled" in str(e).lower():
                        self.log("ERROR", f"❌ 匹配过程中出错: {str(e)}")
            
            # 关闭executor
            if self.executor:
                self.executor.shutdown(wait=False)
                self.executor = None
            
            # 检查是否被停止
            if self.stop_requested:
                self.log("STOP", "⏹️ 匹配已停止")
                self.update_status("匹配已停止")
                elapsed_time = time.time() - self.start_time
                match_rate = (self.matched_channels / self.total_channels * 100) if self.total_channels > 0 else 0
                
                self.log("INFO", f"📊 已匹配: {self.matched_channels}/{self.total_channels} ({match_rate:.1f}%)")
                self.log("INFO", f"⏱️  耗时: {elapsed_time:.2f} 秒")
                
                # 保存部分结果
                if self.matched_channels > 0:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    base_name = os.path.splitext(os.path.basename(self.m3u_file))[0] if self.m3u_file else "output"
                    out_name = os.path.join("m3u", f"{base_name}_部分匹配_{timestamp}.m3u")
                    
                    # 重建完整的M3U文件，保留所有原始行
                    final_output = []
                    for i, line in enumerate(self.m3u_lines):
                        if i in match_results_dict:
                            # 使用匹配后的#EXTINF行
                            final_output.append(match_results_dict[i])
                        else:
                            # 保留所有其他行
                            final_output.append(line)
                    
                    with open(out_name, "w", encoding="utf-8") as f:
                        f.writelines(final_output)
                    
                    self.log("INFO", f"📁 部分结果已保存到: {out_name}")
                
                self.is_matching = False
                self.master.after(0, lambda: self.update_button_states(False))
                return
            
            # 7. 重建完整的M3U文件（保留所有原始行）
            self.log("INFO", "🔄 正在重建M3U文件...")
            
            # 8. 保存输出文件
            self.log("INFO", "💾 正在保存匹配结果...")
            self.update_status("正在保存结果...")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = os.path.splitext(os.path.basename(self.m3u_file))[0] if self.m3u_file else "output"
            out_name = os.path.join("m3u", f"{base_name}_已匹配_{timestamp}.m3u")
            
            # 重建完整的M3U文件，保留所有原始行
            final_output = []
            for i, line in enumerate(self.m3u_lines):
                if i in match_results_dict:
                    # 使用匹配后的#EXTINF行
                    final_output.append(match_results_dict[i])
                else:
                    # 保留所有其他行，包括:
                    # - #EXTM3U 头部
                    # - #KODIPROP 等播放器参数
                    # - 播放链接
                    # - 其他所有行
                    final_output.append(line)
            
            with open(out_name, "w", encoding="utf-8") as f:
                f.writelines(final_output)
            
            # 9. 保存匹配统计报表（CSV格式）
            csv_name = os.path.join("m3u", f"匹配统计_{timestamp}.csv")
            with open(csv_name, "w", encoding="utf-8-sig", newline="") as csvfile:  # utf-8-sig支持Excel中文
                fieldnames = ["原始显示名", "匹配结果", "匹配类型", "EPG来源", "tvg-id"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for result in match_report_data:
                    writer.writerow(result)
            
            # 10. 保存详细日志文件
            log_name = os.path.join("m3u", f"匹配报告_{timestamp}.txt")
            elapsed_time = time.time() - self.start_time
            
            with open(log_name, "w", encoding="utf-8") as f:
                f.write("=" * 70 + "\n")
                f.write("EPG M3U 智能匹配报告 (优化速度版)\n")
                f.write("=" * 70 + "\n\n")
                
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"耗时: {elapsed_time:.2f} 秒\n")
                f.write(f"使用线程数: {self.thread_var.get()}\n\n")
                
                f.write(f"M3U文件: {self.m3u_file or '未知'}\n")
                f.write(f"EPG源数量: {epg_loaded}\n")
                f.write(f"EPG频道数: {len(epg_channels)}\n")
                f.write(f"总频道数: {self.total_channels}\n")
                f.write(f"匹配成功: {self.matched_channels}\n")
                match_rate = (self.matched_channels / self.total_channels * 100) if self.total_channels > 0 else 0
                f.write(f"匹配率: {match_rate:.1f}%\n\n")
                
                f.write("启用的匹配类型:\n")
                for key, var in self.match_vars.items():
                    status = "✓" if var.get() else "✗"
                    f.write(f"  {status} {key}\n")
                f.write(f"模糊匹配阈值: {self.similarity_var.get()}\n\n")
                
                f.write("匹配类型统计:\n")
                match_type_stats = {}
                for result in match_report_data:
                    match_type = result["匹配类型"]
                    match_type_stats[match_type] = match_type_stats.get(match_type, 0) + 1
                
                for match_type, count in match_type_stats.items():
                    f.write(f"  {match_type}: {count}\n")
                
                if unmatched:
                    f.write("\n" + "-" * 60 + "\n")
                    f.write(f"未匹配频道列表 ({len(unmatched)}个):\n")
                    f.write("-" * 60 + "\n")
                    for name in unmatched[:50]:  # 只显示前50个
                        f.write(name + "\n")
                    if len(unmatched) > 50:
                        f.write(f"... 还有 {len(unmatched)-50} 个未显示\n")
            
            # 11. 完成处理
            self.log("SUCCESS", "=" * 60)
            self.log("SUCCESS", "🎉 匹配完成!")
            self.log("SUCCESS", "=" * 60)
            self.log("INFO", f"📊 统计信息:")
            self.log("INFO", f"   总频道数: {self.total_channels}")
            self.log("INFO", f"   匹配成功: {self.matched_channels}")
            self.log("INFO", f"   匹配率: {match_rate:.1f}%")
            self.log("INFO", f"   耗时: {elapsed_time:.2f} 秒")
            self.log("INFO", f"   使用线程: {self.thread_var.get()}")
            self.log("INFO", f"   处理速度: {self.total_channels/elapsed_time:.1f} 频道/秒")
            self.log("INFO", f"📁 输出文件: {out_name}")
            self.log("INFO", f"📋 详细报告: {log_name}")
            self.log("INFO", f"📈 统计报表: {csv_name}")
            
            # 显示匹配类型统计
            self.log("INFO", "📊 匹配类型统计:")
            for match_type, count in match_type_stats.items():
                self.log("INFO", f"   {match_type}: {count}")
            
            self.update_status(f"匹配完成: {match_rate:.1f}% ({self.matched_channels}/{self.total_channels})")
            self.progress_label.config(text=f"匹配完成! 匹配率: {match_rate:.1f}%")
            self.eta_label.config(text="匹配完成!")
            
            # 播放完成提示音
            self.master.bell()
            
            # 显示完成对话框
            self.master.after(0, lambda: messagebox.showinfo(
                "匹配完成",
                f"优化匹配处理完成!\n\n"
                f"总频道数: {self.total_channels}\n"
                f"匹配成功: {self.matched_channels}\n"
                f"匹配率: {match_rate:.1f}%\n"
                f"耗时: {elapsed_time:.2f}秒\n"
                f"处理速度: {self.total_channels/elapsed_time:.1f} 频道/秒\n"
                f"使用线程: {self.thread_var.get()}\n\n"
                f"文件已保存到 m3u/ 文件夹:\n"
                f"- 已匹配的M3U文件 (保留所有播放数据)\n"
                f"- 详细匹配报告\n"
                f"- CSV统计报表"
            ))
            
        except Exception as e:
            self.log("ERROR", f"❌ 匹配过程中出现错误: {str(e)}")
            import traceback
            self.log("ERROR", traceback.format_exc())
            self.update_status(f"匹配失败: {str(e)}")
        finally:
            # 更新匹配状态
            self.is_matching = False
            
            # 重新启用按钮
            self.master.after(0, lambda: self.update_button_states(False))
            
            if not self.stop_requested:
                self.progress_var.set(100)

# -------------------- 启动应用程序 --------------------
if __name__ == "__main__":
    # 设置DPI感知
    try:
        from ctypes import windll
        windll.shcore.SetProcessDpiAwareness(1)
    except:
        pass
    
    root = tk.Tk()
    
    # 设置窗口居中
    window_width = 1100
    window_height = 800
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    center_x = int(screen_width/2 - window_width/2)
    center_y = int(screen_height/2 - window_height/2)
    root.geometry(f"{window_width}x{window_height}+{center_x}+{center_y}")
    
    app = EPGMatcherGUI(root)
    root.mainloop()