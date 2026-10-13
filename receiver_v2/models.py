# -*- coding: utf-8 -*-
"""
models.py — 业务数据模型

包含两个核心数据结构:
  1. Device : 单个 ESP 设备的所有运行时状态 (网络信息 / CSI 缓存 / TCP 控制队列)
  2. _State : 全局共享状态 (设备表 / 采集模式 / 视频录制 / 采样频率)

_State 使用类属性做全局单例, 访问方式为 S.xxx (例如 S.running, S.devices).
所有跨线程共享的可变对象都加了 Lock 保护, 详见字段注释.
"""
from __future__ import annotations

import collections
import queue
import threading
import time

import numpy as np

from constants import DEFAULT_SAMPLE_RATE

# 实际采样率统计窗口大小 (秒): 仅统计该窗口内到达的 CSI 包,
# 由此换算"实测 Hz". 窗口越短越灵敏, 越长越稳定.
ACTUAL_RATE_WINDOW_SEC = 2.0


# ============================================================
# 跨线程 GUI 事件队列 (后台线程 → GUI 主线程)
# ============================================================
# 事件格式: (kind: str, payload: Any)
#   kind 取值:
#     "device_added"            新设备首次 HELLO (payload = ip)
#     "device_hello"            已存在设备的周期性 HELLO (payload = ip)
#     "device_online_changed"   在线状态翻转 (payload = ip)
#     "udp_error"               UDP 端口绑定失败 (payload = 错误描述)
#     "udp_port"                UDP 实际绑定端口 (payload = int)
#     "phone_status"            手机在线状态变化 (payload = bool)
#     "csi_start"               GUI 通知自己开始累积 (payload = None 或 file_path)
#     "video_fail"              视频录制失败 (payload = err_str)
_gui_event_queue: queue.Queue = queue.Queue()


# ============================================================
# Device: 单台 ESP 的完整状态
# ============================================================
class Device:
    """单个 ESP 设备的所有状态.

    使用 __slots__ 限制属性, 防止运行时任意加字段;
    所有 CSI 数据相关的可变结构都自带 Lock, 可在 UDP 线程和 GUI 线程间共享.
    """

    # 字段白名单, 改字段时这里要同步加
    __slots__ = (
        # 网络标识
        "ip", "mac", "name", "position",
        # 选中 / 在线状态
        "selected", "online", "last_seen", "last_hello",
        # 最近一次 HELLO 上报的信息
        "rssi", "csi_len", "seq", "tcp_status",
        # ESP 端状态字段
        "esp_state", "esp_packets", "esp_streaming",
        # CSI 数据缓冲
        "csi_queue", "all_csi_data",
        # 可视化用的振幅缓冲 + 多普勒 + 方差
        "amp_buf", "amp_buf_lock", "doppler_data", "variance",
        # 采集计数
        "running_packets", "total_packets",
        # TCP 控制命令队列 + 采样频率下发标记
        "tcp_cmd_queue", "rate_pushed",
        # 实测采样率: packet_timestamps (滑动窗口) + 计算结果 actual_rate (Hz)
        "packet_timestamps", "packet_ts_lock", "actual_rate", "actual_rate_ts",
    )

    def __init__(self, ip: str, name: str = None, position: str = "东南角"):
        # --- 网络标识 ---
        self.ip       = ip
        self.mac      = "等待连接..."
        self.name     = name if name else f"ESP-{ip.split('.')[-1]}"
        self.position = position
        # --- 选中 / 在线 ---
        self.selected   = True
        self.online     = False
        self.last_seen  = 0.0   # 上次收到任意 UDP 包的时间 (time.time())
        self.last_hello = 0.0   # 上次收到 HELLO 包的时间, 用于"在线"判定
        # --- HELLO 上报 ---
        self.rssi       = None
        self.csi_len    = None
        self.seq        = 0
        self.tcp_status = "未连接"
        # --- ESP 状态 ---
        self.esp_state    = ""
        self.esp_packets  = 0
        self.esp_streaming = False
        # --- CSI 数据缓冲 ---
        # csi_queue : UDP 线程往里 put, GUI 线程 get; queue 本身线程安全
        # all_csi_data : 当前正在累积的窗口, GUI 线程独占
        self.csi_queue    = queue.Queue(maxsize=1000)
        self.all_csi_data = []
        # --- TCP 命令下发队列 (元素是 (cmd_str, response_queue) ) ---
        self.tcp_cmd_queue = queue.Queue()
        # --- 采样频率已下发标记 ---
        self.rate_pushed = False
        # --- 振幅可视化环形缓冲 (含 Lock 保护, UDP 写 / GUI 读) ---
        self.amp_buf      = []
        self.amp_buf_lock = threading.Lock()
        # --- 多普勒 / 方差 (供 _on_amplitude_update 写入) ---
        self.doppler_data = np.zeros((33, 1), dtype=np.float32)
        self.variance     = 0.0
        # --- 采集计数 ---
        self.running_packets = 0  # 当前采集会话已收集的 CSI 包数
        self.total_packets   = 0  # 累计 CSI 包数
        # --- 实测采样率 (基于 UDP 接收时间戳) ---
        # deque 自动丢弃超出 maxlen 的最早元素, 配合 Lock 实现线程安全.
        # 元素: time.time() 秒. maxlen 取 5000 (≈ 1000Hz × 5s 上限), 留出余量.
        self.packet_timestamps = collections.deque(maxlen=5000)
        self.packet_ts_lock    = threading.Lock()
        self.actual_rate       = 0.0  # 最近 2 秒窗口内的实测 Hz
        self.actual_rate_ts    = 0.0  # 上次计算 actual_rate 的时间戳

    def update_actual_rate(self, now: float | None = None, window: float = ACTUAL_RATE_WINDOW_SEC) -> float:
        """基于最近 'window' 秒内到达的包数, 计算实测采样频率 (Hz).

        调用方: GUI 线程 (150ms 一次) 或 UDP 线程 (按需).
        算法:
          1) 在锁内拷贝 deque 头尾两端, 把超出窗口的过期时间戳弹出;
          2) 若剩余样本 >= 2 且首尾跨度 >= window/2, 取 (count-1)/span;
          3) 否则视为样本不足, 返回 0.

        返回值是新的 actual_rate (Hz), 数值保留 1 位小数.
        """
        if now is None:
            now = time.time()
        with self.packet_ts_lock:
            # 弹出超过窗口的过期时间戳 (左侧是队首, 最早)
            cutoff = now - window
            while self.packet_timestamps and self.packet_timestamps[0] < cutoff:
                self.packet_timestamps.popleft()
            n = len(self.packet_timestamps)
            if n < 2:
                rate = 0.0
            else:
                span = self.packet_timestamps[-1] - self.packet_timestamps[0]
                if span <= 0.05:  # 包全部挤在同一瞬间 (如刚上线首帧), 视为无效
                    rate = 0.0
                else:
                    # count / span 得到平均 Hz, 比 (count-1)/span 更贴近实际包率
                    rate = n / span
            self.actual_rate = rate
            self.actual_rate_ts = now
        return rate

    def record_packet_arrival(self, now: float | None = None) -> None:
        """UDP 线程调用: 记录一个包到达时间戳到滑动窗口. 锁开销极小, 不影响吞吐."""
        if now is None:
            now = time.time()
        with self.packet_ts_lock:
            self.packet_timestamps.append(now)

    def assign_name_from_mac(self, mac_str: str) -> None:
        """根据 MAC 地址生成稳定设备名 ESP-XX (XX = MAC 最后一字节的 2 位大写 hex).

        设计要点:
          - IP 变化不影响设备名 (MAC 是硬件地址, 唯一不变).
          - 用 MAC 最后 1 字节 (00-FF) 作 2 位 hex, 共 256 个值.
          - 极小概率碰撞时 (典型 4-8 台设备不会遇到) 退回用最后 2 字节的 4 位 hex
            (如 ESP-12AB), 几乎保证唯一.

        调用方: UDP 线程 / TCP 线程拿到 STATUS 后, 在 devices_lock 内调用.
        不在此处做碰撞检测 (需要在所有 device 上扫描), 由调用方负责.
        """
        if not mac_str or not isinstance(mac_str, str):
            return
        # 解析 MAC 字节 (兼容 "AA:BB:CC:DD:EE:12" / "AA-BB-..." / "AABBCCDDEE12")
        hex_part = mac_str.replace(":", "").replace("-", "").replace(" ", "")
        if len(hex_part) < 2:
            return
        try:
            last_byte = int(hex_part[-2:], 16)  # 最后 1 字节 = 2 个 hex 字符
        except ValueError:
            return
        self.name = f"ESP-{last_byte:02X}"


def unique_name_from_mac(mac_str: str, current_name: str = "",
                         existing_names: set[str] | None = None) -> str:
    """生成全局唯一的 ESP-XX 名称, 在所有已知设备名不冲突.

    优先: MAC 最后 1 字节 → "ESP-12"
    碰撞: MAC 最后 2 字节 → "ESP-12AB"
    仍碰撞: 拼上 IP 末段  → "ESP-12AB-107" (理论上几乎不可能)

    existing_names: 已知的所有设备名集合 (调用方在 devices_lock 内构造).
    返回的字符串不含 current_name 已存在冲突时, 也照样重命名 (因为 IP 变了).
    """
    if not mac_str:
        return current_name or "ESP-?"
    hex_part = mac_str.replace(":", "").replace("-", "").replace(" ", "")
    if len(hex_part) < 2:
        return current_name or "ESP-?"
    try:
        last_byte = int(hex_part[-2:], 16)
    except ValueError:
        return current_name or "ESP-?"
    name = f"ESP-{last_byte:02X}"
    if existing_names is None or name not in existing_names:
        return name
    # 碰撞: 退到 4 位 hex
    if len(hex_part) >= 4:
        try:
            last_two = int(hex_part[-4:], 16)
            name2 = f"ESP-{last_two:04X}"
            if name2 not in existing_names:
                return name2
        except ValueError:
            pass
    # 仍碰撞 (极端): 加 IP 末段 (兜底, 仅本机 fallback 用)
    return f"ESP-{last_byte:02X}-DUP"


# ============================================================
# _State: 进程级全局状态 (类属性即全局变量)
# ============================================================
class _State:
    """全局状态, 字段都是类属性 (单例模式).

    加锁原则: 任何在 UDP / TCP / 心跳 / 视频子线程里修改,
    又同时在 GUI 主线程里读取的字段, 必须配 Lock.
    """

    # ---- 设备表 ----
    devices      = {}                    # ip -> Device
    devices_lock = threading.Lock()      # 保护 devices 的并发读写

    # ---- 采集运行标志 ----
    running       = False   # 任意采集模式正在运行
    timer_running = False   # 按窗口采集中
    timed_mode    = False   # 按窗口 (定时) 模式
    frame_mode    = False   # 按帧采集模式
    target_frames = 0       # 按帧采集的目标帧数 (每设备)
    target_files  = 1       # 按帧采集中要保存的文件数量
    saved_files   = 0       # 当前会话已保存的文件数 (按窗口/按帧共用)
    # 多文件增量采集 (按帧/按窗口) 中, 第一次保存时确定"每个设备"各自的子文件夹,
    # 后续 N 个文件都存入该设备自己的目录, 不与其他设备混在一起.
    frame_session_dirs = {}  # dev_ip -> 目录路径 (按帧数采集)
    frame_target_reached = ""  # 第一个达到 target_frames 的设备 IP
    # 按窗口采集 (定时多文件): 每过"窗口长度"秒自动保存一个文件
    win_target_files = 1     # 按窗口采集要保存的文件数量
    win_session_dirs = {}    # dev_ip -> 目录路径 (按窗口采集)

    # ---- 计时 / 选中 ----
    start_time    = None    # 当前采集会话的开始时间戳
    active_device = None    # GUI 当前选中的设备 IP

    # ---- 语音提示 (按窗口采集专用) ----
    # 记录"本次采集会话开始时是否启用了语音提示", 用于停止时判断是否播报"采集结束".
    # 若按本次勾选开始, 中途再取消勾选, 仍会播报一次 (因为本次会话仍按语音流程启动).
    voice_session_active = False

    # ---- IP Webcam 视频录制 ----
    phone_ip         = ""       # 用户填写的手机 IP
    phone_online     = False    # 心跳探测结论
    phone_last_check = 0.0      # 上次探测时间
    phone_check_lock = threading.Lock()
    video_enabled    = False    # 用户是否勾选 "同时采集视频"
    video_recording  = False    # 当前是否在录像
    video_start_ts   = 0.0
    video_stop_ts    = 0.0
    video_filename   = ""
    video_sync_log   = ""
    last_video_path  = ""
    ffmpeg_proc      = None     # ffmpeg 子进程

    # ---- 采样频率 (Hz, ESP 主动 ping 路由器) ----
    sample_rate         = DEFAULT_SAMPLE_RATE  # 当前配置的频率
    applied_sample_rate = None                 # 已下发的频率 (用于检测变化)

    # ---- 本机 IP (用于过滤回环 UDP) ----
    local_ip = None  # _udp_listener 启动时初始化一次


# 单例实例, 业务代码统一用 S.xxx 访问
S = _State()
