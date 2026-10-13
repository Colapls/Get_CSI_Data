# -*- coding: utf-8 -*-
"""
socketio_bridge.py — WebSocket 事件桥接器

把 receiver/ 后台模块产生的 _gui_event_queue 和 _State 状态变化,
转换成 SocketIO 事件推送到所有连接的浏览器客户端。

启动两个后台线程:
  1. _event_pump(sio)        排空 _gui_event_queue, 翻译后 emit
  2. _snapshot_pump(sio)     每 250ms 抓一次"全设备快照 + 当前状态",
                              推送给前端用于刷新设备列表 + 状态栏 + 顶部实测Hz

复用原则:
  - 不修改 receiver/ 任何业务模块; 全部用 import + 读 _State.
  - CSI 帧推送: 客户端 emit 'select_device' 选定设备后, 服务端只对该设备
    的 csi_queue 做批 drain, 减少带宽与前端负担.
  - csi_start 事件: 由 api.py 的 _start_video_then_csi 通过 _gui_event_queue 投递,
    触发 S.running / S.timed_mode / S.frame_mode 翻转, 与 gui.py._refresh_plot 同款.
"""
from __future__ import annotations

import queue
import threading
import time
from datetime import datetime

import numpy as np

# 复用 receiver 的业务模块 (不重复造轮子)
from constants import (
    BROADCAST_HELLO, CHINESE_FONT_BOLD, FONT_FAMILY,
    PORT, SAMPLE_RATE_MIN, SAMPLE_RATE_MAX, VIDEO_SAVE,
)
from models import Device, S, _gui_event_queue
from udp_listener import _tcp_listener_for, _udp_listener
from device_status import _device_online_watchdog, _phone_heartbeat_loop


# ============================================================
# 启动入口 (被 app.py 调用一次)
# ============================================================
def start_background_threads(socketio):
    """启动 5 个后台线程.

    设计要点:
      - 后台线程全部 daemon=True, 主进程退出时自动结束.
      - _udp_listener / _phone_heartbeat_loop / _device_online_watchdog
        完全复用 receiver 原版, 不修改其内部行为.
      - 新增 _event_pump / _snapshot_pump: 把事件和状态转发到 socketio.
      - 新增 _timed_frame_watch_loop: 按窗口 / 按帧数采集中, 看门狗定时检查
        目标是否达到, 达到则自动保存并停止 (与 gui.py 同款).
    """
    threading.Thread(target=_udp_listener, daemon=True,
                     name="udp-listener").start()
    threading.Thread(target=_phone_heartbeat_loop, daemon=True,
                     name="phone-heartbeat").start()
    threading.Thread(target=_device_online_watchdog, daemon=True,
                     name="device-watchdog").start()
    threading.Thread(target=_event_pump, args=(socketio,), daemon=True,
                     name="socketio-event-pump").start()
    threading.Thread(target=_snapshot_pump, args=(socketio,), daemon=True,
                     name="socketio-snapshot-pump").start()
    threading.Thread(target=_timed_frame_watch_loop, args=(socketio,),
                     daemon=True, name="web-watch-loop").start()
    print("[bridge] 后台线程已启动 (udp/phone/watchdog/event/snapshot/web-watch)")


# ============================================================
# 事件泵: 翻译 _gui_event_queue → socketio.emit
# ============================================================
def _event_pump(sio):
    """排空 _gui_event_queue 并 emit 对应 socketio 事件.

    事件 → socketio event 映射:
      device_added            → 'device_added'        (payload: ip)
      device_hello            → 'device_hello'        (payload: ip)
      device_online_changed   → 'device_update'       (full snapshot)
      udp_error               → 'message'             (red)
      udp_port                → 'message'             (orange)
      phone_status            → 'phone_update'        (payload: bool)
      csi_start / video_fail  → 'status_update'       (payload: snapshot)
                                + 翻转 S.running / S.timed_mode / S.frame_mode
    """
    while True:
        try:
            try:
                kind, payload = _gui_event_queue.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                if kind == "device_added":
                    sio.emit("device_added", {"ip": payload})
                    # 新设备: 顺便启 TCP 控制长连接
                    try:
                        threading.Thread(target=_tcp_listener_for,
                                         args=(payload,), daemon=True).start()
                    except Exception:
                        pass
                    sio.emit("message", {
                        "level": "green", "text": f"✓ 新设备发现: {payload}"})
                elif kind == "device_hello":
                    sio.emit("device_hello", {"ip": payload})
                elif kind == "device_online_changed":
                    sio.emit("device_update", {"ip": payload})
                elif kind == "udp_error":
                    sio.emit("message", {
                        "level": "red", "text": f"✗ UDP 监听失败: {payload}"})
                elif kind == "udp_port":
                    sio.emit("message", {
                        "level": "orange",
                        "text": f"! 端口冲突 → UDP {payload}"})
                elif kind == "phone_status":
                    sio.emit("phone_update", {"online": bool(payload)})
                elif kind in ("csi_start", "video_fail"):
                    # csi_start: 视频就绪 (或无需视频), 翻转 S.running 并开始计时
                    # video_fail: 视频启动失败但仍要开始 CSI
                    if _state().get("_pending_start"):
                        _state()["_pending_start"] = False
                        mode = _state().get("_pending_mode", "free")
                        timed  = (mode == "window")
                        framed = (mode == "frame")
                        S.running       = True
                        S.timer_running = timed
                        S.timed_mode    = timed
                        S.frame_mode    = framed
                        if framed:
                            S.frame_target_reached = ""
                        S.start_time = time.time()
                        # 把当前采样频率下发到选中的设备 (走所有在线设备)
                        from socketio_bridge import _push_sample_rate_now
                        _push_sample_rate_now()
                        # LED 模式
                        led = "WINDOW" if timed or framed else "FREE"
                        with S.devices_lock:
                            online_ips = [ip for ip, d in
                                          S.devices.items() if d.online]
                        _push_led_to_online(led)
                        # 清空缓冲
                        with S.devices_lock:
                            for d in S.devices.values():
                                d.all_csi_data    = []
                                d.running_packets = 0
                        # 推送当前状态快照
                        sio.emit("status_update", _build_status_snapshot())
                        # 状态栏文本
                        n = sum(1 for d in S.devices.values()
                                if d.selected and d.online)
                        if kind == "video_fail":
                            sio.emit("message", {
                                "level": "orange",
                                "text": f"CSI 采集中 · {n} 台 (视频失败)"})
                        elif payload:
                            sio.emit("message", {
                                "level": "green",
                                "text": f"采集中 · {n} 台 + 视频"})
                        else:
                            sio.emit("message", {
                                "level": "green",
                                "text": f"采集中 · {n} 台"})
                    else:
                        # 非 _pending_start 状态, 仅推送状态
                        sio.emit("status_update", _build_status_snapshot())
            except Exception as e:
                print(f"[bridge] event emit error: {e}")
        except Exception:
            time.sleep(0.1)


# ============================================================
# 快照泵: 每 250ms 推送全设备快照 + 当前选中设备详情
# ============================================================
_active_device_ip = {"ip": None}
_active_device_lock = threading.Lock()


def set_active_device(ip):
    """前端切换设备时调用, 通知后端推送该设备的 CSI 帧."""
    with _active_device_lock:
        _active_device_ip["ip"] = ip
    # 同步更新全局状态
    with S.devices_lock:
        S.active_device = ip


def get_active_device():
    with _active_device_lock:
        return _active_device_ip["ip"]


def _snapshot_pump(sio):
    """周期推送 4 类数据:
      1) device_snapshot: 所有设备的概要 (用于设备列表渲染, 含实测Hz)
      2) active_device_detail: 当前选中设备的详细状态 (用于底部状态栏)
      3) csi_batch: 当前选中设备的 CSI 包批量 (用于波形绘制)
      4) status_update: 采集状态 + 进度 + 实测Hz

    频率: 200ms (5Hz). 设备列表的实测Hz 列每行实时更新, 不再依赖点击切换;
    CSI 波形刷新约 5fps 仍流畅, CPU/带宽开销可忽略.
    """
    last_emit = 0.0
    while True:
        time.sleep(0.2)
        now = time.time()
        if now - last_emit < 0.19:
            continue
        last_emit = now
        try:
            # ★ 关键: 先把所有设备的 actual_rate 重算一次, 不再依赖 active device.
            #   旧逻辑只对 active 设备 update_actual_rate(), 其它设备的 Hz 会卡在
            #   上次 active 时算出的旧值.
            _refresh_all_actual_rates(now)
            snap = _build_device_snapshot()
            sio.emit("device_snapshot", snap)
            active_ip = get_active_device()
            if active_ip:
                detail = _build_active_detail(active_ip)
                if detail:
                    sio.emit("active_device_detail", detail)
                batch = _drain_csi_batch(active_ip, max_items=80)
                if batch:
                    sio.emit("csi_batch", batch)
            sio.emit("status_update", _build_status_snapshot())
        except Exception as e:
            print(f"[bridge] snapshot pump error: {e}")


def _refresh_all_actual_rates(now: float | None = None) -> None:
    """对所有在线设备调用 update_actual_rate(), 让设备列表的 Hz 列实时刷新.

    不加锁: update_actual_rate() 内部用 packet_ts_lock 串行化, 与 record_packet_arrival
    安全并发 (deque.append + popleft 都是原子的). devices 字典本身在 snapshot pump
    里只读不写, 没问题.
    """
    if now is None:
        now = time.time()
    with S.devices_lock:
        devices = list(S.devices.values())
    for d in devices:
        try:
            d.update_actual_rate(now)
        except Exception:
            pass


# ============================================================
# 按窗口 / 按帧数 看门狗 (与 gui.py._start_timed_watch_loop / _start_frame_watch_loop 同款)
# ============================================================
def _timed_frame_watch_loop(sio):
    """后台线程: 周期检查"按窗口 / 按帧数"采集是否达到目标,
    达到则触发自动保存 (类似 gui.py 的 watch loop)."""
    # 共享配置 (来自 api.py._state())
    from api import _state as _api_state
    while True:
        time.sleep(0.3)
        try:
            if not S.running:
                continue
            cfg = _api_state()["config"]
            if S.timed_mode:
                _check_timed_window(sio, cfg)
            elif S.frame_mode:
                _check_frame_window(sio, cfg)
        except Exception as e:
            print(f"[bridge] watch loop error: {e}")


def _check_timed_window(sio, cfg):
    """按窗口采集中: 每过 窗口长度 秒, 把累积的数据保存为下一个文件。

    计时基准是 S.start_time (整个采集会话的开始时间), 不是单个窗口的开始时间.
    所以阈值是 (saved_files + 1) * win_sec, 不要在每次保存后重置 start_time,
    否则从下一次保存起阈值会成倍增长, 8 个文件 5s 窗口只会保存 ~3 个文件."""
    win_sec = max(1, int(cfg.get("win_len", 5)))
    target_n = max(1, S.win_target_files)
    if S.start_time is None:
        return
    elapsed = time.time() - S.start_time
    if elapsed < (S.saved_files + 1) * win_sec:
        return
    # 到时间了, 触发保存
    is_last = (S.saved_files + 1 >= target_n)
    try:
        sio.emit("message", {
            "level": "green",
            "text": (f"✓ 已到窗口时长, 保存 {S.saved_files + 1}/{target_n}..."
                     if not is_last else "✓ 已到窗口时长, 保存最后文件...")})
    except Exception:
        pass
    # 同步保存 (不能开线程: 否则 saved_files 的递增、all_csi_data 的清空
    # 会与保存线程竞争, 导致文件编号错乱 / 大小忽大忽小)
    from api import _save_all_then_reset
    _save_all_then_reset(sio)
    S.saved_files += 1
    # 存够目标文件数, 主动停止 (skip_save=True: 最后一个文件刚已保存, 不要再存一次)
    if S.saved_files >= target_n:
        from api import _do_stop
        _do_stop(sio, skip_save=True)
    else:
        # 未存够: 只清零 running_packets, 不要重置 S.start_time.
        # S.start_time 保留为整个会话的起始时刻, 让阈值
        # (saved_files + 1) * win_sec 与 elapsed 保持单调增长.
        with S.devices_lock:
            for d in S.devices.values():
                d.running_packets = 0


def _check_frame_window(sio, cfg):
    """按帧采集中: 任一设备先达 target_frames, 触发保存。"""
    target = S.target_frames
    target_n = max(1, S.target_files)
    if target <= 0:
        return
    winner_ip = ""
    winner_name = ""
    with S.devices_lock:
        for ip, d in S.devices.items():
            if not d.selected or not d.online:
                continue
            if d.running_packets >= target:
                winner_ip = ip
                winner_name = d.name
                break
    if not winner_ip:
        return
    S.frame_target_reached = winner_ip
    is_last = (S.saved_files + 1 >= target_n)
    try:
        sio.emit("message", {
            "level": "purple",
            "text": (f"✓ {winner_name} 先达 {target} 帧, 保存"
                     f" {S.saved_files + 1}/{target_n}..."
                     if not is_last else
                     f"✓ {winner_name} 先达 {target} 帧, 保存最后文件...")})
    except Exception:
        pass
    # 同步保存 (同 _check_timed_window, 避免线程竞争)
    from api import _save_all_then_reset
    _save_all_then_reset(sio)
    S.saved_files += 1
    if S.saved_files >= target_n:
        from api import _do_stop
        _do_stop(sio, skip_save=True)
    else:
        # 重置 running_packets, 让下一文件继续累积
        with S.devices_lock:
            for d in S.devices.values():
                d.running_packets = 0
        S.frame_target_reached = ""


# ============================================================
# 快照构造器
# ============================================================
def _build_device_snapshot():
    """构造所有设备的概要, 给前端设备列表用."""
    items = []
    with S.devices_lock:
        devices = list(S.devices.values())
    for d in devices:
        actual = round(d.actual_rate) if d.online and d.actual_rate > 0.5 else None
        items.append({
            "ip": d.ip,
            "name": d.name,
            "selected": d.selected,
            "online": d.online,
            "rssi": d.rssi,
            "position": d.position,
            "actual_hz": actual,
        })
    return {"devices": items, "ts": time.time()}


def _build_active_detail(ip):
    """构造当前选中设备的详细状态 (用于底部状态栏)."""
    with S.devices_lock:
        d = S.devices.get(ip)
    if not d:
        return None
    d.update_actual_rate()
    rssi = d.rssi
    pct = None
    if rssi is not None:
        pct = max(0, min(100, int((rssi + 100) * 1.25)))
    return {
        "ip": d.ip,
        "name": d.name,
        "mac": d.mac,
        "rssi": rssi,
        "rssi_pct": pct,
        "csi_len": d.csi_len,
        "total_packets": d.total_packets,
        "running_packets": d.running_packets,
        "online": d.online,
        "tcp_status": d.tcp_status,
        "esp_state": getattr(d, "esp_state", ""),
        "position": getattr(d, "position", ""),
        "variance": round(d.variance, 2),
        "actual_rate": round(d.actual_rate, 1),
        "seq": d.seq,
    }


def _build_status_snapshot():
    """构造采集状态 + 进度 + 中部状态栏文本."""
    elapsed = 0
    if S.running and S.start_time:
        elapsed = int(time.time() - S.start_time)
    avg_actual = 0.0
    n_active = 0
    with S.devices_lock:
        for d in S.devices.values():
            if d.selected and d.online and d.actual_rate > 0.5:
                avg_actual += d.actual_rate
                n_active += 1
    if n_active > 0:
        avg_actual = round(avg_actual / n_active, 1)
    cfg_hz = int(round(S.sample_rate))
    # target_files: 按窗口/按帧共享
    if S.frame_mode:
        target_files = max(1, S.target_files)
    else:
        target_files = max(1, S.win_target_files)
    # 当前领先设备的帧数 (按帧数采集中)
    leader_ip = ""
    leader_name = ""
    leader_n = 0
    if S.frame_mode:
        with S.devices_lock:
            for ip, d in S.devices.items():
                if not d.selected or not d.online:
                    continue
                if d.running_packets > leader_n:
                    leader_n = d.running_packets
                    leader_ip = ip
                    leader_name = d.name
    from api import _state
    countdown_active = bool(_state().get("_countdown_active", False))
    return {
        "running": S.running,
        "timed_mode": S.timed_mode,
        "frame_mode": S.frame_mode,
        "saved_files": S.saved_files,
        "target_files": target_files,
        "target_frames": S.target_frames,
        "elapsed_sec": elapsed,
        "avg_actual_hz": avg_actual,
        "cfg_hz": cfg_hz,
        "sample_rate_enabled": bool(S.applied_sample_rate
                                    and S.applied_sample_rate > 0),
        "video_recording": S.video_recording,
        "video_enabled": S.video_enabled,
        "phone_online": S.phone_online,
        "countdown_active": countdown_active,
        "leader_ip": leader_ip,
        "leader_name": leader_name,
        "leader_packets": leader_n,
        "ts": time.time(),
    }


def _push_led_to_online(mode):
    """下发 LED_MODE 命令. mode: 'WINDOW'/'FREE'/'OFF'."""
    with S.devices_lock:
        for ip, d in S.devices.items():
            if d.online:
                try:
                    d.tcp_cmd_queue.put((f"LED_MODE {mode}", queue.Queue()))
                except Exception:
                    pass


def _push_sample_rate_now():
    """把当前采样频率 (含启用标志) 推到所有在线设备."""
    enabled = bool(S.applied_sample_rate and S.applied_sample_rate > 0)
    rate = max(1, int(round(S.sample_rate)))
    cmd = f"SAMPLE_RATE {rate if enabled else 0}"
    with S.devices_lock:
        for ip, d in S.devices.items():
            if d.online:
                try:
                    d.tcp_cmd_queue.put((cmd, queue.Queue()))
                except Exception:
                    pass


def _drain_csi_batch(ip, max_items=80):
    """从设备 CSI 队列取出最多 max_items 个包, 编码成可 JSON 序列化的波形数据.

    每帧只推送代表子载波 SC 8/24/40/56 的振幅 (dB), 大幅减少带宽:
      - 完整 CSI: 64 subcarriers × int8 ≈ 64 bytes/帧
      - 推送版:  4 subcarriers × float ≈ 32 bytes/帧 (减半)

    返回 {'ip': ip, 'frames': [{seq, rssi, t, amps: [4 floats]}, ...], 'buf': [N×4 floats]}
    其中 'buf' 是设备 amp_buf 全量快照, 用于前端重绘历史 + 当前叠加.
    """
    with S.devices_lock:
        d = S.devices.get(ip)
    if not d:
        return None
    frames = []
    with d.amp_buf_lock:
        n = 0
        while n < max_items:
            try:
                latest = d.csi_queue.get_nowait()
            except queue.Empty:
                break
            seq, rssi, csi_raw, csi_len, mac, wifi_ts = latest
            iq = np.frombuffer(csi_raw, dtype=np.int8)
            n_samples = len(iq) // 2
            if n_samples <= 0:
                continue
            i_vals = iq[0::2].astype(np.float32)[:n_samples]
            q_vals = iq[1::2].astype(np.float32)[:n_samples]
            amp = np.sqrt(i_vals * i_vals + q_vals * q_vals)
            if amp.size < 64:
                continue
            amp64 = amp[:64]
            amp_db = (20.0 * np.log10(amp64 + 1e-10)).astype(np.float32)
            d.amp_buf.append(amp_db.copy())
            if len(d.amp_buf) > 250:
                d.amp_buf.pop(0)
            frames.append({
                "seq": int(seq),
                "rssi": int(rssi),
                "amps": [float(amp_db[i]) for i in (8, 24, 40, 56)],
            })
            n += 1
        if d.amp_buf:
            buf = np.array(d.amp_buf, dtype=np.float32)
            buf = np.clip(buf, -50.0, 50.0)
            buf_full = buf[:, [8, 24, 40, 56]].tolist()
        else:
            buf_full = []
    if not frames and not buf_full:
        return None
    return {
        "ip": ip,
        "frames": frames,
        "buf": buf_full,
        "ts": time.time(),
    }


def _state():
    """代理 api._state(), 便于本地调用."""
    from api import _state
    return _state()