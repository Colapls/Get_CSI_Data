# -*- coding: utf-8 -*-
"""
api.py — REST API 路由注册

所有接口都按"动作"语义设计: POST 一条命令即执行一次, 返回 {'ok': bool, 'msg': str}.

接口分类:
  设备管理
      POST /api/probe                  触发局域网扫描
      POST /api/select                 切换设备选中状态 (ip + selected)
      POST /api/select_all             全选/全不选
      POST /api/position               修改设备位置
      POST /api/active                 设置当前要观察的设备
  采样配置
      POST /api/sample_rate            设置采样频率 (rate Hz)
      POST /api/sample_rate_toggle     启用/禁用主动 ping 取样
  语音 / 延时 (仅按窗口采集)
      POST /api/voice_toggle           启用/禁用语音提示
      POST /api/delay_toggle           启用/禁用延时采集
      POST /api/delay_seconds          设置延时秒数
      POST /api/voice_test             试听按钮 (TTS 测试)
  采集控制
      POST /api/start_window           按窗口采集 (含语音/延时流程)
      POST /api/start_frame            按帧数采集
      POST /api/start_free             自由采集
      POST /api/stop               停止采集 (含倒计时取消)
  数据集配置
      POST /api/config                 批量更新数据标签/保存路径/格式 等
      GET  /api/config                 读取当前数据集配置
      POST /api/phone_ip               修改 IP Webcam 手机 IP
      POST /api/video_toggle           切换视频采集开关
  视频
      POST /api/open_video_dir         打开视频保存目录
  系统
      GET  /api/status                 一次性拉全状态 (前端初始化用)
      POST /api/open_visualizer        拉起 csi_visualizer/start.bat

注意:
  - 所有控制操作都是"触发式", 不在 HTTP 线程里跑长任务.
    采集启动用 _pending_start 标志 + 异步视频线程, 与 gui.py 同套路.
  - 倒计时/语音流程也走异步线程, 完成后通过 socketio 推送 csi_start 事件.
  - 不在前端持久化任何状态: 全部状态由后端 _State (S) 持有.
"""
from __future__ import annotations

import json
import os
import queue
import socket
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime

# 复用 receiver 业务模块
from constants import (
    DEFAULT_LABEL, DEFAULT_SAMPLE_RATE, SAMPLE_RATE_MIN,
    SAMPLE_RATE_MAX, LABEL_NO_PERSON, LABEL_OPTIONS, PORT,
    ACK_MSG, BROADCAST_HELLO,
)
from models import S, _gui_event_queue
from socketio_bridge import set_active_device, _build_active_detail


# ============================================================
# 路径设置: 找项目根目录, 便于拉起 csi_visualizer
# ============================================================
def _find_project_root():
    """向上找 Get_CSI_Data_V2 项目根目录 (有 constants.py 的最近目录)."""
    cur = os.path.dirname(os.path.abspath(__file__))
    for _ in range(6):
        if os.path.isfile(os.path.join(cur, "constants.py")):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
    # 兜底: 用本文件父目录的父目录
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


_ROOT = _find_project_root()

# 默认数据保存根目录: <项目根目录的上级目录>\CSI_data
# 即 E:\ESP_Projects\CSI_data (与 Get_CSI_Data_V2 同级)
_DATA_ROOT = os.path.join(os.path.dirname(_ROOT), "CSI_data")


def _default_save_dir():
    r"""默认 CSI 保存路径: <DATA_ROOT>\csi_data_YYYYMMDD"""
    return os.path.join(_DATA_ROOT, f"csi_data_{datetime.now().strftime('%Y%m%d')}")


def _default_video_save_dir(save_dir=None):
    """默认视频保存路径: <CSI保存路径>\video_data (视频放在 CSI 目录内的子目录)."""
    return os.path.join(save_dir or _default_save_dir(), "video_data")


def register_api_routes(app, socketio):
    """把全部 REST 路由挂到 Flask app 上.

    把所有处理函数闭包到本函数内, 让它们共享 _pending_start 等
    本文件私有的状态变量 (避免污染 _State).
    """

    # ============================================================
    # 设备管理
    # ============================================================
    @app.post("/api/probe")
    def api_probe():
        """触发局域网搜索 (多轮广播 + 子网扫描, 不阻塞主线程)."""
        from udp_listener import _udp_listener  # noqa
        threading.Thread(target=_probe_worker_bg, args=(socketio,),
                         daemon=True).start()
        return {"ok": True, "msg": "搜索已启动"}

    @app.post("/api/select")
    def api_select():
        """切换设备选中状态. body: {ip, selected (bool)}"""
        body = request_json()
        ip = body.get("ip")
        selected = bool(body.get("selected"))
        if not ip:
            return {"ok": False, "msg": "缺少 ip"}
        with S.devices_lock:
            d = S.devices.get(ip)
            if d is None:
                return {"ok": False, "msg": f"未知设备 {ip}"}
            d.selected = selected
        return {"ok": True, "msg": f"{ip} selected={selected}"}

    @app.post("/api/position")
    def api_position():
        """修改设备位置. body: {ip, position}"""
        body = request_json()
        ip = body.get("ip")
        pos = body.get("position", "").strip()
        if not ip or not pos:
            return {"ok": False, "msg": "缺少 ip 或 position"}
        with S.devices_lock:
            d = S.devices.get(ip)
            if d is None:
                return {"ok": False, "msg": f"未知设备 {ip}"}
            d.position = pos
        return {"ok": True, "msg": f"{ip} position={pos}"}

    @app.post("/api/active")
    def api_active():
        """设置当前要观察 (绘波形) 的设备. body: {ip}.
        切换后立即推送一次 active_device_detail, 不等下个 250ms 快照周期,
        让底部状态栏在用户点击后 < 50ms 内就有视觉反馈."""
        body = request_json()
        ip = body.get("ip") or None
        set_active_device(ip)
        with S.devices_lock:
            S.active_device = ip
        # 立即推详情 (绕开 250ms 延迟)
        if ip:
            detail = _build_active_detail(ip)
            if detail:
                socketio.emit("active_device_detail", detail)
        return {"ok": True, "msg": f"active={ip}"}

    @app.post("/api/select_all")
    def api_select_all():
        """全选/全不选. body: {selected: bool}"""
        body = request_json()
        flag = bool(body.get("selected", True))
        with S.devices_lock:
            for d in S.devices.values():
                d.selected = flag
        return {"ok": True, "msg": f"all selected={flag}"}

    # ============================================================
    # 采样配置
    # ============================================================
    @app.post("/api/sample_rate")
    def api_sample_rate():
        """设置采样频率 Hz. body: {rate}"""
        body = request_json()
        try:
            rate = int(round(float(body.get("rate", DEFAULT_SAMPLE_RATE))))
        except Exception:
            return {"ok": False, "msg": "rate 格式错误"}
        rate = max(SAMPLE_RATE_MIN, min(SAMPLE_RATE_MAX, rate))
        S.sample_rate = rate
        # 如果"主动 ping 取样"已启用, 把新频率下发到所有在线设备
        if _state()["config"].get("sample_rate_enabled", False):
            S.applied_sample_rate = rate
            pushed = _push_cmd_to_online(f"SAMPLE_RATE {rate}")
            return {"ok": True, "msg": f"已下发 {rate}Hz → {pushed} 台"}
        return {"ok": True, "msg": f"已设置 {rate}Hz (未启用主动 ping, 未下发)"}

    @app.post("/api/sample_rate_toggle")
    def api_sample_rate_toggle():
        """启用/禁用主动 ping. body: {enabled: bool}"""
        body = request_json()
        enabled = bool(body.get("enabled", False))
        _state()["config"]["sample_rate_enabled"] = enabled
        if not enabled:
            S.applied_sample_rate = 0
            pushed = _push_cmd_to_online("SAMPLE_RATE 0")
            return {"ok": True, "msg": f"停止 ping → {pushed} 台"}
        else:
            rate = int(round(S.sample_rate))
            S.applied_sample_rate = rate
            pushed = _push_cmd_to_online(f"SAMPLE_RATE {rate}")
            return {"ok": True, "msg": f"已下发 {rate}Hz → {pushed} 台"}

    @app.post("/api/mode")
    def api_mode():
        """切换采集模式 (多发多收 / 一发多收). body: {mode, sender_ip (可选)}
        - mode = "multi"   : 多发多收, 每台 ESP 独立 ping 采样.
        - mode = "single"  : 一发多收, 需指定 sender_ip (或自动选第一台在线设备).
                             下发到所有设备: 发送端 -> SENDER, 其他 -> RECEIVER.
        返回: {ok, msg, mode, sender_ip, sender_mac, pushed}
        """
        body = request_json()
        mode = str(body.get("mode", "multi")).lower().strip()
        cfg = _state()["config"]
        if mode not in ("multi", "single"):
            return {"ok": False, "msg": "mode 必须是 'multi' 或 'single'"}
        sender_ip = (body.get("sender_ip") or cfg.get("sender_ip") or "").strip()

        # 取所有在线设备 (按 IP 排序, 与设备列表显示顺序一致)
        with S.devices_lock:
            online_devs = [d for d in S.devices.values() if d.online]
        online_devs.sort(key=lambda d: d.ip)
        if not online_devs:
            return {"ok": False, "msg": "无在线设备, 请先点 '搜索' 发现设备"}

        sender_mac = ""
        if mode == "single":
            # 找 sender: 优先 sender_ip 对应的设备; 没有则取列表第一个
            sender_dev = None
            if sender_ip:
                for d in online_devs:
                    if d.ip == sender_ip:
                        sender_dev = d
                        break
                if sender_dev is None:
                    return {"ok": False, "msg": f"指定的发送端 {sender_ip} 不在线"}
            else:
                sender_dev = online_devs[0]
            sender_ip = sender_dev.ip
            sender_mac = (getattr(sender_dev, "mac", "") or "").strip()
            if not sender_mac or sender_mac == "等待连接...":
                # 设备刚被发现, 还没收到 CSI 包拿到真实 MAC, 让前端稍候再试
                return {"ok": False, "msg": f"设备 {sender_ip} 尚未上报 MAC, 请稍候再试"}

        # 推送到所有设备, 等待每台固件确认. 任何一台没成功确认都视为失败,
        # 前端按 ok=false 处理, 避免出现"以为切了实际没切"的鬼切换.
        if mode == "multi":
            ok, total, errors = _push_cmd_and_wait("MODE MULTI", timeout=6.0)
            cfg["csi_mode"] = "multi"
            cfg["sender_ip"] = ""
            if total == 0:
                return {"ok": False, "msg": "无可推送的在线设备",
                        "mode": "multi", "sender_ip": "", "sender_mac": ""}
            if ok == total:
                return {"ok": True,
                        "msg": f"已切换为多发多收模式 ({ok}/{total} 台已确认)",
                        "mode": "multi", "sender_ip": "", "sender_mac": "",
                        "ok_count": ok, "total_count": total,
                        "errors": []}
            else:
                err_str = "; ".join(f"{ip}: {e}" for ip, e in errors)
                return {"ok": False,
                        "msg": f"部分设备切换失败 ({ok}/{total}): {err_str}",
                        "mode": "multi", "sender_ip": "", "sender_mac": "",
                        "ok_count": ok, "total_count": total,
                        "errors": errors}
        else:
            # single: 推送到所有设备, 各自根据自身 MAC 与 sender_mac 比较
            # 决定自己是 SENDER 还是 RECEIVER
            ok = 0
            total = 0
            errors = []
            for d in online_devs:
                cmd = f"MODE SINGLE {sender_mac}"
                try:
                    q: queue.Queue = queue.Queue()
                    d.tcp_cmd_queue.put((cmd, q))
                    total += 1
                    line = q.get(timeout=6.0)
                except queue.Empty:
                    errors.append((d.ip, "timeout"))
                    continue
                except Exception:
                    errors.append((d.ip, "push_fail"))
                    continue
                if '"type":"ok"' in line:
                    ok += 1
                else:
                    errors.append((d.ip, line))

            cfg["csi_mode"] = "single"
            cfg["sender_ip"] = sender_ip
            if total == 0:
                return {"ok": False, "msg": "无可推送的在线设备",
                        "mode": "single", "sender_ip": sender_ip,
                        "sender_mac": sender_mac}
            if ok == total:
                return {"ok": True,
                        "msg": f"已切换为一发多收模式 ({ok}/{total} 台已确认, 发送端 {sender_ip})",
                        "mode": "single", "sender_ip": sender_ip,
                        "sender_mac": sender_mac,
                        "ok_count": ok, "total_count": total,
                        "errors": []}
            else:
                err_str = "; ".join(f"{ip}: {e}" for ip, e in errors)
                return {"ok": False,
                        "msg": f"部分设备切换失败 ({ok}/{total}): {err_str}",
                        "mode": "single", "sender_ip": sender_ip,
                        "sender_mac": sender_mac,
                        "ok_count": ok, "total_count": total,
                        "errors": errors}

    # ============================================================
    # 语音 / 延时 (仅按窗口采集生效)
    # ============================================================
    @app.post("/api/voice_toggle")
    def api_voice_toggle():
        """启用/禁用语音提示. body: {enabled}"""
        body = request_json()
        enabled = bool(body.get("enabled", False))
        _state()["config"]["voice_enabled"] = enabled
        if enabled:
            # 顺便预热 TTS 一次 (PowerShell 首次启动开销大)
            try:
                threading.Thread(target=_tts_speak_async,
                                 args=("语音提示已启用",), daemon=True).start()
            except Exception:
                pass
        return {"ok": True, "msg": f"voice_enabled={enabled}"}

    @app.post("/api/delay_toggle")
    def api_delay_toggle():
        """启用/禁用延时采集. body: {enabled}"""
        body = request_json()
        enabled = bool(body.get("enabled", False))
        _state()["config"]["delay_enabled"] = enabled
        return {"ok": True, "msg": f"delay_enabled={enabled}"}

    @app.post("/api/delay_seconds")
    def api_delay_seconds():
        """设置延时秒数 (1-60). body: {sec}"""
        body = request_json()
        try:
            sec = int(body.get("sec", 3))
        except Exception:
            return {"ok": False, "msg": "参数错误"}
        sec = max(1, min(60, sec))
        _state()["config"]["delay_seconds"] = sec
        return {"ok": True, "msg": f"delay_seconds={sec}"}

    @app.post("/api/voice_test")
    def api_voice_test():
        """试听按钮: 不开采集, 试一下语音通道."""
        cfg = _state()["config"]
        # 同步播两句话 (避免重叠), 在后台线程跑
        threading.Thread(target=_tts_speak_test, daemon=True).start()
        return {"ok": True, "msg": "正在试听..."}

    # ============================================================
    # 采集控制
    # ============================================================
    @app.post("/api/start_window")
    def api_start_window():
        """按窗口采集 (含语音/延时). body: {win_sec, target_files}"""
        body = request_json()
        try:
            win_sec = int(body.get("win_sec", 5))
            target_files = int(body.get("target_files", 1))
        except Exception:
            return {"ok": False, "msg": "参数错误"}
        if win_sec < 1 or target_files < 1:
            return {"ok": False, "msg": "参数必须 >=1"}
        selected = [d for d in S.devices.values() if d.selected and d.online]
        if not selected:
            return {"ok": False, "msg": "请先勾选至少一台在线设备"}
        if S.running or _state().get("_countdown_active"):
            return {"ok": False, "msg": "已有采集/倒计时进行中"}

        cfg = _state()["config"]
        voice_on = bool(cfg.get("voice_enabled", False))
        delay_on = bool(cfg.get("delay_enabled", False))
        try:
            delay_sec = max(1, int(cfg.get("delay_seconds", 3)))
        except Exception:
            delay_sec = 3

        # 视频预检: 不可用但用户仍可继续 (只采 CSI 不录视频)
        if cfg.get("video_enabled") and not cfg.get("phone_ip"):
            return {"ok": False, "msg": "已勾选'同时采集视频', 请填写手机IP"}

        # 保存启动参数, 待倒计时/语音结束后真正启动
        _state()["_pending_mode"] = "window"
        _state()["_pending_win_sec"] = win_sec
        _state()["_pending_target_frames"] = 0
        _state()["_pending_target_files"] = target_files
        _state()["_voice_session_active"] = voice_on  # 启动时的语音开关
        cfg["win_len"] = win_sec
        cfg["win_files"] = target_files

        if voice_on or delay_on:
            # 进入倒计时/语音阶段
            _state()["_countdown_active"] = True
            _state()["_voice_countdown_cancelled"] = False
            threading.Thread(
                target=_voice_countdown_then_start,
                args=(socketio, "window", voice_on, delay_on, delay_sec),
                daemon=True,
            ).start()
            return {"ok": True, "msg": f"按窗口采集中 · {len(selected)} 台 × {win_sec}s × {target_files}文件 (等待语音/倒计时)"}
        else:
            # 没有任何语音/延时, 直接开始
            _begin_collection(socketio, mode="window", win_sec=win_sec,
                              target_frames=0, target_files=target_files)
            return {"ok": True, "msg": f"按窗口采集中 · {len(selected)} 台 × {win_sec}s × {target_files}文件"}

    @app.post("/api/start_frame")
    def api_start_frame():
        """按帧数采集 (支持语音/延时). body: {target_frames, target_files}"""
        body = request_json()
        try:
            target_frames = int(body.get("target_frames", 500))
            target_files = int(body.get("target_files", 1))
        except Exception:
            return {"ok": False, "msg": "参数错误"}
        if target_frames < 10 or target_files < 1:
            return {"ok": False, "msg": "帧数 >=10, 文件数 >=1"}
        selected = [d for d in S.devices.values() if d.selected and d.online]
        if not selected:
            return {"ok": False, "msg": "请先勾选至少一台在线设备"}
        if S.running or _state().get("_countdown_active"):
            return {"ok": False, "msg": "已有采集/倒计时进行中"}

        cfg = _state()["config"]
        if cfg.get("video_enabled") and not cfg.get("phone_ip"):
            return {"ok": False, "msg": "已勾选'同时采集视频', 请填写手机IP"}

        voice_on = bool(cfg.get("voice_enabled", False))
        delay_on = bool(cfg.get("delay_enabled", False))
        try:
            delay_sec = max(1, int(cfg.get("delay_seconds", 3)))
        except Exception:
            delay_sec = 3

        cfg["target_frames"] = target_frames
        cfg["target_files"] = target_files

        # 保存启动参数, 待倒计时/语音结束后真正启动
        _state()["_pending_mode"] = "frame"
        _state()["_pending_win_sec"] = 0
        _state()["_pending_target_frames"] = target_frames
        _state()["_pending_target_files"] = target_files

        if voice_on or delay_on:
            _state()["_countdown_active"] = True
            _state()["_voice_countdown_cancelled"] = False
            threading.Thread(
                target=_voice_countdown_then_start,
                args=(socketio, "frame", voice_on, delay_on, delay_sec),
                daemon=True,
            ).start()
            return {"ok": True, "msg": f"按帧采集中 · {len(selected)} 台 × {target_frames}帧 × {target_files}文件 (等待语音/倒计时)"}
        else:
            _begin_collection(socketio, mode="frame", win_sec=0,
                              target_frames=target_frames,
                              target_files=target_files)
            return {"ok": True, "msg": f"按帧采集中 · {len(selected)} 台 × {target_frames}帧 × {target_files}文件"}

    @app.post("/api/start_free")
    def api_start_free():
        """自由采集 (支持语音/延时)."""
        selected = [d for d in S.devices.values() if d.selected and d.online]
        if not selected:
            return {"ok": False, "msg": "请先勾选至少一台在线设备"}
        if S.running or _state().get("_countdown_active"):
            return {"ok": False, "msg": "已有采集/倒计时进行中"}
        cfg = _state()["config"]
        if cfg.get("video_enabled") and not cfg.get("phone_ip"):
            return {"ok": False, "msg": "已勾选'同时采集视频', 请填写手机IP"}

        voice_on = bool(cfg.get("voice_enabled", False))
        delay_on = bool(cfg.get("delay_enabled", False))
        try:
            delay_sec = max(1, int(cfg.get("delay_seconds", 3)))
        except Exception:
            delay_sec = 3

        # 保存启动参数, 待倒计时/语音结束后真正启动
        _state()["_pending_mode"] = "free"
        _state()["_pending_win_sec"] = 0
        _state()["_pending_target_frames"] = 0
        _state()["_pending_target_files"] = 1

        if voice_on or delay_on:
            _state()["_countdown_active"] = True
            _state()["_voice_countdown_cancelled"] = False
            threading.Thread(
                target=_voice_countdown_then_start,
                args=(socketio, "free", voice_on, delay_on, delay_sec),
                daemon=True,
            ).start()
            return {"ok": True, "msg": f"自由采集中 · {len(selected)} 台 (等待语音/倒计时)"}
        else:
            _begin_collection(socketio, mode="free", win_sec=0,
                              target_frames=0, target_files=1)
            return {"ok": True, "msg": f"自由采集中 · {len(selected)} 台"}

    @app.post("/api/stop")
    def api_stop():
        """停止采集 (含倒计时取消)."""
        # 如果正处于语音倒计时阶段, 取消倒计时
        if not S.running and _state().get("_countdown_active"):
            _state()["_voice_countdown_cancelled"] = True
            _state()["_countdown_active"] = False
            try:
                socketio.emit("message", {"level": "gray",
                                          "text": "已取消 (倒计时终止)"})
            except Exception:
                pass
            return {"ok": True, "msg": "倒计时已取消"}

        if not S.running and not _state().get("_pending_start"):
            return {"ok": True, "msg": "未在采集中"}

        # 如果在等待视频就绪的 pending 状态, 也要取消
        if _state().get("_pending_start") and not S.running:
            _state()["_pending_start"] = False
            if S.video_recording:
                try:
                    from video import pc_stop_record
                    pc_stop_record()
                except Exception:
                    pass
                S.video_recording = False
            try:
                socketio.emit("message", {"level": "gray", "text": "已取消"})
            except Exception:
                pass
            return {"ok": True, "msg": "已取消"}

        _state()["_pending_start"] = False
        _do_stop(socketio)
        return {"ok": True, "msg": "已停止"}

    # ============================================================
    # 数据集配置
    # ============================================================
    @app.post("/api/config")
    def api_config():
        """批量更新数据标签 / 保存路径 / 格式 / 路由器位置 等.
        body: {router_loc, save_dir, save_format, data_label,
               person_name, behavior_loc, win_len, win_files,
               target_frames, target_files, sample_rate,
               voice_enabled, delay_enabled, delay_seconds,
               sample_rate_enabled, phone_ip, video_save_dir, video_enabled}
        只更新 body 中给出的字段.
        """
        body = request_json()
        mapping = {
            "router_loc":    ("router_loc",    str),
            "save_dir":      ("save_dir",      str),
            "save_format":   ("save_format",   str),
            "data_label":    ("data_label",    str),
            "person_name":   ("person_name",   str),
            "behavior_loc":  ("behavior_loc",  str),
            "win_len":       ("win_len",       int),
            "win_files":     ("win_files",     int),
            "target_frames": ("target_frames", int),
            "target_files":  ("target_files",  int),
            "sample_rate":   ("sample_rate",   float),
            "voice_enabled": ("voice_enabled", bool),
            "delay_enabled": ("delay_enabled", bool),
            "delay_seconds": ("delay_seconds", int),
            "sample_rate_enabled": ("sample_rate_enabled", bool),
            "phone_ip":      ("phone_ip",      str),
            "video_save_dir": ("video_save_dir", str),
            "video_enabled": ("video_enabled", bool),
            # 单发多收 / 多发多收 模式
            "csi_mode":      ("csi_mode",      str),
            "sender_ip":     ("sender_ip",     str),
            # V6.1: 人员设置 (列表: [{name, pos, sel}]) + 环境设置 (dict + list)
            "persons":       ("persons",       list),
            "env_values":    ("env_values",    dict),
            "env_custom":    ("env_custom",    list),
        }
        cfg = _state()["config"]
        applied = []
        for k, (target, typ) in mapping.items():
            if k in body:
                try:
                    cfg[target] = typ(body[k])
                    applied.append(target)
                except Exception as e:
                    return {"ok": False, "msg": f"{k}: {e}"}

        # 联动: 当标签 == '无人' 时, 自动清空 person_name
        if "data_label" in body and cfg.get("data_label") == LABEL_NO_PERSON:
            cfg["person_name"] = ""
        # 同步 S 中的 phone_ip (供心跳线程用)
        if "phone_ip" in cfg:
            S.phone_ip = cfg["phone_ip"]
        return {"ok": True, "msg": f"已更新 {len(applied)} 项", "applied": applied}

    @app.get("/api/config")
    def api_config_get():
        """读取当前数据集配置."""
        return _state()["config"]

    # ============================================================
    # 视频
    # ============================================================
    @app.post("/api/phone_ip")
    def api_phone_ip():
        body = request_json()
        ip = body.get("ip", "").strip()
        S.phone_ip = ip
        _state()["config"]["phone_ip"] = ip
        return {"ok": True, "msg": f"phone_ip={ip}"}

    @app.post("/api/video_toggle")
    def api_video_toggle():
        body = request_json()
        enabled = bool(body.get("enabled", False))
        S.video_enabled = enabled
        _state()["config"]["video_enabled"] = enabled
        return {"ok": True, "msg": f"video_enabled={enabled}"}

    @app.post("/api/open_video_dir")
    def api_open_video_dir():
        target = (_state()["config"]["video_save_dir"].strip() or
                  _default_video_save_dir())
        try:
            os.makedirs(target, exist_ok=True)
        except Exception:
            pass
        try:
            if sys.platform.startswith("win"):
                os.startfile(target)
            elif sys.platform == "darwin":
                os.system(f'open "{target}"')
            else:
                os.system(f'xdg-open "{target}"')
            return {"ok": True, "msg": f"已打开 {target}"}
        except Exception as e:
            return {"ok": False, "msg": str(e)}

    # ============================================================
    # 系统
    # ============================================================
    @app.get("/api/status")
    def api_status():
        """一次性拉全状态 (前端初始化用)."""
        cfg = _state()["config"]
        with S.devices_lock:
            devices = [{
                "ip": d.ip, "name": d.name, "selected": d.selected,
                "online": d.online, "rssi": d.rssi, "position": d.position,
            } for d in S.devices.values()]
        return {
            "config": cfg,
            "devices": devices,
            "running": S.running,
            "sample_rate": S.sample_rate,
            "video_enabled": S.video_enabled,
            "video_recording": S.video_recording,
            "phone_online": S.phone_online,
            "countdown_active": bool(_state().get("_countdown_active", False)),
            "csi_mode":  cfg.get("csi_mode", "multi"),
            "sender_ip": cfg.get("sender_ip", ""),
        }

    @app.get("/api/diag/<ip>")
    def api_diag(ip):
        """返回某台设备的本地诊断状态 (last_seen / total_packets / rssi / active /
        applied_rate), 帮助排查"设备在线但看不到 CSI / RSSI 不更新"类问题.

        典型诊断:
          total_packets 持续增长但 is_active=False → CSI 收到了, 用户没点这行
          total_packets=0 + applied_rate>0 → 固件没在发包, 重发 SAMPLE_RATE / MODE
          tcp_status=已连接 + applied_rate=0 → 用户之前关了主动 ping
        """
        with S.devices_lock:
            d = S.devices.get(ip)
            if d is None:
                return {"ok": False, "msg": f"未知设备 {ip}"}
            now = time.time()
            snapshot = {
                "ip": d.ip,
                "name": d.name,
                "online": d.online,
                "rssi": d.rssi,
                "csi_len": d.csi_len,
                "seq": d.seq,
                "total_packets": d.total_packets,
                "running_packets": d.running_packets,
                "last_seen_ago": round(now - d.last_seen, 2) if d.last_seen else None,
                "last_hello_ago": round(now - d.last_hello, 2) if d.last_hello else None,
                "tcp_status": d.tcp_status,
                "esp_state": d.esp_state,
                "is_active": (S.active_device == ip),
                "applied_rate": S.applied_sample_rate,
                "actual_rate": round(d.actual_rate, 1) if d.actual_rate > 0 else 0.0,
                "esp_status": getattr(d, "esp_status", None),
            }
        return {"ok": True, **snapshot}

    @app.post("/api/open_visualizer")
    def api_open_visualizer():
        """拉起 csi_visualizer/start.bat."""
        bat = os.path.join(_ROOT, "csi_visualizer", "start.bat")
        if not os.path.isfile(bat):
            return {"ok": False, "msg": f"未找到 {bat}"}
        try:
            if sys.platform.startswith("win"):
                CREATE_NEW_PROCESS_GROUP = 0x00000200
                DETACHED_PROCESS        = 0x00000008
                subprocess.Popen(
                    ["cmd", "/c", "start", "", bat],
                    creationflags=CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS,
                    close_fds=True,
                )
            else:
                subprocess.Popen(["xdg-open", bat], close_fds=True)
            return {"ok": True, "msg": "已启动可视化"}
        except Exception as e:
            return {"ok": False, "msg": str(e)}


# ============================================================
# 内部工具函数
# ============================================================
def request_json():
    """从 Flask request 取出 JSON, 失败返回空 dict。"""
    from flask import request
    try:
        return request.get_json(force=True, silent=True) or {}
    except Exception:
        return {}


_state_obj = {}


def _state():
    """获取本模块的单例状态容器 (避免和 S 全局冲突)."""
    if "_init" not in _state_obj:
        _state_obj["_init"] = True
        _save_dir = _default_save_dir()
        _state_obj["config"] = {
            "router_loc":     "地上",
            "save_dir":       _save_dir,
            "video_save_dir": _default_video_save_dir(_save_dir),
            "save_format":    "npz",
            "data_label":     DEFAULT_LABEL,
            "person_name":    "",       # 兼容旧字段 (人员设置面板勾选后回填)
            "behavior_loc":   "",       # 兼容旧字段 (人员设置面板勾选后回填)
            "win_len":        5,
            "win_files":      1,
            "target_frames":  500,
            "target_files":   1,
            "sample_rate":    float(DEFAULT_SAMPLE_RATE),
            # 语音 / 延时 (仅按窗口采集生效)
            "voice_enabled":  False,
            "delay_enabled":  False,
            "delay_seconds":  3,
            "sample_rate_enabled": False,
            "phone_ip":       "",
            "video_enabled":  False,
            # 单发多收 / 多发多收 模式 (默认多发多收, 兼容现有固件)
            "csi_mode":       "multi",
            "sender_ip":      "",
            # V6.1: 人员设置 + 环境设置 (前端持久化 + 后端镜像)
            "persons":        [],      # [{name, pos, sel}, ...]
            "env_values":     {"天气": "晴", "风": "微风", "门": "关", "窗户": "关", "空调": "关"},
            "env_custom":     [],      # [{name, value}, ...]
        }
        _state_obj["_pending_start"] = False
        _state_obj["_pending_mode"] = ""
        _state_obj["_skip_final_save"] = False
        _state_obj["_countdown_active"] = False
        _state_obj["_voice_countdown_cancelled"] = False
        _state_obj["_voice_session_active"] = False
        _state_obj["_pending_win_sec"] = 0
        _state_obj["_pending_target_files"] = 1
    return _state_obj


def _push_cmd_to_online(cmd):
    """把命令 push 到所有在线设备的 tcp_cmd_queue. 返回成功推送的设备数."""
    n = 0
    with S.devices_lock:
        for ip, d in S.devices.items():
            if d.online:
                try:
                    d.tcp_cmd_queue.put((cmd, queue.Queue()))
                    n += 1
                except Exception:
                    pass
    return n


def _push_cmd_and_wait(cmd, timeout=6.0):
    """把命令 push 到所有在线设备, 等每台都返回 type=ok / type=error 应答.

    返回:
      ok_count:    收到 {"type":"ok",...} 的设备数
      total_count: 成功 push 出去的设备数 (可能 > 在线数, 因为 push 时不剔除离线)
      errors:      [(ip, err_line_or_msg), ...] 失败的设备
    注: TCP 监听线程 (receiver_v2/udp_listener.py) 收到 ESP 应答后会写进
    resp_q, 我们从 resp_q.get(timeout) 阻塞读取.
    """
    resp_queues = []
    with S.devices_lock:
        for ip, d in S.devices.items():
            if d.online:
                try:
                    q: queue.Queue = queue.Queue()
                    d.tcp_cmd_queue.put((cmd, q))
                    resp_queues.append((ip, q))
                except Exception:
                    pass

    ok_count = 0
    errors = []
    for ip, q in resp_queues:
        try:
            line = q.get(timeout=timeout)
        except queue.Empty:
            errors.append((ip, "timeout"))
            continue
        if '"type":"ok"' in line:
            ok_count += 1
        else:
            errors.append((ip, line))

    return ok_count, len(resp_queues), errors


def _push_led_to_online(mode):
    """下发 LED_MODE 命令. mode: 'WINDOW'/'FREE'/'OFF'."""
    _push_cmd_to_online(f"LED_MODE {mode}")


# ============================================================
# TTS 异步播报 (Windows)
# ============================================================
def _tts_speak_async(text: str):
    """异步播报: 启动 PowerShell 调 Microsoft SpeechSynthesizer 播一次。"""
    if not text:
        return
    if not sys.platform.startswith("win"):
        print(f"[TTS] {text}")
        return
    safe = text.replace('"', '""')
    script = (
        "Add-Type -AssemblyName System.Speech; "
        "$s = New-Object System.Speech.Synthesis.SpeechSynthesizer; "
        "$v = $s.GetInstalledVoices() | "
        "  Where-Object { $_.VoiceInfo.Culture.Name -like 'zh-CN*' } | "
        "  Select-Object -First 1; "
        "if ($v) { $s.SelectVoice($v.VoiceInfo.Name) }; "
        f"$s.Speak(\"{safe}\")"
    )
    try:
        CREATE_NO_WINDOW = 0x08000000
        proc = subprocess.Popen(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            stdin=subprocess.DEVNULL,
            creationflags=CREATE_NO_WINDOW,
        )
        proc.wait(timeout=15)
    except Exception as e:
        print(f"[TTS] 失败: {e}")


def _tts_speak_test():
    """试听: 串行播两句话 (不并发)."""
    _tts_speak_async("按窗口采集开始")
    _tts_speak_async("采集结束")


# ============================================================
# 倒计时 + 语音流程 (后台线程)
# ============================================================
def _voice_countdown_then_start(socketio, mode, voice_on, delay_on, delay_sec):
    """后台线程: 延时倒计时 (若勾选) → 语音提示"XX 采集开始" (若勾选)
    → 全部播完调 _begin_collection() 进入正常采集流程。

    支持 3 种模式: 'window' / 'frame' / 'free'
    每种模式的语音播报文本不同:
      - window: "按窗口采集开始"
      - frame:  "按帧数采集开始"
      - free:   "自由采集开始"

    设计要点:
      - 全程在 daemon 线程跑, 不阻塞 HTTP / SocketIO;
      - 每秒一次 beep 严格 1Hz 节奏, 状态栏用 socketio.emit 同步显示倒计时;
      - 用户点"停止采集"时 _voice_countdown_cancelled=True 立即响应.
    """
    # 不同模式对应的语音播报文本
    voice_text_map = {
        "window": "按窗口采集开始",
        "frame":  "按帧数采集开始",
        "free":   "自由采集开始",
    }
    voice_text = voice_text_map.get(mode, "采集开始")

    try:
        def _cancelled() -> bool:
            return bool(_state().get("_voice_countdown_cancelled"))

        # 1) 延时倒计时: 每秒一次 beep
        if delay_on:
            start_time = time.time()
            for i in range(delay_sec, 0, -1):
                if _cancelled():
                    return
                target = start_time + (delay_sec - i + 1)
                while time.time() < target:
                    if _cancelled():
                        return
                    time.sleep(0.05)
                _beep(str(i))
                # 推送倒计时状态到前端
                try:
                    socketio.emit("countdown_update", {"remaining": i})
                    socketio.emit("message", {
                        "level": "orange",
                        "text": f"⏳ 倒计时 {i} 秒 · 等待开始采集..."})
                except Exception:
                    pass

        if _cancelled():
            return

        # 2) 语音提示: "XX 采集开始"
        if voice_on:
            _tts_speak_async(voice_text)
            if _cancelled():
                return
            # 等播完, 防止语音和视频启动撞车
            time.sleep(0.5)

        # 3) 真正启动采集
        try:
            socketio.emit("countdown_update", {"remaining": 0})
        except Exception:
            pass
        win_sec = _state().get("_pending_win_sec", 5)
        target_frames = _state().get("_pending_target_frames", 0)
        target_files = _state().get("_pending_target_files", 1)
        _state()["_countdown_active"] = False
        if _state().get("_voice_countdown_cancelled"):
            _state()["_voice_countdown_cancelled"] = False
            try:
                socketio.emit("message", {
                    "level": "gray", "text": "已取消 (倒计时终止)"})
            except Exception:
                pass
            return
        _begin_collection(socketio, mode=mode,
                          win_sec=win_sec, target_frames=target_frames,
                          target_files=target_files)
    except Exception as e:
        _state()["_countdown_active"] = False
        print(f"[{mode}-voice] 后台流程出错: {e}")


def _beep(tag: str = "") -> None:
    """短促提示音 (~120ms)."""
    if not sys.platform.startswith("win"):
        print(f"[BEEP] {tag}")
        return
    try:
        import winsound
        winsound.Beep(880, 120)
    except Exception as e:
        print(f"[beep] 失败: {e}")


# ============================================================
# 采集启停 (与 gui.py 同款)
# ============================================================
def _begin_collection(socketio, mode, win_sec, target_frames, target_files):
    """通用启动入口: 设置 _pending_start + 异步线程处理.

    mode: 'window' / 'frame' / 'free'
    """
    cfg = _state()["config"]
    S.win_target_files = target_files if mode == "window" else 1
    S.saved_files      = 0
    S.win_session_dirs = {}
    S.target_frames    = target_frames
    S.target_files     = target_files
    S.frame_session_dirs = {}
    S.frame_target_reached = ""
    _state()["_pending_start"] = True
    _state()["_pending_mode"] = mode
    _state()["_skip_final_save"] = False

    led = "WINDOW" if mode in ("window", "frame") else "FREE"
    with S.devices_lock:
        online_ips = [ip for ip, d in S.devices.items() if d.online]
    _push_led_to_online(led)
    _push_cmd_to_online(
        f"SAMPLE_RATE {int(round(cfg['sample_rate']))}")
    threading.Thread(target=_start_video_then_csi,
                     args=(socketio,), daemon=True).start()


def _start_video_then_csi(socketio):
    """异步: 启动视频 (如需) → 通过 _gui_event_queue 通知主线程开始 CSI 累积."""
    cfg = _state()["config"]
    if cfg.get("video_enabled") and cfg.get("phone_ip"):
        try:
            from video import pc_start_record
            save_dir = cfg["video_save_dir"].strip() or _default_video_save_dir()
            ok, fpath, err = pc_start_record(cfg["phone_ip"], save_dir)
            S.video_start_ts = time.time()
            if ok:
                S.video_recording = True
                _gui_event_queue.put_nowait(("csi_start", fpath))
            else:
                _gui_event_queue.put_nowait(("video_fail", err))
            return
        except Exception as e:
            _gui_event_queue.put_nowait(("video_fail", str(e)))
            return
    # 没开视频, 直接开始
    _gui_event_queue.put_nowait(("csi_start", None))


def _do_stop(socketio, skip_save=False):
    """执行停止: 与 gui.py._on_stop 等价的精简版。

    - skip_save=True  看门狗已自动保存完最后一个文件, 这里不再重复保存;
    - skip_save=False 手动停止, 把当前累积的 CSI 保存为最后一个文件。

    保存完成后统一清空 timed_mode / frame_mode / saved_files 等运行态字段;
    这些字段绝不能放在 _save_all_then_reset 里清空, 否则多文件采集会提前终止.
    """
    if not S.running:
        return
    was_timed_mode = S.timed_mode
    voice_was_active = bool(_state().get("_voice_session_active", False))
    S.running = False
    S.timer_running = False
    S.start_time = None
    with S.devices_lock:
        online_ips = [ip for ip, d in S.devices.items() if d.online]
    _push_cmd_to_online("LED_MODE OFF")
    # 停视频 (如有)
    _video_was_active = False
    if S.video_recording:
        _video_was_active = True
        try:
            from video import pc_stop_record, _write_video_sync_log
            ok, msg = pc_stop_record()
            S.video_recording = False
            S.video_stop_ts = time.time()
            try:
                sdir = _state()["config"]["save_dir"].strip() or _default_save_dir()
                os.makedirs(sdir, exist_ok=True)
                tag = datetime.fromtimestamp(S.video_start_ts).strftime("%Y%m%d_%H%M%S")
                S.video_sync_log = os.path.join(sdir, f"sync_{tag}.log")
                _write_video_sync_log(tag)
            except Exception:
                pass
        except Exception as e:
            print(f"[stop] video stop err: {e}")
    # 把本次是否有视频的标志透传给 _save_all_then_reset
    _state()["_video_was_active"] = _video_was_active
    # 同步保存 (不能开线程: 否则 saved_files / all_csi_data 与看门狗竞争)
    if not skip_save:
        _save_all_then_reset(socketio)
    # 保存完成后统一清空运行模式字段 (必须在 save 之后, 否则 save 拿不到
    # 正确的 mode token / 子目录 / target_frames / target_files)
    S.timed_mode = False
    S.frame_mode = False
    S.saved_files = 0
    S.win_target_files = 1
    S.win_session_dirs = {}
    S.frame_session_dirs = {}
    S.frame_target_reached = ""
    S.target_frames = 0
    S.target_files = 1
    # 语音提示"采集结束": 仅在按窗口采集且本会话启用了语音时, 且所有文件
    # 保存完成 (或停止) 之后才播报一次 (不能在每存一个文件时播).
    if was_timed_mode and voice_was_active:
        _state()["_voice_session_active"] = False
        threading.Thread(target=_tts_speak_async,
                         args=("采集结束",), daemon=True).start()


def _save_all_then_reset(socketio):
    """保存所有选中设备的所有 CSI 数据 → 清缓冲 → 推消息.

    注意: 本函数会清空 dev.all_csi_data, 但绝不触碰 saved_files /
    win_target_files / timed_mode / frame_mode 等会话计数 (这些由 _do_stop 清).
    """
    try:
        cfg = _state()["config"]
        sdir = cfg["save_dir"].strip() or _default_save_dir()
        if S.timed_mode:
            mode_token = "按窗口采集"
        elif S.frame_mode:
            mode_token = "按帧数采集"
        else:
            mode_token = "自由采集"
        sdir = os.path.join(sdir, mode_token)
        os.makedirs(sdir, exist_ok=True)
        fmt = cfg["save_format"]
        label = cfg["data_label"].strip() or "未填标签"
        # V6.1: 优先用人员设置面板里勾选的第 1 个人员;
        #       兼容旧的 person_name / behavior_loc 字段.
        person = "未填人员"
        loc = "未填位置"
        try:
            persons = cfg.get("persons") or []
            sel = [p for p in persons if isinstance(p, dict) and p.get("sel")]
            if sel:
                person = (sel[0].get("name") or "").strip() or "未填人员"
                lp = (sel[0].get("pos") or "").strip()
                if lp:
                    loc = lp
        except Exception:
            pass
        # 兜底: 兼容旧 cfg 字段
        if person == "未填人员":
            person = (cfg.get("person_name") or "").strip() or "未填人员"
        if loc == "未填位置":
            loc = (cfg.get("behavior_loc") or "").strip() or "未填位置"
        if label == LABEL_NO_PERSON:
            person = ""
            if not loc or loc == "未填位置":
                loc = "未填房间"
        sample_rate = cfg["sample_rate"]
        rate_token = (f"{int(round(sample_rate))}Hz"
                      if (S.applied_sample_rate and S.applied_sample_rate > 0
                          and cfg.get("sample_rate_enabled"))
                      else "广播")
        router = cfg["router_loc"]
        win_sec = cfg["win_len"]
        saved = []
        device_frame_counts = []  # 用于按帧数采集模式完成后显示每设备最终帧数
        with S.devices_lock:
            devices = list(S.devices.values())
        for dev in devices:
            if not dev.selected or not dev.all_csi_data:
                continue
            data = dev.all_csi_data
            if S.frame_mode:
                device_frame_counts.append(
                    (dev.name, dev.running_packets))
            dev_pos = (dev.position or "未填位置").strip() or "未填位置"
            first_dt = datetime.fromisoformat(data[0]["recv_time"])
            folder_date = first_dt.strftime("%Y%m%d")
            base = (f"{folder_date}_{rate_token}_{label}_"
                    f"{dev.name}_{dev_pos}_{person}_{loc}")
            if S.timed_mode:
                base += f"_{win_sec}s"
            elif S.frame_mode:
                base += f"_{S.target_frames}f"
            # 多文件: 每设备独立子文件夹锁定 (与 gui.py 同款)
            if S.timed_mode:
                session_dirs = S.win_session_dirs
            elif S.frame_mode:
                session_dirs = S.frame_session_dirs
            else:
                session_dirs = None
            if session_dirs is not None:
                dev_dir = session_dirs.get(dev.ip)
                if dev_dir is None:
                    base_dir0 = os.path.join(sdir, base)
                    suffix = ""
                    if os.path.exists(base_dir0):
                        i = 1
                        while os.path.exists(f"{base_dir0}_{i}"):
                            i += 1
                        suffix = f"_{i}"
                    dev_dir = base_dir0 + suffix
                    os.makedirs(dev_dir, exist_ok=True)
                    session_dirs[dev.ip] = dev_dir
            else:
                # 自由采集: 全局 session suffix
                base_dir0 = os.path.join(sdir, base)
                suffix = ""
                if os.path.exists(base_dir0):
                    i = 1
                    while os.path.exists(f"{base_dir0}_{i}"):
                        i += 1
                    suffix = f"_{i}"
                dev_dir = base_dir0 + suffix
                os.makedirs(dev_dir, exist_ok=True)

            # 按窗口/按帧: 整段累积数据 = 1 个文件; 自由: 按 win_sec 切窗
            if S.timed_mode or S.frame_mode:
                windows = [data]
            else:
                windows = _split_windows(data, win_sec)
            for idx, win_data in enumerate(windows):
                if not win_data:
                    continue
                start_dt = datetime.fromisoformat(win_data[0]["recv_time"])
                end_dt   = datetime.fromisoformat(win_data[-1]["recv_time"])
                start_ts = start_dt.strftime("%H%M%S")
                end_ts   = end_dt.strftime("%H%M%S")
                file_tag = f"_file{S.saved_files + 1:03d}" if (
                    S.timed_mode or S.frame_mode) else ""
                fname = (f"{label}_{dev.name}_{folder_date}_"
                         f"{start_ts}-{end_ts}{file_tag}.{fmt}")
                fpath = os.path.join(dev_dir, fname)
                if fmt == "npz":
                    import numpy as np
                    seqs = [d["seq"] for d in win_data]
                    rssis = [d["rssi"] for d in win_data]
                    csi_l = [np.array(d["csi"], dtype=np.int8) for d in win_data]
                    max_len = max(d["csi_len"] for d in win_data)
                    csi_arr = np.zeros((len(csi_l), max_len), dtype=np.int8)
                    for k, c in enumerate(csi_l):
                        csi_arr[k, :len(c)] = c
                    np.savez(fpath,
                             seq=np.array(seqs, dtype=np.uint32),
                             rssi=np.array(rssis, dtype=np.int8),
                             csi=csi_arr, csi_len=max_len,
                             mac=win_data[0]["mac"], ip=dev.ip,
                             device_name=dev.name, position=dev.position,
                             label=label, router_loc=router,
                             win_sec=win_sec, sample_rate=sample_rate)
                else:
                    import csv as csv_mod
                    with open(fpath, "w", newline="", encoding="utf-8-sig") as f:
                        w = csv_mod.writer(f)
                        w.writerow(["seq", "mac", "ip", "device", "rssi",
                                    "board_ts_us", "pc_recv_time", "csi_len",
                                    "router_loc", "position", "label",
                                    "csi_data", "sample_rate"])
                        for d in win_data:
                            w.writerow([d["seq"], d["mac"], dev.ip, dev.name,
                                        d["rssi"], d["wifi_ts"], d["recv_time"],
                                        d["csi_len"], router, dev.position,
                                        label, str(d["csi"]), sample_rate])
                saved.append(fpath)
            dev.all_csi_data = []

        # 保存成功消息 + 子文件夹提示 (与 gui.py 同款)
        was_timed_mode = S.timed_mode
        was_frame_mode = S.frame_mode
        was_target_files = S.target_files
        was_win_files = S.win_target_files
        done_files = S.saved_files + (1 if saved and not _state().get(
            "_skip_final_save", False) else 0)

        # 归集视频 (若本次采集录了视频)
        _video_was_active = _state().get("_video_was_active", False)
        if saved and _video_was_active and getattr(S, "video_path", ""):
            try:
                video_path = S.video_path
                if video_path and os.path.isfile(video_path):
                    video_root = cfg["video_save_dir"].strip() or _default_video_save_dir()
                    label2 = (cfg["data_label"].strip() or "未填标签")
                    # V6.1: 同样使用人员设置面板的勾选人员
                    person2 = "未填人员"
                    loc2 = "未填位置"
                    try:
                        ps2 = cfg.get("persons") or []
                        sel2 = [p for p in ps2 if isinstance(p, dict) and p.get("sel")]
                        if sel2:
                            person2 = (sel2[0].get("name") or "").strip() or "未填人员"
                            lp2 = (sel2[0].get("pos") or "").strip()
                            if lp2:
                                loc2 = lp2
                    except Exception:
                        pass
                    if person2 == "未填人员":
                        person2 = (cfg.get("person_name") or "").strip() or "未填人员"
                    if loc2 == "未填位置":
                        loc2 = (cfg.get("behavior_loc") or "").strip() or "未填位置"
                    if label2 == LABEL_NO_PERSON:
                        person2 = ""
                        if not loc2 or loc2 == "未填位置":
                            loc2 = "未填房间"
                    video_sub_base = (f"{datetime.now().strftime('%Y%m%d')}"
                                      f"_{rate_token}_{label2}_{person2}_{loc2}")
                    target_dir = os.path.join(video_root, mode_token, video_sub_base)
                    if os.path.exists(target_dir):
                        i = 1
                        while os.path.exists(f"{target_dir}_{i}"):
                            i += 1
                        target_dir = f"{target_dir}_{i}"
                    os.makedirs(target_dir, exist_ok=True)
                    dst = os.path.join(target_dir, os.path.basename(video_path))
                    if os.path.abspath(video_path) != os.path.abspath(dst):
                        try:
                            if os.path.exists(dst):
                                os.remove(dst)
                            os.replace(video_path, dst)
                            S.video_path = dst
                        except Exception:
                            import shutil
                            shutil.copy2(video_path, dst)
                            S.video_path = dst
                    if S.video_sync_log and os.path.isfile(S.video_sync_log):
                        try:
                            target_log = os.path.join(
                                target_dir, os.path.basename(S.video_sync_log))
                            if os.path.abspath(S.video_sync_log) != os.path.abspath(target_log):
                                if os.path.exists(target_log):
                                    os.remove(target_log)
                                os.replace(S.video_sync_log, target_log)
                                S.video_sync_log = target_log
                        except Exception:
                            pass
            except Exception as e:
                print(f"[video] 归集视频文件失败: {e}")

        # 推送保存结果消息
        if saved:
            sub_folder = os.path.basename(os.path.dirname(saved[0]))
            sub_folder_hint = ""
            if sub_folder and len(sub_folder) > 16:
                sub_folder_hint = f" → …{sub_folder[-14:]}"
            elif sub_folder:
                sub_folder_hint = f" → {sub_folder}"
            tail = " + 视频已保存" if _video_was_active else ""
            extra = ""
            if was_timed_mode and was_win_files > 1:
                extra = f" · 已完成 {done_files}/{was_win_files} 个文件"
            elif was_frame_mode:
                if was_target_files > 1:
                    extra = f" · 已完成 {done_files}/{was_target_files} 个文件"
                if device_frame_counts:
                    extra += " (" + ", ".join(
                        f"{n}={c}帧" for n, c in device_frame_counts) + ")"
            try:
                socketio.emit("message", {
                    "level": "green",
                    "text": (f"✓ 已保存 {len(saved)} 个文件"
                             f"{sub_folder_hint}{extra}{tail}")})
            except Exception:
                pass
        else:
            try:
                socketio.emit("message", {
                    "level": "red", "text": "采集已停止 (无数据)"})
            except Exception:
                pass

    except Exception as e:
        try:
            socketio.emit("message", {
                "level": "red", "text": f"✗ 保存失败: {e}"})
        except Exception:
            pass
        # 注意: 这里绝不能清空 timed_mode / frame_mode / saved_files /
        # win_target_files / win_session_dirs / frame_session_dirs!
        # 按窗口/按帧多文件采集中, 每保存一个文件都会调用本函数, 若在此清空计数,
        # 第 1 个文件保存后 win_target_files 会被重置为 1、saved_files 被重置为 0,
        # 看门狗随即误判"已存够文件"提前停止 (8 个文件只会存 2~3 个).
        # 这些字段统一由 _do_stop() 在最终停止、最后一次保存完成之后清空.


def _split_windows(data, win_sec):
    """按 win_sec 秒切窗 (自由采集模式用)."""
    if not data:
        return []
    windows = []
    current = []
    win_start = None
    for d in data:
        t = datetime.fromisoformat(d["recv_time"])
        if win_start is None:
            win_start = t
        current.append(d)
        if (t - win_start).total_seconds() >= win_sec:
            windows.append(list(current))
            current = []
            win_start = None
    if current:
        windows.append(list(current))
    return windows


# ============================================================
# 局域网扫描 (与 gui.py._probe_worker_impl 等价)
# ============================================================
def _probe_worker_bg(socketio):
    """后台扫描: ~3 秒内多轮广播 + 子网扫描 + 单播触发已知 IP."""
    import ipaddress
    total_rounds = 6
    try:
        socketio.emit("message", {"level": "purple",
                                   "text": "搜索中 (1/6 轮)"})
    except Exception:
        pass
    local_ip = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.settimeout(2.0)
            s.connect(("223.5.5.5", 80))
            local_ip = s.getsockname()[0]
        finally:
            s.close()
    except Exception:
        pass
    if not local_ip:
        try:
            local_ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            pass
    if not local_ip or local_ip.startswith("127."):
        local_ip = "192.168.1.100"
    try:
        net = ipaddress.IPv4Network(f"{local_ip}/24", strict=False)
    except Exception:
        return
    with S.devices_lock:
        known_ips = list(S.devices.keys())
    for round_idx in range(1, total_rounds + 1):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.settimeout(0.05)
            try:
                sock.sendto(BROADCAST_HELLO.encode(),
                            ("255.255.255.255", PORT))
            except Exception:
                pass
            if round_idx == 1:
                for host in net.hosts():
                    try:
                        sock.sendto(BROADCAST_HELLO.encode(),
                                    (str(host), PORT))
                    except Exception:
                        pass
            for ip in known_ips:
                try:
                    sock.sendto(BROADCAST_HELLO.encode(), (ip, PORT))
                except Exception:
                    pass
            sock.close()
        except Exception:
            pass
        if round_idx < total_rounds:
            time.sleep(0.5)
            try:
                with S.devices_lock:
                    online = sum(1 for d in S.devices.values() if d.online)
                    total = len(S.devices)
                socketio.emit("message", {
                    "level": "purple",
                    "text": f"搜索中 ({round_idx + 1}/6, 已发现 {online}/{total})"})
            except Exception:
                pass
    time.sleep(0.3)
    try:
        with S.devices_lock:
            online = sum(1 for d in S.devices.values() if d.online)
            total = len(S.devices)
        if online > 0:
            socketio.emit("message", {
                "level": "green",
                "text": f"✓ 找到 {online}/{total} 台"})
        else:
            socketio.emit("message", {
                "level": "red",
                "text": f"✗ 无设备 (历史 {total})"})
    except Exception:
        pass