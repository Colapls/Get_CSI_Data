# -*- coding: utf-8 -*-
"""
device_status.py — 设备在线状态维护 (后台心跳 + 看门狗)

包含两个独立线程:

  1. _phone_heartbeat_loop()
     - 每 3 秒主动探测 IP Webcam 手机是否在线
     - 通过 GET /status.json 判断, 结果 push 到 _gui_event_queue

  2. _device_online_watchdog()
     - 每 1 秒检查 S.devices, 超过 HELLO_OFFLINE_AFTER 秒没收到
       HELLO 包的设备标记为离线
     - 在线状态变化时 push 事件, GUI 线程负责刷新 Treeview

还包含:

  - _phone_base_url(ip)         : 给 IP 补 :8080 并拼 http:// 前缀
  - check_phone_online(timeout) : 单次探测, 返回 bool
"""
from __future__ import annotations

import time

import requests

from constants import HELLO_OFFLINE_AFTER, HTTP_TIMEOUT
from models import S, _gui_event_queue


# ============================================================
# IP Webcam 工具
# ============================================================
def _phone_base_url(ip: str) -> str:
    """生成 IP Webcam 的基础 URL (自动补端口 8080).

    支持三种入参:
      - "192.168.1.10"     → "http://192.168.1.10:8080"
      - "192.168.1.10:80"  → "http://192.168.1.10:80"
      - "http://x.com/a"   → "http://x.com/a" (已有 scheme 直接用)
    """
    ip = (ip or "").strip()
    if not ip:
        return ""
    if "://" in ip:
        return ip.rstrip("/")
    if ":" not in ip:
        ip = f"{ip}:8080"
    return f"http://{ip}"


def check_phone_online(timeout: float = HTTP_TIMEOUT) -> bool:
    """主动探测手机 IP Webcam 是否在线 (请求 /status.json).

    timeout 用模块常量 HTTP_TIMEOUT=3.0; 心跳线程传 2.0 提速.
    返回 True / False, 不抛异常 (网络错误一律视为离线).
    """
    ip = S.phone_ip
    if not ip:
        return False
    base = _phone_base_url(ip)
    try:
        r = requests.get(f"{base}/status.json", timeout=timeout)
        if r.status_code == 200:
            return True
    except Exception:
        pass
    return False


# ============================================================
# 1. 手机心跳 (探测线程)
# ============================================================
def _phone_heartbeat_loop():
    """后台心跳: 周期探测手机在线状态, 并把结果投到 GUI 事件队列.

    周期: 3 秒一次 (HTTP 请求可能慢, 频率不宜太高).
    写入 S.phone_online / S.phone_last_check, 加锁保护避免与 GUI 竞争.
    """
    while True:
        try:
            online = False
            if S.phone_ip.strip():
                online = check_phone_online(timeout=2.0)
            with S.phone_check_lock:
                S.phone_online     = online
                S.phone_last_check = time.time()
            try:
                _gui_event_queue.put_nowait(("phone_status", online))
            except Exception:
                pass
        except Exception:
            pass
        time.sleep(3.0)


# ============================================================
# 2. ESP 看门狗 (在线状态判定)
# ============================================================
def _device_online_watchdog():
    """后台看门狗: 周期检查 ESP 设备, 标记超时未收 HELLO 的设备为离线.

    判定规则: last_hello > 0 且距上次 HELLO ≤ HELLO_OFFLINE_AFTER(8s) 视为在线.
    CSI 包不算"在线"依据 — 必须 ESP 主动发了 HELLO 包才算.
    (因为 CSI 数据流可能因路由器限速 / ESP 切换信道临时中断几秒,
     用 HELLO 更稳; ESP 在阶段 1 默认每 3 秒发一次 HELLO).

    离线时把 rate_pushed 重置, 下次上线 _refresh_plot 会重新推采样频率.
    """
    while True:
        try:
            now = time.time()
            changed = []
            with S.devices_lock:
                for ip, d in S.devices.items():
                    want_online = (d.last_hello > 0 and
                                   (now - d.last_hello) <= HELLO_OFFLINE_AFTER)
                    if d.online != want_online:
                        d.online = want_online
                        if not want_online:
                            # 离线后采样频率下发标记重置, 重连后重新推
                            d.rate_pushed = False
                        changed.append((ip, want_online))
            for ip, online in changed:
                try:
                    _gui_event_queue.put_nowait(("device_online_changed", ip))
                except Exception:
                    pass
        except Exception:
            pass
        time.sleep(1.0)
