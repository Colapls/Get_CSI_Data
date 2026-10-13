# -*- coding: utf-8 -*-
"""
udp_listener.py — UDP / TCP 网络监听与命令下发

包含两个后台线程函数:

  1. _udp_listener()
     - 绑定 0.0.0.0:3333 (或 3334-3337 备用), 单 socket 接收所有 ESP 设备数据
     - 同时识别两类 UDP 包:
        a) "ESP32_CSI_HELLO"  (设备首次上线 / 周期性心跳)
        b) 二进制 CSI 数据包 (magic=0x31495343)
     - 解析后维护 S.devices 字典, 并把事件 push 到 _gui_event_queue,
       由 GUI 线程在主循环里 poll 处理.

  2. _tcp_listener_for(ip)
     - 每台设备一条长连接 (PC 作客户端, ESP 作 TCP server)
     - 启动时发 STATUS, 之后按 2 秒间隔轮询状态
     - 同时维护 dev.tcp_cmd_queue: GUI 线程往里 put 控制命令
       (如 "WIFI_CONFIG ssid/pwd", "SAMPLE_RATE 200", "LED_MODE ..."),
       本线程取出后通过同一 socket 下发并等 "type=ok/error" 应答.
"""
from __future__ import annotations

import json
import queue
import socket
import struct
import threading
import time
from datetime import datetime

from constants import ACK_MSG, BROADCAST_HELLO, MAGIC, PORT, TCP_PORT
from models import Device, S, _gui_event_queue, unique_name_from_mac


# ============================================================
# 1. UDP 监听 (所有 ESP 共用一个 socket)
# ============================================================
def _udp_listener():
    """监听所有 ESP 设备的 UDP 数据 / HELLO.

    线程生命周期: 程序启动时启动一次, 永不退出.
    出错仅打印, 不抛异常退出 — 上层靠看门狗 + 重连机制恢复.
    """
    # ---- 1) 解析电脑本机 IP, 用于过滤回环 UDP 包 ----
    try:
        _probe_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        _probe_sock.connect(("223.5.5.5", 80))   # 用阿里 DNS 探测出口 IP
        S.local_ip = _probe_sock.getsockname()[0]
        _probe_sock.close()
    except Exception:
        S.local_ip = None

    # ---- 2) 绑定 UDP 端口 (主端口被占则顺延) ----
    sock = None
    actual_port = PORT
    for try_port in (PORT, PORT + 1, PORT + 2, PORT + 3, PORT + 4):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("0.0.0.0", try_port))
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            actual_port = try_port
            break
        except OSError as e:
            print(f"[UDP] 端口 {try_port} 绑定失败: {e}")
            try:
                sock.close()
            except Exception:
                pass
            sock = None
            continue

    if sock is None:
        # 通知 GUI: 5 个端口全部占用, 用户能看到红色提示
        try:
            _gui_event_queue.put_nowait(("udp_error", "无可用 UDP 端口 (3333-3337)"))
        except Exception:
            pass
        return

    sock.settimeout(0.5)
    if actual_port != PORT:
        print(f"[UDP] 主端口 {PORT} 被占用, 已切换到备用端口 {actual_port}")
        try:
            _gui_event_queue.put_nowait(("udp_port", actual_port))
        except Exception:
            pass

    # ---- 3) 主循环: 阻塞接收 + 分发 ----
    while True:
        try:
            try:
                data, addr = sock.recvfrom(4096)
            except socket.timeout:
                continue

            ip = addr[0]

            # --- A. HELLO 包 (设备主动上报, ASCII 字符串) ---
            if data == BROADCAST_HELLO.encode():
                # 过滤电脑端自己发出去被回环接收的包 (Windows 默认回环广播)
                if S.local_ip is not None and ip == S.local_ip:
                    sock.sendto(ACK_MSG.encode(), addr)
                    continue
                sock.sendto(ACK_MSG.encode(), addr)
                with S.devices_lock:
                    if ip not in S.devices:
                        dev = Device(ip=ip)
                        S.devices[ip] = dev
                        try:
                            _gui_event_queue.put_nowait(("device_added", ip))
                        except Exception:
                            pass
                    dev = S.devices[ip]
                    now = time.time()
                    dev.online     = True
                    dev.last_seen  = now
                    dev.last_hello = now
                try:
                    _gui_event_queue.put_nowait(("device_hello", ip))
                except Exception:
                    pass
                continue

            # --- B. CSI 二进制包 ---
            # 最小长度: 12(magic/seq/len) + 1(rssi) + 6(mac) + 1(保留) + 8(wifi_ts) = 28
            if len(data) < 28:
                continue
            # CSI 数据不会从电脑端发出, 但保险起见也过滤本机回环
            if S.local_ip is not None and ip == S.local_ip:
                continue

            magic, seq, csi_len = struct.unpack_from("<III", data, 0)
            if magic != MAGIC:
                continue
            if csi_len > 512 or csi_len < 0 or 28 + csi_len > len(data):
                continue

            rssi    = struct.unpack_from("<b", data, 12)[0]
            mac     = ":".join(f"{b:02X}" for b in data[13:19])
            # flags 字节 (offset 19) — 新固件按时间窗标记 is_ping_reply;
            # 旧固件此字节常为 0 (uninitialized reserved).
            flags   = data[19]
            is_new_fw     = bool(flags & 0x80)
            is_ping_reply = bool(flags & 0x01) if is_new_fw else True
            #   旧固件: 0x80 位未置位 → 视为全部 ping reply (向后兼容)
            wifi_ts = struct.unpack_from("<q", data, 20)[0]
            csi_list = list(struct.unpack_from(f"<{csi_len}b", data, 28))
            csi_raw  = data[28:28 + csi_len]

            with S.devices_lock:
                if ip not in S.devices:
                    dev = Device(ip=ip)
                    S.devices[ip] = dev
                    try:
                        _gui_event_queue.put_nowait(("device_added", ip))
                    except Exception:
                        pass
                dev = S.devices[ip]
                now = time.time()
                was_online = dev.online
                # CSI 包也算心跳: ESP 进入 CSI 阶段后只发 CSI 不发 HELLO,
                # 必须用 CSI 数据判定"在线", 否则会被看门狗误杀.
                dev.online     = True
                dev.last_seen  = now
                dev.last_hello = now
                if not was_online:
                    try:
                        _gui_event_queue.put_nowait(("device_online_changed", ip))
                    except Exception:
                        pass
                dev.mac       = mac
                dev.rssi      = rssi
                dev.csi_len   = csi_len
                dev.seq       = seq
                dev.total_packets += 1
                # 设备名跟随 MAC (跨 IP 变化保持稳定), 碰撞时退到 4 位 hex
                existing = {d.name for d in S.devices.values()
                             if d is not dev and d.name}
                new_name = unique_name_from_mac(mac, dev.name, existing)
                if new_name and new_name != dev.name:
                    dev.name = new_name
                if not dev.esp_state:
                    dev.esp_state = "GAIN_LOCK"
                # 记录包到达时间戳 (用于后续计算"实测采样频率").
                # 只把 is_ping_reply=1 的包计入, 避免 beacon / 其他 AP 帧污染实测频率.
                # 旧固件 (is_new_fw=False) 一律视为 ping reply, 保持向后兼容.
                # total_packets 仍 += 1 (统计所有 CSI 包, 用于诊断).
                # 放在 with 块内避免锁外访问, 但 record_packet_arrival 内部自带锁, 双锁安全.
                if is_ping_reply:
                    try:
                        dev.record_packet_arrival(now)
                    except Exception:
                        pass

                try:
                    dev.csi_queue.put_nowait((seq, rssi, csi_raw, csi_len, mac, wifi_ts))
                except queue.Full:
                    pass

                # 采集模式下: 追加到窗口缓冲
                if S.running and dev.selected:
                    dev.all_csi_data.append({
                        "seq": seq, "rssi": rssi, "mac": mac,
                        "wifi_ts": wifi_ts, "csi_len": csi_len,
                        "csi": csi_list, "csi_raw": csi_raw,
                        "recv_time": datetime.now().isoformat(timespec="milliseconds"),
                        "ip": ip,
                    })
                    dev.running_packets += 1
        except Exception:
            # 任何异常吞掉, 保持线程活着; 出错会在下个 recvfrom 自然恢复
            pass


# ============================================================
# 2. TCP 控制 (每设备一条长连接)
# ============================================================
def _tcp_listener_for(ip: str):
    """维护与单台 ESP 的常驻 TCP 连接.

    协议:
      - PC 客户端连上后先发 STATUS\\n, 之后每 2 秒再发一次
      - 固件 TCP server 同时间只接受 1 个客户端, 因此控制命令
        (WIFI_CONFIG / SAMPLE_RATE / LED_MODE / ... ) 必须复用本连接
        通过 dev.tcp_cmd_queue 排队下发, 并等 type=ok/error 应答.
      - 心跳 / status 行的 JSON 由 GUI 线程 poll 处理.
    """
    buf = ""
    while True:
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((ip, TCP_PORT))
            with S.devices_lock:
                if ip in S.devices:
                    S.devices[ip].tcp_status = "已连接"
            sock.sendall(b"STATUS\n")

            while True:
                # ---- A. 取出待执行的 TCP 控制命令 ----
                cmd_item = (None, None)
                with S.devices_lock:
                    d = S.devices.get(ip)
                    if d is not None:
                        try:
                            cmd_item = d.tcp_cmd_queue.get_nowait()
                        except queue.Empty:
                            cmd_item = (None, None)
                cmd, resp_q = cmd_item
                if cmd:
                    try:
                        sock.sendall((cmd + "\n").encode())
                        print(f"[TCP] 下发命令到 {ip}: {cmd}")
                    except Exception:
                        print(f"[TCP] {ip} 命令发送失败")
                        break
                    # 等待应答, 截止 6 秒
                    deadline = time.time() + 6.0
                    while time.time() < deadline:
                        try:
                            chunk = sock.recv(4096)
                        except socket.timeout:
                            continue
                        if not chunk:
                            break
                        buf += chunk.decode("utf-8", errors="replace")
                        while "\n" in buf:
                            line, buf = buf.split("\n", 1)
                            line = line.strip()
                            if not line:
                                continue
                            if resp_q is not None and (
                                '"type":"ok"' in line or '"type":"error"' in line
                            ):
                                try:
                                    resp_q.put_nowait(line)
                                    print(f"[TCP] {ip} 命令应答: {line}")
                                except Exception:
                                    pass
                                resp_q = None
                        if resp_q is None:
                            break
                    if resp_q is not None:
                        print(f"[TCP] {ip} 命令超时无应答: {cmd}")

                # ---- B. 常规 STATUS 轮询 ----
                try:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    buf += chunk.decode("utf-8", errors="replace")
                except socket.timeout:
                    # 没新数据就再发一次 STATUS
                    try:
                        sock.sendall(b"STATUS\n")
                    except Exception:
                        break

                # ---- C. 解析 JSON 行 ----
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    with S.devices_lock:
                        if ip in S.devices:
                            d = S.devices[ip]
                            t = obj.get("type")
                            if t == "status":
                                d.esp_state     = obj.get("state", d.esp_state)
                                d.esp_packets   = obj.get("packets", d.esp_packets)
                                d.esp_streaming = obj.get("streaming", d.esp_streaming)
                                # 固件 STATUS 已携带本机 MAC, 无需等 CSI 数据回流即可拿到,
                                # 支撑"一发多收"模式在 TCP 建立后立即指定/确认发送端
                                m = obj.get("mac")
                                if m:
                                    d.mac = m
                                    # 设备名跟随 MAC, 碰撞时退到 4 位 hex
                                    existing = {other.name for other in S.devices.values()
                                                if other is not d and other.name}
                                    new_name = unique_name_from_mac(m, d.name, existing)
                                    if new_name and new_name != d.name:
                                        d.name = new_name
                            elif t == "restarting":
                                d.esp_state   = "RESTARTING"
                                d.tcp_status  = "未连接"
        except Exception:
            pass
        finally:
            with S.devices_lock:
                if ip in S.devices:
                    S.devices[ip].tcp_status = "未连接"
            try:
                sock.close()
            except Exception:
                pass
        # 断线后 2 秒重试
        time.sleep(2)
