# -*- coding: utf-8 -*-
"""
video.py — IP Webcam 视频同步录制 (基于 ffmpeg 子进程)

包含三个对外函数:

  - _find_ffmpeg()             : 查 PATH + 常见安装路径, 返回可执行文件路径
  - pc_start_record(ip, dir)   : 启动 ffmpeg 录制 MJPEG 流到本地 mp4
  - pc_stop_record()           : 优雅停止 (SIGINT), 让 ffmpeg 写完 moov
  - _write_video_sync_log(tag) : 写入 sync 日志, 供 CSI 与视频帧时间对齐

工作流:
  GUI "开始采集" → 若勾选了 "同时采集视频" → 调 pc_start_record
  GUI "停止采集" → 调 pc_stop_record → 调 _write_video_sync_log

ffmpeg 命令选型说明:
  - libx264 + ultrafast + zerolatency  →  实时软编码, CPU 占用低
  - +faststart+frag_keyframe+empty_moov →  mp4 可边录边播, 修复 moov 丢失问题
"""
from __future__ import annotations

import os
import signal
import subprocess
import time
from datetime import datetime

from device_status import _phone_base_url
from models import S


# ============================================================
# 1. 查找 ffmpeg
# ============================================================
def _find_ffmpeg() -> str:
    """查找 ffmpeg 路径: 先查 PATH, 再查常见安装目录.

    返回可执行文件绝对路径; 找不到时直接返回 "ffmpeg" 让 subprocess
    自行报错, 这样错误信息能告诉用户 PATH 没配.
    """
    try:
        result = subprocess.run(
            ["where", "ffmpeg"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                line = line.strip()
                if line and os.path.isfile(line):
                    return line
    except Exception:
        pass

    # Windows 常见安装路径
    common = [
        r"D:\ffmpeg\ffmpeg-9.0.1-essentials_build\bin\ffmpeg.exe",
        r"C:\ffmpeg\bin\ffmpeg.exe",
        r"D:\tools\ffmpeg\bin\ffmpeg.exe",
    ]
    for p in common:
        if os.path.isfile(p):
            return p
    return "ffmpeg"


# ============================================================
# 2. 启动 / 停止录制
# ============================================================
def pc_start_record(ip: str, save_dir: str) -> tuple[bool, str, str]:
    """用 ffmpeg 在电脑端录制 IP Webcam 的 MJPEG 视频流.

    参数:
      ip       : 手机 IP (如 "192.168.1.10" 或 "192.168.1.10:8080")
      save_dir : 视频文件保存目录

    返回: (成功, 文件路径, 错误信息)
      - 成功: (True, "/path/video_20260120_153045.mp4", "")
      - 失败: (False, "", "错误说明")

    实现要点:
      1. 文件名 video_YYYYMMDD_HHMMSS.mp4
      2. 等 2 秒确认 ffmpeg 没立刻退出 (流没连上会秒退)
      3. 成功后保存 proc 到 S.ffmpeg_proc, GUI 之后会用它停止
    """
    ffmpeg = _find_ffmpeg()
    os.makedirs(save_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"video_{ts}.mp4"
    fpath = os.path.join(save_dir, fname)
    stream_url = f"{_phone_base_url(ip)}/video"

    # ffmpeg 命令: libx264 软编码 + 零延迟调优 + 流式 moov
    cmd = [
        ffmpeg, "-y",
        "-loglevel", "info",
        "-fflags", "nobuffer",
        "-flags", "low_delay",
        "-i", stream_url,
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-tune", "zerolatency",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart+frag_keyframe+empty_moov",
        "-f", "mp4",
        fpath,
    ]

    # 启动子进程
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        return False, "", (
            "未找到 ffmpeg!请按以下步骤安装:\n"
            "1) 访问 https://www.gyan.dev/ffmpeg/builds/\n"
            "2) 下载 ffmpeg-release-essentials.zip\n"
            "3) 解压后把 bin 目录添加到系统 PATH 环境变量\n"
            "4) 重启本工具"
        )
    except Exception as e:
        return False, "", str(e)

    # 等 2 秒确认 ffmpeg 没秒退 (如 IP 写错 / 手机没开 IP Webcam 都会秒退)
    time.sleep(2.0)
    if proc.poll() is not None:
        stderr = proc.stderr.read().decode("utf-8", errors="replace")
        err = stderr.strip().split("\n")[-1] if stderr else "ffmpeg 已退出"
        return False, "", f"ffmpeg 启动失败: {err}"

    # 成功: 把 proc 和路径存到全局, GUI 后续会用到
    S.ffmpeg_proc    = proc
    S.video_path     = fpath
    S.video_filename = fname
    return True, fpath, ""


def pc_stop_record() -> tuple[bool, str]:
    """停止 ffmpeg 录制, 返回 (成功, 文件路径/错误信息).

    关闭策略:
      1. 先 SIGINT (Ctrl+C) 让 ffmpeg 走优雅退出 — 写完 moov box
         这是修复 mp4 录完无法播放的关键
      2. 10 秒没退就 SIGTERM
      3. 再 5 秒没退就 SIGKILL

    这样无论 ffmpeg 在做什么编码, 最终都能结束, 且文件能正常播放.
    """
    proc = getattr(S, "ffmpeg_proc", None)
    if proc is None:
        return False, "ffmpeg 录制未启动"

    try:
        if proc.poll() is None:
            # 1) SIGINT 优雅退出
            try:
                proc.send_signal(signal.SIGINT)
            except Exception:
                try:
                    proc.terminate()
                except Exception:
                    pass
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                # 2) 强制 SIGTERM
                try:
                    proc.terminate()
                except Exception:
                    pass
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # 3) 最后手段 SIGKILL
                    try:
                        proc.kill()
                    except Exception:
                        pass
                    try:
                        proc.wait(timeout=3)
                    except Exception:
                        pass
    except Exception:
        pass

    S.ffmpeg_proc = None
    fpath = getattr(S, "video_path", "")
    if fpath and os.path.isfile(fpath):
        return True, fpath
    return False, f"视频文件不存在: {fpath}"


# ============================================================
# 3. 录像同步日志
# ============================================================
def _write_video_sync_log(session_tag: str) -> None:
    """把本次录像的电脑端开始/结束时间戳写入 sync_<session>.log.

    文件格式 (供后续 Python 脚本解析):
      # 录像同步日志: 电脑端 ffmpeg 录制
      # IP Webcam 地址: 192.168.1.10
      video_record_start,1764123456.789
      video_record_stop,1764123678.123
      video_file,D:/data/video_20260120_153045.mp4

    时间戳是 time.time() 浮点秒, 与 CSI recv_time 用同一时基,
    后期可直接做帧级时间对齐.
    """
    if not S.video_sync_log:
        return
    try:
        with open(S.video_sync_log, "w", encoding="utf-8") as f:
            f.write(f"# 录像同步日志: 电脑端 ffmpeg 录制\n")
            f.write(f"# IP Webcam 地址: {S.phone_ip}\n")
            f.write(f"# 视频来源: http://{S.phone_ip}:8080/video (MJPEG 流)\n")
            f.write(f"video_record_start,{S.video_start_ts}\n")
            f.write(f"video_record_stop,{S.video_stop_ts}\n")
            f.write(f"video_file,{getattr(S, 'video_path', '')}\n")
            f.write(
                "# 提示: 与 CSI 文件夹中的 npz 一起存档, "
                "用于后期 CSI 与视频帧时间对齐\n"
            )
    except Exception as e:
        print(f"[video] 写同步日志失败: {e}")
