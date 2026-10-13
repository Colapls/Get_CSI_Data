#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app.py — 网页版 CSI 采集工具 后端入口

模块结构:
  app.py              本文件: Flask + SocketIO 启动, 路由注册
  socketio_bridge.py  WebSocket 事件桥接 (复用 receiver/ 后台线程)
  api.py              REST API: 采集控制 / 设备管理 / 采样频率 / 文件保存

设计目标:
  1. 复用 receiver/ 下所有现有业务模块 (models, udp_listener,
     device_status, video, constants), 不重复造轮子.
  2. SocketIO 推送 4 类事件:
       - device_update     设备列表 / 在线状态变化
       - csi_batch         CSI 包批量推送 (前端绘 Canvas 波形)
       - status_update     采集状态 / 进度
       - phone_update      IP Webcam 在线状态
       - message           状态栏文本 (搜索中/保存中/...)
  3. REST API 设计为"动作"语义: POST 一条命令即执行一次,
     响应返回 {'ok': True/False, 'msg': '...'}.

启动: python app.py  → 浏览器访问 http://127.0.0.1:2234/
"""
from __future__ import annotations

import os
import sys
import threading

# 把项目根目录加入 sys.path, 让 web_collector 能 import receiver_v2 模块
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
_RECEIVER = os.path.join(_ROOT, "receiver_v2")
_RECEIVER_OLD = os.path.join(_ROOT, "receiver")  # 兼容老版本
for p in (_HERE, _RECEIVER, _RECEIVER_OLD, _ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

# 强制导入 receiver 的常量, 让 matplotlib 中文字体初始化生效
import constants as _rc  # noqa: E402

from flask import Flask, render_template, send_from_directory  # noqa: E402
from flask_socketio import SocketIO  # noqa: E402

# 创建 Flask 应用
app = Flask(
    __name__,
    template_folder=os.path.join(_HERE, "templates"),
    static_folder=os.path.join(_HERE, "static"),
)
app.config["SECRET_KEY"] = "csi-web-collector-v1"
app.config["JSON_AS_ASCII"] = False  # 中文不转 \uXXXX

# async_mode 默认 threading (依赖 simple_websocket, 已安装)
# 不使用 eventlet/gevent, 减少依赖
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# 启动后台采集线程 (复用 receiver 已有模块)
from socketio_bridge import start_background_threads  # noqa: E402
from api import register_api_routes  # noqa: E402

# 注册 REST API 路由
register_api_routes(app, socketio)

# 启动后台 UDP / TCP / 心跳线程, 把事件转发到所有 WebSocket 客户端
start_background_threads(socketio)


# ============================================================
# 页面路由
# ============================================================
@app.route("/")
def index():
    """主页: 单页应用入口"""
    return render_template(
        "index.html",
        version="V6.1",
        label_options=_rc.LABEL_OPTIONS,
        default_label=_rc.DEFAULT_LABEL,
    )


@app.route("/logo.png")
def logo():
    """项目根目录 logo.png (网页版右上角显示)"""
    return send_from_directory(_ROOT, "logo.png")


@app.route("/health")
def health():
    """健康检查端点, 前端用 /health 探测后端是否就绪"""
    return {"ok": True, "version": "V6.1"}


# ============================================================
# 启动入口
# ============================================================
def main():
    """启动 Flask + SocketIO 服务.
    默认端口 2234 (避开 csi_visualizer 的 2233)."""
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "2234"))
    print(f"\n[web_collector] 启动中...")
    print(f"[web_collector] 浏览器访问: http://127.0.0.1:{port}/")
    print(f"[web_collector] 健康检查:    http://127.0.0.1:{port}/health")
    # allow_unsafe_werkzeug=True 让开发期可以用 Flask 自带 server;
    # 生产请用 gunicorn / eventlet
    socketio.run(app, host=host, port=port,
                 allow_unsafe_werkzeug=True, debug=False)


if __name__ == "__main__":
    main()