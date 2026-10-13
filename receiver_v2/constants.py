# -*- coding: utf-8 -*-
"""
constants.py — 全局常量、字体、协议魔数集中定义

本模块只放"纯数据": 没有可执行逻辑, 不依赖其他业务模块,
可以被任意其它模块安全 import 而不触发副作用.

分四类:
  1. 字体 (matplotlib + tkinter)
  2. 网络协议 (端口/魔数/握手字符串)
  3. 采集配置 (采样频率/超时/默认保存目录)
  4. 文件命名 (视频子目录分隔符)
"""
from __future__ import annotations

import locale
import sys

# ============================================================
# 1. 字体 (matplotlib 中文字体 + tkinter 全局字体族)
# ============================================================
# 优先尝试 Windows 自带的微软雅黑 (msyh.ttc), 失败回退 simhei.ttf;
# 两个都失败则用 matplotlib 默认无衬线字体.
plt_font = None
try:
    from matplotlib.font_manager import FontProperties, fontManager

    for _fname in ("C:/Windows/Fonts/msyh.ttc", "C:/Windows/Fonts/simhei.ttf"):
        try:
            _fp = FontProperties(fname=_fname)
            fontManager.addfont(_fname)
            plt_font = _fp
            break
        except Exception:
            continue
except Exception:
    pass

# 在 matplotlib rcParams 里同时设置 sans-serif, 兜底保证中文能渲染
import matplotlib.pyplot as plt

if plt_font is not None:
    plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS"]
else:
    plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False  # 负号正常显示

# tkinter 控件用的统一字体族, 全部微软雅黑
FONT_FAMILY = "微软雅黑"
CHINESE_FONT         = (FONT_FAMILY, 9)
CHINESE_FONT_BOLD    = (FONT_FAMILY, 9, "bold")
CHINESE_FONT_TITLE   = (FONT_FAMILY, 10, "bold")
CHINESE_FONT_BTN     = (FONT_FAMILY, 12, "bold")
# 状态栏专用: 比普通字体小 1pt, 避免左右两边的标签和数值在窄窗口下互相遮挡
CHINESE_FONT_STATUSBAR      = (FONT_FAMILY, 8)
CHINESE_FONT_STATUSBAR_BOLD = (FONT_FAMILY, 8, "bold")
# 中部控制栏 (停止采集按钮右侧) 状态文字专用: 比按钮小 2pt,
# 避免长状态文本在窄窗口下挤占 CSI数据渲染按钮的空间.
CHINESE_FONT_MID_STATUS     = (FONT_FAMILY, 10, "bold")

# ============================================================
# 2. 网络协议常量
# ============================================================
PORT            = 3333   # UDP 发现 / CSI 数据端口 (需与 ESP 固件 PC_PORT 一致)
TCP_PORT        = 8888   # ESP TCP 控制端口 (PC → ESP 下发命令)
MAGIC           = 0x31495343  # CSI 包魔数 ("CSI1" 小端, 与 ESP 固件一致)
BROADCAST_HELLO = "ESP32_CSI_HELLO"  # ESP 主动上报的发现包
ACK_MSG         = "PC_ACK"          # PC 对 HELLO 的回复

# ============================================================
# 3. 采集配置
# ============================================================
DEFAULT_SAMPLE_RATE = 100   # 默认 ping 频率 (Hz) = CSI 采样频率
SAMPLE_RATE_MIN     = 1     # 最小采样频率
SAMPLE_RATE_MAX     = 1000  # 最大采样频率

DEFAULT_SAVE        = "csi_data"   # 默认 CSI 数据保存根目录
VIDEO_SAVE          = "video_data" # 视频根目录前缀
VIDEO_SUBFOLDER_SEP = "_"          # 子文件夹名分隔符

# 设备管理中"位置"下拉框的选项 (可直接修改, 后续按需增删)
POSITION_OPTIONS = [
    "床头柜-靠门", "床头柜-靠窗", "床尾-靠窗", "床尾-靠门", "洗衣机旁", "床头上方", "床尾", "洗手台右侧",
    "东南角", "东北角", "西南角", "西北角",
    "客厅边几", "书房书桌", "书房沙发旁",
]

# 数据标签 (下拉框选项 + 默认值) — 统一在 PC GUI 和 Web 采集页面使用.
# 修改后, 下次启动 PC GUI / 刷新 Web 页面即可看到新的标签选项.
# 第一个不一定是默认; 默认值由下面 DEFAULT_LABEL 控制, 必须能在 LABEL_OPTIONS 里找到.
LABEL_OPTIONS = [
    "无人", "有人", "静坐", "静站", "静躺", "跌倒",
    "跌倒-往前", "跌倒-往后", "跌倒-侧向", "跌倒-垂直", "行走",
    "多人-2人", "多人-3人", "多人-4人", "多人-5人",
]
DEFAULT_LABEL = "静坐"
# 特殊语义标记: 当数据标签 == 此值时, 表示"无人", 用于联动禁用"行为执行人员"等.
LABEL_NO_PERSON = "无人"

HTTP_TIMEOUT        = 3.0  # IP Webcam HTTP 请求超时 (秒)
PHONE_OFFLINE_AFTER = 6.0  # 手机心跳失败多久后判定离线 (秒)
HELLO_OFFLINE_AFTER = 8.0  # ESP HELLO 包丢失多久后判定离线 (秒)

# ============================================================
# 4. UTF-8 locale (Windows 下让 strftime 输出中文周名等)
# ============================================================
if sys.version_info[0] >= 3:
    try:
        locale.setlocale(locale.LC_ALL, "zh_CN.UTF-8")
    except Exception:
        pass
