# Get CSI Data

ESP32-S3 WiFi CSI (Channel State Information) 采集工具集 — 网页版实时可视化 + 多设备同步采集 + 固件源码。

面向 **CSI 感知 / 行为识别 / 跌倒检测 / 存在性检测 / 人数估计 / AoA 估计** 等研究方向。

---

## 仓库内容

```
.
├── main/                # ESP32-S3 固件 (ESP-IDF v5/v6)
│   ├── main.c           # CSI 采集、UDP 上报、TCP 控制、AP 配网 fallback
│   └── CMakeLists.txt
├── web_collector/       # 网页版实时采集工具 (Flask + SocketIO + Canvas)
│   ├── app.py           # Flask 入口
│   ├── api.py           # REST 路由
│   ├── socketio_bridge.py
│   ├── templates/       # 单页应用 (index.html)
│   ├── static/          # css / js
│   └── requirements.txt
├── receiver_v2/         # web_collector 复用的后台业务模块 (必带)
│   ├── udp_listener.py  # UDP 3333 监听 + HELLO/PC_ACK 设备发现
│   ├── device_status.py # 设备在线看门狗 + IP Webcam 心跳
│   ├── models.py        # Device 数据模型 + 全局状态
│   ├── constants.py     # 字体/协议魔数/标签选项
│   └── video.py         # ffmpeg 视频同步录制
└── CMakeLists.txt       # ESP-IDF 项目入口
```

---

## 系统架构

```
                    ┌──────────────────┐
                    │   路由器 (AP)    │
                    └────────┬─────────┘
                          WiFi HT
        ┌──────────────────┼──────────────────┐
   ┌────▼─────┐      ┌──────▼───┐       ┌──────▼───┐
   │ ESP32-S3 │ ...  │ ESP32-S3 │  ...  │ ESP32-S3 │
   │ (node)   │      │ (node)   │       │ (node)   │
   └────┬─────┘      └─────┬────┘       └─────┬────┘
        │  UDP 3333  +  TCP 8888           │
        └────────────────┬──────────────────┘
                         ▼
              ┌──────────────────────┐
              │   web_collector/     │   Flask + SocketIO
              │   (本仓库)           │   http://<PC-IP>:2234
              └─────────┬────────────┘
                        │ 归档 npz / csv (+ 可选视频)
                        ▼
              ┌──────────────────────┐
              │  csi_data_YYYYMMDD/  │
              └──────────────────────┘
```

每台 ESP 主动 ping 路由器触发 CSI；UDP 上报到 PC 的 Flask 后端；前端通过 WebSocket 实时刷新波形并落盘。

---

## 快速开始

### 1. 固件 (`main/`)

需要 ESP-IDF v5.x 或 v6.x + ESP32-S3 工具链。

```bash
cd main
idf.py set-target esp32s3
idf.py build
idf.py -p COMx flash monitor
```

默认配置（修改 `main/main.c` 顶部宏）：

| 宏 | 默认值 | 含义 |
|---|---|---|
| `WIFI_SSID` / `WIFI_PASS` | `Test-403` / `12345678` | STA 模式凭据（NVS 持久化） |
| `PING_INTERVAL_MS` | `20` | 默认 ping 间隔 = 50 Hz |
| `DEFAULT_SAMPLE_RATE` | `50` | 默认采样频率 Hz |
| `SAMPLE_RATE_MIN/MAX` | `1` / `1000` | 频率范围 |
| `LED_GPIO` | `GPIO_NUM_48` | WS2812 状态灯 |
| `PC_PORT` / `TCP_PORT` | `3333` / `8888` | UDP 数据 / TCP 控制 |
| `AP_CONFIG_*` | `csi_setup` / `csi12345` / `8899` | AP 配网 fallback |

连不上 WiFi 5 秒后自动开 AP `csi_setup`，浏览器会自动弹出配网页（Captive Portal）；USB 串口 `115200` 也可同时输入命令救砖。

### 2. 网页版采集工具 (`web_collector/`)

```bash
cd web_collector
pip install -r requirements.txt
python app.py
# 浏览器打开 http://127.0.0.1:2234/
```

Windows 双击 `start.bat` 自动装依赖 + 拉起浏览器。

依赖：`flask`、`flask-socketio`、`simple-websocket`、`numpy`、`matplotlib`、`requests`、`pillow`。

> 网页版后台复用了 `receiver_v2/` 的 UDP/TCP 业务模块，因此本仓库必须包含该目录，单独 `web_collector/` 跑不起来。

---

## TCP 控制命令（连上 ESP 后发送）

| 命令 | 说明 |
|---|---|
| `STATUS` | 返回 `{packets, rx, streaming, mode, role, sender_mac, ...}` |
| `PING` | 心跳 |
| `SAMPLE_RATE <hz>` | 动态设置采样频率；单独 `SAMPLE_RATE` 查询当前频率 |
| `LED_MODE WINDOW\|FREE\|OFF` | 切换状态灯 |
| `WIFI_CONFIG <ssid> <pwd>` | 换网并重启 |
| `MODE MULTI` | 多发多收：每台 ESP 独立 ping 路由器 |
| `MODE SINGLE <AA:BB:CC:DD:EE:FF>` | 一发多收：MAC 匹配的 ESP 当 ESP-NOW sender，其余 promiscuous 接收，统一 seq 对齐 |
| `MODE STATUS` | 查询当前 mode / role / sender_mac |
| `RESTART` / `REBOOT` | 软重启 |

## UDP 数据包格式（PC 端解析用）

```c
typedef struct __attribute__((packed)) {
    uint32_t magic;      // 0x31495343  ("CSI1", little-endian)
    uint32_t seq;        // 包序号 (MULTI 自增, SINGLE 来自 ESP-NOW)
    uint32_t csi_len;    // CSI 字节数 (≤ 512)
    int8_t   rssi;       // RSSI
    uint8_t  mac[6];     // 源 MAC
    uint8_t  reserved;
    int64_t  wifi_ts;    // PHY 时间戳 (μs)
    int8_t   csi[];      // int8 I/Q 交错原始数据
} csi_packet_t;
```

## 端口 / 通道一览

| 用途 | 协议 | 端口 |
|---|---|---|
| CSI 数据 + HELLO/PC_ACK 设备发现 | UDP | 3333 |
| TCP 控制通道 | TCP | 8888 |
| AP fallback 配网 TCP（兜底） | TCP | 8899 |
| AP fallback Captive Portal | HTTP | 80 |
| 串口命令 | UART | 115200 |
| 网页版采集工具 | HTTP | 2234 |
| IP Webcam 探测 | HTTP | 8080（可改） |

---

## 采集模式

两种模式运行时可热切换（`MODE MULTI` ↔ `MODE SINGLE <mac>`）。

### MULTI — 多发多收（默认）
每台 ESP 各自 ping 路由器采样；本地自增 `seq`。各设备完全独立，对齐简单。

### SINGLE — 一发多收
指定一台 ESP 作为 sender，通过 ESP-NOW (vendor-specific action frame) 周期性广播；其余 ESP 进入 promiscuous，按 sender MAC 过滤接收；`seq` 跨设备对齐，便于后期多设备联合分析。

底层约定：`csi_packet_t.flags` 高位（bit7）标记新固件帧、低位（bit0）标记 `is_ping_reply`，接收端据此过滤。

---

## 关键约束 / 已踩过的坑

- **`wifi_csi_config_t.channel_filter_en` 是 PHY BSSID 过滤**，会屏蔽 ESP-NOW 帧。SINGLE 模式必须 `false`；MULTI 必须 `true`。
- **ESP-NOW 是 802.11 vendor-specific action frame**（管理帧），`wifi_csi_info_t.mac` 经常为 0，过滤时需退回 `info->payload + 10`（Address 2 = SA）。
- **2.4 GHz ESP-NOW 推荐 `WIFI_PHY_MODE_HT20`**，HT40 在 HT20 接收端丢包率 ~90%。
- ESP-IDF v5.1+ 用 `esp_now_set_peer_rate_config(peer, &rate)`，旧版用 `esp_wifi_config_espnow_rate(ifx, rate)`。

---

## 文件说明

- **UDP 数据落盘格式**：`.npz` 含 `seq` / `rssi` / `csi` / `mac` / `ip` / `device_name` / `position` / `label` / `router_loc` / `win_sec` / `sample_rate`。`csi` 是 `(T, csi_len)` int8 矩阵，每两字节一组 `(I, Q)`。
- **三种采集方式**：按窗口采集（定时长）/ 按帧数采集（达帧数自动保存）/ 自由采集（手动起停）。
- **自动命名**：`{日期}_{Hz/广播}_{标签}_{设备名}_{位置}_{人员}__{N}`，重复时追加序号。
- **网页版实测采样率**：5 处位置显示，绿=实测≈配置（85–115%）、橙=偏离、红=严重偏离。

---

## License

仅供学习与研究使用。
