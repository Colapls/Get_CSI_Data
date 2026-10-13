#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <math.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/event_groups.h"
#include "freertos/semphr.h"
#include "esp_log.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "esp_wifi.h"
#include "esp_now.h"
#include "esp_mac.h"
#include "nvs_flash.h"
#include "driver/gpio.h"
#include "lwip/sockets.h"
#include "lwip/inet.h"
#include "ping/ping_sock.h"
#include "esp_system.h"
#include "esp_timer.h"
#include "driver/uart.h"
#include "esp_http_server.h"
#include "esp_netif_net_stack.h"
#include "esp_netif_defaults.h"
#include "driver/rmt_tx.h"

/* ========== 需要按你的环境修改的部分 ========== */
#define WIFI_SSID        "Test-403"
#define WIFI_PASS        "12345678"
// #define WIFI_SSID        "TP-LINK_3588"
// #define WIFI_PASS        "ms546648"
#define PC_PORT          3333           // UDP 发现 + CSI 数据端口
#define TCP_PORT         8888           // TCP 控制端口（远程命令）
#define LED_GPIO         GPIO_NUM_48    // 状态灯
#define PING_INTERVAL_MS 20             // 默认 ping 间隔 (20ms = 50Hz)
#define DEFAULT_SAMPLE_RATE 50          // 默认采样频率 (Hz), 与 PING_INTERVAL_MS 对应
#define SAMPLE_RATE_MIN  1              // 采样频率下限 (Hz)
#define SAMPLE_RATE_MAX  1000           // 采样频率上限 (Hz)
/* ============================================ */

#define CSI_BUF_MAX      512
#define CSI_MAGIC        0x31495343     // "CSI1"


static const char *TAG = "csi_node";

/* ---------- 数据包结构（UDP 发送用） ---------- */
/* flags 字段语义 (1 字节):
 *   bit7 (0x80) : 固件版本标记, 1 = 新固件 (按时间窗标记 is_ping_reply).
 *                 PC 端据此判断是否解析 is_ping_reply bit;
 *                 旧固件此字段为 0 (原 uninitialized reserved 字节).
 *   bit0 (0x01) : is_ping_reply, 1 = 当前 CSI 帧 wifi_ts 落在
 *                 ping 预期回包时刻 ±4000us 内.
 *   被动模式 (rate=0 / ping 未启用) 时: flags = 0x81 (全部算合法包).
 */
typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t seq;
    uint32_t csi_len;
    int8_t   rssi;
    uint8_t  mac[6];
    uint8_t  flags;
    int64_t  wifi_ts;
    int8_t   csi[CSI_BUF_MAX];
} csi_packet_t;

/* ===================== 采集模式 (Multi vs Single TX) =====================
 * - MODE_MULTI:        多发多收 (默认). 每台 ESP 主动 ping 路由器,
 *                       CSI 由路由器回包触发. 所有设备各自独立采采样.
 * - MODE_SINGLE_TX:    一发多收. 电脑端指定一台作为 ESP-NOW 发包端,
 *                       其它设备进入 promiscuous 监听 + MAC 过滤,
 *                       按发送端的 seq 对齐多设备数据.
 * 通过 TCP 命令 "MODE MULTI" / "MODE SINGLE <sender_mac>" 运行时切换. */
typedef enum { MODE_MULTI = 0, MODE_SINGLE_TX } csi_mode_t;
typedef enum { ROLE_IDLE = 0, ROLE_SENDER, ROLE_RECEIVER } csi_role_t;

#define ESPNOW_PMK       "pmk1234567890123"        /* 16 字节 */
#define ROLE_SENDER_BIT  BIT0

/* ---------- 全局变量 ---------- */
static volatile bool     s_streaming = false;
static volatile uint32_t s_seq = 0;
static volatile uint32_t s_total_packets = 0;
static volatile uint32_t s_udp_rx_cnt = 0;   // 已收到的电脑触发包数（用于丢包统计/验证）
static volatile bool     s_pc_known  = false;
static struct sockaddr_in s_pc_addr;
static uint8_t           s_own_mac[6] = {0};   // 本机 WiFi MAC (唯一设备标识)

/* 模式 + 角色状态 */
static volatile csi_mode_t s_mode = MODE_MULTI;
static volatile csi_role_t s_role = ROLE_IDLE;
static uint8_t             s_single_tx_sender_mac[6] = {0};  /* 单发模式: 发送端 MAC (接收端按此过滤) */
static EventGroupHandle_t  s_role_evt = NULL;
static bool                s_espnow_ready = false;
static volatile uint32_t   s_espnow_freq_hz = 100;             /* 单发模式: 发送端 ESP-NOW 广播频率 */
static TaskHandle_t        s_espnow_sender_task_handle = NULL;

/* ===================== AP 配置模式 (fallback) =====================
 * 当 NVS 里的 WiFi 配置在 25 秒内连不上时, 启动一个 AP 模式热点
 * (SSID="csi_setup", 默认密码 "csi12345"), 同时在端口 8899 提供 TCP
 * 控制通道, 接受 WIFI_CONFIG <ssid> <pwd> 命令. 这样不管 ESP 换了
 * 什么环境, 都能被电脑 / 手机发现并配网, 彻底解决"配网死锁". */
#define AP_FALLBACK_TIMEOUT_MS 5000
#define AP_CONFIG_SSID         "csi_setup"
#define AP_CONFIG_PASS         "csi12345"
#define AP_CONFIG_PORT         8899           // AP 模式下的 TCP 端口
static int               s_udp_sock = -1;
static int               s_tcp_listen_sock = -1;
static int               s_tcp_client = -1;
static QueueHandle_t     s_csi_queue;
static EventGroupHandle_t s_wifi_eg;
static SemaphoreHandle_t s_sock_mutex;
#define WIFI_OK_BIT BIT0

/* Ping 会话句柄 + 当前 ping 间隔 (ms): 采样频率 = 1000 / 间隔.
 * 采样频率通过本机主动 ping 路由器实现: 路由器回包到达即触发 CSI 回调,
 * 电脑端用 TCP 命令 SAMPLE_RATE <hz> 动态调整间隔. */
static esp_ping_handle_t s_ping = NULL;
static volatile uint32_t s_ping_interval_ms = PING_INTERVAL_MS;
static volatile bool    s_ping_active     = false;  // 当前是否在主动 ping (采样节奏)

/* csi_rx_cb 用这个时间窗判定 is_ping_reply:
 *   - start_ping() 时设为 esp_timer_get_time() + interval_us,
 *     即"下一次预期回包到达时间".
 *   - csi_rx_cb 命中时 += interval_us, 滑动到下一格, 不会因抖动误判.
 *   - 当前时刻超出 s_next_expected_reply_us + 3 个间隔, 自动重置. */
static volatile int64_t  s_next_expected_reply_us = 0;

/* ===================== WS2812 LED 状态灯 =====================
 * 板载 RGB 灯珠 (GPIO48), 通过 RMT 驱动:
 * - LED_MODE_OFF:    熄灭
 * - LED_MODE_WINDOW: 蓝灯闪烁 (窗口采集模式)
 * - LED_MODE_FREE:   绿灯闪烁 (自由采集模式)
 * - 连上 WiFi 但未发现电脑: 常亮白灯
 */
typedef enum { LED_MODE_OFF = 0, LED_MODE_WINDOW, LED_MODE_FREE, LED_MODE_WIFI_ON } led_mode_t;
static volatile led_mode_t s_led_mode = LED_MODE_OFF;
static rmt_channel_handle_t s_led_rmt_ch  = NULL;
static rmt_encoder_handle_t s_led_encoder = NULL;

/* WS2812 颜色: 3 字节 GRB (不是 RGB) */
static inline void ws2812_set_color(uint8_t g, uint8_t r, uint8_t b) {
    if (!s_led_rmt_ch || !s_led_encoder) return;
    uint8_t grb[3] = { g, r, b };
    rmt_transmit_config_t tx_cfg = { .loop_count = 0 };
    rmt_transmit(s_led_rmt_ch, s_led_encoder, grb, sizeof(grb), &tx_cfg);
    rmt_tx_wait_all_done(s_led_rmt_ch, 10);
    /* WS2812 reset >50us: 1ms 延时足够 */
    vTaskDelay(pdMS_TO_TICKS(1));
}

static void led_task(void *arg) {
    bool on = false;
    while (1) {
        led_mode_t mode = s_led_mode;
        if (mode == LED_MODE_OFF) {
            ws2812_set_color(0, 0, 0);
            vTaskDelay(pdMS_TO_TICKS(500));
        } else if (mode == LED_MODE_WIFI_ON) {
            ws2812_set_color(50, 50, 50);  /* 常亮白灯 (亮度低) */
            vTaskDelay(pdMS_TO_TICKS(500));
        } else {
            /* 闪烁 */
            on = !on;
            if (mode == LED_MODE_WINDOW) {
                /* 蓝灯闪烁 */
                ws2812_set_color(0, 0, on ? 80 : 0);
            } else if (mode == LED_MODE_FREE) {
                /* 绿灯闪烁 */
                ws2812_set_color(on ? 80 : 0, 0, 0);
            }
            vTaskDelay(pdMS_TO_TICKS(300));
        }
    }
}

static void led_init(void) {
    rmt_tx_channel_config_t tx_cfg = {
        .clk_src          = RMT_CLK_SRC_DEFAULT,
        .gpio_num         = LED_GPIO,
        .mem_block_symbols = 64,
        .resolution_hz    = 10 * 1000 * 1000,  /* 10 MHz → 100ns/tick */
    };
    rmt_new_tx_channel(&tx_cfg, &s_led_rmt_ch);

    /* bytes encoder: bit0 = 0.4us高 + 0.8us低, bit1 = 0.8us高 + 0.4us低 */
    rmt_bytes_encoder_config_t enc_cfg = {
        .bit0 = { .level0 = 1, .duration0 = 4,  /* 0.4us 高 */
                  .level1 = 0, .duration1 = 8 }, /* 0.8us 低 */
        .bit1 = { .level0 = 1, .duration0 = 8,  /* 0.8us 高 */
                  .level1 = 0, .duration1 = 4 }, /* 0.4us 低 */
        .flags.msb_first = 1,
    };
    rmt_new_bytes_encoder(&enc_cfg, &s_led_encoder);
    rmt_enable(s_led_rmt_ch);
    ws2812_set_color(0, 0, 0);
}

/* 前向声明: tcp_server_task 中的 SAMPLE_RATE 命令需要动态重启 ping */
static void stop_ping(void);
static void start_ping(void);

/* 前向声明: MODE 命令相关 (单发多收 / 多发多收 模式切换) */
static void mode_become_multi(void);
static void mode_become_single_tx(const uint8_t *sender_mac);
static void mode_release_role(void);
static bool parse_mac_string(char *s, uint8_t *out_mac);

/* ===================== TCP 命令处理 ===================== */

static void tcp_send(const char *msg) {
    if (s_tcp_client < 0) return;
    xSemaphoreTake(s_sock_mutex, portMAX_DELAY);
    if (s_tcp_client >= 0) {
        send(s_tcp_client, msg, strlen(msg), 0);
    }
    xSemaphoreGive(s_sock_mutex);
}

/* 前向声明: process_command 引用下面定义的 NVS 函数 */
static bool save_wifi_config(const char *ssid, const char *pass);
static bool save_sample_rate(int rate);

static void tcp_send_json(const char *fmt, ...) {
    char buf[512];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    tcp_send(buf);
}

static void tcp_send_status(void) {
    const char *mode_s = (s_mode == MODE_SINGLE_TX) ? "single" : "multi";
    const char *role_s = (s_role == ROLE_SENDER) ? "sender"
                        : (s_role == ROLE_RECEIVER) ? "receiver" : "idle";
    char mac_s[20] = {0};
    if (s_mode == MODE_SINGLE_TX) {
        snprintf(mac_s, sizeof(mac_s), "%02X:%02X:%02X:%02X:%02X:%02X",
                 s_single_tx_sender_mac[0], s_single_tx_sender_mac[1],
                 s_single_tx_sender_mac[2], s_single_tx_sender_mac[3],
                 s_single_tx_sender_mac[4], s_single_tx_sender_mac[5]);
    }
    /* 上报本机 MAC: 电脑端在"一发多收"模式下据此指定/确认发送端,
     * 无需等 CSI 数据回流即可拿到 MAC (CSI 包 mac 字段同为 s_own_mac). */
    char own_mac_s[20] = {0};
    snprintf(own_mac_s, sizeof(own_mac_s), "%02X:%02X:%02X:%02X:%02X:%02X",
             s_own_mac[0], s_own_mac[1], s_own_mac[2],
             s_own_mac[3], s_own_mac[4], s_own_mac[5]);
    tcp_send_json(
        "{\"type\":\"status\",\"packets\":%lu,\"rx\":%lu,\"streaming\":%s,"
        "\"mode\":\"%s\",\"role\":\"%s\",\"sender_mac\":\"%s\",\"mac\":\"%s\"}\n",
        s_total_packets, s_udp_rx_cnt, s_streaming ? "true" : "false",
        mode_s, role_s, mac_s, own_mac_s);
}

/* ============ 命令分发 (TCP 和 UART 共用) ============ */
static volatile bool s_cmd_should_restart = false;

static void process_command(char *raw, const char *source) {
    char *trimmed = raw;
    while (*trimmed == ' ' || *trimmed == '\t' || *trimmed == '\n' || *trimmed == '\r') trimmed++;
    char *end = trimmed + strlen(trimmed) - 1;
    while (end > trimmed && (*end == '\n' || *end == '\r' || *end == ' ')) *end-- = '\0';

    ESP_LOGI(TAG, "%s 命令: %s", source, trimmed);

    if (strcmp(trimmed, "RESTART") == 0 || strcmp(trimmed, "REBOOT") == 0) {
        tcp_send("{\"type\":\"ok\",\"cmd\":\"restarting\"}\n");
        s_cmd_should_restart = true;
        /* TCP: 立即关闭, 让调用者延后重启; UART: 设置标志, 由 uart_task 重启 */
    } else if (strcmp(trimmed, "STATUS") == 0 || strcmp(trimmed, "STATUS\n") == 0) {
        tcp_send_status();
    } else if (strncmp(trimmed, "PING", 4) == 0) {
        tcp_send_json("{\"type\":\"pong\",\"uptime\":%lu}\n", s_total_packets);
    } else if (strncmp(trimmed, "WIFI_CONFIG ", 12) == 0) {
        /* 格式: WIFI_CONFIG <ssid> <password> (SSID 不能含空格, 密码可为空) */
        char *rest = trimmed + 12;
        char *sp = strchr(rest, ' ');
        if (sp == rest) {
            tcp_send_json("{\"type\":\"error\",\"msg\":\"wifi_config_format\"}\n");
            return;
        }
        if (sp != NULL) *sp = '\0';
        if (save_wifi_config(rest, sp ? sp + 1 : "")) {
            ESP_LOGI(TAG, "WiFi 已保存, 即将重启连接: %s", rest);
            tcp_send("{\"type\":\"ok\",\"cmd\":\"wifi_config\"}\n");
            s_cmd_should_restart = true;
        } else {
            tcp_send_json("{\"type\":\"error\",\"msg\":\"nvs_fail\"}\n");
        }
    } else if (strncmp(trimmed, "LED_MODE ", 9) == 0) {
        /* 格式: LED_MODE WINDOW | LED_MODE FREE | LED_MODE OFF */
        char *mode_str = trimmed + 9;
        if (strcmp(mode_str, "WINDOW") == 0) {
            s_led_mode = LED_MODE_WINDOW;
            ESP_LOGI(TAG, "LED: 窗口采集模式 (蓝灯闪烁)");
            tcp_send_json("{\"type\":\"ok\",\"cmd\":\"led_mode\",\"mode\":\"window\"}\n");
        } else if (strcmp(mode_str, "FREE") == 0) {
            s_led_mode = LED_MODE_FREE;
            ESP_LOGI(TAG, "LED: 自由采集模式 (绿灯闪烁)");
            tcp_send_json("{\"type\":\"ok\",\"cmd\":\"led_mode\",\"mode\":\"free\"}\n");
        } else if (strcmp(mode_str, "OFF") == 0) {
            s_led_mode = LED_MODE_OFF;
            ESP_LOGI(TAG, "LED: 关闭");
            tcp_send_json("{\"type\":\"ok\",\"cmd\":\"led_mode\",\"mode\":\"off\"}\n");
        } else {
            tcp_send_json("{\"type\":\"error\",\"msg\":\"led_mode_unknown\"}\n");
        }
    } else if (strncmp(trimmed, "SAMPLE_RATE", 11) == 0) {
        /* 格式: SAMPLE_RATE <hz>
         *   > 0 : 主动 ping 路由器, 采样频率 = hz (立即生效, NVS 持久化)
         *   = 0 : 停止主动 ping, CSI 由路由器广播帧触发
         * 若只发 "SAMPLE_RATE" (无参数), 上报当前频率
         */
        char *arg = trimmed + 11;
        while (*arg == ' ' || *arg == '\t') arg++;
        if (*arg == '\0') {
            /* 仅查询当前频率 */
            int cur = s_ping_active ? (1000 / (int)s_ping_interval_ms) : 0;
            tcp_send_json("{\"type\":\"sample_rate\",\"rate\":%d,\"active\":%s}\n",
                          cur, s_ping_active ? "true" : "false");
        } else {
            int rate = atoi(arg);
            if (rate < 0 || rate > SAMPLE_RATE_MAX) {
                tcp_send_json("{\"type\":\"error\",\"msg\":\"sample_rate_range\"}\n");
                return;
            }
            if (!save_sample_rate(rate)) {
                tcp_send_json("{\"type\":\"error\",\"msg\":\"nvs_fail\"}\n");
                return;
            }
            /* 先停止旧 ping (如果存在) */
            stop_ping();
            if (rate > 0) {
                s_ping_interval_ms = 1000 / rate;
                start_ping();
                ESP_LOGI(TAG, "采样频率已设为 %d Hz (ping 间隔 %lu ms)",
                         rate, (unsigned long)s_ping_interval_ms);
                tcp_send_json("{\"type\":\"ok\",\"cmd\":\"sample_rate\",\"rate\":%d}\n", rate);
            } else {
                ESP_LOGI(TAG, "主动 ping 已停止, CSI 由广播帧触发");
                tcp_send_json("{\"type\":\"ok\",\"cmd\":\"sample_rate\",\"rate\":0}\n");
            }
            /* 单发模式下: 发送端用 ESP-NOW 广播, 同步频率 */
            s_espnow_freq_hz = (rate > 0) ? (uint32_t)rate : s_espnow_freq_hz;
        }
    } else if (strncmp(trimmed, "MODE", 4) == 0) {
        /* 格式:
         *   MODE MULTI                -> 切换为多发多收模式
         *   MODE SINGLE <sender_mac>  -> 切换为单发多收模式, 自己是发送端
         *                                或仅作为接收端按 <sender_mac> 过滤.
         *   MODE SINGLE               -> 退出发送/接收角色, 保留 s_mode 不动
         *   MODE STATUS               -> 查询当前 mode / role / sender_mac
         * MAC 格式: AA:BB:CC:DD:EE:FF (冒号分隔) 或 AABBCCDDEEFF.
         */
        char *arg = trimmed + 4;
        while (*arg == ' ' || *arg == '\t') arg++;
        if (*arg == '\0') {
            tcp_send_json("{\"type\":\"error\",\"msg\":\"mode_arg_missing\"}\n");
            return;
        }
        /* 解析第一段 (MULTI / SINGLE / STATUS) */
        char *p = arg;
        while (*p && *p != ' ' && *p != '\t') p++;
        char first_word[16] = {0};
        size_t fw_len = (size_t)(p - arg);
        if (fw_len >= sizeof(first_word)) fw_len = sizeof(first_word) - 1;
        memcpy(first_word, arg, fw_len);
        first_word[fw_len] = '\0';

        if (strcmp(first_word, "MULTI") == 0) {
            mode_become_multi();
            tcp_send_json("{\"type\":\"ok\",\"cmd\":\"mode\",\"mode\":\"multi\"}\n");
        } else if (strcmp(first_word, "SINGLE") == 0) {
            /* 解析 sender MAC (若提供) */
            while (*p == ' ' || *p == '\t') p++;
            uint8_t sender_mac[6] = {0};
            if (*p == '\0') {
                /* 不带 MAC: 仅退出发送/接收, mode 保持原状 */
                mode_release_role();
                tcp_send_json("{\"type\":\"ok\",\"cmd\":\"mode\",\"mode\":\"single_idle\"}\n");
                return;
            }
            if (!parse_mac_string(p, sender_mac)) {
                tcp_send_json("{\"type\":\"error\",\"msg\":\"mode_mac_format\"}\n");
                return;
            }
            mode_become_single_tx(sender_mac);
            tcp_send_json("{\"type\":\"ok\",\"cmd\":\"mode\",\"mode\":\"single\",\"role\":\"%s\"}\n",
                          (s_role == ROLE_SENDER) ? "sender" : "receiver");
        } else if (strcmp(first_word, "STATUS") == 0) {
            const char *mode_s = (s_mode == MODE_SINGLE_TX) ? "single" : "multi";
            const char *role_s = (s_role == ROLE_SENDER) ? "sender"
                                : (s_role == ROLE_RECEIVER) ? "receiver" : "idle";
            char mac_s[20] = {0};
            if (s_mode == MODE_SINGLE_TX) {
                snprintf(mac_s, sizeof(mac_s), "%02X:%02X:%02X:%02X:%02X:%02X",
                         s_single_tx_sender_mac[0], s_single_tx_sender_mac[1],
                         s_single_tx_sender_mac[2], s_single_tx_sender_mac[3],
                         s_single_tx_sender_mac[4], s_single_tx_sender_mac[5]);
            }
            tcp_send_json(
                "{\"type\":\"mode_status\",\"mode\":\"%s\",\"role\":\"%s\","
                "\"sender_mac\":\"%s\"}\n",
                mode_s, role_s, mac_s);
        } else {
            tcp_send_json("{\"type\":\"error\",\"msg\":\"mode_unknown\"}\n");
        }
    } else {
        tcp_send_json("{\"type\":\"error\",\"msg\":\"unknown_cmd\"}\n");
    }
}

/* ===================== WiFi 配置 (NVS 持久化) =====================
 * 电脑端可通过 TCP 命令 WIFI_CONFIG <ssid> <password> 写入并重启,
 * 下次启动时连接新 WiFi, 无需重新烧录固件.
 * 注意: SSID 不能含空格 (用空格分隔), 密码可以含空格.
 */

static bool load_wifi_config(char *ssid, size_t ssid_len,
                             char *pass, size_t pass_len) {
    nvs_handle_t h;
    if (nvs_open("wifi", NVS_READONLY, &h) != ESP_OK) return false;
    size_t len = ssid_len;
    esp_err_t r1 = nvs_get_str(h, "ssid", ssid, &len);
    len = pass_len;
    esp_err_t r2 = nvs_get_str(h, "pass", pass, &len);
    nvs_close(h);
    return (r1 == ESP_OK && r2 == ESP_OK);
}

static bool save_wifi_config(const char *ssid, const char *pass) {
    nvs_handle_t h;
    if (nvs_open("wifi", NVS_READWRITE, &h) != ESP_OK) return false;
    esp_err_t r = nvs_set_str(h, "ssid", ssid);
    if (r == ESP_OK) r = nvs_set_str(h, "pass", pass);
    if (r == ESP_OK) r = nvs_commit(h);
    nvs_close(h);
    return (r == ESP_OK);
}

/* ===================== 采样频率 (NVS 持久化) =====================
 * 采样频率由"主动 ping 路由器的间隔"决定, 电脑端 SAMPLE_RATE <hz> 命令
 * 会修改间隔并写入 NVS, 重启后仍保持上次设定的频率.
 */

static int load_sample_rate(void) {
    nvs_handle_t h;
    int32_t rate = 0;
    if (nvs_open("sampling", NVS_READONLY, &h) != ESP_OK) return DEFAULT_SAMPLE_RATE;
    esp_err_t r = nvs_get_i32(h, "rate_hz", &rate);
    nvs_close(h);
    /* 0 = 用户上次选择了"停止主动 ping", 开机保持暂停状态, 不主动 ping */
    if (r == ESP_OK && rate == 0) return 0;
    if (r != ESP_OK || rate < SAMPLE_RATE_MIN || rate > SAMPLE_RATE_MAX) {
        return DEFAULT_SAMPLE_RATE;
    }
    return (int)rate;
}

static bool save_sample_rate(int rate) {
    nvs_handle_t h;
    if (nvs_open("sampling", NVS_READWRITE, &h) != ESP_OK) return false;
    esp_err_t r = nvs_set_i32(h, "rate_hz", rate);
    if (r == ESP_OK) r = nvs_commit(h);
    nvs_close(h);
    return (r == ESP_OK);
}


/* ===================== TCP 服务器任务 ===================== */

/* UART (串口) 命令接收: 用于 ESP 因为保存了错误 WiFi 而连不上时, 通过 USB 串口直接下发配置.
 * 与 TCP 共用 process_command 处理逻辑. 收到的行以 \n 结尾, 缓冲 128 字节够用. */
#define UART_CMD_PORT  UART_NUM_0
#define UART_CMD_BAUD  115200

static void uart_cmd_task(void *arg) {
    uart_config_t ucfg = {
        .baud_rate  = UART_CMD_BAUD,
        .data_bits  = UART_DATA_8_BITS,
        .parity     = UART_PARITY_DISABLE,
        .stop_bits  = UART_STOP_BITS_1,
        .flow_ctrl  = UART_HW_FLOWCTRL_DISABLE,
        .source_clk = UART_SCLK_DEFAULT,
    };
    uart_driver_install(UART_CMD_PORT, 1024, 256, 0, NULL, 0);
    uart_param_config(UART_CMD_PORT, &ucfg);

    char line[160];
    int pos = 0;
    char ch;
    while (1) {
        int n = uart_read_bytes(UART_CMD_PORT, &ch, 1, pdMS_TO_TICKS(500));
        if (n != 1) continue;
        if (ch == '\n' || ch == '\r') {
            if (pos == 0) continue;
            line[pos] = '\0';
            process_command(line, "UART");
            pos = 0;
            if (s_cmd_should_restart) {
                /* 给应用日志一点时间刷出去 */
                vTaskDelay(pdMS_TO_TICKS(500));
                esp_restart();
                /* 不返回 */
            }
        } else if (pos < (int)sizeof(line) - 1) {
            line[pos++] = ch;
        }
    }
}

/* ===================== AP 配置模式 (fallback) =====================
 * 当 STA 模式下 NVS 里的 WiFi 在 AP_FALLBACK_TIMEOUT_MS 内连不上时,
 * 进入配置模式: 启动 AP + STA 双模, 电脑 / 手机连到 csi_setup 热点
 * 后会被 Captive Portal 自动弹出配网页面. */

static void ap_config_task(void *arg);

/* 进入 AP fallback 后, 阻止 wifi_handler 再触发 esp_wifi_connect().
 * 因为 STA 反复重连会抢 WiFi 时间片, 干扰手机连接 AP (AP 与 STA 共享同一
 * 2.4GHz 射频, 切换信道会让手机连接的 AP 瞬间消失).
 * 配网成功后用户提交新 WiFi 后, ESP 会 esp_restart() 重新走正常 STA 流程. */
static volatile bool s_in_ap_fallback = false;

/* ============ Captive Portal + WiFi 配网页面 ============
 *
 * 原理: 手机/电脑连上 csi_setup 热点时, 系统会自动探测网络连通性:
 *   - iOS/macOS 访问 http://captive.apple.com/hotspot-detect.html
 *   - Android 访问 http://connectivitycheck.gstatic.com/generate_204
 *   - Windows 访问 http://www.msftconnecttest.com/connecttest.txt
 * 只要 ESP 把这些探测 URL 重定向到 192.168.4.1 的 / 配网页面,
 * 系统就会自动弹出浏览器并显示配网页面, 用户无需手动输入 IP.
 *
 * 配网页面包含一个简单的 HTML 表单, 用户填写 SSID + 密码后提交,
 * ESP 解析 POST 数据 -> 保存 NVS -> esp_restart() 重启连接新 WiFi.
 */

/* 内嵌的 HTML 配网页面 (ROM, 无文件系统依赖) */
static const char INDEX_HTML[] =
"<!DOCTYPE html><html lang=zh><head>"
"<meta charset=utf-8>"
"<meta name=viewport content=\"width=device-width,initial-scale=1\">"
"<title>ESP32 CSI 配网</title>"
"<style>"
"body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;"
"  max-width:420px;margin:40px auto;padding:24px;background:#fafafa;color:#222}"
"h1{font-size:22px;margin:0 0 18px;color:#1976d2;text-align:center}"
"label{display:block;margin:14px 0 6px;font-size:14px;color:#555}"
"input[type=text],input[type=password]{width:100%;padding:10px 12px;"
"  font-size:16px;border:1px solid #ccc;border-radius:6px;box-sizing:border-box}"
"button{margin-top:22px;width:100%;padding:12px;font-size:16px;"
"  background:#1976d2;color:#fff;border:0;border-radius:6px;cursor:pointer}"
"button:hover{background:#1565c0}"
".tip{font-size:12px;color:#888;margin-top:18px;text-align:center}"
"</style></head><body>"
"<h1>ESP32 CSI 配网</h1>"
"<form method=POST action=/save>"
"<label>WiFi 名称 (SSID)</label>"
"<input name=ssid type=text required maxlength=31 placeholder=请输入当前环境的 WiFi 名称>"
"<label>WiFi 密码 (可空)</label>"
"<input name=password type=password maxlength=63 placeholder=没有密码则留空>"
"<button type=submit>保存并连接</button>"
"</form>"
"<p class=tip>设备将在收到新配置后自动重启并连接新 WiFi</p>"
"</body></html>";

/* URL 解码 (form-urlencoded): 把 %XX 转字节, + 转空格. 原地解码, 限制最大长度. */
static int url_decode_inplace(char *s) {
    char *r = s, *w = s;
    while (*r) {
        if (*r == '+') { *w++ = ' '; r++; }
        else if (*r == '%' && r[1] && r[2]) {
            char hex[3] = { r[1], r[2], 0 };
            *w++ = (char)strtol(hex, NULL, 16);
            r += 3;
        } else { *w++ = *r++; }
    }
    *w = '\0';
    return (int)(w - s);
}

/* 从 form-urlencoded body 中取键值: key=value&key=value ...
 * 把 key 对应值拷贝到 out (限定 out_sz), 找不到返回 false. */
static bool form_get_value(const char *body, const char *key, char *out, size_t out_sz) {
    if (!body || !key || !out || out_sz == 0) return false;
    size_t klen = strlen(key);
    const char *p = body;
    while (*p) {
        if (strncmp(p, key, klen) == 0 && (p[klen] == '=' || p[klen] == '\0')) {
            if (p[klen] != '=') return false;
            const char *v = p + klen + 1;
            const char *e = strchr(v, '&');
            size_t n = e ? (size_t)(e - v) : strlen(v);
            if (n >= out_sz) n = out_sz - 1;
            memcpy(out, v, n); out[n] = '\0';
            url_decode_inplace(out);
            return true;
        }
        const char *amp = strchr(p, '&');
        if (!amp) break;
        p = amp + 1;
    }
    return false;
}

/* GET / -> 配网页面 */
static esp_err_t http_get_index(httpd_req_t *req) {
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");
    return httpd_resp_send(req, INDEX_HTML, HTTPD_RESP_USE_STRLEN);
}

/* 用新凭证尝试连接 STA, timeout_sec 秒内拿到 IP 则返回 true;
 * 失败返回 false (已自动 esp_wifi_disconnect). 复用 app_main 已建好的
 * s_wifi_eg/WIFI_OK_BIT. */
static bool try_connect_sta(const char *ssid, const char *pwd, int timeout_sec) {
    wifi_config_t sta_cfg = {0};
    strncpy((char *)sta_cfg.sta.ssid, ssid, sizeof(sta_cfg.sta.ssid) - 1);
    if (pwd && pwd[0]) {
        strncpy((char *)sta_cfg.sta.password, pwd, sizeof(sta_cfg.sta.password) - 1);
    }
    /* 清掉之前成功标志, 等待本次新的 IP 事件 */
    xEventGroupClearBits(s_wifi_eg, WIFI_OK_BIT);

    esp_wifi_disconnect();
    esp_err_t ret = esp_wifi_set_config(WIFI_IF_STA, &sta_cfg);
    if (ret != ESP_OK) {
        ESP_LOGE(TAG, "try_connect_sta: set_config 失败: %s", esp_err_to_name(ret));
        return false;
    }
    esp_wifi_connect();

    EventBits_t bits = xEventGroupWaitBits(
        s_wifi_eg, WIFI_OK_BIT, pdFALSE, pdTRUE,
        pdMS_TO_TICKS(timeout_sec * 1000));

    if (bits & WIFI_OK_BIT) {
        ESP_LOGI(TAG, "STA 连接成功 (ssid=%s)", ssid);
        return true;
    }
    ESP_LOGW(TAG, "STA 连接超时 (ssid=%s, %d 秒)", ssid, timeout_sec);
    esp_wifi_disconnect();
    return false;
}

/* POST /save handler: 见下方 (validate_and_finalize_task 在前, 真正的
 * handler 在后, 这样阅读时能看到 task 的接口). */

/* 配网后台任务参数: 不能用栈变量, 因为 http_post_save 返回后栈就释放了 */
typedef struct {
    char ssid[64];
    char pwd[64];
} save_creds_t;

/* 后台任务: 用新凭证真正去连一次 STA.
 *  - 成功 -> 等 2s 让浏览器渲染页面, 然后 esp_restart() 进 STA 模式
 *  - 失败 -> 留在 AP 模式, 用户可再次连 csi_setup 重新配 */
static void validate_and_finalize_task(void *arg) {
    save_creds_t *c = (save_creds_t *)arg;
    ESP_LOGI(TAG, "[validate] 启动后台验证: ssid='%s'", c->ssid);

    bool ok = try_connect_sta(c->ssid, c->pwd, 15);
    free(c);

    if (ok) {
        esp_netif_t *sta_netif = esp_netif_get_handle_from_ifkey("WIFI_STA_DEF");
        if (sta_netif) {
            esp_netif_ip_info_t ip;
            if (esp_netif_get_ip_info(sta_netif, &ip) == ESP_OK) {
                ESP_LOGI(TAG, "[validate] STA 连接成功, IP=" IPSTR ", 准备重启",
                         IP2STR(&ip.ip));
            }
        }
        vTaskDelay(pdMS_TO_TICKS(2000));    // 让已经回发的页面有时间渲染
        ESP_LOGI(TAG, "[validate] 重启进入 STA 模式");
        esp_restart();
    } else {
        ESP_LOGW(TAG, "[validate] STA 连接失败, 留在 AP 模式, 用户可重试");
        /* 任务结束, AP server 还在跑, 用户重新连 csi_setup 即可 */
    }
    vTaskDelete(NULL);
}

/* POST /save -> 解析表单 -> 保存 NVS -> 立即返回"已保存"页 (关键!) ->
 * 后台任务异步验证连接 -> 成功则重启, 失败则继续留在 AP 模式.
 *
 * 关键设计: 之前在 HTTP handler 内阻塞 15s 等 try_connect_sta 结果,
 * 会导致手机浏览器等不到响应 (TCP 超时) 或者 AP 在切换 STA 时抖动
 * 让 TCP 断开, 用户什么都看不到. 现在是立即响应 + 后台验证, 体验稳. */
static esp_err_t http_post_save(httpd_req_t *req) {
    char buf[256] = {0};
    int n = httpd_req_recv(req, buf, sizeof(buf) - 1);
    if (n <= 0) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "no body");
        return ESP_OK;
    }
    buf[n] = '\0';

    char ssid[64] = {0};
    char pwd[64] = {0};
    if (!form_get_value(buf, "ssid", ssid, sizeof(ssid)) || ssid[0] == '\0') {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "missing ssid");
        return ESP_OK;
    }
    form_get_value(buf, "password", pwd, sizeof(pwd));   // 密码可为空

    ESP_LOGI(TAG, "配网页面 POST: ssid='%s' pwd_len=%d", ssid, (int)strlen(pwd));

    if (!save_wifi_config(ssid, pwd)) {
        /* NVS 保存失败: 立即返回错误页 (不需要等后台) */
        const char *fail =
            "<!DOCTYPE html><html lang=zh><head>"
            "<meta charset=utf-8><meta name=viewport content=\"width=device-width,initial-scale=1\">"
            "<title>保存失败</title>"
            "<style>body{font-family:sans-serif;max-width:420px;margin:60px auto;padding:24px;text-align:center;color:#222}"
            "h1{color:#e53935}.btn{display:inline-block;margin-top:24px;padding:12px 32px;background:#1976d2;color:#fff;text-decoration:none;border-radius:6px}</style>"
            "</head><body>"
            "<h1>保存失败</h1>"
            "<p>NVS 写入失败, 请重试.</p>"
            "<a class=btn href='/'>返回</a>"
            "</body></html>";
        httpd_resp_set_type(req, "text/html; charset=utf-8");
        httpd_resp_send(req, fail, HTTPD_RESP_USE_STRLEN);
        return ESP_OK;
    }

    /* NVS 保存成功 -> 把 ssid/pwd 拷到堆, 交给后台任务验证连接.
     * 这里 http_post_save 立即返回, 浏览器能立刻看到"已保存"页面. */
    save_creds_t *c = malloc(sizeof(save_creds_t));
    if (!c) {
        ESP_LOGE(TAG, "malloc 失败, 无法启动后台验证");
        httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "OOM");
        return ESP_OK;
    }
    strncpy(c->ssid, ssid, sizeof(c->ssid) - 1);
    c->ssid[sizeof(c->ssid) - 1] = '\0';
    strncpy(c->pwd, pwd, sizeof(c->pwd) - 1);
    c->pwd[sizeof(c->pwd) - 1] = '\0';

    if (xTaskCreate(validate_and_finalize_task, "wifi_validate",
                    4096, c, 4, NULL) != pdPASS) {
        ESP_LOGE(TAG, "xTaskCreate 失败");
        free(c);
        httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "task fail");
        return ESP_OK;
    }

    /* 立即返回"已保存"页面, 不要再阻塞等连接结果.
     * 提示用户去 WiFi 列表观察 csi_setup 是否消失来判断成功/失败. */
    char html[2048];
    snprintf(html, sizeof(html),
        "<!DOCTYPE html><html lang=zh><head>"
        "<meta charset=utf-8><meta name=viewport content=\"width=device-width,initial-scale=1\">"
        "<title>已保存</title>"
        "<style>"
        "body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;max-width:420px;margin:40px auto;padding:24px;text-align:center;color:#222}"
        "h1{color:#1976d2;font-size:22px;margin:0 0 10px}"
        ".spinner{display:inline-block;width:36px;height:36px;border:3px solid #ddd;border-top-color:#1976d2;border-radius:50%%;animation:s 1s linear infinite;margin:14px 0}"
        "@keyframes s{to{transform:rotate(360deg)}}"
        ".net{font-size:15px;margin:8px 0 18px}"
        ".tip{font-size:13px;color:#555;line-height:1.75;text-align:left;background:#f5f9ff;padding:14px;border-radius:8px;margin-top:18px}"
        ".tip b{color:#1976d2}"
        ".ok{color:#43a047;font-weight:bold}"
        ".fail{color:#e53935;font-weight:bold}"
        "</style></head><body>"
        "<h1>\xe2\x9c\x93 已保存</h1>"
        "<div class=spinner></div>"
        "<p class=net>网络: <b>%s</b></p>"
        "<p style=font-size:14px;color:#666>设备正在尝试连接, 约 15 秒内出结果</p>"
        "<div class=tip>"
        "<b>如何判断结果 (无需停留此页):</b><br><br>"
        "<span class=ok>\xe2\x9c\x93 成功</span> &rarr; 手机 WiFi 列表中 <b>csi_setup</b> 热点消失<br>"
        "&nbsp;&nbsp;&nbsp;&nbsp;这时请重新连接原来的 WiFi, 打开 CSI 采集工具即可发现此设备.<br><br>"
        "<span class=fail>\xe2\x9c\x97 失败</span> &rarr; <b>csi_setup</b> 热点仍然存在<br>"
        "&nbsp;&nbsp;&nbsp;&nbsp;请检查密码 / 2.4GHz / 信号后, 重新连接 csi_setup 再次配网."
        "</div>"
        "</body></html>",
        ssid);
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    httpd_resp_send(req, html, HTTPD_RESP_USE_STRLEN);
    return ESP_OK;
}

/* Captive Portal: 把各种探测 URL 都 302 到 / */
static esp_err_t http_captive_redirect(httpd_req_t *req) {
    httpd_resp_set_status(req, "302 Found");
    httpd_resp_set_hdr(req, "Location", "http://192.168.4.1/");
    /* iOS 探测期望 200, 这里同时兼容: 用 GET 302 还是会触发浏览器跳转 */
    return httpd_resp_send(req, NULL, 0);
}

/* /generate_204: Android 探测, 返回 204 不带 Location 才能让 Android 认为是无网络,
 * 但要让浏览器打开登录页需要 302. 这里返回 302 触发弹出登录页. */
static esp_err_t http_generate_204(httpd_req_t *req) {
    return http_captive_redirect(req);
}

/* /hotspot-detect.html: iOS 探测 */
static esp_err_t http_apple_hotspot_detect(httpd_req_t *req) {
    /* iOS 在收到 200 + <HTML> 时会显示登录页; 直接返回配网页面也行 */
    httpd_resp_set_type(req, "text/html; charset=utf-8");
    return httpd_resp_send(req, INDEX_HTML, HTTPD_RESP_USE_STRLEN);
}

static esp_err_t http_404(httpd_req_t *req, httpd_err_code_t err) {
    /* 未注册的 URL 一律 302 到 /, 方便用户在浏览器直接输入 IP 或域名 */
    httpd_resp_set_status(req, "302 Found");
    httpd_resp_set_hdr(req, "Location", "http://192.168.4.1/");
    return httpd_resp_send(req, NULL, 0);
}

/* 启动 HTTP 服务器 (AP 模式下) */
static httpd_handle_t start_ap_http_server(void) {
    httpd_config_t cfg = HTTPD_DEFAULT_CONFIG();
    cfg.server_port = 80;                      // 标准 HTTP 端口, 浏览器直接打开
    cfg.max_uri_handlers = 8;
    cfg.lru_purge_enable = true;
    cfg.stack_size = 8192;                     // 配网页面较小, 不需要很大
    httpd_handle_t server = NULL;
    if (httpd_start(&server, &cfg) != ESP_OK) {
        ESP_LOGE(TAG, "AP 配置模式: HTTP 服务器启动失败");
        return NULL;
    }
    httpd_register_uri_handler(server, &(httpd_uri_t){
        .uri      = "/",
        .method   = HTTP_GET,
        .handler  = http_get_index,
    });
    httpd_register_uri_handler(server, &(httpd_uri_t){
        .uri      = "/save",
        .method   = HTTP_POST,
        .handler  = http_post_save,
    });
    httpd_register_uri_handler(server, &(httpd_uri_t){
        .uri      = "/generate_204",
        .method   = HTTP_GET,
        .handler  = http_generate_204,
    });
    httpd_register_uri_handler(server, &(httpd_uri_t){
        .uri      = "/hotspot-detect.html",
        .method   = HTTP_GET,
        .handler  = http_apple_hotspot_detect,
    });
    httpd_register_uri_handler(server, &(httpd_uri_t){
        .uri      = "/connecttest.txt",
        .method   = HTTP_GET,
        .handler  = http_captive_redirect,
    });
    httpd_register_uri_handler(server, &(httpd_uri_t){
        .uri      = "/ncsi.txt",
        .method   = HTTP_GET,
        .handler  = http_captive_redirect,
    });
    httpd_register_err_handler(server, HTTPD_404_NOT_FOUND, http_404);
    ESP_LOGI(TAG, "AP 配置模式: HTTP 服务器已启动 (端口 80)");
    return server;
}

/* ============ Captive Portal DNS 服务器 (UDP 53) ============
 *
 * 关键优化: 仅靠 OS 探测 URL 触发 captive portal 弹出不够可靠
 * (不同 OS/版本/厂商探测 URL 各异, 用户手动打开浏览器输入域名
 * 时若无 DNS 服务直接失败).
 *
 * 标准做法: 在 AP 上跑一个 DNS 服务器, 对所有 A 记录查询都返回
 * 192.168.4.1. 这样手机/电脑上任何 App/浏览器发起的 DNS 解析都
 * 会被劫持到 ESP, 然后由 HTTP 404 handler 把它们 302 到配网页面,
 * 实现真正的"连接热点即自动弹出浏览器登录页". */

#define DNS_PORT 53

/* 构造 DNS 响应: 在 resp 缓冲区就地构造后返回响应长度 (>0);
 * 解析失败返回 -1 (调用方丢弃). 对 A 记录查询返回 192.168.4.1,
 * 其他 QTYPE (含 AAAA) 返回 NODATA (header+question, ANCOUNT=0),
 * 让客户端回退到 A 查询. */
static int build_dns_response(const uint8_t *req, int req_len, uint8_t *resp) {
    if (req_len < 12) return -1;
    uint16_t qdcount = (req[4] << 8) | req[5];
    if (qdcount != 1) return -1;   // 只处理单问题查询

    /* 跳过 QNAME: 一系列 length-prefixed label, 0x00 结束, 也支持压缩指针 */
    int pos = 12;
    int used_pointer = 0;
    while (pos < req_len && req[pos] != 0) {
        if ((req[pos] & 0xC0) == 0xC0) { pos += 2; used_pointer = 1; break; }
        if (req[pos] > 63) return -1;   // 非法 label 长度
        pos += req[pos] + 1;
    }
    if (!used_pointer) {
        if (pos >= req_len) return -1;
        pos++;                          // 跳过终止 0x00
    }
    if (pos + 4 > req_len) return -1;
    uint16_t qtype = (req[pos] << 8) | req[pos + 1];
    pos += 4;
    int question_end = pos;

    /* 拷贝 header + question, 改写 flags + counts */
    memcpy(resp, req, question_end);
    resp[2] = 0x84;    // QR=1 (response), AA=1 (authoritative)
    resp[3] = 0x00;    // RA=0, RCODE=0
    resp[6] = resp[7] = 0;            // ANCOUNT = 0 (按需改)
    resp[8] = resp[9] = 0;            // NSCOUNT = 0
    resp[10] = resp[11] = 0;          // ARCOUNT = 0

    int out_len = question_end;
    if (qtype == 1) {                 // 仅 A 记录附 answer
        static const uint8_t answer[] = {
            0xC0, 0x0C,                 // NAME: 指针指向 question 起点
            0x00, 0x01,                 // TYPE: A
            0x00, 0x01,                 // CLASS: IN
            0x00, 0x00, 0x00, 0x3C,     // TTL: 60s
            0x00, 0x04,                 // RDLENGTH: 4
            192, 168, 4, 1,             // RDATA: 192.168.4.1
        };
        memcpy(resp + out_len, answer, sizeof(answer));
        out_len += sizeof(answer);
        resp[7] = 1;                    // ANCOUNT = 1
    }
    return out_len;
}

static void dns_server_task(void *arg) {
    int sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
    if (sock < 0) {
        ESP_LOGE(TAG, "DNS server: socket() 失败");
        vTaskDelete(NULL); return;
    }
    int yes = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    struct sockaddr_in addr = {
        .sin_family      = AF_INET,
        .sin_port        = htons(DNS_PORT),
        .sin_addr.s_addr = htonl(INADDR_ANY),
    };
    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        ESP_LOGE(TAG, "DNS server: bind 端口 %d 失败", DNS_PORT);
        close(sock);
        vTaskDelete(NULL); return;
    }
    ESP_LOGI(TAG, "Captive Portal DNS server 已启动 (UDP %d) -> 192.168.4.1", DNS_PORT);

    uint8_t buf[512], resp[512];
    while (1) {
        struct sockaddr_in from;
        socklen_t fl = sizeof(from);
        int n = recvfrom(sock, buf, sizeof(buf), 0,
                         (struct sockaddr *)&from, &fl);
        if (n < 12) continue;
        int rlen = build_dns_response(buf, n, resp);
        if (rlen > 0) {
            sendto(sock, resp, rlen, 0,
                   (struct sockaddr *)&from, fl);
        }
    }
    vTaskDelete(NULL);
}

static void enter_ap_fallback(void) {
    ESP_LOGW(TAG, "STA 连接超时, 进入 AP 配置模式 (SSID=%s, 密码 %s, channel=1)",
             AP_CONFIG_SSID, AP_CONFIG_PASS);

    /* 设置标志位: wifi_handler 不再触发 STA 重连 (避免跳信道打断 AP) */
    s_in_ap_fallback = true;

    /* 主动断开 STA, 停止扫描/重连 */
    esp_wifi_disconnect();

    /* 双模: STA + AP 共存 (这样如果新 WiFi 配置保存到 NVS, esp_restart 后
     * 会自动从 STA 起来, 不需要切模式) */
    esp_wifi_set_mode(WIFI_MODE_APSTA);

    /* 显式设 AP 在 channel 1: ESP-IDF 默认是 1, 但显式更稳妥.
     * 固定信道避免 STA 扫描时切换信道让手机连接 AP 的过程出现瞬断. */
    uint8_t primary_channel = 1;
    esp_wifi_set_channel(primary_channel, WIFI_SECOND_CHAN_NONE);

    /* 创建 AP 的 netif 并启动 DHCP server:
     * 之前 bug 是只有 STA netif, 切到 APSTA 后 AP 模式没有 DHCP, 手机连上后
     * 拿不到 IP, captive portal 自然弹不出. */
    esp_netif_t *ap_netif = esp_netif_create_default_wifi_ap();
    if (ap_netif == NULL) {
        ESP_LOGE(TAG, "创建 AP netif 失败");
    } else {
        /* 停掉 ESP-IDF 默认启动的 DHCP server (用 lwIP 默认配置可能与我们要的网段冲突),
         * 然后用我们自己的 IP 段 192.168.4.1 重启 DHCP. */
        esp_netif_dhcps_stop(ap_netif);

        esp_netif_ip_info_t ip_info = {
            .ip      = { .addr = ESP_IP4TOADDR(192, 168, 4, 1) },
            .gw      = { .addr = ESP_IP4TOADDR(192, 168, 4, 1) },
            .netmask = { .addr = ESP_IP4TOADDR(255, 255, 255, 0) },
        };
        esp_netif_set_ip_info(ap_netif, &ip_info);
        /* DHCP option 6 (DNS): 让手机把 192.168.4.1 当作 DNS 服务器,
         * DNS 劫持才能生效 (否则手机用 0.0.0.0 或上次的 DNS, 解析请求到不了 ESP). */
        uint8_t dns_ip[4] = {192, 168, 4, 1};
        esp_netif_dhcps_option(ap_netif, ESP_NETIF_OP_SET,
                               ESP_NETIF_DOMAIN_NAME_SERVER,
                               dns_ip, sizeof(dns_ip));
        esp_netif_dhcps_start(ap_netif);
        ESP_LOGI(TAG, "AP netif 已创建, IP=192.168.4.1, DHCP server 已启动");
    }

    wifi_config_t ap_cfg = { 0 };
    strncpy((char *)ap_cfg.ap.ssid,     AP_CONFIG_SSID, sizeof(ap_cfg.ap.ssid) - 1);
    strncpy((char *)ap_cfg.ap.password, AP_CONFIG_PASS, sizeof(ap_cfg.ap.password) - 1);
    ap_cfg.ap.ssid_len       = strlen(AP_CONFIG_SSID);
    ap_cfg.ap.channel        = primary_channel;
    ap_cfg.ap.max_connection = 4;
    ap_cfg.ap.authmode       = WIFI_AUTH_WPA2_PSK;
    esp_wifi_set_config(WIFI_IF_AP, &ap_cfg);

    /* 启动 HTTP 配网服务器 (Captive Portal 自动弹出) */
    start_ap_http_server();

    /* 启动 Captive Portal DNS 服务器 (UDP 53): 劫持所有域名解析到
     * 192.168.4.1, 让手机浏览器/App 任何 DNS 请求都被导向 ESP, 然后
     * 由 404 handler 302 到配网页面 -> 自动弹出登录页. */
    xTaskCreate(dns_server_task, "dns", 3072, NULL, 4, NULL);

    /* 兼容老逻辑: 保留 TCP 配置通道 (8899), 给不想用浏览器的用户用 */
    xTaskCreate(ap_config_task, "apcfg", 4096, NULL, 4, NULL);
}

static void ap_config_task(void *arg) {
    /* 监听 AP 端口: 接受 1 个客户端, 接收 1 行 WIFI_CONFIG 后重启 */
    int listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    if (listen_sock < 0) {
        ESP_LOGE(TAG, "AP 配置模式: 创建 socket 失败");
        vTaskDelete(NULL);
        return;
    }
    int yes = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port   = htons(AP_CONFIG_PORT),
        .sin_addr.s_addr = htonl(INADDR_ANY),
    };
    if (bind(listen_sock, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
        listen(listen_sock, 1) < 0) {
        ESP_LOGE(TAG, "AP 配置模式: 端口 %d 绑定失败", AP_CONFIG_PORT);
        close(listen_sock);
        vTaskDelete(NULL);
        return;
    }
    ESP_LOGI(TAG, "AP 配置模式: TCP 监听端口 %d (电脑/手机连上热点后访问此端口)",
             AP_CONFIG_PORT);

    char buf[256];
    while (1) {
        struct sockaddr_in from; socklen_t fl = sizeof(from);
        int client = accept(listen_sock, (struct sockaddr *)&from, &fl);
        if (client < 0) continue;
        ESP_LOGI(TAG, "AP 配置客户端连接: %s", inet_ntoa(from.sin_addr));

        int n = recv(client, buf, sizeof(buf) - 1, 0);
        if (n > 0) {
            buf[n] = '\0';
            process_command(buf, "AP");
            if (s_cmd_should_restart) {
                /* 给应答时间发出 */
                vTaskDelay(pdMS_TO_TICKS(300));
                close(client);
                close(listen_sock);
                esp_restart();
                /* 不返回 */
            }
        }
        close(client);
    }
}

static void tcp_server_task(void *arg) {
    struct sockaddr_in server_addr = {
        .sin_family = AF_INET,
        .sin_port = htons(TCP_PORT),
        .sin_addr.s_addr = htonl(INADDR_ANY),
    };

    s_tcp_listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    int reuse = 1;
    setsockopt(s_tcp_listen_sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    bind(s_tcp_listen_sock, (struct sockaddr *)&server_addr, sizeof(server_addr));
    listen(s_tcp_listen_sock, 1);
    ESP_LOGI(TAG, "TCP 控制端口 %d 监听中", TCP_PORT);

    char cmd_buf[128];
    struct sockaddr_in client_addr;
    socklen_t clen = sizeof(client_addr);

    while (1) {
        if (s_tcp_client >= 0) {
            // 已有客户端，检测是否断开
            char test;
            int r = recv(s_tcp_client, &test, 1, MSG_PEEK | MSG_DONTWAIT);
            if (r <= 0) {
                ESP_LOGI(TAG, "TCP 客户端断开");
                close(s_tcp_client);
                s_tcp_client = -1;
            } else {
                vTaskDelay(pdMS_TO_TICKS(100));
                continue;
            }
        }

        // 接受新连接
        int client = accept(s_tcp_listen_sock, (struct sockaddr *)&client_addr, &clen);
        if (client < 0) {
            vTaskDelay(pdMS_TO_TICKS(500));
            continue;
        }

        s_tcp_client = client;
        ESP_LOGI(TAG, "TCP 客户端连接: %s:%d",
                 inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

        /* 客户端 15s 无数据则关闭: 防止残留/半死连接把 TCP 服务器卡死,
         * 导致电脑端命令(如 WIFI_CONFIG)无法送达 */
        struct timeval tv = { .tv_sec = 15, .tv_usec = 0 };
        setsockopt(s_tcp_client, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        tcp_send_status();

        while (1) {
            int n = recv(s_tcp_client, cmd_buf, sizeof(cmd_buf) - 1, 0);
            if (n <= 0) break;

            cmd_buf[n] = '\0';
            process_command(cmd_buf, "TCP");

            /* WIFI_CONFIG/RESTART 命令已设置重启标志, 关闭连接后立即重启 */
            if (s_cmd_should_restart) {
                if (s_tcp_client >= 0) {
                    close(s_tcp_client);
                    s_tcp_client = -1;
                }
                vTaskDelay(pdMS_TO_TICKS(500));   // 让 ok 应答先发出
                esp_restart();
            }
        }

        if (s_tcp_client >= 0) {
            close(s_tcp_client);
            s_tcp_client = -1;
        }
    }
}

/* ===================== 单发多收: ESP-NOW + 模式切换 ===================== */

/* ESP-NOW 触发包 payload (10 字节): seq(u32) + mac(u8×6) */
typedef struct __attribute__((packed)) {
    uint32_t seq;
    uint8_t  mac[6];
} espnow_trig_t;

/* 解析 "AA:BB:CC:DD:EE:FF" 或 "AABBCCDDEEFF" 到 6 字节 MAC.
 * 成功返回 true, 出参 out_mac 写入 6 字节. */
static bool parse_mac_string(char *s, uint8_t *out_mac) {
    if (!s || !out_mac) return false;
    while (*s == ' ' || *s == '\t') s++;
    size_t len = strlen(s);
    /* 去掉尾部空白 */
    while (len > 0 && (s[len - 1] == ' ' || s[len - 1] == '\t'
                       || s[len - 1] == '\n' || s[len - 1] == '\r')) {
        s[--len] = '\0';
    }
    if (len != 12 && len != 17) return false;
    uint8_t tmp[6] = {0};
    int hi = -1;
    int idx = 0;
    for (size_t i = 0; i < len; i++) {
        char c = s[i];
        int v;
        if (c >= '0' && c <= '9') v = c - '0';
        else if (c >= 'a' && c <= 'f') v = c - 'a' + 10;
        else if (c >= 'A' && c <= 'F') v = c - 'A' + 10;
        else if (c == ':' || c == '-') { hi = -1; continue; }
        else return false;
        if (hi < 0) hi = v;
        else {
            if (idx >= 6) return false;
            tmp[idx++] = (uint8_t)((hi << 4) | v);
            hi = -1;
        }
    }
    if (idx != 6) return false;
    memcpy(out_mac, tmp, 6);
    return true;
}

/* 初始化 ESP-NOW 协议栈 + 注册广播 peer (idempotent). */
static void espnow_prepare(void) {
    if (s_espnow_ready) return;
    esp_err_t ret = esp_now_init();
    if (ret != ESP_OK && ret != ESP_ERR_ESPNOW_EXIST) {
        ESP_LOGE(TAG, "esp_now_init 失败: %s", esp_err_to_name(ret));
        return;
    }
    ret = esp_now_set_pmk((uint8_t *)ESPNOW_PMK);
    if (ret != ESP_OK) {
        ESP_LOGW(TAG, "esp_now_set_pmk 失败: %s (忽略, 不加密)", esp_err_to_name(ret));
    }
    esp_now_peer_info_t peer = {
        .channel   = 0,                       /* 跟随当前 STA 信道 */
        .ifidx     = WIFI_IF_STA,
        .encrypt   = false,
        .peer_addr = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
    };
    if (!esp_now_is_peer_exist(peer.peer_addr)) {
        ret = esp_now_add_peer(&peer);
        if (ret != ESP_OK) {
            ESP_LOGE(TAG, "esp_now_add_peer(bcast) 失败: %s", esp_err_to_name(ret));
        }
    }
    /* ESP-NOW 速率配置: 用跨版本兼容的 esp_wifi_config_espnow_rate (WIFI_PHY_RATE_MCS0_LGI).
     * 注: ESP-IDF v5.1+ 提供 esp_now_set_peer_rate_config(peer_addr, &rate),
     * 但旧版 SDK 只有全局配置; 这里用全局配置已经能满足固定 MCS0/HT20 的实验要求.
     *
     * 重要: 用 HT20 (20MHz) 而  HT40 (40MHz):
     *   - 大多数 2.4GHz AP 只在 HT20 模式工作, 接收端 STA 也是 HT20;
     *   - HT40 ESP-NOW 帧在 HT20 接收端解码成功率显著下降 (实测丢包 ~90%),
     *     直接表现为接收端实测频率只有配置值的 1/10 左右.
     *   - HT20 MCS0 = 6.5 Mbps, 帧 ~50 字节, 100Hz 发送占空比 ~0.4%, 完全够. */
#if defined(ESP_IDF_VERSION) && ESP_IDF_VERSION >= ESP_IDF_VERSION_VAL(5, 1, 0)
    esp_now_rate_config_t rate = {
        .phymode = WIFI_PHY_MODE_HT20,         /* 与 2.4GHz AP/STA 兼容 */
        .rate    = WIFI_PHY_RATE_MCS0_LGI,
        .ersu    = false,
        .dcm     = false,
    };
    ret = esp_now_set_peer_rate_config(peer.peer_addr, &rate);
    if (ret != ESP_OK) {
        ESP_LOGW(TAG, "esp_now_set_peer_rate_config 失败: %s", esp_err_to_name(ret));
    }
#else
    ret = esp_wifi_config_espnow_rate(WIFI_IF_STA, WIFI_PHY_RATE_MCS0_LGI);
    if (ret != ESP_OK) {
        ESP_LOGW(TAG, "esp_wifi_config_espnow_rate 失败: %s", esp_err_to_name(ret));
    }
#endif
    s_espnow_ready = true;
}

/* ESP-NOW 发送任务 (仅 ROLE_SENDER 才跑):
 * 按 s_espnow_freq_hz 节奏广播 trigger 包, 包内 seq 自增,
 * 接收端 (ROLE_RECEIVER) 用这个 seq 做跨设备对齐.
 *
 * ★ 关键: 不使用 usleep/vTaskDelay, 它们在 CONFIG_FREERTOS_HZ=100 时
 *   分辨率只有 10ms, 100Hz 配置下 10ms 周期会被舍入成 10-20ms, 实测只有 ~50Hz.
 *   这里用 esp_timer_get_time() 计算绝对 deadline,
 *   < 2ms 用 esp_rom_delay_us() 自旋 (us 精度), >= 2ms 用 vTaskDelay(1) 让出 CPU. */
static void precise_period_delay_us(int64_t deadline_us) {
    while (1) {
        int64_t now = esp_timer_get_time();
        int64_t remaining = deadline_us - now;
        if (remaining <= 0) break;
        if (remaining > 2000) {
            vTaskDelay(pdMS_TO_TICKS(1));   /* 让出 CPU ~1ms */
        } else {
            esp_rom_delay_us((uint32_t)remaining);  /* 自旋 ~us 精度 */
        }
    }
}

static void espnow_sender_task(void *arg) {
    espnow_prepare();
    static const uint8_t bcast[6] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
    espnow_trig_t trig = { .seq = 0 };
    memcpy(trig.mac, s_own_mac, 6);
    ESP_LOGI(TAG, "ESP-NOW 发送任务启动: 频率=%lu Hz", (unsigned long)s_espnow_freq_hz);
    int64_t next_deadline_us = esp_timer_get_time();
    int64_t period_us = 1000000 / (int64_t)s_espnow_freq_hz;
    uint32_t send_cnt = 0;
    int64_t first_send_us = 0;
    while (1) {
        xEventGroupWaitBits(
            s_role_evt, ROLE_SENDER_BIT, pdFALSE, pdTRUE, portMAX_DELAY);
        /* 刚被重新启用时, 把 deadline 重置为 "现在 + 1 个周期",
         * 避免在长暂停后 next_deadline_us 已过期, 立即疯狂补发积压包 */
        int64_t now_after_wait = esp_timer_get_time();
        if (next_deadline_us < now_after_wait) {
            next_deadline_us = now_after_wait;
        }
        trig.seq++;
        esp_err_t ret = esp_now_send(bcast, (const uint8_t *)&trig, sizeof(trig));
        if (ret != ESP_OK) {
            static int err_cnt = 0;
            if (++err_cnt % 100 == 1) {
                ESP_LOGW(TAG, "esp_now_send 失败: %s", esp_err_to_name(ret));
            }
        }
        uint32_t f = s_espnow_freq_hz ? s_espnow_freq_hz : 1;
        period_us = 1000000 / (int64_t)f;
        if (period_us < 1000) period_us = 1000;   /* 限速: ESP-NOW 周期 >= 1ms */
        next_deadline_us += period_us;
        precise_period_delay_us(next_deadline_us);

        /* 诊断: 每 500 次发送, 打印平均周期 vs 配置, 验证精确度 */
        if (++send_cnt % 500 == 0) {
            int64_t now = esp_timer_get_time();
            if (first_send_us == 0) first_send_us = now;
            int64_t elapsed = now - first_send_us;
            int64_t expected = (int64_t)(send_cnt - 1) * period_us;
            int64_t drift_us = elapsed - expected;
            ESP_LOGI(TAG, "ESP-NOW #%lu: 平均周期=%lldus, 配置=%lldus, 累计漂移=%+lldus",
                     (unsigned long)send_cnt,
                     (long long)(elapsed / (send_cnt - 1)),
                     (long long)period_us,
                     (long long)drift_us);
        }
    }
}

/* 启动 ESP-NOW 发送任务 (一次性) */
static void espnow_sender_task_start(void) {
    if (s_espnow_sender_task_handle) return;
    xTaskCreate(espnow_sender_task, "espnow_tx", 4096, NULL, 5,
                &s_espnow_sender_task_handle);
}

/* 把本机设为单发模式的 SENDER:
 *   1) 停掉主动 ping (避免干扰 ESP-NOW 发包节奏)
 *   2) 关 promiscuous, 关 CSI 接收
 *   3) 启动 ESP-NOW 发送任务
 *   4) 置 ROLE_SENDER_BIT 让 sender_task 跑起来
 * 注意: 必须先 esp_wifi_set_csi(false) 再 start_ping(false), 否则双路
 * 收发会互相竞争. */
static void become_sender(void) {
    stop_ping();
    esp_wifi_set_csi(false);
    esp_wifi_set_promiscuous(false);
    espnow_sender_task_start();
    s_role = ROLE_SENDER;
    xEventGroupSetBits(s_role_evt, ROLE_SENDER_BIT);
    ESP_LOGI(TAG, ">> 单发模式: 本机为 SENDER, %lu Hz",
             (unsigned long)s_espnow_freq_hz);
}

/* ===================== CSI 配置切换 =====================
 * channel_filter_en=true 时, PHY 层只捕获 AP 帧的 CSI, ESP-NOW 帧被滤掉,
 * 单发模式接收端因此收不到任何 CSI → 波形/RSSI/Hz 全无.
 * 单发模式 RECEIVER 必须关掉; 多发模式打开 (与启动默认一致).
 * 接收端靠 csi_rx_cb 里的 memcmp(info->mac, s_single_tx_sender_mac) 做
 * 兜底过滤, 不会污染其他模式. */
static void set_csi_channel_filter(bool enable) {
    wifi_csi_config_t csi_cfg = {
        .lltf_en           = true,
        .htltf_en          = true,
        .stbc_htltf2_en    = true,
        .ltf_merge_en      = true,
        .channel_filter_en = enable,
        .manu_scale        = false,
        .shift             = false,
    };
    esp_err_t ret = esp_wifi_set_csi_config(&csi_cfg);
    if (ret != ESP_OK) {
        ESP_LOGW(TAG, "esp_wifi_set_csi_config(channel_filter=%d) 失败: %s",
                 enable ? 1 : 0, esp_err_to_name(ret));
    }
}

/* 把本机设为单发模式的 RECEIVER:
 *   1) 停掉主动 ping
 *   2) 关 channel_filter_en, 开 promiscuous + CSI
 *   3) 清 ROLE_SENDER_BIT 让 sender_task 阻塞
 *   4) MAC 过滤在 csi_rx_cb 中做 */
static void become_receiver(const uint8_t *sender_mac) {
    memcpy(s_single_tx_sender_mac, sender_mac, 6);
    stop_ping();
    set_csi_channel_filter(false);   /* 必须先关 filter, 否则 esp_now 帧被 PHY 滤掉 */
    esp_wifi_set_csi(true);
    esp_wifi_set_promiscuous(true);
    xEventGroupClearBits(s_role_evt, ROLE_SENDER_BIT);
    s_role = ROLE_RECEIVER;
    ESP_LOGI(TAG, ">> 单发模式: 本机为 RECEIVER, 过滤发送端 "
             MACSTR, MAC2STR(sender_mac));
}

/* 退出发送/接收角色, 但保留 s_mode 状态. */
static void become_idle_role(void) {
    xEventGroupClearBits(s_role_evt, ROLE_SENDER_BIT);
    esp_wifi_set_promiscuous(false);
    /* CSI 在多发模式下保持开 (上层 router 帧触发); 这里只清 promiscuous */
    s_role = ROLE_IDLE;
    ESP_LOGI(TAG, ">> 角色: IDLE");
}

/* 对外命令实现: 切到多发多收模式. */
static void mode_become_multi(void) {
    s_mode = MODE_MULTI;
    /* 退出任何单发角色, 关闭 promiscuous */
    xEventGroupClearBits(s_role_evt, ROLE_SENDER_BIT);
    esp_wifi_set_promiscuous(false);
    /* 把 channel_filter_en 恢复为 true (多发模式只看路由器(AP)帧) */
    set_csi_channel_filter(true);
    /* ★ 关键: 单发模式的 SENDER 在 become_sender() 里调用了 esp_wifi_set_csi(false),
     *   切回多发时必须重新打开, 否则 CSI 回调永远不会触发, 表现为 "无 Hz / 无波形". */
    esp_wifi_set_csi(true);
    s_role = ROLE_IDLE;
    /* 多发模式: 如果之前在单发模式下被 stop_ping 了, 这里按当前 sample_rate 重启.
     * 双重保险: 先 stop_ping() 清掉任何残留状态 (例如从未切过 single 的设备
     *   s_ping 仍指向旧 session), 再 start_ping(). */
    stop_ping();
    int saved = load_sample_rate();
    if (saved > 0) {
        s_ping_interval_ms = 1000 / saved;
        start_ping();
    }
    ESP_LOGI(TAG, ">> MODE: MULTI (多发多收)");
}

/* 对外命令实现: 切到单发模式. 根据本机 MAC 是否匹配 sender_mac 决定角色.
 * 若 sender_mac 为 0 则仅作为接收端 (不主动广播). */
static void mode_become_single_tx(const uint8_t *sender_mac) {
    if (!sender_mac) {
        become_receiver(s_single_tx_sender_mac);
        s_mode = MODE_SINGLE_TX;
        return;
    }
    memcpy(s_single_tx_sender_mac, sender_mac, 6);
    s_mode = MODE_SINGLE_TX;
    if (memcmp(sender_mac, s_own_mac, 6) == 0) {
        become_sender();
    } else {
        become_receiver(sender_mac);
    }
}

/* 退出当前发送/接收角色 (保持 s_mode 不变). */
static void mode_release_role(void) {
    become_idle_role();
    if (s_mode == MODE_SINGLE_TX) {
        stop_ping();
    }
}

/* ===================== CSI 回调 ===================== */

static void csi_rx_cb(void *ctx, wifi_csi_info_t *info) {
    if (!info || !info->buf) return;

    s_total_packets++;

    // 诊断日志: 每 100 次回调打印一次, 帮助定位为什么没发包
    static uint32_t cb_cnt = 0;
    cb_cnt++;
    if (cb_cnt % 100 == 1) {
        ESP_LOGI(TAG, "CSI cb #%u len=%d pc_known=%d mode=%d role=%d",
                 cb_cnt, info->len, s_pc_known ? 1 : 0,
                 (int)s_mode, (int)s_role);
    }

    if (!s_pc_known || info->len == 0 || info->len > CSI_BUF_MAX) return;

    /* 单发多收 (MODE_SINGLE_TX) 接收端: 只采纳来自发送端 MAC 的帧.
     * ESP-NOW 帧不是路由器发来的, 必须开 promiscuous 才能收到 CSI,
     * 然后用软件 MAC 过滤.
     *
     * info->mac 在 ESP-IDF 里对不同 frame type 填充策略不一致:
     *   - data 帧: 填的是 Address 2 (SA)
     *   - mgmt 帧 (含 ESP-NOW 的 vendor-specific action frame):
     *     子类型 = beacon/probe_resp 时填 BSSID, 其它子类型通常填 0.
     * ESP-NOW 用 vendor-specific action frame, info->mac 经常是 0,
     * 会把合法的 ESP-NOW 包误过滤掉. 因此提取 MAC 时, 如果 info->mac 全 0,
     * 退回到从 info->payload 802.11 MAC 头直接取 Address 2 (offset 10). */
    if (s_mode == MODE_SINGLE_TX) {
        if (s_role != ROLE_RECEIVER) return;                  /* 发送端/空闲态不收 */

        uint8_t src_mac[6];
        bool mac_ok = false;
        const uint8_t zero[6] = {0};
        if (memcmp(info->mac, zero, 6) != 0) {
            memcpy(src_mac, info->mac, 6);
            mac_ok = true;
        } else if (info->payload && info->payload_len >= 16) {
            /* 802.11 MAC 头偏移 10 = Address 2 (源地址). */
            memcpy(src_mac, info->payload + 10, 6);
            mac_ok = true;
        }
        if (!mac_ok) {
            /* 极端情况下放弃本帧 */
            static int no_mac_cnt = 0;
            if (++no_mac_cnt <= 3) {
                ESP_LOGW(TAG, "单发模式: 帧无 MAC (info->mac=0, payload 不够), 丢弃");
            }
            return;
        }

        /* 统计: 接收端总回调数 / 来自 sender 的 / 来自其它 MAC 的.
         * 每 200 次总回调打印一次, 便于定位是 MAC 过滤丢还是 PHY 丢. */
        static uint32_t rx_total = 0, rx_match = 0, rx_other = 0;
        rx_total++;
        bool mac_match = (memcmp(src_mac, s_single_tx_sender_mac, 6) == 0);
        if (mac_match) {
            rx_match++;
        } else {
            rx_other++;
        }
        if (rx_total % 200 == 0) {
            ESP_LOGI(TAG, "单发统计: 总回调=%lu, 匹配=%lu (%.1f%%), 其它MAC=%lu",
                     (unsigned long)rx_total, (unsigned long)rx_match,
                     rx_total ? (100.0f * rx_match / rx_total) : 0.0f,
                     (unsigned long)rx_other);
        }
        if (!mac_match) return;

        /* 诊断日志: 单发模式前 50 次回调打印 MAC 来源, 验证过滤逻辑 */
        static uint32_t single_log_cnt = 0;
        if (single_log_cnt < 50) {
            single_log_cnt++;
            ESP_LOGI(TAG, "单发 #%u mac_src=%02X:%02X:%02X:%02X:%02X:%02X "
                          "(mac=%d, pl_len=%d) sender=%02X:%02X:%02X:%02X:%02X:%02X",
                     single_log_cnt,
                     src_mac[0], src_mac[1], src_mac[2], src_mac[3], src_mac[4], src_mac[5],
                     memcmp(info->mac, zero, 6) != 0 ? 1 : 0,
                     (int)info->payload_len,
                     s_single_tx_sender_mac[0], s_single_tx_sender_mac[1],
                     s_single_tx_sender_mac[2], s_single_tx_sender_mac[3],
                     s_single_tx_sender_mac[4], s_single_tx_sender_mac[5]);
        }
    }

    csi_packet_t pkt = {0};
    pkt.magic   = CSI_MAGIC;
    /* seq 选择:
     *   多发模式: 每台 ESP 独立本地计数 (s_seq++).
     *   单发模式 (接收端): seq 直接来自发送端 ESP-NOW 包内 payload,
     *     偏移 15 字节处为 4 字节 seq (与参考固件 app_main.c 一致).
     *     这样所有接收端按同一 seq 对齐, 跨设备融合. */
    if (s_mode == MODE_SINGLE_TX && s_role == ROLE_RECEIVER
        && info->payload && info->payload_len >= 19) {
        memcpy(&pkt.seq, info->payload + 15, 4);
    } else {
        pkt.seq = s_seq++;
    }
    pkt.csi_len = info->len;
    pkt.rssi    = info->rx_ctrl.rssi;
    memcpy(pkt.mac, s_own_mac, 6);   // 用本机 MAC 替代 info->mac (路由器 MAC), 用于区分设备
    pkt.wifi_ts = info->rx_ctrl.timestamp;   // PHY 时间戳，PC 端按它做间隔/时序分析

    /* === flags 标记 is_ping_reply ===
     * 主动 ping 模式: 维护"下一次预期回包时间" s_next_expected_reply_us.
     *   当前 esp_timer_get_time() 落在 ±4000us 内 → flags = 0x81 (新固件 + ping reply).
     *   命中后 s_next_expected_reply_us += interval, 滑动到下一格.
     *   漂移过大 (>3 个间隔) 自动重置 (处理 wifi 重连 / 时间戳漂移).
     * 被动模式 / 没启用 ping: flags = 0x81 (全部算合法包). */
    pkt.flags = 0x80;  // 默认: 新固件, is_ping_reply = 0
    int64_t interval_us = (int64_t)s_ping_interval_ms * 1000;
    if (!s_ping_active || s_next_expected_reply_us == 0 || interval_us <= 0) {
        pkt.flags |= 0x01;  // 被动模式: 全部合法
    } else {
        int64_t now = esp_timer_get_time();
        if (now > s_next_expected_reply_us + 3 * interval_us) {
            /* 漂移过大 (例如 wifi 重连), 重置预期时间 */
            s_next_expected_reply_us = now + interval_us;
        }
        int64_t drift = now - s_next_expected_reply_us;
        if (drift < 0) drift = -drift;
        /* 容差 5000us (5ms): 覆盖普通家用 wifi RTT 1-3ms + 抖动.
         * 第一个 ping 回包通常距 start_ping < 5ms, 这样也能命中. */
        if (drift < 5000) {
            pkt.flags |= 0x01;
            s_next_expected_reply_us += interval_us;  // 滑动到下一格
        }
    }

    memcpy(pkt.csi, info->buf, info->len);
    BaseType_t ok = xQueueSend(s_csi_queue, &pkt, 0);
    if (ok != pdTRUE) {
        static int drop_cnt = 0;
        drop_cnt++;
        if (drop_cnt <= 3) ESP_LOGW(TAG, "队列满丢包 %d 次", drop_cnt);
    }
}

/* ===================== UDP 发送任务 ===================== */

static void udp_send_task(void *arg) {
    csi_packet_t pkt;
    while (1) {
        if (xQueueReceive(s_csi_queue, &pkt, portMAX_DELAY) == pdTRUE) {
            if (!s_pc_known || s_udp_sock < 0) continue;
            size_t n = offsetof(csi_packet_t, csi) + pkt.csi_len;
            sendto(s_udp_sock, &pkt, n, 0, (struct sockaddr *)&s_pc_addr, sizeof(s_pc_addr));
        }
    }
}

/* ===================== UDP 触发包接收任务 =====================
 * 阶段1: 广播 HELLO 等待电脑回 PC_ACK（发现电脑，决定 CSI 发往哪）
 * 阶段2: 电脑端按采样率定时发来的 UDP 触发包到达后, PHY 层即触发 CSI 回调;
 *        这里 recvfrom 只把包从 socket 缓冲"消化"掉, 并按序号统计丢包.
 * 注意: 合并到同一任务是为了避免两个任务并发 recvfrom 同一 socket 的竞争.
 */

static void udp_rx_task(void *arg) {
    const char hello[] = "ESP32_CSI_HELLO";
    struct sockaddr_in bcast = {
        .sin_family = AF_INET,
        .sin_port   = htons(PC_PORT),
        .sin_addr.s_addr = htonl(INADDR_BROADCAST),
    };
    char buf[64];
    struct sockaddr_in from; socklen_t fl = sizeof(from);
    struct timeval tv = { .tv_sec = 3 };
    setsockopt(s_udp_sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    /* ---- 阶段1: 发现电脑 ---- */
    while (!s_pc_known) {
        sendto(s_udp_sock, hello, sizeof(hello) - 1, 0,
               (struct sockaddr *)&bcast, sizeof(bcast));
        int n = recvfrom(s_udp_sock, buf, sizeof(buf), 0,
                         (struct sockaddr *)&from, &fl);
        if (n <= 0) continue;               // 超时(3s), 继续广播
        if (strncmp(buf, "PC_ACK", 6) == 0) {
            s_pc_addr = from;
            s_pc_addr.sin_port = htons(PC_PORT);
            s_pc_known = true;
            s_led_mode = LED_MODE_WIFI_ON;
            ESP_LOGI(TAG, "发现电脑: %s", inet_ntoa(from.sin_addr));
            // 通知 TCP 客户端
            if (s_tcp_client >= 0) {
                tcp_send_json("{\"type\":\"board_online\",\"ip\":\"%s\"}\n", inet_ntoa(from.sin_addr));
            }
        } else {
            // 电脑发包器已开始工作(触发包先到), 先消化掉, 再稍候继续广播 HELLO
            vTaskDelay(pdMS_TO_TICKS(10));
        }
    }

    /* ---- 阶段2: 消化触发包 + 丢包统计 + 周期性心跳 ---- */
    uint32_t last_seq = 0;
    bool seq_valid = false;
    /* 周期性心跳: 每 3s 主动广播一次 HELLO, 让电脑的 _device_online_watchdog
     * (8s 没收到 HELLO 就判定离线) 始终能看到本设备在线.
     * 必须放在 recvfrom 超时分支也能到达的位置, 否则心跳永远不触发. */
    TickType_t last_hb = xTaskGetTickCount();
    while (1) {
        int n = recvfrom(s_udp_sock, buf, sizeof(buf), 0,
                         (struct sockaddr *)&from, &fl);
        if (n >= 4) {
            /* 电脑端"搜索"探测包: 回发 HELLO 让电脑随时能发现本设备
             * (否则只有刚开机广播的那段时间能被发现) */
            if (n == 14 && strncmp(buf, "ESP32_CSI_HELLO", 14) == 0) {
                sendto(s_udp_sock, buf, 14, 0, (struct sockaddr *)&from, sizeof(from));
                s_pc_addr = from;
                s_pc_addr.sin_port = htons(PC_PORT);
            }
            /* 电脑回发的 ACK, 不参与序号统计 */
            else if (n == 6 && strncmp(buf, "PC_ACK", 6) == 0) {
                /* ignore */
            }
            /* 触发包: 序号统计 */
            else {
                uint32_t seq;
                memcpy(&seq, buf, sizeof(seq));     // 包前 4 字节是序号(little-endian)
                if (seq_valid && seq != last_seq + 1) {
                    ESP_LOGW(TAG, "触发包丢包: %lu -> %lu",
                             (unsigned long)last_seq, (unsigned long)seq);
                }
                last_seq = seq;
                seq_valid = true;
                s_udp_rx_cnt++;
                if (s_udp_rx_cnt % 1000 == 0) {
                    ESP_LOGI(TAG, "UDP 触发包累计 %lu 个", (unsigned long)s_udp_rx_cnt);
                }
            }
        }

        /* 心跳: 每 3s 主动广播一次 HELLO, 兜底保证 PC 看门狗不会误判离线.
         * 必须在 if (n >= 4) 之外: 当 PC 停止发探测包, recvfrom 会一直超时
         * 返回 -1, 此时仍要走完这一圈让心跳有机会触发. */
        TickType_t now = xTaskGetTickCount();
        if (now - last_hb >= pdMS_TO_TICKS(3000)) {
            last_hb = now;
            sendto(s_udp_sock, hello, sizeof(hello) - 1, 0,
                   (struct sockaddr *)&bcast, sizeof(bcast));
        }
    }
    vTaskDelete(NULL);
}

/* ===================== Ping (采样节奏 = ping 频率) =====================
 * 采样频率由本机主动 ping 路由器的间隔决定: 路由器回包(HT 帧)到达
 * 即触发 CSI 回调, 每次 ping 得到一帧 CSI. 电脑端通过 SAMPLE_RATE 命令
 * 动态修改间隔后, 调用 stop_ping + start_ping 使新频率立即生效.
 */

static void stop_ping(void) {
    if (s_ping != NULL) {
        esp_ping_stop(s_ping);
        esp_ping_delete_session(s_ping);
        s_ping = NULL;
    }
    s_ping_active = false;
    s_next_expected_reply_us = 0;  // 关掉时间窗, csi_rx_cb 走被动模式
}

static void start_ping(void) {
    esp_netif_t *netif = esp_netif_get_handle_from_ifkey("WIFI_STA_DEF");
    if (netif == NULL) return;
    esp_netif_ip_info_t ip;
    if (esp_netif_get_ip_info(netif, &ip) != ESP_OK) return;

    esp_ping_config_t cfg = ESP_PING_DEFAULT_CONFIG();
    cfg.interval_ms = s_ping_interval_ms;
    cfg.count       = ESP_PING_COUNT_INFINITE;
    cfg.timeout_ms  = 200;
    ip_addr_t target = { .type = IPADDR_TYPE_V4 };
    target.u_addr.ip4.addr = ip.gw.addr;
    cfg.target_addr = target;

    ESP_LOGI(TAG, "Ping 网关 %s interval=%lums (%dHz)",
             inet_ntoa(ip.gw.addr),
             (unsigned long)s_ping_interval_ms,
             1000 / (int)s_ping_interval_ms);
    esp_ping_handle_t ping = NULL;
    if (esp_ping_new_session(&cfg, NULL, &ping) == ESP_OK) {
        s_ping = ping;
        esp_ping_start(ping);
        s_ping_active = true;
        /* 初始化预期回包时间窗: 设为当前时刻 (下一个回包在 RTT 后到,
         * 通常 < 5ms). 设得太靠后会漏掉第一个真正的 ping 回包. */
        s_next_expected_reply_us = esp_timer_get_time();
    }
}

/* ===================== WiFi 事件 ===================== */

static void wifi_handler(void *arg, esp_event_base_t base, int32_t id, void *data) {
    if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) {
        if (!s_in_ap_fallback) esp_wifi_connect();
    } else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED) {
        s_pc_known = false;
        s_led_mode = LED_MODE_OFF;
        /* AP fallback 期间不再触发 STA 重连: 重连会跳到其它信道,
         * 让手机的 AP 连接瞬间中断, 转圈连不上. */
        if (!s_in_ap_fallback) esp_wifi_connect();
    } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
        /* 关闭省电: 否则 ESP 休眠时收不到触发包, 丢点严重 */
        esp_wifi_set_ps(WIFI_PS_NONE);
        xEventGroupSetBits(s_wifi_eg, WIFI_OK_BIT);
        /* 打印分配的 IP, 方便用户通过串口/电脑搜索确认设备已成功上网 */
        esp_netif_t *sta_netif = esp_netif_get_handle_from_ifkey("WIFI_STA_DEF");
        if (sta_netif) {
            esp_netif_ip_info_t ip;
            if (esp_netif_get_ip_info(sta_netif, &ip) == ESP_OK) {
                ESP_LOGI(TAG, "STA 已连接, IP=" IPSTR ", 正在广播 HELLO 等电脑发现",
                         IP2STR(&ip.ip));
            }
        }
    }
}

/* ===================== app_main ===================== */

void app_main(void) {
    esp_err_t nvs_err = nvs_flash_init();
    if (nvs_err == ESP_ERR_NVS_NO_FREE_PAGES || nvs_err == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        nvs_err = nvs_flash_init();
    }
    ESP_ERROR_CHECK(nvs_err);
    esp_netif_init();
    esp_event_loop_create_default();
    esp_netif_create_default_wifi_sta();
    s_wifi_eg = xEventGroupCreate();
    s_role_evt = xEventGroupCreate();
    s_sock_mutex = xSemaphoreCreateMutex();

    wifi_init_config_t wicfg = WIFI_INIT_CONFIG_DEFAULT();
    esp_wifi_init(&wicfg);
    esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, wifi_handler, NULL);
    esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, wifi_handler, NULL);

    /* WiFi 配置: 优先使用 NVS 中保存的 (电脑端"配置WiFi"命令写入), 否则用编译期默认值 */
    char wifi_ssid[33] = WIFI_SSID;
    char wifi_pass[65] = WIFI_PASS;
    load_wifi_config(wifi_ssid, sizeof(wifi_ssid), wifi_pass, sizeof(wifi_pass));

    wifi_config_t wcfg = {0};
    strncpy((char *)wcfg.sta.ssid,     wifi_ssid, sizeof(wcfg.sta.ssid) - 1);
    strncpy((char *)wcfg.sta.password, wifi_pass, sizeof(wcfg.sta.password) - 1);
    esp_wifi_set_mode(WIFI_MODE_STA);
    esp_wifi_set_config(WIFI_IF_STA, &wcfg);
    esp_wifi_stop();
    esp_wifi_deinit();
    esp_wifi_init(&wicfg);
    esp_wifi_set_mode(WIFI_MODE_STA);
    esp_wifi_set_config(WIFI_IF_STA, &wcfg);
    esp_wifi_start();

    /* 读取本机 WiFi MAC, 作为 CSI 包里的"设备唯一标识".
     * 注意: info->mac 是触发 CSI 的帧的源地址 (channel_filter_en 后固定为路由器
     * MAC), 所有节点都一样, 不能用来区分设备; 这里改用本机 MAC. */
    esp_wifi_get_mac(WIFI_IF_STA, s_own_mac);
    ESP_LOGI(TAG, "本机 MAC: %02X:%02X:%02X:%02X:%02X:%02X",
             s_own_mac[0], s_own_mac[1], s_own_mac[2],
             s_own_mac[3], s_own_mac[4], s_own_mac[5]);

    /* 等待 WiFi 连接 (最多 AP_FALLBACK_TIMEOUT_MS, 超时进入 AP 配置模式) */
    EventBits_t bits = xEventGroupWaitBits(
        s_wifi_eg, WIFI_OK_BIT, false, true,
        pdMS_TO_TICKS(AP_FALLBACK_TIMEOUT_MS));
    if ((bits & WIFI_OK_BIT) == 0) {
        /* 连接超时: 启动 AP 配置模式 (不会返回, ap_config_task 持续运行) */
        enter_ap_fallback();
        /* 进入 AP 后, 也继续等 STA 偶尔连上; 一旦连上 WIFI_OK_BIT 置位, 下面
         * 的代码就能继续执行 UDP / TCP 任务. */
        xEventGroupWaitBits(s_wifi_eg, WIFI_OK_BIT, false, true, portMAX_DELAY);
    }
    ESP_LOGI(TAG, "WiFi 已连接");

    /* WS2812 LED 初始化 */
    led_init();
    s_led_mode = LED_MODE_WIFI_ON;
    xTaskCreate(led_task, "led", 2048, NULL, 3, NULL);

    /* UDP socket */
    s_udp_sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
    int yes = 1;
    setsockopt(s_udp_sock, SOL_SOCKET, SO_BROADCAST, &yes, sizeof(yes));
    struct sockaddr_in local = {
        .sin_family = AF_INET,
        .sin_port   = htons(PC_PORT),
        .sin_addr.s_addr = htonl(INADDR_ANY),
    };
    bind(s_udp_sock, (struct sockaddr *)&local, sizeof(local));

    /* 队列 + 任务 */
    s_csi_queue = xQueueCreate(32, sizeof(csi_packet_t));
    xTaskCreate(udp_send_task, "udp",   4096, NULL, 5, NULL);
    xTaskCreate(udp_rx_task,   "udprx", 4096, NULL, 5, NULL);
    xTaskCreate(tcp_server_task,"tcp",  4096, NULL, 5, NULL);
    xTaskCreate(uart_cmd_task, "ucmd",  4096, NULL, 4, NULL);

    /* CSI */
    /* CSI 按 HT 数据帧优化: 触发包经路由器转发是 HT 格式, 能拿到 htltf 的丰富 CSI */
    wifi_csi_config_t csi_cfg = {
        .lltf_en           = true,
        .htltf_en          = true,
        .stbc_htltf2_en    = true,
        .ltf_merge_en      = true,
        .channel_filter_en = true,   // 只保留路由器(AP)发来的帧
        .manu_scale        = false,  // 自动缩放
        .shift             = false,  // 不做手动移位
    };
    esp_wifi_set_csi_config(&csi_cfg);
    esp_wifi_set_csi_rx_cb(csi_rx_cb, NULL);
    esp_wifi_set_csi(true);

    /* 采样频率由本机主动 ping 路由器的间隔决定, 开机加载上次保存的频率.
     * 若用户上次选择了"停止 ping", NVS 中保存 0, 开机不做主动 ping, 由广播触发 CSI. */
    int saved_rate = load_sample_rate();
    if (saved_rate > 0) {
        s_ping_interval_ms = 1000 / saved_rate;
        start_ping();
    } else {
        ESP_LOGI(TAG, "上电保持停止主动 ping, 等待广播触发 CSI");
    }

    ESP_LOGI(TAG, "就绪: UDP %d, TCP %d | 等待发现电脑", PC_PORT, TCP_PORT);
}
