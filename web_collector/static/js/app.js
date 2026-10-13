/* ============================================================
   CSI 网页版采集工具 - 前端逻辑 (V6.1)
   ============================================================
   模块:
     - SocketIO 连接与事件订阅
     - REST API 调用封装
     - 设备列表渲染 (含「角色」列)
     - Canvas 实时波形绘制 (高效: 仅绘制变化部分)
     - 控件事件 → 后端命令映射
     - 状态栏 / Toast / 信号条 实时刷新
     - 语音/延时配置 + 倒计时 UI
     - 人员设置 (折叠面板 + 多人增删)
     - 环境设置 (折叠面板 + 预设 chip + 自定义项)
     - 采集模式 (多发多收 / 一发多收 + 发送端选择)
   ============================================================ */

// ============================================================
// 全局状态 (前端镜像)
// ============================================================
const state = {
  socket: null,
  connected: false,
  devices: {},                 // ip -> {ip, name, selected, online, rssi, position, actual_hz, role}
  activeIp: null,              // 当前选中设备
  activeDetail: null,          // 后端推送的详情
  status: {},                  // 后端推送的采集状态
  cfg: {                       // 数据集配置 (前端 → 后端)
    router_loc: "地上",
    save_dir: "",
    save_format: "npz",
    data_label: "静坐",
    win_len: 5,
    win_files: 1,
    target_frames: 500,
    target_files: 1,
    sample_rate: 100,
    sample_rate_enabled: false,
    phone_ip: "",
    video_save_dir: "",
    video_enabled: false,
    voice_enabled: false,
    delay_enabled: false,
    delay_seconds: 3,
    // 采集模式: "multi" 多发多收 (默认) / "single" 一发多收
    csi_mode: "multi",
    sender_ip: "",
    // V6.1: 人员设置 (上传后端用于文件命名 + 元数据)
    persons: [],
    env_values: {},
    env_custom: [],
  },
  positionOptions: [
    "床头柜-靠门", "床头柜-靠窗", "床尾-靠窗", "床尾-靠门", "洗衣机旁", "床头上方", "床尾", "洗手台右侧",
    "东南角", "东北角", "西南角", "西北角",
    "客厅边几", "书房书桌", "书房沙发旁",
  ],
  // Canvas 状态
  canvasBuf: [],               // 二维数组, 每行 [sc8, sc24, sc40, sc56] 振幅 dB
  canvasMax: 250,              // 缓冲最大帧数
  // 倒计时状态
  countdownActive: false,
  // 人员设置 (localStorage 持久化, 不上送后端)
  persons: [],
  // 环境设置 (localStorage 持久化, 不上送后端, 仅用于文件名生成与预览)
  env: { "天气": "晴", "风": "微风", "门": "关", "窗户": "关", "空调": "关" },
  envCustom: [],
};

const SC_IDX = [8, 24, 40, 56];
const SC_COLORS = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"];
const LABEL_NO_PERSON = "无人";
const ENV_PRESET_KEYS = ["天气", "风", "门", "窗户", "空调"];
const LS_PERSONS = "csi.persons.v61";
const LS_ENV_VALS = "csi.env.values.v61";
const LS_ENV_CUST = "csi.env.custom.v61";
const LS_MODE     = "csi.mode.v61";

// ============================================================
// 启动: 连接 SocketIO + 拉取初始状态
// ============================================================
window.addEventListener("DOMContentLoaded", () => {
  state.cfg.save_dir = `csi_data_${todayYMD()}`;
  state.cfg.video_save_dir = `video_data_${todayYMD()}`;
  loadLocalConfig();
  bindControlEvents();
  bindCanvasResize();
  bindTbodyDelegation();
  bindFoldPanels();
  bindPersonPanel();
  bindEnvPanel();
  updateCollectParams();
  drawEmptyWave();
  connectSocket();
});

function todayYMD() {
  const d = new Date();
  return `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}${String(d.getDate()).padStart(2, "0")}`;
}

// ============================================================
// localStorage 加载/保存 (人员 + 环境 + 模式)
// ============================================================
function loadLocalConfig() {
  try { state.persons = JSON.parse(localStorage.getItem(LS_PERSONS) || "[]"); } catch(e) { state.persons = []; }
  try {
    const v = JSON.parse(localStorage.getItem(LS_ENV_VALS) || "{}");
    Object.assign(state.env, v);
  } catch(e) {}
  try { state.envCustom = JSON.parse(localStorage.getItem(LS_ENV_CUST) || "[]"); } catch(e) { state.envCustom = []; }
  try {
    const m = localStorage.getItem(LS_MODE);
    if (m === "single" || m === "multi") state.cfg.csi_mode = m;
  } catch(e) {}
  // 兼容旧 localStorage key (用户在 V6.0 之前可能已添加过人员/环境项)
  try {
    if (!localStorage.getItem(LS_PERSONS)) {
      const old = JSON.parse(localStorage.getItem("csi.persons") || "[]");
      if (Array.isArray(old) && old.length) { state.persons = old; savePersons(); }
    }
    if (!localStorage.getItem(LS_ENV_VALS)) {
      const old = JSON.parse(localStorage.getItem("csi.env.values") || "{}");
      if (old && Object.keys(old).length) {
        Object.assign(state.env, old);
        saveEnvValues();
      }
    }
    if (!localStorage.getItem(LS_ENV_CUST)) {
      const old = JSON.parse(localStorage.getItem("csi.env.custom") || "[]");
      if (Array.isArray(old) && old.length) { state.envCustom = old; saveEnvCustom(); }
    }
  } catch(e) {}
}

function savePersons() { localStorage.setItem(LS_PERSONS, JSON.stringify(state.persons)); }
function saveEnvValues() { localStorage.setItem(LS_ENV_VALS, JSON.stringify(state.env)); }
function saveEnvCustom() { localStorage.setItem(LS_ENV_CUST, JSON.stringify(state.envCustom)); }

function connectSocket() {
  state.socket = io({ transports: ["polling", "websocket"] });
  state.socket.on("connect", () => {
    state.connected = true;
    showToast(`✓ 已连接服务器`, "green");
    apiGet("/api/status").then((st) => {
      if (st && st.config) {
        Object.assign(state.cfg, st.config);
      }
      if (st && typeof st.csi_mode === "string") {
        state.cfg.csi_mode = st.csi_mode;
      }
      if (st && typeof st.sender_ip === "string") {
        state.cfg.sender_ip = st.sender_ip;
      }
      pushCfgToUI();
      renderPersons();
      renderEnvUI();
      refreshFileDemo();
      renderDeviceTable(st.devices || []);
      // 同步显示模式状态
      if (state.cfg.csi_mode === "single") {
        setModeStatus(state.cfg.sender_ip
                      ? `一发多收 · 发送端=${state.cfg.sender_ip}`
                      : "一发多收 (未指定发送端)", "purple");
      } else {
        setModeStatus("多发多收", "blue");
      }
      setModeHint();
      renderStatusContext(st || {}, state.cfg);
      // 选中第一个设备作为默认 active
      const ips = (st.devices || []).map(d => d.ip);
      if (ips.length && !state.activeIp) {
        selectDevice(ips[0]);
      }
    });
  });
  state.socket.on("disconnect", () => {
    state.connected = false;
    showToast("✗ 与服务器断开", "red", 5000);
  });
  state.socket.on("device_added", (data) => {
    showToast(`✓ 新设备: ${data.ip}`, "green", 2500);
  });
  state.socket.on("device_hello", () => {});
  state.socket.on("device_update", () => {});
  state.socket.on("device_snapshot", (snap) => {
    renderDeviceTable(snap.devices || []);
  });
  state.socket.on("active_device_detail", (detail) => {
    state.activeDetail = detail;
    renderActiveDetail(detail);
  });
  state.socket.on("csi_batch", (batch) => {
    if (batch.ip !== state.activeIp) return;
    state.canvasBuf = batch.buf || [];
    drawWave();
  });
  state.socket.on("status_update", (st) => {
    state.status = st;
    renderStatus(st);
    renderStatusContext(st, state.cfg);
  });
  state.socket.on("phone_update", (data) => {
    const dot = document.getElementById("phone-dot");
    const txt = document.getElementById("phone-status");
    if (!state.cfg.phone_ip) {
      dot.className = "phone-dot gray";
      txt.textContent = "未填写";
      txt.style.color = "#a0aec0";
      return;
    }
    if (data.online) {
      dot.className = "phone-dot green";
      txt.textContent = "在线";
      txt.style.color = "#38a169";
    } else {
      dot.className = "phone-dot red";
      txt.textContent = "离线/不可达";
      txt.style.color = "#e53e3e";
    }
  });
  state.socket.on("countdown_update", (data) => {
    if (data.remaining > 0) {
      showCountdown(data.remaining);
    } else {
      hideCountdown();
    }
  });
  state.socket.on("message", (m) => {
    showToast(m.text, m.level || "gray", 3000);
  });
}

// ============================================================
// REST API 封装
// ============================================================
async function apiPost(path, body) {
  try {
    const r = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    });
    return await r.json();
  } catch (e) {
    return { ok: false, msg: String(e) };
  }
}

async function apiGet(path) {
  try {
    const r = await fetch(path);
    return await r.json();
  } catch (e) {
    return null;
  }
}

// ============================================================
// 折叠面板: 人员设置 + 环境设置
// ============================================================
function bindFoldPanels() {
  bindFold("personBar", "personPanel", "personCaret");
  bindFold("envBar", "envPanel", "envCaret");
}
function bindFold(barId, panelId, caretId) {
  const bar = document.getElementById(barId);
  if (!bar) return;
  bar.addEventListener("click", () => {
    const p = document.getElementById(panelId);
    const open = p.style.display !== "none";
    p.style.display = open ? "none" : "";
    const c = document.getElementById(caretId);
    if (c) c.style.transform = open ? "" : "rotate(90deg)";
  });
}

// ============================================================
// UI 控件事件绑定 (一次性)
// ============================================================
function bindControlEvents() {
  // 路由器位置
  document.getElementById("cfg-router").addEventListener("change", e => {
    state.cfg.router_loc = e.target.value;
    pushCfgToServer();
  });
  // 数据标签
  document.getElementById("cfg-label").addEventListener("change", e => {
    state.cfg.data_label = e.target.value;
    pushCfgToServer();
  });
  // 采样频率 (新位置: 设备工具栏 sr-inline, 全选按钮左侧)
  document.getElementById("cfg-sr-toggle").addEventListener("change", e => {
    const enabled = e.target.checked;
    document.getElementById("cfg-sr").disabled = !enabled;
    state.cfg.sample_rate_enabled = enabled;
    apiPost("/api/sample_rate_toggle", { enabled });
    if (enabled) apiPost("/api/sample_rate", { rate: state.cfg.sample_rate });
    pushCfgToServer();
  });
  document.getElementById("cfg-sr").addEventListener("change", e => {
    const v = parseInt(e.target.value, 10);
    if (!isNaN(v) && v >= 1 && v <= 1000) {
      state.cfg.sample_rate = v;
      apiPost("/api/sample_rate", { rate: v });
      pushCfgToServer();
    }
  });
  // 采集模式 (单一下拉框, 切换即自动应用到 ESPs)
  document.getElementById("cfg-mode").addEventListener("change", async e => {
    state.cfg.csi_mode = e.target.value;
    localStorage.setItem(LS_MODE, state.cfg.csi_mode);
    setModeHint();
    // 一发多收: 自动选第一台在线设备作为发送端
    if (state.cfg.csi_mode === "single") {
      const first = Object.values(state.devices)
        .filter(d => d.online)
        .sort((a, b) => a.ip.localeCompare(b.ip))[0];
      state.cfg.sender_ip = first ? first.ip : "";
    }
    renderDeviceTable(Object.values(state.devices));
    pushCfgToServer();
    // 自动应用模式到 ESPs (无 apply 按钮)
    await applyMode();
  });
  // 保存路径 + 格式
  document.getElementById("cfg-save-dir").addEventListener("change", e => {
    state.cfg.save_dir = e.target.value;
    pushCfgToServer();
  });
  document.getElementById("cfg-format").addEventListener("change", e => {
    state.cfg.save_format = e.target.value;
    pushCfgToServer();
  });
  // 采集方式 (单一 dropdown, 控制栏中部) → 切换显示对应参数
  document.getElementById("collect-mode").addEventListener("change", e => {
    state._collectMode = e.target.value;
    updateCollectParams();
  });
  // 按窗口采集 / 按帧数采集 (参数, 已搬到控制栏)
  document.getElementById("cfg-win-len").addEventListener("change", e => {
    state.cfg.win_len = parseInt(e.target.value, 10) || 5;
    pushCfgToServer();
  });
  document.getElementById("cfg-win-files").addEventListener("change", e => {
    state.cfg.win_files = parseInt(e.target.value, 10) || 1;
    pushCfgToServer();
  });
  document.getElementById("cfg-target-frames").addEventListener("change", e => {
    state.cfg.target_frames = parseInt(e.target.value, 10) || 500;
    pushCfgToServer();
  });
  document.getElementById("cfg-target-files").addEventListener("change", e => {
    state.cfg.target_files = parseInt(e.target.value, 10) || 1;
    pushCfgToServer();
  });
  // 视频
  document.getElementById("cfg-phone-ip").addEventListener("change", e => {
    state.cfg.phone_ip = e.target.value.trim();
    apiPost("/api/phone_ip", { ip: state.cfg.phone_ip });
    pushCfgToServer();
  });
  document.getElementById("cfg-video-toggle").addEventListener("change", e => {
    state.cfg.video_enabled = e.target.checked;
    apiPost("/api/video_toggle", { enabled: state.cfg.video_enabled });
    pushCfgToServer();
  });
  document.getElementById("cfg-video-dir").addEventListener("change", e => {
    state.cfg.video_save_dir = e.target.value;
    pushCfgToServer();
  });
  // 语音 / 延时
  document.getElementById("cfg-voice-toggle").addEventListener("change", e => {
    state.cfg.voice_enabled = e.target.checked;
    apiPost("/api/voice_toggle", { enabled: state.cfg.voice_enabled });
    pushCfgToServer();
  });
  document.getElementById("cfg-delay-toggle").addEventListener("change", e => {
    const enabled = e.target.checked;
    state.cfg.delay_enabled = enabled;
    document.getElementById("cfg-delay-sec").disabled = !enabled;
    apiPost("/api/delay_toggle", { enabled });
    pushCfgToServer();
  });
  document.getElementById("cfg-delay-sec").addEventListener("change", e => {
    const v = parseInt(e.target.value, 10);
    if (!isNaN(v) && v >= 1 && v <= 60) {
      state.cfg.delay_seconds = v;
      apiPost("/api/delay_seconds", { sec: v });
      pushCfgToServer();
    }
  });
}

function pushCfgToUI() {
  const c = state.cfg;
  document.getElementById("cfg-router").value = c.router_loc || "地上";
  document.getElementById("cfg-label").value = c.data_label || "静坐";
  document.getElementById("cfg-sr-toggle").checked = !!c.sample_rate_enabled;
  document.getElementById("cfg-sr").disabled = !c.sample_rate_enabled;
  document.getElementById("cfg-sr").value = c.sample_rate || 100;
  // 采集模式 (单一下拉框)
  document.getElementById("cfg-mode").value = c.csi_mode || "multi";
  setModeHint();
  // 保存路径 / 格式
  document.getElementById("cfg-save-dir").value = c.save_dir || "";
  document.getElementById("cfg-format").value = c.save_format || "npz";
  // 按窗口 / 按帧数
  document.getElementById("cfg-win-len").value = c.win_len || 5;
  document.getElementById("cfg-win-files").value = c.win_files || 1;
  document.getElementById("cfg-target-frames").value = c.target_frames || 500;
  document.getElementById("cfg-target-files").value = c.target_files || 1;
  // 采集方式 (控制栏中部 dropdown)
  const collectModeEl = document.getElementById("collect-mode");
  if (collectModeEl) {
    collectModeEl.value = state._collectMode || "window";
    updateCollectParams();
  }
  document.getElementById("cfg-phone-ip").value = c.phone_ip || "";
  document.getElementById("cfg-video-toggle").checked = !!c.video_enabled;
  document.getElementById("cfg-video-dir").value = c.video_save_dir || "";
  document.getElementById("cfg-voice-toggle").checked = !!c.voice_enabled;
  document.getElementById("cfg-delay-toggle").checked = !!c.delay_enabled;
  document.getElementById("cfg-delay-sec").disabled = !c.delay_enabled;
  document.getElementById("cfg-delay-sec").value = c.delay_seconds || 3;
  // 模式提示
  setModeHint();
}

// 同步"发送端下拉框"
function syncSenderDropdown() {
  const sel = document.getElementById("cfg-sender");
  if (!sel) return;
  const mode = state.cfg.csi_mode || "multi";
  sel.disabled = (mode !== "single");
  const onlineIps = Object.values(state.devices)
    .filter(d => d.online).map(d => d.ip).sort();
  const cur = sel.value;
  let html = '<option value="">自动 (第一台在线设备)</option>';
  onlineIps.forEach(ip => {
    const name = (state.devices[ip] && state.devices[ip].name) || ip;
    html += `<option value="${esc(ip)}">${esc(name)} (${esc(ip)})</option>`;
  });
  sel.innerHTML = html;
  if (cur && onlineIps.includes(cur)) sel.value = cur;
}

// 模式提示文字 (显示在配置行右侧)
function setModeHint() {
  const hint = document.getElementById("modeHint");
  if (!hint) return;
  if (state.cfg.csi_mode === "single") {
    hint.textContent = "一台 ESP 作为发送端发送 ESP-NOW 广播，其余设备接收";
    hint.style.color = "#7a5bd6";
  } else {
    hint.textContent = "所有 ESP 各自 ping 路由器，互为收发";
    hint.style.color = "#a0aec0";
  }
  const scope = document.getElementById("pingScope");
  if (scope) {
    scope.textContent = (state.cfg.csi_mode === "single")
      ? "（仅发送端生效）" : "（全部设备生效）";
  }
}

// 应用模式: 调用 /api/mode 把当前 mode + sender_ip 下发到设备.
// 后端会阻塞等固件确认 (每台最多 6 秒), 前端拿到的 r.ok=true
// 表示所有设备都成功切到新模式, 才显示"已切换为..."的绿色 toast.
async function applyMode() {
  const mode = state.cfg.csi_mode || "multi";
  const sender_ip = (mode === "single") ? (state.cfg.sender_ip || "") : "";
  const r = await apiPost("/api/mode", { mode, sender_ip });
  if (!r.ok) {
    showToast(`✗ ${r.msg}`, "red", 5000);
    setModeStatus("应用失败", "red");
    return;
  }
  // 后端 msg 已经带了 "(N/M 台已确认)" 的真实确认数, 直接显示
  showToast(`✓ ${r.msg}`, "green", 3500);
  if (mode === "single") {
    setModeStatus(`一发多收 · 发送端=${r.sender_ip || "(自动)"}`, "purple");
  } else {
    setModeStatus("多发多收", "blue");
  }
  state.cfg.csi_mode = r.mode;
  state.cfg.sender_ip = r.sender_ip || "";
  document.getElementById("cfg-mode").value = r.mode;
  renderDeviceTable(Object.values(state.devices));
}

function setModeStatus(text, color = "blue") {
  const el = document.getElementById("cfg-mode-status");
  if (!el) return;
  el.textContent = text;
  el.style.color = (color === "red") ? "#e53e3e"
                  : (color === "purple") ? "#7b1fa2"
                  : (color === "green") ? "#38a169"
                  : "#1e88e5";
}

function pushCfgToServer() {
  apiPost("/api/config", state.cfg);
  renderStatusContext(state.status || {}, state.cfg);
}

// ============================================================
// 人员设置面板
// ============================================================
function bindPersonPanel() {
  document.getElementById("addPerson").addEventListener("click", () => {
    const names = document.getElementById("pName").value.trim();
    const pos = document.getElementById("pPos").value.trim();
    if (!names) { showToast("请输入姓名", "orange", 1500); return; }
    let added = 0;
    names.split(/[,，、\s]+/).forEach(n => {
      n = n.trim();
      if (!n) return;
      if (n.length > 10) { showToast(`姓名「${n}」过长，已跳过`, "orange", 1500); return; }
      if (state.persons.some(p => p.name === n)) {
        showToast(`人员「${n}」已存在，已跳过`, "orange", 1500); return;
      }
      state.persons.push({ name: n, pos: pos, sel: true });
      added++;
    });
    if (added) {
      document.getElementById("pName").value = "";
      document.getElementById("pPos").value = "";
      savePersons();
      renderPersons();
      refreshFileDemo();
      syncCfgPersonsEnv();
      pushCfgToServer();
    }
  });
}

function selectedPersons() {
  return state.persons.filter(p => p.sel);
}
function personSummary() {
  const sel = selectedPersons();
  if (!sel.length) return "未选择人员";
  const s = sel.map(p => p.name).join("、");
  return s.length > 18 ? s.slice(0, 18) + "…" : s;
}
function renderPersons() {
  const list = document.getElementById("personList");
  if (!list) return;
  list.innerHTML = "";
  state.persons.forEach((p, i) => {
    const row = document.createElement("div");
    row.className = "person-row";
    row.innerHTML =
      `<input type="checkbox" ${p.sel ? "checked" : ""} data-i="${i}" class="pSel">` +
      `<input type="text" class="cfg-input pName" style="width:110px;" value="${esc(p.name)}" data-i="${i}">` +
      `<span style="font-size:12px; color:#718096;">位置</span>` +
      `<input type="text" class="cfg-input pPos" style="width:130px;" value="${esc(p.pos)}" placeholder="如: 客厅" data-i="${i}">` +
      `<span class="${p.sel ? "tag-tx" : "tag-rx"}">${p.sel ? "参与本次" : "不参与"}</span>` +
      `<button class="del" data-i="${i}" title="删除">✕ 删除</button>`;
    list.appendChild(row);
  });
  list.querySelectorAll(".pSel").forEach(c => {
    c.addEventListener("change", function() {
      state.persons[+this.dataset.i].sel = this.checked;
      savePersons();
      renderPersons();
      refreshFileDemo();
      syncCfgPersonsEnv();
      pushCfgToServer();
    });
  });
  list.querySelectorAll(".pName").forEach(inp => {
    inp.addEventListener("change", function() {
      const v = this.value.trim();
      if (!v) { showToast("姓名不能为空", "orange", 1500); renderPersons(); return; }
      state.persons[+this.dataset.i].name = v;
      savePersons();
      refreshFileDemo();
      document.getElementById("personSummary").textContent = personSummary();
      syncCfgPersonsEnv();
      pushCfgToServer();
    });
  });
  list.querySelectorAll(".pPos").forEach(inp => {
    inp.addEventListener("change", function() {
      state.persons[+this.dataset.i].pos = this.value.trim();
      savePersons();
      refreshFileDemo();
      syncCfgPersonsEnv();
      pushCfgToServer();
    });
  });
  list.querySelectorAll(".del").forEach(b => {
    b.addEventListener("click", function() {
      state.persons.splice(+this.dataset.i, 1);
      savePersons();
      renderPersons();
      refreshFileDemo();
      document.getElementById("personSummary").textContent = personSummary();
      syncCfgPersonsEnv();
      pushCfgToServer();
    });
  });
  const sum = document.getElementById("personSummary");
  if (sum) sum.textContent = personSummary();
}

// ============================================================
// 环境设置面板
// ============================================================
function bindEnvPanel() {
  // 预设 chip 点击
  document.querySelectorAll(".env-chip[data-k]").forEach(c => {
    c.addEventListener("click", function() {
      state.env[this.dataset.k] = this.textContent;
      saveEnvValues();
      refreshEnvUI();
      syncCfgPersonsEnv();
      pushCfgToServer();
    });
  });
  // 添加自定义项
  document.getElementById("addEnv").addEventListener("click", () => {
    const n = document.getElementById("cName").value.trim();
    const v = document.getElementById("cVal").value.trim() || "--";
    if (!n) { showToast("请输入环境项名称", "orange", 1500); return; }
    if (n.length > 10) { showToast("名称不超过 10 个字符", "orange", 1500); return; }
    if (ENV_PRESET_KEYS.includes(n) || state.envCustom.some(c => c.name === n)) {
      showToast(`环境项「${n}」已存在`, "orange", 1500); return;
    }
    state.envCustom.push({ name: n, value: v });
    document.getElementById("cName").value = "";
    document.getElementById("cVal").value = "";
    saveEnvCustom();
    refreshEnvUI();
    syncCfgPersonsEnv();
    pushCfgToServer();
  });
}

// 把人员 / 环境 数据同步到 cfg, 然后推送到后端
function syncCfgPersonsEnv() {
  state.cfg.persons = state.persons.map(p => ({
    name: p.name, pos: p.pos, sel: !!p.sel,
  }));
  state.cfg.env_values = Object.assign({}, state.env);
  state.cfg.env_custom = state.envCustom.map(c => ({
    name: c.name, value: c.value,
  }));
}

function envSummary() {
  const parts = ENV_PRESET_KEYS.map(k => `${k}:${state.env[k]}`);
  state.envCustom.forEach(c => parts.push(`${c.name}:${c.value}`));
  const s = parts.join(" · ");
  return s.length > 60 ? parts.slice(0, 3).join(" · ") + " … +" + (parts.length - 3) + "项" : s;
}

function refreshEnvUI() {
  const sum = document.getElementById("envSummary");
  if (sum) sum.textContent = envSummary();
  document.querySelectorAll(".env-chip[data-k]").forEach(c => {
    c.classList.toggle("on", state.env[c.dataset.k] === c.textContent);
  });
  const g = document.getElementById("customGroups");
  if (!g) return;
  g.innerHTML = "";
  state.envCustom.forEach((c, i) => {
    const div = document.createElement("div");
    div.className = "env-group";
    div.innerHTML =
      `<span class="gname">${esc(c.name)}</span>` +
      `<span class="custom-tag">值：<b>${esc(c.value)}</b>` +
      `<button class="del" data-i="${i}" title="删除">✕</button></span>`;
    g.appendChild(div);
  });
  g.querySelectorAll(".del").forEach(b => {
    b.addEventListener("click", function() {
      state.envCustom.splice(+this.dataset.i, 1);
      saveEnvCustom();
      refreshEnvUI();
      refreshFileDemo();
      syncCfgPersonsEnv();
      pushCfgToServer();
    });
  });
  refreshFileDemo();
}

// 清理文件名非法字符
function clean(s) {
  return (s || "--").replace(/[\\/:*?"<>|\s]/g, "").slice(0, 8) || "--";
}

// 文件名示例
function refreshFileDemo() {
  const sel = selectedPersons();
  const pSeg = sel.length ? sel.map(p => clean(p.name)).join("+") : "--";
  const posSeg = (sel[0] && sel[0].pos) ? clean(sel[0].pos) : "--";
  const stamp = todayYMD() + "143022_001";
  const envSeg = ENV_PRESET_KEYS.map(k => clean(state.env[k])).join("_");
  const cSeg = state.envCustom.map(c => clean(c.name) + clean(c.value)).join("_");
  const demo = (state.cfg.data_label || "静坐") + "_" + pSeg + "_" + posSeg + "_" +
               envSeg + (cSeg ? "_" + cSeg : "") + "_" + stamp + "." + (state.cfg.save_format || "npz");
  const el = document.getElementById("fileDemo");
  if (el) el.textContent = demo;
}

// ============================================================
// 设备列表渲染 (含「角色」列)
// ============================================================
function renderDeviceTable(devices) {
  const newMap = {};
  devices.forEach(d => newMap[d.ip] = d);
  state.devices = newMap;

  const tbody = document.getElementById("device-tbody");
  if (!tbody) return;
  const isOM = state.cfg.csi_mode === "single";
  const colspanAll = 8;  // 角色列始终显示
  if (!devices.length) {
    tbody.innerHTML = `<tr><td colspan="${colspanAll}" class="empty-tip">等待设备出现... 点右上 "搜索" 主动发现</td></tr>`;
    // 角色列始终可见; 仅切换提示文案
    updateRoleHint(isOM);
    syncSenderDropdown();
    return;
  }
  const sorted = devices.slice().sort((a, b) => a.ip.localeCompare(b.ip));

  // 角色列始终可见; 仅切换提示文案
  updateRoleHint(isOM);

  // 增量更新: 避免每 250ms 重建 <select>
  const existing = tbody.querySelectorAll("tr[data-ip]");
  const existingIps = Array.from(existing).map(tr => tr.dataset.ip);
  const newIps = sorted.map(d => d.ip);
  const sameIps = existingIps.length === newIps.length &&
                  existingIps.every((ip, i) => ip === newIps[i]);

  if (!sameIps) {
    tbody.innerHTML = sorted.map((d, idx) => buildDeviceRow(d, idx, isOM)).join("");
  } else {
    sorted.forEach((d, idx) => {
      const tr = tbody.querySelector(`tr[data-ip="${d.ip}"]`);
      if (!tr) return;
      const idxNum = tr.querySelector(".idx-num");
      if (idxNum) idxNum.textContent = idx + 1;
      const cb = tr.querySelector(".idx-check");
      if (cb && cb.checked !== d.selected) cb.checked = d.selected;
      // 在线
      const onlineCell = tr.children[3];
      if (onlineCell) {
        onlineCell.innerHTML = d.online
          ? `<span class="online-yes">在线</span>`
          : `<span class="online-no">离线</span>`;
      }
      // RSSI
      const rssiCellEl = tr.children[4];
      if (rssiCellEl) {
        rssiCellEl.innerHTML = d.rssi != null ? rssiCell(d.rssi) : "--";
      }
      // 实测Hz
      const actCell = tr.children[5];
      if (actCell) {
        actCell.innerHTML = d.actual_hz != null
          ? `<span class="${actualClass(state.cfg.sample_rate, d.actual_hz)}">${d.actual_hz}</span>`
          : "--";
      }
      // 角色 (始终更新; 模式决定显示内容) - buildRoleCell 返回完整 <td>
      const roleCell = tr.children[6];
      if (roleCell) roleCell.outerHTML = buildRoleCell(d.ip, d.online, isOM);
      // 位置下拉框: 仅在不同时更新
      const posSel = tr.querySelector(".pos-select");
      if (posSel && posSel.value !== (d.position || "")) {
        posSel.value = d.position || "";
      }
      // 行样式
      tr.classList.toggle("active-row", d.ip === state.activeIp);
      const isSender = isOM && state.cfg.sender_ip === d.ip;
      tr.classList.toggle("sender-row", isSender);
    });
  }
  // 发送端警告 + 校验开始按钮
  validateStart();
  syncSenderDropdown();
}

function buildRoleCell(ip, online, isOM) {
  // 多发多收: 每台设备既是发送端又是接收端 -> 显示 S/R (不可点击)
  if (!isOM) {
    return `<td class="role-td"><div class="role-cell role-cell-both" title="多发多收: 此设备既发送又接收">
      <span class="role-tag-both">S/R</span>
    </div></td>`;
  }
  // 一发多收: 用真实 <button> 渲染, 自带可点击 + disabled 语义
  const isSender = state.cfg.sender_ip === ip;
  const label = isSender ? "S" : "R";
  const cls = isSender ? "role-tag-tx" : "role-tag-rx";
  const title = !online
    ? "离线设备不可设为发送端"
    : (isSender ? "当前发送端 (点击其他行切换)" : "点击切换此设备为发送端 (S)");
  const disabled = online ? "" : "disabled";
  return `<td class="role-td"><button type="button"
        class="role-cell role-cell-clickable ${cls}"
        data-ip="${esc(ip)}"
        title="${title}"
        ${disabled}>${label}</button></td>`;
}

// 角色提示文案 (根据当前模式切换)
function updateRoleHint(isOM) {
  const el = document.getElementById("roleHint");
  if (!el) return;
  if (isOM) {
    el.textContent = "一发多收: 一台 ESP 作为发送端 (S) 通过 ESP-NOW 发送广播包，其余 ESP 接收 (R)。点击任意一行的「角色」单元格可把该设备设为 S，其他设备自动变为 R。";
    el.style.display = "";
  } else {
    el.textContent = "多发多收: 所有 ESP 互为发送端 (S) 与接收端 (R)。如需切换至一发多收，请在控制栏选择。";
    el.style.display = "";
  }
}

function buildDeviceRow(d, idx, isOM) {
  const checked = d.selected ? "checked" : "";
  const online = d.online ? `<span class="online-yes">在线</span>` : `<span class="online-no">离线</span>`;
  const rssi = d.rssi != null ? rssiCell(d.rssi) : "--";
  const actual = d.actual_hz != null
    ? `<span class="${actualClass(state.cfg.sample_rate, d.actual_hz)}">${d.actual_hz}</span>`
    : "--";
  const activeCls = (d.ip === state.activeIp) ? "active-row" : "";
  const isSender = isOM && state.cfg.sender_ip === d.ip;
  const senderCls = isSender ? " sender-row" : "";
  const posOpts = state.positionOptions.map(o =>
    `<option value="${esc(o)}"${o === d.position ? " selected" : ""}>${esc(o)}</option>`
  ).join("");
  const knownOpts = state.positionOptions;
  const posTail = (d.position && !knownOpts.includes(d.position))
    ? `<option value="${esc(d.position)}" selected>${esc(d.position)} (自定义)</option>`
    : "";
  // buildRoleCell 已返回完整 <td>, 直接嵌入
  const roleCell = buildRoleCell(d.ip, d.online, isOM);
  return `<tr class="${activeCls}${senderCls}" data-ip="${d.ip}">
    <td class="idx-cell" title="单击切换选中">
      <span class="idx-inner">
        <input type="checkbox" class="idx-check" ${checked}>
        <span class="idx-num">${idx + 1}</span>
      </span>
    </td>
    <td>${esc(d.name)}</td>
    <td>${esc(d.ip)}</td>
    <td>${online}</td>
    <td>${rssi}</td>
    <td>${actual}</td>
    ${roleCell}
    <td class="pos-cell">
      <select class="pos-select">${posTail}${posOpts}</select>
    </td>
    <td class="diag-cell">
      <button type="button" class="diag-btn" data-ip="${esc(d.ip)}" title="查看诊断信息 (last_seen / total_packets / rssi / active / applied_rate)">🔍</button>
    </td>
  </tr>`;
}

// 一次性绑定 tbody 事件委托
let _tbodyDelegationBound = false;
function bindTbodyDelegation() {
  if (_tbodyDelegationBound) return;
  _tbodyDelegationBound = true;
  const tbody = document.getElementById("device-tbody");
  if (!tbody) return;
  tbody.addEventListener("click", (e) => {
    const tr = e.target.closest("tr[data-ip]");
    if (!tr) return;
    const ip = tr.dataset.ip;
    // 1) 复选框 → 切换选中
    if (e.target.classList.contains("idx-check")) {
      toggleSelected(ip, e.target.checked);
      return;
    }
    // 2) 角色按钮 (一发多收模式下点击设置发送端)
    const roleBtn = e.target.closest("button.role-cell-clickable");
    if (roleBtn && !roleBtn.disabled) {
      console.log(`[role] click → set sender = ${ip}`);
      changeSender(ip);
      return;
    }
    // 3) 诊断按钮 → 弹诊断窗 (不触发选中)
    const diagBtn = e.target.closest("button.diag-btn");
    if (diagBtn) {
      showDiagModal(ip);
      return;
    }
    // 4) 位置下拉框 → 不触发选中
    if (e.target.classList.contains("pos-select")) return;
    // 5) 其它 → 选中此设备
    selectDevice(ip);
  });
  tbody.addEventListener("change", (e) => {
    if (e.target.classList.contains("pos-select")) {
      const tr = e.target.closest("tr[data-ip]");
      if (tr) changePosition(tr.dataset.ip, e.target.value);
    }
  });
}

function rssiCell(v) {
  const cls = v > -60 ? "rssi-strong" : (v > -80 ? "rssi-medium" : "rssi-weak");
  return `<span class="${cls}">${v}</span>`;
}

function actualClass(cfg, actual) {
  if (!cfg || cfg <= 0 || !actual) return "actual-good";
  const ratio = actual / cfg;
  if (ratio >= 0.85 && ratio <= 1.15) return "actual-good";
  if (ratio >= 0.5) return "actual-warn";
  return "actual-bad";
}

function esc(s) {
  return String(s || "").replace(/[&<>]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;"}[c]));
}

async function toggleSelected(ip, selected) {
  const r = await apiPost("/api/select", { ip, selected });
  if (!r.ok) showToast(`✗ ${r.msg}`, "red");
}

async function selectAll(selected) {
  const r = await apiPost("/api/select_all", { selected });
  if (!r.ok) showToast(`✗ ${r.msg}`, "red");
}

async function probeDevices() {
  const r = await apiPost("/api/probe");
  if (!r.ok) showToast(`✗ ${r.msg}`, "red");
}

// 切换发送端: 仅在一发多收模式下有效 (点击设备表「角色」列的 S/R 按钮)
async function changeSender(ip) {
  if (state.cfg.csi_mode !== "single") return;
  const old = state.cfg.sender_ip;
  if (old === ip) {
    console.log(`[role] ${ip} 已是发送端, 无需切换`);
    return;
  }
  state.cfg.sender_ip = ip;
  renderDeviceTable(Object.values(state.devices));
  // 调用后端 /api/mode 下发, 然后用返回值校正 sender_ip
  await applyMode();
  // 兜底: 如果后端返回的 sender_ip 与用户点击不一致, 回退
  if (state.cfg.csi_mode === "single" && state.cfg.sender_ip && state.cfg.sender_ip !== ip) {
    console.warn(`[role] 后端返回 sender_ip=${state.cfg.sender_ip}, 与点击 ${ip} 不一致`);
    renderDeviceTable(Object.values(state.devices));
  }
}

function selectDevice(ip) {
  if (!ip) return;
  document.querySelectorAll(".device-table tbody tr").forEach(tr => {
    tr.classList.toggle("active-row", tr.dataset.ip === ip);
  });
  if (state.activeIp === ip) return;
  state.activeIp = ip;
  state.canvasBuf = [];
  drawWave();
  const dev = state.devices[ip];
  if (dev) {
    const optimistic = {
      ip: dev.ip,
      name: dev.name,
      mac: "等待连接...",
      online: dev.online,
      rssi: dev.rssi,
      rssi_pct: null,
      csi_len: 0,
      total_packets: 0,
      running_packets: 0,
      tcp_status: "未连接",
      position: dev.position || "",
      actual_rate: 0,
    };
    renderActiveDetail(optimistic);
  }
  apiPost("/api/active", { ip });
}

async function changePosition(ip, position) {
  if (!position) return;
  const r = await apiPost("/api/position", { ip, position });
  if (!r.ok) showToast(`✗ ${r.msg}`, "red");
  else showToast(`✓ 已设置位置: ${position}`, "green", 1500);
}

/* ============================================================
   设备诊断弹窗 — 排查"在线但看不到 CSI / RSSI 不更新"
   ============================================================ */
async function showDiagModal(ip) {
  const r = await apiGet(`/api/diag/${encodeURIComponent(ip)}`);
  let body;
  if (!r || !r.ok) {
    body = `<div class="diag-err">拉取诊断失败: ${esc(r ? r.msg : "无响应")}</div>`;
  } else {
    const rows = [
      ["IP",         r.ip],
      ["名称",        r.name],
      ["在线",        r.online ? "✓ 是" : "✗ 否"],
      ["RSSI",       r.rssi != null ? `${r.rssi} dBm` : "--"],
      ["CSI 长度",   r.csi_len != null ? `${r.csi_len} 字节` : "--"],
      ["包序号",     r.seq],
      ["总包数",     r.total_packets],
      ["本次会话包数", r.running_packets],
      ["上次收到包",  r.last_seen_ago != null ? `${r.last_seen_ago} 秒前` : "--"],
      ["上次 HELLO",  r.last_hello_ago != null ? `${r.last_hello_ago} 秒前` : "--"],
      ["TCP 状态",   r.tcp_status || "--"],
      ["ESP 状态",   r.esp_state || "--"],
      ["是否 active", r.is_active ? "✓ 是 (正在绘波形)" : "✗ 否 (点击行选中即可绘波形)"],
      ["已下发频率",  r.applied_rate != null ? `${r.applied_rate} Hz` : "未下发"],
      ["实测频率",    r.actual_rate > 0 ? `${r.actual_rate} Hz` : "--"],
      ["ESP STATUS", r.esp_status ? JSON.stringify(r.esp_status) : "(tcp_listener 未上报)"],
    ];
    let hint = "";
    if (!r.is_active) {
      hint = `<div class="diag-hint warn">提示: 此设备当前不是 active, 波形 / RSSI 不刷新属正常. 点击本行其他位置即可选中.</div>`;
    } else if (r.total_packets === 0 && r.applied_rate > 0) {
      hint = `<div class="diag-hint bad">提示: 已下发 ${r.applied_rate}Hz 但 0 包, 固件没在发包. 建议重新下发 SAMPLE_RATE ${r.applied_rate} 或检查设备.</div>`;
    } else if (r.applied_rate === 0) {
      hint = `<div class="diag-hint warn">提示: 已下发频率为 0 (用户暂停了主动 ping), CSI 只能被动捕获 beacon, 频率不可控.</div>`;
    } else if (r.actual_rate > 0 && Math.abs(r.actual_rate - r.applied_rate) / r.applied_rate > 0.15) {
      hint = `<div class="diag-hint warn">提示: 实测 ${r.actual_rate}Hz 与配置 ${r.applied_rate}Hz 偏差 &gt; 15%, 检查 wifi 环境.</div>`;
    } else if (r.total_packets > 0) {
      hint = `<div class="diag-hint ok">提示: 一切正常, 数据在跑.</div>`;
    }
    body = `<table class="diag-table">${
      rows.map(([k, v]) => `<tr><td class="diag-k">${esc(k)}</td><td class="diag-v">${esc(String(v))}</td></tr>`).join("")
    }</table>${hint}`;
  }
  showModal(`设备诊断 — ${ip}`, body);
}

/* 通用 modal: 标题 + body, 点背景 / 关闭按钮消失 */
function showModal(title, bodyHtml) {
  let overlay = document.getElementById("diag-modal-overlay");
  if (!overlay) {
    overlay = document.createElement("div");
    overlay.id = "diag-modal-overlay";
    overlay.className = "diag-modal-overlay";
    overlay.innerHTML = `<div class="diag-modal">
        <div class="diag-modal-header">
          <span id="diag-modal-title"></span>
          <button class="diag-modal-close" type="button">×</button>
        </div>
        <div id="diag-modal-body" class="diag-modal-body"></div>
      </div>`;
    document.body.appendChild(overlay);
    overlay.addEventListener("click", (e) => {
      if (e.target === overlay || e.target.classList.contains("diag-modal-close")) {
        overlay.style.display = "none";
      }
    });
  }
  document.getElementById("diag-modal-title").textContent = title;
  document.getElementById("diag-modal-body").innerHTML = bodyHtml;
  overlay.style.display = "flex";
}

// 一发多收模式 + 发送端离线 → 显示警告 + 禁用开始按钮
function validateStart() {
  const isOM = state.cfg.csi_mode === "single";
  const senderDev = state.devices[state.cfg.sender_ip];
  const bad = isOM && (!senderDev || !senderDev.online);
  const warn = document.getElementById("senderWarn");
  if (warn) warn.style.display = bad ? "" : "none";
  const el = document.getElementById("btn-start");
  if (!el) return;
  const stopBtn = document.getElementById("btn-stop");
  const isRunning = stopBtn && !stopBtn.disabled;
  el.disabled = bad || isRunning;
}

// ============================================================
// 选中设备详情渲染 (底部状态栏)
// ============================================================
function renderActiveDetail(d) {
  if (!d) {
    ["st-mac", "st-csi-len", "st-tcp", "st-cfg-hz"]
      .forEach(id => {
        const el = document.getElementById(id);
        if (el) { el.textContent = "--"; el.style.color = ""; }
      });
    document.getElementById("st-cur").textContent = "0";
    document.getElementById("st-total").textContent = "0";
    document.getElementById("st-tcp").textContent = "未连接";
    document.getElementById("st-rssi-bar").style.width = "0%";
    return;
  }
  const macEl = document.getElementById("st-mac");
  if (d.mac && d.mac !== "等待连接..." && d.mac !== "--") {
    macEl.textContent = d.mac;
  } else if (d.name) {
    macEl.textContent = `${d.name} (${(d.ip || "").split(".").pop()})`;
  } else {
    macEl.textContent = d.ip || "--";
  }
  document.getElementById("st-csi-len").textContent = d.csi_len || "--";
  document.getElementById("st-total").textContent = d.total_packets || 0;
  document.getElementById("st-cur").textContent = d.running_packets || 0;

  const cfgHzEl = document.getElementById("st-cfg-hz");
  if (state.cfg.sample_rate_enabled) {
    cfgHzEl.textContent = `${state.cfg.sample_rate || 100}`;
  } else {
    cfgHzEl.textContent = "广播";
  }
  cfgHzEl.style.color = state.cfg.sample_rate_enabled ? "#1e88e5" : "#a0aec0";

  const tcpEl = document.getElementById("st-tcp");
  tcpEl.textContent = d.tcp_status || "未连接";
  tcpEl.style.color = (d.tcp_status && d.tcp_status.indexOf("已连接") >= 0)
                      ? "#38a169" : "#a0aec0";

  const bar = document.getElementById("st-rssi-bar");
  if (d.rssi_pct != null) {
    bar.style.width = d.rssi_pct + "%";
    bar.classList.remove("warn", "bad");
    if (d.rssi <= -80) bar.classList.add("bad");
    else if (d.rssi <= -60) bar.classList.add("warn");
  } else {
    bar.style.width = "0%";
  }
}

// ============================================================
// 状态栏: 模式
// ============================================================
function renderStatusContext(st, cfg) {
  if (!st) return;
  const modeEl = document.getElementById("st-mode");
  let modeText = "空闲", modeColor = "#a0aec0";
  if (st.countdown_active) { modeText = "⏳ 倒计时"; modeColor = "#dd6b20"; }
  else if (st.running && st.timed_mode) { modeText = "⏱ 按窗口"; modeColor = "#1e88e5"; }
  else if (st.running && st.frame_mode) { modeText = "🎯 按帧数"; modeColor = "#7b1fa2"; }
  else if (st.running) { modeText = "▶ 自由采集"; modeColor = "#38a169"; }
  modeEl.textContent = modeText;
  modeEl.style.color = modeColor;
}

// ============================================================
// 中部状态栏渲染
// ============================================================
function renderStatus(st) {
  const base = document.querySelector("#status-big .status-base");
  const ext  = document.getElementById("status-ext");
  const box  = document.getElementById("status-big");

  const actualTop = document.getElementById("cfg-sr-actual");
  if (st.avg_actual_hz) {
    const cfg = st.cfg_hz || state.cfg.sample_rate;
    const cls = actualClass(cfg, st.avg_actual_hz);
    actualTop.textContent = `${Math.round(st.avg_actual_hz)} Hz`;
    actualTop.className = "val-actual " + (cls === "actual-good" ? "green"
      : cls === "actual-warn" ? "orange" : "red");
  } else {
    actualTop.textContent = "-- Hz";
    actualTop.className = "val-actual";
  }
  if (st.cfg_hz && document.getElementById("cfg-sr") !== document.activeElement) {
    document.getElementById("cfg-sr").value = st.cfg_hz;
  }
  if (st.countdown_active && !state.countdownActive) {
    state.countdownActive = true;
    showCountdown(state.cfg.delay_seconds || 3);
  } else if (!st.countdown_active && state.countdownActive) {
    state.countdownActive = false;
    hideCountdown();
  }

  if (!st.running) {
    box.className = "status-big status-idle";
    base.textContent = "空闲";
    ext.textContent = "";
    setStartBtnsDisabled(false);
    setStopBtnDisabled(true);
    // 重新校验发送端警告
    validateStart();
    return;
  }
  setStartBtnsDisabled(true);
  setStopBtnDisabled(false);
  let baseText = "";
  let extText = "";
  let cls = "status-green";
  const n = st.saved_files + 1;
  const tn = st.target_files || 1;
  const elapsed = st.elapsed_sec || 0;
  if (st.timed_mode) {
    baseText = `⏱ 窗口采集中`;
    extText = `${n}/${tn} 文件 · 已采集 ${elapsed}s`;
    cls = "status-green";
  } else if (st.frame_mode) {
    baseText = `🎯 按帧采集中`;
    const target = st.target_frames || 0;
    const leaderPackets = st.leader_packets || 0;
    const leaderName = st.leader_name || "";
    const pct = target > 0 ? Math.round(leaderPackets * 100 / target) : 0;
    let leaderText = "";
    if (leaderName && st.leader_ip && st.leader_packets >= target) {
      leaderText = ` · ${leaderName} 达标`;
    } else if (leaderName) {
      leaderText = ` · ${leaderName}`;
    }
    extText = `${n}/${tn} 文件 · ${leaderPackets}/${target}帧 (${pct}%)${leaderText}`;
    cls = "status-purple";
  } else {
    baseText = `▶ 自由采集中`;
    if (elapsed >= 60) {
      const m = Math.floor(elapsed / 60), s = elapsed % 60;
      extText = `已采集 ${m}分${s}秒`;
    } else {
      extText = `已采集 ${elapsed}s`;
    }
    cls = "status-blue";
  }
  box.className = "status-big " + cls;
  base.textContent = baseText;
  ext.textContent = extText;
}

function setStartBtnsDisabled(d) {
  const el = document.getElementById("btn-start");
  if (el) el.disabled = d;
}
function setStopBtnDisabled(d) {
  document.getElementById("btn-stop").disabled = d;
}

// ============================================================
// 倒计时 UI
// ============================================================
function showCountdown(remaining) {
  const overlay = document.getElementById("countdown-overlay");
  const num = document.getElementById("countdown-number");
  const hint = document.getElementById("countdown-hint");
  num.textContent = remaining;
  overlay.classList.remove("hidden");
  const mode = state._activeStartMode || "window";
  const modeText = mode === "window" ? "按窗口采集"
                 : mode === "frame"  ? "按帧数采集"
                 :                     "自由采集";
  if (state.cfg.voice_enabled && state.cfg.delay_enabled) {
    hint.textContent = `语音倒计时 + 播报 "${modeText}开始"...`;
  } else if (state.cfg.voice_enabled) {
    hint.textContent = `语音播报 "${modeText}开始" 中...`;
  } else if (state.cfg.delay_enabled) {
    hint.textContent = `等待开始采集...`;
  } else {
    hint.textContent = `准备开始...`;
  }
}

function hideCountdown() {
  document.getElementById("countdown-overlay").classList.add("hidden");
  state.countdownActive = false;
}

// ============================================================
// 采集控制按钮 (单一「开始采集」按钮 + 合并「采集方式」下拉)
// ============================================================
function collectMode() {
  // 优先读 DOM (用户可能改了 UI 但还没触发 change), 兜底 cfg
  const sel = document.getElementById("collect-mode");
  if (sel && sel.value) return sel.value;
  return state._collectMode || "window";
}

// 切换 采集方式 时显示对应参数
function updateCollectParams() {
  const m = collectMode();
  state._collectMode = m;
  const w = document.getElementById("collect-params-window");
  const f = document.getElementById("collect-params-frame");
  const fr = document.getElementById("collect-params-free");
  if (w) w.style.display = (m === "window") ? "inline-flex" : "none";
  if (f) f.style.display = (m === "frame") ? "inline-flex" : "none";
  if (fr) fr.style.display = (m === "free") ? "inline-flex" : "none";
}

async function startCollect() {
  const sel = Object.values(state.devices).filter(d => d.selected && d.online);
  if (!sel.length) {
    showToast("请先勾选至少一台在线设备", "orange");
    return;
  }
  const mode = collectMode();
  state._activeStartMode = mode;
  let r;
  if (mode === "window") {
    r = await apiPost("/api/start_window", {
      win_sec: state.cfg.win_len,
      target_files: state.cfg.win_files,
    });
  } else if (mode === "frame") {
    r = await apiPost("/api/start_frame", {
      target_frames: state.cfg.target_frames,
      target_files: state.cfg.target_files,
    });
  } else {
    r = await apiPost("/api/start_free");
  }
  if (!r.ok) {
    showToast(`✗ ${r.msg}`, "red");
    state._activeStartMode = null;
  }
}

async function stopCollect() {
  const r = await apiPost("/api/stop");
  if (!r.ok) showToast(`✗ ${r.msg}`, "red");
  state._activeStartMode = null;
}

// ============================================================
// 视频 / 可视化工具
// ============================================================
function browseSaveDir() {
  const cur = document.getElementById("cfg-save-dir").value;
  const v = prompt("CSI 保存根目录:", cur);
  if (v) {
    document.getElementById("cfg-save-dir").value = v;
    state.cfg.save_dir = v;
    pushCfgToServer();
  }
}
async function checkPhone() {
  if (!state.cfg.phone_ip) {
    showToast("请先填写手机 IP", "orange", 2000);
    return;
  }
  document.getElementById("phone-status").textContent = "检测中...";
  document.getElementById("phone-status").style.color = "#dd6b20";
  await apiPost("/api/phone_ip", { ip: state.cfg.phone_ip });
}
async function openVideoDir() {
  const r = await apiPost("/api/open_video_dir");
  if (!r.ok) showToast(`✗ ${r.msg}`, "red");
}
async function openVisualizer() {
  const r = await apiPost("/api/open_visualizer");
  if (!r.ok) showToast(`✗ ${r.msg}`, "red");
}
async function voiceTest() {
  const r = await apiPost("/api/voice_test");
  if (!r.ok) showToast(`✗ ${r.msg}`, "red");
}

// ============================================================
// Canvas 实时波形绘制
// ============================================================
let canvasCtx = null;

function bindCanvasResize() {
  window.addEventListener("resize", () => drawWave());
}

function drawEmptyWave() {
  drawWave();
}

function drawWave() {
  const c = document.getElementById("wave-canvas");
  if (!c) return;
  const dpr = window.devicePixelRatio || 1;
  const cssW = c.clientWidth;
  const cssH = c.clientHeight;
  if (c.width !== Math.floor(cssW * dpr) || c.height !== Math.floor(cssH * dpr)) {
    c.width = Math.floor(cssW * dpr);
    c.height = Math.floor(cssH * dpr);
  }
  if (!canvasCtx) canvasCtx = c.getContext("2d");
  const ctx = canvasCtx;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssW, cssH);

  ctx.fillStyle = "#fafbfc";
  ctx.fillRect(0, 0, cssW, cssH);

  ctx.strokeStyle = "#e2e8f0";
  ctx.lineWidth = 1;
  const padL = 38, padR = 8, padT = 8, padB = 22;
  const plotW = cssW - padL - padR;
  const plotH = cssH - padT - padB;
  ctx.beginPath();
  for (let i = 0; i <= 4; i++) {
    const y = padT + (plotH * i) / 4;
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
  }
  ctx.stroke();

  ctx.fillStyle = "#718096";
  ctx.font = "10px Microsoft YaHei, sans-serif";
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let v = 50; v >= -50; v -= 25) {
    const y = padT + plotH * (1 - (v + 50) / 100);
    ctx.fillText(`${v}`, padL - 4, y);
  }
  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  ctx.fillText("0", padL, padT + plotH + 2);
  ctx.fillText(`${state.canvasMax}`, padL + plotW, padT + plotH + 2);
  ctx.fillStyle = "#a0aec0";
  ctx.fillText("帧 →", padL + plotW / 2, padT + plotH + 2);

  if (!state.canvasBuf.length) {
    ctx.fillStyle = "#a0aec0";
    ctx.font = "12px Microsoft YaHei, sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText("等待 CSI 数据...", padL + plotW / 2, padT + plotH / 2);
    return;
  }
  const T = state.canvasBuf.length;
  const xPer = plotW / state.canvasMax;
  for (let s = 0; s < SC_IDX.length; s++) {
    ctx.strokeStyle = SC_COLORS[s];
    ctx.lineWidth = 1.4;
    ctx.beginPath();
    let started = false;
    for (let i = 0; i < T; i++) {
      const v = state.canvasBuf[i][s];
      if (v == null || isNaN(v)) continue;
      const x = padL + (i + (state.canvasMax - T)) * xPer;
      const y = padT + plotH * (1 - (v + 50) / 100);
      if (!started) { ctx.moveTo(x, y); started = true; }
      else ctx.lineTo(x, y);
    }
    ctx.stroke();
  }

  const title = document.getElementById("wave-title");
  if (state.activeDetail) {
    const cfg = state.cfg.sample_rate_enabled
      ? `${state.cfg.sample_rate || 100}Hz` : "广播";
    const act = state.activeDetail.actual_rate > 0.5
      ? `${Math.round(state.activeDetail.actual_rate)}Hz`
      : "--";
    title.textContent = `${state.activeDetail.name} · 配置 ${cfg} · 实测 ${act} · ${T} 帧 · SC 8/24/40/56`;
  } else if (state.activeIp) {
    title.textContent = `设备 ${state.activeIp} · ${T} 帧`;
  } else {
    title.textContent = "CSI 实时信号 (请选择设备)";
  }
}

// ============================================================
// Toast 提示
// ============================================================
function showToast(text, level = "gray", autoClearMs = 3000) {
  const host = document.getElementById("toast-host");
  const el = document.createElement("div");
  el.className = "toast " + level;
  el.textContent = text;
  host.appendChild(el);
  if (autoClearMs > 0) {
    setTimeout(() => {
      el.style.opacity = "0";
      el.style.transform = "translateX(20px)";
      el.style.transition = "all 0.3s";
      setTimeout(() => el.remove(), 300);
    }, autoClearMs);
  }
}