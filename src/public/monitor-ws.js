const terminalPane = document.getElementById("terminalPane");
const runBadge = document.getElementById("runBadge");
const runMeta = document.getElementById("runMeta");
const runtimeHint = document.getElementById("runtimeHint");
const startRuntimeBtn = document.getElementById("startRuntimeBtn");
const refreshRuntimeBtn = document.getElementById("refreshRuntimeBtn");
const autoRefreshToggle = document.getElementById("autoRefreshToggle");

const discordMinScoreInput = document.getElementById("discordMinScore");
const saveScoreBtn = document.getElementById("saveScoreBtn");
const moduleGrid = document.getElementById("moduleGrid");
const enableAllBtn = document.getElementById("enableAllBtn");
const disableAllBtn = document.getElementById("disableAllBtn");
const saveModulesBtn = document.getElementById("saveModulesBtn");
const settingsHint = document.getElementById("settingsHint");

let moduleMeta = [];
let refreshTimer = null;

function escapeHtml(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatTime(isoText) {
  if (!isoText) return "-";
  const d = new Date(isoText);
  if (Number.isNaN(d.getTime())) return String(isoText);
  return d.toLocaleString("zh-CN", { hour12: false });
}

function setButtonsDisabled(disabled) {
  const state = Boolean(disabled);
  saveScoreBtn.disabled = state;
  saveModulesBtn.disabled = state;
  enableAllBtn.disabled = state;
  disableAllBtn.disabled = state;
}

function renderModuleSwitches(modules) {
  const blocks = moduleMeta.map((item) => {
    const checked = modules && modules[item.key] ? "checked" : "";
    return `
      <label class="module-item">
        <span>${escapeHtml(item.label || item.key)}</span>
        <input type="checkbox" data-module-key="${escapeHtml(item.key)}" ${checked} />
      </label>
    `;
  });
  moduleGrid.innerHTML = blocks.join("");
}

function collectModuleValues() {
  const out = {};
  const inputs = moduleGrid.querySelectorAll("input[type='checkbox'][data-module-key]");
  for (const input of inputs) {
    const key = input.getAttribute("data-module-key");
    if (!key) continue;
    out[key] = Boolean(input.checked);
  }
  return out;
}

async function loadSettings() {
  const res = await fetch("/api/monitor-ws/settings");
  const payload = await res.json();
  if (!payload.ok) {
    settingsHint.textContent = `读取配置失败：${payload.message || "unknown error"}`;
    return;
  }
  const data = payload.data || {};
  moduleMeta = Array.isArray(data.module_meta) ? data.module_meta : [];
  discordMinScoreInput.value = Number(data.discord_min_score ?? 50);
  renderModuleSwitches(data.modules || {});
  settingsHint.textContent = `配置文件：${data.control_file || "-"} | 最近更新：${formatTime(data.updated_at)}`;
}

async function saveSettings(patch, okMessage) {
  setButtonsDisabled(true);
  settingsHint.textContent = "保存中...";
  try {
    const res = await fetch("/api/monitor-ws/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(patch || {})
    });
    const payload = await res.json();
    if (!payload.ok) {
      settingsHint.textContent = `保存失败：${payload.message || "unknown error"}`;
      return;
    }
    await loadSettings();
    const msg = payload.message || okMessage || "保存成功";
    settingsHint.textContent = `${msg} (${formatTime(new Date().toISOString())})`;
  } catch (error) {
    settingsHint.textContent = `保存失败：${error?.message || "network error"}`;
  } finally {
    setButtonsDisabled(false);
  }
}

async function saveScore() {
  const score = Number(discordMinScoreInput.value);
  if (!Number.isFinite(score)) {
    settingsHint.textContent = "请输入有效数字分数。";
    return;
  }
  await saveSettings({ discord_min_score: score }, "分数已保存");
}

async function saveModules() {
  await saveSettings({ modules: collectModuleValues() }, "模块配置已保存");
}

async function refreshRuntime() {
  refreshRuntimeBtn.disabled = true;
  try {
    const res = await fetch("/api/monitor-ws/runtime?lines=180");
    const payload = await res.json();
    if (!payload.ok) {
      runBadge.textContent = "读取失败";
      runBadge.className = "badge";
      runMeta.textContent = payload.message || "unknown error";
      runtimeHint.textContent = formatTime(new Date().toISOString());
      if (startRuntimeBtn) startRuntimeBtn.disabled = false;
      return;
    }
    const data = payload.data || {};
    const proc = data.process || null;
    if (data.running && proc) {
      runBadge.textContent = "运行中";
      runBadge.className = "badge up-badge";
      runMeta.textContent = `PID ${proc.pid} | 运行时长 ${proc.etime}`;
      if (startRuntimeBtn) startRuntimeBtn.disabled = true;
    } else {
      runBadge.textContent = "未运行";
      runBadge.className = "badge down-badge";
      runMeta.textContent = "未检测到 monitor_ws 进程";
      if (startRuntimeBtn) startRuntimeBtn.disabled = false;
    }

    const cmd = proc && proc.command ? `> ${proc.command}` : "> (no process)";
    const logTail = data.log_tail ? data.log_tail : "(log is empty)";
    terminalPane.textContent = `${cmd}\n\n${logTail}`;
    runtimeHint.textContent = `日志文件：${data.log_file || "-"} | 最近检查：${formatTime(data.checked_at)}`;
  } catch (error) {
    runBadge.textContent = "读取失败";
    runBadge.className = "badge";
    runMeta.textContent = String(error?.message || "network error");
    if (startRuntimeBtn) startRuntimeBtn.disabled = false;
  } finally {
    refreshRuntimeBtn.disabled = false;
  }
}

async function startRuntime() {
  if (startRuntimeBtn) startRuntimeBtn.disabled = true;
  runtimeHint.textContent = "正在下发启动命令...";
  try {
    const res = await fetch("/api/monitor-ws/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({})
    });
    const payload = await res.json();
    if (!payload.ok) {
      runtimeHint.textContent = payload.message || "启动失败";
      return;
    }
    const msg = payload.message || "启动命令已下发";
    runtimeHint.textContent = `${msg} | ${formatTime(new Date().toISOString())}`;
    await refreshRuntime();
  } catch (error) {
    runtimeHint.textContent = `启动失败：${error?.message || "network error"}`;
  } finally {
    if (startRuntimeBtn) startRuntimeBtn.disabled = false;
  }
}

function setAllModules(checked) {
  const inputs = moduleGrid.querySelectorAll("input[type='checkbox'][data-module-key]");
  for (const input of inputs) {
    input.checked = Boolean(checked);
  }
}

function startAutoRefresh() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
  if (!autoRefreshToggle.checked) return;
  refreshTimer = setInterval(() => {
    refreshRuntime().catch(() => {});
  }, 5000);
}

refreshRuntimeBtn?.addEventListener("click", () => {
  refreshRuntime().catch(() => {});
});

startRuntimeBtn?.addEventListener("click", () => {
  startRuntime().catch(() => {});
});

autoRefreshToggle?.addEventListener("change", () => {
  startAutoRefresh();
});

saveScoreBtn?.addEventListener("click", () => {
  saveScore().catch(() => {});
});

saveModulesBtn?.addEventListener("click", () => {
  saveModules().catch(() => {});
});

enableAllBtn?.addEventListener("click", () => {
  setAllModules(true);
});

disableAllBtn?.addEventListener("click", () => {
  setAllModules(false);
});

(async () => {
  await Promise.all([loadSettings(), refreshRuntime()]);
  startAutoRefresh();
})().catch((error) => {
  settingsHint.textContent = `初始化失败：${error?.message || "unknown error"}`;
});
