const FIXED_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "TAOUSDT", "ZECUSDT", "PEPEUSDT"];

function directionClass(dir) {
  if (dir === "上涨") return "up";
  if (dir === "下跌") return "down";
  return "flat";
}

function directionText(dir) {
  if (dir === "上涨") return "上涨 ↑";
  if (dir === "下跌") return "下跌 ↓";
  return "横盘 -";
}

function fmtPct(conf) {
  const n = Number(conf || 0);
  if (!Number.isFinite(n)) return "-";
  return `${(n * 100).toFixed(0)}%`;
}

function fmtTime(iso) {
  if (!iso) return "-";
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "-";
    return d.toLocaleString("zh-CN", { hour12: false });
  } catch (_) {
    return "-";
  }
}

function renderCards(data) {
  const box = document.getElementById("cards");
  if (!box) return;

  const state = (data && data.state) || {};
  const dirs = state.lastDirections || {};
  const confs = state.lastConfidence || {};

  box.innerHTML = FIXED_SYMBOLS.map((s) => {
    const dir = String(dirs[s] || "横盘");
    const cls = directionClass(dir);
    const conf = fmtPct(confs[s]);
    return `
      <article class="card">
        <div class="symbol">${s}</div>
        <div class="dir ${cls}">${directionText(dir)}</div>
        <div class="meta">信心：${conf}</div>
      </article>
    `;
  }).join("");
}

function renderMeta(data) {
  const enabledEl = document.getElementById("systemEnabled");
  const intervalEl = document.getElementById("systemInterval");
  const horizonEl = document.getElementById("systemHorizon");
  const runEl = document.getElementById("lastRunAt");
  const errEl = document.getElementById("lastError");

  if (!enabledEl || !intervalEl || !horizonEl || !runEl || !errEl) return;

  enabledEl.textContent = data && data.enabled ? "运行中" : "未启用";
  intervalEl.textContent = `${Number((data && data.predictIntervalMinutes) || 1)} 分钟/次`;
  horizonEl.textContent = `${Number((data && data.horizonMinutes) || 10)} 分钟`;

  const state = (data && data.state) || {};
  runEl.textContent = fmtTime(state.lastRunAt);
  errEl.textContent = state.lastError ? String(state.lastError) : "无";
}

async function refreshDashboard() {
  const statusText = document.getElementById("statusText");
  try {
    if (statusText) statusText.textContent = "刷新中...";
    const res = await fetch("/api/trend-status", { cache: "no-store" });
    const payload = await res.json();
    if (!payload.ok || !payload.data) {
      throw new Error(payload.message || "trend-status 返回异常");
    }
    renderCards(payload.data);
    renderMeta(payload.data);
    if (statusText) statusText.textContent = `已更新：${new Date().toLocaleTimeString("zh-CN", { hour12: false })}`;
  } catch (error) {
    if (statusText) statusText.textContent = `刷新失败：${String(error.message || error)}`;
  }
}

document.getElementById("refreshBtn")?.addEventListener("click", () => {
  refreshDashboard().catch(() => {});
});

setInterval(() => {
  refreshDashboard().catch(() => {});
}, 15000);

refreshDashboard().catch(() => {});
