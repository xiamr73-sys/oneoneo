const windowHoursEl = document.getElementById("windowHours");
const stageFilterEl = document.getElementById("stageFilter");
const symbolSearchEl = document.getElementById("symbolSearch");
const refreshBtnEl = document.getElementById("refreshBtn");
const autoRefreshEl = document.getElementById("autoRefresh");
const tableBodyEl = document.getElementById("tableBody");
const summaryTextEl = document.getElementById("summaryText");
const hintTextEl = document.getElementById("hintText");
const sortHitsBtnEl = document.getElementById("sortHitsBtn");
const sortCooldownBtnEl = document.getElementById("sortCooldownBtn");
const sortStageBtnEl = document.getElementById("sortStageBtn");
const sortLastFakeBtnEl = document.getElementById("sortLastFakeBtn");

const MONITOR_WS_SOURCE = String(window.MONITOR_WS_SOURCE || "binance").trim().toLowerCase() === "aster"
  ? "aster"
  : "binance";

let allRows = [];
let refreshTimer = null;
let meta = {};
let sortState = { key: "hits_current", order: "desc" };

const STAGE_ORDER = {
  READY: 4,
  COOLDOWN: 3,
  TREND: 2,
  HITS: 1
};

function esc(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function fmtNum(v, digits = 1) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "-";
  return n.toFixed(digits);
}

function fmtAgo(tsSec) {
  const ts = Number(tsSec || 0);
  if (!Number.isFinite(ts) || ts <= 0) return "-";
  const diffSec = Math.max(0, Math.floor(Date.now() / 1000 - ts));
  if (diffSec < 60) return `${diffSec}s前`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m前`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h前`;
  return `${Math.floor(diffSec / 86400)}d前`;
}

function fmtBjtOrTs(bjt, ts) {
  const s = String(bjt || "").trim();
  if (s) return s;
  const n = Number(ts || 0);
  if (!Number.isFinite(n) || n <= 0) return "-";
  return new Date(n * 1000).toLocaleString("zh-CN", { hour12: false });
}

function stageClass(stage) {
  const s = String(stage || "").toUpperCase();
  if (s === "READY") return "stage-ready";
  if (s === "HITS") return "stage-hits";
  if (s === "TREND") return "stage-trend";
  if (s === "COOLDOWN") return "stage-cooldown";
  return "stage-hits";
}

function stageText(stage) {
  const s = String(stage || "").toUpperCase();
  if (s === "READY") return "READY";
  if (s === "HITS") return "卡HITS";
  if (s === "TREND") return "卡TREND";
  if (s === "COOLDOWN") return "卡COOLDOWN";
  return s || "-";
}

function sortedRows(rows) {
  const out = [...rows];
  const { key, order } = sortState;
  out.sort((a, b) => {
    if (key === "hits_current") {
      if (Number(b.hits_current || 0) !== Number(a.hits_current || 0)) {
        return Number(b.hits_current || 0) - Number(a.hits_current || 0);
      }
      if (Number(b.last_fake_ts || 0) !== Number(a.last_fake_ts || 0)) {
        return Number(b.last_fake_ts || 0) - Number(a.last_fake_ts || 0);
      }
      return String(a.symbol || "").localeCompare(String(b.symbol || ""));
    }
    if (key === "cooldown_left_sec") {
      if (Number(b.cooldown_left_sec || 0) !== Number(a.cooldown_left_sec || 0)) {
        return Number(b.cooldown_left_sec || 0) - Number(a.cooldown_left_sec || 0);
      }
      return Number(b.hits_current || 0) - Number(a.hits_current || 0);
    }
    if (key === "stage") {
      const sa = STAGE_ORDER[String(a.stage || "").toUpperCase()] || 0;
      const sb = STAGE_ORDER[String(b.stage || "").toUpperCase()] || 0;
      if (sb !== sa) return sb - sa;
      return Number(b.hits_current || 0) - Number(a.hits_current || 0);
    }
    if (key === "last_fake_ts") {
      if (Number(b.last_fake_ts || 0) !== Number(a.last_fake_ts || 0)) {
        return Number(b.last_fake_ts || 0) - Number(a.last_fake_ts || 0);
      }
      return Number(b.hits_current || 0) - Number(a.hits_current || 0);
    }
    return 0;
  });
  if (order === "asc") out.reverse();
  return out;
}

function filteredRows(rows) {
  const kw = String(symbolSearchEl?.value || "").trim().toUpperCase();
  const stage = String(stageFilterEl?.value || "ALL").trim().toUpperCase();
  return rows.filter((r) => {
    if (kw && !String(r.symbol || "").toUpperCase().includes(kw)) return false;
    if (stage !== "ALL" && String(r.stage || "").toUpperCase() !== stage) return false;
    return true;
  });
}

function updateSortButtons() {
  const items = [
    { key: "hits_current", el: sortHitsBtnEl },
    { key: "cooldown_left_sec", el: sortCooldownBtnEl },
    { key: "stage", el: sortStageBtnEl },
    { key: "last_fake_ts", el: sortLastFakeBtnEl }
  ];
  for (const item of items) {
    if (!item.el) continue;
    item.el.classList.remove("active");
    if (item.key === sortState.key) {
      item.el.classList.add("active");
      item.el.textContent = sortState.order === "asc" ? "↑" : "↓";
    } else {
      item.el.textContent = "↕";
    }
  }
}

function setSortKey(key) {
  if (sortState.key === key) {
    sortState.order = sortState.order === "desc" ? "asc" : "desc";
  } else {
    sortState.key = key;
    sortState.order = "desc";
  }
  updateSortButtons();
  renderRows();
  renderSummary();
}

function renderRows() {
  const rows = sortedRows(filteredRows(allRows));
  if (!rows.length) {
    tableBodyEl.innerHTML = '<tr><td colspan="11" class="empty">当前条件无数据</td></tr>';
    return;
  }
  const html = rows.map((r, idx) => {
    const blockedHits = Boolean(r.blocked_hits);
    const blockedTrend = Boolean(r.blocked_trend);
    const blockedCooldown = Boolean(r.blocked_cooldown);
    const trendLine = [
      `centroid ${fmtNum(r.centroid_change_pct, 2)}% / vwap ${fmtNum(r.vwap_change_pct, 2)}%`,
      `EMA12-36: ${fmtNum(Number(r.ema_short_end || 0) - Number(r.ema_long_end || 0), 3)} | stair ${(Number(r.stair_up_ratio || 0) * 100).toFixed(0)}%`
    ].join("<br />");
    const modeText = String(r.trend_mode || "none").trim();
    return `
      <tr>
        <td>${idx + 1}</td>
        <td class="sym">${esc(r.symbol)}</td>
        <td class="count">${Number(r.hits_current || 0)}</td>
        <td>${Number(r.in_band_window || 0)} / ${Number(r.total_fake_window || 0)}</td>
        <td class="${blockedHits ? "bool-yes" : "bool-no"}">${blockedHits ? "是" : "否"}</td>
        <td class="${blockedTrend ? "bool-yes" : "bool-no"}">${blockedTrend ? "是" : "否"}</td>
        <td class="${blockedCooldown ? "bool-yes" : "bool-no"}">${blockedCooldown ? `是 (${Number(r.cooldown_left_sec || 0)}s)` : "否"}</td>
        <td><span class="stage-chip ${stageClass(r.stage)}">${stageText(r.stage)}</span></td>
        <td class="trend-cell">${trendLine}<br /><span class="trend-mode">mode: ${esc(modeText)}</span></td>
        <td>${esc(fmtBjtOrTs(r.last_fake_bjt, r.last_fake_ts))} (${fmtAgo(r.last_fake_ts)})</td>
        <td>${esc(fmtBjtOrTs(r.last_slow_bjt, r.last_slow_ts))}</td>
      </tr>`;
  }).join("");
  tableBodyEl.innerHTML = html;
}

function renderSummary() {
  const rows = filteredRows(allRows);
  const ready = rows.filter((r) => String(r.stage || "").toUpperCase() === "READY").length;
  const hits = rows.filter((r) => String(r.stage || "").toUpperCase() === "HITS").length;
  const trend = rows.filter((r) => String(r.stage || "").toUpperCase() === "TREND").length;
  const cooldown = rows.filter((r) => String(r.stage || "").toUpperCase() === "COOLDOWN").length;
  const top = sortedRows(rows)[0];
  const topText = top ? `${top.symbol} (${top.hits_current})` : "-";
  summaryTextEl.textContent = [
    `来源: ${MONITOR_WS_SOURCE}`,
    `窗口: ${Number(meta.window_hours || windowHoursEl?.value || 24)}h`,
    `总币种: ${rows.length}`,
    `READY: ${ready}`,
    `卡HITS: ${hits}`,
    `卡TREND: ${trend}`,
    `卡COOLDOWN: ${cooldown}`,
    `Top命中: ${topText}`
  ].join(" | ");
}

async function loadRows() {
  const windowHours = Number(windowHoursEl?.value || 24);
  const params = new URLSearchParams({
    source: MONITOR_WS_SOURCE,
    window_hours: String(windowHours),
    limit: "1200"
  });
  refreshBtnEl.disabled = true;
  hintTextEl.textContent = "加载中...";
  try {
    const res = await fetch(`/api/monitor-ws/slow-accum-breakdown?${params.toString()}`, { cache: "no-store" });
    const payload = await res.json();
    if (!payload.ok) throw new Error(payload.message || "unknown error");
    const data = payload.data || {};
    allRows = Array.isArray(data.rows) ? data.rows : [];
    meta = data;
    renderRows();
    renderSummary();
    hintTextEl.textContent = [
      `阈值: hits>=${Number(data.min_hits || 0)}`,
      `ret区间: ${fmtNum(data.min_ret_1m_pct, 3)}%~${fmtNum(data.max_ret_1m_pct, 3)}%`,
      `trend最小抬升: ${fmtNum(data.min_stair_step_pct, 2)}%`,
      `cooldown: ${Number(data.cooldown_sec || 0)}s`,
      `slow窗口: ${Number(data.slow_window_sec || 0)}s`,
      `数据库: ${data.db_file || "-"}`,
      `更新时间: ${new Date(data.generated_at || Date.now()).toLocaleString("zh-CN", { hour12: false })}`
    ].join(" | ");
  } catch (error) {
    allRows = [];
    renderRows();
    renderSummary();
    hintTextEl.textContent = `加载失败: ${String(error?.message || error)}`;
  } finally {
    refreshBtnEl.disabled = false;
  }
}

function toggleAutoRefresh() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
  if (!autoRefreshEl?.checked) return;
  refreshTimer = setInterval(() => {
    loadRows().catch(() => {});
  }, 10000);
}

refreshBtnEl?.addEventListener("click", () => loadRows().catch(() => {}));
windowHoursEl?.addEventListener("change", () => loadRows().catch(() => {}));
stageFilterEl?.addEventListener("change", () => {
  renderRows();
  renderSummary();
});
symbolSearchEl?.addEventListener("input", () => {
  renderRows();
  renderSummary();
});
autoRefreshEl?.addEventListener("change", toggleAutoRefresh);
sortHitsBtnEl?.addEventListener("click", () => setSortKey("hits_current"));
sortCooldownBtnEl?.addEventListener("click", () => setSortKey("cooldown_left_sec"));
sortStageBtnEl?.addEventListener("click", () => setSortKey("stage"));
sortLastFakeBtnEl?.addEventListener("click", () => setSortKey("last_fake_ts"));

loadRows()
  .then(() => {
    updateSortButtons();
    toggleAutoRefresh();
  })
  .catch((error) => {
    hintTextEl.textContent = `初始化失败: ${String(error?.message || error)}`;
  });
