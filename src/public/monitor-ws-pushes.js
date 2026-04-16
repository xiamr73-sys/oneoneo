const windowHoursEl = document.getElementById("windowHours");
const symbolSearchEl = document.getElementById("symbolSearch");
const refreshBtnEl = document.getElementById("refreshBtn");
const autoRefreshEl = document.getElementById("autoRefresh");
const tableBodyEl = document.getElementById("tableBody");
const summaryTextEl = document.getElementById("summaryText");
const hintTextEl = document.getElementById("hintText");
const internalChampionCardEl = document.getElementById("internalChampionCard");
const internalChampionSymbolEl = document.getElementById("internalChampionSymbol");
const internalChampionMetaEl = document.getElementById("internalChampionMeta");
const internalChampionNoteEl = document.getElementById("internalChampionNote");
const dcChampionCardEl = document.getElementById("dcChampionCard");
const dcChampionSymbolEl = document.getElementById("dcChampionSymbol");
const dcChampionMetaEl = document.getElementById("dcChampionMeta");
const dcChampionNoteEl = document.getElementById("dcChampionNote");
const sortCountBtnEl = document.getElementById("sortCountBtn");
const sortTimeBtnEl = document.getElementById("sortTimeBtn");
const sortLevelBtnEl = document.getElementById("sortLevelBtn");
const sortInternalScoreBtnEl = document.getElementById("sortInternalScoreBtn");
const sortDcScoreBtnEl = document.getElementById("sortDcScoreBtn");
const MONITOR_WS_SOURCE = String(window.MONITOR_WS_SOURCE || "binance").trim().toLowerCase() === "aster"
  ? "aster"
  : "binance";

let allRows = [];
let refreshTimer = null;
const CHAMPION_SWITCH_DELAY_MS = 5000;
const CHAMPION_BUCKET_SEC = 5 * 60;
let lastWindowHours = Number(windowHoursEl?.value || 168);
const championState = {
  internal: { displayed: null, pending: null, timer: null, bucketStartSec: 0 },
  dc: { displayed: null, pending: null, timer: null, bucketStartSec: 0 }
};
let lastMeta = {
  window_hours: Number(windowHoursEl?.value || 168),
  total_symbols: 0,
  total_pushes: 0
};
let sortState = {
  key: "last_push_ts",
  order: "desc"
};

const SORT_BUTTONS = [
  { key: "push_count", el: sortCountBtnEl },
  { key: "last_push_ts", el: sortTimeBtnEl },
  { key: "level_tier", el: sortLevelBtnEl },
  { key: "last_score", el: sortInternalScoreBtnEl },
  { key: "dc_visible_score", el: sortDcScoreBtnEl }
];

function fmtDate(isoText) {
  if (!isoText) return "-";
  const d = new Date(isoText);
  if (Number.isNaN(d.getTime())) return String(isoText);
  return d.toLocaleString("zh-CN", { hour12: false });
}

function fmtBjtOrTs(bjt, tsSec) {
  const b = String(bjt || "").trim();
  if (b) return b;
  const ts = Number(tsSec || 0);
  if (!Number.isFinite(ts) || ts <= 0) return "-";
  return new Date(ts * 1000).toLocaleString("zh-CN", { hour12: false });
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

function esc(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function fmtScore(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "-";
  return n.toFixed(1);
}

function getCurrentBucketRange(nowSec = Math.floor(Date.now() / 1000)) {
  const safeNow = Math.max(0, Number(nowSec || 0));
  const bucketStartSec = Math.floor(safeNow / CHAMPION_BUCKET_SEC) * CHAMPION_BUCKET_SEC;
  return {
    bucketStartSec,
    bucketEndSec: bucketStartSec + CHAMPION_BUCKET_SEC
  };
}

function inBucket(tsSec, bucket) {
  const ts = Number(tsSec || 0);
  if (!Number.isFinite(ts) || ts <= 0) return false;
  return ts >= bucket.bucketStartSec && ts < bucket.bucketEndSec;
}

function fmtBucketRange(bucketStartSec) {
  const startMs = Number(bucketStartSec || 0) * 1000;
  if (!Number.isFinite(startMs) || startMs <= 0) return "-";
  const endMs = startMs + CHAMPION_BUCKET_SEC * 1000;
  const startText = new Date(startMs).toLocaleTimeString("zh-CN", { hour12: false });
  const endText = new Date(endMs).toLocaleTimeString("zh-CN", { hour12: false });
  return `${startText} - ${endText}`;
}

function getInternalCandidate(rows, bucket) {
  const list = Array.isArray(rows) ? rows : [];
  let best = null;
  for (const r of list) {
    if (!inBucket(r.last_push_ts, bucket)) continue;
    const score = Number(r.last_score || 0);
    if (!Number.isFinite(score)) continue;
    if (!best) {
      best = { ...r, score };
      continue;
    }
    if (score > best.score || (score === best.score && Number(r.last_push_ts || 0) > Number(best.last_push_ts || 0))) {
      best = { ...r, score };
    }
  }
  return best;
}

function getDcCandidate(rows, bucket) {
  const list = Array.isArray(rows) ? rows : [];
  let best = null;
  for (const r of list) {
    if (!inBucket(r.last_push_ts, bucket)) continue;
    if (!r.dc_score_visible) continue;
    const score = Number(r.dc_visible_score);
    if (!Number.isFinite(score)) continue;
    if (!best) {
      best = { ...r, score };
      continue;
    }
    if (score > best.score || (score === best.score && Number(r.last_push_ts || 0) > Number(best.last_push_ts || 0))) {
      best = { ...r, score };
    }
  }
  return best;
}

function sameChampion(a, b) {
  if (!a && !b) return true;
  if (!a || !b) return false;
  return String(a.symbol || "") === String(b.symbol || "")
    && Number(a.score || 0) === Number(b.score || 0)
    && Number(a.last_push_ts || 0) === Number(b.last_push_ts || 0);
}

function clearChampionTimer(kind) {
  const state = championState[kind];
  if (!state) return;
  if (state.timer) {
    clearTimeout(state.timer);
    state.timer = null;
  }
}

function clearChampionStates() {
  for (const kind of ["internal", "dc"]) {
    clearChampionTimer(kind);
    championState[kind].displayed = null;
    championState[kind].pending = null;
    championState[kind].bucketStartSec = 0;
  }
}

function renderChampionCard(kind) {
  const state = championState[kind];
  const isInternal = kind === "internal";
  const cardEl = isInternal ? internalChampionCardEl : dcChampionCardEl;
  const symbolEl = isInternal ? internalChampionSymbolEl : dcChampionSymbolEl;
  const metaEl = isInternal ? internalChampionMetaEl : dcChampionMetaEl;
  const noteEl = isInternal ? internalChampionNoteEl : dcChampionNoteEl;
  if (!state || !cardEl || !symbolEl || !metaEl || !noteEl) return;

  const shown = state.displayed;
  const bucketRangeText = fmtBucketRange(state.bucketStartSec);
  if (!shown) {
    symbolEl.textContent = "-";
    metaEl.textContent = `窗口: ${bucketRangeText} | 暂无数据`;
  } else {
    symbolEl.textContent = String(shown.symbol || "-");
    const scoreText = fmtScore(shown.score);
    const tsText = fmtBjtOrTs(shown.last_push_bjt, shown.last_push_ts);
    metaEl.textContent = `窗口: ${bucketRangeText} | Score ${scoreText} | ${tsText}`;
  }

  if (state.pending) {
    symbolEl.classList.add("pending-switch");
    noteEl.textContent = `检测到更高分：${state.pending.symbol} (${fmtScore(state.pending.score)})，5秒后切换`;
  } else {
    symbolEl.classList.remove("pending-switch");
    noteEl.textContent = "";
  }
}

function renderChampionCards() {
  renderChampionCard("internal");
  renderChampionCard("dc");
}

function setChampionImmediate(kind, candidate) {
  const state = championState[kind];
  if (!state) return;
  clearChampionTimer(kind);
  state.pending = null;
  state.displayed = candidate ? { ...candidate } : null;
  renderChampionCard(kind);
}

function scheduleChampionSwitch(kind, candidate) {
  const state = championState[kind];
  if (!state || !candidate) return;
  state.pending = { ...candidate };
  renderChampionCard(kind);
  clearChampionTimer(kind);
  state.timer = setTimeout(() => {
    state.displayed = state.pending ? { ...state.pending } : state.displayed;
    state.pending = null;
    state.timer = null;
    renderChampionCard(kind);
  }, CHAMPION_SWITCH_DELAY_MS);
}

function upsertChampion(kind, candidate, forceReset = false) {
  const state = championState[kind];
  if (!state) return;
  if (forceReset) {
    setChampionImmediate(kind, candidate);
    return;
  }
  if (!state.displayed) {
    setChampionImmediate(kind, candidate);
    return;
  }
  if (!candidate) return;

  const current = state.displayed;
  const candidateScore = Number(candidate.score || 0);
  const currentScore = Number(current.score || 0);
  const isHigher = Number.isFinite(candidateScore) && Number.isFinite(currentScore) && candidateScore > currentScore;
  const sameSymbol = String(candidate.symbol || "") === String(current.symbol || "");

  if (sameSymbol && isHigher) {
    setChampionImmediate(kind, candidate);
    return;
  }
  if (!isHigher) return;
  if (sameChampion(state.pending, candidate)) return;
  scheduleChampionSwitch(kind, candidate);
}

function updateChampionWindows(forceReset = false) {
  const bucket = getCurrentBucketRange();
  for (const kind of ["internal", "dc"]) {
    const state = championState[kind];
    if (!state) continue;
    const bucketChanged = Number(state.bucketStartSec || 0) !== Number(bucket.bucketStartSec || 0);
    if (forceReset || bucketChanged) {
      state.bucketStartSec = bucket.bucketStartSec;
      if (bucketChanged) {
        clearChampionTimer(kind);
        state.pending = null;
        state.displayed = null;
      }
    }
  }
  const internalCandidate = getInternalCandidate(allRows, bucket);
  const dcCandidate = getDcCandidate(allRows, bucket);
  upsertChampion("internal", internalCandidate, forceReset);
  upsertChampion("dc", dcCandidate, forceReset);
  renderChampionCards();
}

function applyFilter(rows) {
  const kw = String(symbolSearchEl?.value || "").trim().toUpperCase();
  if (!kw) return rows;
  return rows.filter((r) => String(r.symbol || "").toUpperCase().includes(kw));
}

function tierRank(tierText) {
  const t = String(tierText || "").toUpperCase();
  if (t === "TIER1") return 4;
  if (t === "TIER2") return 3;
  if (t === "TIER3") return 2;
  if (t === "TIER4") return 1;
  return 0;
}

function levelRank(levelText) {
  const s = String(levelText || "").toUpperCase();
  if (s === "SNIPER") return 4;
  if (s === "HOT") return 3;
  if (s === "WATCH") return 2;
  if (s === "NONE") return 1;
  return 0;
}

function compareNullableNumber(a, b, asc = false) {
  const av = Number(a);
  const bv = Number(b);
  const aValid = Number.isFinite(av);
  const bValid = Number.isFinite(bv);
  if (!aValid && !bValid) return 0;
  if (!aValid) return 1;
  if (!bValid) return -1;
  return asc ? av - bv : bv - av;
}

function sortRows(rows) {
  const { key, order } = sortState;
  const asc = order === "asc";
  const out = [...rows];
  out.sort((a, b) => {
    let cmp = 0;
    if (key === "push_count") {
      cmp = asc
        ? Number(a.push_count || 0) - Number(b.push_count || 0)
        : Number(b.push_count || 0) - Number(a.push_count || 0);
    } else if (key === "last_push_ts") {
      cmp = asc
        ? Number(a.last_push_ts || 0) - Number(b.last_push_ts || 0)
        : Number(b.last_push_ts || 0) - Number(a.last_push_ts || 0);
    } else if (key === "level_tier") {
      const va = tierRank(a.last_tier) * 10 + levelRank(a.last_level);
      const vb = tierRank(b.last_tier) * 10 + levelRank(b.last_level);
      cmp = asc ? va - vb : vb - va;
    } else if (key === "dc_visible_score") {
      const va = a.dc_score_visible ? a.dc_visible_score : null;
      const vb = b.dc_score_visible ? b.dc_visible_score : null;
      cmp = compareNullableNumber(va, vb, asc);
    } else if (key === "last_score") {
      cmp = asc
        ? Number(a.last_score || 0) - Number(b.last_score || 0)
        : Number(b.last_score || 0) - Number(a.last_score || 0);
    }
    if (cmp !== 0) return cmp;
    const tcmp = Number(b.last_push_ts || 0) - Number(a.last_push_ts || 0);
    if (tcmp !== 0) return tcmp;
    return String(a.symbol || "").localeCompare(String(b.symbol || ""));
  });
  return out;
}

function currentRows() {
  return sortRows(applyFilter(allRows));
}

function updateSortButtons() {
  for (const item of SORT_BUTTONS) {
    if (!item?.el) continue;
    const isActive = item.key === sortState.key;
    if (isActive) {
      item.el.textContent = sortState.order === "asc" ? "↑" : "↓";
      item.el.classList.add("active");
    } else {
      item.el.textContent = "↕";
      item.el.classList.remove("active");
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
  renderSummary(lastMeta);
}

function renderRows() {
  const rows = currentRows();
  if (!rows.length) {
    tableBodyEl.innerHTML = `<tr><td colspan="7" class="empty">当前条件下无数据</td></tr>`;
    return;
  }
  tableBodyEl.innerHTML = rows
    .map((r, idx) => {
      const tsText = fmtBjtOrTs(r.last_push_bjt, r.last_push_ts);
      const ago = fmtAgo(r.last_push_ts);
      const internalScore = Number(r.last_score || 0).toFixed(1);
      const dcScore = r.dc_score_visible ? Number(r.dc_visible_score || 0).toFixed(1) : "-";
      return `
        <tr>
          <td>${idx + 1}</td>
          <td class="sym">${esc(r.symbol)}</td>
          <td class="count">${Number(r.push_count || 0)}</td>
          <td>${esc(tsText)} <span class="k">(${esc(ago)})</span></td>
          <td>${esc(r.last_level || "-")} / ${esc(r.last_tier || "-")}</td>
          <td class="score">${internalScore}</td>
          <td class="dc-score">${dcScore}</td>
        </tr>
      `;
    })
    .join("");
}

function renderSummary(meta) {
  const filtered = currentRows();
  const top = filtered.reduce(
    (acc, r) => (Number(r.last_push_ts || 0) > Number(acc.last_push_ts || 0) ? r : acc),
    {}
  );
  const latestText = top && top.symbol
    ? `${top.symbol} @ ${fmtBjtOrTs(top.last_push_bjt, top.last_push_ts)}`
    : "-";
  summaryTextEl.textContent = [
    `来源: ${MONITOR_WS_SOURCE}`,
    `窗口: ${meta.window_hours}h`,
    `总币种: ${meta.total_symbols}`,
    `总推送: ${meta.total_pushes}`,
    `当前筛选后: ${filtered.length}`,
    `排序: ${sortState.key}/${sortState.order}`,
    `最近推送: ${latestText}`
  ].join(" | ");
}

async function loadPushRows() {
  const windowHours = Number(windowHoursEl?.value || 168);
  const windowChanged = windowHours !== lastWindowHours;
  if (windowChanged) {
    lastWindowHours = windowHours;
    clearChampionStates();
    renderChampionCards();
  }
  const params = new URLSearchParams({
    source: MONITOR_WS_SOURCE,
    window_hours: String(windowHours),
    sort_by: "time",
    order: "desc",
    limit: "2000"
  });
  refreshBtnEl.disabled = true;
  hintTextEl.textContent = "加载中...";
  try {
    const res = await fetch(`/api/monitor-ws/pushes?${params.toString()}`, { cache: "no-store" });
    const payload = await res.json();
    if (!payload.ok) {
      throw new Error(payload.message || "unknown error");
    }
    const data = payload.data || {};
    allRows = Array.isArray(data.rows) ? data.rows : [];
    lastMeta = {
      window_hours: Number(data.window_hours || windowHours),
      total_symbols: Number(data.total_symbols || 0),
      total_pushes: Number(data.total_pushes || 0)
    };
    updateChampionWindows(windowChanged);
    renderRows();
    renderSummary(lastMeta);
    hintTextEl.textContent = `来源: ${MONITOR_WS_SOURCE} | 数据库: ${data.db_file || "-"} | 更新时间: ${fmtDate(data.generated_at)} | 说明: fake_breakout_probe/slow_accumulation 的 DC可见分数为“-”`;
  } catch (error) {
    allRows = [];
    clearChampionStates();
    renderChampionCards();
    renderRows();
    summaryTextEl.textContent = "加载失败";
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
  if (!autoRefreshEl.checked) return;
  refreshTimer = setInterval(() => {
    loadPushRows().catch(() => {});
  }, 10000);
}

refreshBtnEl?.addEventListener("click", () => {
  loadPushRows().catch(() => {});
});

windowHoursEl?.addEventListener("change", () => {
  loadPushRows().catch(() => {});
});
symbolSearchEl?.addEventListener("input", () => {
  renderRows();
  renderSummary(lastMeta);
});
autoRefreshEl?.addEventListener("change", () => {
  toggleAutoRefresh();
});
sortCountBtnEl?.addEventListener("click", () => setSortKey("push_count"));
sortTimeBtnEl?.addEventListener("click", () => setSortKey("last_push_ts"));
sortLevelBtnEl?.addEventListener("click", () => setSortKey("level_tier"));
sortInternalScoreBtnEl?.addEventListener("click", () => setSortKey("last_score"));
sortDcScoreBtnEl?.addEventListener("click", () => setSortKey("dc_visible_score"));

loadPushRows()
  .then(() => {
    updateSortButtons();
    toggleAutoRefresh();
  })
  .catch((error) => {
    hintTextEl.textContent = `初始化失败: ${String(error?.message || error)}`;
  });
