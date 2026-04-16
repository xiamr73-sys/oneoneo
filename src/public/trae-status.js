const overallBadge = document.getElementById("overallBadge");
const overallMeta = document.getElementById("overallMeta");
const startBtn = document.getElementById("startBtn");
const refreshBtn = document.getElementById("refreshBtn");
const autoRefresh = document.getElementById("autoRefresh");
const hintText = document.getElementById("hintText");

const planABadge = document.getElementById("planABadge");
const planAPid = document.getElementById("planAPid");
const planAEtime = document.getElementById("planAEtime");
const planACmd = document.getElementById("planACmd");

const bridgeBadge = document.getElementById("bridgeBadge");
const bridgePid = document.getElementById("bridgePid");
const bridgeEtime = document.getElementById("bridgeEtime");
const bridgeCmd = document.getElementById("bridgeCmd");

const guardBadge = document.getElementById("guardBadge");
const guardPid = document.getElementById("guardPid");
const guardEtime = document.getElementById("guardEtime");
const guardCmd = document.getElementById("guardCmd");

const dbSummary = document.getElementById("dbSummary");
const rowsBody = document.getElementById("rowsBody");

const planALog = document.getElementById("planALog");
const bridgeOutLog = document.getElementById("bridgeOutLog");
const bridgeDebugLog = document.getElementById("bridgeDebugLog");
const guardLog = document.getElementById("guardLog");

let refreshTimer = null;

function esc(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function fmtIso(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleString("zh-CN", { hour12: false });
}

function fmtNum(v, digits = 1) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "-";
  return n.toFixed(digits);
}

function setBadge(el, running) {
  if (!el) return;
  if (running) {
    el.textContent = "运行中";
    el.className = "badge up-badge";
  } else {
    el.textContent = "未运行";
    el.className = "badge down-badge";
  }
}

function renderProcess(info, refs) {
  const x = info || {};
  setBadge(refs.badge, Boolean(x.running));
  refs.pid.textContent = x.pid ? String(x.pid) : "-";
  refs.etime.textContent = x.etime || "-";
  refs.cmd.textContent = x.command || "-";
}

function renderRows(rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) {
    rowsBody.innerHTML = '<tr><td colspan="9" class="empty">暂无二次判定记录</td></tr>';
    return;
  }
  rowsBody.innerHTML = list
    .map((r, idx) => {
      const alertFlag = Number(r.planb_alert || 0) > 0;
      const watchFlag = Number(r.planb_watch_alert || 0) > 0;
      return `
      <tr>
        <td>${idx + 1}</td>
        <td>${esc(r.analyzed_time_bjt || "-")}</td>
        <td class="sym">${esc(r.symbol_norm || "-")}</td>
        <td>${esc(r.planb_level || "-")}</td>
        <td class="score">${fmtNum(r.planb_score, 1)}</td>
        <td>${fmtNum(r.planb_threshold, 1)}</td>
        <td>
          <span class="${alertFlag ? "flag-alert" : "k"}">A:${alertFlag ? "Y" : "N"}</span>
          /
          <span class="${watchFlag ? "flag-watch" : "k"}">W:${watchFlag ? "Y" : "N"}</span>
        </td>
        <td>${esc(r.source_symbol_raw || "-")}</td>
        <td>${Number(r.source_alert_count || 0)}</td>
      </tr>`;
    })
    .join("");
}

async function refreshRuntime() {
  refreshBtn.disabled = true;
  try {
    const res = await fetch("/api/trae/runtime?lines=180&limit=80", { cache: "no-store" });
    const payload = await res.json();
    if (!payload.ok) {
      throw new Error(payload.message || "unknown error");
    }
    const data = payload.data || {};

    setBadge(overallBadge, Boolean(data.running));
    overallMeta.textContent = `检查时间: ${fmtIso(data.checked_at)}`;

    renderProcess(data.plan_a, { badge: planABadge, pid: planAPid, etime: planAEtime, cmd: planACmd });
    renderProcess(data.bridge, { badge: bridgeBadge, pid: bridgePid, etime: bridgeEtime, cmd: bridgeCmd });
    renderProcess(data.guard, { badge: guardBadge, pid: guardPid, etime: guardEtime, cmd: guardCmd });

    const stats = data.db_stats || {};
    dbSummary.textContent = [
      `source_events: ${Number(stats.source_events || 0)}`,
      `rejudge_events: ${Number(stats.rejudge_events || 0)}`,
      `checkpoints: ${Number(stats.checkpoints || 0)}`,
      `last_source(BJT): ${stats.last_source_bjt || "-"}`,
      `last_rejudge(BJT): ${stats.last_rejudge_bjt || "-"}`
    ].join(" | ");

    renderRows(data.recent_rows || []);

    planALog.textContent = data?.plan_a?.log_tail || "(plan_a log empty)";
    bridgeOutLog.textContent = data?.bridge?.log_tail || "(bridge out log empty)";
    bridgeDebugLog.textContent = data?.bridge_debug?.log_tail || "(bridge debug log empty)";
    if (guardLog) guardLog.textContent = data?.guard?.log_tail || "(guard log empty)";

    hintText.textContent = [
      `TRAE目录: ${data.trae_dir || "-"}`,
      `DB: ${data.db_file || "-"}`,
      `plan_a日志: ${data?.plan_a?.log_file || "-"}`,
      `bridge日志: ${data?.bridge?.log_file || "-"}`,
      `guard日志: ${data?.guard?.log_file || "-"}`
    ].join(" | ");
  } catch (error) {
    setBadge(overallBadge, false);
    overallMeta.textContent = `读取失败: ${String(error?.message || error)}`;
  } finally {
    refreshBtn.disabled = false;
  }
}

async function startTrae() {
  startBtn.disabled = true;
  hintText.textContent = "正在下发 TRAE 启动命令...";
  try {
    const res = await fetch("/api/trae/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({})
    });
    const payload = await res.json();
    if (!payload.ok) {
      hintText.textContent = payload.message || "启动失败";
      return;
    }
    hintText.textContent = `${payload.message || "已启动"} | ${fmtIso(new Date().toISOString())}`;
    await refreshRuntime();
  } catch (error) {
    hintText.textContent = `启动失败: ${String(error?.message || error)}`;
  } finally {
    startBtn.disabled = false;
  }
}

function toggleAutoRefresh() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
  if (!autoRefresh.checked) return;
  refreshTimer = setInterval(() => {
    refreshRuntime().catch(() => {});
  }, 5000);
}

refreshBtn?.addEventListener("click", () => refreshRuntime().catch(() => {}));
startBtn?.addEventListener("click", () => startTrae().catch(() => {}));
autoRefresh?.addEventListener("change", toggleAutoRefresh);

refreshRuntime()
  .then(toggleAutoRefresh)
  .catch((error) => {
    overallMeta.textContent = `初始化失败: ${String(error?.message || error)}`;
  });
