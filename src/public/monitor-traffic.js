const refreshBtnEl = document.getElementById("refreshBtn");
const autoRefreshEl = document.getElementById("autoRefresh");
const statusTextEl = document.getElementById("statusText");
const summaryTextEl = document.getElementById("summaryText");
const hintTextEl = document.getElementById("hintText");
const ifaceBodyEl = document.getElementById("ifaceBody");
const groupBodyEl = document.getElementById("groupBody");
const procBodyEl = document.getElementById("procBody");
const billingInTextEl = document.getElementById("billingInText");
const billingOutTextEl = document.getElementById("billingOutText");
const billingTotalTextEl = document.getElementById("billingTotalText");
const billingIfaceTextEl = document.getElementById("billingIfaceText");
const monitorInTextEl = document.getElementById("monitorInText");
const monitorOutTextEl = document.getElementById("monitorOutText");
const monitorTotalTextEl = document.getElementById("monitorTotalText");
const procCountTextEl = document.getElementById("procCountText");

let timer = null;

function esc(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function fmtDate(isoText) {
  if (!isoText) return "-";
  const d = new Date(isoText);
  if (Number.isNaN(d.getTime())) return String(isoText);
  return d.toLocaleString("zh-CN", { hour12: false });
}

function setStatus(text) {
  if (statusTextEl) statusTextEl.textContent = text;
}

function renderIfaceRows(rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) {
    ifaceBodyEl.innerHTML = '<tr><td colspan="7" class="empty">暂无计费接口采样数据</td></tr>';
    return;
  }
  ifaceBodyEl.innerHTML = list
    .map(
      (row, idx) => `
      <tr>
        <td>${idx + 1}</td>
        <td class="mono">${esc(row.iface || "-")}</td>
        <td>${row.active ? "active" : "idle"}</td>
        <td class="bytes in">${esc(row.bytes_in_text || "-")}</td>
        <td class="bytes out">${esc(row.bytes_out_text || "-")}</td>
        <td class="bytes total">${esc(row.total_bytes_text || "-")}</td>
        <td class="mono">${esc(`${row.current_counter_in_text || "0 B"} / ${row.current_counter_out_text || "0 B"}`)}</td>
      </tr>
    `
    )
    .join("");
}

function renderGroupRows(rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) {
    groupBodyEl.innerHTML = '<tr><td colspan="7" class="empty">当前没有运行中的监控程序</td></tr>';
    return;
  }
  groupBodyEl.innerHTML = list
    .map((row, idx) => {
      const pids = Array.isArray(row.pids) ? row.pids.join(", ") : "-";
      return `
        <tr>
          <td>${idx + 1}</td>
          <td class="program">${esc(row.target_label || row.target_id || "-")}</td>
          <td>${Number(row.process_count || 0)}</td>
          <td class="mono">${esc(pids)}</td>
          <td class="bytes in">${esc(row.bytes_in_text || "-")}</td>
          <td class="bytes out">${esc(row.bytes_out_text || "-")}</td>
          <td class="bytes total">${esc(row.total_bytes_text || "-")}</td>
        </tr>
      `.trim();
    })
    .join("");
}

function renderProcessRows(rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (!list.length) {
    procBodyEl.innerHTML = '<tr><td colspan="8" class="empty">当前没有运行中的监控程序</td></tr>';
    return;
  }
  procBodyEl.innerHTML = list
    .map(
      (row, idx) => `
      <tr>
        <td>${idx + 1}</td>
        <td class="program">${esc(row.target_label || row.target_id || "-")}</td>
        <td class="mono">${esc(row.pid)}</td>
        <td class="mono">${esc(row.etime || "-")}</td>
        <td class="bytes in">${esc(row.bytes_in_text || "-")}</td>
        <td class="bytes out">${esc(row.bytes_out_text || "-")}</td>
        <td class="bytes total">${esc(row.total_bytes_text || "-")}</td>
        <td class="cmd">${esc(row.command || "-")}</td>
      </tr>
    `
    )
    .join("");
}

function render(payload) {
  const data = payload && payload.data ? payload.data : {};
  const monitorTotals = data.monitor_totals || data.totals || {};
  const billingTotals = data.billing_totals || {};
  billingInTextEl.textContent = billingTotals.bytes_in_text || "0 B";
  billingOutTextEl.textContent = billingTotals.bytes_out_text || "0 B";
  billingTotalTextEl.textContent = billingTotals.total_bytes_text || "0 B";
  billingIfaceTextEl.textContent = `${Number(billingTotals.active_iface_count || 0)} / ${Number(billingTotals.iface_count || 0)}`;
  monitorInTextEl.textContent = monitorTotals.bytes_in_text || "0 B";
  monitorOutTextEl.textContent = monitorTotals.bytes_out_text || "0 B";
  monitorTotalTextEl.textContent = monitorTotals.total_bytes_text || "0 B";
  procCountTextEl.textContent = `${Number(monitorTotals.process_count || 0)} 个进程 / ${Number(monitorTotals.target_count || 0)} 类`;
  renderIfaceRows(data.billing_ifaces || []);
  renderGroupRows(data.groups || []);
  renderProcessRows(data.rows || []);
  const generatedAtText = fmtDate(data.generated_at);
  const startAtText = fmtDate(data.accum_started_at);
  const lastSampleText = fmtDate(data.last_sample_at);
  const sampleCount = Number(data.sample_count || 0);
  const billingLastSampleText = fmtDate(data.billing_last_sample_at);
  const billingSampleCount = Number(data.billing_sample_count || 0);
  summaryTextEl.textContent = `更新时间：${generatedAtText} | 监控累计起点：${startAtText} | 监控最近采样：${lastSampleText}(${sampleCount}) | 计费最近采样：${billingLastSampleText}(${billingSampleCount})`;
  const parts = [];
  parts.push(`模式：${String(data.mode || "continuous")}（持续累计）`);
  parts.push(`采样间隔：${Number(data.sample_interval_sec || 5)}s`);
  parts.push(`计费口径：${String(data.billing_source || "netstat -ib")}`);
  parts.push(`计费采样：${data.billing_netstat_ok ? "ok" : `异常(${String(data.billing_netstat_error || "unknown")})`}`);
  parts.push(`进程采样：${data.nettop_ok ? "ok" : `异常(${String(data.nettop_error || "unknown")})`}`);
  hintTextEl.textContent = parts.join(" | ");
}

async function loadData() {
  setStatus("加载中...");
  try {
    const res = await fetch("/api/monitor-traffic", { cache: "no-store" });
    const payload = await res.json();
    if (!payload.ok) {
      throw new Error(payload.message || "读取失败");
    }
    render(payload);
    setStatus("已更新");
  } catch (error) {
    const msg = String(error && error.message ? error.message : error);
    ifaceBodyEl.innerHTML = `<tr><td colspan="7" class="empty">读取失败：${esc(msg)}</td></tr>`;
    groupBodyEl.innerHTML = `<tr><td colspan="7" class="empty">读取失败：${esc(msg)}</td></tr>`;
    procBodyEl.innerHTML = `<tr><td colspan="8" class="empty">读取失败：${esc(msg)}</td></tr>`;
    hintTextEl.textContent = `读取失败：${msg}`;
    setStatus("读取失败");
  }
}

function resetTimer() {
  if (timer) clearInterval(timer);
  timer = null;
  if (autoRefreshEl && autoRefreshEl.checked) {
    timer = setInterval(loadData, 5000);
  }
}

refreshBtnEl?.addEventListener("click", () => {
  loadData();
});

autoRefreshEl?.addEventListener("change", () => {
  resetTimer();
});

loadData();
resetTimer();
