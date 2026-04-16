const express = require("express");
const dns = require("node:dns");
const https = require("node:https");
const net = require("node:net");
const fs = require("node:fs");
const path = require("node:path");
const { execFile, spawn } = require("node:child_process");
const readline = require("node:readline");
const { Client, Events, GatewayIntentBits } = require("discord.js");
const { config } = require("./config");
const logger = require("./logger");
const { loadState, saveState, loadSnapshots } = require("./stateStore");
const { runOnce } = require("./monitorService");
const { buildDailyReport, buildWeeklyReport } = require("./engine/reportEngine");
const { formatSignals } = require("./notifier");

const state = loadState();
state.trend = state.trend || {
  bootstrapped: false,
  bootstrapAt: null,
  lastRunAt: null,
  lastDirections: {},
  lastConfidence: {},
  modelMeta: {},
  coverageMinutes: {},
  lastError: "",
  backfill: {
    lastAttemptAt: null,
    lastResult: null
  }
};
const app = express();
const trendPageApp = express();
app.use(express.json({ limit: "512kb" }));
app.use(express.static("src/public"));
trendPageApp.use(express.static("src/trend-public"));

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") {
    res.status(204).end();
    return;
  }
  next();
});

let discordClient = null;
let loopRunning = false;
const trainLocks = new Map();
const predictJobs = new Map();
const predictLongJobs = new Map();
const predictWorkers = new Map();
const collectorJobs = new Map();
const collectorRuntime = {
  activeJobId: "",
  stopRequested: false
};
const AUTO_COLLECT_HOURS = "0.03";
const AUTO_TRAIN_ARGS = ["--horizon-minutes", "1", "--label-threshold", "0.0", "--min-samples", "20"];
const AUTO_LONG_HORIZON_TRIALS = [30, 15, 5, 3];
const AUTO_LONG_MIN_SAMPLES = "10";
const AUTO_LONG_MIN_COVERAGE_MINUTES = 8.0;
const COLLECT_BATCH_HOURS = "0.02";
const COLLECT_CONCURRENCY = 8;
const oiCache = {
  snapshot: { ts: 0, data: null },
  history: new Map()
};
const MONITOR_CONTROL_PATH = path.join(process.cwd(), "config", "monitor_ws_control.json");
const MONITOR_LOG_DIR = path.join(process.cwd(), "data", "logs");
const MONITOR_WS_LOG_FILE = path.join(MONITOR_LOG_DIR, "monitor_ws_console.log");
const MONITOR_WS_SIGNAL_DB_DEFAULT = path.join(process.cwd(), "data", "logs", "monitor_ws_signals.db");
const MONITOR_WS_SIGNAL_DB_ASTER_DEFAULT = path.join(process.cwd(), "data", "logs", "monitor_ws_signals_aster.db");
const TRAE_DIR = path.join(process.cwd(), "imports", "trae_original");
const TRAE_DB_FILE = path.join(TRAE_DIR, "trae_original.db");
const TRAE_PLAN_A_LOG_FILE = path.join(TRAE_DIR, "plan_a_monitor.log");
const TRAE_BRIDGE_OUT_FILE = path.join(TRAE_DIR, "trae_original_bridge.out");
const TRAE_BRIDGE_LOG_FILE = path.join(TRAE_DIR, "trae_original_bridge.log");
const TRAE_GUARD_LOG_FILE = path.join(TRAE_DIR, "trae_original_guard.log");
const TRAE_PLAN_A_PID_FILE = path.join(TRAE_DIR, "plan_a_monitor.pid");
const TRAE_BRIDGE_PID_FILE = path.join(TRAE_DIR, "trae_original_bridge.pid");
const TRAE_GUARD_PID_FILE = path.join(TRAE_DIR, "trae_original_guard.pid");
const TRAE_START_SCRIPT = path.join(TRAE_DIR, "start_trae_original.sh");
const TRAE_START_LAUNCH_LOG = path.join(TRAE_DIR, "trae_start_launcher.log");
const MONITOR_WS_DC_SCORE_HIDDEN_REASONS = new Set([
  "fake_breakout_probe",
  "slow_accumulation",
  "liquidation_spike_watch",
  "liquidation_spike_confirm"
]);
const MONITOR_WS_DEFAULT_ARGS = [
  "-u",
  "-m",
  "binance_futures_monitor.monitor_ws",
  "--workers",
  "3",
  "--mode",
  "lob",
  "--control-file",
  "config/monitor_ws_control.json",
  "--onchain-targets-file",
  "config/onchain_targets.json",
  "--onchain-sector-file",
  "config/onchain_sectors.json"
];
const MONITOR_MODULE_FIELDS = [
  { key: "mod_trend_seg_enabled", label: "趋势分段识别" },
  { key: "mod_combo_enabled", label: "三者组合判断" },
  { key: "mod_cvd_structure_enabled", label: "CVD结构分析" },
  { key: "mod_aux_indicators_enabled", label: "辅助指标确认" },
  { key: "mod_flow_mtf_enabled", label: "多周期资金流分析" },
  { key: "mod_scorecard_enabled", label: "五维评分卡" },
  { key: "mod_contradiction_enabled", label: "矛盾信号守卫" },
  { key: "mod_alert_digest_enabled", label: "报警摘要模块" },
  { key: "mod_backtest_metrics_enabled", label: "回测结构统计" },
  { key: "mod_trade_plan_enabled", label: "交易计划建议" }
];
const MONITOR_TRAFFIC_TARGETS = [
  {
    id: "monitor_ws",
    label: "monitor_ws 主监控",
    include_any: ["binance_futures_monitor.monitor_ws"]
  },
  {
    id: "spot_inflow_multi",
    label: "spot_inflow 多交易所现货",
    include_any: ["binance_futures_monitor.spot_inflow_multi_exchange_monitor"]
  },
  {
    id: "trae_plan_a",
    label: "TRAE plan_a 监控",
    include_any: ["imports/plan_a/binance_futures_monitor/monitor.py"]
  },
  {
    id: "trae_bridge",
    label: "TRAE bridge 二次判定",
    include_any: ["imports/trae_original/trae_original_bridge.py"]
  },
  {
    id: "plan_b_monitor",
    label: "plan_b 监控",
    include_any: ["binance_futures_monitor/monitor.py"],
    exclude_any: ["imports/plan_a/binance_futures_monitor/monitor.py", "trae-sandbox exec"]
  },
  {
    id: "explosive_spot_radar",
    label: "现货爆发雷达",
    include_any: ["binance_futures_monitor.explosive_spot_radar"]
  }
];
const MONITOR_TRAFFIC_SAMPLE_INTERVAL_MS = 5000;
const MONITOR_TRAFFIC_PERSIST_INTERVAL_MS = 15000;
const MONITOR_TRAFFIC_STALE_PID_MS = 30 * 60 * 1000;
const MONITOR_TRAFFIC_STATE_FILE = path.join(MONITOR_LOG_DIR, "monitor_traffic_state.json");
const BILLING_LIKE_IFACE_INCLUDE_RE = /^(en[0-9]+|eth[0-9]+|ens[0-9]+|eno[0-9]+|enp[0-9]+|bond[0-9]+|pdp_ip[0-9]+|wlan[0-9]+)$/i;
const BILLING_LIKE_IFACE_EXCLUDE_PREFIXES = [
  "lo",
  "utun",
  "awdl",
  "llw",
  "bridge",
  "ap",
  "anpi",
  "gif",
  "stf"
];
const monitorTrafficState = {
  mode: "continuous",
  sample_interval_ms: MONITOR_TRAFFIC_SAMPLE_INTERVAL_MS,
  accum_started_at: new Date().toISOString(),
  last_sample_at: "",
  sample_count: 0,
  nettop_ok: false,
  nettop_error: "",
  sampling: false,
  timer: null,
  by_pid: new Map(),
  last_persist_ms: 0,
  system_net: {
    mode: "billing_like_netstat",
    source: "netstat -ib",
    sample_count: 0,
    last_sample_at: "",
    netstat_ok: false,
    netstat_error: "",
    by_iface: {},
    totals: { bytes_in: 0, bytes_out: 0, total_bytes: 0 }
  }
};
const TREND_BOOTSTRAP_TARGET_MINUTES = Math.max(1, Number(config.trendBootstrapDays || 7) * 24 * 60);
let trendCycleRunning = false;

function canConnectTcp(host, port, timeoutMs = 350) {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    let done = false;
    const finish = (ok) => {
      if (done) return;
      done = true;
      try {
        socket.destroy();
      } catch (_) {}
      resolve(Boolean(ok));
    };
    socket.setTimeout(timeoutMs);
    socket.once("connect", () => finish(true));
    socket.once("timeout", () => finish(false));
    socket.once("error", () => finish(false));
    socket.connect(port, host);
  });
}

async function resolvePreferredProxyUrl() {
  const explicit = process.env.HTTPS_PROXY || process.env.HTTP_PROXY || process.env.ALL_PROXY || "";
  if (explicit) return explicit;
  const candidates = [7897, 7890];
  for (const port of candidates) {
    // Local proxy auto-detection for Discord/Binance outbound requests.
    const ok = await canConnectTcp("127.0.0.1", port);
    if (ok) return `http://127.0.0.1:${port}`;
  }
  return "";
}

function applyProxyEnv(env, proxyUrl) {
  if (!proxyUrl) return env;
  const next = { ...env };
  next.HTTP_PROXY = next.HTTP_PROXY || proxyUrl;
  next.HTTPS_PROXY = next.HTTPS_PROXY || proxyUrl;
  next.ALL_PROXY = next.ALL_PROXY || proxyUrl;
  next.NO_PROXY = next.NO_PROXY || "127.0.0.1,localhost";
  return next;
}

function defaultMonitorWsControl() {
  const modules = {};
  for (const item of MONITOR_MODULE_FIELDS) modules[item.key] = true;
  return {
    discord_min_score: 50,
    modules,
    updated_at: new Date().toISOString()
  };
}

function sanitizeMonitorWsControl(raw) {
  const defaults = defaultMonitorWsControl();
  const src = raw && typeof raw === "object" ? raw : {};
  const base = { ...src };
  const score = Number(src.discord_min_score);
  base.discord_min_score = Number.isFinite(score)
    ? Math.max(0, Math.min(200, score))
    : defaults.discord_min_score;
  const modules = src.modules && typeof src.modules === "object" ? src.modules : src;
  base.modules = { ...defaults.modules };
  for (const item of MONITOR_MODULE_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(modules, item.key)) {
      base.modules[item.key] = Boolean(modules[item.key]);
    }
  }
  base.updated_at = src.updated_at ? String(src.updated_at) : new Date().toISOString();
  return base;
}

function loadMonitorWsControl() {
  try {
    if (!fs.existsSync(MONITOR_CONTROL_PATH)) {
      return defaultMonitorWsControl();
    }
    const raw = JSON.parse(fs.readFileSync(MONITOR_CONTROL_PATH, "utf8"));
    return sanitizeMonitorWsControl(raw);
  } catch (error) {
    logger.warn("Load monitor_ws control failed, fallback defaults", { error: String(error.message || error) });
    return defaultMonitorWsControl();
  }
}

function saveMonitorWsControl(data) {
  const next = sanitizeMonitorWsControl({
    ...data,
    updated_at: new Date().toISOString()
  });
  fs.mkdirSync(path.dirname(MONITOR_CONTROL_PATH), { recursive: true });
  fs.writeFileSync(MONITOR_CONTROL_PATH, `${JSON.stringify(next, null, 2)}\n`, "utf8");
  return next;
}

function detectMonitorWsLogFile() {
  try {
    if (!fs.existsSync(MONITOR_LOG_DIR)) return "";
    const files = fs
      .readdirSync(MONITOR_LOG_DIR)
      .filter((name) => /^monitor_ws.*\.log$/i.test(name))
      .map((name) => {
        const fp = path.join(MONITOR_LOG_DIR, name);
        let mtimeMs = 0;
        try {
          mtimeMs = Number(fs.statSync(fp).mtimeMs || 0);
        } catch (_) {}
        return { fp, mtimeMs };
      })
      .sort((a, b) => b.mtimeMs - a.mtimeMs);
    return files.length ? files[0].fp : "";
  } catch (_) {
    return "";
  }
}

function toNumberInRange(v, fallback, min, max) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(n, min), max);
}

function normalizePushSortBy(v) {
  const s = String(v || "").trim().toLowerCase();
  if (s === "count") return "count";
  return "time";
}

function normalizePushSortOrder(v) {
  const s = String(v || "").trim().toLowerCase();
  if (s === "asc") return "asc";
  return "desc";
}

function normalizeMonitorSource(v) {
  const s = String(v || "").trim().toLowerCase();
  if (s === "aster") return "aster";
  return "binance";
}

function parsePsRows(rawText) {
  const text = String(rawText || "");
  const lines = text.split(/\r?\n/);
  const out = [];
  for (const line of lines) {
    const m = String(line).match(/^\s*(\d+)\s+(\S+)\s+(.+)\s*$/);
    if (!m) continue;
    const pid = Number(m[1]);
    if (!Number.isFinite(pid) || pid <= 0) continue;
    out.push({
      pid,
      etime: String(m[2] || "").trim(),
      command: String(m[3] || "").trim()
    });
  }
  return out;
}

function matchMonitorTrafficTarget(command, target) {
  const cmd = String(command || "").toLowerCase();
  const includeAny = Array.isArray(target.include_any) ? target.include_any : [];
  if (!includeAny.length) return false;
  const hasInclude = includeAny.some((k) => cmd.includes(String(k || "").toLowerCase()));
  if (!hasInclude) return false;
  const excludeAny = Array.isArray(target.exclude_any) ? target.exclude_any : [];
  const hitExclude = excludeAny.some((k) => cmd.includes(String(k || "").toLowerCase()));
  return !hitExclude;
}

function normalizeBillingIfaceName(rawName) {
  return String(rawName || "")
    .trim()
    .replace(/\*+$/, "")
    .toLowerCase();
}

function isBillingLikeIface(name) {
  const n = normalizeBillingIfaceName(name);
  if (!n) return false;
  if (!BILLING_LIKE_IFACE_INCLUDE_RE.test(n)) return false;
  if (BILLING_LIKE_IFACE_EXCLUDE_PREFIXES.some((prefix) => n.startsWith(prefix))) return false;
  return true;
}

function parseNetstatInterfaceRows(rawText) {
  const lines = String(rawText || "")
    .split(/\r?\n/)
    .map((x) => x.trim())
    .filter(Boolean);
  const byIface = new Map();
  for (const line of lines) {
    if (line.startsWith("Name ")) continue;
    const cols = line.split(/\s+/);
    if (cols.length < 10) continue;
    const iface = normalizeBillingIfaceName(cols[0]);
    if (!isBillingLikeIface(iface)) continue;
    const bytesIn = Math.max(0, Number(cols[6] || 0));
    const bytesOut = Math.max(0, Number(cols[9] || 0));
    if (!Number.isFinite(bytesIn) || !Number.isFinite(bytesOut)) continue;
    const prev = byIface.get(iface) || { iface, bytes_in: 0, bytes_out: 0 };
    prev.bytes_in = Math.max(prev.bytes_in, bytesIn);
    prev.bytes_out = Math.max(prev.bytes_out, bytesOut);
    byIface.set(iface, prev);
  }
  return [...byIface.values()].sort((a, b) => a.iface.localeCompare(b.iface, "en"));
}

async function collectBillingLikeRawSnapshot() {
  let rows = [];
  let netstatError = "";
  try {
    const out = await execFileAsync("netstat", ["-ib"], 12000);
    rows = parseNetstatInterfaceRows(out.stdout);
  } catch (error) {
    netstatError = String(error.message || error);
    logger.warn("collect billing-like traffic snapshot failed", { error: netstatError });
  }
  return {
    rows,
    netstat_ok: !netstatError,
    netstat_error: netstatError,
    generated_at: new Date().toISOString()
  };
}

function normalizePersistedPidRow(rawRow = {}) {
  const pid = Number(rawRow.pid || 0);
  if (!Number.isFinite(pid) || pid <= 0) return null;
  const row = {
    target_id: String(rawRow.target_id || ""),
    target_label: String(rawRow.target_label || ""),
    pid,
    command: String(rawRow.command || ""),
    etime: String(rawRow.etime || ""),
    process_cell: String(rawRow.process_cell || ""),
    running: false,
    first_seen_at: String(rawRow.first_seen_at || ""),
    last_seen_at: String(rawRow.last_seen_at || ""),
    last_seen_ms: Number(rawRow.last_seen_ms || 0),
    samples: Math.max(0, Number(rawRow.samples || 0)),
    has_counter: Boolean(rawRow.has_counter),
    unknown_counter_samples: Math.max(0, Number(rawRow.unknown_counter_samples || 0)),
    last_counter_in: Math.max(0, Number(rawRow.last_counter_in || 0)),
    last_counter_out: Math.max(0, Number(rawRow.last_counter_out || 0)),
    current_counter_in: Math.max(0, Number(rawRow.current_counter_in || 0)),
    current_counter_out: Math.max(0, Number(rawRow.current_counter_out || 0)),
    current_counter_total: Math.max(0, Number(rawRow.current_counter_total || 0)),
    bytes_in: Math.max(0, Number(rawRow.bytes_in || 0)),
    bytes_out: Math.max(0, Number(rawRow.bytes_out || 0)),
    total_bytes: Math.max(0, Number(rawRow.total_bytes || 0))
  };
  return row;
}

function normalizePersistedSystemNetState(rawState = {}) {
  const rawByIface = rawState && typeof rawState.by_iface === "object" ? rawState.by_iface : {};
  const byIface = {};
  for (const [ifaceRaw, raw] of Object.entries(rawByIface || {})) {
    const iface = normalizeBillingIfaceName(ifaceRaw);
    if (!iface) continue;
    byIface[iface] = {
      iface,
      has_counter: Boolean(raw.has_counter),
      last_counter_in: Math.max(0, Number(raw.last_counter_in || 0)),
      last_counter_out: Math.max(0, Number(raw.last_counter_out || 0)),
      current_counter_in: Math.max(0, Number(raw.current_counter_in || 0)),
      current_counter_out: Math.max(0, Number(raw.current_counter_out || 0)),
      current_counter_total: Math.max(0, Number(raw.current_counter_total || 0)),
      bytes_in: Math.max(0, Number(raw.bytes_in || 0)),
      bytes_out: Math.max(0, Number(raw.bytes_out || 0)),
      total_bytes: Math.max(0, Number(raw.total_bytes || 0)),
      active: Boolean(raw.active),
      last_seen_at: String(raw.last_seen_at || "")
    };
  }
  const totals = rawState && typeof rawState.totals === "object" ? rawState.totals : {};
  return {
    mode: String(rawState.mode || "billing_like_netstat"),
    source: String(rawState.source || "netstat -ib"),
    sample_count: Math.max(0, Number(rawState.sample_count || 0)),
    last_sample_at: String(rawState.last_sample_at || ""),
    netstat_ok: Boolean(rawState.netstat_ok),
    netstat_error: String(rawState.netstat_error || ""),
    by_iface: byIface,
    totals: {
      bytes_in: Math.max(0, Number(totals.bytes_in || 0)),
      bytes_out: Math.max(0, Number(totals.bytes_out || 0)),
      total_bytes: Math.max(0, Number(totals.total_bytes || 0))
    }
  };
}

function loadMonitorTrafficStateFromDisk() {
  try {
    if (!fs.existsSync(MONITOR_TRAFFIC_STATE_FILE)) return;
    const raw = JSON.parse(fs.readFileSync(MONITOR_TRAFFIC_STATE_FILE, "utf8"));
    monitorTrafficState.mode = String(raw.mode || monitorTrafficState.mode || "continuous");
    monitorTrafficState.sample_interval_ms = MONITOR_TRAFFIC_SAMPLE_INTERVAL_MS;
    monitorTrafficState.accum_started_at = String(raw.accum_started_at || monitorTrafficState.accum_started_at);
    monitorTrafficState.last_sample_at = String(raw.last_sample_at || "");
    monitorTrafficState.sample_count = Math.max(0, Number(raw.sample_count || 0));
    monitorTrafficState.nettop_ok = Boolean(raw.nettop_ok);
    monitorTrafficState.nettop_error = String(raw.nettop_error || "");
    monitorTrafficState.last_persist_ms = Date.now();
    const list = Array.isArray(raw.by_pid) ? raw.by_pid : [];
    monitorTrafficState.by_pid = new Map();
    for (const item of list) {
      const row = normalizePersistedPidRow(item);
      if (!row) continue;
      monitorTrafficState.by_pid.set(String(row.pid), row);
    }
    monitorTrafficState.system_net = normalizePersistedSystemNetState(raw.system_net || {});
  } catch (error) {
    logger.warn("load monitor traffic state failed", { error: String(error.message || error) });
  }
}

function persistMonitorTrafficState() {
  try {
    fs.mkdirSync(path.dirname(MONITOR_TRAFFIC_STATE_FILE), { recursive: true });
    const payload = {
      mode: String(monitorTrafficState.mode || "continuous"),
      sample_interval_ms: MONITOR_TRAFFIC_SAMPLE_INTERVAL_MS,
      accum_started_at: monitorTrafficState.accum_started_at,
      last_sample_at: monitorTrafficState.last_sample_at,
      sample_count: Number(monitorTrafficState.sample_count || 0),
      nettop_ok: Boolean(monitorTrafficState.nettop_ok),
      nettop_error: String(monitorTrafficState.nettop_error || ""),
      by_pid: [...monitorTrafficState.by_pid.values()],
      system_net: monitorTrafficState.system_net || {}
    };
    fs.writeFileSync(MONITOR_TRAFFIC_STATE_FILE, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
    monitorTrafficState.last_persist_ms = Date.now();
  } catch (error) {
    logger.warn("persist monitor traffic state failed", { error: String(error.message || error) });
  }
}

function maybePersistMonitorTrafficState(nowMs) {
  const ts = Number(nowMs || Date.now());
  if (!Number.isFinite(ts)) return;
  if (ts - Number(monitorTrafficState.last_persist_ms || 0) < MONITOR_TRAFFIC_PERSIST_INTERVAL_MS) return;
  persistMonitorTrafficState();
}

loadMonitorTrafficStateFromDisk();

async function listRunningMonitorTrafficProcesses() {
  const out = await execFileAsync("ps", ["-axo", "pid=,etime=,command="], 12000);
  const rows = parsePsRows(out.stdout);
  const picked = [];
  for (const row of rows) {
    const target = MONITOR_TRAFFIC_TARGETS.find((t) => matchMonitorTrafficTarget(row.command, t));
    if (!target) continue;
    picked.push({
      target_id: target.id,
      target_label: target.label,
      pid: row.pid,
      etime: row.etime,
      command: row.command
    });
  }
  picked.sort((a, b) => {
    const c = String(a.target_label).localeCompare(String(b.target_label), "zh-CN");
    if (c !== 0) return c;
    return a.pid - b.pid;
  });
  return picked;
}

function parseNettopRowsByPid(rawCsv) {
  const lines = String(rawCsv || "")
    .split(/\r?\n/)
    .map((x) => x.trim())
    .filter(Boolean);
  const out = new Map();
  for (const line of lines) {
    if (line.startsWith("time,")) continue;
    const cols = line.split(",");
    if (cols.length < 6) continue;
    const processCell = String(cols[1] || "").trim();
    const pidMatch = processCell.match(/\.([0-9]+)$/);
    if (!pidMatch) continue;
    const pid = Number(pidMatch[1]);
    if (!Number.isFinite(pid) || pid <= 0) continue;
    const bytesIn = Math.max(0, Number(cols[4] || 0));
    const bytesOut = Math.max(0, Number(cols[5] || 0));
    out.set(pid, {
      process_cell: processCell,
      bytes_in: Number.isFinite(bytesIn) ? bytesIn : 0,
      bytes_out: Number.isFinite(bytesOut) ? bytesOut : 0
    });
  }
  return out;
}

async function collectMonitorTrafficRawSnapshot() {
  const processRows = await listRunningMonitorTrafficProcesses();
  let nettopByPid = new Map();
  let nettopError = "";

  if (processRows.length > 0) {
    const args = ["-P", "-L", "1", "-x"];
    for (const row of processRows) {
      args.push("-p", String(row.pid));
    }
    try {
      const out = await execFileAsync("nettop", args, 20000);
      nettopByPid = parseNettopRowsByPid(out.stdout);
    } catch (error) {
      nettopError = String(error.message || error);
      logger.warn("collect monitor traffic snapshot failed", { error: nettopError });
    }
  }

  const rows = processRows.map((row) => {
    const net = nettopByPid.get(row.pid) || { bytes_in: 0, bytes_out: 0, process_cell: "" };
    const bytesIn = Math.max(0, Number(net.bytes_in || 0));
    const bytesOut = Math.max(0, Number(net.bytes_out || 0));
    const totalBytes = bytesIn + bytesOut;
    return {
      ...row,
      process_cell: String(net.process_cell || ""),
      bytes_in: bytesIn,
      bytes_out: bytesOut,
      total_bytes: totalBytes,
      bytes_in_text: fmtBytes(bytesIn),
      bytes_out_text: fmtBytes(bytesOut),
      total_bytes_text: fmtBytes(totalBytes)
    };
  });

  return {
    rows,
    nettop_ok: !nettopError,
    nettop_error: nettopError,
    generated_at: new Date().toISOString()
  };
}

function upsertMonitorTrafficEntry(rawRow, nowMs) {
  const pid = Number(rawRow.pid || 0);
  if (!Number.isFinite(pid) || pid <= 0) return;
  const key = String(pid);
  const hasCounter = Boolean(String(rawRow.process_cell || "").trim());
  const currentIn = Math.max(0, Number(rawRow.bytes_in || 0));
  const currentOut = Math.max(0, Number(rawRow.bytes_out || 0));
  const currentTotal = currentIn + currentOut;

  let entry = monitorTrafficState.by_pid.get(key);
  if (!entry) {
    entry = {
      target_id: String(rawRow.target_id || ""),
      target_label: String(rawRow.target_label || ""),
      pid,
      command: String(rawRow.command || ""),
      etime: String(rawRow.etime || ""),
      process_cell: String(rawRow.process_cell || ""),
      running: true,
      first_seen_at: new Date(nowMs).toISOString(),
      last_seen_at: new Date(nowMs).toISOString(),
      last_seen_ms: nowMs,
      samples: 0,
      has_counter: false,
      unknown_counter_samples: 0,
      last_counter_in: 0,
      last_counter_out: 0,
      current_counter_in: 0,
      current_counter_out: 0,
      current_counter_total: 0,
      bytes_in: 0,
      bytes_out: 0,
      total_bytes: 0
    };
  }

  entry.target_id = String(rawRow.target_id || entry.target_id || "");
  entry.target_label = String(rawRow.target_label || entry.target_label || "");
  entry.pid = pid;
  entry.command = String(rawRow.command || entry.command || "");
  entry.etime = String(rawRow.etime || entry.etime || "");
  entry.process_cell = String(rawRow.process_cell || "");
  entry.running = true;
  entry.last_seen_at = new Date(nowMs).toISOString();
  entry.last_seen_ms = nowMs;
  entry.samples = Number(entry.samples || 0) + 1;

  if (!hasCounter) {
    entry.unknown_counter_samples = Number(entry.unknown_counter_samples || 0) + 1;
    monitorTrafficState.by_pid.set(key, entry);
    return;
  }

  if (!entry.has_counter) {
    entry.has_counter = true;
    entry.last_counter_in = currentIn;
    entry.last_counter_out = currentOut;
    entry.current_counter_in = currentIn;
    entry.current_counter_out = currentOut;
    entry.current_counter_total = currentTotal;
    monitorTrafficState.by_pid.set(key, entry);
    return;
  }

  let deltaIn = currentIn - Number(entry.last_counter_in || 0);
  let deltaOut = currentOut - Number(entry.last_counter_out || 0);
  if (!Number.isFinite(deltaIn) || deltaIn < 0) deltaIn = 0;
  if (!Number.isFinite(deltaOut) || deltaOut < 0) deltaOut = 0;

  entry.bytes_in = Number(entry.bytes_in || 0) + deltaIn;
  entry.bytes_out = Number(entry.bytes_out || 0) + deltaOut;
  entry.total_bytes = Number(entry.total_bytes || 0) + deltaIn + deltaOut;
  entry.last_counter_in = currentIn;
  entry.last_counter_out = currentOut;
  entry.current_counter_in = currentIn;
  entry.current_counter_out = currentOut;
  entry.current_counter_total = currentTotal;
  monitorTrafficState.by_pid.set(key, entry);
}

function upsertBillingLikeTraffic(rawSnapshot, nowMs) {
  const state = normalizePersistedSystemNetState(monitorTrafficState.system_net || {});
  state.sample_count = Math.max(0, Number(state.sample_count || 0)) + 1;
  state.last_sample_at = String(rawSnapshot.generated_at || new Date(nowMs).toISOString());
  state.netstat_ok = Boolean(rawSnapshot.netstat_ok);
  state.netstat_error = String(rawSnapshot.netstat_error || "");

  if (!Array.isArray(rawSnapshot.rows) || !rawSnapshot.rows.length) {
    for (const entry of Object.values(state.by_iface || {})) {
      entry.active = false;
    }
    monitorTrafficState.system_net = state;
    return;
  }

  const activeIfaces = new Set();
  for (const row of rawSnapshot.rows) {
    const iface = normalizeBillingIfaceName(row.iface);
    if (!iface || !isBillingLikeIface(iface)) continue;
    activeIfaces.add(iface);
    const currentIn = Math.max(0, Number(row.bytes_in || 0));
    const currentOut = Math.max(0, Number(row.bytes_out || 0));
    const currentTotal = currentIn + currentOut;
    const entry = state.by_iface[iface] || {
      iface,
      has_counter: false,
      last_counter_in: 0,
      last_counter_out: 0,
      current_counter_in: 0,
      current_counter_out: 0,
      current_counter_total: 0,
      bytes_in: 0,
      bytes_out: 0,
      total_bytes: 0,
      active: true,
      last_seen_at: ""
    };

    if (!entry.has_counter) {
      entry.has_counter = true;
      entry.last_counter_in = currentIn;
      entry.last_counter_out = currentOut;
    } else {
      let deltaIn = currentIn - Number(entry.last_counter_in || 0);
      let deltaOut = currentOut - Number(entry.last_counter_out || 0);
      if (!Number.isFinite(deltaIn) || deltaIn < 0) deltaIn = 0;
      if (!Number.isFinite(deltaOut) || deltaOut < 0) deltaOut = 0;
      entry.bytes_in = Math.max(0, Number(entry.bytes_in || 0)) + deltaIn;
      entry.bytes_out = Math.max(0, Number(entry.bytes_out || 0)) + deltaOut;
      entry.total_bytes = Math.max(0, Number(entry.total_bytes || 0)) + deltaIn + deltaOut;
      entry.last_counter_in = currentIn;
      entry.last_counter_out = currentOut;
    }

    entry.current_counter_in = currentIn;
    entry.current_counter_out = currentOut;
    entry.current_counter_total = currentTotal;
    entry.active = true;
    entry.last_seen_at = new Date(nowMs).toISOString();
    state.by_iface[iface] = entry;
  }

  for (const [iface, entry] of Object.entries(state.by_iface || {})) {
    if (!activeIfaces.has(iface)) entry.active = false;
  }

  const totals = { bytes_in: 0, bytes_out: 0, total_bytes: 0 };
  for (const entry of Object.values(state.by_iface || {})) {
    totals.bytes_in += Math.max(0, Number(entry.bytes_in || 0));
    totals.bytes_out += Math.max(0, Number(entry.bytes_out || 0));
    totals.total_bytes += Math.max(0, Number(entry.total_bytes || 0));
  }
  state.totals = totals;
  monitorTrafficState.system_net = state;
}

function pruneMonitorTrafficEntries(activePidSet, nowMs) {
  const active = activePidSet || new Set();
  for (const [key, entry] of monitorTrafficState.by_pid.entries()) {
    const pid = Number(entry.pid || 0);
    if (active.has(pid)) continue;
    entry.running = false;
    const lastSeenMs = Number(entry.last_seen_ms || 0);
    if (lastSeenMs > 0 && nowMs - lastSeenMs > MONITOR_TRAFFIC_STALE_PID_MS) {
      monitorTrafficState.by_pid.delete(key);
      continue;
    }
    monitorTrafficState.by_pid.set(key, entry);
  }
}

async function sampleMonitorTrafficAccumulator() {
  if (monitorTrafficState.sampling) return;
  monitorTrafficState.sampling = true;
  try {
    const [raw, billingRaw] = await Promise.all([collectMonitorTrafficRawSnapshot(), collectBillingLikeRawSnapshot()]);
    const nowMs = Date.now();
    const activePidSet = new Set();
    for (const row of raw.rows || []) {
      const pid = Number(row.pid || 0);
      if (Number.isFinite(pid) && pid > 0) activePidSet.add(pid);
      upsertMonitorTrafficEntry(row, nowMs);
    }
    pruneMonitorTrafficEntries(activePidSet, nowMs);
    upsertBillingLikeTraffic(billingRaw, nowMs);
    monitorTrafficState.sample_count = Number(monitorTrafficState.sample_count || 0) + 1;
    monitorTrafficState.last_sample_at = raw.generated_at || new Date(nowMs).toISOString();
    monitorTrafficState.nettop_ok = Boolean(raw.nettop_ok);
    monitorTrafficState.nettop_error = String(raw.nettop_error || "");
    maybePersistMonitorTrafficState(nowMs);
  } catch (error) {
    monitorTrafficState.nettop_ok = false;
    monitorTrafficState.nettop_error = String(error.message || error);
  } finally {
    monitorTrafficState.sampling = false;
  }
}

function ensureMonitorTrafficSampler() {
  if (monitorTrafficState.timer) return;
  monitorTrafficState.timer = setInterval(() => {
    sampleMonitorTrafficAccumulator().catch((error) => {
      monitorTrafficState.nettop_ok = false;
      monitorTrafficState.nettop_error = String(error.message || error);
    });
  }, MONITOR_TRAFFIC_SAMPLE_INTERVAL_MS);
  if (typeof monitorTrafficState.timer.unref === "function") {
    monitorTrafficState.timer.unref();
  }
}

function buildMonitorTrafficAccumulatedPayload() {
  const rows = [...monitorTrafficState.by_pid.values()]
    .filter((r) => Boolean(r.running))
    .map((r) => {
      const bytesIn = Math.max(0, Number(r.bytes_in || 0));
      const bytesOut = Math.max(0, Number(r.bytes_out || 0));
      const totalBytes = Math.max(0, Number(r.total_bytes || 0));
      return {
        target_id: String(r.target_id || ""),
        target_label: String(r.target_label || ""),
        pid: Number(r.pid || 0),
        etime: String(r.etime || ""),
        command: String(r.command || ""),
        process_cell: String(r.process_cell || ""),
        samples: Number(r.samples || 0),
        unknown_counter_samples: Number(r.unknown_counter_samples || 0),
        bytes_in: bytesIn,
        bytes_out: bytesOut,
        total_bytes: totalBytes,
        bytes_in_text: fmtBytes(bytesIn),
        bytes_out_text: fmtBytes(bytesOut),
        total_bytes_text: fmtBytes(totalBytes)
      };
    })
    .sort((a, b) => b.total_bytes - a.total_bytes || a.target_label.localeCompare(b.target_label, "zh-CN"));

  const groupsMap = new Map();
  for (const row of rows) {
    const key = row.target_id;
    const cur = groupsMap.get(key) || {
      target_id: row.target_id,
      target_label: row.target_label,
      process_count: 0,
      pids: [],
      bytes_in: 0,
      bytes_out: 0,
      total_bytes: 0,
      unknown_counter_samples: 0
    };
    cur.process_count += 1;
    cur.pids.push(row.pid);
    cur.bytes_in += row.bytes_in;
    cur.bytes_out += row.bytes_out;
    cur.total_bytes += row.total_bytes;
    cur.unknown_counter_samples += Number(row.unknown_counter_samples || 0);
    groupsMap.set(key, cur);
  }

  const groups = [...groupsMap.values()]
    .map((g) => ({
      ...g,
      pids: [...g.pids].sort((a, b) => a - b),
      bytes_in_text: fmtBytes(g.bytes_in),
      bytes_out_text: fmtBytes(g.bytes_out),
      total_bytes_text: fmtBytes(g.total_bytes)
    }))
    .sort((a, b) => b.total_bytes - a.total_bytes || a.target_label.localeCompare(b.target_label, "zh-CN"));

  const totals = rows.reduce(
    (acc, r) => {
      acc.bytes_in += r.bytes_in;
      acc.bytes_out += r.bytes_out;
      acc.total_bytes += r.total_bytes;
      acc.unknown_counter_samples += Number(r.unknown_counter_samples || 0);
      return acc;
    },
    { bytes_in: 0, bytes_out: 0, total_bytes: 0, unknown_counter_samples: 0 }
  );

  const monitorTotals = {
    process_count: rows.length,
    target_count: groups.length,
    bytes_in: totals.bytes_in,
    bytes_out: totals.bytes_out,
    total_bytes: totals.total_bytes,
    unknown_counter_samples: totals.unknown_counter_samples,
    bytes_in_text: fmtBytes(totals.bytes_in),
    bytes_out_text: fmtBytes(totals.bytes_out),
    total_bytes_text: fmtBytes(totals.total_bytes)
  };

  const billingState = normalizePersistedSystemNetState(monitorTrafficState.system_net || {});
  const billingIfaceRows = Object.values(billingState.by_iface || {})
    .map((x) => {
      const bytesIn = Math.max(0, Number(x.bytes_in || 0));
      const bytesOut = Math.max(0, Number(x.bytes_out || 0));
      const totalBytes = Math.max(0, Number(x.total_bytes || 0));
      return {
        iface: String(x.iface || ""),
        active: Boolean(x.active),
        current_counter_in: Math.max(0, Number(x.current_counter_in || 0)),
        current_counter_out: Math.max(0, Number(x.current_counter_out || 0)),
        current_counter_total: Math.max(0, Number(x.current_counter_total || 0)),
        bytes_in: bytesIn,
        bytes_out: bytesOut,
        total_bytes: totalBytes,
        bytes_in_text: fmtBytes(bytesIn),
        bytes_out_text: fmtBytes(bytesOut),
        total_bytes_text: fmtBytes(totalBytes),
        current_counter_in_text: fmtBytes(Math.max(0, Number(x.current_counter_in || 0))),
        current_counter_out_text: fmtBytes(Math.max(0, Number(x.current_counter_out || 0))),
        current_counter_total_text: fmtBytes(Math.max(0, Number(x.current_counter_total || 0))),
        last_seen_at: String(x.last_seen_at || "")
      };
    })
    .sort((a, b) => b.total_bytes - a.total_bytes || a.iface.localeCompare(b.iface, "en"));

  const billingTotalsRaw = billingState.totals || {};
  const billingTotals = {
    iface_count: billingIfaceRows.length,
    active_iface_count: billingIfaceRows.filter((x) => x.active).length,
    bytes_in: Math.max(0, Number(billingTotalsRaw.bytes_in || 0)),
    bytes_out: Math.max(0, Number(billingTotalsRaw.bytes_out || 0)),
    total_bytes: Math.max(0, Number(billingTotalsRaw.total_bytes || 0))
  };
  billingTotals.bytes_in_text = fmtBytes(billingTotals.bytes_in);
  billingTotals.bytes_out_text = fmtBytes(billingTotals.bytes_out);
  billingTotals.total_bytes_text = fmtBytes(billingTotals.total_bytes);

  return {
    mode: monitorTrafficState.mode,
    sample_interval_sec: Math.round(MONITOR_TRAFFIC_SAMPLE_INTERVAL_MS / 1000),
    accum_started_at: monitorTrafficState.accum_started_at,
    last_sample_at: monitorTrafficState.last_sample_at,
    sample_count: Number(monitorTrafficState.sample_count || 0),
    rows,
    groups,
    totals: monitorTotals,
    monitor_totals: monitorTotals,
    billing_mode: String(billingState.mode || "billing_like_netstat"),
    billing_source: String(billingState.source || "netstat -ib"),
    billing_sample_count: Math.max(0, Number(billingState.sample_count || 0)),
    billing_last_sample_at: String(billingState.last_sample_at || ""),
    billing_netstat_ok: Boolean(billingState.netstat_ok),
    billing_netstat_error: String(billingState.netstat_error || ""),
    billing_ifaces: billingIfaceRows,
    billing_totals: billingTotals,
    nettop_ok: Boolean(monitorTrafficState.nettop_ok),
    nettop_error: String(monitorTrafficState.nettop_error || ""),
    generated_at: new Date().toISOString()
  };
}

async function getMonitorTrafficAccumulated({ refresh = false } = {}) {
  ensureMonitorTrafficSampler();
  const lastSampleMs = Date.parse(String(monitorTrafficState.last_sample_at || ""));
  const stale =
    !Number.isFinite(lastSampleMs) || Date.now() - lastSampleMs >= MONITOR_TRAFFIC_SAMPLE_INTERVAL_MS * 2;
  if (refresh || stale || Number(monitorTrafficState.sample_count || 0) <= 0) {
    await sampleMonitorTrafficAccumulator();
  }
  return buildMonitorTrafficAccumulatedPayload();
}

function shouldShowDcScore(reason) {
  const r = String(reason || "").trim();
  if (!r) return true;
  return !MONITOR_WS_DC_SCORE_HIDDEN_REASONS.has(r);
}

function resolveMonitorWsSignalDbPath(source = "binance") {
  try {
    const control = loadMonitorWsControl();
    const src = normalizeMonitorSource(source);
    const rawRawPath = src === "aster" ? control.aster_signal_db_file : control.signal_db_file;
    const rawPath = String(rawRawPath || "").trim();
    if (!rawPath) {
      return src === "aster" ? MONITOR_WS_SIGNAL_DB_ASTER_DEFAULT : MONITOR_WS_SIGNAL_DB_DEFAULT;
    }
    if (path.isAbsolute(rawPath)) return rawPath;
    return path.join(process.cwd(), rawPath);
  } catch (_) {
    return normalizeMonitorSource(source) === "aster"
      ? MONITOR_WS_SIGNAL_DB_ASTER_DEFAULT
      : MONITOR_WS_SIGNAL_DB_DEFAULT;
  }
}

async function queryMonitorWsPushRows({ windowHours, source = "binance" }) {
  const dbPath = resolveMonitorWsSignalDbPath(source);
  if (!fs.existsSync(dbPath)) {
    return { dbPath, rows: [] };
  }
  const minTs = Date.now() / 1000 - Math.max(1, Number(windowHours || 168)) * 3600;
  const minTsSql = Number(minTs.toFixed(3));
  const sql = `
WITH filtered AS (
  SELECT id, symbol, event_ts, time_bjt, reason, level, tier, final_score
  FROM signal_events
  WHERE delivery = 'realtime'
    AND symbol IS NOT NULL
    AND TRIM(symbol) <> ''
    AND event_ts >= ${minTsSql}
),
agg AS (
  SELECT symbol, COUNT(*) AS push_count, MAX(event_ts) AS last_push_ts
  FROM filtered
  GROUP BY symbol
),
last_evt AS (
  SELECT
    symbol, time_bjt, reason, level, tier, final_score,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY event_ts DESC, id DESC) AS rn
  FROM filtered
)
SELECT
  a.symbol AS symbol,
  a.push_count AS push_count,
  a.last_push_ts AS last_push_ts,
  COALESCE(l.time_bjt, '') AS last_push_bjt,
  COALESCE(l.reason, '') AS last_reason,
  COALESCE(l.level, '') AS last_level,
  COALESCE(l.tier, '') AS last_tier,
  COALESCE(l.final_score, 0.0) AS last_score
FROM agg a
LEFT JOIN last_evt l ON l.symbol = a.symbol AND l.rn = 1;
`.trim();
  const out = await execFileAsync("sqlite3", ["-json", dbPath, sql], 20000);
  const text = String(out.stdout || "").trim();
  if (!text) {
    return { dbPath, rows: [] };
  }
  let parsed = [];
  try {
    parsed = JSON.parse(text);
  } catch (_) {
    throw new Error(`解析 sqlite 输出失败: ${text.slice(0, 300)}`);
  }
  const rows = Array.isArray(parsed) ? parsed : [];
  const normalized = rows
    .map((r) => ({
      symbol: String(r.symbol || "").toUpperCase(),
      push_count: Math.max(0, Number(r.push_count || 0)),
      last_push_ts: Number(r.last_push_ts || 0),
      last_push_bjt: String(r.last_push_bjt || ""),
      last_reason: String(r.last_reason || ""),
      last_level: String(r.last_level || ""),
      last_tier: String(r.last_tier || ""),
      last_score: Number(r.last_score || 0)
    }))
    .filter((r) => r.symbol);
  for (const row of normalized) {
    const visible = shouldShowDcScore(row.last_reason);
    row.dc_score_visible = visible;
    row.dc_visible_score = visible ? Number(row.last_score || 0) : null;
  }
  return { dbPath, rows: normalized };
}

function sortMonitorWsPushRows(rows, sortBy, order) {
  const sorted = [...rows];
  sorted.sort((a, b) => {
    if (sortBy === "count") {
      if (b.push_count !== a.push_count) return b.push_count - a.push_count;
      if (b.last_push_ts !== a.last_push_ts) return b.last_push_ts - a.last_push_ts;
      return String(a.symbol).localeCompare(String(b.symbol));
    }
    if (b.last_push_ts !== a.last_push_ts) return b.last_push_ts - a.last_push_ts;
    if (b.push_count !== a.push_count) return b.push_count - a.push_count;
    return String(a.symbol).localeCompare(String(b.symbol));
  });
  if (order === "asc") sorted.reverse();
  return sorted;
}

function safeFiniteNumber(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function fmtBjtFromTsSec(tsSec) {
  const ts = Number(tsSec || 0);
  if (!Number.isFinite(ts) || ts <= 0) return "";
  try {
    const d = new Date(ts * 1000);
    const f = new Intl.DateTimeFormat("zh-CN", {
      timeZone: "Asia/Shanghai",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false
    });
    const parts = f.formatToParts(d);
    const map = {};
    for (const p of parts) map[p.type] = p.value;
    return `${map.year}-${map.month}-${map.day} ${map.hour}:${map.minute}:${map.second}`;
  } catch (_) {
    return "";
  }
}

function weightedAvg(values, weights) {
  if (!Array.isArray(values) || values.length <= 0) return 0;
  let totalW = 0;
  let totalV = 0;
  for (let i = 0; i < values.length; i += 1) {
    const vv = Number(values[i] || 0);
    const ww = Math.max(0, Number((weights || [])[i] || 0));
    if (!Number.isFinite(vv) || vv <= 0) continue;
    if (!Number.isFinite(ww) || ww <= 0) continue;
    totalV += vv * ww;
    totalW += ww;
  }
  if (totalW <= 1e-12) {
    const arr = values.map((x) => Number(x || 0)).filter((x) => Number.isFinite(x) && x > 0);
    if (!arr.length) return 0;
    return arr.reduce((a, b) => a + b, 0) / arr.length;
  }
  return totalV / totalW;
}

function centroidChangePct(values) {
  const vals = (Array.isArray(values) ? values : [])
    .map((x) => Number(x || 0))
    .filter((x) => Number.isFinite(x) && x > 0);
  if (vals.length < 2) return 0;
  const n = vals.length;
  const half = Math.max(1, Math.floor(n / 2));
  const h1 = vals.slice(0, half);
  const h2 = vals.slice(half);
  const c1 = h1.reduce((a, b) => a + b, 0) / h1.length;
  const c2 = h2.reduce((a, b) => a + b, 0) / h2.length;
  if (!Number.isFinite(c1) || c1 <= 1e-12) return 0;
  return ((c2 - c1) / c1) * 100;
}

function emaStartEnd(values, span) {
  const vals = (Array.isArray(values) ? values : [])
    .map((x) => Number(x || 0))
    .filter((x) => Number.isFinite(x) && x > 0);
  if (!vals.length) return { start: 0, end: 0 };
  const nSpan = Math.max(1, Number(span || 1));
  const alpha = 2 / (nSpan + 1);
  let ema = vals[0];
  let start = ema;
  for (let i = 1; i < vals.length; i += 1) {
    ema = alpha * vals[i] + (1 - alpha) * ema;
    if (i === 1) start = ema;
  }
  return { start, end: ema };
}

function buildSlowAccumMetrics(prices, volumes, minStepPct) {
  const vals = (Array.isArray(prices) ? prices : [])
    .map((x) => Number(x || 0))
    .filter((x) => Number.isFinite(x) && x > 0);
  if (vals.length < 16) {
    return {
      trend_ok: false,
      metrics: {
        centroid_change_pct: 0,
        vwap_change_pct: 0,
        stair_up_ratio: 0,
        stair_change_pct: 0,
        stair_ok: 0,
        ema_short_start: 0,
        ema_short_end: 0,
        ema_long_start: 0,
        ema_long_end: 0
      }
    };
  }
  const volsRaw = Array.isArray(volumes) ? volumes : [];
  const vols = vals.map((_, i) => {
    const vv = Number(volsRaw[i] || 1);
    if (!Number.isFinite(vv) || vv <= 0) return 1;
    return vv;
  });
  const n = vals.length;
  const q = Math.max(1, Math.floor(n / 4));
  const q1 = vals.slice(0, q);
  const q2 = vals.slice(q, 2 * q);
  const q3 = vals.slice(2 * q, 3 * q);
  const q4 = (3 * q) < n ? vals.slice(3 * q) : vals.slice(-1);

  const w1 = vols.slice(0, q1.length);
  const w2 = vols.slice(q1.length, q1.length + q2.length);
  const w3 = vols.slice(q1.length + q2.length, q1.length + q2.length + q3.length);
  const w4 = vols.slice(q1.length + q2.length + q3.length, q1.length + q2.length + q3.length + q4.length);

  const c1 = weightedAvg(q1, w1);
  const c2 = weightedAvg(q2, w2);
  const c3 = weightedAvg(q3, w3);
  const c4 = weightedAvg(q4, w4);
  const stairs = [c1, c2, c3, c4];
  let upCnt = 0;
  for (let i = 1; i < stairs.length; i += 1) {
    if (stairs[i] > stairs[i - 1]) upCnt += 1;
  }
  const stairUpRatio = upCnt / 3;
  const stairChangePct = c1 > 1e-12 ? ((c4 - c1) / c1) * 100 : 0;

  const edgeN = Math.max(3, Math.floor(n / 10));
  const vwapHead = weightedAvg(vals.slice(0, edgeN), vols.slice(0, edgeN));
  const vwapTail = weightedAvg(vals.slice(-edgeN), vols.slice(-edgeN));
  const vwapChangePct = vwapHead > 1e-12 ? ((vwapTail - vwapHead) / vwapHead) * 100 : 0;

  const centroidPct = centroidChangePct(vals);
  const emaS = emaStartEnd(vals, 12);
  const emaL = emaStartEnd(vals, 36);
  const emaTrendOk = emaS.end > emaL.end;
  const minStep = Math.max(0, Number(minStepPct || 0));
  const stairOk = stairUpRatio >= (2 / 3) && stairChangePct >= minStep;
  const centroidOk = centroidPct >= minStep;
  const vwapOk = vwapChangePct >= minStep;
  const trendOk = Boolean(emaTrendOk && (centroidOk || vwapOk));

  return {
    trend_ok: trendOk,
    metrics: {
      centroid_change_pct: centroidPct,
      vwap_change_pct: vwapChangePct,
      stair_up_ratio: stairUpRatio,
      stair_change_pct: stairChangePct,
      stair_ok: stairOk ? 1 : 0,
      ema_short_start: emaS.start,
      ema_short_end: emaS.end,
      ema_long_start: emaL.start,
      ema_long_end: emaL.end
    }
  };
}

function makeProxyPriceSeriesFromRet(rows) {
  const out = [];
  for (const row of rows) {
    const r = safeFiniteNumber(row.ret_1m_pct, 0);
    out.push(100 * (1 + r / 100));
  }
  return out;
}

async function querySlowAccumBreakdownRows({ source = "binance", windowHours = 24, limit = 400 }) {
  const dbPath = resolveMonitorWsSignalDbPath(source);
  if (!fs.existsSync(dbPath)) {
    return {
      dbPath,
      rows: [],
      meta: {
        source,
        window_hours: windowHours,
        slow_window_sec: 5400,
        min_hits: 100,
        min_ret_1m_pct: 0.25,
        max_ret_1m_pct: 0.8,
        min_stair_step_pct: 0.5,
        cooldown_sec: 3600
      }
    };
  }

  const control = loadMonitorWsControl();
  const nowTs = Date.now() / 1000;
  const activityWindowSec = Math.max(3600, Number(windowHours || 24) * 3600);
  const slowWindowSec = Math.max(300, safeFiniteNumber(control.slow_accum_window_sec, 5400));
  const minHits = Math.max(1, Math.floor(safeFiniteNumber(control.slow_accum_min_hits, 100)));
  const minRet = Math.max(0, safeFiniteNumber(control.slow_accum_min_ret_1m_pct, 0.25));
  const maxRet = Math.max(minRet, safeFiniteNumber(control.slow_accum_max_ret_1m_pct, 0.8));
  const minStep = Math.max(0, safeFiniteNumber(control.slow_accum_min_stair_step_pct, 0.5));
  const cooldownSec = Math.max(
    300,
    Math.max(
      safeFiniteNumber(control.slow_accum_alert_cooldown_sec, 3600),
      safeFiniteNumber(control.module_signal_cooldown_sec, 1200)
    )
  );
  const minActivityTs = Number((nowTs - activityWindowSec).toFixed(3));
  const minCurrentTs = Number((nowTs - slowWindowSec).toFixed(3));
  const minSlowTs = Number((nowTs - Math.max(activityWindowSec, cooldownSec * 2, 24 * 3600)).toFixed(3));

  const activitySql = `
WITH raw AS (
  SELECT
    symbol,
    event_ts,
    COALESCE(json_extract(payload_json, '$.ret_1m_pct'), 0.0) AS ret_1m_pct
  FROM signal_events
  WHERE reason = 'fake_breakout_watch'
    AND symbol IS NOT NULL
    AND TRIM(symbol) <> ''
    AND event_ts >= ${minActivityTs}
)
SELECT
  symbol,
  COUNT(*) AS total_fake_window,
  SUM(CASE WHEN ret_1m_pct >= ${minRet} AND ret_1m_pct <= ${maxRet} THEN 1 ELSE 0 END) AS in_band_window,
  MAX(event_ts) AS last_fake_ts
FROM raw
GROUP BY symbol
ORDER BY in_band_window DESC, last_fake_ts DESC, symbol ASC;
`.trim();

  const currentSql = `
SELECT
  symbol,
  event_ts,
  COALESCE(json_extract(payload_json, '$.ret_1m_pct'), 0.0) AS ret_1m_pct,
  COALESCE(json_extract(payload_json, '$.vol_1m'), 1.0) AS vol_1m,
  COALESCE(json_extract(payload_json, '$.last_price'), 0.0) AS last_price
FROM signal_events
WHERE reason = 'fake_breakout_watch'
  AND symbol IS NOT NULL
  AND TRIM(symbol) <> ''
  AND event_ts >= ${minCurrentTs}
  AND COALESCE(json_extract(payload_json, '$.ret_1m_pct'), 0.0) >= ${minRet}
  AND COALESCE(json_extract(payload_json, '$.ret_1m_pct'), 0.0) <= ${maxRet}
ORDER BY symbol ASC, event_ts ASC;
`.trim();

  const slowSql = `
SELECT symbol, MAX(event_ts) AS last_slow_ts
FROM signal_events
WHERE reason = 'slow_accumulation'
  AND symbol IS NOT NULL
  AND TRIM(symbol) <> ''
  AND event_ts >= ${minSlowTs}
GROUP BY symbol;
`.trim();

  const [actOut, curOut, slowOut] = await Promise.all([
    execFileAsync("sqlite3", ["-json", dbPath, activitySql], 20000),
    execFileAsync("sqlite3", ["-json", dbPath, currentSql], 20000),
    execFileAsync("sqlite3", ["-json", dbPath, slowSql], 20000)
  ]);

  const actRows = (() => {
    const t = String(actOut.stdout || "").trim();
    if (!t) return [];
    try { return JSON.parse(t); } catch (_) { return []; }
  })();
  const curRows = (() => {
    const t = String(curOut.stdout || "").trim();
    if (!t) return [];
    try { return JSON.parse(t); } catch (_) { return []; }
  })();
  const slowRows = (() => {
    const t = String(slowOut.stdout || "").trim();
    if (!t) return [];
    try { return JSON.parse(t); } catch (_) { return []; }
  })();

  const currentMap = new Map();
  for (const row of Array.isArray(curRows) ? curRows : []) {
    const symbol = String(row.symbol || "").trim().toUpperCase();
    if (!symbol) continue;
    if (!currentMap.has(symbol)) currentMap.set(symbol, []);
    currentMap.get(symbol).push({
      event_ts: safeFiniteNumber(row.event_ts, 0),
      ret_1m_pct: safeFiniteNumber(row.ret_1m_pct, 0),
      vol_1m: Math.max(1, safeFiniteNumber(row.vol_1m, 1)),
      last_price: safeFiniteNumber(row.last_price, 0)
    });
  }

  const slowMap = new Map();
  for (const row of Array.isArray(slowRows) ? slowRows : []) {
    const symbol = String(row.symbol || "").trim().toUpperCase();
    if (!symbol) continue;
    slowMap.set(symbol, safeFiniteNumber(row.last_slow_ts, 0));
  }

  const outRows = [];
  for (const row of Array.isArray(actRows) ? actRows : []) {
    const symbol = String(row.symbol || "").trim().toUpperCase();
    if (!symbol) continue;
    const seq = currentMap.get(symbol) || [];
    const hitsCurrent = seq.length;
    const lastFakeTs = safeFiniteNumber(row.last_fake_ts, 0);
    const lastSlowTs = safeFiniteNumber(slowMap.get(symbol), 0);

    let trendMode = "none";
    let metrics = {
      centroid_change_pct: 0,
      vwap_change_pct: 0,
      stair_up_ratio: 0,
      stair_change_pct: 0,
      stair_ok: 0,
      ema_short_end: 0,
      ema_long_end: 0
    };
    let trendOk = false;
    if (hitsCurrent >= 16) {
      const realPrices = seq.map((x) => safeFiniteNumber(x.last_price, 0)).filter((x) => x > 0);
      const volumes = seq.map((x) => Math.max(1, safeFiniteNumber(x.vol_1m, 1)));
      if (realPrices.length >= 16) {
        const r = buildSlowAccumMetrics(realPrices, volumes, minStep);
        trendOk = Boolean(r.trend_ok);
        metrics = { ...metrics, ...r.metrics };
        trendMode = "price";
      } else {
        const proxyPrices = makeProxyPriceSeriesFromRet(seq);
        const r = buildSlowAccumMetrics(proxyPrices, volumes, minStep);
        trendOk = Boolean(r.trend_ok);
        metrics = { ...metrics, ...r.metrics };
        trendMode = "ret_proxy";
      }
    }

    const blockedHits = hitsCurrent < minHits;
    const blockedTrend = !blockedHits && !trendOk;
    const cooldownLeftSec = lastSlowTs > 0 ? Math.max(0, Math.round(cooldownSec - (nowTs - lastSlowTs))) : 0;
    const blockedCooldown = !blockedHits && !blockedTrend && cooldownLeftSec > 0;
    const stage = blockedHits
      ? "HITS"
      : (blockedTrend ? "TREND" : (blockedCooldown ? "COOLDOWN" : "READY"));

    outRows.push({
      symbol,
      total_fake_window: Math.max(0, Math.floor(safeFiniteNumber(row.total_fake_window, 0))),
      in_band_window: Math.max(0, Math.floor(safeFiniteNumber(row.in_band_window, 0))),
      hits_current: hitsCurrent,
      hits_gap: Math.max(0, minHits - hitsCurrent),
      blocked_hits: blockedHits,
      blocked_trend: blockedTrend,
      blocked_cooldown: blockedCooldown,
      stage,
      trend_mode: trendMode,
      cooldown_left_sec: cooldownLeftSec,
      last_fake_ts: lastFakeTs,
      last_fake_bjt: fmtBjtFromTsSec(lastFakeTs),
      last_slow_ts: lastSlowTs,
      last_slow_bjt: fmtBjtFromTsSec(lastSlowTs),
      centroid_change_pct: safeFiniteNumber(metrics.centroid_change_pct, 0),
      vwap_change_pct: safeFiniteNumber(metrics.vwap_change_pct, 0),
      stair_up_ratio: safeFiniteNumber(metrics.stair_up_ratio, 0),
      stair_change_pct: safeFiniteNumber(metrics.stair_change_pct, 0),
      stair_ok: safeFiniteNumber(metrics.stair_ok, 0) >= 0.5,
      ema_short_end: safeFiniteNumber(metrics.ema_short_end, 0),
      ema_long_end: safeFiniteNumber(metrics.ema_long_end, 0),
      ema_trend_ok: safeFiniteNumber(metrics.ema_short_end, 0) > safeFiniteNumber(metrics.ema_long_end, 0)
    });
  }

  outRows.sort((a, b) => {
    if (b.hits_current !== a.hits_current) return b.hits_current - a.hits_current;
    if (b.in_band_window !== a.in_band_window) return b.in_band_window - a.in_band_window;
    if (b.last_fake_ts !== a.last_fake_ts) return b.last_fake_ts - a.last_fake_ts;
    return String(a.symbol).localeCompare(String(b.symbol));
  });

  return {
    dbPath,
    rows: outRows.slice(0, Math.max(1, Math.floor(Number(limit || 400)))),
    meta: {
      source: normalizeMonitorSource(source),
      window_hours: windowHours,
      slow_window_sec: slowWindowSec,
      min_hits: minHits,
      min_ret_1m_pct: minRet,
      max_ret_1m_pct: maxRet,
      min_stair_step_pct: minStep,
      cooldown_sec: cooldownSec
    }
  };
}

function tailTextFile(filePath, maxLines = 140, maxBytes = 256 * 1024) {
  if (!filePath || !fs.existsSync(filePath)) return "";
  let fd = null;
  try {
    const st = fs.statSync(filePath);
    const size = Number(st.size || 0);
    if (size <= 0) return "";
    const start = Math.max(0, size - maxBytes);
    const readSize = size - start;
    if (readSize <= 0) return "";
    const buf = Buffer.alloc(readSize);
    fd = fs.openSync(filePath, "r");
    fs.readSync(fd, buf, 0, readSize, start);
    let text = buf.toString("utf8");
    if (start > 0) {
      const cut = text.indexOf("\n");
      if (cut >= 0) text = text.slice(cut + 1);
    }
    const lines = text.split(/\r?\n/).filter((line) => line.length > 0);
    return lines.slice(-Math.max(20, maxLines)).join("\n");
  } catch (_) {
    return "";
  } finally {
    if (fd !== null) {
      try {
        fs.closeSync(fd);
      } catch (_) {}
    }
  }
}

function parseMonitorWsProcessList(psText) {
  const rows = String(psText || "").split(/\r?\n/);
  const found = [];
  for (const row of rows) {
    const line = row.trim();
    if (!line) continue;
    const m = line.match(/^(\d+)\s+(\S+)\s+(.+)$/);
    if (!m) continue;
    const pid = Number(m[1]);
    const etime = m[2];
    const command = m[3];
    const isMonitorWs =
      command.includes("binance_futures_monitor.monitor_ws") ||
      command.includes("binance_futures_monitor/monitor_ws.py") ||
      command.includes("monitor_ws.py");
    if (!isMonitorWs) continue;
    found.push({ pid, etime, command });
  }
  found.sort((a, b) => b.pid - a.pid);
  return found[0] || null;
}

async function getMonitorWsRuntime(maxLines = 140) {
  let processInfo = null;
  try {
    const out = await execFileAsync("ps", ["-axo", "pid=,etime=,command="], 8000);
    processInfo = parseMonitorWsProcessList(out.stdout);
  } catch (error) {
    logger.warn("Read monitor_ws process failed", { error: String(error.message || error) });
  }
  const logFile = detectMonitorWsLogFile();
  const logTail = tailTextFile(logFile, maxLines);
  return {
    running: Boolean(processInfo),
    process: processInfo,
    log_file: logFile || "",
    log_tail: logTail,
    checked_at: new Date().toISOString()
  };
}

function readPidFromFile(pidFile) {
  try {
    if (!pidFile || !fs.existsSync(pidFile)) return 0;
    const text = String(fs.readFileSync(pidFile, "utf8") || "").trim();
    const pid = Number(text);
    if (!Number.isFinite(pid) || pid <= 0) return 0;
    return Math.floor(pid);
  } catch (_) {
    return 0;
  }
}

function parseSinglePsLine(text) {
  const line = String(text || "")
    .split(/\r?\n/)
    .map((x) => x.trim())
    .find(Boolean);
  if (!line) return null;
  const m = line.match(/^(\d+)\s+(\S+)\s+(.+)$/);
  if (!m) return null;
  return {
    pid: Number(m[1]),
    etime: String(m[2] || ""),
    command: String(m[3] || "")
  };
}

async function getProcessInfoByPidFile(pidFile, role) {
  const pid = readPidFromFile(pidFile);
  if (!pid) {
    return {
      role,
      running: false,
      pid: 0,
      etime: "",
      command: "",
      pid_file: pidFile,
      stale: false
    };
  }
  try {
    const out = await execFileAsync("ps", ["-p", String(pid), "-o", "pid=,etime=,command="], 8000);
    const info = parseSinglePsLine(out.stdout);
    if (!info || Number(info.pid) !== Number(pid)) {
      return {
        role,
        running: false,
        pid,
        etime: "",
        command: "",
        pid_file: pidFile,
        stale: true
      };
    }
    return {
      role,
      running: true,
      pid: Number(info.pid || 0),
      etime: String(info.etime || ""),
      command: String(info.command || ""),
      pid_file: pidFile,
      stale: false
    };
  } catch (_) {
    return {
      role,
      running: false,
      pid,
      etime: "",
      command: "",
      pid_file: pidFile,
      stale: true
    };
  }
}

function parseSqliteJsonText(text) {
  const t = String(text || "").trim();
  if (!t) return [];
  try {
    const parsed = JSON.parse(t);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
}

async function queryTraeDbStats() {
  if (!fs.existsSync(TRAE_DB_FILE)) {
    return {
      source_events: 0,
      rejudge_events: 0,
      checkpoints: 0,
      last_source_ts: 0,
      last_rejudge_ts: 0,
      last_source_bjt: "",
      last_rejudge_bjt: ""
    };
  }
  const sql = `
SELECT
  (SELECT COUNT(*) FROM source_events) AS source_events,
  (SELECT COUNT(*) FROM rejudge_events) AS rejudge_events,
  (SELECT COUNT(*) FROM checkpoints) AS checkpoints,
  COALESCE((SELECT MAX(last_alert_time) FROM source_events), 0) AS last_source_ts,
  COALESCE((SELECT MAX(analyzed_ts) FROM rejudge_events), 0) AS last_rejudge_ts;
`.trim();
  const out = await execFileAsync("sqlite3", ["-json", TRAE_DB_FILE, sql], 12000);
  const rows = parseSqliteJsonText(out.stdout);
  const row = rows[0] || {};
  const lastSourceTs = Number(row.last_source_ts || 0);
  const lastRejudgeTs = Number(row.last_rejudge_ts || 0);
  return {
    source_events: Math.max(0, Number(row.source_events || 0)),
    rejudge_events: Math.max(0, Number(row.rejudge_events || 0)),
    checkpoints: Math.max(0, Number(row.checkpoints || 0)),
    last_source_ts: lastSourceTs,
    last_rejudge_ts: lastRejudgeTs,
    last_source_bjt: fmtBjtFromTsSec(lastSourceTs),
    last_rejudge_bjt: fmtBjtFromTsSec(lastRejudgeTs)
  };
}

async function queryTraeRecentRows(limit = 60) {
  if (!fs.existsSync(TRAE_DB_FILE)) return [];
  const safeLimit = Math.max(1, Math.min(300, Number(limit || 60)));
  const sql = `
SELECT
  r.id AS id,
  COALESCE(r.analyzed_ts, 0) AS analyzed_ts,
  COALESCE(r.analyzed_time_bjt, '') AS analyzed_time_bjt,
  COALESCE(r.symbol_norm, '') AS symbol_norm,
  COALESCE(r.planb_level, '') AS planb_level,
  COALESCE(r.planb_score, 0.0) AS planb_score,
  COALESCE(r.planb_threshold, 0.0) AS planb_threshold,
  COALESCE(r.planb_alert, 0) AS planb_alert,
  COALESCE(r.planb_watch_alert, 0) AS planb_watch_alert,
  COALESCE(s.symbol_raw, '') AS source_symbol_raw,
  COALESCE(s.source_time_bjt, '') AS source_time_bjt,
  COALESCE(s.alert_count, 0) AS source_alert_count
FROM rejudge_events r
LEFT JOIN source_events s ON s.id = r.source_event_id
ORDER BY r.id DESC
LIMIT ${safeLimit};
`.trim();
  const out = await execFileAsync("sqlite3", ["-json", TRAE_DB_FILE, sql], 15000);
  const rows = parseSqliteJsonText(out.stdout);
  return rows.map((row) => ({
    id: Number(row.id || 0),
    analyzed_ts: Number(row.analyzed_ts || 0),
    analyzed_time_bjt: String(row.analyzed_time_bjt || ""),
    symbol_norm: String(row.symbol_norm || "").toUpperCase(),
    planb_level: String(row.planb_level || ""),
    planb_score: Number(row.planb_score || 0),
    planb_threshold: Number(row.planb_threshold || 0),
    planb_alert: Number(row.planb_alert || 0),
    planb_watch_alert: Number(row.planb_watch_alert || 0),
    source_symbol_raw: String(row.source_symbol_raw || ""),
    source_time_bjt: String(row.source_time_bjt || ""),
    source_alert_count: Number(row.source_alert_count || 0)
  }));
}

async function getTraeRuntime(maxLines = 160, recentLimit = 60) {
  const [planA, bridge, guard, stats, recentRows] = await Promise.all([
    getProcessInfoByPidFile(TRAE_PLAN_A_PID_FILE, "plan_a"),
    getProcessInfoByPidFile(TRAE_BRIDGE_PID_FILE, "bridge"),
    getProcessInfoByPidFile(TRAE_GUARD_PID_FILE, "guard"),
    queryTraeDbStats(),
    queryTraeRecentRows(recentLimit)
  ]);
  return {
    running: Boolean(planA.running && bridge.running),
    running_all: Boolean(planA.running && bridge.running && guard.running),
    trae_dir: TRAE_DIR,
    start_script: TRAE_START_SCRIPT,
    db_file: TRAE_DB_FILE,
    plan_a: {
      ...planA,
      log_file: TRAE_PLAN_A_LOG_FILE,
      log_tail: tailTextFile(TRAE_PLAN_A_LOG_FILE, maxLines)
    },
    bridge: {
      ...bridge,
      log_file: TRAE_BRIDGE_OUT_FILE,
      log_tail: tailTextFile(TRAE_BRIDGE_OUT_FILE, maxLines)
    },
    guard: {
      ...guard,
      log_file: TRAE_GUARD_LOG_FILE,
      log_tail: tailTextFile(TRAE_GUARD_LOG_FILE, maxLines)
    },
    bridge_debug: {
      log_file: TRAE_BRIDGE_LOG_FILE,
      log_tail: tailTextFile(TRAE_BRIDGE_LOG_FILE, maxLines)
    },
    starter_log: {
      log_file: TRAE_START_LAUNCH_LOG,
      log_tail: tailTextFile(TRAE_START_LAUNCH_LOG, 60)
    },
    db_stats: stats,
    recent_rows: recentRows,
    checked_at: new Date().toISOString()
  };
}

async function startTraeProcess() {
  if (!fs.existsSync(TRAE_START_SCRIPT)) {
    return {
      ok: false,
      message: `未找到启动脚本: ${TRAE_START_SCRIPT}`
    };
  }
  const runtime = await getTraeRuntime(40, 10);
  if (runtime.plan_a.running && runtime.bridge.running) {
    return {
      ok: false,
      already_running: true,
      message: `TRAE 已运行 (plan_a PID ${runtime.plan_a.pid}, bridge PID ${runtime.bridge.pid})`,
      runtime
    };
  }

  fs.mkdirSync(TRAE_DIR, { recursive: true });
  let env = { ...process.env, PYTHONUNBUFFERED: process.env.PYTHONUNBUFFERED || "1" };
  const proxyUrl = await resolvePreferredProxyUrl();
  env = applyProxyEnv(env, proxyUrl);
  const outFd = fs.openSync(TRAE_START_LAUNCH_LOG, "a");
  const errFd = fs.openSync(TRAE_START_LAUNCH_LOG, "a");
  try {
    const child = spawn("/bin/zsh", ["-lc", `"${TRAE_START_SCRIPT}"`], {
      cwd: process.cwd(),
      detached: true,
      stdio: ["ignore", outFd, errFd],
      env
    });
    child.unref();
    return {
      ok: true,
      pid: Number(child.pid || 0),
      command: `/bin/zsh -lc "${TRAE_START_SCRIPT}"`,
      log_file: TRAE_START_LAUNCH_LOG,
      started_at: new Date().toISOString()
    };
  } finally {
    try {
      fs.closeSync(outFd);
    } catch (_) {}
    try {
      fs.closeSync(errFd);
    } catch (_) {}
  }
}

function buildMonitorWsStartCommand() {
  return {
    cmd: "python3",
    args: [...MONITOR_WS_DEFAULT_ARGS]
  };
}

async function startMonitorWsProcess() {
  const runtime = await getMonitorWsRuntime(40);
  if (runtime.running && runtime.process) {
    return {
      ok: false,
      message: `monitor_ws 已在运行 (PID ${runtime.process.pid})`,
      already_running: true,
      runtime
    };
  }

  fs.mkdirSync(MONITOR_LOG_DIR, { recursive: true });
  const command = buildMonitorWsStartCommand();
  let env = { ...process.env };
  env.PYTHONUNBUFFERED = env.PYTHONUNBUFFERED || "1";
  env.MONITOR_WS_WEBHOOK_CONFIG_FILE =
    env.MONITOR_WS_WEBHOOK_CONFIG_FILE || "config/webhook_monitor_ws.json";
  const proxyUrl = await resolvePreferredProxyUrl();
  env = applyProxyEnv(env, proxyUrl);
  const outFd = fs.openSync(MONITOR_WS_LOG_FILE, "a");
  const errFd = fs.openSync(MONITOR_WS_LOG_FILE, "a");

  try {
    const child = spawn(command.cmd, command.args, {
      cwd: process.cwd(),
      detached: true,
      stdio: ["ignore", outFd, errFd],
      env
    });
    child.unref();
    return {
      ok: true,
      pid: Number(child.pid || 0),
      command: `${command.cmd} ${command.args.join(" ")}`,
      log_file: MONITOR_WS_LOG_FILE,
      started_at: new Date().toISOString()
    };
  } finally {
    try {
      fs.closeSync(outFd);
    } catch (_) {}
    try {
      fs.closeSync(errFd);
    } catch (_) {}
  }
}

function fmtBytes(bytes) {
  const n = Number(bytes || 0);
  if (!Number.isFinite(n) || n <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let x = n;
  let i = 0;
  while (x >= 1024 && i < units.length - 1) {
    x /= 1024;
    i += 1;
  }
  return `${x.toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
}

function parseSymbolFromFile(name) {
  const m = String(name || "").match(/^([A-Z0-9]+)_(?:klines_)?\d{4}-\d{2}-\d{2}\.parquet$/);
  return m ? m[1] : null;
}

function newCollectorJob(type, symbols) {
  const id = `collector_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const job = {
    id,
    type,
    symbols,
    total: symbols.length,
    done: 0,
    failed: 0,
    status: "queued",
    message: "采集任务排队中",
    startedAt: Date.now(),
    finishedAt: null,
    errors: []
  };
  collectorJobs.set(id, job);
  return job;
}

function updateCollectorJob(job, patch) {
  Object.assign(job, patch);
  collectorJobs.set(job.id, job);
}

async function listTopVolumeSymbols(limit = 10) {
  const rows = await fetchJson("https://fapi.binance.com/fapi/v1/ticker/24hr", 12000);
  if (!Array.isArray(rows)) return [];
  const picked = rows
    .filter((x) => String(x.symbol || "").endsWith("USDT"))
    .map((x) => ({
      symbol: String(x.symbol || "").toUpperCase(),
      quoteVolume: Number(x.quoteVolume || 0)
    }))
    .filter((x) => x.symbol && Number.isFinite(x.quoteVolume))
    .sort((a, b) => b.quoteVolume - a.quoteVolume)
    .slice(0, Math.max(1, Number(limit || 10)))
    .map((x) => x.symbol);
  return [...new Set(picked)];
}

async function runCollectorBatch(job, hours = COLLECT_BATCH_HOURS) {
  const symbols = Array.isArray(job.symbols) ? job.symbols : [];
  updateCollectorJob(job, { status: "running", message: `开始采集 ${symbols.length} 个币种` });
  if (!symbols.length) {
    updateCollectorJob(job, { status: "error", message: "无可采集币种", finishedAt: Date.now() });
    return;
  }

  let idx = 0;
  let running = 0;
  const errors = [];

  await new Promise((resolve) => {
    const pump = async () => {
      while (running < COLLECT_CONCURRENCY && idx < symbols.length) {
        const symbol = symbols[idx++];
        running += 1;
        updateCollectorJob(job, {
          message: `采集中 ${symbol} (${job.done + job.failed + 1}/${job.total})`
        });
        execFileAsync("python3", ["collect_lob.py", symbol, "--hours", String(hours)], 12 * 60 * 1000)
          .then(() => {
            job.done += 1;
          })
          .catch((e) => {
            job.failed += 1;
            errors.push(`${symbol}: ${String(e.message || e).slice(0, 180)}`);
          })
          .finally(() => {
            running -= 1;
            updateCollectorJob(job, {
              done: job.done,
              failed: job.failed,
              message: `进行中 ${job.done + job.failed}/${job.total}`
            });
            if (idx >= symbols.length && running === 0) resolve();
            else pump();
          });
      }
    };
    pump();
  });

  updateCollectorJob(job, {
    status: "done",
    message: `采集完成：成功${job.done}，失败${job.failed}`,
    errors: errors.slice(0, 20),
    finishedAt: Date.now()
  });
}

async function runCollectorRound(job, symbols, hours) {
  let idx = 0;
  let running = 0;
  let okCount = 0;
  let failCount = 0;
  const errors = [];

  await new Promise((resolve) => {
    const pump = async () => {
      while (running < COLLECT_CONCURRENCY && idx < symbols.length) {
        const symbol = symbols[idx++];
        running += 1;
        updateCollectorJob(job, {
          message: `轮次#${job.round} 采集中 ${symbol} (${okCount + failCount + 1}/${symbols.length})`,
          roundDone: okCount,
          roundFailed: failCount
        });

        execFileAsync("python3", ["collect_lob.py", symbol, "--hours", String(hours)], 12 * 60 * 1000)
          .then(() => {
            okCount += 1;
            job.done += 1;
          })
          .catch((e) => {
            failCount += 1;
            job.failed += 1;
            errors.push(`${symbol}: ${String(e.message || e).slice(0, 180)}`);
          })
          .finally(() => {
            running -= 1;
            updateCollectorJob(job, {
              done: job.done,
              failed: job.failed,
              roundDone: okCount,
              roundFailed: failCount,
              message: `轮次#${job.round} 进行中 ${okCount + failCount}/${symbols.length}`
            });
            if (idx >= symbols.length && running === 0) resolve();
            else pump();
          });
      }
    };
    pump();
  });

  return { okCount, failCount, errors };
}

async function runCollectorContinuous(job, hours = COLLECT_BATCH_HOURS) {
  const symbols = Array.isArray(job.symbols) ? job.symbols : [];
  if (!symbols.length) {
    updateCollectorJob(job, { status: "error", message: "无可采集币种", finishedAt: Date.now() });
    return;
  }

  collectorRuntime.activeJobId = job.id;
  collectorRuntime.stopRequested = false;
  updateCollectorJob(job, {
    status: "running",
    message: `持续采集已启动，币种数=${symbols.length}`,
    round: 0,
    roundDone: 0,
    roundFailed: 0
  });

  while (!collectorRuntime.stopRequested && collectorRuntime.activeJobId === job.id) {
    job.round = Number(job.round || 0) + 1;
    updateCollectorJob(job, {
      status: "running",
      round: job.round,
      message: `开始轮次#${job.round}`,
      roundDone: 0,
      roundFailed: 0
    });
    const roundRes = await runCollectorRound(job, symbols, hours);
    updateCollectorJob(job, {
      status: "running",
      message: `轮次#${job.round} 完成：成功${roundRes.okCount}，失败${roundRes.failCount}`,
      errors: roundRes.errors.slice(0, 20)
    });
    await new Promise((r) => setTimeout(r, 1000));
  }

  updateCollectorJob(job, {
    status: "stopped",
    message: "持续采集已停止",
    finishedAt: Date.now()
  });
}

function getCollectorStatus(dataDir = path.join(process.cwd(), "data")) {
  const out = {
    ok: true,
    dataDir,
    fileCount: 0,
    lobFileCount: 0,
    klineFileCount: 0,
    totalBytes: 0,
    totalSize: "0 B",
    latestFile: null,
    latestMtimeMs: 0,
    latestISO: null,
    active: false,
    activeThresholdSec: 20,
    symbols: [],
    collectorJob: null,
    timestamp: new Date().toISOString()
  };

  if (!fs.existsSync(dataDir)) return out;
  const names = fs.readdirSync(dataDir);
  const perSymbol = new Map();
  const now = Date.now();

  for (const name of names) {
    if (!name.endsWith(".parquet")) continue;
    const fp = path.join(dataDir, name);
    let st = null;
    try {
      st = fs.statSync(fp);
    } catch (_) {
      continue;
    }
    if (!st.isFile()) continue;
    const size = Number(st.size || 0);
    const mtimeMs = Number(st.mtimeMs || 0);
    out.fileCount += 1;
    out.totalBytes += size;
    if (name.includes("_klines_")) out.klineFileCount += 1;
    else out.lobFileCount += 1;
    if (mtimeMs > out.latestMtimeMs) {
      out.latestMtimeMs = mtimeMs;
      out.latestFile = name;
      out.latestISO = new Date(mtimeMs).toISOString();
    }

    const symbol = parseSymbolFromFile(name);
    if (!symbol) continue;
    const prev = perSymbol.get(symbol) || { symbol, files: 0, bytes: 0, latestMtimeMs: 0 };
    prev.files += 1;
    prev.bytes += size;
    prev.latestMtimeMs = Math.max(prev.latestMtimeMs, mtimeMs);
    perSymbol.set(symbol, prev);
  }

  out.totalSize = fmtBytes(out.totalBytes);
  out.active = out.latestMtimeMs > 0 ? (now - out.latestMtimeMs <= out.activeThresholdSec * 1000) : false;
  out.symbols = [...perSymbol.values()]
    .sort((a, b) => b.latestMtimeMs - a.latestMtimeMs)
    .slice(0, 8)
    .map((x) => ({
      symbol: x.symbol,
      files: x.files,
      size: fmtBytes(x.bytes),
      latestISO: x.latestMtimeMs ? new Date(x.latestMtimeMs).toISOString() : null
    }));
  const running = [...collectorJobs.values()].reverse().find((j) => j.status === "running" || j.status === "queued");
  if (running) {
    out.collectorJob = {
      id: running.id,
      type: running.type,
      status: running.status,
      done: running.done,
      failed: running.failed,
      total: running.total,
      round: running.round || 0,
      roundDone: running.roundDone || 0,
      roundFailed: running.roundFailed || 0,
      message: running.message
    };
  }
  return out;
}

function getDateString(offsetDays = 0) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() + offsetDays);
  return d.toISOString().slice(0, 10);
}

async function monitorTick() {
  if (loopRunning) {
    logger.warn("Skip monitor tick because previous loop still running");
    return;
  }

  loopRunning = true;
  try {
    await runOnce({ config, state, discordClient });
    saveState(state);
  } catch (error) {
    logger.error("Monitor tick failed", { error: String(error) });
  } finally {
    loopRunning = false;
  }
}

app.get("/health", (req, res) => {
  res.status(200).json({
    ok: true,
    service: "altcoin-monitor",
    running: !loopRunning,
    timestamp: new Date().toISOString()
  });
});

trendPageApp.get("/health", (req, res) => {
  res.status(200).json({
    ok: true,
    service: "trend-page",
    apiBase: `http://${config.host}:${config.port}`,
    timestamp: new Date().toISOString()
  });
});

app.get("/monitor-ws", (req, res) => {
  res.sendFile(path.join(process.cwd(), "src/public/monitor-ws.html"));
});

app.get("/monitor-ws-pushes", (req, res) => {
  res.sendFile(path.join(process.cwd(), "src/public/monitor-ws-pushes.html"));
});

app.get("/monitor-ws-pushes-aster", (req, res) => {
  res.sendFile(path.join(process.cwd(), "src/public/monitor-ws-pushes-aster.html"));
});

app.get("/monitor-ws-slow-accum", (req, res) => {
  res.sendFile(path.join(process.cwd(), "src/public/monitor-ws-slow-accum.html"));
});

app.get("/monitor-ws-slow-accum-aster", (req, res) => {
  res.sendFile(path.join(process.cwd(), "src/public/monitor-ws-slow-accum-aster.html"));
});

app.get("/monitor-traffic", (req, res) => {
  res.sendFile(path.join(process.cwd(), "src/public/monitor-traffic.html"));
});

app.get("/trae", (req, res) => {
  res.sendFile(path.join(process.cwd(), "src/public/trae-status.html"));
});

app.get("/api/monitor-ws/runtime", async (req, res) => {
  const lines = Math.min(Math.max(Number(req.query.lines || 140), 20), 500);
  try {
    const data = await getMonitorWsRuntime(lines);
    res.status(200).json({ ok: true, data });
  } catch (error) {
    res.status(200).json({
      ok: false,
      message: `读取 monitor_ws 运行状态失败: ${String(error.message || error)}`
    });
  }
});

app.get("/api/trae/runtime", async (req, res) => {
  const lines = Math.min(Math.max(Number(req.query.lines || 160), 20), 500);
  const limit = Math.floor(Math.min(Math.max(Number(req.query.limit || 80), 10), 300));
  try {
    const data = await getTraeRuntime(lines, limit);
    res.status(200).json({ ok: true, data });
  } catch (error) {
    res.status(200).json({
      ok: false,
      message: `读取 TRAE 运行状态失败: ${String(error.message || error)}`
    });
  }
});

app.post("/api/trae/start", async (req, res) => {
  try {
    const started = await startTraeProcess();
    if (!started.ok) {
      res.status(200).json(started);
      return;
    }
    await new Promise((r) => setTimeout(r, 600));
    const runtime = await getTraeRuntime(120, 40);
    res.status(200).json({
      ok: true,
      message: "TRAE 启动命令已下发",
      data: {
        ...started,
        runtime
      }
    });
  } catch (error) {
    res.status(200).json({
      ok: false,
      message: `启动 TRAE 失败: ${String(error.message || error)}`
    });
  }
});

app.get("/api/monitor-ws/pushes", async (req, res) => {
  const source = normalizeMonitorSource(req.query.source);
  const windowHours = toNumberInRange(req.query.window_hours, 168, 1, 24 * 30);
  const sortBy = normalizePushSortBy(req.query.sort_by);
  const order = normalizePushSortOrder(req.query.order);
  const limit = Math.floor(toNumberInRange(req.query.limit, 500, 1, 5000));
  try {
    const { dbPath, rows } = await queryMonitorWsPushRows({ windowHours, source });
    const sorted = sortMonitorWsPushRows(rows, sortBy, order).slice(0, limit);
    const totalPushes = rows.reduce((acc, r) => acc + Number(r.push_count || 0), 0);
    res.status(200).json({
      ok: true,
      data: {
        source,
        rows: sorted,
        sort_by: sortBy,
        order,
        window_hours: windowHours,
        limit,
        total_symbols: rows.length,
        total_pushes: totalPushes,
        db_file: dbPath,
        generated_at: new Date().toISOString()
      }
    });
  } catch (error) {
    res.status(200).json({
      ok: false,
      message: `读取推送聚合失败: ${String(error.message || error)}`
    });
  }
});

app.get("/api/monitor-ws/slow-accum-breakdown", async (req, res) => {
  const source = normalizeMonitorSource(req.query.source);
  const windowHours = toNumberInRange(req.query.window_hours, 24, 1, 24 * 7);
  const limit = Math.floor(toNumberInRange(req.query.limit, 400, 1, 3000));
  try {
    const { dbPath, rows, meta } = await querySlowAccumBreakdownRows({ source, windowHours, limit });
    const stageStats = { READY: 0, HITS: 0, TREND: 0, COOLDOWN: 0 };
    for (const r of rows) {
      const k = String(r.stage || "").toUpperCase();
      if (Object.prototype.hasOwnProperty.call(stageStats, k)) stageStats[k] += 1;
    }
    res.status(200).json({
      ok: true,
      data: {
        source,
        rows,
        db_file: dbPath,
        total_symbols: rows.length,
        stage_stats: stageStats,
        generated_at: new Date().toISOString(),
        ...meta
      }
    });
  } catch (error) {
    res.status(200).json({
      ok: false,
      message: `读取 slow_accum 分解失败: ${String(error.message || error)}`
    });
  }
});

app.get("/api/monitor-traffic", async (req, res) => {
  try {
    const refresh = String(req.query.refresh || "").trim().toLowerCase();
    const shouldRefresh = refresh === "1" || refresh === "true" || refresh === "yes";
    const data = await getMonitorTrafficAccumulated({ refresh: shouldRefresh });
    res.status(200).json({ ok: true, data });
  } catch (error) {
    res.status(200).json({
      ok: false,
      message: `读取监控流量失败: ${String(error.message || error)}`
    });
  }
});

app.post("/api/monitor-ws/start", async (req, res) => {
  try {
    const started = await startMonitorWsProcess();
    if (!started.ok) {
      res.status(200).json(started);
      return;
    }
    await new Promise((r) => setTimeout(r, 400));
    const runtime = await getMonitorWsRuntime(80);
    res.status(200).json({
      ok: true,
      message: "monitor_ws 启动命令已下发",
      data: {
        ...started,
        runtime
      }
    });
  } catch (error) {
    res.status(200).json({
      ok: false,
      message: `启动 monitor_ws 失败: ${String(error.message || error)}`
    });
  }
});

app.get("/api/monitor-ws/settings", (req, res) => {
  try {
    const control = loadMonitorWsControl();
    res.status(200).json({
      ok: true,
      data: {
        discord_min_score: control.discord_min_score,
        modules: control.modules,
        module_meta: MONITOR_MODULE_FIELDS,
        updated_at: control.updated_at,
        control_file: MONITOR_CONTROL_PATH
      }
    });
  } catch (error) {
    res.status(200).json({
      ok: false,
      message: `读取 monitor_ws 配置失败: ${String(error.message || error)}`
    });
  }
});

app.post("/api/monitor-ws/settings", (req, res) => {
  try {
    const current = loadMonitorWsControl();
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const next = {
      ...current,
      discord_min_score: current.discord_min_score,
      modules: { ...current.modules }
    };

    if (Object.prototype.hasOwnProperty.call(body, "discord_min_score")) {
      const score = Number(body.discord_min_score);
      if (!Number.isFinite(score)) {
        res.status(200).json({ ok: false, message: "discord_min_score 必须是数字" });
        return;
      }
      next.discord_min_score = Math.max(0, Math.min(200, score));
    }

    const modulesPatch = body.modules && typeof body.modules === "object" ? body.modules : {};
    for (const item of MONITOR_MODULE_FIELDS) {
      if (Object.prototype.hasOwnProperty.call(modulesPatch, item.key)) {
        next.modules[item.key] = Boolean(modulesPatch[item.key]);
      }
      if (Object.prototype.hasOwnProperty.call(body, item.key)) {
        next.modules[item.key] = Boolean(body[item.key]);
      }
    }

    const saved = saveMonitorWsControl(next);
    res.status(200).json({
      ok: true,
      message: "配置已保存。重启 monitor_ws 后生效。",
      data: {
        discord_min_score: saved.discord_min_score,
        modules: saved.modules,
        module_meta: MONITOR_MODULE_FIELDS,
        updated_at: saved.updated_at,
        control_file: MONITOR_CONTROL_PATH
      }
    });
  } catch (error) {
    res.status(200).json({
      ok: false,
      message: `保存 monitor_ws 配置失败: ${String(error.message || error)}`
    });
  }
});

app.get("/api/collector-status", (req, res) => {
  try {
    const status = getCollectorStatus();
    res.status(200).json({ ok: true, data: status });
  } catch (error) {
    res.status(200).json({ ok: false, message: `collector status failed: ${String(error.message || error)}` });
  }
});

app.get("/api/trend-status", (req, res) => {
  try {
    res.status(200).json({
      ok: true,
      data: {
        enabled: Boolean(config.trendPredictorEnabled),
        symbols: config.trendSymbols || [],
        bootstrapTargetMinutes: TREND_BOOTSTRAP_TARGET_MINUTES,
        predictIntervalMinutes: Number(config.trendPredictIntervalMinutes || 1),
        horizonMinutes: Number(config.trendHorizonMinutes || 10),
        macroRule: String(config.trendMacroRule || "1min"),
        state: state.trend || {}
      }
    });
  } catch (error) {
    res.status(200).json({ ok: false, message: `trend status failed: ${String(error.message || error)}` });
  }
});

app.post("/api/collector/start", async (req, res) => {
  try {
    const mode = String((req.body && req.body.mode) || "").toLowerCase();
    const symbolInput = String((req.body && req.body.symbol) || "").trim().toUpperCase();
    const hours = Math.max(0.005, Number((req.body && req.body.hours) || COLLECT_BATCH_HOURS));
    let symbols = [];
    let type = mode;

    if (mode === "top300") symbols = await listTopVolumeSymbols(300);
    else if (mode === "top200") symbols = await listTopVolumeSymbols(200);
    else if (mode === "top100") symbols = await listTopVolumeSymbols(100);
    else if (mode === "top10") symbols = await listTopVolumeSymbols(10);
    else if (mode === "symbol") {
      if (!symbolInput) {
        res.status(200).json({ ok: false, message: "请先输入指定币种" });
        return;
      }
      const valid = await validateFuturesSymbol(symbolInput);
      if (!valid.ok) {
        res.status(200).json({ ok: false, message: valid.message });
        return;
      }
      symbols = [symbolInput];
      type = `symbol:${symbolInput}`;
    } else {
      res.status(200).json({ ok: false, message: "无效采集模式" });
      return;
    }

    const job = newCollectorJob(type, symbols);
    if (collectorRuntime.activeJobId) {
      collectorRuntime.stopRequested = true;
    }
    setImmediate(async () => {
      try {
        await runCollectorContinuous(job, hours);
      } catch (e) {
        updateCollectorJob(job, {
          status: "error",
          message: `采集任务失败: ${String(e.message || e)}`,
          finishedAt: Date.now()
        });
      }
    });

    res.status(200).json({
      ok: true,
      jobId: job.id,
      total: job.total,
      type: job.type
    });
  } catch (e) {
    res.status(200).json({ ok: false, message: `启动采集失败: ${String(e.message || e)}` });
  }
});

app.post("/api/collector/stop", (req, res) => {
  collectorRuntime.stopRequested = true;
  res.status(200).json({ ok: true, message: "已发送停止信号" });
});

app.get("/api/collector/job/:jobId", (req, res) => {
  const jobId = String(req.params.jobId || "");
  const job = collectorJobs.get(jobId);
  if (!job) {
    res.status(200).json({ ok: false, message: "采集任务不存在或已过期" });
    return;
  }

  if (collectorJobs.size > 30) {
    const keys = [...collectorJobs.keys()];
    for (const k of keys.slice(0, collectorJobs.size - 30)) collectorJobs.delete(k);
  }
  res.status(200).json({ ok: true, job });
});

function dashboardHandler(req, res) {
  if (!state.lastSnapshot) {
    res.status(200).json({ ok: true, data: null, message: "No snapshot yet" });
    return;
  }

  res.status(200).json({
    ok: true,
    data: {
      timestamp: state.lastSnapshot.timestamp,
      global: state.lastSnapshot.global,
      topMovers: [...(state.lastSnapshot.coins || [])]
        .sort((a, b) => Math.abs(b.priceChange24hPct || 0) - Math.abs(a.priceChange24hPct || 0))
        .slice(0, 10),
      fearGreed: state.lastSnapshot.sentiment,
      risk: state.lastSnapshot.risk,
      recentSignalCount: (state.lastSignals || []).length
    }
  });
}

app.get("/dashboard", dashboardHandler);
app.get("/api/dashboard", dashboardHandler);

function signalsHandler(req, res) {
  const limit = Math.min(Number(req.query.limit || 30), 200);
  res.status(200).json({ ok: true, data: (state.lastSignals || []).slice(0, limit) });
}

app.get("/signals", signalsHandler);
app.get("/api/signals", signalsHandler);

function dailyReportHandler(req, res) {
  const day = String(req.query.day || getDateString(0));
  const snapshots = loadSnapshots(day);
  res.status(200).json({ ok: true, data: buildDailyReport(day, snapshots) });
}

app.get("/report/daily", dailyReportHandler);
app.get("/api/report/daily", dailyReportHandler);

function weeklyReportHandler(req, res) {
  const days = [];
  for (let i = 0; i < 7; i += 1) {
    const day = getDateString(-i);
    days.push([day, loadSnapshots(day)]);
  }
  const payload = Object.fromEntries(days.filter(([, snapshots]) => snapshots.length));
  res.status(200).json({ ok: true, data: buildWeeklyReport(payload) });
}

app.get("/report/weekly", weeklyReportHandler);
app.get("/api/report/weekly", weeklyReportHandler);

function requestText(url, timeoutMs = 8000) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      url,
      {
        method: "GET",
        timeout: timeoutMs,
        headers: {
          "user-agent": "Mozilla/5.0 (Netflix Connectivity Checker)"
        }
      },
      (res) => {
        let body = "";
        res.setEncoding("utf8");
        res.on("data", (chunk) => {
          if (body.length < 8192) body += chunk;
        });
        res.on("end", () => {
          resolve({
            statusCode: res.statusCode || 0,
            headers: res.headers || {},
            bodySnippet: body.slice(0, 4096),
            bodyLength: body.length
          });
        });
      }
    );

    req.on("timeout", () => {
      req.destroy(new Error("timeout"));
    });
    req.on("error", reject);
    req.end();
  });
}

async function fetchJson(url, timeoutMs = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "GET",
      headers: { "user-agent": "Mozilla/5.0" },
      signal: controller.signal
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    return await res.json();
  } finally {
    clearTimeout(timer);
  }
}

async function validateFuturesSymbol(symbol) {
  const s = String(symbol || "").toUpperCase();
  if (!/^[A-Z0-9]{5,20}$/.test(s)) {
    return { ok: false, message: "币种格式不正确，请输入如 BTCUSDT、ETHUSDT" };
  }
  try {
    const info = await fetchJson(`https://fapi.binance.com/fapi/v1/exchangeInfo?symbol=${encodeURIComponent(s)}`, 8000);
    const arr = Array.isArray(info.symbols) ? info.symbols : [];
    const hit = arr.find((x) => String(x.symbol || "").toUpperCase() === s);
    if (!hit) {
      return { ok: false, message: `Binance Futures 不存在该交易对：${s}` };
    }
    return { ok: true, message: "" };
  } catch (e) {
    return { ok: false, message: `币种校验失败：${String(e.message || e)}` };
  }
}

function execFileAsync(cmd, args, timeoutMs = 300000) {
  return new Promise((resolve, reject) => {
    execFile(
      cmd,
      args,
      {
        cwd: process.cwd(),
        timeout: timeoutMs,
        maxBuffer: 10 * 1024 * 1024
      },
      (error, stdout, stderr) => {
        if (error) {
          reject(new Error(`${error.message}\n${stderr || ""}`.trim()));
          return;
        }
        resolve({ stdout: String(stdout || ""), stderr: String(stderr || "") });
      }
    );
  });
}

function summarizeTrainError(raw, symbol, scene = "micro") {
  const text = String(raw || "");
  const triedMatch = text.match(/已尝试参数:\s*([^\n]+)/);
  const tried = triedMatch ? ` 已尝试参数: ${triedMatch[1]}` : "";
  if (scene === "macro" || text.includes("长周期")) {
    if (text.includes("长周期有效样本过少")) {
      return [
        `训练失败：${symbol} 长周期有效样本不足。`,
        "原因：分钟级样本覆盖不足，或未来窗口过长导致可用标签过少。",
        `建议：继续采集更长时段数据，或降低 macro_horizon_minutes（如 30→15→5）。${tried}`
      ].join(" ");
    }
    if (text.includes("长周期标签类别不足")) {
      return [
        `训练失败：${symbol} 长周期标签类别不足。`,
        "原因：样本方向几乎单边，分类器无法学习有效边界。",
        "建议：增加数据跨度，或缩短预测窗口并观察标签分布。"
      ].join(" ");
    }
  }
  if (text.includes("有效样本过少")) {
    return [
      `训练失败：${symbol} 三重屏障有效样本不足。`,
      "原因：5分钟内未触发TP/SL或触发样本过少，过滤后无可训练样本。",
      `建议：延长采集时长、放宽TP/SL、或先保留中性样本（drop-neutral=0）。${tried}`
    ].join(" ");
  }
  if (text.includes("训练标签类别不足")) {
    return [
      `训练失败：${symbol} 标签类别不足。`,
      "原因：样本几乎都落在同一类（全0或全1）。",
      `建议：增加数据跨度并放宽三重屏障参数。${tried}`
    ].join(" ");
  }
  if (text.includes("FileNotFoundError")) {
    return `未找到 ${symbol} 的LOB数据文件，请先采集数据。`;
  }
  // 避免把超长stderr直接返回到网页
  return text.slice(0, 600);
}

function extractTrainMeta(stdoutText) {
  const text = String(stdoutText || "");
  const m = text.match(/TRAIN_META_JSON=(\{.*\})/);
  if (!m) return null;
  try {
    return JSON.parse(m[1]);
  } catch (_) {
    return null;
  }
}

function ensurePredictWorker(symbol) {
  const key = symbol.toUpperCase();
  const existing = predictWorkers.get(key);
  if (existing && existing.proc && !existing.proc.killed && existing.proc.exitCode === null) {
    return existing;
  }

  const proc = spawn(
    "python3",
    ["predict_once.py", "--symbol", key, "--data-dir", "data", "--daemon"],
    {
      cwd: process.cwd(),
      stdio: ["pipe", "pipe", "pipe"]
    }
  );

  const worker = {
    symbol: key,
    proc,
    rl: readline.createInterface({ input: proc.stdout }),
    seq: 0,
    pending: new Map()
  };

  worker.rl.on("line", (line) => {
    let msg = null;
    try {
      msg = JSON.parse(String(line || "").trim());
    } catch {
      return;
    }
    const reqId = String(msg.request_id || "");
    if (!reqId) return;
    const p = worker.pending.get(reqId);
    if (!p) return;
    clearTimeout(p.timer);
    worker.pending.delete(reqId);
    if (msg.ok) {
      p.resolve(msg.data);
    } else {
      p.reject(new Error(String(msg.error || "worker response error")));
    }
  });

  proc.stderr.on("data", (chunk) => {
    logger.warn("Predict worker stderr", { symbol: key, text: String(chunk || "").trim() });
  });

  proc.on("exit", (code, signal) => {
    const err = new Error(`predict worker exited: code=${code} signal=${signal}`);
    for (const [, p] of worker.pending) {
      clearTimeout(p.timer);
      p.reject(err);
    }
    worker.pending.clear();
    predictWorkers.delete(key);
    logger.warn("Predict worker exited", { symbol: key, code, signal });
  });

  proc.on("error", (error) => {
    logger.error("Predict worker process error", { symbol: key, error: String(error) });
  });

  predictWorkers.set(key, worker);
  logger.info("Predict worker started", { symbol: key, pid: proc.pid });
  return worker;
}

function requestPredictFromWorker(symbol, mode, timeoutMs = 120000) {
  const worker = ensurePredictWorker(symbol);
  const reqId = `${Date.now()}_${++worker.seq}`;
  const payload = {
    cmd: "predict",
    request_id: reqId,
    mode: mode || "lob"
  };

  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      worker.pending.delete(reqId);
      reject(new Error(`predict worker timeout: ${symbol}`));
    }, timeoutMs);

    worker.pending.set(reqId, { resolve, reject, timer });

    try {
      worker.proc.stdin.write(`${JSON.stringify(payload)}\n`);
    } catch (error) {
      clearTimeout(timer);
      worker.pending.delete(reqId);
      reject(error);
    }
  });
}

async function requestLongDirection(symbol, macroRule = "1min") {
  const out = await execFileAsync(
    "python3",
    ["predict_long_direction.py", "--symbol", symbol.toUpperCase(), "--data-dir", "data", "--macro-rule", macroRule],
    120000
  );
  const txt = String(out.stdout || "").trim();
  const line = txt.split("\n").filter(Boolean).pop() || "{}";
  return JSON.parse(line);
}

function hasLobData(symbol) {
  const dataDir = path.join(process.cwd(), "data");
  if (!fs.existsSync(dataDir)) return false;
  const files = fs.readdirSync(dataDir);
  return files.some((f) => f.startsWith(`${symbol}_`) && f.endsWith(".parquet") && !f.includes("_klines_"));
}

function hasModelFile(symbol) {
  const fp = path.join(process.cwd(), `${String(symbol || "").toUpperCase()}_xgboost_model.json`);
  try {
    const st = fs.statSync(fp);
    return st.isFile() && st.size > 0;
  } catch (_) {
    return false;
  }
}

function hasMacroModelFile(symbol) {
  const s = String(symbol || "").toUpperCase();
  const modelPath = path.join(process.cwd(), `${s}_macro_xgboost_model.json`);
  const featPath = path.join(process.cwd(), `${s}_macro_features.json`);
  try {
    const st1 = fs.statSync(modelPath);
    const st2 = fs.statSync(featPath);
    return st1.isFile() && st1.size > 0 && st2.isFile() && st2.size > 0;
  } catch (_) {
    return false;
  }
}

async function sendTrendDiscordNotification(content) {
  const text = String(content || "").slice(0, 1800);
  const tasks = [];

  if (config.discordWebhookUrl) {
    tasks.push(
      fetch(config.discordWebhookUrl, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ content: text })
      })
    );
  }

  if (discordClient && config.discordAlertChannelId && discordClient.isReady()) {
    tasks.push(
      discordClient.channels.fetch(config.discordAlertChannelId).then((ch) => {
        if (ch && typeof ch.send === "function") {
          return ch.send(text);
        }
        return null;
      })
    );
  }

  if (!tasks.length) {
    logger.warn("Trend notify skipped: no discord webhook/channel configured");
    return;
  }

  const rs = await Promise.allSettled(tasks);
  for (const r of rs) {
    if (r.status === "rejected") {
      logger.error("Trend discord notify failed", { error: String(r.reason || "") });
    }
  }
}

async function getLobCoverageMinutes(symbol) {
  try {
    const py = [
      "import json",
      "from pathlib import Path",
      "import pandas as pd",
      "import pyarrow.parquet as pq",
      `symbol='${String(symbol || "").toUpperCase()}'`,
      "data_dir=Path('data')",
      "files=sorted([p for p in data_dir.glob(f'{symbol}_*.parquet') if '_klines_' not in p.name])",
      "if not files:",
      "    print(json.dumps({'ok': True, 'minutes': 0.0, 'bad': 0}))",
      "    raise SystemExit(0)",
      "mins=[]",
      "maxs=[]",
      "bad=0",
      "for fp in files:",
      "    try:",
      "        t=pq.read_table(fp, columns=['timestamp']).to_pandas()",
      "    except Exception:",
      "        bad += 1",
      "        continue",
      "    if len(t)==0: continue",
      "    ts=pd.to_datetime(t['timestamp'], utc=True)",
      "    mins.append(ts.min())",
      "    maxs.append(ts.max())",
      "if not mins:",
      "    print(json.dumps({'ok': True, 'minutes': 0.0, 'bad': bad}))",
      "    raise SystemExit(0)",
      "span=(max(maxs)-min(mins)).total_seconds()/60.0",
      "print(json.dumps({'ok': True, 'minutes': float(max(0.0, span)), 'bad': bad}))"
    ].join("\n");
    const out = await execFileAsync("python3", ["-c", py], 60000);
    const obj = JSON.parse(String(out.stdout || "{}").trim() || "{}");
    return Number(obj.minutes || 0);
  } catch (e) {
    logger.warn("LOB coverage check failed", { symbol, error: String(e.message || e) });
    return 0;
  }
}

async function ensureEnoughLobCoverage(symbol, minMinutes = 1.0, collectHours = AUTO_COLLECT_HOURS, maxAttempts = 6) {
  let minutes = await getLobCoverageMinutes(symbol);
  let attempts = 0;
  while (minutes < minMinutes && attempts < maxAttempts) {
    attempts += 1;
    // 快速分片补采，避免前端长时间卡在 coverage_check
    await execFileAsync("python3", ["collect_lob.py", symbol, "--hours", collectHours], 12 * 60 * 1000);
    minutes = await getLobCoverageMinutes(symbol);
  }
  return { minutes, attempts };
}

async function collectTrendCoverageRound(symbols, hoursPerSymbol) {
  const hs = Math.max(0.01, Number(hoursPerSymbol || 0.03));
  for (const symbol of symbols) {
    try {
      logger.info("Trend bootstrap collect round", { symbol, hours: hs });
      await execFileAsync("python3", ["collect_lob.py", symbol, "--hours", String(hs)], 15 * 60 * 1000);
    } catch (error) {
      logger.warn("Trend bootstrap collect failed", { symbol, error: String(error.message || error) });
    }
  }
}

async function tryRecommendedBackfill(symbols) {
  const syms = (Array.isArray(symbols) ? symbols : []).map((s) => String(s || "").toUpperCase()).filter(Boolean);
  if (!syms.length) return { ok: false, reason: "empty_symbols" };
  try {
    const out = await execFileAsync(
      "python3",
      [
        "scripts/backfill_recommended_combo.py",
        "--symbols",
        syms.join(","),
        "--days",
        String(Math.max(1, Number(config.trendBootstrapDays || 7))),
        "--data-dir",
        "data"
      ],
      12 * 60 * 1000
    );
    const txt = String(out.stdout || "").trim();
    const line = txt.split("\n").filter(Boolean).pop() || "{}";
    const obj = JSON.parse(line);
    return { ok: true, data: obj };
  } catch (error) {
    return { ok: false, reason: String(error.message || error) };
  }
}

async function ensureTrendBootstrapCoverage(symbols) {
  const coverageMinutes = {};
  const missing = [];
  for (const symbol of symbols) {
    const mins = await getLobCoverageMinutes(symbol);
    coverageMinutes[symbol] = mins;
    if (mins < TREND_BOOTSTRAP_TARGET_MINUTES) {
      missing.push(symbol);
    }
  }

  state.trend.coverageMinutes = coverageMinutes;
  state.trend.lastRunAt = new Date().toISOString();
  saveState(state);

  if (!missing.length) return true;

  const bf = state.trend.backfill || {};
  const now = Date.now();
  const lastAttemptMs = bf.lastAttemptAt ? Date.parse(String(bf.lastAttemptAt)) : 0;
  const lastResultJson = JSON.stringify((bf && bf.lastResult) || {});
  const shouldRetryImmediately =
    lastResultJson.includes("cryptohftdata_not_available") ||
    lastResultJson.includes("symbols_success\":0");
  const allowBackfill = shouldRetryImmediately || !lastAttemptMs || (now - lastAttemptMs >= 6 * 60 * 60 * 1000);

  if (allowBackfill) {
    const bfRes = await tryRecommendedBackfill(missing);
    state.trend.backfill = {
      lastAttemptAt: new Date().toISOString(),
      lastResult: bfRes
    };
    saveState(state);

    // 回填后立即复查覆盖
    const postCoverage = {};
    const stillMissing = [];
    for (const symbol of symbols) {
      const mins = await getLobCoverageMinutes(symbol);
      postCoverage[symbol] = mins;
      if (mins < TREND_BOOTSTRAP_TARGET_MINUTES) stillMissing.push(symbol);
    }
    state.trend.coverageMinutes = postCoverage;
    state.trend.lastRunAt = new Date().toISOString();
    saveState(state);
    if (!stillMissing.length) return true;
    missing.splice(0, missing.length, ...stillMissing);
  }

  logger.info("Trend bootstrap coverage not enough, collect more", {
    targetMinutes: TREND_BOOTSTRAP_TARGET_MINUTES,
    missing: missing.map((s) => ({ symbol: s, mins: Number(coverageMinutes[s] || 0).toFixed(2) }))
  });
  await collectTrendCoverageRound(missing, config.trendCollectHoursPerRound);
  return false;
}

async function ensureTrendMacroModel(symbol) {
  const s = String(symbol || "").toUpperCase();
  const desiredHorizon = Math.max(1, Number(config.trendHorizonMinutes || 10));
  const desiredMinSamples = Math.max(10, Number(config.trendMinSamples || 20));
  const oldMeta = (state.trend.modelMeta && state.trend.modelMeta[s]) || {};

  const horizonMatched = Number(oldMeta.horizonMinutes || 0) === desiredHorizon;
  if (hasMacroModelFile(s) && horizonMatched) return;

  logger.info("Trend train macro model", {
    symbol: s,
    horizonMinutes: desiredHorizon,
    minSamples: desiredMinSamples
  });
  const trainOut = await execFileAsync(
    "python3",
    [
      "train.py",
      "--symbol",
      s,
      "--mode",
      "macro",
      "--macro-horizon-minutes",
      String(desiredHorizon),
      "--min-samples",
      String(desiredMinSamples)
    ],
    10 * 60 * 1000
  );
  const trainMeta = extractTrainMeta(trainOut.stdout);
  state.trend.modelMeta[s] = {
    horizonMinutes: desiredHorizon,
    trainedAt: new Date().toISOString(),
    trainMeta: trainMeta || null
  };
}

async function runTrendPredictCycle() {
  if (!config.trendPredictorEnabled) return;
  if (trendCycleRunning) {
    logger.warn("Skip trend cycle because previous cycle still running");
    return;
  }

  trendCycleRunning = true;
  state.trend = state.trend || {};
  state.trend.backfill = state.trend.backfill || { lastAttemptAt: null, lastResult: null };
  const symbols = (Array.isArray(config.trendSymbols) ? config.trendSymbols : [])
    .map((s) => String(s || "").trim().toUpperCase())
    .filter(Boolean);

  try {
    if (!symbols.length) {
      logger.warn("Trend cycle skipped: TREND_SYMBOLS is empty");
      return;
    }

    if (!state.trend.bootstrapped) {
      const ready = await ensureTrendBootstrapCoverage(symbols);
      if (!ready) {
        state.trend.lastError = `Bootstrap中：目标覆盖 ${TREND_BOOTSTRAP_TARGET_MINUTES} 分钟`;
        saveState(state);
        return;
      }
      state.trend.bootstrapped = true;
      state.trend.bootstrapAt = new Date().toISOString();
      state.trend.lastError = "";
      saveState(state);
      logger.info("Trend bootstrap completed", { symbols, targetMinutes: TREND_BOOTSTRAP_TARGET_MINUTES });
      await sendTrendDiscordNotification(
        `【趋势系统】7天数据覆盖达标，开始每分钟预测。\n币种: ${symbols.join(", ")}\n预测窗口: ${Number(config.trendHorizonMinutes || 10)}分钟`
      );
    }

    const macroRule = String(config.trendMacroRule || "1min");
    for (const symbol of symbols) {
      try {
        await withSymbolLock(symbol, async () => {
          await ensureTrendMacroModel(symbol);
          const pred = await requestLongDirection(symbol, macroRule);
          if (!pred || !pred.ok) {
            throw new Error(`predict_long_direction failed: ${JSON.stringify(pred || {})}`);
          }

          const nowIso = new Date().toISOString();
          const prev = String((state.trend.lastDirections || {})[symbol] || "");
          const next = String(pred.direction || "横盘");
          const conf = Number(pred.confidence || 0);

          if (prev && prev !== next) {
            const msg = [
              `【趋势切换】${symbol}`,
              `方向变化: ${prev} -> ${next}`,
              `信心: ${(conf * 100).toFixed(0)}%`,
              `预测窗口: ${Number(config.trendHorizonMinutes || 10)}分钟`,
              `时间: ${nowIso}`
            ].join("\n");
            await sendTrendDiscordNotification(msg);
          }

          state.trend.lastDirections[symbol] = next;
          state.trend.lastConfidence[symbol] = conf;
          state.trend.lastRunAt = nowIso;
          state.trend.lastError = "";
          saveState(state);
        });
      } catch (error) {
        const errText = `Trend symbol cycle failed: ${symbol}, ${String(error.message || error)}`;
        state.trend.lastError = errText.slice(0, 400);
        saveState(state);
        logger.error("Trend symbol cycle failed", { symbol, error: String(error.message || error) });
      }
    }
  } catch (error) {
    state.trend.lastError = String(error.message || error).slice(0, 400);
    saveState(state);
    logger.error("Trend cycle failed", { error: String(error.message || error) });
  } finally {
    trendCycleRunning = false;
  }
}

function withSymbolLock(symbol, fn) {
  const key = symbol.toUpperCase();
  const prev = trainLocks.get(key) || Promise.resolve();
  // 防止前一个任务失败后把后续任务永久卡在队列中
  const next = prev
    .catch(() => undefined)
    .then(() => fn())
    .finally(() => {
    if (trainLocks.get(key) === next) {
      trainLocks.delete(key);
    }
  });
  trainLocks.set(key, next);
  return next;
}

app.get("/api/predict-auto", async (req, res) => {
  const symbol = String(req.query.symbol || "BTCUSDT").toUpperCase();
  const startedAt = Date.now();

  try {
    const payload = await withSymbolLock(symbol, async () => {
      const steps = [];

      if (hasModelFile(symbol) && hasLobData(symbol)) {
        steps.push("predict_cached");
        const data = await requestPredictFromWorker(symbol, "lob", 120000);
        return {
          ok: true,
          data,
          steps,
          trainMeta: null,
          elapsedMs: Date.now() - startedAt
        };
      }

      if (!hasLobData(symbol)) {
        steps.push("collect");
        // 自动先做一段快速补采，尽快进入训练
        await execFileAsync("python3", ["collect_lob.py", symbol, "--hours", AUTO_COLLECT_HOURS], 12 * 60 * 1000);
      }

      const cov = await ensureEnoughLobCoverage(symbol, 1.0);
      steps.push(`coverage_${cov.minutes.toFixed(2)}m`);
      if (cov.minutes < 1.0) {
        throw new Error(`训练前数据覆盖不足：${cov.minutes.toFixed(2)}分钟（需>=1分钟）`);
      }

      // 用户需求：输入币种后自动训练
      steps.push("train");
      const trainOut = await execFileAsync("python3", ["train.py", "--symbol", symbol, ...AUTO_TRAIN_ARGS], 600000);
      const trainMeta = extractTrainMeta(trainOut.stdout);

      // 训练完成后做一次单次推理，返回结果给网页
      steps.push("predict_once");
      const data = await requestPredictFromWorker(symbol, "lob", 120000);

      return {
        ok: true,
        data,
        steps,
        trainMeta,
        elapsedMs: Date.now() - startedAt
      };
    });

    res.status(200).json(payload);
  } catch (error) {
    res.status(200).json({
      ok: false,
      message: `自动训练预测失败: ${String(error.message || error)}`,
      elapsedMs: Date.now() - startedAt
    });
  }
});

function newJob(symbol, mode, action = "predict") {
  const id = `${symbol}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const job = {
    id,
    symbol,
    mode,
    action,
    status: "queued",
    progress: 5,
    step: "queued",
    message: "任务排队中",
    startedAt: Date.now(),
    finishedAt: null,
    result: null,
    error: null
  };
  predictJobs.set(id, job);
  return job;
}

function newLongJob(symbol, mode, action = "predict") {
  const id = `long_${symbol}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const job = {
    id,
    symbol,
    mode,
    action,
    status: "queued",
    progress: 5,
    step: "queued",
    message: "长周期任务排队中",
    startedAt: Date.now(),
    finishedAt: null,
    result: null,
    error: null
  };
  predictLongJobs.set(id, job);
  return job;
}

function updateJob(job, patch) {
  Object.assign(job, patch);
  predictJobs.set(job.id, job);
}

function updateLongJob(job, patch) {
  Object.assign(job, patch);
  predictLongJobs.set(job.id, job);
}

async function runAutoPipeline(job) {
  const { symbol, mode, action } = job;
  const payload = await withSymbolLock(symbol, async () => {
    const steps = [];

    // 查询方向优先直接使用已采集数据 + 已训练模型，避免每次都重训
    const canDirectPredict = action === "predict" && hasModelFile(symbol) && hasLobData(symbol);
    if (canDirectPredict) {
      updateJob(job, {
        status: "running",
        progress: 85,
        step: "predict_once",
        message: "检测到已采集数据与模型，直接生成预测"
      });
      steps.push("predict_cached");
      const data = await requestPredictFromWorker(symbol, mode, 120000);
      return {
        ok: true,
        data,
        steps,
        trainMeta: null
      };
    }

    if (!hasLobData(symbol)) {
      updateJob(job, {
        status: "running",
        progress: 20,
        step: "collect",
        message: "正在采集训练数据"
      });
      steps.push("collect");
      await execFileAsync("python3", ["collect_lob.py", symbol, "--hours", AUTO_COLLECT_HOURS], 12 * 60 * 1000);
    }

    updateJob(job, {
      status: "running",
      progress: 40,
      step: "coverage_check",
      message: "检查并补足训练数据覆盖时长"
    });
    const cov = await ensureEnoughLobCoverage(symbol, 1.0);
    steps.push(`coverage_${cov.minutes.toFixed(2)}m`);
    if (cov.minutes < 1.0) {
      throw new Error(`训练前数据覆盖不足：${cov.minutes.toFixed(2)}分钟（需>=1分钟）`);
    }

    updateJob(job, {
      status: "running",
      progress: 65,
      step: "train",
      message: "正在训练模型"
    });
    steps.push("train");
    const trainOut = await execFileAsync("python3", ["train.py", "--symbol", symbol, ...AUTO_TRAIN_ARGS], 600000);
    const trainMeta = extractTrainMeta(trainOut.stdout);

    let data = null;
    if (action !== "retrain") {
      updateJob(job, {
        status: "running",
        progress: 90,
        step: "predict_once",
        message: "正在生成预测结果"
      });
      steps.push("predict_once");
      data = await requestPredictFromWorker(symbol, mode, 120000);
    }

    return {
      ok: true,
      data,
      steps,
      trainMeta
    };
  });

  updateJob(job, {
    status: "done",
    progress: 100,
    step: "done",
    message: "训练与预测完成",
    result: payload,
    finishedAt: Date.now()
  });
}

async function runLongAutoPipeline(job) {
  const { symbol, mode, action } = job;
  const payload = await withSymbolLock(symbol, async () => {
    const steps = [];

    if (!hasLobData(symbol)) {
      throw new Error(`未检测到 ${symbol} 的本地LOB数据。请先使用“一键采集”持续采集后再进行长周期预测。`);
    }

    updateLongJob(job, {
      status: "running",
      progress: 40,
      step: "coverage_check",
      message: "检查一键采集数据覆盖时长"
    });
    // 长周期预测仅使用“一键采集”已有数据，不在此链路内自动补采
    const covMinutes = await getLobCoverageMinutes(symbol);
    const cov = { minutes: covMinutes, attempts: 0 };
    steps.push(`coverage_${cov.minutes.toFixed(2)}m`);
    if (cov.minutes < AUTO_LONG_MIN_COVERAGE_MINUTES) {
      throw new Error(`训练前数据覆盖不足：${cov.minutes.toFixed(2)}分钟（需>=${AUTO_LONG_MIN_COVERAGE_MINUTES}分钟）。请继续运行“一键采集”后重试。`);
    }

    updateLongJob(job, {
      status: "running",
      progress: 65,
      step: "train_macro",
      message: "正在训练长周期模型"
    });
    steps.push("train_macro");
    let trainMeta = null;
    let lastTrainErr = null;
    for (const h of AUTO_LONG_HORIZON_TRIALS) {
      try {
        const trainOut = await execFileAsync(
          "python3",
          ["train.py", "--symbol", symbol, "--mode", "macro", "--macro-horizon-minutes", String(h), "--min-samples", AUTO_LONG_MIN_SAMPLES],
          600000
        );
        trainMeta = extractTrainMeta(trainOut.stdout);
        if (trainMeta) trainMeta.used_horizon_minutes = h;
        steps.push(`h${h}m`);
        lastTrainErr = null;
        break;
      } catch (e) {
        lastTrainErr = e;
      }
    }
    if (lastTrainErr) throw lastTrainErr;

    let data = null;
    if (action !== "retrain") {
      updateLongJob(job, {
        status: "running",
        progress: 85,
        step: "predict_once",
        message: "生成TP/SL与基础风控"
      });
      const base = await requestPredictFromWorker(symbol, mode, 120000);

      updateLongJob(job, {
        status: "running",
        progress: 95,
        step: "predict_long",
        message: "生成长周期方向信号"
      });
      const longRes = await requestLongDirection(symbol, "1min");
      if (longRes && longRes.ok) {
        base.direction = longRes.direction;
        base.confidence = longRes.confidence;
        base.long_cycle = {
          source: "macro_model",
          p_up: longRes.p_up,
          p_down: longRes.p_down,
          macro_rule: longRes.macro_rule
        };
      }
      steps.push("predict_long");
      data = base;
    }

    return {
      ok: true,
      data,
      steps,
      trainMeta
    };
  });

  updateLongJob(job, {
    status: "done",
    progress: 100,
    step: "done",
    message: "长周期训练与预测完成",
    result: payload,
    finishedAt: Date.now()
  });
}

app.post("/api/predict-auto/start", async (req, res) => {
  const symbol = String((req.body && req.body.symbol) || "BTCUSDT").toUpperCase();
  const mode = String((req.body && req.body.mode) || "lob").toLowerCase();
  const action = String((req.body && req.body.action) || "predict").toLowerCase();
  const normalizedAction = ["predict", "retrain"].includes(action) ? action : "predict";
  const normalizedMode = ["lob", "atr", "fixed", "liquidity"].includes(mode) ? mode : "lob";
  const valid = await validateFuturesSymbol(symbol);
  if (!valid.ok) {
    res.status(200).json({
      ok: false,
      message: valid.message
    });
    return;
  }

  const job = newJob(symbol, normalizedMode, normalizedAction);
  setImmediate(async () => {
    try {
      await runAutoPipeline(job);
    } catch (error) {
      const raw = String(error.message || error);
      const shortMsg = summarizeTrainError(raw, symbol, "micro");
      updateJob(job, {
        status: "error",
        progress: 100,
        step: "error",
        message: "自动训练预测失败",
        error: shortMsg,
        finishedAt: Date.now()
      });
    }
  });

  res.status(200).json({
    ok: true,
    jobId: job.id,
    status: job.status,
    progress: job.progress,
    action: normalizedAction
  });
});

app.post("/api/predict-long/start", async (req, res) => {
  const symbol = String((req.body && req.body.symbol) || "BTCUSDT").toUpperCase();
  const mode = String((req.body && req.body.mode) || "lob").toLowerCase();
  const action = String((req.body && req.body.action) || "predict").toLowerCase();
  const normalizedAction = ["predict", "retrain"].includes(action) ? action : "predict";
  const normalizedMode = ["lob", "atr", "fixed", "liquidity"].includes(mode) ? mode : "lob";
  const valid = await validateFuturesSymbol(symbol);
  if (!valid.ok) {
    res.status(200).json({ ok: false, message: valid.message });
    return;
  }

  const job = newLongJob(symbol, normalizedMode, normalizedAction);
  setImmediate(async () => {
    try {
      await runLongAutoPipeline(job);
    } catch (error) {
      const raw = String(error.message || error);
      const shortMsg = summarizeTrainError(raw, symbol, "macro");
      updateLongJob(job, {
        status: "error",
        progress: 100,
        step: "error",
        message: "长周期自动训练预测失败",
        error: shortMsg,
        finishedAt: Date.now()
      });
    }
  });

  res.status(200).json({
    ok: true,
    jobId: job.id,
    status: job.status,
    progress: job.progress,
    action: normalizedAction
  });
});

app.get("/api/price", async (req, res) => {
  const symbol = String(req.query.symbol || "BTCUSDT").toUpperCase();
  const valid = await validateFuturesSymbol(symbol);
  if (!valid.ok) {
    res.status(200).json({ ok: false, message: valid.message });
    return;
  }
  try {
    const data = await fetchJson(`https://fapi.binance.com/fapi/v1/ticker/price?symbol=${encodeURIComponent(symbol)}`, 8000);
    res.status(200).json({
      ok: true,
      data: {
        symbol,
        price: Number(data.price || 0),
        timestamp: new Date().toISOString()
      }
    });
  } catch (error) {
    res.status(200).json({ ok: false, message: `价格获取失败: ${String(error.message || error)}` });
  }
});

app.get("/api/price-history", async (req, res) => {
  const symbol = String(req.query.symbol || "BTCUSDT").toUpperCase();
  const limit = Math.min(Math.max(Number(req.query.limit || 120), 20), 500);
  const valid = await validateFuturesSymbol(symbol);
  if (!valid.ok) {
    res.status(200).json({ ok: false, message: valid.message });
    return;
  }

  try {
    const rows = await fetchJson(
      `https://fapi.binance.com/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=1m&limit=${limit}`,
      10000
    );
    if (!Array.isArray(rows)) {
      res.status(200).json({ ok: false, message: "K线数据格式异常" });
      return;
    }

    const points = rows.map((r) => ({
      t: Number(r[0] || 0),
      o: Number(r[1] || 0),
      h: Number(r[2] || 0),
      l: Number(r[3] || 0),
      c: Number(r[4] || 0),
      v: Number(r[5] || 0)
    }));

    res.status(200).json({
      ok: true,
      data: {
        symbol,
        points,
        timestamp: new Date().toISOString()
      }
    });
  } catch (error) {
    res.status(200).json({ ok: false, message: `历史价格获取失败: ${String(error.message || error)}` });
  }
});

async function buildOiSnapshot() {
  const now = Date.now();
  const cacheMs = 15000;
  if (oiCache.snapshot.data && now - oiCache.snapshot.ts < cacheMs) {
    return oiCache.snapshot.data;
  }

  const watchSymbols = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT",
    "ADAUSDT", "LINKUSDT", "AVAXUSDT", "DOTUSDT", "TRXUSDT", "LTCUSDT"
  ];

  const tickerRows = await fetchJson("https://fapi.binance.com/fapi/v1/ticker/24hr", 12000);
  const tickerMap = new Map(
    (Array.isArray(tickerRows) ? tickerRows : [])
      .map((x) => [String(x.symbol || "").toUpperCase(), x])
  );

  const top = [];
  for (const symbol of watchSymbols) {
    const tk = tickerMap.get(symbol);
    if (!tk) continue;

    const [oiRaw, premiumRaw] = await Promise.all([
      fetchJson(`https://fapi.binance.com/fapi/v1/openInterest?symbol=${symbol}`, 10000).catch(() => ({})),
      fetchJson(`https://fapi.binance.com/fapi/v1/premiumIndex?symbol=${symbol}`, 10000).catch(() => ({}))
    ]);

    const price = Number(tk.lastPrice || premiumRaw.markPrice || 0);
    const oiQty = Number(oiRaw.openInterest || 0);
    const oiUsd = oiQty * price;

    top.push({
      symbol,
      price,
      oiUsd,
      change24hPct: Number(tk.priceChangePercent || 0),
      fundingRate: Number(premiumRaw.lastFundingRate || 0)
    });
  }

  top.sort((a, b) => b.oiUsd - a.oiUsd);
  const totalOiUsd = top.reduce((acc, r) => acc + Number(r.oiUsd || 0), 0);
  const weightedChangeBase = top.reduce((acc, r) => acc + Math.abs(Number(r.oiUsd || 0)), 0) || 1;
  const totalOiChange24hPct = top.reduce((acc, r) => acc + Number(r.change24hPct || 0) * Math.abs(Number(r.oiUsd || 0)), 0) / weightedChangeBase;
  const oi100 = totalOiUsd / 1e8;
  const oi200 = totalOiUsd / 2e8;

  let longShortRatio = 1;
  try {
    const ls = await fetchJson(
      "https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=1",
      10000
    );
    if (Array.isArray(ls) && ls[0] && Number.isFinite(Number(ls[0].longShortRatio))) {
      longShortRatio = Number(ls[0].longShortRatio);
    }
  } catch (_) {}

  const data = {
    totalOiUsd,
    totalOiChange24hPct,
    oi100,
    oi200,
    longShortRatio,
    top,
    timestamp: new Date().toISOString()
  };
  oiCache.snapshot = { ts: now, data };
  return data;
}

app.get("/api/oi-snapshot", async (req, res) => {
  try {
    const data = await buildOiSnapshot();
    res.status(200).json({ ok: true, data });
  } catch (error) {
    res.status(200).json({ ok: false, message: `OI数据获取失败: ${String(error.message || error)}` });
  }
});

app.get("/api/oi-history", async (req, res) => {
  const symbol = String(req.query.symbol || "BTCUSDT").toUpperCase();
  const period = String(req.query.period || "5m");
  const limit = Math.min(Math.max(Number(req.query.limit || 120), 20), 300);
  const cacheKey = `${symbol}_${period}_${limit}`;
  const now = Date.now();
  const cached = oiCache.history.get(cacheKey);
  if (cached && now - cached.ts < 15000) {
    res.status(200).json({ ok: true, data: cached.data });
    return;
  }

  try {
    const valid = await validateFuturesSymbol(symbol);
    if (!valid.ok) {
      res.status(200).json({ ok: false, message: valid.message });
      return;
    }

    const [oiRows, kRows] = await Promise.all([
      fetchJson(
        `https://fapi.binance.com/futures/data/openInterestHist?symbol=${symbol}&period=${period}&limit=${limit}`,
        12000
      ),
      fetchJson(
        `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=${period}&limit=${limit}`,
        12000
      )
    ]);

    const points = (Array.isArray(oiRows) ? oiRows : []).map((row, idx) => {
      const oiQty = Number(row.sumOpenInterest || row.openInterest || 0);
      const k = Array.isArray(kRows) ? kRows[idx] : null;
      const close = k ? Number(k[4] || 0) : 0;
      return {
        t: Number(row.timestamp || row.time || 0),
        oiQty,
        close,
        oiUsd: oiQty * close
      };
    }).filter((x) => x.t > 0 && x.oiUsd > 0);

    const data = { symbol, period, points, timestamp: new Date().toISOString() };
    oiCache.history.set(cacheKey, { ts: now, data });
    res.status(200).json({ ok: true, data });
  } catch (error) {
    res.status(200).json({ ok: false, message: `OI历史获取失败: ${String(error.message || error)}` });
  }
});

app.get("/api/predict-auto/status/:jobId", (req, res) => {
  const jobId = String(req.params.jobId || "");
  const job = predictJobs.get(jobId);
  if (!job) {
    res.status(200).json({ ok: false, message: "任务不存在或已过期" });
    return;
  }

  // 保留最近50个任务，避免无限增长
  if (predictJobs.size > 50) {
    const keys = [...predictJobs.keys()];
    for (const k of keys.slice(0, predictJobs.size - 50)) {
      predictJobs.delete(k);
    }
  }

  res.status(200).json({ ok: true, job });
});

app.get("/api/predict-long/status/:jobId", (req, res) => {
  const jobId = String(req.params.jobId || "");
  const job = predictLongJobs.get(jobId);
  if (!job) {
    res.status(200).json({ ok: false, message: "任务不存在或已过期" });
    return;
  }

  if (predictLongJobs.size > 50) {
    const keys = [...predictLongJobs.keys()];
    for (const k of keys.slice(0, predictLongJobs.size - 50)) predictLongJobs.delete(k);
  }
  res.status(200).json({ ok: true, job });
});

function calcAtr(klines, period = 14) {
  if (!Array.isArray(klines) || klines.length < period + 1) return 0;
  const highs = klines.map((k) => Number(k[2] || 0));
  const lows = klines.map((k) => Number(k[3] || 0));
  const closes = klines.map((k) => Number(k[4] || 0));
  const tr = highs.map((h, i) => {
    const l = lows[i];
    const prevClose = i === 0 ? closes[i] : closes[i - 1];
    return Math.max(h - l, Math.abs(h - prevClose), Math.abs(l - prevClose));
  });
  let atr = tr.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < tr.length; i += 1) {
    atr = (atr * (period - 1) + tr[i]) / period;
  }
  return atr;
}

function calcWeightedImbalance(bids, asks, levels = 40) {
  let num = 0;
  let den = 0;
  for (let i = 0; i < levels; i += 1) {
    const qb = Number((bids[i] || [0, 0])[1] || 0);
    const qa = Number((asks[i] || [0, 0])[1] || 0);
    const w = 1 / (i + 1);
    num += (qb - qa) * w;
    den += (qb + qa) * w;
  }
  if (Math.abs(den) < 1e-12) return 0;
  return num / den;
}

function imbalanceText(v) {
  if (v >= 0.6) return "极强买盘";
  if (v >= 0.2) return "偏强买盘";
  if (v <= -0.6) return "极强卖盘";
  if (v <= -0.2) return "偏强卖盘";
  return "中性盘口";
}

app.get("/api/predict-lob", async (req, res) => {
  const symbol = String(req.query.symbol || "BTCUSDT").toUpperCase();
  const factor = 0.6;
  try {
    const [depth, klines] = await Promise.all([
      fetchJson(`https://fapi.binance.com/fapi/v1/depth?symbol=${symbol}&limit=1000`, 8000),
      fetchJson(`https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=1m&limit=200`, 8000)
    ]);

    const bids = (depth.bids || []).slice(0, 40);
    const asks = (depth.asks || []).slice(0, 40);
    if (!bids.length || !asks.length) {
      return res.status(200).json({ ok: false, message: "订单簿数据为空" });
    }

    const bid1 = Number(bids[0][0]);
    const ask1 = Number(asks[0][0]);
    const entry = (bid1 + ask1) / 2;
    const spread = ask1 - bid1;
    const atr = calcAtr(klines, 14);
    const baseDistance = Math.max(spread * 3, atr);
    const weightedI = calcWeightedImbalance(bids, asks, 40);

    let direction = "横盘";
    if (weightedI > 0.08) direction = "上涨";
    if (weightedI < -0.08) direction = "下跌";

    const confidence = Math.min(0.95, Math.max(0.5, 0.5 + Math.abs(weightedI) * 0.5));

    let sl = null;
    let tp = null;
    let rr = null;
    let note = "建议观望";

    if (direction !== "横盘") {
      const dynamicFactor = Math.max(0.2, Math.min(1.0, 1 - Math.abs(weightedI) * factor));
      const riskDistance = baseDistance * dynamicFactor;
      if (direction === "上涨") {
        sl = entry - riskDistance;
        tp = entry + (entry - sl) * 2;
      } else {
        sl = entry + riskDistance;
        tp = entry - (sl - entry) * 2;
      }
      rr = 2.0;
      note = dynamicFactor < 1 ? "动态收紧" : "基础距离";
    }

    return res.status(200).json({
      ok: true,
      data: {
        symbol,
        strategy: "LOB动态止损",
        direction,
        confidence,
        entry,
        sl,
        tp,
        rr,
        spread,
        atr,
        weightedI,
        weightedIText: imbalanceText(weightedI),
        note,
        timestamp: new Date().toISOString()
      }
    });
  } catch (error) {
    return res.status(200).json({
      ok: false,
      message: `预测失败: ${String(error.message || error)}`
    });
  }
});

app.get("/api/netflix-check", async (req, res) => {
  const startedAt = Date.now();
  const payload = {
    ok: false,
    phase: "init",
    dns: null,
    http: null,
    verdict: "unknown",
    message: "",
    timestamp: new Date().toISOString()
  };

  try {
    payload.phase = "dns";
    const lookup = await dns.promises.lookup("www.netflix.com");
    payload.dns = { address: lookup.address, family: lookup.family };

    payload.phase = "http";
    const http = await requestText("https://www.netflix.com", 8000);
    const contentType = String(http.headers["content-type"] || "");
    const bodyRaw = String(http.bodySnippet || "");
    const body = bodyRaw.toLowerCase();
    const bodyTrim = body.trim();
    payload.http = {
      statusCode: http.statusCode,
      server: String(http.headers.server || ""),
      location: String(http.headers.location || ""),
      contentType,
      bodyLength: http.bodyLength,
      elapsedMs: Date.now() - startedAt
    };

    if (
      bodyTrim === "not available" ||
      body.includes(">not available<") ||
      (contentType.includes("text/plain") && body.includes("not available"))
    ) {
      payload.verdict = "blocked_page";
      payload.message = "访问到了中间拦截页或受限页（包含 Not Available 文本）。";
      return res.status(200).json(payload);
    }

    if (
      body.includes("<title>netflix") ||
      body.includes("www.netflix.com/signup") ||
      body.includes("nflxext.com")
    ) {
      payload.ok = true;
      payload.verdict = "reachable";
      payload.message = "网络可访问 Netflix 主站。";
      return res.status(200).json(payload);
    }

    payload.verdict = "uncertain";
    payload.message = "已连通，但返回内容不典型，可能被网络设备改写。";
    return res.status(200).json(payload);
  } catch (error) {
    payload.phase = payload.phase === "init" ? "dns" : payload.phase;
    payload.verdict = "unreachable";
    payload.message = `检测失败：${String(error.message || error)}`;
    payload.http = {
      elapsedMs: Date.now() - startedAt
    };
    return res.status(200).json(payload);
  }
});

app.post("/webhooks/whale", (req, res) => {
  const body = req.body || {};
  const event = {
    id: body.id || body.hash || `${Date.now()}`,
    timestamp: body.timestamp || new Date().toISOString(),
    asset: body.asset || body.symbol || "unknown",
    usdValue: Number(body.usdValue || body.usd || 0),
    direction: body.direction || "to_exchange",
    source: body.source || "custom-webhook"
  };

  state.whaleEvents = state.whaleEvents || [];
  state.whaleEvents.unshift(event);
  state.whaleEvents = state.whaleEvents.slice(0, 500);
  saveState(state);

  res.status(200).json({ ok: true, event });
});

if (!config.disableHttp) {
  app.listen(config.port, config.host, () => {
    logger.info("HTTP server listening", { host: config.host, port: config.port });
  });
  if (config.trendPageEnabled) {
    trendPageApp.listen(config.trendPagePort, config.host, () => {
      logger.info("Trend page server listening", { host: config.host, port: config.trendPagePort });
    });
  }
} else {
  logger.warn("HTTP server disabled by DISABLE_HTTP=true");
}

if (config.discordBotToken) {
  discordClient = new Client({
    intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildMessages, GatewayIntentBits.MessageContent]
  });

  discordClient.once(Events.ClientReady, (readyClient) => {
    logger.info("Discord bot ready", { tag: readyClient.user.tag });
  });

  discordClient.on(Events.MessageCreate, async (message) => {
    if (message.author.bot) return;

    if (message.content === "!alt status") {
      const snapshot = state.lastSnapshot;
      if (!snapshot) {
        await message.reply("系统刚启动，尚无快照数据。");
        return;
      }

      const text = [
        `时间: ${snapshot.timestamp}`,
        `BTC.D: ${snapshot.global.btcDominance ?? "n/a"}%`,
        `Fear&Greed: ${snapshot.sentiment.value ?? "n/a"}`,
        `当前信号数: ${(state.lastSignals || []).length}`
      ].join("\n");
      await message.reply(`\`\`\`\n${text}\n\`\`\``);
      return;
    }

    if (message.content === "!alt signals") {
      const topSignals = (state.lastSignals || []).slice(0, 12);
      await message.reply(`\`\`\`\n${formatSignals(topSignals)}\n\`\`\``);
      return;
    }
  });

  discordClient.login(config.discordBotToken).catch((error) => {
    logger.error("Discord login failed", { error: String(error) });
  });
} else {
  logger.warn("DISCORD_BOT_TOKEN missing, running in HTTP-only mode");
}

function shutdownPredictWorkers() {
  for (const [symbol, worker] of predictWorkers) {
    try {
      worker.proc.kill("SIGTERM");
    } catch (_) {}
    predictWorkers.delete(symbol);
  }
}

process.on("SIGINT", () => {
  shutdownPredictWorkers();
  process.exit(0);
});

process.on("SIGTERM", () => {
  shutdownPredictWorkers();
  process.exit(0);
});

async function bootstrap() {
  if (config.trendPredictorEnabled) {
    logger.info("Trend predictor mode enabled", {
      symbols: config.trendSymbols,
      intervalMinutes: Number(config.trendPredictIntervalMinutes || 1),
      horizonMinutes: Number(config.trendHorizonMinutes || 10),
      bootstrapTargetMinutes: TREND_BOOTSTRAP_TARGET_MINUTES
    });

    const intervalMs = Math.max(10, Number(config.trendPredictIntervalMinutes || 1)) * 60 * 1000;
    await runTrendPredictCycle();

    if (config.runOnce) {
      process.exit(0);
      return;
    }

    setInterval(() => {
      runTrendPredictCycle().catch((error) => {
        logger.error("Trend interval cycle failed", { error: String(error.message || error) });
      });
    }, intervalMs);
    return;
  }

  if (config.runOnce) {
    await monitorTick();
    process.exit(0);
    return;
  }

  await monitorTick();
  setInterval(monitorTick, config.pollIntervalMinutes * 60 * 1000);
}

bootstrap().catch((error) => {
  logger.error("Bootstrap failed", { error: String(error) });
  process.exit(1);
});
