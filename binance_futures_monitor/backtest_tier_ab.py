#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Offline A/B backtest for tier decision rules.

This tool replays rows from `monitor_ws_tier_decisions.jsonl` and evaluates two
parameter plans on the same candidate set.

Price outcomes come from:
1) Binance Vision local 1m CSV/ZIP files (preferred), or
2) `monitor_ws_live_pnl.csv` matched by symbol+timestamp (fallback).
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import zipfile
from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


HORIZONS: Tuple[Tuple[int, str], ...] = (
    (300, "5m"),
    (900, "15m"),
    (1800, "30m"),
)

ALERT_LEVEL_RANK: Dict[str, int] = {
    "none": 0,
    "WATCH": 1,
    "normal": 2,
    "HOT": 3,
    "SNIPER": 4,
}


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _to_bool(v: Any) -> bool:
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class ReplayPlan:
    name: str
    disable_three_layer_filter: bool
    tier1_score: float
    tier2_score: float
    tier3_score: float
    tier3_discord_push_enabled: bool
    fake_breakout_filter_enabled: bool
    risk_penalty_liquidity: float
    risk_penalty_slippage: float
    risk_penalty_cross_anomaly: float
    risk_penalty_fake_breakout: float
    risk_penalty_iceberg: float
    ml_shadow_enabled: bool
    ml_shadow_downgrade_prob: float
    ml_shadow_upgrade_prob: float
    ml_shadow_penalty: float
    ml_shadow_bonus: float
    ignition_3of4_blocked: bool
    sniper_risk_downgrade_enabled: bool
    sniper_risk_downgrade_penalty: float
    sniper_risk_drop_on_heavy_fake_breakout: bool
    sniper_risk_drop_penalty: float


@dataclass(frozen=True)
class ExitConfig:
    enabled: bool = True
    breakeven_trigger_pct: float = 0.8
    breakeven_lock_pct: float = 0.05
    trailing_callback_pct: float = 0.35
    timeout_sec: int = 600
    timeout_min_profit_pct: float = 0.5


def _build_plan(name: str, raw: Dict[str, Any]) -> ReplayPlan:
    return ReplayPlan(
        name=str(raw.get("name", name) or name),
        disable_three_layer_filter=bool(raw.get("disable_three_layer_filter", False)),
        tier1_score=float(raw.get("tier1_score", 85.0)),
        tier2_score=float(raw.get("tier2_score", 65.0)),
        tier3_score=float(raw.get("tier3_score", 45.0)),
        tier3_discord_push_enabled=bool(raw.get("tier3_discord_push_enabled", False)),
        fake_breakout_filter_enabled=bool(raw.get("fake_breakout_filter_enabled", True)),
        risk_penalty_liquidity=float(raw.get("risk_penalty_liquidity", 10.0)),
        risk_penalty_slippage=float(raw.get("risk_penalty_slippage", 10.0)),
        risk_penalty_cross_anomaly=float(raw.get("risk_penalty_cross_anomaly", 6.0)),
        risk_penalty_fake_breakout=float(raw.get("risk_penalty_fake_breakout", 12.0)),
        risk_penalty_iceberg=float(raw.get("risk_penalty_iceberg", 4.0)),
        ml_shadow_enabled=bool(raw.get("ml_shadow_enabled", True)),
        ml_shadow_downgrade_prob=float(raw.get("ml_shadow_downgrade_prob", 0.30)),
        ml_shadow_upgrade_prob=float(raw.get("ml_shadow_upgrade_prob", 0.75)),
        ml_shadow_penalty=float(raw.get("ml_shadow_penalty", 8.0)),
        ml_shadow_bonus=float(raw.get("ml_shadow_bonus", 5.0)),
        ignition_3of4_blocked=bool(raw.get("ignition_3of4_blocked", False)),
        sniper_risk_downgrade_enabled=bool(raw.get("sniper_risk_downgrade_enabled", False)),
        sniper_risk_downgrade_penalty=float(raw.get("sniper_risk_downgrade_penalty", 20.0)),
        sniper_risk_drop_on_heavy_fake_breakout=bool(raw.get("sniper_risk_drop_on_heavy_fake_breakout", True)),
        sniper_risk_drop_penalty=float(raw.get("sniper_risk_drop_penalty", 35.0)),
    )


def _default_codex_plan() -> ReplayPlan:
    return _build_plan(
        "codex_plan",
        {
            "disable_three_layer_filter": False,
            "tier1_score": 85.0,
            "tier2_score": 65.0,
            "tier3_score": 45.0,
            "tier3_discord_push_enabled": False,
            "fake_breakout_filter_enabled": True,
            "risk_penalty_liquidity": 10.0,
            "risk_penalty_slippage": 10.0,
            "risk_penalty_cross_anomaly": 6.0,
            "risk_penalty_fake_breakout": 12.0,
            "risk_penalty_iceberg": 4.0,
            "ml_shadow_enabled": True,
            "ml_shadow_downgrade_prob": 0.30,
            "ml_shadow_upgrade_prob": 0.75,
            "ml_shadow_penalty": 8.0,
            "ml_shadow_bonus": 5.0,
            "ignition_3of4_blocked": True,
            "sniper_risk_downgrade_enabled": True,
            "sniper_risk_downgrade_penalty": 20.0,
            "sniper_risk_drop_on_heavy_fake_breakout": True,
            "sniper_risk_drop_penalty": 35.0,
        },
    )


def _default_user_plan() -> ReplayPlan:
    return _build_plan(
        "user_plan",
        {
            "disable_three_layer_filter": False,
            "tier1_score": 90.0,
            "tier2_score": 70.0,
            "tier3_score": 55.0,
            "tier3_discord_push_enabled": False,
            "fake_breakout_filter_enabled": True,
            "risk_penalty_liquidity": 14.0,
            "risk_penalty_slippage": 25.0,
            "risk_penalty_cross_anomaly": 8.0,
            "risk_penalty_fake_breakout": 45.0,
            "risk_penalty_iceberg": 6.0,
            "ml_shadow_enabled": True,
            "ml_shadow_downgrade_prob": 0.40,
            "ml_shadow_upgrade_prob": 0.85,
            "ml_shadow_penalty": 25.0,
            "ml_shadow_bonus": 2.0,
            "ignition_3of4_blocked": True,
            "sniper_risk_downgrade_enabled": True,
            "sniper_risk_downgrade_penalty": 20.0,
            "sniper_risk_drop_on_heavy_fake_breakout": True,
            "sniper_risk_drop_penalty": 35.0,
        },
    )


def _load_json(path: Optional[str]) -> Dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"json not found: {p}")
    raw = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"json must be object: {p}")
    return raw


def _load_decisions(path: Path, symbol_allow: Optional[set[str]] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                d = json.loads(line)
            except Exception:
                continue
            if "base_score" not in d:
                continue
            symbol = str(d.get("symbol") or "").upper()
            if not symbol:
                continue
            if symbol_allow is not None and symbol not in symbol_allow:
                continue
            d["symbol"] = symbol
            d["ts"] = _to_float(d.get("ts"), 0.0)
            out.append(d)
    out.sort(key=lambda x: (_to_float(x.get("ts")), str(x.get("symbol"))))
    return out


def _match_live_pnl_returns(live_pnl: Path) -> Dict[str, List[Tuple[float, Dict[str, float]]]]:
    idx: Dict[str, List[Tuple[float, Dict[str, float]]]] = {}
    with live_pnl.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            symbol = str(row.get("symbol") or "").upper()
            ts = _to_float(row.get("entry_ts"), 0.0)
            if not symbol or ts <= 0:
                continue
            rets = {
                "ret_5m_pct": _to_float(row.get("ret_5m_pct"), 0.0),
                "ret_15m_pct": _to_float(row.get("ret_15m_pct"), 0.0),
                "ret_30m_pct": _to_float(row.get("ret_30m_pct"), 0.0),
                "mfe_30m_pct": _to_float(row.get("mfe_30m_pct"), 0.0),
                "mae_30m_pct": _to_float(row.get("mae_30m_pct"), 0.0),
            }
            idx.setdefault(symbol, []).append((ts, rets))
    for symbol in idx:
        idx[symbol].sort(key=lambda x: x[0])
    return idx


def _calc_rets_from_live_match(
    symbol: str,
    ts: float,
    live_idx: Dict[str, List[Tuple[float, Dict[str, float]]]],
) -> Optional[Dict[str, float]]:
    arr = live_idx.get(symbol)
    if not arr:
        return None
    ts_list = [x[0] for x in arr]
    i = bisect_left(ts_list, ts)
    candidates = [arr[j] for j in (i - 1, i, i + 1) if 0 <= j < len(arr)]
    if not candidates:
        return None
    best = min(candidates, key=lambda x: abs(x[0] - ts))
    if abs(best[0] - ts) > 3.0:
        return None
    return dict(best[1])


def _iter_days(start_dt: datetime, end_dt: datetime) -> Iterable[str]:
    d = datetime(start_dt.year, start_dt.month, start_dt.day)
    end_d = datetime(end_dt.year, end_dt.month, end_dt.day)
    while d <= end_d:
        yield d.strftime("%Y-%m-%d")
        d += timedelta(days=1)


def _vision_candidates(root: Path, symbol: str, day: str) -> List[Path]:
    upper = symbol.upper()
    return [
        root / "data" / "futures" / "um" / "daily" / "klines" / upper / "1m" / f"{upper}-1m-{day}.zip",
        root / "data" / "futures" / "um" / "daily" / "klines" / upper / "1m" / f"{upper}-1m-{day}.csv",
        root / "futures" / "um" / "daily" / "klines" / upper / "1m" / f"{upper}-1m-{day}.zip",
        root / "futures" / "um" / "daily" / "klines" / upper / "1m" / f"{upper}-1m-{day}.csv",
        root / upper / "1m" / f"{upper}-1m-{day}.zip",
        root / upper / "1m" / f"{upper}-1m-{day}.csv",
        root / f"{upper}-1m-{day}.zip",
        root / f"{upper}-1m-{day}.csv",
    ]


def _parse_kline_rows(rows: Sequence[Sequence[str]]) -> List[Tuple[int, float]]:
    out: List[Tuple[int, float]] = []
    for row in rows:
        if not row:
            continue
        try:
            open_ms = int(float(row[0]))
            close_price = float(row[4])
        except Exception:
            continue
        if close_price <= 0:
            continue
        out.append((open_ms // 1000, close_price))
    return out


def _load_vision_symbol_prices(root: Path, symbol: str, days: Sequence[str]) -> List[Tuple[int, float]]:
    out: List[Tuple[int, float]] = []
    for day in days:
        day_rows: List[Tuple[int, float]] = []
        for path in _vision_candidates(root, symbol, day):
            if not path.exists():
                continue
            if path.suffix.lower() == ".zip":
                with zipfile.ZipFile(path, "r") as zf:
                    members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
                    if not members:
                        continue
                    with zf.open(members[0], "r") as fp:
                        text = fp.read().decode("utf-8", errors="ignore").splitlines()
                    rows = list(csv.reader(text))
                    day_rows.extend(_parse_kline_rows(rows))
            else:
                with path.open("r", encoding="utf-8", errors="ignore") as f:
                    rows = list(csv.reader(f))
                day_rows.extend(_parse_kline_rows(rows))
            if day_rows:
                break
        out.extend(day_rows)
    out.sort(key=lambda x: x[0])
    dedup: List[Tuple[int, float]] = []
    last_ts = -1
    for ts, px in out:
        if ts == last_ts:
            if dedup:
                dedup[-1] = (ts, px)
            continue
        dedup.append((ts, px))
        last_ts = ts
    return dedup


def _calc_rets_from_prices(ts: float, prices: Sequence[Tuple[int, float]], direction: float = 1.0) -> Optional[Dict[str, float]]:
    if not prices:
        return None
    ts_list = [x[0] for x in prices]
    i = bisect_right(ts_list, int(ts)) - 1
    if i < 0:
        i = 0
    entry = float(prices[i][1])
    if entry <= 0:
        return None
    out: Dict[str, float] = {}
    for sec, label in HORIZONS:
        j = bisect_left(ts_list, int(ts + sec))
        if j >= len(prices):
            return None
        px = float(prices[j][1])
        out[f"ret_{label}_pct"] = ((px - entry) / entry * 100.0) * float(direction)
    return out


def _side_direction(side: str) -> float:
    s = str(side or "").strip().lower()
    if s == "short":
        return -1.0
    return 1.0


def _extract_side_ret_path(ts: float, prices: Sequence[Tuple[int, float]], direction: float, max_sec: int = 1800) -> List[Tuple[int, float]]:
    if not prices:
        return []
    ts_list = [x[0] for x in prices]
    i = bisect_right(ts_list, int(ts)) - 1
    if i < 0:
        i = 0
    entry = float(prices[i][1])
    if entry <= 0:
        return []
    end_ts = int(ts + max_sec)
    j_end = bisect_left(ts_list, end_ts)
    if j_end < len(ts_list) and ts_list[j_end] <= end_ts:
        j_end += 1
    out: List[Tuple[int, float]] = [(0, 0.0)]
    for j in range(i, min(len(prices), max(j_end, i + 1))):
        t, px = prices[j]
        if t < int(ts):
            continue
        raw_ret = (float(px) - entry) / entry * 100.0
        side_ret = raw_ret * direction
        elapsed = max(0, int(t - int(ts)))
        if elapsed > max_sec:
            break
        out.append((elapsed, side_ret))
    if out and out[-1][0] < max_sec:
        # Keep the same last return until horizon for deterministic fallback.
        out.append((max_sec, out[-1][1]))
    return out


def _simulate_dynamic_exit_exact(path: Sequence[Tuple[int, float]], static_ret_30m: float, cfg: ExitConfig) -> Tuple[float, str]:
    if not cfg.enabled:
        return static_ret_30m, "disabled"
    if not path:
        return static_ret_30m, "no_path"

    max_ret = 0.0
    be_armed = False
    trail_stop: Optional[float] = None
    timeout_done = False

    for elapsed, ret in path:
        max_ret = max(max_ret, float(ret))
        if (not be_armed) and max_ret >= float(cfg.breakeven_trigger_pct):
            be_armed = True
            trail_stop = float(cfg.breakeven_lock_pct)

        if be_armed:
            cur_stop = max(float(cfg.breakeven_lock_pct), max_ret - float(cfg.trailing_callback_pct))
            if trail_stop is None:
                trail_stop = cur_stop
            else:
                trail_stop = max(trail_stop, cur_stop)
            if float(ret) <= float(trail_stop):
                return float(trail_stop), "trailing_exit_exact"

        if (not timeout_done) and elapsed >= int(cfg.timeout_sec):
            timeout_done = True
            if max_ret < float(cfg.timeout_min_profit_pct):
                return float(ret), "timeout_exit_exact"

    return static_ret_30m, "hold_30m_exact"


def _simulate_dynamic_exit_approx(returns: Dict[str, float], static_ret_30m: float, cfg: ExitConfig) -> Tuple[float, str]:
    if not cfg.enabled:
        return static_ret_30m, "disabled"

    ret5 = _to_float(returns.get("ret_5m_pct"), static_ret_30m)
    ret15 = _to_float(returns.get("ret_15m_pct"), static_ret_30m)
    ret30 = _to_float(returns.get("ret_30m_pct"), static_ret_30m)
    mfe = _to_float(returns.get("mfe_30m_pct"), max(ret5, ret15, ret30, 0.0))

    if mfe >= float(cfg.breakeven_trigger_pct):
        trail = max(float(cfg.breakeven_lock_pct), mfe - float(cfg.trailing_callback_pct))
        if ret30 <= trail:
            return float(trail), "trailing_exit_approx"
        return ret30, "hold_after_be_approx"

    # timeout proxy: 5m / 15m checkpoints are available in live_pnl.
    if int(cfg.timeout_sec) <= 300:
        if max(0.0, ret5) < float(cfg.timeout_min_profit_pct):
            return ret5, "timeout_exit_5m_approx"
    elif int(cfg.timeout_sec) <= 900:
        if max(ret5, ret15, 0.0) < float(cfg.timeout_min_profit_pct):
            return ret15, "timeout_exit_15m_approx"
    else:
        if max(ret5, ret15, 0.0) < float(cfg.timeout_min_profit_pct):
            return ret30, "timeout_exit_30m_proxy_approx"

    return ret30, "hold_30m_approx"


def _replay_decision(row: Dict[str, Any], plan: ReplayPlan) -> Dict[str, Any]:
    reason = str(row.get("reason") or "")
    level = str(row.get("level") or "none")

    if plan.ignition_3of4_blocked and reason == "ignition_3of4":
        return {
            "final_score_replay": 0.0,
            "tier_replay": "TIER4",
            "delivery_replay": "record_only",
            "sniper_risk_action": "none",
            "block_action": "blocked_ignition_3of4",
        }

    base_score = _to_float(row.get("base_score"), 0.0)
    market_factor = _to_float(row.get("market_factor"), 1.0)
    symbol_factor = _to_float(row.get("symbol_factor"), 1.0)
    mtf_factor = _to_float(row.get("mtf_factor"), 1.0)
    flow_quality_factor = _to_float(row.get("flow_quality_factor"), 1.0)
    regime_factor = _to_float(row.get("regime_factor"), 1.0)
    sector_cluster_factor = _to_float(row.get("sector_cluster_factor"), 1.0)
    sentiment_factor = _to_float(row.get("sentiment_factor"), 1.0)
    factors_mult = (
        market_factor
        * symbol_factor
        * mtf_factor
        * flow_quality_factor
        * regime_factor
        * sector_cluster_factor
        * sentiment_factor
    )
    pre_risk = base_score * factors_mult

    flags = set(str(x) for x in (row.get("risk_flags") or []))
    risk_penalty = 0.0
    if "fake_breakout" in flags and plan.fake_breakout_filter_enabled:
        risk_penalty += plan.risk_penalty_fake_breakout
    if "liquidity" in flags:
        risk_penalty += plan.risk_penalty_liquidity
    if "slippage" in flags:
        risk_penalty += plan.risk_penalty_slippage
    if "cross_anomaly" in flags:
        risk_penalty += plan.risk_penalty_cross_anomaly
    if "iceberg" in flags or "iceberg_prob" in flags:
        risk_penalty += plan.risk_penalty_iceberg
    if "funding_divergence" in flags:
        risk_penalty += 4.0

    pre_ml = pre_risk - risk_penalty
    ml_adjust = 0.0
    if plan.ml_shadow_enabled:
        ml_win_prob = _to_float(row.get("ml_win_prob"), 0.5)
        if ml_win_prob < plan.ml_shadow_downgrade_prob:
            ml_adjust -= plan.ml_shadow_penalty
        elif ml_win_prob > plan.ml_shadow_upgrade_prob:
            ml_adjust += plan.ml_shadow_bonus

    final_score = max(0.0, pre_ml + ml_adjust)
    if final_score >= plan.tier1_score:
        tier = "TIER1"
    elif final_score >= plan.tier2_score:
        tier = "TIER2"
    elif final_score >= plan.tier3_score:
        tier = "TIER3"
    else:
        tier = "TIER4"

    if plan.fake_breakout_filter_enabled and "fake_breakout" in flags and tier in {"TIER1", "TIER2"}:
        tier = "TIER3"

    sniper_risk_action = "none"
    if level == "SNIPER":
        if plan.sniper_risk_downgrade_enabled:
            fake_breakout_hit = "fake_breakout" in flags
            heavy_fake_breakout = (
                plan.sniper_risk_drop_on_heavy_fake_breakout
                and fake_breakout_hit
                and risk_penalty >= plan.sniper_risk_drop_penalty
            )
            if heavy_fake_breakout:
                tier = "TIER4"
                sniper_risk_action = "drop_tier4_heavy_fake_breakout"
            elif fake_breakout_hit or risk_penalty >= plan.sniper_risk_downgrade_penalty:
                tier = "TIER3"
                sniper_risk_action = "downgrade_tier3_risk"
            else:
                tier = "TIER1"
                sniper_risk_action = "keep_tier1"
        else:
            tier = "TIER1"
            sniper_risk_action = "legacy_force_tier1"

    if plan.disable_three_layer_filter:
        delivery = "realtime"
    elif tier == "TIER1":
        delivery = "realtime"
    elif tier == "TIER2":
        delivery = "confirm"
    elif tier == "TIER3":
        delivery = "summary" if plan.tier3_discord_push_enabled else "record_only"
    else:
        delivery = "record_only"

    return {
        "final_score_replay": final_score,
        "tier_replay": tier,
        "delivery_replay": delivery,
        "sniper_risk_action": sniper_risk_action,
        "block_action": "none",
    }


def _effective_level(level: str) -> str:
    lv = str(level or "").strip()
    if ALERT_LEVEL_RANK.get(lv, 0) > 0:
        return lv
    return "WATCH"


def _agg_metrics(rows: List[Dict[str, Any]], ret_key: str) -> Dict[str, Any]:
    vals = [_to_float(r.get(ret_key), 0.0) for r in rows]
    n = len(vals)
    wins = sum(1 for v in vals if v > 0)
    return {
        "n": n,
        "win_rate": (wins / n) if n else 0.0,
        "avg_ret_pct": (sum(vals) / n) if n else 0.0,
        "sum_ret_pct": sum(vals),
        "median_ret_pct": statistics.median(vals) if vals else 0.0,
    }


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Offline A/B backtest from tier decisions")
    p.add_argument("--tier-decisions", type=str, default="data/logs/monitor_ws_tier_decisions.jsonl")
    p.add_argument("--live-pnl", type=str, default="data/logs/monitor_ws_live_pnl.csv")
    p.add_argument("--vision-root", type=str, default="")
    p.add_argument("--symbols", type=str, default="")
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--bottom-n", type=int, default=10)
    p.add_argument("--plan-a-json", type=str, default="")
    p.add_argument("--plan-b-json", type=str, default="")
    p.add_argument("--exit-enabled", type=_to_bool, default=True)
    p.add_argument("--exit-breakeven-trigger-pct", type=float, default=0.8)
    p.add_argument("--exit-breakeven-lock-pct", type=float, default=0.05)
    p.add_argument("--exit-trailing-callback-pct", type=float, default=0.35)
    p.add_argument("--exit-timeout-sec", type=int, default=600)
    p.add_argument("--exit-timeout-min-profit-pct", type=float, default=0.5)
    p.add_argument("--out-dir", type=str, default="data/backtests/tier_ab_offline")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    tier_path = Path(args.tier_decisions)
    live_path = Path(args.live_pnl)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not tier_path.exists():
        raise FileNotFoundError(f"tier decisions not found: {tier_path}")
    if not live_path.exists():
        raise FileNotFoundError(f"live pnl not found: {live_path}")

    live_idx = _match_live_pnl_returns(live_path)

    if str(args.symbols or "").strip():
        symbol_allow = {x.strip().upper() for x in str(args.symbols).split(",") if x.strip()}
        top_symbols: List[str] = sorted(symbol_allow)
        bottom_symbols: List[str] = []
    else:
        sums: Dict[str, float] = {}
        for symbol, arr in live_idx.items():
            sums[symbol] = sum(float(x[1].get("ret_30m_pct", 0.0)) for x in arr)
        ranked = sorted(sums.items(), key=lambda kv: kv[1], reverse=True)
        top_symbols = [s for s, _ in ranked[: max(0, int(args.top_n))]]
        bottom_symbols = [s for s, _ in ranked[-max(0, int(args.bottom_n)) :]] if args.bottom_n > 0 else []
        symbol_allow = set(top_symbols + bottom_symbols)

    decisions = _load_decisions(tier_path, symbol_allow=symbol_allow)
    if not decisions:
        raise RuntimeError("no candidate decisions after symbol filter")

    min_dt = datetime.utcfromtimestamp(min(_to_float(d.get("ts")) for d in decisions)) - timedelta(hours=1)
    max_dt = datetime.utcfromtimestamp(max(_to_float(d.get("ts")) for d in decisions)) + timedelta(hours=1)
    days = list(_iter_days(min_dt, max_dt))

    vision_root = Path(args.vision_root).expanduser() if str(args.vision_root).strip() else None
    vision_prices: Dict[str, List[Tuple[int, float]]] = {}
    vision_symbols_loaded = 0
    if vision_root is not None and vision_root.exists():
        for symbol in sorted(symbol_allow):
            px = _load_vision_symbol_prices(vision_root, symbol, days)
            if px:
                vision_prices[symbol] = px
                vision_symbols_loaded += 1

    plan_a = _default_codex_plan()
    plan_b = _default_user_plan()
    if args.plan_a_json:
        plan_a = _build_plan(plan_a.name, {**plan_a.__dict__, **_load_json(args.plan_a_json)})
    if args.plan_b_json:
        plan_b = _build_plan(plan_b.name, {**plan_b.__dict__, **_load_json(args.plan_b_json)})
    plans = [plan_a, plan_b]
    exit_cfg = ExitConfig(
        enabled=bool(args.exit_enabled),
        breakeven_trigger_pct=float(args.exit_breakeven_trigger_pct),
        breakeven_lock_pct=float(args.exit_breakeven_lock_pct),
        trailing_callback_pct=float(args.exit_trailing_callback_pct),
        timeout_sec=max(60, int(args.exit_timeout_sec)),
        timeout_min_profit_pct=float(args.exit_timeout_min_profit_pct),
    )

    rows_export: List[Dict[str, Any]] = []
    priced_count = 0
    priced_from_vision = 0
    priced_from_live = 0
    unpriced_count = 0

    for d in decisions:
        symbol = str(d.get("symbol") or "").upper()
        ts = _to_float(d.get("ts"), 0.0)
        side = str(d.get("side") or "neutral")
        direction = _side_direction(side)
        rets = None
        price_source = ""
        ret_path: List[Tuple[int, float]] = []
        if symbol in vision_prices:
            rets = _calc_rets_from_prices(ts, vision_prices[symbol], direction=direction)
            if rets is not None:
                ret_path = _extract_side_ret_path(ts, vision_prices[symbol], direction=direction, max_sec=1800)
                price_source = "vision_csv"
                priced_from_vision += 1
        if rets is None:
            rets = _calc_rets_from_live_match(symbol, ts, live_idx)
            if rets is not None:
                price_source = "live_pnl"
                priced_from_live += 1

        if rets is None:
            unpriced_count += 1
            continue
        priced_count += 1
        static_ret_30 = _to_float(rets.get("ret_30m_pct"), 0.0)
        if ret_path:
            ret_dynamic, exit_type = _simulate_dynamic_exit_exact(ret_path, static_ret_30, exit_cfg)
            exit_calc_mode = "exact_path"
        else:
            ret_dynamic, exit_type = _simulate_dynamic_exit_approx(rets, static_ret_30, exit_cfg)
            exit_calc_mode = "approx_live_pnl"

        for plan in plans:
            replay = _replay_decision(d, plan)
            rows_export.append(
                {
                    "plan": plan.name,
                    "symbol": symbol,
                    "ts": ts,
                    "time_bjt": d.get("time_bjt", ""),
                    "reason": d.get("reason", ""),
                    "level": d.get("level", ""),
                    "risk_flags": ",".join(str(x) for x in (d.get("risk_flags") or [])),
                    "final_score_replay": replay["final_score_replay"],
                    "tier_replay": replay["tier_replay"],
                    "delivery_replay": replay["delivery_replay"],
                    "sniper_risk_action": replay["sniper_risk_action"],
                    "block_action": replay["block_action"],
                    "ret_5m_pct": rets.get("ret_5m_pct", 0.0),
                    "ret_15m_pct": rets.get("ret_15m_pct", 0.0),
                    "ret_30m_pct": rets.get("ret_30m_pct", 0.0),
                    "ret_dynamic_exit_pct": ret_dynamic,
                    "exit_type": exit_type,
                    "exit_calc_mode": exit_calc_mode,
                    "effective_level": _effective_level(str(d.get("level", ""))),
                    "price_source": price_source,
                }
            )

    by_plan: Dict[str, Dict[str, Any]] = {}
    for plan in plans:
        rows = [r for r in rows_export if r["plan"] == plan.name]
        pushed_raw = [r for r in rows if r["delivery_replay"] in {"realtime", "confirm"}]
        pushed_trade = [r for r in pushed_raw if str(r.get("effective_level", "WATCH")) != "WATCH"]
        static_push = _agg_metrics(pushed_trade, "ret_30m_pct")
        dynamic_push = _agg_metrics(pushed_trade, "ret_dynamic_exit_pct")
        by_plan[plan.name] = {
            "rows_priced": len(rows),
            "rows_pushed_raw": len(pushed_raw),
            "rows_pushed_trade": len(pushed_trade),
            "tier_counts": {
                "TIER1": sum(1 for r in rows if r["tier_replay"] == "TIER1"),
                "TIER2": sum(1 for r in rows if r["tier_replay"] == "TIER2"),
                "TIER3": sum(1 for r in rows if r["tier_replay"] == "TIER3"),
                "TIER4": sum(1 for r in rows if r["tier_replay"] == "TIER4"),
            },
            "all_priced": {label: _agg_metrics(rows, f"ret_{label}_pct") for _, label in HORIZONS},
            "push_raw": {label: _agg_metrics(pushed_raw, f"ret_{label}_pct") for _, label in HORIZONS},
            "push_only": {label: _agg_metrics(pushed_trade, f"ret_{label}_pct") for _, label in HORIZONS},
            "all_priced_dynamic": _agg_metrics(rows, "ret_dynamic_exit_pct"),
            "push_raw_dynamic": _agg_metrics(pushed_raw, "ret_dynamic_exit_pct"),
            "push_only_dynamic": dynamic_push,
            "dynamic_vs_static_push_only": {
                "sum_delta_pct": _to_float(dynamic_push.get("sum_ret_pct"), 0.0) - _to_float(static_push.get("sum_ret_pct"), 0.0),
                "avg_delta_pct": _to_float(dynamic_push.get("avg_ret_pct"), 0.0) - _to_float(static_push.get("avg_ret_pct"), 0.0),
            },
            "exit_type_counts_push": {
                k: v
                for k, v in sorted(
                    (
                        (etype, sum(1 for r in pushed_trade if str(r.get("exit_type", "")) == etype))
                        for etype in {str(r.get("exit_type", "")) for r in pushed_trade}
                    ),
                    key=lambda kv: kv[1],
                    reverse=True,
                )
                if v > 0
            },
            "exit_calc_mode_counts_push": {
                k: v
                for k, v in sorted(
                    (
                        (mode, sum(1 for r in pushed_trade if str(r.get("exit_calc_mode", "")) == mode))
                        for mode in {str(r.get("exit_calc_mode", "")) for r in pushed_trade}
                    ),
                    key=lambda kv: kv[1],
                    reverse=True,
                )
                if v > 0
            },
            "reason_counts_push": {
                k: v
                for k, v in sorted(
                    (
                        (reason, sum(1 for r in pushed_trade if str(r["reason"]) == reason))
                        for reason in {str(r["reason"]) for r in pushed_trade}
                    ),
                    key=lambda kv: kv[1],
                    reverse=True,
                )
                if v > 0
            },
        }

    out_csv = out_dir / "tier_ab_rows.csv"
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "plan",
                "symbol",
                "ts",
                "time_bjt",
                "reason",
                "level",
                "risk_flags",
                "final_score_replay",
                "tier_replay",
                "delivery_replay",
                "sniper_risk_action",
                "block_action",
                "ret_5m_pct",
                "ret_15m_pct",
                "ret_30m_pct",
                "ret_dynamic_exit_pct",
                "exit_type",
                "exit_calc_mode",
                "effective_level",
                "price_source",
            ],
        )
        w.writeheader()
        for row in rows_export:
            w.writerow(row)

    out_summary = out_dir / "tier_ab_summary.json"
    out_summary.write_text(
        json.dumps(
            {
                "source": {
                    "tier_decisions": str(tier_path),
                    "live_pnl": str(live_path),
                    "vision_root": str(vision_root) if vision_root is not None else "",
                },
                "selection": {
                    "top_symbols": top_symbols,
                    "bottom_symbols": bottom_symbols,
                    "selected_symbols": sorted(symbol_allow),
                },
                "window_utc": {
                    "start": min_dt.isoformat() + "Z",
                    "end": max_dt.isoformat() + "Z",
                    "days": days,
                },
                "coverage": {
                    "candidate_rows": len(decisions),
                    "priced_rows": priced_count,
                    "priced_from_vision": priced_from_vision,
                    "priced_from_live": priced_from_live,
                    "unpriced_rows": unpriced_count,
                    "vision_symbols_loaded": vision_symbols_loaded,
                },
                "exit_config": {
                    "enabled": bool(exit_cfg.enabled),
                    "breakeven_trigger_pct": float(exit_cfg.breakeven_trigger_pct),
                    "breakeven_lock_pct": float(exit_cfg.breakeven_lock_pct),
                    "trailing_callback_pct": float(exit_cfg.trailing_callback_pct),
                    "timeout_sec": int(exit_cfg.timeout_sec),
                    "timeout_min_profit_pct": float(exit_cfg.timeout_min_profit_pct),
                },
                "plans": by_plan,
                "files": {
                    "rows_csv": str(out_csv),
                    "summary_json": str(out_summary),
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(json.dumps({"summary": str(out_summary), "rows": str(out_csv)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
