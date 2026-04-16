#!/usr/bin/env python3
"""Backtest for explosive_spot_radar score model on Binance USDT-M futures."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo


UA = {"User-Agent": "explosive-score-backtest/1.0"}
BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
BINANCE_OI_HIST_URL = "https://fapi.binance.com/futures/data/openInterestHist"
BINANCE_FUNDING_HISTORY_URL = "https://fapi.binance.com/fapi/v1/fundingRate"


@dataclass(frozen=True)
class BacktestConfig:
    symbol: str
    start_ms: int
    end_ms: int
    score_threshold: float = 45.0
    gain_threshold: float = 15.0
    max_dd: float = -8.0
    vol_surge_threshold: float = 3.0
    funding_threshold_rate: float = 0.0
    oi_surge_threshold: float = 1.2
    trailing_stop_percent: float = 15.0
    whale_funding_reversal_rate: float = 0.0
    whale_oi_drop_ratio: float = 0.7
    ohlcv_window: int = 100
    oi_hist_period: str = "5m"
    oi_hist_limit: int = 12
    force_close_at_end: bool = True


def _http_get_json(url: str, timeout: float = 20.0) -> Any:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)


def _q(base: str, params: Dict[str, Any]) -> str:
    return f"{base}?{urllib.parse.urlencode(params)}"


def _score_breakout(vol_surge: float, gain: float, dd: float) -> float:
    return (
        0.40 * min(vol_surge, 50.0)
        + 0.35 * max(gain, 0.0)
        + 0.25 * (100.0 + max(dd, -20.0))
    )


def _parse_dt_with_tz(s: str, tz_name: str) -> datetime:
    tz = ZoneInfo(tz_name)
    t = str(s).strip()
    if not t:
        raise ValueError("empty datetime")
    if "T" in t:
        dt = datetime.fromisoformat(t)
    else:
        dt = datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(timezone.utc)


def _to_ms(dt_utc: datetime) -> int:
    return int(dt_utc.timestamp() * 1000)


def fetch_klines_1m(symbol: str, start_ms: int, end_ms: int, limit: int = 1500) -> List[List[Any]]:
    out: List[List[Any]] = []
    cursor = start_ms
    step_ms = 60_000
    for _ in range(1000):
        url = _q(
            BINANCE_KLINES_URL,
            {
                "symbol": symbol,
                "interval": "1m",
                "startTime": cursor,
                "endTime": end_ms,
                "limit": limit,
            },
        )
        try:
            rows = _http_get_json(url)
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f"fetch_klines_1m failed: {exc}") from exc
        if not isinstance(rows, list) or not rows:
            break
        rows = sorted(rows, key=lambda x: int(x[0]))
        for r in rows:
            ts = int(r[0])
            if ts < start_ms or ts > end_ms:
                continue
            if not out or ts > int(out[-1][0]):
                out.append(r)
        nxt = int(rows[-1][0]) + step_ms
        if nxt <= cursor:
            break
        cursor = nxt
        if cursor > end_ms or len(rows) < limit:
            break
        time.sleep(0.03)
    return out


def fetch_oi_hist(symbol: str, period: str, start_ms: int, end_ms: int) -> List[Tuple[int, float]]:
    out: List[Tuple[int, float]] = []
    cursor = start_ms
    for _ in range(200):
        url = _q(
            BINANCE_OI_HIST_URL,
            {
                "symbol": symbol,
                "period": period,
                "limit": 500,
                "startTime": cursor,
                "endTime": end_ms,
            },
        )
        rows = _http_get_json(url)
        if not isinstance(rows, list) or not rows:
            break
        rows = sorted(rows, key=lambda x: int(float(x.get("timestamp", 0))))
        for r in rows:
            try:
                ts = int(float(r.get("timestamp", 0)))
                oi = float(r.get("sumOpenInterestValue", 0.0) or 0.0)
            except Exception:
                continue
            if ts < start_ms or ts > end_ms:
                continue
            if oi <= 0:
                continue
            out.append((ts, oi))
        nxt = int(float(rows[-1].get("timestamp", 0))) + 1
        if nxt <= cursor:
            break
        cursor = nxt
        if len(rows) < 500 or cursor > end_ms:
            break
        time.sleep(0.03)

    out.sort(key=lambda x: x[0])
    dedup: List[Tuple[int, float]] = []
    for ts, oi in out:
        if dedup and dedup[-1][0] == ts:
            dedup[-1] = (ts, oi)
        else:
            dedup.append((ts, oi))
    return dedup


def fetch_funding_history(symbol: str, start_ms: int, end_ms: int) -> List[Tuple[int, float]]:
    out: List[Tuple[int, float]] = []
    cursor = start_ms - 8 * 60 * 60 * 1000
    for _ in range(20):
        url = _q(
            BINANCE_FUNDING_HISTORY_URL,
            {
                "symbol": symbol,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1000,
            },
        )
        rows = _http_get_json(url)
        if not isinstance(rows, list) or not rows:
            break
        rows = sorted(rows, key=lambda x: int(x.get("fundingTime", 0)))
        for r in rows:
            try:
                ts = int(r.get("fundingTime", 0))
                fr = float(r.get("fundingRate", 0.0) or 0.0)
            except Exception:
                continue
            if ts > end_ms:
                continue
            out.append((ts, fr))
        nxt = int(rows[-1].get("fundingTime", 0)) + 1
        if nxt <= cursor:
            break
        cursor = nxt
        if len(rows) < 1000 or cursor > end_ms:
            break
        time.sleep(0.03)

    out.sort(key=lambda x: x[0])
    dedup: List[Tuple[int, float]] = []
    for ts, fr in out:
        if dedup and dedup[-1][0] == ts:
            dedup[-1] = (ts, fr)
        else:
            dedup.append((ts, fr))
    return dedup


def _latest_value_before(ts: int, series_ts: Sequence[int], series_vals: Sequence[float], default: float) -> float:
    if not series_ts:
        return default
    i = bisect_right(series_ts, ts) - 1
    if i < 0:
        return default
    return float(series_vals[i])


def _oi_ratio_at(ts: int, oi_rows: Sequence[Tuple[int, float]], oi_hist_limit: int) -> Optional[float]:
    if not oi_rows:
        return None
    ts_list = [x[0] for x in oi_rows]
    i = bisect_right(ts_list, ts) - 1
    if i < 0:
        return None
    lo = max(0, i - max(2, oi_hist_limit) + 1)
    window = [x[1] for x in oi_rows[lo : i + 1] if x[1] > 0]
    if len(window) < 2:
        return 1.0
    latest = window[-1]
    prev = window[:-1]
    prev_mean = statistics.mean(prev) if prev else latest
    if prev_mean <= 0:
        return 1.0
    return latest / prev_mean


def _calc_metrics_from_window(rows: Sequence[List[Any]]) -> Optional[Dict[str, float]]:
    if len(rows) < 30:
        return None
    lows = [float(r[3]) for r in rows]
    highs = [float(r[2]) for r in rows]
    closes = [float(r[4]) for r in rows]
    vols = [float(r[5]) for r in rows]

    alert_price = min(lows[-20:])
    current_price = closes[-1]
    high_window = max(highs)
    if alert_price <= 0.0 or current_price <= 0.0:
        return None

    gain = (current_price / alert_price - 1.0) * 100.0
    dd = (current_price / high_window - 1.0) * 100.0 if high_window > 0.0 else 0.0
    vol_mean = statistics.mean(vols)
    recent_vol = statistics.mean(vols[-3:])
    vol_surge = recent_vol / vol_mean if vol_mean > 0 else 1.0
    score = _score_breakout(vol_surge, gain, dd)
    ts = int(rows[-1][0])
    return {
        "ts": float(ts),
        "alert_price": float(alert_price),
        "current_price": float(current_price),
        "gain": float(gain),
        "dd": float(dd),
        "vol_surge": float(vol_surge),
        "score": float(score),
    }


def run_backtest(cfg: BacktestConfig) -> Dict[str, Any]:
    kl = fetch_klines_1m(cfg.symbol, cfg.start_ms, cfg.end_ms)
    if len(kl) < 60:
        raise RuntimeError(f"not enough kline data: {len(kl)}")

    oi_start = cfg.start_ms - 12 * 60 * 60 * 1000
    oi_rows = fetch_oi_hist(cfg.symbol, cfg.oi_hist_period, oi_start, cfg.end_ms)
    funding_rows = fetch_funding_history(cfg.symbol, cfg.start_ms, cfg.end_ms)
    funding_ts = [x[0] for x in funding_rows]
    funding_vals = [x[1] for x in funding_rows]

    trades: List[Dict[str, Any]] = []
    open_pos: Optional[Dict[str, Any]] = None
    signals_total = 0
    derivative_blocked = 0

    for i in range(len(kl)):
        candle = kl[i]
        ts = int(candle[0])
        if ts < cfg.start_ms or ts > cfg.end_ms:
            continue
        window_lo = max(0, i - cfg.ohlcv_window + 1)
        window = kl[window_lo : i + 1]
        metrics = _calc_metrics_from_window(window)
        if not metrics:
            continue

        current_price = float(metrics["current_price"])
        funding_now = _latest_value_before(ts, funding_ts, funding_vals, default=0.0)
        oi_ratio_now = _oi_ratio_at(ts, oi_rows, cfg.oi_hist_limit)
        if oi_ratio_now is None:
            oi_ratio_now = 1.0

        if open_pos is None:
            score_ok = float(metrics["score"]) > cfg.score_threshold
            gain_ok = float(metrics["gain"]) > cfg.gain_threshold
            dd_ok = float(metrics["dd"]) > cfg.max_dd
            vol_ok = float(metrics["vol_surge"]) > cfg.vol_surge_threshold
            if score_ok and gain_ok and dd_ok and vol_ok:
                funding_ok = funding_now > cfg.funding_threshold_rate
                oi_ok = float(oi_ratio_now) > cfg.oi_surge_threshold
                if funding_ok and oi_ok:
                    open_pos = {
                        "entry_ts": ts,
                        "entry_price": current_price,
                        "highest_price": current_price,
                        "entry_score": float(metrics["score"]),
                        "entry_gain": float(metrics["gain"]),
                        "entry_dd": float(metrics["dd"]),
                        "entry_vol_surge": float(metrics["vol_surge"]),
                        "entry_funding_rate": funding_now,
                        "entry_oi_ratio": float(oi_ratio_now),
                    }
                    signals_total += 1
                else:
                    derivative_blocked += 1
            continue

        if current_price > float(open_pos["highest_price"]):
            open_pos["highest_price"] = current_price

        drawdown_from_peak = (current_price / float(open_pos["highest_price"]) - 1.0) * 100.0
        trailing_hit = drawdown_from_peak <= -cfg.trailing_stop_percent
        funding_reversal = funding_now <= cfg.whale_funding_reversal_rate
        oi_drop = float(oi_ratio_now) <= cfg.whale_oi_drop_ratio

        if trailing_hit or funding_reversal or oi_drop:
            reason = "trailing_stop"
            if not trailing_hit:
                reason = "whale_risk"
            entry_price = float(open_pos["entry_price"])
            pnl_pct = (current_price / entry_price - 1.0) * 100.0
            trades.append(
                {
                    "entry_ts": int(open_pos["entry_ts"]),
                    "entry_price": entry_price,
                    "exit_ts": ts,
                    "exit_price": current_price,
                    "hold_min": round((ts - int(open_pos["entry_ts"])) / 60000.0, 2),
                    "pnl_pct": round(pnl_pct, 4),
                    "max_profit_pct": round((float(open_pos["highest_price"]) / entry_price - 1.0) * 100.0, 4),
                    "exit_reason": reason,
                    "entry_score": round(float(open_pos["entry_score"]), 4),
                    "entry_gain_pct": round(float(open_pos["entry_gain"]), 4),
                    "entry_dd_pct": round(float(open_pos["entry_dd"]), 4),
                    "entry_vol_surge": round(float(open_pos["entry_vol_surge"]), 4),
                    "entry_funding_rate": round(float(open_pos["entry_funding_rate"]), 8),
                    "entry_oi_ratio": round(float(open_pos["entry_oi_ratio"]), 6),
                    "exit_funding_rate": round(float(funding_now), 8),
                    "exit_oi_ratio": round(float(oi_ratio_now), 6),
                }
            )
            open_pos = None

    if open_pos is not None and cfg.force_close_at_end:
        last = kl[-1]
        ts = int(last[0])
        px = float(last[4])
        entry_price = float(open_pos["entry_price"])
        pnl_pct = (px / entry_price - 1.0) * 100.0
        trades.append(
            {
                "entry_ts": int(open_pos["entry_ts"]),
                "entry_price": entry_price,
                "exit_ts": ts,
                "exit_price": px,
                "hold_min": round((ts - int(open_pos["entry_ts"])) / 60000.0, 2),
                "pnl_pct": round(pnl_pct, 4),
                "max_profit_pct": round((float(open_pos["highest_price"]) / entry_price - 1.0) * 100.0, 4),
                "exit_reason": "force_close_eod",
                "entry_score": round(float(open_pos["entry_score"]), 4),
                "entry_gain_pct": round(float(open_pos["entry_gain"]), 4),
                "entry_dd_pct": round(float(open_pos["entry_dd"]), 4),
                "entry_vol_surge": round(float(open_pos["entry_vol_surge"]), 4),
                "entry_funding_rate": round(float(open_pos["entry_funding_rate"]), 8),
                "entry_oi_ratio": round(float(open_pos["entry_oi_ratio"]), 6),
                "exit_funding_rate": None,
                "exit_oi_ratio": None,
            }
        )
        open_pos = None

    pnl_values = [float(t["pnl_pct"]) for t in trades]
    wins = [x for x in pnl_values if x > 0]
    losses = [x for x in pnl_values if x <= 0]
    win_rate = (len(wins) / len(pnl_values)) if pnl_values else 0.0
    total_pnl = sum(pnl_values) if pnl_values else 0.0

    return {
        "symbol": cfg.symbol,
        "window_utc": {
            "start": datetime.fromtimestamp(cfg.start_ms / 1000, tz=timezone.utc).isoformat(),
            "end": datetime.fromtimestamp(cfg.end_ms / 1000, tz=timezone.utc).isoformat(),
        },
        "rule_family": "explosive_score_model",
        "rule_notes": {
            "entry": "score+gain+dd+vol_surge with funding and oi confirmation",
            "exit": "trailing stop or whale risk (funding reversal / oi drop)",
        },
        "thresholds": {
            "score_threshold": cfg.score_threshold,
            "gain_threshold_pct": cfg.gain_threshold,
            "max_dd_pct": cfg.max_dd,
            "vol_surge_threshold_x": cfg.vol_surge_threshold,
            "funding_threshold_rate": cfg.funding_threshold_rate,
            "oi_surge_threshold_x": cfg.oi_surge_threshold,
            "trailing_stop_percent": cfg.trailing_stop_percent,
            "whale_funding_reversal_rate": cfg.whale_funding_reversal_rate,
            "whale_oi_drop_ratio_x": cfg.whale_oi_drop_ratio,
            "oi_hist_period": cfg.oi_hist_period,
            "oi_hist_limit": cfg.oi_hist_limit,
        },
        "counts": {
            "kline_1m_rows": len(kl),
            "oi_rows": len(oi_rows),
            "funding_rows": len(funding_rows),
            "entry_signals_opened": signals_total,
            "entry_blocked_by_derivative": derivative_blocked,
            "trades_closed": len(trades),
        },
        "performance": {
            "total_pnl_pct_sum": round(total_pnl, 4),
            "avg_pnl_pct": round(statistics.mean(pnl_values), 4) if pnl_values else 0.0,
            "median_pnl_pct": round(statistics.median(pnl_values), 4) if pnl_values else 0.0,
            "best_trade_pct": round(max(pnl_values), 4) if pnl_values else 0.0,
            "worst_trade_pct": round(min(pnl_values), 4) if pnl_values else 0.0,
            "win_rate": round(win_rate, 4),
            "wins": len(wins),
            "losses": len(losses),
        },
        "trades": trades,
    }


def _write_outputs(summary: Dict[str, Any], out_dir: Path) -> Dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    symbol = str(summary["symbol"])
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"{symbol}_explosive_score_{stamp}"
    summary_path = out_dir / f"{base}_summary.json"
    trades_path = out_dir / f"{base}_trades.csv"

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    trades = summary.get("trades", [])
    fields = [
        "entry_ts",
        "entry_price",
        "exit_ts",
        "exit_price",
        "hold_min",
        "pnl_pct",
        "max_profit_pct",
        "exit_reason",
        "entry_score",
        "entry_gain_pct",
        "entry_dd_pct",
        "entry_vol_surge",
        "entry_funding_rate",
        "entry_oi_ratio",
        "exit_funding_rate",
        "exit_oi_ratio",
    ]
    with trades_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in trades:
            w.writerow({k: row.get(k) for k in fields})
    return {"summary_json": str(summary_path), "trades_csv": str(trades_path)}


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Backtest explosive score model strategy.")
    p.add_argument("--symbol", type=str, default="SIRENUSDT")
    p.add_argument("--start", type=str, required=True, help="Start local time, e.g. 2026-03-20 00:00:00")
    p.add_argument("--end", type=str, required=True, help="End local time, e.g. 2026-03-21 23:59:59")
    p.add_argument("--tz", type=str, default="Asia/Shanghai")
    p.add_argument("--score-threshold", type=float, default=45.0)
    p.add_argument("--gain-threshold", type=float, default=15.0)
    p.add_argument("--max-dd", type=float, default=-8.0)
    p.add_argument("--vol-surge-threshold", type=float, default=3.0)
    p.add_argument("--funding-threshold-rate", type=float, default=0.0)
    p.add_argument("--oi-surge-threshold", type=float, default=1.2)
    p.add_argument("--trailing-stop-percent", type=float, default=15.0)
    p.add_argument("--whale-funding-reversal-rate", type=float, default=0.0)
    p.add_argument("--whale-oi-drop-ratio", type=float, default=0.7)
    p.add_argument("--oi-hist-period", type=str, default="5m")
    p.add_argument("--oi-hist-limit", type=int, default=12)
    p.add_argument("--ohlcv-window", type=int, default=100)
    p.add_argument("--out-dir", type=str, default="data/backtests/explosive_score")
    return p


def main() -> None:
    args = _build_parser().parse_args()
    start_utc = _parse_dt_with_tz(args.start, args.tz)
    end_utc = _parse_dt_with_tz(args.end, args.tz)
    if end_utc <= start_utc:
        raise SystemExit("end must be later than start")

    cfg = BacktestConfig(
        symbol=str(args.symbol).upper(),
        start_ms=_to_ms(start_utc),
        end_ms=_to_ms(end_utc),
        score_threshold=float(args.score_threshold),
        gain_threshold=float(args.gain_threshold),
        max_dd=float(args.max_dd),
        vol_surge_threshold=float(args.vol_surge_threshold),
        funding_threshold_rate=float(args.funding_threshold_rate),
        oi_surge_threshold=float(args.oi_surge_threshold),
        trailing_stop_percent=float(args.trailing_stop_percent),
        whale_funding_reversal_rate=float(args.whale_funding_reversal_rate),
        whale_oi_drop_ratio=float(args.whale_oi_drop_ratio),
        oi_hist_period=str(args.oi_hist_period),
        oi_hist_limit=max(3, int(args.oi_hist_limit)),
        ohlcv_window=max(30, int(args.ohlcv_window)),
    )

    summary = run_backtest(cfg)
    out_paths = _write_outputs(summary, Path(args.out_dir))
    summary["files"] = out_paths
    Path(out_paths["summary_json"]).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

