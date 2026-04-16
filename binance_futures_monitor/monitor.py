#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Deep analyzer with composite scoring for websocket monitor."""

from __future__ import annotations

import asyncio
import json
import os
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from binance_futures_monitor.advanced_modules import AdvancedModuleConfig, run_advanced_modules
from binance_futures_monitor.utils import ModuleSignals, compute_signals

try:
    import ccxt.async_support as ccxt_async  # type: ignore
except Exception:
    ccxt_async = None  # type: ignore[assignment]


BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_SPOT = "https://api.binance.com"
COINBASE_API = "https://api.exchange.coinbase.com"


@dataclass(frozen=True)
class AnalyzerConfig:
    data_dir: Path = Path("data")
    mode: str = "composite"
    advanced_modules: AdvancedModuleConfig = AdvancedModuleConfig()


def normalize_symbol(raw_symbol: str) -> str:
    s = str(raw_symbol or "").strip().upper()
    if "/" in s:
        s = s.replace("/", "").replace(":USDT", "")
    return s


def to_ccxt_symbol(raw_symbol: str) -> str:
    s = normalize_symbol(raw_symbol)
    if s.endswith("USDT"):
        return f"{s[:-4]}/USDT:USDT"
    return s


def _http_get_json(url: str, timeout: float = 8.0) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": "monitor-ws/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


async def _aget_json(url: str, timeout: float = 8.0) -> Any:
    return await asyncio.to_thread(_http_get_json, url, timeout)


def _q(base: str, params: Dict[str, Any]) -> str:
    return f"{base}?{urllib.parse.urlencode(params)}"


def _parse_klines(rows: Any) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    if not isinstance(rows, list):
        return out
    for r in rows:
        if not isinstance(r, list) or len(r) < 11:
            continue
        try:
            out.append(
                {
                    "ts": float(r[0]),
                    "open": float(r[1]),
                    "high": float(r[2]),
                    "low": float(r[3]),
                    "close": float(r[4]),
                    "base_volume": float(r[5]),
                    "quote_volume": float(r[7]),
                    "trades": float(r[8]),
                    "taker_buy_quote": float(r[10]),
                }
            )
        except Exception:
            continue
    return out


def _parse_open_interest(rows: Any) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    if not isinstance(rows, list):
        return out
    for r in rows:
        if not isinstance(r, dict):
            continue
        try:
            out.append(
                {
                    "ts": float(r.get("timestamp", 0.0) or 0.0),
                    "oi": float(r.get("sumOpenInterestValue", r.get("sumOpenInterest", 0.0)) or 0.0),
                }
            )
        except Exception:
            continue
    out.sort(key=lambda x: x["ts"])
    return out


def _percent_change(new: float, old: float) -> float:
    if abs(old) <= 1e-12:
        return 0.0
    return (new - old) / old * 100.0


def _as_series(rows: List[Dict[str, float]], key: str) -> List[float]:
    return [float(r.get(key, 0.0) or 0.0) for r in rows]


def _funding_level(funding_rate: float) -> str:
    fr = float(funding_rate)
    if fr >= 0.0015:
        return "extreme_positive"
    if fr >= 0.0010:
        return "positive"
    if fr <= -0.0008:
        return "extreme_negative"
    if fr <= -0.0004:
        return "negative"
    return "neutral"


async def get_exchange() -> Optional[Any]:
    if ccxt_async is None:
        print("[monitor] ccxt not installed, running REST-only mode")
        return None
    exchange = ccxt_async.binanceusdm(
        {
            "apiKey": os.getenv("BINANCE_API_KEY", ""),
            "secret": os.getenv("BINANCE_API_SECRET", ""),
            "enableRateLimit": True,
            "options": {"defaultType": "future"},
        }
    )
    try:
        await exchange.load_markets()
        return exchange
    except Exception:
        await exchange.close()
        raise


async def close_exchange(exchange: Optional[Any]) -> None:
    if exchange is None:
        return
    try:
        await exchange.close()
    except Exception as exc:
        print(f"[monitor] exchange close error: {exc!r}")


async def _fetch_snapshot(symbol: str) -> Dict[str, Any]:
    s = normalize_symbol(symbol)
    base_asset = s[:-4] if s.endswith("USDT") else s

    url_k15 = _q(f"{BINANCE_FAPI}/fapi/v1/klines", {"symbol": s, "interval": "15m", "limit": 180})
    url_k1 = _q(f"{BINANCE_FAPI}/fapi/v1/klines", {"symbol": s, "interval": "1m", "limit": 60})
    url_oi = _q(f"{BINANCE_FAPI}/futures/data/openInterestHist", {"symbol": s, "period": "5m", "limit": 60})
    url_funding = _q(f"{BINANCE_FAPI}/fapi/v1/fundingRate", {"symbol": s, "limit": 5})
    url_binance_btc = _q(f"{BINANCE_SPOT}/api/v3/ticker/price", {"symbol": "BTCUSDT"})
    url_coinbase_btc = f"{COINBASE_API}/products/BTC-USD/ticker"
    url_ticker = _q(f"{BINANCE_FAPI}/fapi/v1/ticker/24hr", {"symbol": s})

    k15_raw, k1_raw, oi_raw, funding_raw, bx_raw, cb_raw, tk_raw = await asyncio.gather(
        _aget_json(url_k15),
        _aget_json(url_k1),
        _aget_json(url_oi),
        _aget_json(url_funding),
        _aget_json(url_binance_btc),
        _aget_json(url_coinbase_btc),
        _aget_json(url_ticker),
        return_exceptions=True,
    )

    k15 = _parse_klines(k15_raw if not isinstance(k15_raw, Exception) else [])
    k1 = _parse_klines(k1_raw if not isinstance(k1_raw, Exception) else [])
    oi_hist = _parse_open_interest(oi_raw if not isinstance(oi_raw, Exception) else [])

    funding_rate = 0.0
    funding_rate_prev = 0.0
    funding_rate_delta = 0.0
    funding_rate_level = "neutral"
    if isinstance(funding_raw, list) and funding_raw:
        try:
            fr_rows = [
                (
                    int(x.get("fundingTime", 0) or 0),
                    float(x.get("fundingRate", 0.0) or 0.0),
                )
                for x in funding_raw
                if isinstance(x, dict)
            ]
            fr_rows = [x for x in fr_rows if x[0] >= 0]
            fr_rows.sort(key=lambda x: x[0])
            if fr_rows:
                funding_rate = float(fr_rows[-1][1])
                if len(fr_rows) >= 2:
                    funding_rate_prev = float(fr_rows[-2][1])
                    funding_rate_delta = funding_rate - funding_rate_prev
                funding_rate_level = _funding_level(funding_rate)
        except Exception:
            funding_rate = 0.0
            funding_rate_prev = 0.0
            funding_rate_delta = 0.0
            funding_rate_level = "neutral"

    cbp = 0.0
    try:
        bx = float((bx_raw if isinstance(bx_raw, dict) else {}).get("price", 0.0) or 0.0)
        cb = float((cb_raw if isinstance(cb_raw, dict) else {}).get("price", 0.0) or 0.0)
        if bx > 0:
            cbp = (cb - bx) / bx
    except Exception:
        cbp = 0.0

    oi_series = [float(x["oi"]) for x in oi_hist if x.get("oi", 0.0) > 0]
    oi_change_pct = 0.0
    oi_change_pct_fast = 0.0
    if len(oi_series) >= 2:
        oi_change_pct_fast = _percent_change(oi_series[-1], oi_series[-2])
    if len(oi_series) >= 13:
        oi_change_pct = _percent_change(oi_series[-1], oi_series[-13])
    elif len(oi_series) >= 2:
        oi_change_pct = _percent_change(oi_series[-1], oi_series[0])

    last_price = 0.0
    try:
        last_price = float((tk_raw if isinstance(tk_raw, dict) else {}).get("lastPrice", 0.0) or 0.0)
    except Exception:
        last_price = 0.0
    if last_price <= 0 and k15:
        last_price = float(k15[-1]["close"])

    return {
        "symbol": s,
        "base_asset": base_asset,
        "k15": k15,
        "k1": k1,
        "oi_hist": oi_hist,
        "oi_change_pct": oi_change_pct,
        "oi_change_pct_fast": oi_change_pct_fast,
        "funding_rate": funding_rate,
        "funding_rate_prev": funding_rate_prev,
        "funding_rate_delta": funding_rate_delta,
        "funding_rate_level": funding_rate_level,
        "cbp": cbp,
        "last_price": last_price,
    }


def _build_signal_input(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    k15 = snapshot.get("k15", [])
    k1 = snapshot.get("k1", [])
    return {
        "close": _as_series(k15, "close"),
        "high": _as_series(k15, "high"),
        "low": _as_series(k15, "low"),
        "quote_volume": _as_series(k15, "quote_volume"),
        "base_volume": _as_series(k15, "base_volume"),
        "trades": _as_series(k15, "trades"),
        "taker_buy_quote": _as_series(k15, "taker_buy_quote"),
        "close_1m": _as_series(k1, "close"),
        "quote_volume_1m": _as_series(k1, "quote_volume"),
        "oi_change_pct": float(snapshot.get("oi_change_pct", 0.0)),
        "oi_change_pct_fast": float(snapshot.get("oi_change_pct_fast", 0.0)),
        "funding_rate": float(snapshot.get("funding_rate", 0.0)),
        "funding_rate_delta": float(snapshot.get("funding_rate_delta", 0.0)),
        "funding_rate_level": str(snapshot.get("funding_rate_level", "neutral")),
        "cbp": float(snapshot.get("cbp", 0.0)),
    }


def _signals_to_dict(signals: ModuleSignals) -> Dict[str, bool]:
    return {
        "squeeze": signals.squeeze,
        "breakout": signals.breakout,
        "momentum": signals.momentum,
        "acceleration": signals.acceleration,
        "vol_surge": signals.vol_surge,
        "volume_flow": signals.volume_flow,
        "order_flow_buy": signals.order_flow_buy,
        "order_flow_sell": signals.order_flow_sell,
        "smart_money": signals.smart_money,
        "lurking": signals.lurking,
        "black_horse": signals.black_horse,
        "vp_vacuum": signals.vp_vacuum,
        "sniper_a": signals.sniper_a,
        "sniper_b": signals.sniper_b,
        "trigger_1m": signals.trigger_1m,
        "essential_ok": signals.essential_ok,
    }


async def fetch_data_and_analyze(
    exchange: Optional[Any],
    symbol: str,
    *,
    data_dir: Path = Path("data"),
    mode: str = "composite",
    advanced_modules: Optional[AdvancedModuleConfig] = None,
) -> Dict[str, Any]:
    """
    Composite analysis entry used by websocket workers.

    `exchange` is kept for interface compatibility and optional future use.
    """
    _ = exchange
    _ = data_dir
    started = time.time()

    snapshot = await _fetch_snapshot(symbol)
    signal_input = _build_signal_input(snapshot)
    signals, extra = compute_signals(signal_input)
    adv_cfg = advanced_modules if isinstance(advanced_modules, AdvancedModuleConfig) else AdvancedModuleConfig()
    advanced = run_advanced_modules(snapshot, signal_input, adv_cfg)

    level = str(extra.get("level", "none"))
    should_alert = bool(extra.get("alert", False))
    watch_alert = bool(extra.get("watch_alert", False))
    if not should_alert and watch_alert:
        level = "WATCH"
        should_alert = True
    score = float(extra.get("score_final", 0.0))
    threshold = float(extra.get("threshold", 60.0))
    side = "long" if signals.order_flow_buy else ("short" if signals.order_flow_sell else "neutral")

    result: Dict[str, Any] = {
        "symbol": snapshot.get("symbol"),
        "mode": mode,
        "last_price": snapshot.get("last_price"),
        "side": side,
        "score": score,
        "threshold": threshold,
        "alert": should_alert,
        "watch_alert": watch_alert,
        "level": level,
        "signals": _signals_to_dict(signals),
        "metrics": extra,
        "advanced": advanced,
        "analysis_ms": int((time.time() - started) * 1000),
    }
    print(
        f"[analysis] {result['symbol']} level={result['level']} score={result['score']:.2f} "
        f"threshold={result['threshold']:.2f} side={result['side']}"
    )
    return result
