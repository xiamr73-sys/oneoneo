#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Backtest ignition pre-pump detector with denoise filters."""

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
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Sequence, Tuple

import numpy as np


UA = {"User-Agent": "ignition-backtest/2.0"}
BINANCE_FAPI_HOSTS: Tuple[str, ...] = (
    "fapi.binance.com",
    "fapi1.binance.com",
    "fapi2.binance.com",
    "fapi3.binance.com",
)

CHAIN_ID_TO_GECKO_NETWORK: Dict[str, str] = {
    "ethereum": "eth",
    "eth": "eth",
    "bsc": "bsc",
    "binance": "bsc",
    "base": "base",
    "arbitrum": "arbitrum",
    "polygon": "polygon_pos",
    "optimism": "optimism",
    "avax": "avax",
    "solana": "solana",
}

STABLE_QUOTES = {"USDT", "USDC", "FDUSD", "BUSD", "DAI", "USDE", "TUSD"}


@dataclass(frozen=True)
class RuleConfig:
    strategy_version: str = "1.1"
    # ignition base
    ret_1m_pct: float = 0.18
    vol_ratio_1m: float = 1.6
    cvd_z: float = 1.8
    cvd_use_decayed: bool = True
    cvd_decay_tau: float = 6.0
    oi5m_pct: float = 0.4
    min_signals: int = 2
    bear_veto_enabled: bool = True
    bear_veto_cvd_z: float = -1.5
    bear_veto_oi5m: float = 0.5
    cooldown_min: int = 3
    # S/R filter
    sr_enabled: bool = True
    sr_lookback_5m: int = 72
    sr_breakout_dist_pct: float = 1.8
    sr_support_dist_pct: float = 1.5
    sr_max_compression_pct: float = 4.5
    sr_soft_pass_enabled: bool = True
    sr_soft_min_hit: int = 3
    sr_soft_min_oi5m_pct: float = 1.0
    # cross-exchange filter
    cross_enabled: bool = True
    cross_ret_1m_pct: float = 0.08
    cross_vol_ratio_1m: float = 1.2
    cross_grace_no_data: bool = True
    cross_min_points: int = 3000
    # free flow filter (public endpoints only)
    flow_enabled: bool = True
    flow_buy_ratio_min: float = 0.54
    flow_imbalance_min: float = 0.05
    flow_lsr_5m_min_pct: float = 0.3
    flow_min_signals: int = 2
    flow_grace_no_data: bool = True
    flow_relax_enabled: bool = True
    flow_relax_ret_1m_pct: float = 0.35
    flow_relax_oi5m_pct: float = 0.8
    flow_relax_min_signals: int = 1
    # onchain filter
    onchain_enabled: bool = True
    onchain_ret_5m_pct: float = 0.25
    onchain_vol_ratio_5m: float = 1.8
    onchain_min_signals: int = 1
    onchain_grace_no_data: bool = True

    # adaptive return threshold
    dynamic_ret_enabled: bool = True
    dynamic_ret_lookback: int = 240
    dynamic_ret_quantile: float = 0.90
    dynamic_ret_anchor_pct: float = 0.25
    dynamic_ret_min_scale: float = 0.65
    dynamic_ret_max_scale: float = 1.35
    dynamic_ret_floor_pct: float = 0.06

    # fake breakout guard
    fake_breakout_enabled: bool = True
    fake_breakout_ret_1m_pct: float = 0.30
    fake_breakout_min_vol_ratio_1m: float = 1.05
    fake_breakout_min_taker_buy_ratio: float = 0.50
    fake_breakout_min_buy_imbalance: float = 0.0
    fake_breakout_max_oi5m_pct: float = 0.2
    fake_breakout_min_flags: int = 3


def _to_bool(v: str) -> bool:
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _http_get_json(url: str, timeout: float = 20.0) -> Any:
    urls: List[str] = [url]
    if "fapi.binance.com" in url:
        for host in BINANCE_FAPI_HOSTS[1:]:
            urls.append(url.replace("fapi.binance.com", host))

    last_exc: Optional[Exception] = None
    for idx, u in enumerate(urls):
        try:
            req = urllib.request.Request(u, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8")
                try:
                    return json.loads(body)
                except Exception as exc:
                    last_exc = exc
                    if idx < len(urls) - 1:
                        time.sleep(0.25)
                        continue
                    raise
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code in {418, 429, 451, 500, 502, 503, 504} and idx < len(urls) - 1:
                time.sleep(0.25)
                continue
            raise
        except urllib.error.URLError as exc:
            last_exc = exc
            if idx < len(urls) - 1:
                time.sleep(0.25)
                continue
            raise
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("http get failed without exception")


def _q(base: str, params: Dict[str, Any]) -> str:
    return f"{base}?{urllib.parse.urlencode(params)}"


def _zscore(x: float, values: Sequence[float]) -> float:
    if not values:
        return 0.0
    m = float(statistics.mean(values))
    s = float(statistics.pstdev(values))
    if s <= 1e-12:
        return 0.0
    return (x - m) / s


def _calc_cvd_zscores(cvd_series: Sequence[float], decay_tau: float) -> Tuple[float, float]:
    arr = np.asarray(cvd_series, dtype=np.float64)
    if arr.size < 8:
        return 0.0, 0.0

    base = arr[:-1]
    base_std = float(np.std(base))
    simple_z = 0.0
    if base_std > 1e-9:
        simple_z = float((arr[-1] - float(np.mean(base))) / base_std)

    tau = max(1e-6, float(decay_tau))
    ages = np.arange(arr.size - 1, -1, -1, dtype=np.float64)
    decay_weights = np.exp(-ages / tau)
    decayed = arr * decay_weights
    decayed_base = decayed[:-1]
    decayed_std = float(np.std(decayed_base))
    decayed_z = 0.0
    if decayed_std > 1e-9:
        decayed_z = float((decayed[-1] - float(np.mean(decayed_base))) / decayed_std)
    return simple_z, decayed_z


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int, limit: int = 1500) -> List[List[Any]]:
    out: List[List[Any]] = []
    step_ms = {"1m": 60_000, "5m": 300_000}[interval]
    cursor = start_ms
    for _ in range(80):
        url = _q(
            "https://fapi.binance.com/fapi/v1/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": limit,
            },
        )
        rows = _http_get_json(url)
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
    return out


def fetch_oi_hist(symbol: str, start_ms: int, end_ms: int) -> List[Tuple[int, float]]:
    out: List[Tuple[int, float]] = []
    cursor = start_ms
    for _ in range(40):
        url = _q(
            "https://fapi.binance.com/futures/data/openInterestHist",
            {
                "symbol": symbol,
                "period": "5m",
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
                oi = float(r.get("sumOpenInterestValue", r.get("sumOpenInterest", 0.0)) or 0.0)
            except Exception:
                continue
            if oi > 0:
                out.append((ts, oi))
        nxt = int(float(rows[-1].get("timestamp", 0))) + 1
        if nxt <= cursor:
            break
        cursor = nxt
        if len(rows) < 500:
            break
    dedup: List[Tuple[int, float]] = []
    seen = set()
    for ts, oi in sorted(out, key=lambda x: x[0]):
        if ts not in seen:
            seen.add(ts)
            dedup.append((ts, oi))
    return dedup


def fetch_global_long_short_ratio_hist(symbol: str, start_ms: int, end_ms: int) -> List[Tuple[int, float]]:
    out: List[Tuple[int, float]] = []
    cursor = start_ms
    for _ in range(40):
        url = _q(
            "https://fapi.binance.com/futures/data/globalLongShortAccountRatio",
            {
                "symbol": symbol,
                "period": "5m",
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
                ratio = float(r.get("longShortRatio", 0.0) or 0.0)
            except Exception:
                continue
            if ratio > 0:
                out.append((ts, ratio))
        nxt = int(float(rows[-1].get("timestamp", 0))) + 1
        if nxt <= cursor:
            break
        cursor = nxt
        if len(rows) < 500:
            break
    dedup: List[Tuple[int, float]] = []
    seen = set()
    for ts, ratio in sorted(out, key=lambda x: x[0]):
        if ts not in seen:
            seen.add(ts)
            dedup.append((ts, ratio))
    return dedup


def fetch_bybit_klines(symbol: str, start_ms: int, end_ms: int) -> List[Tuple[int, float, float]]:
    out: List[Tuple[int, float, float]] = []
    cursor = start_ms
    for _ in range(80):
        url = _q(
            "https://api.bybit.com/v5/market/kline",
            {
                "category": "linear",
                "symbol": symbol,
                "interval": "1",
                "start": cursor,
                "end": end_ms,
                "limit": 1000,
            },
        )
        raw = _http_get_json(url, timeout=18.0)
        rows = (((raw or {}).get("result", {}) or {}).get("list", [])) if isinstance(raw, dict) else []
        if not isinstance(rows, list) or not rows:
            break
        parsed: List[Tuple[int, float, float]] = []
        for r in rows:
            if not isinstance(r, list) or len(r) < 7:
                continue
            try:
                ts = int(r[0])
                close = float(r[4])
                qv = float(r[6])
            except Exception:
                continue
            if start_ms <= ts <= end_ms:
                parsed.append((ts, close, qv))
        parsed = sorted(parsed, key=lambda x: x[0])
        for x in parsed:
            if not out or x[0] > out[-1][0]:
                out.append(x)
        if not parsed:
            break
        nxt = parsed[-1][0] + 60_000
        if nxt <= cursor:
            break
        cursor = nxt
        if cursor > end_ms or len(rows) < 1000:
            break
    return out


def fetch_mexc_klines(symbol: str, start_ms: int, end_ms: int) -> List[Tuple[int, float, float]]:
    out: List[Tuple[int, float, float]] = []
    cursor = start_ms
    for _ in range(80):
        url = _q(
            "https://api.mexc.com/api/v3/klines",
            {
                "symbol": symbol,
                "interval": "1m",
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1000,
            },
        )
        rows = _http_get_json(url, timeout=18.0)
        if not isinstance(rows, list) or not rows:
            break
        parsed: List[Tuple[int, float, float]] = []
        for r in rows:
            if not isinstance(r, list) or len(r) < 8:
                continue
            try:
                ts = int(r[0])
                close = float(r[4])
                qv = float(r[7])
            except Exception:
                continue
            if start_ms <= ts <= end_ms:
                parsed.append((ts, close, qv))
        parsed = sorted(parsed, key=lambda x: x[0])
        for x in parsed:
            if not out or x[0] > out[-1][0]:
                out.append(x)
        if not parsed:
            break
        nxt = parsed[-1][0] + 60_000
        if nxt <= cursor:
            break
        cursor = nxt
        if cursor > end_ms or len(rows) < 1000:
            break
    return out


def discover_onchain_pool(base_asset: str, min_liquidity_usd: float = 20_000.0) -> Optional[Dict[str, Any]]:
    query = _q("https://api.dexscreener.com/latest/dex/search", {"q": base_asset})
    raw = _http_get_json(query, timeout=12.0)
    pairs = (raw or {}).get("pairs", []) if isinstance(raw, dict) else []
    if not isinstance(pairs, list):
        return None

    base_sym = base_asset.upper()
    best: Optional[Dict[str, Any]] = None
    best_key: Tuple[int, float, float] = (-1, -1.0, -1.0)
    for p in pairs:
        if not isinstance(p, dict):
            continue
        b = (p.get("baseToken") or {}).get("symbol", "")
        q = (p.get("quoteToken") or {}).get("symbol", "")
        if str(b).upper() != base_sym:
            continue
        liq = float((p.get("liquidity") or {}).get("usd", 0.0) or 0.0)
        if liq < min_liquidity_usd:
            continue
        chain_id = str(p.get("chainId", "")).lower()
        network = CHAIN_ID_TO_GECKO_NETWORK.get(chain_id)
        if not network:
            continue
        vol_h24 = float((p.get("volume") or {}).get("h24", 0.0) or 0.0)
        quote_is_stable = 1 if str(q).upper() in STABLE_QUOTES else 0
        key = (quote_is_stable, liq, vol_h24)
        if key > best_key:
            best_key = key
            best = {
                "network": network,
                "chain_id": chain_id,
                "pool_address": str(p.get("pairAddress", "")),
                "pair_url": str(p.get("url", "")),
                "dex_id": str(p.get("dexId", "")),
                "base_symbol": str(b).upper(),
                "quote_symbol": str(q).upper(),
                "liquidity_usd": liq,
                "volume_h24_usd": vol_h24,
            }
    return best


def fetch_gecko_ohlcv_5m(network: str, pool_address: str, start_ms: int, end_ms: int) -> List[Tuple[int, float, float, float, float, float]]:
    out: List[Tuple[int, float, float, float, float, float]] = []
    before_ts = end_ms // 1000 + 300
    for _ in range(40):
        url = _q(
            f"https://api.geckoterminal.com/api/v2/networks/{network}/pools/{pool_address}/ohlcv/minute",
            {"aggregate": 5, "limit": 1000, "before_timestamp": before_ts},
        )
        raw = _http_get_json(url, timeout=18.0)
        rows = ((((raw or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list", [])) if isinstance(raw, dict) else []
        if not isinstance(rows, list) or not rows:
            break
        oldest = 10**18
        for r in rows:
            if not isinstance(r, list) or len(r) < 6:
                continue
            try:
                ts_sec = int(r[0])
                o = float(r[1])
                h = float(r[2])
                l = float(r[3])
                c = float(r[4])
                v = float(r[5])
            except Exception:
                continue
            oldest = min(oldest, ts_sec)
            ts_ms = ts_sec * 1000
            if start_ms <= ts_ms <= end_ms:
                out.append((ts_ms, o, h, l, c, v))
        if oldest == 10**18:
            break
        if oldest * 1000 <= start_ms or len(rows) < 1000:
            break
        before_ts = oldest - 1

    dedup: Dict[int, Tuple[int, float, float, float, float, float]] = {}
    for row in out:
        dedup[int(row[0])] = row
    return [dedup[k] for k in sorted(dedup.keys())]


def _build_1m_feature_map(rows: Sequence[Tuple[int, float, float]]) -> Tuple[List[int], Dict[int, Dict[str, float]]]:
    keys: List[int] = []
    fmap: Dict[int, Dict[str, float]] = {}
    for i in range(1, len(rows)):
        ts, close, qv = rows[i]
        prev_close = rows[i - 1][1]
        ret = (close - prev_close) / prev_close * 100.0 if prev_close > 0 else 0.0
        hist = [x[2] for x in rows[max(0, i - 20):i]]
        vol_ratio = qv / float(statistics.mean(hist)) if hist and statistics.mean(hist) > 0 else 0.0
        keys.append(ts)
        fmap[ts] = {"ret_1m_pct": ret, "vol_ratio_1m": vol_ratio}
    return keys, fmap


def _build_5m_onchain_feature_map(rows: Sequence[Tuple[int, float, float, float, float, float]]) -> Tuple[List[int], Dict[int, Dict[str, float]]]:
    keys: List[int] = []
    fmap: Dict[int, Dict[str, float]] = {}
    for i in range(1, len(rows)):
        ts, _, _, _, close, vol = rows[i]
        prev_close = rows[i - 1][4]
        ret = (close - prev_close) / prev_close * 100.0 if prev_close > 0 else 0.0
        hist = [x[5] for x in rows[max(0, i - 24):i]]
        vol_ratio = vol / float(statistics.mean(hist)) if hist and statistics.mean(hist) > 0 else 0.0
        keys.append(ts)
        fmap[ts] = {"ret_5m_pct": ret, "vol_ratio_5m": vol_ratio}
    return keys, fmap


def _feature_at_or_before(ts: int, keys: Sequence[int], fmap: Dict[int, Dict[str, float]]) -> Optional[Dict[str, float]]:
    if not keys:
        return None
    i = bisect_right(keys, ts) - 1
    if i < 0:
        return None
    return fmap.get(int(keys[i]))


def detect_pump_events(k5: Sequence[List[Any]], threshold_pct: float, min_gap_min: int) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    min_gap_ms = min_gap_min * 60_000
    for i in range(1, len(k5)):
        t = int(k5[i][0])
        prev_close = float(k5[i - 1][4])
        close = float(k5[i][4])
        if prev_close <= 0:
            continue
        ret = (close - prev_close) / prev_close * 100.0
        if ret >= threshold_pct:
            if events and t - int(events[-1]["ts"]) < min_gap_ms:
                if ret > float(events[-1]["ret_5m_pct"]):
                    events[-1] = {"ts": t, "ret_5m_pct": ret}
                continue
            events.append({"ts": t, "ret_5m_pct": ret})
    return events


def build_ignition_signals(
    k1: Sequence[List[Any]],
    k5: Sequence[List[Any]],
    oi_rows: Sequence[Tuple[int, float]],
    lsr_rows: Sequence[Tuple[int, float]],
    bybit_rows: Sequence[Tuple[int, float, float]],
    mexc_rows: Sequence[Tuple[int, float, float]],
    onchain_rows: Sequence[Tuple[int, float, float, float, float, float]],
    cfg: RuleConfig,
) -> List[Dict[str, Any]]:
    signals: List[Dict[str, Any]] = []
    cooldown_ms = cfg.cooldown_min * 60_000
    last_sig_ts = -10**18
    ret_abs_history: Deque[float] = deque()

    oi_ts = [x[0] for x in oi_rows]
    oi_vals = [x[1] for x in oi_rows]
    lsr_ts = [x[0] for x in lsr_rows]
    lsr_vals = [x[1] for x in lsr_rows]

    def oi_at_or_before(ts: int) -> float:
        idx = bisect_right(oi_ts, ts) - 1
        if idx < 0:
            return 0.0
        return float(oi_vals[idx])

    def lsr_at_or_before(ts: int) -> float:
        idx = bisect_right(lsr_ts, ts) - 1
        if idx < 0:
            return 0.0
        return float(lsr_vals[idx])

    k5_ts = [int(x[0]) for x in k5]
    k5_close = [float(x[4]) for x in k5]
    k5_high = [float(x[2]) for x in k5]
    k5_low = [float(x[3]) for x in k5]

    bybit_keys, bybit_fmap = _build_1m_feature_map(bybit_rows)
    mexc_keys, mexc_fmap = _build_1m_feature_map(mexc_rows)
    bybit_usable = len(bybit_rows) >= cfg.cross_min_points
    mexc_usable = len(mexc_rows) >= cfg.cross_min_points
    onchain_keys, onchain_fmap = _build_5m_onchain_feature_map(onchain_rows)

    for i in range(1, len(k1)):
        ts = int(k1[i][0])
        close = float(k1[i][4])
        prev_close = float(k1[i - 1][4])
        qv = float(k1[i][7])
        tbq = float(k1[i][10])
        cvd_delta = 2.0 * tbq - qv

        ret_1m = (close - prev_close) / prev_close * 100.0 if prev_close > 0 else 0.0
        dyn_ret_threshold = float(cfg.ret_1m_pct)
        dyn_ret_scale = 1.0
        dyn_ret_baseline = 0.0
        if cfg.dynamic_ret_enabled and len(ret_abs_history) >= 20:
            arr = np.asarray(list(ret_abs_history)[-max(30, int(cfg.dynamic_ret_lookback)):], dtype=np.float64)
            if arr.size > 0:
                dyn_ret_baseline = float(np.quantile(np.abs(arr), float(cfg.dynamic_ret_quantile)))
                anchor = max(0.01, float(cfg.dynamic_ret_anchor_pct))
                dyn_ret_scale = dyn_ret_baseline / anchor if anchor > 0 else 1.0
                dyn_ret_scale = max(float(cfg.dynamic_ret_min_scale), min(float(cfg.dynamic_ret_max_scale), dyn_ret_scale))
                dyn_ret_threshold = max(float(cfg.dynamic_ret_floor_pct), float(cfg.ret_1m_pct) * dyn_ret_scale)
        ret_abs_history.append(abs(ret_1m))
        keep = max(30, int(cfg.dynamic_ret_lookback))
        while len(ret_abs_history) > keep:
            ret_abs_history.popleft()

        qv_hist = [float(x[7]) for x in k1[max(0, i - 20):i]]
        vol_ratio = qv / float(statistics.mean(qv_hist)) if qv_hist and statistics.mean(qv_hist) > 0 else 0.0
        cvd_series = []
        for x in k1[max(0, i - 34): i + 1]:
            q = float(x[7])
            b = float(x[10])
            cvd_series.append(2.0 * b - q)
        cvd_z_simple, cvd_z_decayed = _calc_cvd_zscores(cvd_series, cfg.cvd_decay_tau)
        cvd_z_rule = cvd_z_decayed if cfg.cvd_use_decayed else cvd_z_simple

        oi_now = oi_at_or_before(ts)
        oi_prev = oi_at_or_before(ts - 5 * 60_000)
        oi5 = (oi_now - oi_prev) / oi_prev * 100.0 if oi_prev > 0 else 0.0

        c_ret = ret_1m >= dyn_ret_threshold
        c_vol = vol_ratio >= cfg.vol_ratio_1m
        c_cvd = cvd_z_rule >= cfg.cvd_z
        c_oi = oi5 >= cfg.oi5m_pct
        hit = int(c_ret) + int(c_vol) + int(c_cvd) + int(c_oi)
        if hit < cfg.min_signals:
            continue

        bear_veto = False
        if cfg.bear_veto_enabled and oi5 >= cfg.bear_veto_oi5m and cvd_z_rule <= cfg.bear_veto_cvd_z:
            bear_veto = True
        if bear_veto:
            continue

        # --- Free flow filter (public-only metrics) ---
        taker_buy_ratio = (tbq / qv) if qv > 1e-9 else 0.0
        buy_imbalance = ((2.0 * tbq - qv) / qv) if qv > 1e-9 else 0.0
        lsr_now = lsr_at_or_before(ts)
        lsr_prev = lsr_at_or_before(ts - 5 * 60_000)
        lsr_has_data = lsr_now > 0 and lsr_prev > 0
        lsr_5m_chg_pct = ((lsr_now - lsr_prev) / lsr_prev * 100.0) if lsr_has_data else 0.0
        flow_hit = int(taker_buy_ratio >= cfg.flow_buy_ratio_min) + int(buy_imbalance >= cfg.flow_imbalance_min)
        if lsr_has_data:
            flow_hit += int(lsr_5m_chg_pct >= cfg.flow_lsr_5m_min_pct)
        flow_required = max(1, min(3, cfg.flow_min_signals))
        if not lsr_has_data and cfg.flow_grace_no_data:
            flow_required = min(flow_required, 2)
        flow_relaxed = 0
        if cfg.flow_relax_enabled and (ret_1m >= cfg.flow_relax_ret_1m_pct or oi5 >= cfg.flow_relax_oi5m_pct):
            flow_required = min(flow_required, max(1, min(3, int(cfg.flow_relax_min_signals))))
            flow_relaxed = 1
        flow_ok = flow_hit >= flow_required
        if cfg.flow_enabled and not flow_ok:
            continue

        # --- S/R filter (5m structure) ---
        sr_ok = True
        sr_soft_pass = False
        sr_breakout = False
        sr_support = False
        dist_res = 999.0
        dist_sup = 999.0
        compression = 999.0
        if cfg.sr_enabled:
            sr_ok = False
            j = bisect_right(k5_ts, ts) - 1
            if j > 12:
                ref_start = max(0, j - cfg.sr_lookback_5m)
                highs = k5_high[ref_start:j]
                lows = k5_low[ref_start:j]
                close5 = k5_close[j]
                if highs and lows and close5 > 0:
                    res = max(highs)
                    sup = min(lows)
                    dist_res = (res - close5) / close5 * 100.0
                    dist_sup = (close5 - sup) / close5 * 100.0
                    c_start = max(0, j - 12)
                    comp_hi = max(k5_high[c_start:j]) if j > c_start else close5
                    comp_lo = min(k5_low[c_start:j]) if j > c_start else close5
                    compression = (comp_hi - comp_lo) / close5 * 100.0 if close5 > 0 else 999.0
                    sr_breakout = (
                        0.0 <= dist_res <= cfg.sr_breakout_dist_pct and compression <= cfg.sr_max_compression_pct
                    )
                    sr_support = (
                        0.0 <= dist_sup <= cfg.sr_support_dist_pct and compression <= cfg.sr_max_compression_pct * 1.3
                    )
                    sr_ok = bool(sr_breakout or sr_support)
            if (not sr_ok) and cfg.sr_soft_pass_enabled and hit >= cfg.sr_soft_min_hit and oi5 >= cfg.sr_soft_min_oi5m_pct:
                sr_ok = True
                sr_soft_pass = True
            if not sr_ok:
                continue

        # --- Cross-exchange filter (Bybit 1m) ---
        bybit_ret = 0.0
        bybit_vol = 0.0
        mexc_ret = 0.0
        mexc_vol = 0.0
        cross_available = False
        cross_ok = True
        if cfg.cross_enabled:
            bybit_cross = _feature_at_or_before(ts, bybit_keys, bybit_fmap) if bybit_usable else None
            mexc_cross = _feature_at_or_before(ts, mexc_keys, mexc_fmap) if mexc_usable else None
            bybit_hit = False
            mexc_hit = False
            if bybit_cross is not None:
                bybit_ret = float(bybit_cross.get("ret_1m_pct", 0.0))
                bybit_vol = float(bybit_cross.get("vol_ratio_1m", 0.0))
                bybit_hit = bybit_ret >= cfg.cross_ret_1m_pct and bybit_vol >= cfg.cross_vol_ratio_1m
            if mexc_cross is not None:
                mexc_ret = float(mexc_cross.get("ret_1m_pct", 0.0))
                mexc_vol = float(mexc_cross.get("vol_ratio_1m", 0.0))
                mexc_hit = mexc_ret >= cfg.cross_ret_1m_pct and mexc_vol >= cfg.cross_vol_ratio_1m
            cross_available = (bybit_cross is not None) or (mexc_cross is not None)
            if not cross_available:
                cross_ok = cfg.cross_grace_no_data
            else:
                cross_ok = bool(bybit_hit or mexc_hit)
            if not cross_ok:
                continue

        # --- Onchain filter (DEX 5m) ---
        onchain_ret = 0.0
        onchain_vol = 0.0
        onchain_hit = 0
        onchain_available = False
        onchain_ok = True
        if cfg.onchain_enabled:
            oc = _feature_at_or_before(ts, onchain_keys, onchain_fmap)
            onchain_available = oc is not None
            if oc is None:
                onchain_ok = cfg.onchain_grace_no_data
            else:
                onchain_ret = float(oc.get("ret_5m_pct", 0.0))
                onchain_vol = float(oc.get("vol_ratio_5m", 0.0))
                c_oc_ret = onchain_ret >= cfg.onchain_ret_5m_pct
                c_oc_vol = onchain_vol >= cfg.onchain_vol_ratio_5m
                onchain_hit = int(c_oc_ret) + int(c_oc_vol)
                onchain_ok = onchain_hit >= max(1, min(2, cfg.onchain_min_signals))
            if not onchain_ok:
                continue

        # --- Fake breakout guard ---
        fake_breakout = False
        fake_flags = 0
        fake_weak_vol = False
        fake_weak_oi = False
        fake_weak_flow = False
        fake_weak_cross = False
        fake_weak_sr = False
        if cfg.fake_breakout_enabled and ret_1m >= cfg.fake_breakout_ret_1m_pct:
            fake_weak_vol = vol_ratio < cfg.fake_breakout_min_vol_ratio_1m
            fake_weak_oi = oi5 <= cfg.fake_breakout_max_oi5m_pct
            fake_weak_flow = (
                taker_buy_ratio < cfg.fake_breakout_min_taker_buy_ratio
                or buy_imbalance < cfg.fake_breakout_min_buy_imbalance
            )
            fake_weak_cross = cfg.cross_enabled and cross_available and (not cross_ok)
            fake_weak_sr = cfg.sr_enabled and (not sr_breakout) and (not sr_support) and (not sr_soft_pass)
            fake_flags = (
                int(fake_weak_vol)
                + int(fake_weak_oi)
                + int(fake_weak_flow)
                + int(fake_weak_cross)
                + int(fake_weak_sr)
            )
            fake_breakout = fake_flags >= max(1, int(cfg.fake_breakout_min_flags))
            if fake_breakout:
                continue

        if ts - last_sig_ts < cooldown_ms:
            continue
        last_sig_ts = ts

        signals.append(
            {
                "ts": ts,
                "ret_1m_pct": ret_1m,
                "dyn_ret_threshold_pct": dyn_ret_threshold,
                "dyn_ret_scale": dyn_ret_scale,
                "dyn_ret_baseline_pct": dyn_ret_baseline,
                "vol_ratio_1m": vol_ratio,
                "cvd_z": cvd_z_rule,
                "cvd_z_rule": cvd_z_rule,
                "cvd_z_simple": cvd_z_simple,
                "cvd_z_decayed": cvd_z_decayed,
                "oi5m_pct": oi5,
                "hit_count": hit,
                "c_ret": int(c_ret),
                "c_vol": int(c_vol),
                "c_cvd": int(c_cvd),
                "c_oi": int(c_oi),
                "bear_veto": int(bear_veto),
                "flow_ok": int(flow_ok),
                "flow_hit": int(flow_hit),
                "flow_required": int(flow_required),
                "flow_relaxed": int(flow_relaxed),
                "flow_has_lsr": int(lsr_has_data),
                "taker_buy_ratio": taker_buy_ratio,
                "buy_imbalance": buy_imbalance,
                "lsr_5m_chg_pct": lsr_5m_chg_pct,
                "sr_ok": int(sr_ok),
                "sr_soft_pass": int(sr_soft_pass),
                "sr_breakout": int(sr_breakout),
                "sr_support": int(sr_support),
                "sr_dist_res_pct": dist_res,
                "sr_dist_sup_pct": dist_sup,
                "sr_compression_pct": compression,
                "cross_ok": int(cross_ok),
                "cross_available": int(cross_available),
                "bybit_ret_1m_pct": bybit_ret,
                "bybit_vol_ratio_1m": bybit_vol,
                "mexc_ret_1m_pct": mexc_ret,
                "mexc_vol_ratio_1m": mexc_vol,
                "onchain_ok": int(onchain_ok),
                "onchain_available": int(onchain_available),
                "onchain_hit": onchain_hit,
                "onchain_ret_5m_pct": onchain_ret,
                "onchain_vol_ratio_5m": onchain_vol,
                "fake_breakout": int(fake_breakout),
                "fake_flags": int(fake_flags),
                "fake_weak_vol": int(fake_weak_vol),
                "fake_weak_oi": int(fake_weak_oi),
                "fake_weak_flow": int(fake_weak_flow),
                "fake_weak_cross": int(fake_weak_cross),
                "fake_weak_sr": int(fake_weak_sr),
            }
        )
    return signals


def evaluate(signals: Sequence[Dict[str, Any]], events: Sequence[Dict[str, Any]], lead_window_min: int) -> Dict[str, Any]:
    lead_window_ms = lead_window_min * 60_000
    events_eval: List[Dict[str, Any]] = []
    leads: List[float] = []
    for ev in events:
        ev_ts = int(ev["ts"])
        pre = [s for s in signals if ev_ts - lead_window_ms <= int(s["ts"]) < ev_ts]
        if pre:
            first = min(pre, key=lambda x: int(x["ts"]))
            lead_min = (ev_ts - int(first["ts"])) / 60_000.0
            leads.append(lead_min)
            events_eval.append(
                {
                    "event_ts": ev_ts,
                    "event_ret_5m_pct": float(ev["ret_5m_pct"]),
                    "detected": True,
                    "lead_min": lead_min,
                    "first_signal_ts": int(first["ts"]),
                }
            )
        else:
            events_eval.append(
                {
                    "event_ts": ev_ts,
                    "event_ret_5m_pct": float(ev["ret_5m_pct"]),
                    "detected": False,
                    "lead_min": None,
                    "first_signal_ts": None,
                }
            )

    false_positives = 0
    for s in signals:
        s_ts = int(s["ts"])
        followed = any(s_ts < int(e["ts"]) <= s_ts + lead_window_ms for e in events)
        if not followed:
            false_positives += 1

    detection_rate = (sum(1 for x in events_eval if x["detected"]) / len(events_eval)) if events_eval else 0.0
    fp_rate = (false_positives / len(signals)) if signals else 0.0
    return {
        "events_eval": events_eval,
        "detection_rate": detection_rate,
        "false_positives": false_positives,
        "false_positive_rate": fp_rate,
        "lead_min_mean": float(statistics.mean(leads)) if leads else None,
        "lead_min_median": float(statistics.median(leads)) if leads else None,
        "lead_min_best": max(leads) if leads else None,
        "lead_min_worst": min(leads) if leads else None,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backtest ignition pre-pump detector")
    p.add_argument("--strategy-version", type=str, default="1.1")
    p.add_argument("--symbol", type=str, default="AINUSDT")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--event-threshold-5m", type=float, default=20.0)
    p.add_argument("--event-gap-min", type=int, default=30)
    p.add_argument("--lead-window-min", type=int, default=60)
    p.add_argument("--ret-1m-pct", type=float, default=0.18)
    p.add_argument("--vol-ratio-1m", type=float, default=1.6)
    p.add_argument("--cvd-z", type=float, default=1.8)
    p.add_argument("--cvd-use-decayed", type=_to_bool, default=True)
    p.add_argument("--cvd-decay-tau", type=float, default=6.0)
    p.add_argument("--oi5m-pct", type=float, default=0.4)
    p.add_argument("--min-signals", type=int, default=2)
    p.add_argument("--bear-veto-enabled", type=_to_bool, default=True)
    p.add_argument("--bear-veto-cvd-z", type=float, default=-1.5)
    p.add_argument("--bear-veto-oi5m", type=float, default=0.5)
    p.add_argument("--cooldown-min", type=int, default=3)
    p.add_argument("--sr-enabled", type=_to_bool, default=True)
    p.add_argument("--sr-lookback-5m", type=int, default=72)
    p.add_argument("--sr-breakout-dist-pct", type=float, default=1.8)
    p.add_argument("--sr-support-dist-pct", type=float, default=1.5)
    p.add_argument("--sr-max-compression-pct", type=float, default=4.5)
    p.add_argument("--sr-soft-pass-enabled", type=_to_bool, default=True)
    p.add_argument("--sr-soft-min-hit", type=int, default=3)
    p.add_argument("--sr-soft-min-oi5m-pct", type=float, default=1.0)
    p.add_argument("--cross-enabled", type=_to_bool, default=True)
    p.add_argument("--cross-ret-1m-pct", type=float, default=0.08)
    p.add_argument("--cross-vol-ratio-1m", type=float, default=1.2)
    p.add_argument("--cross-grace-no-data", type=_to_bool, default=True)
    p.add_argument("--cross-min-points", type=int, default=3000)
    p.add_argument("--flow-enabled", type=_to_bool, default=True)
    p.add_argument("--flow-buy-ratio-min", type=float, default=0.54)
    p.add_argument("--flow-imbalance-min", type=float, default=0.05)
    p.add_argument("--flow-lsr-5m-min-pct", type=float, default=0.3)
    p.add_argument("--flow-min-signals", type=int, default=2)
    p.add_argument("--flow-grace-no-data", type=_to_bool, default=True)
    p.add_argument("--flow-relax-enabled", type=_to_bool, default=True)
    p.add_argument("--flow-relax-ret-1m-pct", type=float, default=0.35)
    p.add_argument("--flow-relax-oi5m-pct", type=float, default=0.8)
    p.add_argument("--flow-relax-min-signals", type=int, default=1)
    p.add_argument("--onchain-enabled", type=_to_bool, default=True)
    p.add_argument("--onchain-ret-5m-pct", type=float, default=0.25)
    p.add_argument("--onchain-vol-ratio-5m", type=float, default=1.8)
    p.add_argument("--onchain-min-signals", type=int, default=1)
    p.add_argument("--onchain-grace-no-data", type=_to_bool, default=True)
    p.add_argument("--dynamic-ret-enabled", type=_to_bool, default=True)
    p.add_argument("--dynamic-ret-lookback", type=int, default=240)
    p.add_argument("--dynamic-ret-quantile", type=float, default=0.90)
    p.add_argument("--dynamic-ret-anchor-pct", type=float, default=0.25)
    p.add_argument("--dynamic-ret-min-scale", type=float, default=0.65)
    p.add_argument("--dynamic-ret-max-scale", type=float, default=1.35)
    p.add_argument("--dynamic-ret-floor-pct", type=float, default=0.06)
    p.add_argument("--fake-breakout-enabled", type=_to_bool, default=True)
    p.add_argument("--fake-breakout-ret-1m-pct", type=float, default=0.30)
    p.add_argument("--fake-breakout-min-vol-ratio-1m", type=float, default=1.05)
    p.add_argument("--fake-breakout-min-taker-buy-ratio", type=float, default=0.50)
    p.add_argument("--fake-breakout-min-buy-imbalance", type=float, default=0.0)
    p.add_argument("--fake-breakout-max-oi5m-pct", type=float, default=0.2)
    p.add_argument("--fake-breakout-min-flags", type=int, default=3)
    p.add_argument("--onchain-network", type=str, default="")
    p.add_argument("--onchain-pool-address", type=str, default="")
    p.add_argument("--onchain-min-liquidity-usd", type=float, default=20_000.0)
    p.add_argument("--out-dir", type=str, default="data/backtests")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    tz_bjt = timezone(timedelta(hours=8))
    end_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    start_ms = end_ms - args.days * 24 * 60 * 60 * 1000

    cfg = RuleConfig(
        strategy_version=str(args.strategy_version or "1.1").strip() or "1.1",
        ret_1m_pct=args.ret_1m_pct,
        vol_ratio_1m=args.vol_ratio_1m,
        cvd_z=args.cvd_z,
        cvd_use_decayed=bool(args.cvd_use_decayed),
        cvd_decay_tau=max(0.1, float(args.cvd_decay_tau)),
        oi5m_pct=args.oi5m_pct,
        min_signals=max(2, min(4, args.min_signals)),
        bear_veto_enabled=bool(args.bear_veto_enabled),
        bear_veto_cvd_z=float(args.bear_veto_cvd_z),
        bear_veto_oi5m=float(args.bear_veto_oi5m),
        cooldown_min=max(1, args.cooldown_min),
        sr_enabled=bool(args.sr_enabled),
        sr_lookback_5m=max(24, int(args.sr_lookback_5m)),
        sr_breakout_dist_pct=max(0.1, float(args.sr_breakout_dist_pct)),
        sr_support_dist_pct=max(0.1, float(args.sr_support_dist_pct)),
        sr_max_compression_pct=max(0.5, float(args.sr_max_compression_pct)),
        sr_soft_pass_enabled=bool(args.sr_soft_pass_enabled),
        sr_soft_min_hit=max(2, min(4, int(args.sr_soft_min_hit))),
        sr_soft_min_oi5m_pct=float(args.sr_soft_min_oi5m_pct),
        cross_enabled=bool(args.cross_enabled),
        cross_ret_1m_pct=float(args.cross_ret_1m_pct),
        cross_vol_ratio_1m=max(0.1, float(args.cross_vol_ratio_1m)),
        cross_grace_no_data=bool(args.cross_grace_no_data),
        cross_min_points=max(0, int(args.cross_min_points)),
        flow_enabled=bool(args.flow_enabled),
        flow_buy_ratio_min=min(1.0, max(0.0, float(args.flow_buy_ratio_min))),
        flow_imbalance_min=min(1.0, max(-1.0, float(args.flow_imbalance_min))),
        flow_lsr_5m_min_pct=float(args.flow_lsr_5m_min_pct),
        flow_min_signals=max(1, min(3, int(args.flow_min_signals))),
        flow_grace_no_data=bool(args.flow_grace_no_data),
        flow_relax_enabled=bool(args.flow_relax_enabled),
        flow_relax_ret_1m_pct=float(args.flow_relax_ret_1m_pct),
        flow_relax_oi5m_pct=float(args.flow_relax_oi5m_pct),
        flow_relax_min_signals=max(1, min(3, int(args.flow_relax_min_signals))),
        onchain_enabled=bool(args.onchain_enabled),
        onchain_ret_5m_pct=float(args.onchain_ret_5m_pct),
        onchain_vol_ratio_5m=max(0.1, float(args.onchain_vol_ratio_5m)),
        onchain_min_signals=max(1, min(2, int(args.onchain_min_signals))),
        onchain_grace_no_data=bool(args.onchain_grace_no_data),
        dynamic_ret_enabled=bool(args.dynamic_ret_enabled),
        dynamic_ret_lookback=max(30, int(args.dynamic_ret_lookback)),
        dynamic_ret_quantile=min(0.99, max(0.5, float(args.dynamic_ret_quantile))),
        dynamic_ret_anchor_pct=max(0.05, float(args.dynamic_ret_anchor_pct)),
        dynamic_ret_min_scale=max(0.2, float(args.dynamic_ret_min_scale)),
        dynamic_ret_max_scale=max(0.3, float(args.dynamic_ret_max_scale)),
        dynamic_ret_floor_pct=max(0.01, float(args.dynamic_ret_floor_pct)),
        fake_breakout_enabled=bool(args.fake_breakout_enabled),
        fake_breakout_ret_1m_pct=max(0.05, float(args.fake_breakout_ret_1m_pct)),
        fake_breakout_min_vol_ratio_1m=max(0.1, float(args.fake_breakout_min_vol_ratio_1m)),
        fake_breakout_min_taker_buy_ratio=min(1.0, max(0.0, float(args.fake_breakout_min_taker_buy_ratio))),
        fake_breakout_min_buy_imbalance=min(1.0, max(-1.0, float(args.fake_breakout_min_buy_imbalance))),
        fake_breakout_max_oi5m_pct=float(args.fake_breakout_max_oi5m_pct),
        fake_breakout_min_flags=max(1, min(5, int(args.fake_breakout_min_flags))),
    )

    k1 = fetch_klines(args.symbol, "1m", start_ms, end_ms)
    k5 = fetch_klines(args.symbol, "5m", start_ms, end_ms)
    oi_rows = fetch_oi_hist(args.symbol, start_ms, end_ms)
    lsr_rows = fetch_global_long_short_ratio_hist(args.symbol, start_ms, end_ms)
    bybit_rows = fetch_bybit_klines(args.symbol, start_ms, end_ms)
    mexc_rows = fetch_mexc_klines(args.symbol, start_ms, end_ms)

    onchain_pool: Optional[Dict[str, Any]] = None
    onchain_rows: List[Tuple[int, float, float, float, float, float]] = []
    if cfg.onchain_enabled:
        try:
            if str(args.onchain_network).strip() and str(args.onchain_pool_address).strip():
                onchain_pool = {
                    "network": str(args.onchain_network).strip(),
                    "pool_address": str(args.onchain_pool_address).strip(),
                    "chain_id": str(args.onchain_network).strip(),
                    "pair_url": "",
                    "dex_id": "manual",
                    "base_symbol": args.symbol.replace("USDT", ""),
                    "quote_symbol": "",
                    "liquidity_usd": None,
                    "volume_h24_usd": None,
                }
            else:
                base_asset = str(args.symbol).upper()
                for suffix in ("USDT", "USDC", "BUSD", "FDUSD"):
                    if base_asset.endswith(suffix):
                        base_asset = base_asset[: -len(suffix)]
                        break
                onchain_pool = discover_onchain_pool(base_asset, min_liquidity_usd=float(args.onchain_min_liquidity_usd))
            if onchain_pool:
                onchain_rows = fetch_gecko_ohlcv_5m(
                    str(onchain_pool["network"]),
                    str(onchain_pool["pool_address"]),
                    start_ms,
                    end_ms,
                )
        except Exception:
            onchain_pool = None
            onchain_rows = []

    signals = build_ignition_signals(k1, k5, oi_rows, lsr_rows, bybit_rows, mexc_rows, onchain_rows, cfg)
    events = detect_pump_events(k5, args.event_threshold_5m, args.event_gap_min)
    metrics = evaluate(signals, events, args.lead_window_min)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"{args.symbol}_ignition_{args.days}d_{stamp}"
    signals_csv = out_dir / f"{base}_signals.csv"
    events_csv = out_dir / f"{base}_events.csv"
    summary_json = out_dir / f"{base}_summary.json"

    with signals_csv.open("w", newline="", encoding="utf-8") as f:
        fields = [
            "ts",
            "time_bjt",
            "ret_1m_pct",
            "dyn_ret_threshold_pct",
            "dyn_ret_scale",
            "dyn_ret_baseline_pct",
            "vol_ratio_1m",
            "cvd_z",
            "cvd_z_rule",
            "cvd_z_simple",
            "cvd_z_decayed",
            "oi5m_pct",
            "hit_count",
            "c_ret",
            "c_vol",
            "c_cvd",
            "c_oi",
            "bear_veto",
            "flow_ok",
            "flow_hit",
            "flow_required",
            "flow_relaxed",
            "flow_has_lsr",
            "taker_buy_ratio",
            "buy_imbalance",
            "lsr_5m_chg_pct",
            "sr_ok",
            "sr_soft_pass",
            "sr_breakout",
            "sr_support",
            "sr_dist_res_pct",
            "sr_dist_sup_pct",
            "sr_compression_pct",
            "cross_ok",
            "cross_available",
            "bybit_ret_1m_pct",
            "bybit_vol_ratio_1m",
            "mexc_ret_1m_pct",
            "mexc_vol_ratio_1m",
            "onchain_ok",
            "onchain_available",
            "onchain_hit",
            "onchain_ret_5m_pct",
            "onchain_vol_ratio_5m",
            "fake_breakout",
            "fake_flags",
            "fake_weak_vol",
            "fake_weak_oi",
            "fake_weak_flow",
            "fake_weak_cross",
            "fake_weak_sr",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for s in signals:
            row = dict(s)
            row["time_bjt"] = datetime.fromtimestamp(int(s["ts"]) / 1000, tz=tz_bjt).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow(row)

    with events_csv.open("w", newline="", encoding="utf-8") as f:
        fields = ["event_ts", "event_time_bjt", "event_ret_5m_pct", "detected", "lead_min", "first_signal_ts", "first_signal_time_bjt"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for e in metrics["events_eval"]:
            row = dict(e)
            row["event_time_bjt"] = datetime.fromtimestamp(int(e["event_ts"]) / 1000, tz=tz_bjt).strftime("%Y-%m-%d %H:%M:%S")
            fs = e.get("first_signal_ts")
            row["first_signal_time_bjt"] = (
                datetime.fromtimestamp(int(fs) / 1000, tz=tz_bjt).strftime("%Y-%m-%d %H:%M:%S") if fs else ""
            )
            w.writerow(row)

    summary = {
        "symbol": args.symbol,
        "window_bjt": {
            "start": datetime.fromtimestamp(start_ms / 1000, tz=tz_bjt).isoformat(),
            "end": datetime.fromtimestamp(end_ms / 1000, tz=tz_bjt).isoformat(),
        },
        "rule_config": {
            "strategy_version": cfg.strategy_version,
            "ret_1m_pct": cfg.ret_1m_pct,
            "vol_ratio_1m": cfg.vol_ratio_1m,
            "cvd_z": cfg.cvd_z,
            "cvd_use_decayed": cfg.cvd_use_decayed,
            "cvd_decay_tau": cfg.cvd_decay_tau,
            "oi5m_pct": cfg.oi5m_pct,
            "min_signals": cfg.min_signals,
            "bear_veto_enabled": cfg.bear_veto_enabled,
            "bear_veto_cvd_z": cfg.bear_veto_cvd_z,
            "bear_veto_oi5m": cfg.bear_veto_oi5m,
            "cooldown_min": cfg.cooldown_min,
            "sr_enabled": cfg.sr_enabled,
            "sr_lookback_5m": cfg.sr_lookback_5m,
            "sr_breakout_dist_pct": cfg.sr_breakout_dist_pct,
            "sr_support_dist_pct": cfg.sr_support_dist_pct,
            "sr_max_compression_pct": cfg.sr_max_compression_pct,
            "sr_soft_pass_enabled": cfg.sr_soft_pass_enabled,
            "sr_soft_min_hit": cfg.sr_soft_min_hit,
            "sr_soft_min_oi5m_pct": cfg.sr_soft_min_oi5m_pct,
            "cross_enabled": cfg.cross_enabled,
            "cross_ret_1m_pct": cfg.cross_ret_1m_pct,
            "cross_vol_ratio_1m": cfg.cross_vol_ratio_1m,
            "cross_grace_no_data": cfg.cross_grace_no_data,
            "cross_min_points": cfg.cross_min_points,
            "flow_enabled": cfg.flow_enabled,
            "flow_buy_ratio_min": cfg.flow_buy_ratio_min,
            "flow_imbalance_min": cfg.flow_imbalance_min,
            "flow_lsr_5m_min_pct": cfg.flow_lsr_5m_min_pct,
            "flow_min_signals": cfg.flow_min_signals,
            "flow_grace_no_data": cfg.flow_grace_no_data,
            "flow_relax_enabled": cfg.flow_relax_enabled,
            "flow_relax_ret_1m_pct": cfg.flow_relax_ret_1m_pct,
            "flow_relax_oi5m_pct": cfg.flow_relax_oi5m_pct,
            "flow_relax_min_signals": cfg.flow_relax_min_signals,
            "onchain_enabled": cfg.onchain_enabled,
            "onchain_ret_5m_pct": cfg.onchain_ret_5m_pct,
            "onchain_vol_ratio_5m": cfg.onchain_vol_ratio_5m,
            "onchain_min_signals": cfg.onchain_min_signals,
            "onchain_grace_no_data": cfg.onchain_grace_no_data,
            "dynamic_ret_enabled": cfg.dynamic_ret_enabled,
            "dynamic_ret_lookback": cfg.dynamic_ret_lookback,
            "dynamic_ret_quantile": cfg.dynamic_ret_quantile,
            "dynamic_ret_anchor_pct": cfg.dynamic_ret_anchor_pct,
            "dynamic_ret_min_scale": cfg.dynamic_ret_min_scale,
            "dynamic_ret_max_scale": cfg.dynamic_ret_max_scale,
            "dynamic_ret_floor_pct": cfg.dynamic_ret_floor_pct,
            "fake_breakout_enabled": cfg.fake_breakout_enabled,
            "fake_breakout_ret_1m_pct": cfg.fake_breakout_ret_1m_pct,
            "fake_breakout_min_vol_ratio_1m": cfg.fake_breakout_min_vol_ratio_1m,
            "fake_breakout_min_taker_buy_ratio": cfg.fake_breakout_min_taker_buy_ratio,
            "fake_breakout_min_buy_imbalance": cfg.fake_breakout_min_buy_imbalance,
            "fake_breakout_max_oi5m_pct": cfg.fake_breakout_max_oi5m_pct,
            "fake_breakout_min_flags": cfg.fake_breakout_min_flags,
        },
        "event_definition": {
            "threshold_5m_pct": args.event_threshold_5m,
            "event_gap_min": args.event_gap_min,
            "lead_window_min": args.lead_window_min,
        },
        "data_source": {
            "lsr_5m_points": len(lsr_rows),
            "bybit_1m_points": len(bybit_rows),
            "mexc_1m_points": len(mexc_rows),
            "onchain_5m_points": len(onchain_rows),
            "onchain_pool": onchain_pool,
        },
        "counts": {
            "k1_bars": len(k1),
            "k5_bars": len(k5),
            "oi_points": len(oi_rows),
            "signals": len(signals),
            "events": len(events),
            "events_detected": sum(1 for x in metrics["events_eval"] if x["detected"]),
            "false_positives": metrics["false_positives"],
        },
        "metrics": {
            "detection_rate": metrics["detection_rate"],
            "false_positive_rate": metrics["false_positive_rate"],
            "lead_min_mean": metrics["lead_min_mean"],
            "lead_min_median": metrics["lead_min_median"],
            "lead_min_best": metrics["lead_min_best"],
            "lead_min_worst": metrics["lead_min_worst"],
        },
        "files": {
            "signals_csv": str(signals_csv),
            "events_csv": str(events_csv),
            "summary_json": str(summary_json),
        },
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
