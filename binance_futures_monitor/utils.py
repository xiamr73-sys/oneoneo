#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Signal utilities for Binance futures composite scoring."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np


@dataclass(frozen=True)
class ModuleSignals:
    squeeze: bool
    breakout: bool
    momentum: bool
    acceleration: bool
    vol_surge: bool
    volume_flow: bool
    order_flow_buy: bool
    order_flow_sell: bool
    smart_money: bool
    lurking: bool
    black_horse: bool
    vp_vacuum: bool
    sniper_a: bool
    sniper_b: bool
    trigger_1m: bool
    essential_ok: bool


def _arr(values: Sequence[float]) -> np.ndarray:
    return np.asarray(values, dtype=np.float64)


def sma(values: Sequence[float], period: int) -> float:
    a = _arr(values)
    if a.size == 0:
        return 0.0
    p = max(1, min(period, int(a.size)))
    return float(np.mean(a[-p:]))


def stddev(values: Sequence[float], period: int) -> float:
    a = _arr(values)
    if a.size == 0:
        return 0.0
    p = max(1, min(period, int(a.size)))
    return float(np.std(a[-p:]))


def ema(values: Sequence[float], period: int) -> float:
    a = _arr(values)
    if a.size == 0:
        return 0.0
    p = max(1, int(period))
    alpha = 2.0 / (p + 1.0)
    out = float(a[0])
    for x in a[1:]:
        out = alpha * float(x) + (1.0 - alpha) * out
    return out


def rsi(values: Sequence[float], period: int = 14) -> float:
    a = _arr(values)
    if a.size <= period:
        return 50.0
    d = np.diff(a)
    up = np.where(d > 0.0, d, 0.0)
    dn = np.where(d < 0.0, -d, 0.0)
    avg_up = np.mean(up[:period])
    avg_dn = np.mean(dn[:period])
    for i in range(period, len(d)):
        avg_up = (avg_up * (period - 1) + up[i]) / period
        avg_dn = (avg_dn * (period - 1) + dn[i]) / period
    if avg_dn <= 1e-12:
        return 100.0
    rs = avg_up / avg_dn
    return float(100.0 - (100.0 / (1.0 + rs)))


def linear_slope(values: Sequence[float], period: int = 20) -> float:
    a = _arr(values)
    if a.size < 2:
        return 0.0
    p = max(2, min(period, int(a.size)))
    y = a[-p:]
    x = np.arange(p, dtype=np.float64)
    xm = float(np.mean(x))
    ym = float(np.mean(y))
    den = float(np.sum((x - xm) ** 2))
    if den <= 1e-12:
        return 0.0
    return float(np.sum((x - xm) * (y - ym)) / den)


def bollinger(closes: Sequence[float], period: int = 20, mult: float = 2.0) -> Tuple[float, float, float]:
    mid = sma(closes, period)
    sd = stddev(closes, period)
    return (mid - mult * sd, mid, mid + mult * sd)


def atr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int = 14) -> float:
    h = _arr(highs)
    l = _arr(lows)
    c = _arr(closes)
    n = min(h.size, l.size, c.size)
    if n < 2:
        return 0.0
    h = h[-n:]
    l = l[-n:]
    c = c[-n:]
    prev = c[:-1]
    tr = np.maximum.reduce([h[1:] - l[1:], np.abs(h[1:] - prev), np.abs(l[1:] - prev)])
    if tr.size == 0:
        return 0.0
    p = max(1, min(period, int(tr.size)))
    return float(np.mean(tr[-p:]))


def keltner(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int = 20, mult: float = 1.5) -> Tuple[float, float, float]:
    basis = ema(closes, period)
    a = atr(highs, lows, closes, period=max(10, period))
    return (basis - mult * a, basis, basis + mult * a)


def obv(closes: Sequence[float], volumes: Sequence[float]) -> np.ndarray:
    c = _arr(closes)
    v = _arr(volumes)
    n = min(c.size, v.size)
    if n == 0:
        return np.zeros(0, dtype=np.float64)
    c = c[-n:]
    v = v[-n:]
    out = np.zeros(n, dtype=np.float64)
    for i in range(1, n):
        if c[i] > c[i - 1]:
            out[i] = out[i - 1] + v[i]
        elif c[i] < c[i - 1]:
            out[i] = out[i - 1] - v[i]
        else:
            out[i] = out[i - 1]
    return out


def volume_profile(closes: Sequence[float], quote_volumes: Sequence[float], bins: int = 24) -> Dict[str, float]:
    c = _arr(closes)
    qv = _arr(quote_volumes)
    n = min(c.size, qv.size)
    if n < 10:
        return {"poc": float(c[-1]) if n else 0.0, "cur_node_vol": 0.0, "low_node_cutoff": 0.0}
    c = c[-n:]
    qv = qv[-n:]
    low = float(np.min(c))
    high = float(np.max(c))
    if high <= low:
        return {"poc": low, "cur_node_vol": float(qv[-1]), "low_node_cutoff": float(np.percentile(qv, 30))}
    hist, edges = np.histogram(c, bins=bins, range=(low, high), weights=qv)
    idx_poc = int(np.argmax(hist))
    poc = float((edges[idx_poc] + edges[idx_poc + 1]) * 0.5)
    cur_bin = int(np.clip(np.searchsorted(edges, c[-1], side="right") - 1, 0, len(hist) - 1))
    cur_node_vol = float(hist[cur_bin])
    low_node_cutoff = float(np.percentile(hist, 30))
    return {"poc": poc, "cur_node_vol": cur_node_vol, "low_node_cutoff": low_node_cutoff}


def compute_signals(data: Dict[str, Any]) -> Tuple[ModuleSignals, Dict[str, Any]]:
    close = _arr(data.get("close", []))
    high = _arr(data.get("high", []))
    low = _arr(data.get("low", []))
    quote_vol = _arr(data.get("quote_volume", []))
    base_vol = _arr(data.get("base_volume", []))
    trades = _arr(data.get("trades", []))
    taker_buy_quote = _arr(data.get("taker_buy_quote", []))
    oi = _arr(data.get("oi", []))
    close_1m = _arr(data.get("close_1m", []))
    quote_vol_1m = _arr(data.get("quote_volume_1m", []))
    oi_change_pct = float(data.get("oi_change_pct", 0.0))
    funding_rate = float(data.get("funding_rate", 0.0))
    funding_rate_delta = float(data.get("funding_rate_delta", 0.0))
    funding_rate_level = str(data.get("funding_rate_level", "neutral") or "neutral").strip().lower()
    cbp = float(data.get("cbp", 0.0))

    n = int(min(close.size, high.size, low.size, quote_vol.size, base_vol.size, trades.size, taker_buy_quote.size))
    if n < 30:
        empty = ModuleSignals(
            squeeze=False,
            breakout=False,
            momentum=False,
            acceleration=False,
            vol_surge=False,
            volume_flow=False,
            order_flow_buy=False,
            order_flow_sell=False,
            smart_money=False,
            lurking=False,
            black_horse=False,
            vp_vacuum=False,
            sniper_a=False,
            sniper_b=False,
            trigger_1m=False,
            essential_ok=False,
        )
        return empty, {"score_raw": 0.0, "score_final": 0.0, "cbp": cbp}

    close = close[-n:]
    high = high[-n:]
    low = low[-n:]
    quote_vol = quote_vol[-n:]
    base_vol = base_vol[-n:]
    trades = trades[-n:]
    taker_buy_quote = taker_buy_quote[-n:]

    bb_low, bb_mid, bb_up = bollinger(close, 20, 2.0)
    kc_low, _, kc_up = keltner(high, low, close, 20, 1.5)
    squeeze = bb_low >= kc_low and bb_up <= kc_up

    rsi14 = rsi(close, 14)
    vma20 = sma(quote_vol, 20)
    breakout = bool(close[-1] > bb_up and rsi14 > 60.0 and quote_vol[-1] > 1.5 * vma20)

    slope20 = linear_slope(close, 20)
    momentum = bool(50.0 <= rsi14 <= 70.0 and slope20 > 0.0)
    atr14 = atr(high, low, close, 14)
    atr_pct = (atr14 / max(float(close[-1]), 1e-9) * 100.0) if n > 0 else 0.0

    bull3 = bool(
        close[-1] > close[-2] > close[-3] > close[-4]
        and (close[-1] - low[-1]) > (close[-2] - low[-2]) > (close[-3] - low[-3])
        and quote_vol[-1] > quote_vol[-2] > quote_vol[-3]
    )
    acceleration = bull3

    vol_1h_avg = float(np.mean(quote_vol[-5:-1])) if n >= 5 else float(np.mean(quote_vol[:-1]))
    vol_surge = bool(quote_vol[-1] > 3.0 * max(vol_1h_avg, 1e-9))
    volume_flow = bool(quote_vol[-1] > vma20)

    taker_sell_quote = np.maximum(quote_vol - taker_buy_quote, 0.0)
    delta = taker_buy_quote - taker_sell_quote
    cvd = float(np.sum(delta[-20:]))
    px_chg = float((close[-1] - close[-5]) / max(close[-5], 1e-9) * 100.0) if n >= 5 else 0.0
    oi_trend_up = oi_change_pct > 0.2
    order_flow_buy = bool(px_chg > 0 and oi_trend_up and cvd > 0)
    order_flow_sell = bool(px_chg < 0 and oi_trend_up and cvd < 0)

    avg_trade_value = quote_vol / np.maximum(trades, 1.0)
    smart_money = bool(avg_trade_value[-1] > 2.0 * sma(avg_trade_value, 50))

    range20 = float((np.max(high[-20:]) - np.min(low[-20:])) / max(close[-1], 1e-9) * 100.0)
    obv_line = obv(close, base_vol)
    obv_up = linear_slope(obv_line, 20) > 0
    lurking = bool(range20 < 1.0 and oi_change_pct > 1.0 and obv_up)

    vol50 = float(np.std(close[-50:]) / max(np.mean(close[-50:]), 1e-9) * 100.0) if n >= 50 else 999.0
    qv50 = float(np.mean(quote_vol[-50:])) if n >= 50 else float(np.mean(quote_vol))
    qv_recent = float(np.mean(quote_vol[-5:])) if n >= 5 else float(np.mean(quote_vol))
    black_horse = bool(vol50 < 1.2 and qv_recent < 0.6 * qv50 and oi_change_pct > 1.5)

    vp = volume_profile(close[-96:], quote_vol[-96:], bins=24)
    vp_vacuum = bool(close[-1] > float(vp["poc"]) and float(vp["cur_node_vol"]) < float(vp["low_node_cutoff"]))

    oi_fast = float(data.get("oi_change_pct_fast", oi_change_pct))
    sniper_a = bool(funding_rate < -0.0005 and oi_change_pct > 1.0)
    sniper_b = bool((order_flow_buy or order_flow_sell) and (vol_surge or squeeze) and oi_change_pct > 4.0)

    trigger_1m = False
    if close_1m.size >= 2 and quote_vol_1m.size >= 2:
        pump_1m = float((close_1m[-1] - close_1m[-2]) / max(close_1m[-2], 1e-9) * 100.0)
        flow_ok = quote_vol_1m[-1] > max(vma20 / 15.0 * 2.0, 1e-9)
        oi_burst = oi_fast > 0.8
        trigger_1m = bool((pump_1m > 0.35 and flow_ok) or (oi_burst and flow_ok))

    essential_ok = bool(squeeze or ((order_flow_buy or order_flow_sell) and vol_surge))

    score = 0.0
    if squeeze:
        score += 10.0
    if breakout:
        score += 16.0
    if momentum:
        score += 10.0
    if acceleration:
        score += 9.0
    if vol_surge:
        score += 14.0
    if volume_flow:
        score += 8.0
    if order_flow_buy or order_flow_sell:
        score += 16.0
    if smart_money:
        score += 10.0
    if lurking:
        score += 8.0
    if black_horse:
        score += 11.0
    if vp_vacuum:
        score += 10.0

    score_raw = score
    if sniper_a or sniper_b:
        score = 150.0
    elif trigger_1m:
        score = max(score, 75.0)

    funding_penalty = 0.0
    funding_bonus = 0.0
    if funding_rate_level == "extreme_positive" or funding_rate >= 0.0015:
        funding_penalty = 8.0
    elif funding_rate_level == "positive" or funding_rate >= 0.0010:
        funding_penalty = 5.0

    if (funding_rate_level == "extreme_negative" or funding_rate <= -0.0008) and oi_change_pct > 0.3:
        funding_bonus = 6.0
    elif (funding_rate_level == "negative" or funding_rate <= -0.0004) and oi_change_pct > 0.3:
        funding_bonus = 3.0

    funding_divergence = False
    if px_chg > 0.0 and funding_rate_delta < -0.00003:
        funding_divergence = True
    elif px_chg < 0.0 and funding_rate_delta > 0.00003:
        funding_divergence = True
    if funding_divergence and score < 150.0:
        funding_penalty += 4.0

    if score < 150.0:
        score = score + funding_bonus - funding_penalty

    if not essential_ok and score < 150.0:
        score *= 0.7

    base_threshold = 60.0
    if atr_pct <= 0.8:
        base_threshold -= 4.0
    elif atr_pct <= 1.5:
        base_threshold -= 2.0
    elif atr_pct >= 3.0:
        base_threshold += 4.0
    elif atr_pct >= 2.2:
        base_threshold += 2.0

    if funding_penalty > 0:
        base_threshold += min(4.0, funding_penalty * 0.5)
    if funding_bonus > 0:
        base_threshold -= min(3.0, funding_bonus * 0.5)

    base_threshold = min(78.0, max(48.0, base_threshold))
    if cbp > 0.0005:
        threshold = base_threshold - 6.0
    elif cbp < -0.0005:
        threshold = base_threshold + 8.0
    else:
        threshold = base_threshold
    score = max(0.0, score)

    level = "none"
    if score >= 150.0:
        level = "SNIPER"
    elif score >= 90.0:
        level = "HOT"
    elif score >= threshold:
        level = "normal"

    signals = ModuleSignals(
        squeeze=squeeze,
        breakout=breakout,
        momentum=momentum,
        acceleration=acceleration,
        vol_surge=vol_surge,
        volume_flow=volume_flow,
        order_flow_buy=order_flow_buy,
        order_flow_sell=order_flow_sell,
        smart_money=smart_money,
        lurking=lurking,
        black_horse=black_horse,
        vp_vacuum=vp_vacuum,
        sniper_a=sniper_a,
        sniper_b=sniper_b,
        trigger_1m=trigger_1m,
        essential_ok=essential_ok,
    )
    extra = {
        "score_raw": round(score_raw, 2),
        "score_final": round(score, 2),
        "threshold": round(threshold, 2),
        "alert": bool(score >= threshold),
        "watch_alert": bool(trigger_1m or ((momentum or breakout) and volume_flow and (order_flow_buy or order_flow_sell))),
        "level": level,
        "rsi14": round(rsi14, 2),
        "atr14": atr14,
        "atr_pct": atr_pct,
        "slope20": slope20,
        "cvd20": cvd,
        "oi_change_pct": oi_change_pct,
        "funding_rate": funding_rate,
        "funding_rate_delta": funding_rate_delta,
        "funding_rate_level": funding_rate_level,
        "funding_divergence": funding_divergence,
        "funding_bonus": funding_bonus,
        "funding_penalty": funding_penalty,
        "cbp": cbp,
        "vp_poc": vp.get("poc"),
    }
    return signals, extra
