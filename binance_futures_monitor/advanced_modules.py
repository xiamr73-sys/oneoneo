#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Advanced analysis modules with independent on/off switches."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np

from binance_futures_monitor.utils import atr, bollinger, ema, linear_slope


def _to_arr(values: Sequence[float]) -> np.ndarray:
    return np.asarray(values, dtype=np.float64)


def _sign(v: float) -> int:
    if v > 1e-12:
        return 1
    if v < -1e-12:
        return -1
    return 0


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _trend_direction(first: float, last: float, flat_pct: float = 0.2) -> Tuple[str, float]:
    if abs(first) <= 1e-12:
        return "sideways", 0.0
    pct = (last - first) / abs(first) * 100.0
    if abs(pct) <= flat_pct:
        return "sideways", pct
    return ("up" if pct > 0 else "down"), pct


def _speed_label(change_pct: float, bars: int) -> str:
    per_bar = abs(change_pct) / max(1, int(bars))
    if per_bar >= 0.25:
        return "fast"
    if per_bar >= 0.08:
        return "moderate"
    return "slow"


def _three_ranges(n: int) -> List[Tuple[int, int]]:
    if n <= 0:
        return []
    b1 = max(1, n // 3)
    b2 = max(b1 + 1, (2 * n) // 3)
    if b2 >= n:
        b2 = n - 1
    if b2 <= b1:
        b2 = min(n - 1, b1 + 1)
    return [(0, b1), (b1, b2), (b2, n)]


def _segment_series(values: Sequence[float], labels: Sequence[str]) -> Dict[str, Any]:
    arr = _to_arr(values)
    n = int(arr.size)
    segs: List[Dict[str, Any]] = []
    for i, (s, e) in enumerate(_three_ranges(n)):
        seg = arr[s:e]
        if seg.size == 0:
            segs.append(
                {
                    "phase": labels[i] if i < len(labels) else f"p{i+1}",
                    "min": 0.0,
                    "max": 0.0,
                    "direction": "sideways",
                    "change_pct": 0.0,
                    "speed": "slow",
                    "bars": 0,
                }
            )
            continue
        direction, chg = _trend_direction(float(seg[0]), float(seg[-1]), flat_pct=0.2)
        segs.append(
            {
                "phase": labels[i] if i < len(labels) else f"p{i+1}",
                "min": float(np.min(seg)),
                "max": float(np.max(seg)),
                "direction": direction,
                "change_pct": float(chg),
                "speed": _speed_label(chg, int(seg.size)),
                "bars": int(seg.size),
            }
        )

    dirs = [str(x.get("direction", "sideways")) for x in segs]
    non_flat = [d for d in dirs if d != "sideways"]
    if len(non_flat) >= 2 and non_flat[-1] == non_flat[-2]:
        relation = "continuation"
    elif len(non_flat) >= 2 and non_flat[-1] != non_flat[-2]:
        relation = "reversal"
    elif len(dirs) == 3 and dirs[0] == "sideways" and dirs[1] == "sideways" and dirs[2] in {"up", "down"}:
        relation = "compression_breakout"
    else:
        relation = "mixed"

    last = float(arr[-1]) if n else 0.0
    lo = float(np.min(arr)) if n else 0.0
    hi = float(np.max(arr)) if n else 0.0
    pos = 50.0
    if hi > lo:
        pos = (last - lo) / (hi - lo) * 100.0

    return {
        "segments": segs,
        "relation": relation,
        "current_position_pct": float(_clamp(pos, 0.0, 100.0)),
        "latest_direction": dirs[-1] if dirs else "sideways",
    }


def _build_cvd_series(quote_vol: Sequence[float], taker_buy_quote: Sequence[float]) -> np.ndarray:
    q = _to_arr(quote_vol)
    b = _to_arr(taker_buy_quote)
    n = min(int(q.size), int(b.size))
    if n <= 0:
        return np.zeros(0, dtype=np.float64)
    q = q[-n:]
    b = b[-n:]
    delta = 2.0 * b - q
    return np.cumsum(delta)


def _analyze_segmentation(snapshot: Dict[str, Any], signal_input: Dict[str, Any]) -> Dict[str, Any]:
    k15 = snapshot.get("k15", []) if isinstance(snapshot.get("k15", []), list) else []
    ts_list = [float(x.get("ts", 0.0) or 0.0) for x in k15 if isinstance(x, dict)]
    if ts_list:
        span_min = max(0.0, (ts_list[-1] - ts_list[0]) / 60000.0)
    else:
        span_min = 0.0

    close = signal_input.get("close", [])
    oi_hist = snapshot.get("oi_hist", []) if isinstance(snapshot.get("oi_hist", []), list) else []
    oi_series = [float(x.get("oi", 0.0) or 0.0) for x in oi_hist if isinstance(x, dict)]
    quote_vol = signal_input.get("quote_volume", [])
    taker_buy_quote = signal_input.get("taker_buy_quote", [])
    cvd = _build_cvd_series(quote_vol, taker_buy_quote)

    return {
        "time_span_min": round(float(span_min), 2),
        "price": _segment_series(close, ("early", "mid", "late")),
        "oi": _segment_series(oi_series, ("early", "mid", "late")),
        "cvd": _segment_series(cvd.tolist(), ("early", "mid", "late")),
    }


def _classify_combo(seg: Dict[str, Any]) -> Dict[str, Any]:
    p = str((seg.get("price", {}) or {}).get("latest_direction", "sideways"))
    o = str((seg.get("oi", {}) or {}).get("latest_direction", "sideways"))
    c = str((seg.get("cvd", {}) or {}).get("latest_direction", "sideways"))
    combo_map = {
        ("up", "up", "up"): ("P↑ OI↑ CVD↑", "真实买方驱动，趋势可信", "green"),
        ("up", "up", "down"): ("P↑ OI↑ CVD↓", "可能存在动态托举，需验证", "yellow"),
        ("up", "down", "up"): ("P↑ OI↓ CVD↑", "偏被动反弹，动能可能受限", "yellow"),
        ("up", "down", "down"): ("P↑ OI↓ CVD↓", "现货期货背离，方向不够清晰", "warning"),
        ("down", "up", "down"): ("P↓ OI↑ CVD↓", "真实空方驱动，趋势可信", "red"),
        ("down", "up", "up"): ("P↓ OI↑ CVD↑", "可能存在动态压制，需验证", "yellow"),
        ("down", "down", "down"): ("P↓ OI↓ CVD↓", "偏被动下砸，延续性一般", "yellow"),
        ("down", "down", "up"): ("P↓ OI↓ CVD↑", "现货期货背离，方向不够清晰", "warning"),
    }
    label, meaning, tone = combo_map.get((p, o, c), ("MIXED", "多空混杂，等待确认", "warning"))
    return {
        "price_dir": p,
        "oi_dir": o,
        "cvd_dir": c,
        "combo": label,
        "meaning": meaning,
        "tone": tone,
    }


def _analyze_cvd_structure(signal_input: Dict[str, Any], seg: Dict[str, Any]) -> Dict[str, Any]:
    close = _to_arr(signal_input.get("close", []))
    cvd = _build_cvd_series(signal_input.get("quote_volume", []), signal_input.get("taker_buy_quote", []))
    if close.size == 0 or cvd.size == 0:
        return {
            "zero_cross": False,
            "pattern": "insufficient",
            "divergence": "unknown",
            "health": "weak",
            "price_alignment": "unknown",
        }

    zero_cross = bool(np.any(np.signbit(cvd[1:]) != np.signbit(cvd[:-1]))) if cvd.size >= 2 else False
    deep_v = False
    if cvd.size >= 12:
        idx_min = int(np.argmin(cvd))
        deep_v = idx_min > cvd.size // 5 and idx_min < (cvd.size * 4) // 5 and cvd[-1] > cvd[idx_min]
    px_chg = (float(close[-1]) - float(close[0])) / max(abs(float(close[0])), 1e-12)
    cvd_chg = (float(cvd[-1]) - float(cvd[0])) / max(abs(float(cvd[0])) if abs(float(cvd[0])) > 1e-12 else 1.0, 1e-12)
    if px_chg > 0 and cvd_chg < 0:
        divergence = "bearish"
    elif px_chg < 0 and cvd_chg > 0:
        divergence = "bullish"
    else:
        divergence = "none"

    latest_cvd_dir = str((seg.get("cvd", {}) or {}).get("latest_direction", "sideways"))
    latest_px_dir = str((seg.get("price", {}) or {}).get("latest_direction", "sideways"))
    aligned = latest_cvd_dir == latest_px_dir and latest_cvd_dir in {"up", "down"}
    if divergence != "none":
        health = "weak"
    elif aligned:
        health = "healthy"
    else:
        health = "neutral"

    pattern = "deep_v" if deep_v else ("persistent_up" if cvd_chg > 0 else "persistent_down")
    return {
        "zero_cross": zero_cross,
        "pattern": pattern,
        "divergence": divergence,
        "health": health,
        "price_alignment": "aligned" if aligned else "diverged",
    }


def _calc_td_count(close: np.ndarray) -> Dict[str, Any]:
    if close.size < 6:
        return {"up": 0, "down": 0, "signal": "none"}
    up = 0
    down = 0
    for i in range(4, int(close.size)):
        if close[i] > close[i - 4]:
            up += 1
            down = 0
        elif close[i] < close[i - 4]:
            down += 1
            up = 0
        else:
            up = 0
            down = 0
    signal = "none"
    if up >= 13:
        signal = "sell_13"
    elif down >= 13:
        signal = "buy_13"
    elif up >= 9:
        signal = "sell_9"
    elif down >= 9:
        signal = "buy_9"
    return {"up": int(up), "down": int(down), "signal": signal}


def _analyze_aux_indicators(signal_input: Dict[str, Any]) -> Dict[str, Any]:
    close = _to_arr(signal_input.get("close", []))
    high = _to_arr(signal_input.get("high", []))
    low = _to_arr(signal_input.get("low", []))
    quote_vol = _to_arr(signal_input.get("quote_volume", []))
    if close.size == 0:
        return {
            "ema": {"ema24": 0.0, "ema55": 0.0, "ema77": 0.0, "stack": "mixed"},
            "vwap": {"value": 0.0, "price_above": False},
            "bb": {"pct": 50.0, "zone": "middle"},
            "td": {"up": 0, "down": 0, "signal": "none"},
            "resonance": "mixed",
        }

    ema24 = float(ema(close.tolist(), 24))
    ema55 = float(ema(close.tolist(), 55))
    ema77 = float(ema(close.tolist(), 77))
    stack = "mixed"
    if ema24 > ema55 > ema77:
        stack = "bull"
    elif ema24 < ema55 < ema77:
        stack = "bear"

    vol = quote_vol if quote_vol.size == close.size else np.ones_like(close)
    vwap = float(np.sum(close * np.maximum(vol, 1e-12)) / np.sum(np.maximum(vol, 1e-12)))
    price_above_vwap = bool(float(close[-1]) > vwap)

    bb_low, _, bb_up = bollinger(close.tolist(), 20, 2.0)
    if bb_up > bb_low:
        bb_pct = (float(close[-1]) - float(bb_low)) / (float(bb_up) - float(bb_low)) * 100.0
    else:
        bb_pct = 50.0
    bb_pct = _clamp(bb_pct, 0.0, 100.0)
    if bb_pct >= 85.0:
        bb_zone = "upper_extreme"
    elif bb_pct <= 15.0:
        bb_zone = "lower_extreme"
    else:
        bb_zone = "middle"

    td = _calc_td_count(close)

    bull_votes = 0
    bear_votes = 0
    bull_votes += int(stack == "bull")
    bear_votes += int(stack == "bear")
    bull_votes += int(price_above_vwap)
    bear_votes += int(not price_above_vwap)
    bull_votes += int(bb_pct >= 50.0 and bb_pct < 90.0)
    bear_votes += int(bb_pct <= 50.0 and bb_pct > 10.0)
    if td.get("signal", "").startswith("buy"):
        bull_votes += 1
    elif td.get("signal", "").startswith("sell"):
        bear_votes += 1
    resonance = "mixed"
    if bull_votes >= bear_votes + 2:
        resonance = "bull"
    elif bear_votes >= bull_votes + 2:
        resonance = "bear"

    return {
        "ema": {"ema24": ema24, "ema55": ema55, "ema77": ema77, "stack": stack},
        "vwap": {"value": vwap, "price_above": price_above_vwap},
        "bb": {"pct": float(bb_pct), "zone": bb_zone},
        "td": td,
        "resonance": resonance,
    }


def _oi_change_for_minutes(oi_hist: List[Dict[str, float]], minutes: int) -> float:
    if not oi_hist:
        return 0.0
    ts = np.asarray([float(x.get("ts", 0.0) or 0.0) for x in oi_hist], dtype=np.float64)
    oi = np.asarray([float(x.get("oi", 0.0) or 0.0) for x in oi_hist], dtype=np.float64)
    if ts.size < 2 or oi.size < 2:
        return 0.0
    now_ts = float(ts[-1])
    target = now_ts - float(minutes) * 60.0 * 1000.0
    idx = int(np.searchsorted(ts, target, side="left"))
    idx = max(0, min(idx, int(ts.size - 1)))
    old = float(oi[idx])
    if abs(old) <= 1e-12:
        return 0.0
    return (float(oi[-1]) - old) / abs(old) * 100.0


def _analyze_capital_flow_mtf(snapshot: Dict[str, Any], signal_input: Dict[str, Any]) -> Dict[str, Any]:
    quote_vol = _to_arr(signal_input.get("quote_volume", []))
    taker_buy_quote = _to_arr(signal_input.get("taker_buy_quote", []))
    n = min(int(quote_vol.size), int(taker_buy_quote.size))
    if n <= 0:
        return {"windows": {}, "resonance": "mixed", "weighted_bias": 0.0, "oi_match": "unknown"}
    q = quote_vol[-n:]
    b = taker_buy_quote[-n:]
    delta = 2.0 * b - q

    def net_window(bars: int) -> float:
        m = max(1, min(int(bars), int(delta.size)))
        return float(np.sum(delta[-m:]))

    w = {
        "15m": net_window(1),
        "1h": net_window(4),
        "4h": net_window(16),
        "24h": net_window(96),
    }
    windows: Dict[str, Dict[str, Any]] = {}
    for k, v in w.items():
        direction = "inflow" if v > 0 else ("outflow" if v < 0 else "flat")
        windows[k] = {"net": float(v), "direction": direction}

    s15 = _sign(w["15m"])
    s1h = _sign(w["1h"])
    s4h = _sign(w["4h"])
    s24h = _sign(w["24h"])
    weighted_bias = 0.45 * s15 + 0.35 * s1h + 0.15 * s4h + 0.05 * s24h
    same_all = s15 != 0 and s15 == s1h == s4h == s24h
    if same_all:
        resonance = "all_aligned"
    elif s15 != 0 and s1h != 0 and s15 == s1h:
        resonance = "short_aligned"
    else:
        resonance = "mixed"

    oi_hist = snapshot.get("oi_hist", []) if isinstance(snapshot.get("oi_hist", []), list) else []
    oi_15m = _oi_change_for_minutes(oi_hist, 15)
    oi_1h = _oi_change_for_minutes(oi_hist, 60)
    oi_4h = _oi_change_for_minutes(oi_hist, 240)
    oi_24h = _oi_change_for_minutes(oi_hist, 24 * 60)
    oi_short_sign = _sign(oi_1h if abs(oi_1h) > abs(oi_15m) else oi_15m)
    flow_short_sign = _sign(0.55 * s15 + 0.45 * s1h)
    if oi_short_sign == 0 or flow_short_sign == 0:
        oi_match = "unknown"
    elif oi_short_sign == flow_short_sign:
        oi_match = "matched"
    else:
        oi_match = "diverged"

    return {
        "windows": windows,
        "resonance": resonance,
        "weighted_bias": float(weighted_bias),
        "oi_change_pct": {
            "15m": float(oi_15m),
            "1h": float(oi_1h),
            "4h": float(oi_4h),
            "24h": float(oi_24h),
        },
        "oi_match": oi_match,
    }


def _score_to_rating(v: float) -> str:
    if v >= 0.6:
        return "strong"
    if v <= -0.6:
        return "weak"
    return "medium"


def _analyze_five_dim_scoring(
    combo: Dict[str, Any],
    seg: Dict[str, Any],
    cvd_structure: Dict[str, Any],
    flow: Dict[str, Any],
    aux: Dict[str, Any],
) -> Dict[str, Any]:
    p_rel = str((seg.get("price", {}) or {}).get("relation", "mixed"))
    p_dir = str((seg.get("price", {}) or {}).get("latest_direction", "sideways"))
    p_score = 0.0
    if p_dir == "up":
        p_score += 0.6
    elif p_dir == "down":
        p_score -= 0.6
    if p_rel == "continuation":
        p_score += 0.25 if p_score >= 0 else -0.25
    elif p_rel == "reversal":
        p_score *= 0.4

    o_dir = str((seg.get("oi", {}) or {}).get("latest_direction", "sideways"))
    o_score = 0.0
    if o_dir == "up":
        o_score = 0.6
    elif o_dir == "down":
        o_score = -0.6

    cvd_health = str(cvd_structure.get("health", "neutral"))
    cvd_score = 0.0
    if cvd_health == "healthy":
        cvd_score = 0.7
    elif cvd_health == "weak":
        cvd_score = -0.7

    flow_score = float(flow.get("weighted_bias", 0.0))
    flow_score = _clamp(flow_score, -1.0, 1.0)

    aux_res = str(aux.get("resonance", "mixed"))
    aux_score = 0.0
    if aux_res == "bull":
        aux_score = 0.7
    elif aux_res == "bear":
        aux_score = -0.7

    dims = {
        "price_structure": {"score": p_score, "rating": _score_to_rating(p_score)},
        "oi_structure": {"score": o_score, "rating": _score_to_rating(o_score)},
        "cvd_structure": {"score": cvd_score, "rating": _score_to_rating(cvd_score)},
        "capital_flow": {"score": flow_score, "rating": _score_to_rating(flow_score)},
        "auxiliary": {"score": aux_score, "rating": _score_to_rating(aux_score)},
    }
    total = float(np.mean([x["score"] for x in dims.values()])) if dims else 0.0
    return {"dimensions": dims, "overall_score": total, "overall_rating": _score_to_rating(total), "combo": combo}


def _analyze_contradiction_guard(
    combo: Dict[str, Any],
    cvd_structure: Dict[str, Any],
    flow: Dict[str, Any],
    aux: Dict[str, Any],
) -> Dict[str, Any]:
    contradictions: List[str] = []
    combo_name = str(combo.get("combo", "MIXED"))
    if "CVD↓" in combo_name and str(cvd_structure.get("divergence", "none")) == "bearish":
        contradictions.append("price_and_cvd_bearish_divergence")
    if "CVD↑" in combo_name and str(cvd_structure.get("divergence", "none")) == "bullish":
        contradictions.append("price_and_cvd_bullish_divergence")

    short_dir = str((((flow.get("windows", {}) or {}).get("15m", {}) or {}).get("direction", "flat")))
    long_dir = str((((flow.get("windows", {}) or {}).get("24h", {}) or {}).get("direction", "flat")))
    if short_dir in {"inflow", "outflow"} and long_dir in {"inflow", "outflow"} and short_dir != long_dir:
        contradictions.append("short_vs_long_flow_conflict")

    aux_res = str(aux.get("resonance", "mixed"))
    if "P↑" in combo_name and aux_res == "bear":
        contradictions.append("price_up_but_aux_bear")
    if "P↓" in combo_name and aux_res == "bull":
        contradictions.append("price_down_but_aux_bull")

    confidence = 0.85 - 0.12 * float(len(contradictions))
    confidence = _clamp(confidence, 0.2, 0.95)
    return {
        "contradictions": contradictions,
        "is_contradicted": bool(contradictions),
        "confidence": float(confidence),
        "status": "unclear" if contradictions else "coherent",
    }


def _build_alert_digest(
    combo: Dict[str, Any],
    contradiction: Dict[str, Any],
    score5: Dict[str, Any],
) -> Dict[str, Any]:
    conf = float(contradiction.get("confidence", 0.5))
    combo_txt = str(combo.get("combo", "MIXED"))
    tone = str(combo.get("tone", "warning"))
    rating = str(score5.get("overall_rating", "medium"))
    contradictions = contradiction.get("contradictions", [])
    if contradictions:
        summary = f"{combo_txt} | conf={conf:.0%} | contradictions={len(contradictions)}"
    else:
        summary = f"{combo_txt} | conf={conf:.0%} | rating={rating}"
    return {"summary": summary, "tone": tone}


def _build_backtest_struct_metrics(
    combo: Dict[str, Any],
    contradiction: Dict[str, Any],
    score5: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "regime": str(combo.get("combo", "MIXED")),
        "confidence": float(contradiction.get("confidence", 0.5)),
        "contradiction_count": int(len(contradiction.get("contradictions", []) or [])),
        "overall_rating": str(score5.get("overall_rating", "medium")),
    }


def _build_trade_suggestion(
    signal_input: Dict[str, Any],
    combo: Dict[str, Any],
    contradiction: Dict[str, Any],
) -> Dict[str, Any]:
    close = _to_arr(signal_input.get("close", []))
    high = _to_arr(signal_input.get("high", []))
    low = _to_arr(signal_input.get("low", []))
    if close.size == 0:
        return {"action": "wait", "reason": "insufficient_data"}
    last = float(close[-1])
    a = float(atr(high.tolist(), low.tolist(), close.tolist(), period=14))
    a = max(a, max(last * 0.002, 1e-8))
    conf = float(contradiction.get("confidence", 0.5))
    combo_name = str(combo.get("combo", "MIXED"))
    bias = "wait"
    if combo_name.startswith("P↑"):
        bias = "long"
    elif combo_name.startswith("P↓"):
        bias = "short"
    if contradiction.get("is_contradicted", False):
        bias = "wait"

    if bias == "long" and conf >= 0.55:
        return {
            "action": "long",
            "entry": last,
            "stop": last - 1.5 * a,
            "target": last + 2.5 * a,
            "max_risk": "flow_or_cvd_reversal",
        }
    if bias == "short" and conf >= 0.55:
        return {
            "action": "short",
            "entry": last,
            "stop": last + 1.5 * a,
            "target": last - 2.5 * a,
            "max_risk": "flow_or_cvd_reversal",
        }
    return {"action": "wait", "reason": "low_confidence_or_mixed_structure", "max_risk": "contradictory_signals"}


@dataclass(frozen=True)
class AdvancedModuleConfig:
    trend_segmentation: bool = True
    combo_classifier: bool = True
    cvd_structure: bool = True
    auxiliary_indicators: bool = True
    capital_flow_mtf: bool = True
    five_dim_scoring: bool = True
    contradiction_guard: bool = True
    alert_digest: bool = True
    backtest_struct_metrics: bool = True
    trade_suggestion: bool = True

    def to_dict(self) -> Dict[str, bool]:
        return {
            "trend_segmentation": bool(self.trend_segmentation),
            "combo_classifier": bool(self.combo_classifier),
            "cvd_structure": bool(self.cvd_structure),
            "auxiliary_indicators": bool(self.auxiliary_indicators),
            "capital_flow_mtf": bool(self.capital_flow_mtf),
            "five_dim_scoring": bool(self.five_dim_scoring),
            "contradiction_guard": bool(self.contradiction_guard),
            "alert_digest": bool(self.alert_digest),
            "backtest_struct_metrics": bool(self.backtest_struct_metrics),
            "trade_suggestion": bool(self.trade_suggestion),
        }


def run_advanced_modules(
    snapshot: Dict[str, Any],
    signal_input: Dict[str, Any],
    cfg: AdvancedModuleConfig,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"enabled": cfg.to_dict()}

    segmentation: Dict[str, Any] = {}
    combo: Dict[str, Any] = {}
    cvd_structure: Dict[str, Any] = {}
    aux: Dict[str, Any] = {}
    flow: Dict[str, Any] = {}
    score5: Dict[str, Any] = {}
    contradiction: Dict[str, Any] = {}

    if cfg.trend_segmentation:
        segmentation = _analyze_segmentation(snapshot, signal_input)
        out["trend_segmentation"] = segmentation

    if cfg.combo_classifier:
        combo = _classify_combo(segmentation)
        out["combo_classifier"] = combo

    if cfg.cvd_structure:
        cvd_structure = _analyze_cvd_structure(signal_input, segmentation)
        out["cvd_structure"] = cvd_structure

    if cfg.auxiliary_indicators:
        aux = _analyze_aux_indicators(signal_input)
        out["auxiliary_indicators"] = aux

    if cfg.capital_flow_mtf:
        flow = _analyze_capital_flow_mtf(snapshot, signal_input)
        out["capital_flow_mtf"] = flow

    if cfg.five_dim_scoring:
        score5 = _analyze_five_dim_scoring(combo, segmentation, cvd_structure, flow, aux)
        out["five_dim_scoring"] = score5

    if cfg.contradiction_guard:
        contradiction = _analyze_contradiction_guard(combo, cvd_structure, flow, aux)
        out["contradiction_guard"] = contradiction

    if cfg.alert_digest:
        out["alert_digest"] = _build_alert_digest(combo, contradiction, score5)

    if cfg.backtest_struct_metrics:
        out["backtest_struct_metrics"] = _build_backtest_struct_metrics(combo, contradiction, score5)

    if cfg.trade_suggestion:
        out["trade_suggestion"] = _build_trade_suggestion(signal_input, combo, contradiction)

    return out
