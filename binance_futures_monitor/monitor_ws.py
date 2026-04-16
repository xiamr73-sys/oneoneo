#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""WebSocket-driven event monitor for Binance futures pump detection."""

from __future__ import annotations

import argparse
import asyncio
import csv
import fnmatch
import json
import os
import re
import sqlite3
import statistics
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Set, Tuple

import numpy as np

try:
    import websockets
except Exception as exc:  # pragma: no cover
    websockets = None  # type: ignore[assignment]
    _WS_IMPORT_ERROR = exc
else:
    _WS_IMPORT_ERROR = None

from binance_futures_monitor.advanced_modules import AdvancedModuleConfig
from binance_futures_monitor.bayesian_scorer import BayesianScorer, BayesianScorerConfig
from binance_futures_monitor.monitor import close_exchange, fetch_data_and_analyze, get_exchange

AnalyzerFunc = Callable[[Optional[Any], str], Awaitable[Any]]

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

CONTROL_BOOL_KEYS: Tuple[str, ...] = (
    "mod_trend_seg_enabled",
    "mod_combo_enabled",
    "mod_cvd_structure_enabled",
    "mod_aux_indicators_enabled",
    "mod_flow_mtf_enabled",
    "mod_scorecard_enabled",
    "mod_contradiction_enabled",
    "mod_alert_digest_enabled",
    "mod_backtest_metrics_enabled",
    "mod_trade_plan_enabled",
)


def _to_bool(v: Any) -> bool:
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _parse_symbols(v: str) -> Tuple[str, ...]:
    symbols = [x.strip().upper() for x in str(v).split(",") if x.strip()]
    return tuple(dict.fromkeys(symbols))


def _parse_symbol_collection(v: Any) -> Set[str]:
    out: Set[str] = set()
    if isinstance(v, str):
        items = [x.strip() for x in v.split(",")]
    elif isinstance(v, (list, tuple, set)):
        items = [str(x).strip() for x in v]
    else:
        items = []
    for item in items:
        if not item:
            continue
        out.add(item.upper())
    return out


def _normalize_webhook(v: Any) -> str:
    return str(v or "").strip()


def _mask_webhook(v: str) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if len(s) <= 40:
        return "***"
    return f"{s[:34]}...{s[-6:]}"


def _load_webhook_overrides(webhook_file: str) -> Dict[str, Any]:
    path_s = str(webhook_file or "").strip()
    if not path_s:
        return {}
    path = Path(path_s).expanduser()
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"[webhook] load failed path={path}: {exc!r}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f"[webhook] invalid config, JSON object required: path={path}")
    return raw


def _resolve_monitor_ws_webhook(
    control: Dict[str, Any],
    cli_webhook_url: str,
    webhook_file: str,
) -> str:
    webhook_override = _load_webhook_overrides(webhook_file)
    file_webhook = _normalize_webhook(
        webhook_override.get("discord_webhook_url", webhook_override.get("discordWebhookUrl", ""))
    )
    control_webhook = _normalize_webhook(control.get("discord_webhook_url", control.get("discordWebhookUrl", "")))
    cli_webhook = _normalize_webhook(cli_webhook_url)

    sources = [
        ("webhook_config_file", file_webhook),
        ("control_file", control_webhook),
        ("cli_or_env", cli_webhook),
    ]
    non_empty = [(name, value) for name, value in sources if value]
    unique_values = {value for _, value in non_empty}
    if len(unique_values) > 1:
        detail = ", ".join(f"{name}={_mask_webhook(value)}" for name, value in non_empty)
        raise RuntimeError(
            "startup rejected: monitor_ws webhook mismatch detected across sources. "
            f"webhook_file={webhook_file} details=({detail})"
        )
    return file_webhook or control_webhook or cli_webhook


def _load_control_overrides(control_file: str) -> Dict[str, Any]:
    path_s = str(control_file or "").strip()
    if not path_s:
        return {}
    path = Path(path_s).expanduser()
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[control] load failed path={path}: {exc!r}")
        return {}
    if not isinstance(raw, dict):
        print(f"[control] ignored non-object config path={path}")
        return {}

    out: Dict[str, Any] = {}
    if "strategy_version" in raw:
        try:
            out["strategy_version"] = str(raw.get("strategy_version") or "").strip() or "1.1"
        except Exception:
            pass
    if "discord_min_score" in raw:
        try:
            out["discord_min_score"] = float(raw.get("discord_min_score"))
        except Exception:
            pass
    if "discord_watch_min_hit" in raw:
        try:
            out["discord_watch_min_hit"] = int(raw.get("discord_watch_min_hit"))
        except Exception:
            pass
    if "disable_three_layer_filter" in raw:
        try:
            out["disable_three_layer_filter"] = _to_bool(raw.get("disable_three_layer_filter"))
        except Exception:
            pass
    if "discord_archive_enabled" in raw:
        try:
            out["discord_archive_enabled"] = _to_bool(raw.get("discord_archive_enabled"))
        except Exception:
            pass
    if "discord_archive_file" in raw:
        try:
            out["discord_archive_file"] = str(raw.get("discord_archive_file") or "").strip()
        except Exception:
            pass
    if "discord_module_stats_enabled" in raw:
        try:
            out["discord_module_stats_enabled"] = _to_bool(raw.get("discord_module_stats_enabled"))
        except Exception:
            pass
    if "discord_webhook_url" in raw or "discordWebhookUrl" in raw:
        try:
            out["discord_webhook_url"] = str(
                raw.get("discord_webhook_url", raw.get("discordWebhookUrl")) or ""
            ).strip()
        except Exception:
            pass
    if "aster_discord_webhook_url" in raw:
        try:
            out["aster_discord_webhook_url"] = str(raw.get("aster_discord_webhook_url") or "").strip()
        except Exception:
            pass
    for key in ("aster_ws_url", "aster_exchange_info_url", "aster_rest_base_url"):
        if key in raw:
            try:
                out[key] = str(raw.get(key) or "").strip()
            except Exception:
                pass
    for key in (
        "decision_tier_enabled",
        "aster_ws_enabled",
        "aster_only_usdt_contracts",
        "ingress_audit_enabled",
        "ingress_audit_discord_enabled",
        "ingress_force_record_enabled",
        "dynamic_trigger_use_micro",
        "dynamic_trigger_event_factor_enabled",
        "slow_breakout_enabled",
        "liquidation_spike_enabled",
        "slow_accum_enabled",
        "slow_accum_tier3_realtime",
        "fake_breakout_probe_enabled",
        "fake_breakout_probe_tier3_realtime",
        "whale_testing_enabled",
        "whale_bonus_enabled",
        "whale_waive_fake_breakout_penalty",
        "whale_waive_top_risk_penalty",
        "probe_follow_waive_fake_breakout_once",
        "bayesian_scorer_enabled",
        "bayesian_block_fake_weak_oi_penalty",
        "bayesian_block_funding_divergence_penalty",
        "bayesian_force_sniper_enabled",
        "ignition_3of4_blocked",
        "signal_db_enabled",
        "sniper_risk_downgrade_enabled",
        "sniper_risk_drop_on_heavy_fake_breakout",
        "symbol_adapt_enabled",
        "mtf_resonance_enabled",
        "risk_liquidity_enabled",
        "risk_slippage_enabled",
        "risk_cross_anomaly_enabled",
        "ml_shadow_enabled",
        "tier3_discord_push_enabled",
        "auto_tune_enabled",
        "auto_tune_apply_enabled",
        "auto_tune_persist_control",
        "symbol_quality_enabled",
        "sector_cluster_enabled",
        "regime_pattern_enabled",
        "sentiment_enabled",
    ):
        if key in raw:
            try:
                out[key] = _to_bool(raw.get(key))
            except Exception:
                pass
    if "dynamic_trigger_enabled" in raw:
        try:
            out["dynamic_trigger_enabled"] = _to_bool(raw.get("dynamic_trigger_enabled"))
        except Exception:
            pass
    if "fake_breakout_filter_enabled" in raw:
        try:
            out["fake_breakout_filter_enabled"] = _to_bool(raw.get("fake_breakout_filter_enabled"))
        except Exception:
            pass
    for key in (
        "dynamic_trigger_quantile",
        "dynamic_trigger_anchor_pct",
        "dynamic_trigger_min_scale",
        "dynamic_trigger_max_scale",
        "aster_symbols_reload_sec",
        "ingress_audit_min_baseline_count",
        "ingress_audit_low_ratio",
        "ingress_audit_volume_spike_ratio",
        "ingress_audit_alert_cooldown_sec",
        "ingress_force_min_ret_1m_pct",
        "ingress_force_min_vol_ratio_1m",
        "ingress_force_record_cooldown_sec",
        "dynamic_min_pump_pct",
        "dynamic_min_pump_pct_with_volume",
        "flow_pump_min_ret_1m_pct",
        "flow_pump_min_volume_1m",
        "flow_pump_min_vol_ratio_1m",
        "slow_breakout_ret_1m_pct",
        "slow_breakout_ret_5m_pct",
        "slow_breakout_min_vol_ratio_1m",
        "slow_breakout_min_oi5m_pct",
        "slow_breakout_min_delta_persist",
        "fake_breakout_ret_1m_pct",
        "fake_breakout_min_vol_ratio_1m",
        "fake_breakout_min_taker_buy_ratio",
        "fake_breakout_min_buy_imbalance",
        "fake_breakout_max_oi5m_pct",
        "fake_breakout_pullback_pct",
        "fake_breakout_delta_persist_max",
        "liquidation_spike_ret_1m_pct",
        "liquidation_spike_min_vol_ratio_1m",
        "liquidation_spike_max_oi5m_pct",
        "liquidation_spike_timer_sec",
        "liquidation_spike_retrace_pct",
        "liquidation_spike_stabilize_abs_ret_1m_pct",
        "liquidation_spike_stabilize_max_vol_ratio_1m",
        "liquidation_spike_confirm_min_oi5m_pct",
        "liquidation_spike_follow_window_sec",
        "liquidation_spike_check_interval_sec",
        "liquidation_spike_alert_cooldown_sec",
        "liquidation_spike_min_taker_buy_ratio",
        "liquidation_spike_min_buy_imbalance",
        "slow_accum_window_sec",
        "slow_accum_min_ret_1m_pct",
        "slow_accum_max_ret_1m_pct",
        "slow_accum_min_stair_step_pct",
        "slow_accum_alert_cooldown_sec",
        "slow_accum_alpha_ret_1m_pct",
        "slow_accum_alpha_bonus",
        "slow_accum_alpha_plus_bonus",
        "fake_breakout_probe_window_sec",
        "fake_breakout_probe_max_centroid_drop_pct",
        "fake_breakout_probe_alert_cooldown_sec",
        "fake_breakout_probe_push_max_ret_1m_pct",
        "fake_breakout_probe_push_max_centroid_change_pct",
        "fake_breakout_probe_bonus",
        "fake_breakout_probe_alpha_bonus",
        "probe_follow_breakout_window_sec",
        "probe_follow_whale_bonus",
        "module_signal_cooldown_sec",
        "whale_testing_window_sec",
        "whale_testing_max_drawdown_pct",
        "whale_testing_ttl_sec",
        "whale_bonus_score",
        "bayesian_prior_max",
        "bayesian_prior_min",
        "bayesian_prior_decay_hits",
        "bayesian_evidence_a_pump20s_pct",
        "bayesian_evidence_b_funding_rate_lt",
        "bayesian_evidence_b_funding_delta_lt",
        "bayesian_evidence_c_oi5m_pct_le",
        "bayesian_p_ea_given_h",
        "bayesian_p_eb_given_h",
        "bayesian_p_ec_given_h",
        "bayesian_p_ea_given_not_h",
        "bayesian_p_eb_given_not_h",
        "bayesian_p_ec_given_not_h",
        "bayesian_override_posterior_threshold",
        "bayesian_score_multiplier",
        "ignition_min_buy_imbalance",
        "ignition_max_fake_flags",
        "dynamic_trigger_event_major_factor",
        "dynamic_trigger_event_minor_factor",
        "tier1_score",
        "tier2_score",
        "tier3_score",
        "tier2_confirm_k_sec",
        "tier2_confirm_pullback_pct",
        "risk_spread_bps_max",
        "risk_depth_min_usd",
        "risk_slippage_notional_usd",
        "risk_slippage_max_pct",
        "risk_cross_anomaly_pct",
        "risk_penalty_liquidity",
        "risk_penalty_slippage",
        "risk_penalty_cross_anomaly",
        "risk_penalty_fake_breakout",
        "risk_penalty_iceberg",
        "ml_shadow_downgrade_prob",
        "ml_shadow_upgrade_prob",
        "ml_shadow_penalty",
        "ml_shadow_bonus",
        "sniper_risk_downgrade_penalty",
        "sniper_risk_drop_penalty",
        "symbol_liq_high_24h",
        "symbol_liq_mid_24h",
        "auto_tune_interval_sec",
        "auto_tune_cooldown_sec",
        "auto_tune_win_rate_high",
        "auto_tune_win_rate_low",
        "auto_tune_tier_step_up",
        "auto_tune_tier_step_down",
        "auto_tune_tier2_min",
        "auto_tune_tier2_max",
        "symbol_quality_alpha",
        "symbol_quality_beta",
        "symbol_quality_floor",
        "symbol_quality_cap",
        "sector_cluster_window_sec",
        "sector_cluster_boost_factor",
        "sector_cluster_ttl_sec",
        "sector_cluster_min_base_score",
        "regime_win_rate_high",
        "regime_win_rate_low",
        "regime_factor_boost",
        "regime_factor_cut",
        "regime_morning_factor",
        "regime_friday_night_factor",
        "sentiment_whale_notional_usd",
        "sentiment_lsr_weight",
        "sentiment_whale_weight",
        "sentiment_concentration_weight",
        "sentiment_funding_div_weight",
        "discord_module_stats_interval_sec",
        "discord_module_stats_window_sec",
    ):
        if key in raw:
            try:
                out[key] = float(raw.get(key))
            except Exception:
                pass
    for key in (
        "ingress_audit_baseline_minutes",
        "ingress_audit_min_samples",
        "ingress_audit_history_minutes",
        "dynamic_trigger_lookback",
        "fake_breakout_min_flags",
        "liquidation_spike_min_fake_flags",
        "slow_accum_min_hits",
        "slow_accum_tier2_min_hits",
        "slow_accum_tier3_min_hits",
        "slow_accum_alpha_hits",
        "fake_breakout_probe_min_hits",
        "fake_breakout_probe_push_min_hits",
        "whale_testing_min_hits",
        "tier2_confirm_min_checks",
        "symbol_new_listing_days",
        "auto_tune_window_rows",
        "auto_tune_min_samples",
        "symbol_quality_window_rows",
        "symbol_quality_min_samples",
        "sector_cluster_min_count",
        "regime_window_rows",
        "regime_min_samples",
        "sentiment_funding_div_streak_warn",
        "discord_module_stats_top_n",
    ):
        if key in raw:
            try:
                out[key] = int(raw.get(key))
            except Exception:
                pass
    for key in ("tier_decision_file", "auto_tune_log_file", "signal_db_file", "aster_signal_db_file"):
        if key in raw:
            try:
                out[key] = str(raw.get(key) or "").strip()
            except Exception:
                pass
    if "bayesian_reason_override" in raw:
        try:
            out["bayesian_reason_override"] = str(raw.get("bayesian_reason_override") or "").strip()
        except Exception:
            pass
    if "ingress_force_symbols" in raw:
        try:
            out["ingress_force_symbols"] = sorted(_parse_symbol_collection(raw.get("ingress_force_symbols")))
        except Exception:
            pass

    modules = raw.get("modules", {})
    if not isinstance(modules, dict):
        modules = {}
    for key in CONTROL_BOOL_KEYS:
        if key in raw:
            out[key] = _to_bool(raw.get(key))
        elif key in modules:
            out[key] = _to_bool(modules.get(key))
    if out:
        print(f"[control] loaded overrides path={path} keys={sorted(out.keys())}")
    return out


@dataclass(frozen=True)
class Config:
    strategy_version: str = "1.1"
    ws_url: str = "wss://fstream.binance.com/ws/!ticker@arr"
    aster_ws_enabled: bool = False
    aster_ws_url: str = "wss://fstream.asterdex.com/ws/!ticker@arr"
    aster_exchange_info_url: str = "https://fapi.asterdex.com/fapi/v1/exchangeInfo"
    aster_rest_base_url: str = "https://fapi.asterdex.com"
    aster_symbols_reload_sec: float = 1800.0
    aster_only_usdt_contracts: bool = True
    workers: int = 3
    queue_maxsize: int = 2000
    min_pump_pct: float = 1.5
    min_pump_pct_with_volume: float = 0.8
    min_volume_1m: float = 500_000.0
    ingress_audit_enabled: bool = True
    ingress_audit_discord_enabled: bool = False
    ingress_audit_baseline_minutes: int = 20
    ingress_audit_min_samples: int = 6
    ingress_audit_history_minutes: int = 240
    ingress_audit_min_baseline_count: float = 8.0
    ingress_audit_low_ratio: float = 0.35
    ingress_audit_volume_spike_ratio: float = 2.5
    ingress_audit_alert_cooldown_sec: float = 900.0
    ingress_force_record_enabled: bool = True
    ingress_force_symbols: Tuple[str, ...] = ("BLURUSDT",)
    ingress_force_min_ret_1m_pct: float = 0.25
    ingress_force_min_vol_ratio_1m: float = 1.20
    ingress_force_record_cooldown_sec: float = 30.0
    flow_pump_min_ret_1m_pct: float = 0.6
    flow_pump_min_volume_1m: float = 100_000.0
    flow_pump_min_vol_ratio_1m: float = 3.0

    # coarse early momentum gate
    early_pump_pct_1m: float = 0.35
    early_volume_1m: float = 120_000.0
    early_pump_pct_20s: float = 0.18
    early_volume_20s: float = 40_000.0
    dynamic_trigger_enabled: bool = True
    dynamic_trigger_lookback: int = 240
    dynamic_trigger_quantile: float = 0.90
    dynamic_trigger_anchor_pct: float = 0.25
    dynamic_trigger_min_scale: float = 0.65
    dynamic_trigger_max_scale: float = 1.35
    dynamic_min_pump_pct: float = 0.55
    dynamic_min_pump_pct_with_volume: float = 0.25
    dynamic_trigger_use_micro: bool = True
    dynamic_trigger_event_factor_enabled: bool = True
    dynamic_trigger_event_major_factor: float = 1.12
    dynamic_trigger_event_minor_factor: float = 1.05

    # slow breakout capture (avoid missing sustained ladders)
    slow_breakout_enabled: bool = True
    slow_breakout_ret_1m_pct: float = 0.30
    slow_breakout_ret_5m_pct: float = 1.20
    slow_breakout_min_vol_ratio_1m: float = 0.80
    slow_breakout_min_oi5m_pct: float = 0.60
    slow_breakout_min_delta_persist: float = 0.40

    # fake breakout guard
    fake_breakout_filter_enabled: bool = True
    fake_breakout_ret_1m_pct: float = 0.30
    fake_breakout_min_vol_ratio_1m: float = 1.05
    fake_breakout_min_taker_buy_ratio: float = 0.50
    fake_breakout_min_buy_imbalance: float = 0.0
    fake_breakout_max_oi5m_pct: float = 0.2
    fake_breakout_pullback_pct: float = 0.45
    fake_breakout_delta_persist_max: float = 0.35
    fake_breakout_min_flags: int = 3
    liquidation_spike_enabled: bool = True
    liquidation_spike_ret_1m_pct: float = 1.80
    liquidation_spike_min_vol_ratio_1m: float = 3.00
    liquidation_spike_max_oi5m_pct: float = 0.30
    liquidation_spike_min_fake_flags: int = 2
    liquidation_spike_timer_sec: float = 900.0
    liquidation_spike_retrace_pct: float = 50.0
    liquidation_spike_stabilize_abs_ret_1m_pct: float = 0.35
    liquidation_spike_stabilize_max_vol_ratio_1m: float = 1.20
    liquidation_spike_confirm_min_oi5m_pct: float = 0.50
    liquidation_spike_follow_window_sec: float = 1800.0
    liquidation_spike_check_interval_sec: float = 5.0
    liquidation_spike_alert_cooldown_sec: float = 3600.0
    liquidation_spike_min_taker_buy_ratio: float = 0.58
    liquidation_spike_min_buy_imbalance: float = 0.08
    slow_accum_enabled: bool = True
    slow_accum_window_sec: float = 5400.0
    slow_accum_min_hits: int = 100
    slow_accum_tier2_min_hits: int = 200
    slow_accum_tier3_min_hits: int = 121
    slow_accum_tier3_realtime: bool = True
    slow_accum_min_ret_1m_pct: float = 0.25
    slow_accum_max_ret_1m_pct: float = 0.80
    slow_accum_min_stair_step_pct: float = 0.50
    slow_accum_alert_cooldown_sec: float = 3600.0
    slow_accum_alpha_ret_1m_pct: float = 0.406
    slow_accum_alpha_hits: int = 200
    slow_accum_alpha_bonus: float = 10.0
    slow_accum_alpha_plus_bonus: float = 18.0
    fake_breakout_probe_enabled: bool = True
    fake_breakout_probe_window_sec: float = 4 * 3600.0
    fake_breakout_probe_min_hits: int = 50
    fake_breakout_probe_max_centroid_drop_pct: float = 2.0
    fake_breakout_probe_alert_cooldown_sec: float = 3600.0
    fake_breakout_probe_push_min_hits: int = 251
    fake_breakout_probe_push_max_ret_1m_pct: float = 0.335
    fake_breakout_probe_push_max_centroid_change_pct: float = -0.35
    fake_breakout_probe_bonus: float = 8.0
    fake_breakout_probe_alpha_bonus: float = 15.0
    fake_breakout_probe_tier3_realtime: bool = True
    probe_follow_breakout_window_sec: float = 3600.0
    probe_follow_whale_bonus: float = 12.0
    probe_follow_waive_fake_breakout_once: bool = True
    module_signal_cooldown_sec: float = 1200.0
    whale_testing_enabled: bool = True
    whale_testing_window_sec: float = 4 * 3600.0
    whale_testing_min_hits: int = 50
    whale_testing_max_drawdown_pct: float = 3.0
    whale_testing_ttl_sec: float = 4 * 3600.0
    whale_bonus_enabled: bool = True
    whale_bonus_score: float = 15.0
    whale_waive_fake_breakout_penalty: bool = False
    whale_waive_top_risk_penalty: bool = False

    # ignition: 3-of-4 rule (ret/vol/cvd/oi)
    ignition_pump_1m: float = 0.18
    ignition_pump_20s: float = 0.10
    ignition_vol_ratio_1m: float = 1.6
    ignition_cvd_z: float = 1.8
    ignition_cvd_decay_tau: float = 6.0
    ignition_oi5m_pct: float = 0.4
    ignition_min_signals: int = 2
    ignition_bear_veto_enabled: bool = True
    ignition_bear_veto_cvd_z: float = -1.5
    ignition_bear_veto_oi5m: float = 0.5
    ignition_3of4_blocked: bool = False
    ignition_min_buy_imbalance: float = 0.20
    ignition_max_fake_flags: int = 1
    micro_cache_ttl_sec: float = 8.0
    ignition_watch_ttl_sec: float = 120.0

    # structure filter (support/resistance)
    sr_filter_enabled: bool = True
    sr_lookback_5m: int = 72
    sr_breakout_dist_pct: float = 1.8
    sr_support_dist_pct: float = 1.5
    sr_max_compression_pct: float = 4.5
    sr_soft_pass_enabled: bool = True
    sr_soft_min_hit: int = 3
    sr_soft_min_oi5m_pct: float = 1.0

    # cross-exchange filter (Bybit)
    cross_filter_enabled: bool = True
    cross_ret_1m_pct: float = 0.08
    cross_vol_ratio_1m: float = 1.2
    cross_grace_no_data: bool = True

    # free flow filter (public endpoints)
    flow_filter_enabled: bool = True
    flow_buy_ratio_min: float = 0.54
    flow_imbalance_min: float = 0.05
    flow_lsr_5m_min_pct: float = 0.3
    flow_min_signals: int = 2
    flow_grace_no_data: bool = True
    flow_relax_enabled: bool = True
    flow_relax_ret_1m_pct: float = 0.35
    flow_relax_oi5m_pct: float = 0.8
    flow_relax_min_signals: int = 1

    # onchain filter (DexScreener + GeckoTerminal)
    onchain_filter_enabled: bool = True
    onchain_symbols: Tuple[str, ...] = ("AINUSDT",)
    onchain_targets_file: str = "config/onchain_targets.json"
    onchain_targets_reload_sec: float = 10.0
    onchain_sector_file: str = "config/onchain_sectors.json"
    onchain_sector_reload_sec: float = 30.0
    onchain_ret_5m_pct: float = 0.25
    onchain_vol_ratio_5m: float = 1.8
    onchain_min_signals: int = 1
    onchain_grace_no_data: bool = True
    onchain_auto_add_enabled: bool = True
    onchain_auto_add_vol_ratio_1m: float = 5.0
    onchain_auto_add_min_volume_1m: float = 200_000.0
    onchain_auto_add_ttl_sec: float = 86_400.0
    onchain_auto_add_discord_enabled: bool = False
    onchain_auto_add_discord_batch_window_sec: float = 3.0
    onchain_auto_add_discord_max_rows: int = 15
    onchain_sector_auto_expand_enabled: bool = True
    onchain_sector_expand_ttl_sec: float = 86_400.0
    onchain_sector_min_quote_volume_24h: float = 1_000_000.0
    onchain_sector_max_peers_per_seed: int = 5
    onchain_priority_enabled: bool = True
    onchain_high_worker_cooldown_sec: float = 0.05
    onchain_discovery_ttl_sec: float = 1800.0
    onchain_micro_ttl_sec: float = 20.0
    onchain_min_liquidity_usd: float = 20_000.0

    worker_cooldown_sec: float = 0.5
    worker_healthcheck_enabled: bool = True
    worker_healthcheck_interval_sec: float = 10.0
    worker_healthcheck_stall_sec: float = 60.0
    worker_healthcheck_min_triggers: int = 1
    queue_symbol_cooldown_sec: float = 10.0
    reconnect_base_sec: float = 1.0
    reconnect_max_sec: float = 30.0
    ping_interval: float = 20.0
    ping_timeout: float = 20.0
    cache_window_sec: float = 60.0
    alert_cooldown_sec: float = 180.0
    alert_cooldown_low_sec: float = 90.0
    discord_min_score: float = 50.0
    discord_realtime_score: float = 60.0
    discord_summary_score: float = 45.0
    discord_watch_min_hit: int = 2
    discord_summary_interval_sec: float = 300.0
    alert_score_breakthrough: float = 12.0
    discord_min_interval_sec: float = 0.35
    discord_retry_max: int = 2
    disable_three_layer_filter: bool = False
    discord_heartbeat_enabled: bool = False
    discord_heartbeat_interval_sec: float = 900.0
    discord_heartbeat_content: str = "初号机运行中"
    discord_module_stats_enabled: bool = False
    discord_module_stats_interval_sec: float = 7200.0
    discord_module_stats_window_sec: float = 86400.0
    discord_module_stats_top_n: int = 10
    discord_webhook_url: str = ""
    aster_discord_webhook_url: str = ""
    discord_username: str = "Futures Monitor"
    control_file: str = "config/monitor_ws_control.json"
    webhook_config_file: str = "config/webhook_monitor_ws.json"
    discord_archive_enabled: bool = True
    discord_archive_file: str = "data/logs/monitor_ws_discord_push.jsonl"
    signal_db_enabled: bool = True
    signal_db_file: str = "data/logs/monitor_ws_signals.db"
    aster_signal_db_file: str = "data/logs/monitor_ws_signals_aster.db"

    # tier decision and delivery
    decision_tier_enabled: bool = True
    tier1_score: float = 85.0
    tier2_score: float = 65.0
    tier3_score: float = 45.0
    tier2_confirm_k_sec: float = 60.0
    tier2_confirm_pullback_pct: float = 0.45
    tier2_confirm_min_checks: int = 1
    tier3_discord_push_enabled: bool = False
    sniper_risk_downgrade_enabled: bool = False
    sniper_risk_downgrade_penalty: float = 20.0
    sniper_risk_drop_on_heavy_fake_breakout: bool = True
    sniper_risk_drop_penalty: float = 35.0
    tier_decision_file: str = "data/logs/monitor_ws_tier_decisions.jsonl"

    # symbol adaptation
    symbol_adapt_enabled: bool = True
    symbol_liq_high_24h: float = 200_000_000.0
    symbol_liq_mid_24h: float = 20_000_000.0
    symbol_new_listing_days: int = 7

    # mtf resonance
    mtf_resonance_enabled: bool = True

    # risk layer
    risk_liquidity_enabled: bool = True
    risk_slippage_enabled: bool = True
    risk_cross_anomaly_enabled: bool = True
    risk_spread_bps_max: float = 12.0
    risk_depth_min_usd: float = 8000.0
    risk_slippage_notional_usd: float = 5000.0
    risk_slippage_max_pct: float = 0.15
    risk_cross_anomaly_pct: float = 0.6
    risk_penalty_liquidity: float = 10.0
    risk_penalty_slippage: float = 10.0
    risk_penalty_cross_anomaly: float = 6.0
    risk_penalty_fake_breakout: float = 12.0
    risk_penalty_iceberg: float = 4.0

    # Bayesian dynamic scorer
    bayesian_scorer_enabled: bool = True
    bayesian_prior_max: float = 0.50
    bayesian_prior_min: float = 0.15
    bayesian_prior_decay_hits: float = 100.0
    bayesian_evidence_a_pump20s_pct: float = 3.0
    bayesian_evidence_b_funding_rate_lt: float = 0.0
    bayesian_evidence_b_funding_delta_lt: float = -0.0001
    bayesian_evidence_c_oi5m_pct_le: float = 0.0
    bayesian_p_ea_given_h: float = 0.90
    bayesian_p_eb_given_h: float = 0.85
    bayesian_p_ec_given_h: float = 0.90
    bayesian_p_ea_given_not_h: float = 0.10
    bayesian_p_eb_given_not_h: float = 0.20
    bayesian_p_ec_given_not_h: float = 0.20
    bayesian_override_posterior_threshold: float = 0.80
    bayesian_score_multiplier: float = 1.20
    bayesian_block_fake_weak_oi_penalty: bool = True
    bayesian_block_funding_divergence_penalty: bool = True
    bayesian_force_sniper_enabled: bool = False
    bayesian_reason_override: str = "short_squeeze_ignition"

    # ML shadow rerank (non-blocking)
    ml_shadow_enabled: bool = True
    ml_shadow_downgrade_prob: float = 0.30
    ml_shadow_upgrade_prob: float = 0.75
    ml_shadow_penalty: float = 8.0
    ml_shadow_bonus: float = 5.0

    # auto tuning (closed-loop adaptation)
    auto_tune_enabled: bool = True
    auto_tune_apply_enabled: bool = True
    auto_tune_persist_control: bool = False
    auto_tune_interval_sec: float = 300.0
    auto_tune_window_rows: int = 180
    auto_tune_min_samples: int = 30
    auto_tune_cooldown_sec: float = 3600.0
    auto_tune_win_rate_high: float = 0.70
    auto_tune_win_rate_low: float = 0.40
    auto_tune_tier_step_up: float = 2.0
    auto_tune_tier_step_down: float = 3.0
    auto_tune_tier2_min: float = 55.0
    auto_tune_tier2_max: float = 80.0
    auto_tune_log_file: str = "data/logs/monitor_ws_auto_tune.jsonl"

    # historical symbol quality (Bayesian-smoothed)
    symbol_quality_enabled: bool = True
    symbol_quality_window_rows: int = 500
    symbol_quality_min_samples: int = 20
    symbol_quality_alpha: float = 4.0
    symbol_quality_beta: float = 4.0
    symbol_quality_floor: float = 0.70
    symbol_quality_cap: float = 1.25

    # sector signal clustering
    sector_cluster_enabled: bool = True
    sector_cluster_window_sec: float = 180.0
    sector_cluster_min_count: int = 3
    sector_cluster_boost_factor: float = 1.30
    sector_cluster_ttl_sec: float = 900.0
    sector_cluster_min_base_score: float = 20.0

    # anomaly / regime learning
    regime_pattern_enabled: bool = True
    regime_window_rows: int = 500
    regime_min_samples: int = 20
    regime_win_rate_high: float = 0.62
    regime_win_rate_low: float = 0.42
    regime_factor_boost: float = 1.05
    regime_factor_cut: float = 0.92
    regime_morning_factor: float = 1.03
    regime_friday_night_factor: float = 0.92

    # market sentiment extensions
    sentiment_enabled: bool = True
    sentiment_whale_notional_usd: float = 100_000.0
    sentiment_lsr_weight: float = 0.06
    sentiment_whale_weight: float = 0.07
    sentiment_concentration_weight: float = 0.06
    sentiment_funding_div_weight: float = 0.05
    sentiment_funding_div_streak_warn: int = 3

    # advanced ten-module switches
    mod_trend_seg_enabled: bool = True
    mod_combo_enabled: bool = True
    mod_cvd_structure_enabled: bool = True
    mod_aux_indicators_enabled: bool = True
    mod_flow_mtf_enabled: bool = True
    mod_scorecard_enabled: bool = True
    mod_contradiction_enabled: bool = True
    mod_alert_digest_enabled: bool = True
    mod_backtest_metrics_enabled: bool = True
    mod_trade_plan_enabled: bool = True


ALERT_LEVEL_RANK: Dict[str, int] = {
    "none": 0,
    "WATCH": 1,
    "normal": 2,
    "HOT": 3,
    "SNIPER": 4,
}
ALERT_FREQ_WINDOW_SEC = 3600.0
ALERT_WAVE_RESET_SEC = 6 * 3600.0
LIVE_PNL_HORIZONS: Tuple[Tuple[int, str], ...] = (
    (300, "5m"),
    (900, "15m"),
    (1800, "30m"),
)
LIVE_PNL_CSV_FIELDS: Tuple[str, ...] = (
    "signal_id",
    "symbol",
    "time_bjt",
    "entry_ts",
    "entry_price",
    "side",
    "level",
    "tier",
    "score",
    "final_score",
    "threshold",
    "reason",
    "ret_5m_pct",
    "ret_15m_pct",
    "ret_30m_pct",
    "price_5m",
    "price_15m",
    "price_30m",
    "mfe_30m_pct",
    "mae_30m_pct",
    "close_ts",
    "close_time_bjt",
)


class WSMonitor:
    def __init__(self, cfg: Config, analyzer: AnalyzerFunc, exchange: Optional[Any]):
        self.cfg = cfg
        self.analyzer = analyzer
        self.exchange = exchange
        self.bayesian_scorer: Optional[BayesianScorer] = None
        if bool(cfg.bayesian_scorer_enabled):
            self.bayesian_scorer = BayesianScorer(
                BayesianScorerConfig(
                    prior_max=float(cfg.bayesian_prior_max),
                    prior_min=float(cfg.bayesian_prior_min),
                    prior_decay_hits=float(cfg.bayesian_prior_decay_hits),
                    evidence_a_pump20s_pct=float(cfg.bayesian_evidence_a_pump20s_pct),
                    evidence_b_funding_rate_lt=float(cfg.bayesian_evidence_b_funding_rate_lt),
                    evidence_b_funding_delta_lt=float(cfg.bayesian_evidence_b_funding_delta_lt),
                    evidence_c_oi5m_pct_le=float(cfg.bayesian_evidence_c_oi5m_pct_le),
                    p_ea_given_h=float(cfg.bayesian_p_ea_given_h),
                    p_eb_given_h=float(cfg.bayesian_p_eb_given_h),
                    p_ec_given_h=float(cfg.bayesian_p_ec_given_h),
                    p_ea_given_not_h=float(cfg.bayesian_p_ea_given_not_h),
                    p_eb_given_not_h=float(cfg.bayesian_p_eb_given_not_h),
                    p_ec_given_not_h=float(cfg.bayesian_p_ec_given_not_h),
                )
            )
        half = max(1, cfg.queue_maxsize // 2)
        self.analysis_queue_high: asyncio.Queue[str] = asyncio.Queue(maxsize=half)
        self.analysis_queue_normal: asyncio.Queue[str] = asyncio.Queue(maxsize=cfg.queue_maxsize)
        self.queue_activity_event = asyncio.Event()

        self.price_cache: Dict[str, Deque[Tuple[float, float]]] = defaultdict(deque)
        self.qvol_cache: Dict[str, Deque[Tuple[float, float]]] = defaultdict(deque)
        self.vol1m_history: Dict[str, Deque[float]] = defaultdict(deque)
        self.pump1m_history: Dict[str, Deque[float]] = defaultdict(deque)
        self.oi_point_cache_by_source: Dict[str, Dict[str, Deque[Tuple[float, float]]]] = defaultdict(
            lambda: defaultdict(deque)
        )
        self.oi_fetch_warn_last_ts: Dict[str, float] = {}
        self.latest_price: Dict[str, float] = {}
        self.latest_qvol24h: Dict[str, float] = {}
        self.ingress_force_symbols_rt: Set[str] = {
            s.strip().upper() for s in cfg.ingress_force_symbols if str(s or "").strip()
        }
        self.ingress_minute_bucket_by_source: Dict[str, int] = {}
        self.ingress_minute_counts_by_source: Dict[str, Dict[str, int]] = defaultdict(dict)
        self.ingress_minute_volmax_by_source: Dict[str, Dict[str, float]] = defaultdict(dict)
        self.ingress_history_by_source: Dict[str, Dict[str, Deque[Tuple[int, int, float]]]] = defaultdict(
            lambda: defaultdict(deque)
        )
        self.ingress_anomaly_last_alert_ts: Dict[str, float] = {}
        self.ingress_force_last_record_ts: Dict[str, float] = {}
        self.last_enqueued_at: Dict[str, float] = {}
        self.in_queue_or_running: Set[str] = set()
        self.alert_state: Dict[str, Tuple[float, int, float]] = {}
        self.alert_1h_timestamps: Dict[str, Deque[float]] = defaultdict(deque)
        self.alert_wave_state: Dict[str, Dict[str, Any]] = {}
        self.trigger_meta: Dict[str, Dict[str, Any]] = {}
        self.micro_cache: Dict[str, Tuple[float, Dict[str, float]]] = {}
        self.onchain_pool_cache: Dict[str, Tuple[float, Optional[Dict[str, Any]]]] = {}
        self.onchain_micro_cache: Dict[str, Tuple[float, Dict[str, float]]] = {}
        self.onchain_cli_exact: Set[str] = set(cfg.onchain_symbols)
        self.onchain_file_exact: Set[str] = set()
        self.onchain_patterns: Set[str] = set()
        self.onchain_auto_expire: Dict[str, float] = {}
        self.onchain_tau_overrides: Dict[str, float] = {}
        self.onchain_targets_path = Path(cfg.onchain_targets_file).expanduser()
        self.onchain_targets_mtime: float = 0.0
        self.onchain_targets_next_reload_ts: float = 0.0
        self.onchain_auto_add_enabled_rt: bool = cfg.onchain_auto_add_enabled
        self.onchain_auto_add_vol_ratio_rt: float = cfg.onchain_auto_add_vol_ratio_1m
        self.onchain_auto_add_min_volume_rt: float = cfg.onchain_auto_add_min_volume_1m
        self.onchain_auto_add_ttl_rt: float = cfg.onchain_auto_add_ttl_sec
        self.onchain_auto_add_discord_enabled_rt: bool = cfg.onchain_auto_add_discord_enabled
        self.onchain_auto_add_discord_batch_window_rt: float = cfg.onchain_auto_add_discord_batch_window_sec
        self.onchain_auto_add_discord_max_rows_rt: int = cfg.onchain_auto_add_discord_max_rows
        self.onchain_auto_add_discord_buffer: List[Tuple[str, str, float, float]] = []
        self.onchain_auto_add_discord_flush_task: Optional[asyncio.Task[Any]] = None
        self.onchain_sector_path = Path(cfg.onchain_sector_file).expanduser()
        self.onchain_sector_mtime: float = 0.0
        self.onchain_sector_next_reload_ts: float = 0.0
        self.onchain_sector_by_symbol: Dict[str, str] = {}
        self.onchain_sector_symbols: Dict[str, Set[str]] = {}
        self.onchain_sector_patterns: List[Tuple[str, str]] = []
        self.discord_send_lock = asyncio.Lock()
        self.discord_next_send_ts: float = 0.0
        self.discord_summary_buffer: Dict[str, Dict[str, Any]] = {}
        self.discord_summary_flush_task: Optional[asyncio.Task[Any]] = None
        self.aster_contract_symbols: Set[str] = set()
        self.aster_symbols_next_reload_ts: float = 0.0
        self.live_pnl_dir = Path("data/logs")
        self.live_pnl_file = self.live_pnl_dir / "monitor_ws_live_pnl.csv"
        self.live_pnl_summary_file = self.live_pnl_dir / "monitor_ws_live_pnl_summary.json"
        self.auto_tune_log_file = Path(str(cfg.auto_tune_log_file or "data/logs/monitor_ws_auto_tune.jsonl")).expanduser()
        self.control_file_path = Path(str(cfg.control_file or "config/monitor_ws_control.json")).expanduser()
        self.discord_archive_enabled_rt = bool(cfg.discord_archive_enabled)
        self.discord_archive_file = Path(str(cfg.discord_archive_file or "data/logs/monitor_ws_discord_push.jsonl")).expanduser()
        self.signal_db_enabled_rt = bool(cfg.signal_db_enabled)
        self.signal_db_file = Path(str(cfg.signal_db_file or "data/logs/monitor_ws_signals.db")).expanduser()
        self.aster_signal_db_file = Path(
            str(cfg.aster_signal_db_file or "data/logs/monitor_ws_signals_aster.db")
        ).expanduser()
        self.signal_db_conn: Optional[sqlite3.Connection] = None
        self.signal_db_aster_conn: Optional[sqlite3.Connection] = None
        self.signal_db_lock = threading.Lock()
        self.tier_decision_file = Path(str(cfg.tier_decision_file or "data/logs/monitor_ws_tier_decisions.jsonl")).expanduser()
        self.live_pnl_open: Dict[str, Dict[str, Any]] = {}
        self.live_pnl_open_by_symbol: Dict[str, Set[str]] = defaultdict(set)
        self.live_pnl_seq: int = 0
        self.live_pnl_summary_state: Dict[str, Any] = self._default_live_pnl_summary_state()
        self.pending_tier2: Dict[str, Dict[str, Any]] = {}
        self.exchange_info_cache_ts: float = 0.0
        self.exchange_info_symbols: Dict[str, Dict[str, Any]] = {}
        self.atr15m_pct_history: Dict[str, Deque[float]] = defaultdict(deque)
        self.vol1h_pct_history: Dict[str, Deque[float]] = defaultdict(deque)
        self.last_trigger_ts: float = 0.0
        self.last_analysis_ts: float = 0.0
        self.trigger_count_since_analysis: int = 0
        self.worker_stall_active: bool = False
        self.last_worker_stall_alert_ts: float = 0.0
        self.tier1_score_rt: float = float(cfg.tier1_score)
        self.tier2_score_rt: float = float(cfg.tier2_score)
        self.tier3_score_rt: float = float(cfg.tier3_score)
        self.tier_gap_12: float = max(1.0, float(cfg.tier1_score) - float(cfg.tier2_score))
        self.tier_gap_23: float = max(1.0, float(cfg.tier2_score) - float(cfg.tier3_score))
        self.last_auto_tune_ts: float = 0.0
        self.last_auto_tune_adjust_ts: float = 0.0
        self.symbol_quality_factor_map: Dict[str, float] = {}
        self.symbol_quality_count_map: Dict[str, int] = {}
        self.regime_hour_factor_map: Dict[int, float] = {}
        self.regime_hour_count_map: Dict[int, int] = {}
        self.sector_recent_events: Dict[str, Deque[Tuple[float, str]]] = defaultdict(deque)
        self.sector_last_symbol_ts: Dict[Tuple[str, str], float] = {}
        self.sector_boost_until: Dict[str, float] = {}
        self.funding_divergence_streak: Dict[str, int] = defaultdict(int)
        self.liquidation_spike_state: Dict[str, Dict[str, Any]] = {}
        self.liquidation_spike_last_alert_ts: Dict[str, float] = {}
        self.slow_accum_events: Dict[str, Deque[Tuple[float, float, float, float]]] = defaultdict(deque)
        self.slow_accum_last_alert_ts: Dict[str, float] = {}
        self.fake_breakout_probe_events: Dict[str, Deque[Tuple[float, float]]] = defaultdict(deque)
        self.fake_breakout_probe_last_alert_ts: Dict[str, float] = {}
        self.fake_breakout_probe_last_record_ts: Dict[str, float] = {}
        self.probe_alpha_active: Dict[str, Dict[str, float]] = {}
        self.whale_testing_events: Dict[str, Deque[Tuple[float, float]]] = defaultdict(deque)
        self.whale_testing_price_high: Dict[str, Deque[Tuple[float, float]]] = defaultdict(deque)
        self.whale_testing_active: Dict[str, Dict[str, float]] = {}

        self._init_live_pnl_logging()
        self._init_auto_tune_logging()
        self._init_discord_archive_logging()
        self._init_signal_db()
        self._restore_runtime_state_from_signal_db()
        self._restore_live_pnl_open_from_db()
        self._init_tier_decision_logging()
        self._reload_onchain_targets_if_due(force=True)
        self._reload_onchain_sectors_if_due(force=True)
        self._reload_aster_contract_symbols_if_due(force=True)
        try:
            self._refresh_adaptive_models()
        except Exception as exc:
            print(f"[auto-tune] warmup failed: {exc!r}")

    def _push_rolling(self, cache: Dict[str, Deque[Tuple[float, float]]], symbol: str, ts: float, value: float) -> None:
        dq = cache[symbol]
        dq.append((ts, value))
        cutoff = ts - self.cfg.cache_window_sec
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def _realtime_pump_pct(self, symbol: str, ts: float, price: float) -> float:
        self._push_rolling(self.price_cache, symbol, ts, price)
        dq = self.price_cache[symbol]
        if len(dq) < 2:
            return 0.0
        old_price = dq[0][1]
        if old_price <= 0:
            return 0.0
        return (price - old_price) / old_price * 100.0

    def _volume_1m(self, symbol: str, ts: float, qvol_24h: float) -> float:
        self._push_rolling(self.qvol_cache, symbol, ts, qvol_24h)
        dq = self.qvol_cache[symbol]
        if len(dq) < 2:
            return 0.0
        oldest = dq[0][1]
        newest = dq[-1][1]
        if newest >= oldest:
            return newest - oldest
        # 24h rolling volume reset case
        return newest

    def _update_vol1m_history(self, symbol: str, vol_1m: float) -> None:
        dq = self.vol1m_history[symbol]
        dq.append(max(0.0, vol_1m))
        while len(dq) > 180:
            dq.popleft()

    def _update_pump1m_history(self, symbol: str, pump_1m_pct: float) -> None:
        dq = self.pump1m_history[symbol]
        dq.append(float(pump_1m_pct))
        keep = max(30, int(self.cfg.dynamic_trigger_lookback))
        while len(dq) > keep:
            dq.popleft()

    def _roll_ingress_minute_if_needed(self, source: str, ts: float) -> None:
        src = str(source or "binance").strip().lower() or "binance"
        minute_bucket = int(float(ts) // 60.0)
        prev_bucket = self.ingress_minute_bucket_by_source.get(src)
        if prev_bucket is None:
            self.ingress_minute_bucket_by_source[src] = minute_bucket
            return
        if minute_bucket <= prev_bucket:
            return
        prev_counts = dict(self.ingress_minute_counts_by_source.get(src, {}))
        prev_vols = dict(self.ingress_minute_volmax_by_source.get(src, {}))
        self._finalize_ingress_minute(src, prev_bucket, prev_counts, prev_vols)
        self.ingress_minute_bucket_by_source[src] = minute_bucket
        self.ingress_minute_counts_by_source[src] = {}
        self.ingress_minute_volmax_by_source[src] = {}

    def _record_ingress_tick(self, source: str, symbol: str, ts: float, vol_1m: float) -> None:
        if not bool(self.cfg.ingress_audit_enabled):
            return
        src = str(source or "binance").strip().lower() or "binance"
        sym = str(symbol or "").strip().upper()
        if not sym:
            return
        self._roll_ingress_minute_if_needed(src, ts)
        counts = self.ingress_minute_counts_by_source[src]
        counts[sym] = int(counts.get(sym, 0)) + 1
        volmax = self.ingress_minute_volmax_by_source[src]
        vol_1m_v = max(0.0, float(vol_1m))
        prev_v = float(volmax.get(sym, 0.0) or 0.0)
        if vol_1m_v >= prev_v:
            volmax[sym] = vol_1m_v

    def _prune_ingress_history(self, dq: Deque[Tuple[int, int, float]], minute_bucket: int) -> None:
        keep_minutes = max(60, int(self.cfg.ingress_audit_history_minutes))
        cutoff_bucket = int(minute_bucket) - keep_minutes
        while dq and int(dq[0][0]) < cutoff_bucket:
            dq.popleft()
        max_len = keep_minutes + 4
        while len(dq) > max_len:
            dq.popleft()

    def _build_ingress_gap_discord_message(
        self,
        symbol: str,
        source: str,
        minute_bucket: int,
        count_now: int,
        baseline_count: float,
        vol_1m_now: float,
        baseline_vol_1m: float,
    ) -> str:
        src = str(source or "binance").strip().lower() or "binance"
        src_tag = "ASTER" if src == "aster" else "BINANCE"
        start_ts = float(minute_bucket) * 60.0
        end_ts = start_ts + 60.0
        lines = [
            f"⚠️ **Ingress Gap Watch** **{self._display_symbol(symbol)}** `{src_tag}`",
            f"time(BJT): {self._fmt_bjt(time.time())}",
            f"窗口: {self._fmt_bjt(start_ts)} ~ {self._fmt_bjt(end_ts)}",
            f"入流计数: {int(count_now)} / 基线 {baseline_count:.1f}",
            f"1m成交额: {vol_1m_now:,.0f} / 基线 {baseline_vol_1m:,.0f}",
            "说明: WS入流显著偏低，但成交额脉冲异常放大",
        ]
        return "\n".join(lines)[:1900]

    def _maybe_emit_ingress_gap_watch(
        self,
        *,
        symbol: str,
        source: str,
        minute_bucket: int,
        count_now: int,
        vol_1m_now: float,
        history: Deque[Tuple[int, int, float]],
    ) -> None:
        if not bool(self.cfg.ingress_audit_enabled):
            return
        baseline_minutes = max(5, int(self.cfg.ingress_audit_baseline_minutes))
        min_samples = max(1, int(self.cfg.ingress_audit_min_samples))
        rows = [row for row in list(history)[:-1] if int(row[1]) > 0]
        if not rows:
            return
        rows = rows[-baseline_minutes:]
        if len(rows) < min_samples:
            return
        count_samples = [float(row[1]) for row in rows]
        vol_samples = [max(0.0, float(row[2])) for row in rows if float(row[2]) > 0.0]
        if not count_samples:
            return
        baseline_count = float(statistics.median(count_samples))
        baseline_vol_1m = float(statistics.median(vol_samples)) if vol_samples else 0.0
        if baseline_count < max(1.0, float(self.cfg.ingress_audit_min_baseline_count)):
            return
        low_ratio = max(0.05, min(0.95, float(self.cfg.ingress_audit_low_ratio)))
        low_gate = max(1.0, baseline_count * low_ratio)
        if float(count_now) > low_gate:
            return
        vol_spike_ratio = max(1.0, float(self.cfg.ingress_audit_volume_spike_ratio))
        vol_gate = max(
            float(self.cfg.flow_pump_min_volume_1m),
            baseline_vol_1m * vol_spike_ratio,
        )
        if float(vol_1m_now) < vol_gate:
            return
        now = time.time()
        source_l = str(source or "binance").strip().lower() or "binance"
        key = f"{source_l}:{str(symbol).strip().upper()}"
        cd = max(120.0, float(self.cfg.ingress_audit_alert_cooldown_sec))
        if now - float(self.ingress_anomaly_last_alert_ts.get(key, 0.0) or 0.0) < cd:
            return
        self.ingress_anomaly_last_alert_ts[key] = now
        print(
            f"[ingress-gap] {symbol} source={source_l} count={int(count_now)} "
            f"baseline={baseline_count:.1f} vol_1m={float(vol_1m_now):,.0f} baseline_vol={baseline_vol_1m:,.0f}"
        )
        payload = {
            "type": "ingress_gap_watch",
            "trigger_source": source_l,
            "minute_bucket": int(minute_bucket),
            "window_start_bjt": self._fmt_bjt(float(minute_bucket) * 60.0),
            "window_end_bjt": self._fmt_bjt(float(minute_bucket) * 60.0 + 60.0),
            "ingress_count": float(count_now),
            "ingress_baseline_count": float(baseline_count),
            "vol_1m": float(vol_1m_now),
            "vol_1m_baseline": float(baseline_vol_1m),
            "ingress_low_ratio": float(low_ratio),
            "volume_spike_ratio": float(vol_spike_ratio),
        }
        self._persist_signal_event(
            ts=now,
            symbol=str(symbol).strip().upper(),
            reason="ingress_gap_watch",
            side="neutral",
            level="WATCH",
            alert=False,
            base_score=0.0,
            threshold=float(low_gate),
            tier="TIER4",
            delivery="record_only",
            final_score=0.0,
            hit_count=int(count_now),
            payload=payload,
            source=source_l,
        )
        if bool(self.cfg.ingress_audit_discord_enabled):
            msg = self._build_ingress_gap_discord_message(
                symbol=str(symbol).strip().upper(),
                source=source_l,
                minute_bucket=int(minute_bucket),
                count_now=int(count_now),
                baseline_count=float(baseline_count),
                vol_1m_now=float(vol_1m_now),
                baseline_vol_1m=float(baseline_vol_1m),
            )
            try:
                asyncio.get_running_loop().create_task(self._send_discord_webhook(msg, source=source_l))
            except Exception:
                pass

    def _finalize_ingress_minute(
        self,
        source: str,
        minute_bucket: int,
        counts: Dict[str, int],
        volmax: Dict[str, float],
    ) -> None:
        src = str(source or "binance").strip().lower() or "binance"
        symbols: Set[str] = set(str(k).strip().upper() for k in counts.keys())
        if src == "binance":
            symbols.update(self.ingress_force_symbols_rt)
        for symbol in symbols:
            if not symbol:
                continue
            cnt = int(counts.get(symbol, 0) or 0)
            vol_1m = max(0.0, float(volmax.get(symbol, 0.0) or 0.0))
            dq = self.ingress_history_by_source[src][symbol]
            dq.append((int(minute_bucket), int(cnt), float(vol_1m)))
            self._prune_ingress_history(dq, int(minute_bucket))
            if src == "binance":
                self._maybe_emit_ingress_gap_watch(
                    symbol=symbol,
                    source=src,
                    minute_bucket=int(minute_bucket),
                    count_now=int(cnt),
                    vol_1m_now=float(vol_1m),
                    history=dq,
                )

    def _maybe_record_force_chain_audit(
        self,
        *,
        symbol: str,
        ts: float,
        source: str,
        pump_pct: float,
        pump_20s: float,
        vol_1m: float,
        vol_ratio_1m: float,
        vol_z_1m: float,
        should_trigger: bool,
        trigger_reason: str,
        details: Optional[Dict[str, float]],
        micro: Optional[Dict[str, float]],
    ) -> None:
        if not bool(self.cfg.ingress_force_record_enabled):
            return
        source_l = str(source or "binance").strip().lower() or "binance"
        if source_l != "binance":
            return
        sym = str(symbol or "").strip().upper()
        if not sym or sym not in self.ingress_force_symbols_rt:
            return
        ret_gate = max(0.0, float(self.cfg.ingress_force_min_ret_1m_pct))
        vol_ratio_gate = max(0.0, float(self.cfg.ingress_force_min_vol_ratio_1m))
        if float(pump_pct) < ret_gate and float(vol_ratio_1m) < vol_ratio_gate:
            return
        key = f"{source_l}:{sym}"
        now = float(ts)
        cd = max(1.0, float(self.cfg.ingress_force_record_cooldown_sec))
        if now - float(self.ingress_force_last_record_ts.get(key, 0.0) or 0.0) < cd:
            return
        self.ingress_force_last_record_ts[key] = now
        m = micro or {}
        d = details or {}
        payload = {
            "type": "force_chain_audit",
            "trigger_source": source_l,
            "pump_1m_pct": float(pump_pct),
            "pump_20s_pct": float(pump_20s),
            "vol_1m": float(vol_1m),
            "vol_ratio_1m": float(vol_ratio_1m),
            "vol_z_1m": float(vol_z_1m),
            "should_trigger": 1.0 if bool(should_trigger) else 0.0,
            "trigger_reason": str(trigger_reason or ""),
            "fake_breakout": float(d.get("fake_breakout", 0.0) or 0.0),
            "fake_flags": float(d.get("fake_flags", 0.0) or 0.0),
            "ret_1m_pct_micro": float(m.get("ret_1m_pct", 0.0) or 0.0),
            "oi5m_pct": float(m.get("oi_change_5m_pct", 0.0) or 0.0),
            "cvd_z": float(m.get("cvd_z", 0.0) or 0.0),
            "buy_imbalance": float(m.get("buy_imbalance", 0.0) or 0.0),
            "taker_buy_ratio": float(m.get("taker_buy_ratio", 0.0) or 0.0),
        }
        self._persist_signal_event(
            ts=now,
            symbol=sym,
            reason="force_chain_audit",
            side="neutral",
            level="WATCH",
            alert=False,
            base_score=0.0,
            threshold=float(ret_gate),
            tier="TIER4",
            delivery="record_only",
            final_score=0.0,
            hit_count=int(d.get("n_hit", 0.0) or 0),
            payload=payload,
            source=source_l,
        )

    def _adaptive_trigger_thresholds(self, symbol: str, micro: Optional[Dict[str, float]], ts: float) -> Tuple[float, float, float, Dict[str, float]]:
        hard = float(self.cfg.min_pump_pct)
        flow = float(self.cfg.min_pump_pct_with_volume)
        meta: Dict[str, float] = {
            "dyn_market_factor": 1.0,
            "atr_factor": 1.0,
            "vol_factor": 1.0,
            "trend_factor": 1.0,
            "event_factor": 1.0,
        }
        if not self.cfg.dynamic_trigger_enabled:
            return hard, flow, 1.0, meta
        dq = self.pump1m_history.get(symbol)
        if not dq or len(dq) < 20:
            return hard, flow, 1.0, meta

        arr = np.asarray(list(dq), dtype=np.float64)
        if arr.size <= 0:
            return hard, flow, 1.0, meta
        q = min(0.99, max(0.50, float(self.cfg.dynamic_trigger_quantile)))
        baseline = float(np.quantile(np.abs(arr), q))
        anchor = max(0.01, float(self.cfg.dynamic_trigger_anchor_pct))
        pump_scale = baseline / anchor if anchor > 0 else 1.0
        market_factor = 1.0
        if self.cfg.dynamic_trigger_use_micro:
            market_factor, m_meta = self._market_factor_from_micro(symbol, micro, ts)
            meta.update({k: float(v) for k, v in m_meta.items() if isinstance(v, (float, int))})
        scale = pump_scale * market_factor
        scale = max(float(self.cfg.dynamic_trigger_min_scale), min(float(self.cfg.dynamic_trigger_max_scale), scale))

        hard = max(float(self.cfg.dynamic_min_pump_pct), hard * scale)
        flow = max(float(self.cfg.dynamic_min_pump_pct_with_volume), flow * scale)
        meta.update(
            {
                "dyn_baseline_1m_pct": baseline,
                "dyn_pump_scale": pump_scale,
                "dyn_market_factor": market_factor,
                "market_factor": market_factor,
            }
        )
        return hard, flow, scale, meta

    def _is_fake_breakout(
        self,
        micro: Optional[Dict[str, float]],
        *,
        ret_1m_pct: float,
        vol_ratio_1m: float,
        oi5m_pct: float,
    ) -> Tuple[bool, Dict[str, float]]:
        details: Dict[str, float] = {}
        if not self.cfg.fake_breakout_filter_enabled:
            details["fake_breakout"] = 0.0
            details["fake_flags"] = 0.0
            return False, details
        if float(ret_1m_pct) < float(self.cfg.fake_breakout_ret_1m_pct):
            details["fake_breakout"] = 0.0
            details["fake_flags"] = 0.0
            return False, details

        flags = 0
        weak_vol = float(vol_ratio_1m) < float(self.cfg.fake_breakout_min_vol_ratio_1m)
        weak_oi = float(oi5m_pct) <= float(self.cfg.fake_breakout_max_oi5m_pct)
        weak_flow = False
        weak_cross = False
        weak_sr = False
        weak_delta_persist = False
        weak_pullback = False

        if weak_vol:
            flags += 1
        if weak_oi:
            flags += 1

        m = micro or {}
        flow_available = float(m.get("flow_available", 0.0)) >= 0.5
        if flow_available:
            taker_buy_ratio = float(m.get("taker_buy_ratio", 0.0))
            buy_imbalance = float(m.get("buy_imbalance", 0.0))
            weak_flow = (
                taker_buy_ratio < float(self.cfg.fake_breakout_min_taker_buy_ratio)
                or buy_imbalance < float(self.cfg.fake_breakout_min_buy_imbalance)
            )
            if weak_flow:
                flags += 1

        cross_available = float(m.get("cross_available", 0.0)) >= 0.5
        if cross_available:
            weak_cross = float(m.get("cross_ok", 0.0)) < 0.5
            if weak_cross:
                flags += 1

        if self.cfg.sr_filter_enabled:
            sr_breakout = float(m.get("sr_breakout", 0.0)) >= 0.5
            sr_support = float(m.get("sr_support", 0.0)) >= 0.5
            sr_soft_pass = float(m.get("sr_soft_pass", 0.0)) >= 0.5
            weak_sr = (not sr_breakout) and (not sr_support) and (not sr_soft_pass)
            if weak_sr:
                flags += 1

        delta_persist = float(m.get("delta_persist_score", 0.0) or 0.0)
        weak_delta_persist = delta_persist <= float(self.cfg.fake_breakout_delta_persist_max)
        if weak_delta_persist:
            flags += 1

        pullback = float(m.get("breakout_pullback_pct", 0.0) or 0.0)
        weak_pullback = pullback >= float(self.cfg.fake_breakout_pullback_pct) and weak_oi
        if weak_pullback:
            flags += 1

        min_flags = max(1, int(self.cfg.fake_breakout_min_flags))
        fake_breakout = flags >= min_flags
        details.update(
            {
                "fake_breakout": 1.0 if fake_breakout else 0.0,
                "watch_only": 1.0 if fake_breakout else 0.0,
                "fake_flags": float(flags),
                "fake_weak_vol": 1.0 if weak_vol else 0.0,
                "fake_weak_oi": 1.0 if weak_oi else 0.0,
                "fake_weak_flow": 1.0 if weak_flow else 0.0,
                "fake_weak_cross": 1.0 if weak_cross else 0.0,
                "fake_weak_sr": 1.0 if weak_sr else 0.0,
                "fake_weak_delta_persist": 1.0 if weak_delta_persist else 0.0,
                "fake_weak_pullback": 1.0 if weak_pullback else 0.0,
            }
        )
        return fake_breakout, details

    def _volume_ratio_1m(self, symbol: str, current_vol_1m: float) -> float:
        dq = self.vol1m_history.get(symbol)
        if not dq or len(dq) < 12:
            return 0.0
        baseline = float(statistics.mean(list(dq)[-60:]))
        if baseline <= 1e-9:
            return 0.0
        return current_vol_1m / baseline

    def _volume_zscore_1m(self, symbol: str, current_vol_1m: float) -> float:
        dq = self.vol1m_history.get(symbol)
        if not dq or len(dq) < 20:
            return 0.0
        arr = list(dq)[-120:]
        mean_v = float(statistics.mean(arr))
        std_v = float(statistics.pstdev(arr))
        if std_v <= 1e-9:
            return 0.0
        return (current_vol_1m - mean_v) / std_v

    def _window_oldest(self, cache: Dict[str, Deque[Tuple[float, float]]], symbol: str, ts: float, window_sec: float) -> Optional[float]:
        dq = cache.get(symbol)
        if not dq:
            return None
        cutoff = ts - window_sec
        for t, v in dq:
            if t >= cutoff:
                return v
        return dq[0][1] if dq else None

    def _price_change_pct_window(self, symbol: str, ts: float, current_price: float, window_sec: float) -> float:
        old = self._window_oldest(self.price_cache, symbol, ts, window_sec)
        if old is None or old <= 0:
            return 0.0
        return (current_price - old) / old * 100.0

    def _volume_window(self, symbol: str, ts: float, current_qvol_24h: float, window_sec: float) -> float:
        old = self._window_oldest(self.qvol_cache, symbol, ts, window_sec)
        if old is None:
            return 0.0
        if current_qvol_24h >= old:
            return current_qvol_24h - old
        return current_qvol_24h

    @staticmethod
    def _http_get_json(url: str, timeout: float = 3.0) -> Any:
        req = urllib.request.Request(url, headers={"User-Agent": "monitor-ws-ignition/2.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    @staticmethod
    def _unwrap_list_payload(raw: Any) -> List[Any]:
        if isinstance(raw, list):
            return raw
        if not isinstance(raw, dict):
            return []

        cand: List[Any] = [
            raw.get("data"),
            raw.get("rows"),
            raw.get("list"),
            raw.get("result"),
        ]
        for item in cand:
            if isinstance(item, list):
                return item
            if isinstance(item, dict):
                for k in ("list", "rows", "data", "result"):
                    v = item.get(k)
                    if isinstance(v, list):
                        return v
        return []

    def _build_oi_hist_urls(self, symbol: str, rest_base: str) -> List[str]:
        params = urllib.parse.urlencode({"symbol": symbol, "period": "5m", "limit": 30})
        base = str(rest_base or "").rstrip("/")
        return [
            f"{base}/futures/data/openInterestHist?{params}",
            f"{base}/fapi/v1/openInterestHist?{params}",
        ]

    def _build_oi_now_urls(self, symbol: str, rest_base: str) -> List[str]:
        params = urllib.parse.urlencode({"symbol": symbol})
        base = str(rest_base or "").rstrip("/")
        return [
            f"{base}/fapi/v1/openInterest?{params}",
            f"{base}/futures/data/openInterest?{params}",
        ]

    def _http_get_json_first(self, urls: List[str], timeout: float = 4.0) -> Any:
        last_exc: Optional[Exception] = None
        for u in urls:
            try:
                raw = self._http_get_json(u, timeout=timeout)
                if self._unwrap_list_payload(raw):
                    return raw
                if isinstance(raw, dict):
                    if any(k in raw for k in ("openInterest", "sumOpenInterest", "sumOpenInterestValue", "data", "result")):
                        return raw
            except Exception as exc:
                last_exc = exc
                continue
        if last_exc is not None:
            raise last_exc
        return []

    @staticmethod
    def _normalize_ts(ts: float) -> float:
        t = float(ts or 0.0)
        if t <= 0:
            return 0.0
        if t > 1e12:
            return t / 1000.0
        if t > 1e10:
            return t / 1000.0
        return t

    def _extract_oi_value(self, row: Any) -> float:
        if isinstance(row, dict):
            for k in (
                "sumOpenInterestValue",
                "sumOpenInterest",
                "openInterest",
                "open_interest",
                "oi",
                "value",
            ):
                v = self._safe_float(row.get(k, 0.0), 0.0)
                if v > 0.0:
                    return v
            return 0.0
        if isinstance(row, (list, tuple)):
            nums: List[float] = []
            for item in row:
                try:
                    nums.append(float(item))
                except Exception:
                    continue
            if len(nums) >= 2 and nums[1] > 0.0:
                return float(nums[1])
            for v in nums:
                if v > 0.0:
                    return float(v)
        return 0.0

    def _extract_oi_ts(self, row: Any, fallback_ts: float) -> float:
        if isinstance(row, dict):
            for k in ("timestamp", "time", "ts", "T"):
                v = self._safe_float(row.get(k, 0.0), 0.0)
                if v > 0.0:
                    t = self._normalize_ts(v)
                    if t > 0.0:
                        return t
        elif isinstance(row, (list, tuple)) and row:
            v = self._safe_float(row[0], 0.0)
            if v > 0.0:
                t = self._normalize_ts(v)
                if t > 0.0:
                    return t
        return float(fallback_ts)

    def _extract_oi_points(self, raw: Any, fallback_ts: float) -> List[Tuple[float, float]]:
        rows = self._unwrap_list_payload(raw)
        if not rows:
            return []
        points: List[Tuple[float, float]] = []
        for r in rows:
            oi_v = self._extract_oi_value(r)
            if oi_v <= 0.0:
                continue
            ts_v = self._extract_oi_ts(r, fallback_ts)
            points.append((float(ts_v), float(oi_v)))
        points.sort(key=lambda x: x[0])
        return points

    def _extract_oi_now(self, raw: Any) -> float:
        if isinstance(raw, dict):
            for k in ("openInterest", "sumOpenInterestValue", "sumOpenInterest", "oi"):
                v = self._safe_float(raw.get(k, 0.0), 0.0)
                if v > 0.0:
                    return v
            for k in ("data", "result"):
                nested = raw.get(k)
                if isinstance(nested, dict):
                    for kk in ("openInterest", "sumOpenInterestValue", "sumOpenInterest", "oi"):
                        v = self._safe_float(nested.get(kk, 0.0), 0.0)
                        if v > 0.0:
                            return v
        points = self._extract_oi_points(raw, time.time())
        if points:
            return float(points[-1][1])
        return 0.0

    def _record_oi_point(self, source: str, symbol: str, ts: float, oi_value: float) -> None:
        if oi_value <= 0.0:
            return
        src = str(source or "binance").strip().lower() or "binance"
        sym = str(symbol or "").strip().upper()
        if not sym:
            return
        dq = self.oi_point_cache_by_source[src][sym]
        dq.append((float(ts), float(oi_value)))
        cutoff = float(ts) - 6.0 * 3600.0
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        while len(dq) > 720:
            dq.popleft()

    def _calc_oi_change_from_points(self, points: List[Tuple[float, float]], now_ts: float, window_sec: float = 300.0) -> float:
        if len(points) < 2:
            return 0.0
        cutoff = float(now_ts) - float(window_sec)
        base_oi = 0.0
        curr_oi = float(points[-1][1])
        for t, oi in points:
            if t >= cutoff:
                base_oi = float(oi)
                break
        if base_oi <= 0.0:
            base_oi = float(points[0][1])
        if base_oi <= 0.0:
            return 0.0
        return (curr_oi - base_oi) / base_oi * 100.0

    def _calc_oi_change_from_cache(self, source: str, symbol: str, now_ts: float, window_sec: float = 300.0) -> float:
        src = str(source or "binance").strip().lower() or "binance"
        sym = str(symbol or "").strip().upper()
        if not sym:
            return 0.0
        dq = self.oi_point_cache_by_source.get(src, {}).get(sym)
        if not dq or len(dq) < 2:
            return 0.0
        points = list(dq)
        return self._calc_oi_change_from_points(points, now_ts, window_sec=window_sec)

    def _maybe_warn_oi_unavailable(self, source: str, symbol: str, ts: float, reason: str) -> None:
        src = str(source or "binance").strip().lower() or "binance"
        sym = str(symbol or "").strip().upper()
        if not sym:
            return
        key = f"{src}:{sym}"
        now = float(ts)
        last = float(self.oi_fetch_warn_last_ts.get(key, 0.0) or 0.0)
        if (now - last) < 90.0:
            return
        self.oi_fetch_warn_last_ts[key] = now
        print(f"[oi] source={src} symbol={sym} unavailable, fallback_zero reason={reason}")

    @staticmethod
    def _extract_contract_symbols_from_exchange_info(raw: Any, usdt_only: bool = True) -> Set[str]:
        out: Set[str] = set()
        symbols = ((raw or {}).get("symbols") if isinstance(raw, dict) else None) or []
        if not isinstance(symbols, list):
            return out
        for row in symbols:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            status = str(row.get("status", "")).strip().upper()
            if status and status not in {"TRADING", "PENDING_TRADING"}:
                continue
            contract_type = str(row.get("contractType", "")).strip().upper()
            if contract_type and contract_type not in {"PERPETUAL"}:
                continue
            if usdt_only:
                quote_asset = str(row.get("quoteAsset", "")).strip().upper()
                if quote_asset and quote_asset != "USDT":
                    continue
                if (not quote_asset) and (not symbol.endswith("USDT")):
                    continue
            out.add(symbol)
        return out

    def _reload_aster_contract_symbols_if_due(self, force: bool = False) -> None:
        if not bool(self.cfg.aster_ws_enabled):
            return
        now = time.time()
        if (not force) and now < self.aster_symbols_next_reload_ts:
            return
        self.aster_symbols_next_reload_ts = now + max(30.0, float(self.cfg.aster_symbols_reload_sec))
        url = str(self.cfg.aster_exchange_info_url or "").strip()
        if not url:
            return
        try:
            raw = self._http_get_json(url, timeout=6.0)
            symbols = self._extract_contract_symbols_from_exchange_info(
                raw,
                usdt_only=bool(self.cfg.aster_only_usdt_contracts),
            )
            if symbols:
                self.aster_contract_symbols = symbols
                print(f"[aster] contracts loaded={len(symbols)}")
        except Exception as exc:
            print(f"[aster] exchangeInfo reload failed: {exc!r}")

    @staticmethod
    def _http_post_json(url: str, payload: Dict[str, Any], timeout: float = 5.0) -> Any:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "User-Agent": "monitor-ws-ignition/2.0",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            if not body:
                return {}
            try:
                return json.loads(body)
            except Exception:
                return {"raw": body}

    @staticmethod
    def _calc_cvd_zscores(cvd_series: Any, decay_tau: float) -> Tuple[float, float]:
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
        # Recent bars keep larger weights to emphasize fresh order-flow pressure.
        decay_weights = np.exp(-ages / tau)
        decayed = arr * decay_weights
        decayed_base = decayed[:-1]
        decayed_std = float(np.std(decayed_base))
        decayed_z = 0.0
        if decayed_std > 1e-9:
            decayed_z = float((decayed[-1] - float(np.mean(decayed_base))) / decayed_std)
        return simple_z, decayed_z

    @staticmethod
    def _base_asset(symbol: str) -> str:
        s = str(symbol).upper()
        for suffix in ("USDT", "USDC", "FDUSD", "BUSD"):
            if s.endswith(suffix):
                return s[: -len(suffix)]
        return s

    @staticmethod
    def _bjt_time_text(ts: float) -> str:
        bjt = timezone(timedelta(hours=8))
        try:
            return datetime.fromtimestamp(float(ts), tz=bjt).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return "-"

    @staticmethod
    def _new_horizon_stats() -> Dict[str, Any]:
        return {
            "count": 0,
            "wins": 0,
            "losses": 0,
            "flats": 0,
            "sum_ret_pct": 0.0,
            "avg_ret_pct": 0.0,
            "best_ret_pct": None,
            "worst_ret_pct": None,
        }

    @staticmethod
    def _new_symbol_profit_stats() -> Dict[str, Any]:
        return {
            "closed_signals": 0,
            "max_mfe_30m_pct": None,
            "max_ret_5m_pct": None,
            "max_ret_15m_pct": None,
            "max_ret_30m_pct": None,
            "best_signal_id_by_mfe": None,
            "updated_at": None,
        }

    def _default_live_pnl_summary_state(self) -> Dict[str, Any]:
        horizons = {label: self._new_horizon_stats() for _, label in LIVE_PNL_HORIZONS}
        return {
            "updated_at": self._bjt_time_text(time.time()),
            "rows": 0,
            "active_signals": 0,
            "horizons": horizons,
            "symbol_max_profit": {},
            "last_closed": None,
        }

    def _save_live_pnl_summary_state(self) -> None:
        try:
            self.live_pnl_summary_state["updated_at"] = self._bjt_time_text(time.time())
            self.live_pnl_summary_state["active_signals"] = len(self.live_pnl_open)
            self.live_pnl_summary_file.parent.mkdir(parents=True, exist_ok=True)
            self.live_pnl_summary_file.write_text(
                json.dumps(self.live_pnl_summary_state, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            print(f"[live-pnl] summary write failed: {exc!r}")

    def _rewrite_live_pnl_schema(self) -> None:
        old_rows: List[Dict[str, Any]] = []
        if self.live_pnl_file.exists() and self.live_pnl_file.stat().st_size > 0:
            try:
                with self.live_pnl_file.open("r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if isinstance(row, dict):
                            old_rows.append(dict(row))
            except Exception as exc:
                print(f"[live-pnl] schema rewrite read failed: {exc!r}")
        try:
            with self.live_pnl_file.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(LIVE_PNL_CSV_FIELDS))
                w.writeheader()
                for row in old_rows:
                    merged = {k: row.get(k, "") for k in LIVE_PNL_CSV_FIELDS}
                    if (not self._is_non_empty(merged.get("final_score"))) and self._is_non_empty(merged.get("score")):
                        merged["final_score"] = merged.get("score", "")
                    w.writerow(merged)
            print(f"[live-pnl] schema normalized rows={len(old_rows)}")
        except Exception as exc:
            print(f"[live-pnl] schema rewrite failed: {exc!r}")

    def _init_live_pnl_logging(self) -> None:
        try:
            self.live_pnl_dir.mkdir(parents=True, exist_ok=True)
            if (not self.live_pnl_file.exists()) or self.live_pnl_file.stat().st_size <= 0:
                with self.live_pnl_file.open("w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=list(LIVE_PNL_CSV_FIELDS))
                    w.writeheader()
            else:
                header: List[str] = []
                try:
                    with self.live_pnl_file.open("r", newline="", encoding="utf-8") as f:
                        r = csv.reader(f)
                        header = next(r, [])
                except Exception:
                    header = []
                if list(header) != list(LIVE_PNL_CSV_FIELDS):
                    self._rewrite_live_pnl_schema()
            if self.live_pnl_summary_file.exists():
                try:
                    raw = json.loads(self.live_pnl_summary_file.read_text(encoding="utf-8"))
                    if isinstance(raw, dict):
                        merged = self._default_live_pnl_summary_state()
                        merged.update(raw)
                        h = merged.get("horizons")
                        if not isinstance(h, dict):
                            h = {}
                        for _, label in LIVE_PNL_HORIZONS:
                            if not isinstance(h.get(label), dict):
                                h[label] = self._new_horizon_stats()
                            else:
                                base = self._new_horizon_stats()
                                base.update(h[label])
                                h[label] = base
                        merged["horizons"] = h
                        sm = merged.get("symbol_max_profit")
                        if not isinstance(sm, dict):
                            sm = {}
                        normalized_sm: Dict[str, Dict[str, Any]] = {}
                        for k, v in sm.items():
                            sym = str(k or "").strip().upper()
                            if not sym:
                                continue
                            base = self._new_symbol_profit_stats()
                            if isinstance(v, dict):
                                base.update(v)
                            normalized_sm[sym] = base
                        merged["symbol_max_profit"] = normalized_sm
                        self.live_pnl_summary_state = merged
                except Exception:
                    self.live_pnl_summary_state = self._default_live_pnl_summary_state()
            self._save_live_pnl_summary_state()
        except Exception as exc:
            print(f"[live-pnl] init failed: {exc!r}")

    def _init_auto_tune_logging(self) -> None:
        try:
            self.auto_tune_log_file.parent.mkdir(parents=True, exist_ok=True)
            if not self.auto_tune_log_file.exists():
                self.auto_tune_log_file.touch()
        except Exception as exc:
            print(f"[auto-tune] init failed: {exc!r}")

    def _init_discord_archive_logging(self) -> None:
        if not self.discord_archive_enabled_rt:
            return
        try:
            self.discord_archive_file.parent.mkdir(parents=True, exist_ok=True)
            if not self.discord_archive_file.exists():
                self.discord_archive_file.touch()
        except Exception as exc:
            print(f"[discord-archive] init failed: {exc!r}")

    def _init_signal_db(self) -> None:
        if not self.signal_db_enabled_rt:
            return

        self.signal_db_conn = self._open_signal_db_connection(self.signal_db_file, tag="binance")
        self.signal_db_aster_conn = self._open_signal_db_connection(self.aster_signal_db_file, tag="aster")

    def _open_signal_db_connection(self, db_path: Path, *, tag: str) -> Optional[sqlite3.Connection]:
        try:
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(
                str(db_path),
                timeout=8.0,
                isolation_level=None,
                check_same_thread=False,
            )
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_ts REAL NOT NULL,
                    time_bjt TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    side TEXT NOT NULL,
                    level TEXT NOT NULL,
                    alert INTEGER NOT NULL DEFAULT 0,
                    base_score REAL NOT NULL DEFAULT 0.0,
                    threshold REAL NOT NULL DEFAULT 0.0,
                    tier TEXT NOT NULL DEFAULT '',
                    delivery TEXT NOT NULL DEFAULT '',
                    final_score REAL NOT NULL DEFAULT 0.0,
                    hit_count INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS discord_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_ts REAL NOT NULL,
                    time_bjt TEXT NOT NULL,
                    status TEXT NOT NULL,
                    http_status INTEGER,
                    error TEXT NOT NULL DEFAULT '',
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS live_pnl_open (
                    signal_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    time_bjt TEXT NOT NULL,
                    entry_ts REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    side TEXT NOT NULL,
                    level TEXT NOT NULL,
                    tier TEXT NOT NULL DEFAULT '',
                    score REAL NOT NULL DEFAULT 0.0,
                    final_score REAL NOT NULL DEFAULT 0.0,
                    threshold REAL NOT NULL DEFAULT 0.0,
                    reason TEXT NOT NULL DEFAULT '',
                    direction REAL NOT NULL DEFAULT 1.0,
                    mfe_30m_pct REAL NOT NULL DEFAULT 0.0,
                    mae_30m_pct REAL NOT NULL DEFAULT 0.0,
                    close_ts REAL,
                    close_time_bjt TEXT
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_events_ts ON signal_events(event_ts)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_events_symbol ON signal_events(symbol)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_discord_events_ts ON discord_events(event_ts)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_live_pnl_open_symbol ON live_pnl_open(symbol)")
            print(f"[signal-db:{tag}] ready path={db_path}")
            return conn
        except Exception as exc:
            print(f"[signal-db:{tag}] init failed: {exc!r}")
            return None

    def _iter_signal_event_conns(self) -> List[sqlite3.Connection]:
        conns: List[sqlite3.Connection] = []
        seen: Set[int] = set()
        for conn in (self.signal_db_conn, self.signal_db_aster_conn):
            if conn is None:
                continue
            cid = id(conn)
            if cid in seen:
                continue
            seen.add(cid)
            conns.append(conn)
        return conns

    def _select_signal_event_conn(self, source: str = "binance") -> Optional[sqlite3.Connection]:
        src = str(source or "binance").strip().lower() or "binance"
        if src == "aster":
            if self.signal_db_aster_conn is not None:
                return self.signal_db_aster_conn
            return self.signal_db_conn
        return self.signal_db_conn if self.signal_db_conn is not None else self.signal_db_aster_conn

    def _persist_signal_event(
        self,
        *,
        ts: float,
        symbol: str,
        reason: str,
        side: str,
        level: str,
        alert: bool,
        base_score: float,
        threshold: float,
        tier: str,
        delivery: str,
        final_score: float,
        hit_count: int,
        payload: Optional[Dict[str, Any]] = None,
        source: str = "binance",
    ) -> None:
        payload_source = str(((payload or {}).get("trigger_source", source)) or source).strip().lower() or "binance"
        conn = self._select_signal_event_conn(payload_source)
        if (not self.signal_db_enabled_rt) or conn is None:
            return
        payload_json = ""
        if isinstance(payload, dict) and payload:
            try:
                payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                payload_json = "{}"
        try:
            with self.signal_db_lock:
                conn.execute(
                    """
                    INSERT INTO signal_events (
                        event_ts, time_bjt, symbol, reason, side, level, alert, base_score, threshold,
                        tier, delivery, final_score, hit_count, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        float(ts),
                        self._bjt_time_text(float(ts)),
                        str(symbol or ""),
                        str(reason or ""),
                        str(side or ""),
                        str(level or ""),
                        1 if bool(alert) else 0,
                        float(base_score or 0.0),
                        float(threshold or 0.0),
                        str(tier or ""),
                        str(delivery or ""),
                        float(final_score or 0.0),
                        int(hit_count or 0),
                        payload_json,
                    ),
                )
        except Exception as exc:
            print(f"[signal-db] insert signal failed: {exc!r}")

    def _persist_discord_event(
        self,
        *,
        ts: float,
        status: str,
        content: str,
        http_status: Optional[int] = None,
        error: str = "",
    ) -> None:
        conn = self.signal_db_conn
        if (not self.signal_db_enabled_rt) or conn is None:
            return
        try:
            with self.signal_db_lock:
                conn.execute(
                    """
                    INSERT INTO discord_events (
                        event_ts, time_bjt, status, http_status, error, content
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        float(ts),
                        self._bjt_time_text(float(ts)),
                        str(status or ""),
                        int(http_status) if http_status is not None else None,
                        str(error or ""),
                        str(content or ""),
                    ),
                )
        except Exception as exc:
            print(f"[signal-db] insert discord failed: {exc!r}")

    def _persist_live_pnl_open_entry(self, entry: Dict[str, Any]) -> None:
        conn = self.signal_db_conn
        if (not self.signal_db_enabled_rt) or conn is None:
            return
        if not isinstance(entry, dict):
            return
        try:
            with self.signal_db_lock:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO live_pnl_open (
                        signal_id, symbol, time_bjt, entry_ts, entry_price, side, level, tier,
                        score, final_score, threshold, reason, direction, mfe_30m_pct, mae_30m_pct,
                        close_ts, close_time_bjt
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(entry.get("signal_id", "") or ""),
                        str(entry.get("symbol", "") or ""),
                        str(entry.get("time_bjt", "") or ""),
                        float(entry.get("entry_ts", 0.0) or 0.0),
                        float(entry.get("entry_price", 0.0) or 0.0),
                        str(entry.get("side", "") or ""),
                        str(entry.get("level", "") or ""),
                        str(entry.get("tier", "") or ""),
                        float(entry.get("score", 0.0) or 0.0),
                        float(entry.get("final_score", 0.0) or 0.0),
                        float(entry.get("threshold", 0.0) or 0.0),
                        str(entry.get("reason", "") or ""),
                        float(entry.get("direction", 1.0) or 1.0),
                        float(entry.get("mfe_30m_pct", 0.0) or 0.0),
                        float(entry.get("mae_30m_pct", 0.0) or 0.0),
                        float(entry.get("close_ts", 0.0) or 0.0) if entry.get("close_ts") is not None else None,
                        str(entry.get("close_time_bjt", "") or "") if entry.get("close_time_bjt") is not None else None,
                    ),
                )
        except Exception as exc:
            print(f"[signal-db] upsert live_pnl_open failed: {exc!r}")

    def _delete_live_pnl_open_entry(self, signal_id: str) -> None:
        conn = self.signal_db_conn
        if (not self.signal_db_enabled_rt) or conn is None:
            return
        sid = str(signal_id or "").strip()
        if not sid:
            return
        try:
            with self.signal_db_lock:
                conn.execute("DELETE FROM live_pnl_open WHERE signal_id = ?", (sid,))
        except Exception as exc:
            print(f"[signal-db] delete live_pnl_open failed: {exc!r}")

    def _restore_runtime_state_from_signal_db(self) -> None:
        conns = self._iter_signal_event_conns()
        if (not self.signal_db_enabled_rt) or (not conns):
            return
        now = time.time()
        lookback_sec = max(
            float(ALERT_FREQ_WINDOW_SEC),
            float(ALERT_WAVE_RESET_SEC) + 60.0,
            float(self.cfg.alert_cooldown_sec) * 2.0,
            float(self.cfg.alert_cooldown_low_sec) * 2.0,
            float(self.cfg.module_signal_cooldown_sec) * 2.0,
            float(self.cfg.slow_accum_window_sec) + float(self.cfg.slow_accum_alert_cooldown_sec) + 120.0,
            float(self.cfg.fake_breakout_probe_window_sec)
            + max(
                float(self.cfg.fake_breakout_probe_alert_cooldown_sec),
                float(self.cfg.module_signal_cooldown_sec),
            )
            + 120.0,
            float(self.cfg.whale_testing_window_sec) + float(self.cfg.whale_testing_ttl_sec) + 120.0,
            float(self.cfg.probe_follow_breakout_window_sec) + 120.0,
        )
        min_ts = now - lookback_sec
        rows: List[Tuple[Any, ...]] = []
        try:
            with self.signal_db_lock:
                for conn in conns:
                    part_rows = conn.execute(
                        """
                        SELECT
                            event_ts, symbol, level, alert, base_score, final_score,
                            reason, tier, delivery, hit_count, payload_json
                        FROM signal_events
                        WHERE event_ts >= ?
                        ORDER BY event_ts ASC
                        """,
                        (float(min_ts),),
                    ).fetchall()
                    if part_rows:
                        rows.extend(part_rows)
        except Exception as exc:
            print(f"[signal-db] restore runtime failed: {exc!r}")
            return

        rows.sort(key=lambda x: float(x[0] or 0.0))

        def _to_f(v: Any, default: float = 0.0) -> float:
            try:
                return float(v or 0.0)
            except Exception:
                return float(default)

        wave_rows: Dict[str, List[float]] = defaultdict(list)
        restored_alert_state = 0
        restored_slow_cd: Dict[str, float] = {}
        restored_probe_alert_cd: Dict[str, float] = {}
        restored_probe_record_cd: Dict[str, float] = {}
        restored_liq_cd: Dict[str, float] = {}
        restored_probe_alpha: Dict[str, Dict[str, float]] = {}
        restored_whale_active: Dict[str, Dict[str, float]] = {}
        probe_ttl = max(300.0, float(self.cfg.probe_follow_breakout_window_sec))
        whale_ttl = max(300.0, float(self.cfg.whale_testing_ttl_sec))
        for row in rows:
            try:
                ts = float(row[0] or 0.0)
            except Exception:
                continue
            symbol = str(row[1] or "").strip().upper()
            level = str(row[2] or "").strip()
            alert_v = bool(int(row[3] or 0))
            reason = str(row[6] or "").strip()
            tier = str(row[7] or "").strip().upper()
            hit_count = int(_to_f(row[9], 0.0))
            payload_json = str(row[10] or "").strip()
            payload: Dict[str, Any] = {}
            if payload_json:
                try:
                    raw_payload = json.loads(payload_json)
                    if isinstance(raw_payload, dict):
                        payload = raw_payload
                except Exception:
                    payload = {}
            if (not symbol) or (not alert_v):
                if not symbol:
                    continue
            else:
                rank = ALERT_LEVEL_RANK.get(level, 0)
                if rank > 0:
                    try:
                        base_score = float(row[4] or 0.0)
                    except Exception:
                        base_score = 0.0
                    try:
                        final_score = float(row[5] or 0.0)
                    except Exception:
                        final_score = 0.0
                    score = final_score if abs(final_score) > 1e-9 else base_score
                    self.alert_state[symbol] = (ts, int(rank), float(score))
                    restored_alert_state += 1

                    if ts >= (now - float(ALERT_FREQ_WINDOW_SEC)):
                        self.alert_1h_timestamps[symbol].append(ts)
                    if ts >= (now - float(ALERT_WAVE_RESET_SEC)):
                        wave_rows[symbol].append(ts)

            if reason == "slow_accumulation":
                prev_ts = float(restored_slow_cd.get(symbol, 0.0) or 0.0)
                if ts >= prev_ts:
                    restored_slow_cd[symbol] = float(ts)
            elif reason == "fake_breakout_probe":
                push_candidate = bool(
                    _to_f(payload.get("push_candidate", 1.0 if tier == "TIER3" else 0.0), 0.0) >= 0.5
                )
                if push_candidate:
                    prev_ts = float(restored_probe_alert_cd.get(symbol, 0.0) or 0.0)
                    if ts >= prev_ts:
                        restored_probe_alert_cd[symbol] = float(ts)
                else:
                    prev_ts = float(restored_probe_record_cd.get(symbol, 0.0) or 0.0)
                    if ts >= prev_ts:
                        restored_probe_record_cd[symbol] = float(ts)
                probe_alpha = bool(_to_f(payload.get("probe_alpha", 0.0), 0.0) >= 0.5)
                expires_ts = float(ts + probe_ttl)
                if probe_alpha and expires_ts > now:
                    prev_state = restored_probe_alpha.get(symbol, {})
                    prev_updated_ts = _to_f(prev_state.get("updated_ts", 0.0), 0.0)
                    if ts >= prev_updated_ts:
                        restored_probe_alpha[symbol] = {
                            "activated_ts": float(ts),
                            "expires_ts": float(expires_ts),
                            "hits": float(_to_f(payload.get("hits", hit_count), float(hit_count))),
                            "ret_1m_pct": float(_to_f(payload.get("ret_1m_pct", 0.0), 0.0)),
                            "centroid_change_pct": float(_to_f(payload.get("centroid_change_pct", 0.0), 0.0)),
                            "waive_used": float(_to_f(payload.get("waive_used", 0.0), 0.0)),
                            "updated_ts": float(ts),
                        }
            elif reason == "liquidation_spike_watch":
                prev_ts = float(restored_liq_cd.get(symbol, 0.0) or 0.0)
                if ts >= prev_ts:
                    restored_liq_cd[symbol] = float(ts)
            elif reason == "fake_breakout_watch":
                whale_active = bool(_to_f(payload.get("whale_testing_active", 0.0), 0.0) >= 0.5)
                expires_ts = float(ts + whale_ttl)
                if whale_active and expires_ts > now:
                    prev_state = restored_whale_active.get(symbol, {})
                    prev_updated_ts = _to_f(prev_state.get("updated_ts", 0.0), 0.0)
                    if ts >= prev_updated_ts:
                        restored_whale_active[symbol] = {
                            "activated_ts": float(ts),
                            "expires_ts": float(expires_ts),
                            "hits": float(_to_f(payload.get("whale_testing_hits", 0.0), 0.0)),
                            "drawdown_pct": float(_to_f(payload.get("whale_testing_drawdown_pct", 0.0), 0.0)),
                            "window_high": float(_to_f(payload.get("whale_testing_window_high", 0.0), 0.0)),
                            "last_fake_flags": float(_to_f(payload.get("fake_flags", 0.0), 0.0)),
                            "updated_ts": float(ts),
                        }

        restored_wave = 0
        for symbol, ts_list in wave_rows.items():
            if not ts_list:
                continue
            first_ts = float(ts_list[0])
            last_ts = float(ts_list[-1])
            self.alert_wave_state[symbol] = {
                "first_ts": first_ts,
                "first_price": float(self.latest_price.get(symbol, 0.0) or 0.0),
                "count": int(len(ts_list)),
                "last_ts": last_ts,
            }
            restored_wave += 1

        self.slow_accum_last_alert_ts.update(restored_slow_cd)
        self.fake_breakout_probe_last_alert_ts.update(restored_probe_alert_cd)
        self.fake_breakout_probe_last_record_ts.update(restored_probe_record_cd)
        self.liquidation_spike_last_alert_ts.update(restored_liq_cd)
        self.probe_alpha_active.update(restored_probe_alpha)
        self.whale_testing_active.update(restored_whale_active)

        print(
            "[signal-db] restored runtime "
            f"rows={len(rows)} lookback_sec={int(lookback_sec)} "
            f"alert_state={len(self.alert_state)} freq_symbols={len(self.alert_1h_timestamps)} wave_symbols={restored_wave} "
            f"slow_cd={len(restored_slow_cd)} probe_alert_cd={len(restored_probe_alert_cd)} "
            f"probe_record_cd={len(restored_probe_record_cd)} liq_cd={len(restored_liq_cd)} "
            f"probe_alpha_active={len(restored_probe_alpha)} whale_active={len(restored_whale_active)}"
        )

    def _restore_live_pnl_open_from_db(self) -> None:
        conn = self.signal_db_conn
        if (not self.signal_db_enabled_rt) or conn is None:
            return
        try:
            with self.signal_db_lock:
                rows = conn.execute(
                    """
                    SELECT
                        signal_id, symbol, time_bjt, entry_ts, entry_price, side, level, tier,
                        score, final_score, threshold, reason, direction, mfe_30m_pct, mae_30m_pct
                    FROM live_pnl_open
                    WHERE close_ts IS NULL
                    ORDER BY entry_ts ASC
                    """
                ).fetchall()
        except Exception as exc:
            print(f"[signal-db] restore live_pnl_open failed: {exc!r}")
            return

        if not rows:
            return

        restored = 0
        max_seq = int(self.live_pnl_seq)
        for row in rows:
            signal_id = str(row[0] or "").strip()
            symbol = str(row[1] or "").strip().upper()
            if (not signal_id) or (not symbol):
                continue
            try:
                entry_ts = float(row[3] or 0.0)
                entry_price = float(row[4] or 0.0)
            except Exception:
                continue
            if entry_price <= 0.0:
                continue
            entry: Dict[str, Any] = {
                "signal_id": signal_id,
                "symbol": symbol,
                "time_bjt": str(row[2] or self._bjt_time_text(entry_ts or time.time())),
                "entry_ts": entry_ts,
                "entry_price": entry_price,
                "side": str(row[5] or "neutral"),
                "level": str(row[6] or "none"),
                "tier": str(row[7] or ""),
                "score": float(row[8] or 0.0),
                "final_score": float(row[9] or 0.0),
                "threshold": float(row[10] or 0.0),
                "reason": str(row[11] or ""),
                "direction": float(row[12] or 1.0),
                "mfe_30m_pct": float(row[13] or 0.0),
                "mae_30m_pct": float(row[14] or 0.0),
                "close_ts": None,
                "close_time_bjt": None,
            }
            for _, label in LIVE_PNL_HORIZONS:
                entry[f"ret_{label}_pct"] = None
                entry[f"price_{label}"] = None
            self.live_pnl_open[signal_id] = entry
            self.live_pnl_open_by_symbol[symbol].add(signal_id)
            restored += 1
            try:
                seq_v = int(str(signal_id).split("_")[-1])
                if seq_v > max_seq:
                    max_seq = seq_v
            except Exception:
                pass

        self.live_pnl_seq = max(self.live_pnl_seq, max_seq)
        if restored > 0:
            self._save_live_pnl_summary_state()
            print(f"[signal-db] restored live_pnl_open={restored}")

    def _archive_discord_payload(
        self,
        *,
        content: str,
        status: str,
        http_status: Optional[int] = None,
        error: str = "",
    ) -> None:
        now = time.time()
        self._persist_discord_event(
            ts=now,
            status=str(status),
            content=str(content or ""),
            http_status=http_status,
            error=str(error or ""),
        )
        if not self.discord_archive_enabled_rt:
            return
        try:
            row: Dict[str, Any] = {
                "ts": now,
                "time_bjt": self._bjt_time_text(now),
                "status": str(status),
                "http_status": int(http_status) if http_status is not None else None,
                "error": str(error or ""),
                "content": str(content or ""),
            }
            with self.discord_archive_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception as exc:
            print(f"[discord-archive] write failed: {exc!r}")

    def _init_tier_decision_logging(self) -> None:
        try:
            self.tier_decision_file.parent.mkdir(parents=True, exist_ok=True)
            if not self.tier_decision_file.exists():
                self.tier_decision_file.touch()
        except Exception as exc:
            print(f"[tier] init failed: {exc!r}")

    def _append_tier_decision_row(self, row: Dict[str, Any]) -> None:
        try:
            self.tier_decision_file.parent.mkdir(parents=True, exist_ok=True)
            with self.tier_decision_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception as exc:
            print(f"[tier] write failed: {exc!r}")

    @staticmethod
    def _clamp(v: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, float(v)))

    @staticmethod
    def _sign(v: float) -> int:
        if v > 1e-12:
            return 1
        if v < -1e-12:
            return -1
        return 0

    @staticmethod
    def _safe_float(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return float(default)

    @staticmethod
    def _parse_bjt_text_to_ts(s: str) -> Optional[float]:
        text = str(s or "").strip()
        if not text:
            return None
        bjt = timezone(timedelta(hours=8))
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=bjt).timestamp()
            except Exception:
                continue
        return None

    def _append_auto_tune_log(self, action: str, payload: Dict[str, Any]) -> None:
        try:
            row = {
                "ts": time.time(),
                "time_bjt": self._bjt_time_text(time.time()),
                "action": str(action),
                "payload": payload,
            }
            self.auto_tune_log_file.parent.mkdir(parents=True, exist_ok=True)
            with self.auto_tune_log_file.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception as exc:
            print(f"[auto-tune] log write failed: {exc!r}")

    def _read_recent_live_pnl_rows(self, limit: int) -> List[Dict[str, Any]]:
        n = max(1, int(limit))
        if not self.live_pnl_file.exists():
            return []
        out: Deque[Dict[str, Any]] = deque(maxlen=n)
        try:
            with self.live_pnl_file.open("r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if isinstance(row, dict):
                        out.append(dict(row))
        except Exception as exc:
            print(f"[auto-tune] read live pnl failed: {exc!r}")
            return []
        return list(out)

    @staticmethod
    def _is_non_empty(v: Any) -> bool:
        if v is None:
            return False
        text = str(v).strip().lower()
        return text not in {"", "none", "null", "nan"}

    def _current_tier_thresholds(self) -> Tuple[float, float, float]:
        if bool(self.cfg.auto_tune_enabled) and bool(self.cfg.auto_tune_apply_enabled):
            return (
                float(self.tier1_score_rt),
                float(self.tier2_score_rt),
                float(self.tier3_score_rt),
            )
        return (
            float(self.cfg.tier1_score),
            float(self.cfg.tier2_score),
            float(self.cfg.tier3_score),
        )

    def _persist_control_tier_scores(self) -> None:
        if not bool(self.cfg.auto_tune_persist_control):
            return
        path = self.control_file_path
        if not path:
            return
        try:
            raw: Dict[str, Any] = {}
            if path.exists():
                old = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(old, dict):
                    raw = old
            raw["tier1_score"] = round(float(self.tier1_score_rt), 4)
            raw["tier2_score"] = round(float(self.tier2_score_rt), 4)
            raw["tier3_score"] = round(float(self.tier3_score_rt), 4)
            raw["updated_at"] = datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(raw, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:
            print(f"[auto-tune] persist control failed: {exc!r}")

    def _update_auto_tune_thresholds(self, rows: List[Dict[str, Any]]) -> None:
        if not bool(self.cfg.auto_tune_enabled):
            return
        now = time.time()
        self.last_auto_tune_ts = now
        if now - float(self.last_auto_tune_adjust_ts) < float(self.cfg.auto_tune_cooldown_sec):
            return
        sample_rows: List[Dict[str, Any]] = []
        for row in rows[-max(1, int(self.cfg.auto_tune_window_rows)) :]:
            tier = str(row.get("tier", "") or "").strip().upper()
            if tier != "TIER2":
                continue
            ret5 = row.get("ret_5m_pct")
            if not self._is_non_empty(ret5):
                continue
            sample_rows.append(row)
        n = len(sample_rows)
        if n < max(1, int(self.cfg.auto_tune_min_samples)):
            return
        wins = 0
        for row in sample_rows:
            if self._safe_float(row.get("ret_5m_pct"), 0.0) > 0.0:
                wins += 1
        win_rate = wins / max(1.0, float(n))
        new_tier2 = float(self.tier2_score_rt)
        action = "hold"
        if win_rate >= float(self.cfg.auto_tune_win_rate_high):
            new_tier2 += float(self.cfg.auto_tune_tier_step_up)
            action = "raise_tier2"
        elif win_rate <= float(self.cfg.auto_tune_win_rate_low):
            new_tier2 -= float(self.cfg.auto_tune_tier_step_down)
            action = "lower_tier2"

        lo = min(float(self.cfg.auto_tune_tier2_min), float(self.cfg.auto_tune_tier2_max))
        hi = max(float(self.cfg.auto_tune_tier2_min), float(self.cfg.auto_tune_tier2_max))
        new_tier2 = self._clamp(new_tier2, lo, hi)
        if abs(new_tier2 - float(self.tier2_score_rt)) < 1e-9:
            return

        self.tier2_score_rt = float(new_tier2)
        self.tier1_score_rt = max(self.tier2_score_rt + 1.0, float(new_tier2 + self.tier_gap_12))
        self.tier3_score_rt = max(0.0, float(new_tier2 - self.tier_gap_23))
        self.last_auto_tune_adjust_ts = now
        payload = {
            "action": action,
            "sample_count": n,
            "wins": wins,
            "win_rate": round(win_rate, 6),
            "tier1_score_rt": round(self.tier1_score_rt, 4),
            "tier2_score_rt": round(self.tier2_score_rt, 4),
            "tier3_score_rt": round(self.tier3_score_rt, 4),
        }
        print(
            f"[auto-tune] {action} n={n} wr={win_rate:.2%} "
            f"tier1={self.tier1_score_rt:.1f} tier2={self.tier2_score_rt:.1f} tier3={self.tier3_score_rt:.1f}"
        )
        self._append_auto_tune_log("threshold_adjust", payload)
        self._persist_control_tier_scores()

    def _update_symbol_quality_factors(self, rows: List[Dict[str, Any]]) -> None:
        if not bool(self.cfg.symbol_quality_enabled):
            self.symbol_quality_factor_map.clear()
            self.symbol_quality_count_map.clear()
            return
        grouped: Dict[str, List[float]] = defaultdict(list)
        for row in rows[-max(1, int(self.cfg.symbol_quality_window_rows)) :]:
            symbol = str(row.get("symbol", "") or "").strip().upper()
            if not symbol:
                continue
            ret5 = row.get("ret_5m_pct")
            if not self._is_non_empty(ret5):
                continue
            grouped[symbol].append(self._safe_float(ret5, 0.0))
        alpha = max(0.1, float(self.cfg.symbol_quality_alpha))
        beta = max(0.1, float(self.cfg.symbol_quality_beta))
        floor = float(self.cfg.symbol_quality_floor)
        cap = max(floor, float(self.cfg.symbol_quality_cap))
        factors: Dict[str, float] = {}
        counts: Dict[str, int] = {}
        min_samples = max(1, int(self.cfg.symbol_quality_min_samples))
        for symbol, rets in grouped.items():
            n = len(rets)
            counts[symbol] = n
            if n < min_samples:
                continue
            wins = sum(1 for x in rets if x > 0.0)
            post_wr = (float(wins) + alpha) / (float(n) + alpha + beta)
            centered = self._clamp((post_wr - 0.5) / 0.5, -1.0, 1.0)
            factor = self._clamp(1.0 + centered * 0.25, floor, cap)
            factors[symbol] = factor
        self.symbol_quality_factor_map = factors
        self.symbol_quality_count_map = counts

    def _update_regime_hour_factors(self, rows: List[Dict[str, Any]]) -> None:
        if not bool(self.cfg.regime_pattern_enabled):
            self.regime_hour_factor_map.clear()
            self.regime_hour_count_map.clear()
            return
        grouped: Dict[int, List[float]] = defaultdict(list)
        for row in rows[-max(1, int(self.cfg.regime_window_rows)) :]:
            t0 = self._parse_bjt_text_to_ts(str(row.get("time_bjt", "") or ""))
            if t0 is None:
                continue
            dt = datetime.fromtimestamp(t0, tz=timezone(timedelta(hours=8)))
            ret5 = row.get("ret_5m_pct")
            if not self._is_non_empty(ret5):
                continue
            grouped[dt.hour].append(self._safe_float(ret5, 0.0))
        factors: Dict[int, float] = {}
        counts: Dict[int, int] = {}
        min_samples = max(1, int(self.cfg.regime_min_samples))
        high = float(self.cfg.regime_win_rate_high)
        low = float(self.cfg.regime_win_rate_low)
        for hour, vals in grouped.items():
            n = len(vals)
            counts[hour] = n
            if n < min_samples:
                continue
            wins = sum(1 for x in vals if x > 0.0)
            wr = wins / max(1.0, float(n))
            if wr >= high:
                factors[hour] = float(self.cfg.regime_factor_boost)
            elif wr <= low:
                factors[hour] = float(self.cfg.regime_factor_cut)
            else:
                factors[hour] = 1.0
        self.regime_hour_factor_map = factors
        self.regime_hour_count_map = counts

    def _refresh_adaptive_models(self) -> None:
        max_rows = max(
            120,
            int(self.cfg.auto_tune_window_rows),
            int(self.cfg.symbol_quality_window_rows),
            int(self.cfg.regime_window_rows),
        )
        rows = self._read_recent_live_pnl_rows(max_rows)
        if not rows:
            return
        self._update_auto_tune_thresholds(rows)
        self._update_symbol_quality_factors(rows)
        self._update_regime_hour_factors(rows)

    async def adaptive_maintenance_loop(self) -> None:
        interval = max(30.0, float(self.cfg.auto_tune_interval_sec))
        while True:
            try:
                await asyncio.to_thread(self._refresh_adaptive_models)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[auto-tune] refresh failed: {exc!r}")
            await asyncio.sleep(interval)

    def _symbol_quality_factor(self, symbol: str) -> Tuple[float, Dict[str, float]]:
        if not bool(self.cfg.symbol_quality_enabled):
            return 1.0, {"symbol_quality_factor": 1.0, "symbol_quality_samples": 0.0}
        s = str(symbol or "").upper()
        factor = float(self.symbol_quality_factor_map.get(s, 1.0))
        n = int(self.symbol_quality_count_map.get(s, 0))
        return factor, {"symbol_quality_factor": factor, "symbol_quality_samples": float(n)}

    def _regime_factor(self, ts: float) -> Tuple[float, Dict[str, float]]:
        if not bool(self.cfg.regime_pattern_enabled):
            return 1.0, {"regime_factor": 1.0}
        bjt = timezone(timedelta(hours=8))
        dt = datetime.fromtimestamp(float(ts), tz=bjt)
        hour = int(dt.hour)
        learned = float(self.regime_hour_factor_map.get(hour, 1.0))
        sample_n = int(self.regime_hour_count_map.get(hour, 0))
        session = 1.0
        minute_of_day = hour * 60 + int(dt.minute)
        if 9 * 60 + 30 <= minute_of_day <= 10 * 60 + 30:
            session *= float(self.cfg.regime_morning_factor)
        if dt.weekday() == 4 and 20 <= hour <= 23:
            session *= float(self.cfg.regime_friday_night_factor)
        factor = self._clamp(learned * session, 0.75, 1.25)
        return factor, {
            "regime_factor": factor,
            "regime_hour_factor": learned,
            "regime_session_factor": session,
            "regime_hour": float(hour),
            "regime_hour_samples": float(sample_n),
        }

    def _sector_cluster_factor(self, symbol: str, ts: float, base_score: float) -> Tuple[float, Dict[str, float]]:
        if not bool(self.cfg.sector_cluster_enabled):
            return 1.0, {"sector_cluster_factor": 1.0}
        sector = self._sector_for_symbol(symbol)
        if not sector:
            return 1.0, {"sector_cluster_factor": 1.0, "sector_cluster_active": 0.0}
        now = float(ts)
        window = max(10.0, float(self.cfg.sector_cluster_window_sec))
        ev = self.sector_recent_events[sector]
        cutoff = now - window
        while ev and ev[0][0] < cutoff:
            ev.popleft()
        last_key = (sector, str(symbol).upper())
        last_ts = float(self.sector_last_symbol_ts.get(last_key, 0.0))
        if (now - last_ts) >= 15.0 and float(base_score) >= float(self.cfg.sector_cluster_min_base_score):
            ev.append((now, str(symbol).upper()))
            self.sector_last_symbol_ts[last_key] = now
        unique_count = len({sym for _, sym in ev})
        min_count = max(2, int(self.cfg.sector_cluster_min_count))
        boosted = bool(unique_count >= min_count)
        if boosted:
            until = now + max(30.0, float(self.cfg.sector_cluster_ttl_sec))
            prev_until = float(self.sector_boost_until.get(sector, 0.0))
            if until > prev_until:
                self.sector_boost_until[sector] = until
                self._append_auto_tune_log(
                    "sector_cluster_boost",
                    {
                        "sector": sector,
                        "unique_symbols": unique_count,
                        "window_sec": window,
                        "boost_until": self._bjt_time_text(until),
                    },
                )
        active = now <= float(self.sector_boost_until.get(sector, 0.0))
        factor = float(self.cfg.sector_cluster_boost_factor) if active else 1.0
        factor = self._clamp(factor, 1.0, 1.8) if active else 1.0
        return factor, {
            "sector_cluster_factor": factor,
            "sector_cluster_active": 1.0 if active else 0.0,
            "sector_cluster_count": float(unique_count),
            "sector_cluster_sector": sector,
        }

    def _sentiment_factor(self, details: Dict[str, float], side: str) -> Tuple[float, Dict[str, float]]:
        if not bool(self.cfg.sentiment_enabled):
            return 1.0, {"sentiment_factor": 1.0}
        sgn = -1.0 if str(side).lower() == "short" else 1.0
        lsr_5m = float(details.get("lsr_5m_chg_pct", 0.0) or 0.0)
        top_pos = float(details.get("top_pos_lsr_5m_chg_pct", 0.0) or 0.0)
        top_acc = float(details.get("top_acc_lsr_5m_chg_pct", 0.0) or 0.0)
        taker_lsr = float(details.get("taker_lsr_5m_chg_pct", 0.0) or 0.0)
        whale_notional = float(details.get("whale_notional_1m", 0.0) or 0.0)
        concentration = float(details.get("trade_notional_top10_ratio", 0.0) or 0.0)
        funding_div = float(details.get("funding_divergence", 0.0) or 0.0)
        funding_div_streak = float(details.get("funding_div_streak", 0.0) or 0.0)
        lsr_mix = 0.45 * lsr_5m + 0.30 * top_pos + 0.15 * top_acc + 0.10 * taker_lsr
        lsr_component = self._clamp(sgn * lsr_mix / 5.0, -1.0, 1.0)
        whale_thr = max(1.0, float(self.cfg.sentiment_whale_notional_usd))
        whale_ratio = whale_notional / whale_thr
        direction_hint = self._clamp(sgn * float(details.get("buy_imbalance", 0.0) or 0.0) * 3.0, -1.0, 1.0)
        whale_component = self._clamp((whale_ratio - 1.0) * 0.6 + direction_hint * 0.4, -1.0, 1.0)
        concentration_component = 0.0
        if concentration > 0.0:
            concentration_component = self._clamp(0.35 - concentration, -1.0, 1.0)
        funding_penalty = 1.0 if funding_div >= 0.5 else 0.0
        if funding_div_streak >= max(1.0, float(self.cfg.sentiment_funding_div_streak_warn)):
            funding_penalty = 1.0
        shift = 0.0
        shift += float(self.cfg.sentiment_lsr_weight) * lsr_component
        shift += float(self.cfg.sentiment_whale_weight) * whale_component
        shift += float(self.cfg.sentiment_concentration_weight) * concentration_component
        shift -= float(self.cfg.sentiment_funding_div_weight) * funding_penalty
        factor = self._clamp(1.0 + shift, 0.80, 1.20)
        return factor, {
            "sentiment_factor": factor,
            "sentiment_lsr_component": lsr_component,
            "sentiment_whale_component": whale_component,
            "sentiment_concentration_component": concentration_component,
            "sentiment_funding_penalty": funding_penalty,
            "sentiment_shift": shift,
        }

    def _event_time_factor(self, ts: float) -> float:
        if not self.cfg.dynamic_trigger_event_factor_enabled:
            return 1.0
        dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        minute_of_day = dt.hour * 60 + dt.minute
        funding_marks = (0, 8 * 60, 16 * 60)
        for x in funding_marks:
            if abs(minute_of_day - x) <= 8:
                return max(1.0, float(self.cfg.dynamic_trigger_event_major_factor))
        if dt.hour in {13, 14, 15, 19, 20, 21}:
            return max(1.0, float(self.cfg.dynamic_trigger_event_minor_factor))
        return 1.0

    def _update_market_state_histories(self, symbol: str, micro: Optional[Dict[str, float]]) -> None:
        if not isinstance(micro, dict):
            return
        atr_pct = float(micro.get("atr_15m_pct", 0.0) or 0.0)
        vol1h_pct = float(micro.get("vol_1h_pct", 0.0) or 0.0)
        if atr_pct > 0:
            dq = self.atr15m_pct_history[symbol]
            dq.append(atr_pct)
            while len(dq) > 360:
                dq.popleft()
        if vol1h_pct > 0:
            dq = self.vol1h_pct_history[symbol]
            dq.append(vol1h_pct)
            while len(dq) > 360:
                dq.popleft()

    @staticmethod
    def _q_or(v: Deque[float], q: float, fallback: float) -> float:
        if not v:
            return fallback
        arr = np.asarray(list(v), dtype=np.float64)
        if arr.size <= 0:
            return fallback
        try:
            return float(np.quantile(arr, q))
        except Exception:
            return fallback

    def _market_factor_from_micro(self, symbol: str, micro: Optional[Dict[str, float]], ts: float) -> Tuple[float, Dict[str, float]]:
        if not isinstance(micro, dict) or not micro:
            return 1.0, {"atr_factor": 1.0, "vol_factor": 1.0, "trend_factor": 1.0, "event_factor": 1.0}

        self._update_market_state_histories(symbol, micro)
        atr_pct = float(micro.get("atr_15m_pct", 0.0) or 0.0)
        vol1h = float(micro.get("vol_1h_pct", 0.0) or 0.0)
        slope = abs(float(micro.get("slope_15m_pct", 0.0) or 0.0))

        atr_q = max(0.05, self._q_or(self.atr15m_pct_history.get(symbol, deque()), float(self.cfg.dynamic_trigger_quantile), max(atr_pct, 0.2)))
        vol_q = max(0.02, self._q_or(self.vol1h_pct_history.get(symbol, deque()), float(self.cfg.dynamic_trigger_quantile), max(vol1h, 0.12)))

        atr_ratio = atr_pct / atr_q if atr_q > 1e-9 else 1.0
        vol_ratio = vol1h / vol_q if vol_q > 1e-9 else 1.0

        atr_factor = 1.0
        if atr_ratio < 0.8:
            atr_factor = 0.88
        elif atr_ratio > 1.25:
            atr_factor = 1.12
        elif atr_ratio > 1.05:
            atr_factor = 1.05

        vol_factor = 1.0
        if vol_ratio < 0.8:
            vol_factor = 0.90
        elif vol_ratio > 1.30:
            vol_factor = 1.10
        elif vol_ratio > 1.10:
            vol_factor = 1.04

        trend_factor = 1.0
        if slope < 0.05:
            trend_factor = 0.90
        elif slope > 0.32:
            trend_factor = 1.12
        elif slope > 0.20:
            trend_factor = 1.06

        event_factor = self._event_time_factor(ts)
        factor = self._clamp(atr_factor * vol_factor * trend_factor * event_factor, 0.72, 1.35)
        return factor, {
            "atr_ratio": atr_ratio,
            "vol_ratio_state": vol_ratio,
            "atr_factor": atr_factor,
            "vol_factor": vol_factor,
            "trend_factor": trend_factor,
            "event_factor": event_factor,
        }

    def _flow_quality_factor(self, details: Dict[str, float]) -> Tuple[float, Dict[str, float]]:
        buy_ratio = float(details.get("taker_buy_ratio", 0.0) or 0.0)
        buy_imb = float(details.get("buy_imbalance", 0.0) or 0.0)
        flow_hit = float(details.get("flow_hit", 0.0) or 0.0)
        flow_req = max(1.0, float(details.get("flow_required", 2.0) or 2.0))
        delta_persist = float(details.get("delta_persist_score", 0.0) or 0.0)
        book_ratio = float(details.get("book_ratio_top5", 1.0) or 1.0)
        trade_rate = float(details.get("trade_rate_per_sec", 0.0) or 0.0)

        score = 0.45
        score += 0.20 if buy_ratio >= 0.53 else (-0.08 if buy_ratio < 0.48 else 0.0)
        score += 0.16 if buy_imb >= 0.03 else (-0.10 if buy_imb < -0.03 else 0.0)
        score += 0.18 * self._clamp(flow_hit / flow_req, 0.0, 1.5)
        score += 0.12 * self._clamp(delta_persist, 0.0, 1.0)
        if book_ratio < 0.65:
            score -= 0.10
        elif book_ratio > 1.25:
            score += 0.06
        score += 0.05 if trade_rate >= 3.0 else (-0.05 if trade_rate <= 0.3 else 0.0)
        score = self._clamp(score, 0.0, 1.2)
        factor = self._clamp(0.72 + 0.46 * score, 0.70, 1.30)
        return factor, {"flow_quality_score": score}

    def _mtf_factor(self, details: Dict[str, float], side: str) -> Tuple[float, Dict[str, float]]:
        if not self.cfg.mtf_resonance_enabled:
            return 1.0, {"mtf_score": 0.5}
        sgn = -1.0 if str(side).lower() == "short" else 1.0
        pump20 = float(details.get("pump_20s_pct", 0.0) or 0.0) * sgn
        pump1m = float(details.get("pump_1m_pct", 0.0) or details.get("ret_1m_pct", 0.0) or 0.0) * sgn
        ret5m = float(details.get("ret_5m_pct", 0.0) or 0.0) * sgn
        slope15 = float(details.get("slope_15m_pct", 0.0) or 0.0) * sgn
        vol20s = float(details.get("vol_20s", 0.0) or 0.0)

        h15s = 1.0 if (pump20 >= float(self.cfg.early_pump_pct_20s) and vol20s >= float(self.cfg.early_volume_20s)) else 0.0
        h1m = 1.0 if pump1m >= float(self.cfg.ignition_pump_1m) else 0.0
        h5m = 1.0 if ret5m >= 0.12 else 0.0
        h15m = 1.0 if slope15 >= 0.08 else 0.0

        mtf_score = 0.20 * h15s + 0.35 * h1m + 0.25 * h5m + 0.20 * h15m
        factor = self._clamp(0.78 + 0.55 * mtf_score, 0.72, 1.35)
        return factor, {
            "mtf_score": mtf_score,
            "mtf_15s": h15s,
            "mtf_1m": h1m,
            "mtf_5m": h5m,
            "mtf_15m": h15m,
        }

    def _symbol_factor(self, symbol: str, details: Dict[str, float]) -> Tuple[float, Dict[str, float]]:
        if not self.cfg.symbol_adapt_enabled:
            return 1.0, {}
        s = str(symbol or "").upper()
        qvol24 = float(self.latest_qvol24h.get(s, 0.0) or 0.0)
        age_days = float(details.get("listing_age_days", 9999.0) or 9999.0)

        factor = 1.0
        if s in {"BTCUSDT", "ETHUSDT"}:
            factor += 0.08
        if qvol24 >= float(self.cfg.symbol_liq_high_24h):
            factor += 0.06
            liq_bucket = "high"
        elif qvol24 >= float(self.cfg.symbol_liq_mid_24h):
            liq_bucket = "mid"
        else:
            factor -= 0.10
            liq_bucket = "low"

        if age_days <= max(1, int(self.cfg.symbol_new_listing_days)):
            factor -= 0.12
        elif age_days <= 30:
            factor -= 0.04

        factor = self._clamp(factor, 0.75, 1.20)
        return factor, {"liq_bucket": liq_bucket, "listing_age_days": age_days, "qvol24h": qvol24}

    def _risk_penalty(self, details: Dict[str, float], side: str, contradiction_conf: float) -> Tuple[float, List[str], Dict[str, float]]:
        penalty = 0.0
        flags: List[str] = []
        metrics: Dict[str, float] = {}
        p_fake_breakout = 0.0
        p_liquidity = 0.0
        p_slippage = 0.0
        p_cross_anomaly = 0.0
        p_iceberg = 0.0
        p_low_confidence = 0.0
        p_funding_divergence = 0.0

        if bool(self.cfg.fake_breakout_filter_enabled) and float(details.get("fake_breakout", 0.0) or 0.0) >= 0.5:
            p_fake_breakout = float(self.cfg.risk_penalty_fake_breakout)
            penalty += p_fake_breakout
            flags.append("fake_breakout")

        if self.cfg.risk_liquidity_enabled:
            spread_bps = float(details.get("spread_bps", 0.0) or 0.0)
            depth_top5 = float(details.get("depth_total_usd_top5", 0.0) or 0.0)
            if spread_bps > float(self.cfg.risk_spread_bps_max) or depth_top5 < float(self.cfg.risk_depth_min_usd):
                p_liquidity = float(self.cfg.risk_penalty_liquidity)
                penalty += p_liquidity
                flags.append("liquidity")
            metrics["spread_bps"] = spread_bps
            metrics["depth_total_usd_top5"] = depth_top5

        if self.cfg.risk_slippage_enabled:
            slip = float(details.get("slippage_est_pct", 0.0) or 0.0)
            if slip > float(self.cfg.risk_slippage_max_pct):
                over = self._clamp(slip / max(1e-9, float(self.cfg.risk_slippage_max_pct)), 1.0, 3.0)
                p_slippage = float(self.cfg.risk_penalty_slippage) * over
                penalty += p_slippage
                flags.append("slippage")
            metrics["slippage_est_pct"] = slip

        if self.cfg.risk_cross_anomaly_enabled:
            cross_diff = abs(float(details.get("cross_ret_diff_pct", 0.0) or 0.0))
            if cross_diff >= float(self.cfg.risk_cross_anomaly_pct):
                p_cross_anomaly = float(self.cfg.risk_penalty_cross_anomaly)
                penalty += p_cross_anomaly
                flags.append("cross_anomaly")
            metrics["cross_ret_diff_pct"] = cross_diff

        iceberg_prob = float(details.get("iceberg_prob", 0.0) or 0.0)
        if iceberg_prob >= 0.7:
            p_iceberg = float(self.cfg.risk_penalty_iceberg) * self._clamp(iceberg_prob, 0.7, 1.0)
            penalty += p_iceberg
            flags.append("iceberg_prob")
        metrics["iceberg_prob"] = iceberg_prob

        if contradiction_conf < 0.45:
            p_low_confidence = 5.0
            penalty += p_low_confidence
            flags.append("low_confidence")

        funding_delta = float(details.get("funding_rate_delta", 0.0) or 0.0)
        funding_divergence = float(details.get("funding_divergence", 0.0) or 0.0) >= 0.5
        ret_1m = float(details.get("ret_1m_pct", 0.0) or 0.0)
        funding_div_hit = False
        if funding_divergence:
            funding_div_hit = True
        elif str(side).lower() == "short":
            funding_div_hit = bool(ret_1m < 0.0 and funding_delta > 0.00003)
        else:
            funding_div_hit = bool(ret_1m > 0.0 and funding_delta < -0.00003)
        if funding_div_hit:
            p_funding_divergence = 4.0
            penalty += p_funding_divergence
            flags.append("funding_divergence")
        metrics["funding_rate_delta"] = funding_delta
        metrics["funding_divergence"] = 1.0 if funding_div_hit else 0.0
        metrics["risk_penalty_fake_breakout"] = float(p_fake_breakout)
        metrics["risk_penalty_liquidity"] = float(p_liquidity)
        metrics["risk_penalty_slippage"] = float(p_slippage)
        metrics["risk_penalty_cross_anomaly"] = float(p_cross_anomaly)
        metrics["risk_penalty_iceberg"] = float(p_iceberg)
        metrics["risk_penalty_low_confidence"] = float(p_low_confidence)
        metrics["risk_penalty_funding_divergence"] = float(p_funding_divergence)

        return penalty, flags, metrics

    def _shadow_ml_win_prob(self, base_score: float, market_factor: float, symbol_factor: float, mtf_factor: float, flow_factor: float, risk_penalty: float, funding_rate: float) -> float:
        if not self.cfg.ml_shadow_enabled:
            return 0.5
        z = -1.30
        z += 0.018 * float(base_score)
        z += 0.9 * (float(market_factor) - 1.0)
        z += 0.8 * (float(symbol_factor) - 1.0)
        z += 1.0 * (float(mtf_factor) - 1.0)
        z += 1.1 * (float(flow_factor) - 1.0)
        z -= 0.08 * float(risk_penalty)
        z += 0.7 * self._clamp(-funding_rate * 1000.0, -1.0, 1.0)
        prob = 1.0 / (1.0 + float(np.exp(-z)))
        return self._clamp(prob, 0.01, 0.99)

    def _compute_tier_decision(self, symbol: str, result: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
        details = meta.get("details", {}) if isinstance(meta.get("details", {}), dict) else {}
        base_score = float(result.get("score", 0.0) or 0.0)
        side = str(result.get("side", "neutral") or "neutral")
        reason = str(meta.get("reason", "analysis") or "analysis")
        ts = float(meta.get("ts", time.time()) or time.time())
        last_price = float(result.get("last_price", self.latest_price.get(symbol, 0.0)) or 0.0)
        metrics = result.get("metrics", {}) if isinstance(result.get("metrics", {}), dict) else {}
        adv = result.get("advanced", {}) if isinstance(result.get("advanced", {}), dict) else {}
        contradiction = adv.get("contradiction_guard", {}) if isinstance(adv.get("contradiction_guard", {}), dict) else {}
        contradiction_conf = float(contradiction.get("confidence", 0.7) or 0.7)
        funding_rate = float(metrics.get("funding_rate", 0.0) or 0.0)
        funding_rate_delta = float(metrics.get("funding_rate_delta", 0.0) or 0.0)
        funding_divergence = bool(metrics.get("funding_divergence", False))
        details["funding_rate"] = funding_rate
        details["funding_rate_delta"] = funding_rate_delta
        details["funding_divergence"] = 1.0 if funding_divergence else 0.0
        if funding_divergence:
            self.funding_divergence_streak[symbol] = int(self.funding_divergence_streak.get(symbol, 0)) + 1
        else:
            self.funding_divergence_streak[symbol] = 0
        details["funding_div_streak"] = float(self.funding_divergence_streak.get(symbol, 0))

        market_factor = float(details.get("market_factor", 1.0) or 1.0)
        if abs(market_factor - 1.0) < 1e-9 and isinstance(details, dict):
            market_factor, m_meta = self._market_factor_from_micro(symbol, details, ts)
            details.update({k: float(v) for k, v in m_meta.items()})
            details["market_factor"] = market_factor
        symbol_factor_struct, symbol_meta = self._symbol_factor(symbol, details)
        symbol_quality_factor, symbol_quality_meta = self._symbol_quality_factor(symbol)
        symbol_factor = self._clamp(symbol_factor_struct * symbol_quality_factor, 0.60, 1.40)
        mtf_factor, mtf_meta = self._mtf_factor(details, side)
        flow_factor, flow_meta = self._flow_quality_factor(details)
        regime_factor, regime_meta = self._regime_factor(ts)
        sector_factor, sector_meta = self._sector_cluster_factor(symbol, ts, base_score)
        sentiment_factor, sentiment_meta = self._sentiment_factor(details, side)
        whale_state = self._get_whale_testing_state(symbol=symbol, ts=ts, last_price=last_price)
        whale_active = bool(float(whale_state.get("active", 0.0) or 0.0) >= 0.5)
        whale_reason_ok = bool(reason in {"hard_pump", "slow_breakout"})
        probe_alpha_state = self._get_probe_alpha_state(symbol=symbol, ts=ts)
        probe_alpha_active = bool(float(probe_alpha_state.get("active", 0.0) or 0.0) >= 0.5)
        probe_alpha_reason_ok = bool(reason in {"hard_pump", "slow_breakout"})
        risk_penalty, risk_flags, risk_metrics = self._risk_penalty(details, side, contradiction_conf)
        risk_flags = list(risk_flags)

        effective_base_score = float(base_score)
        whale_bonus_applied = 0.0
        whale_waive_penalty_applied = 0.0
        whale_waived_flags: List[str] = []
        probe_follow_bonus_applied = 0.0
        probe_follow_waive_penalty_applied = 0.0
        probe_follow_waived_flags: List[str] = []
        if whale_active and whale_reason_ok:
            if bool(self.cfg.whale_bonus_enabled):
                whale_bonus_applied = max(0.0, float(self.cfg.whale_bonus_score))
                effective_base_score += whale_bonus_applied
            if bool(self.cfg.whale_waive_fake_breakout_penalty):
                waive_v = float(risk_metrics.get("risk_penalty_fake_breakout", 0.0) or 0.0)
                if waive_v <= 0.0 and "fake_breakout" in set(risk_flags):
                    waive_v = float(self.cfg.risk_penalty_fake_breakout)
                if waive_v > 0.0:
                    risk_penalty = max(0.0, float(risk_penalty) - waive_v)
                    whale_waive_penalty_applied += waive_v
                    whale_waived_flags.append("fake_breakout")
                    risk_metrics["risk_penalty_fake_breakout"] = 0.0
                    risk_flags = [f for f in risk_flags if f != "fake_breakout"]
            if bool(self.cfg.whale_waive_top_risk_penalty):
                candidate_map = [
                    ("fake_breakout", "risk_penalty_fake_breakout"),
                    ("liquidity", "risk_penalty_liquidity"),
                    ("slippage", "risk_penalty_slippage"),
                    ("cross_anomaly", "risk_penalty_cross_anomaly"),
                    ("iceberg_prob", "risk_penalty_iceberg"),
                    ("low_confidence", "risk_penalty_low_confidence"),
                    ("funding_divergence", "risk_penalty_funding_divergence"),
                ]
                best_flag = ""
                best_key = ""
                best_val = 0.0
                for f_name, m_key in candidate_map:
                    if f_name in set(whale_waived_flags):
                        continue
                    val = float(risk_metrics.get(m_key, 0.0) or 0.0)
                    if val > best_val:
                        best_val = val
                        best_flag = f_name
                        best_key = m_key
                if best_val > 0.0 and best_key:
                    risk_penalty = max(0.0, float(risk_penalty) - best_val)
                    whale_waive_penalty_applied += best_val
                    whale_waived_flags.append(best_flag)
                    risk_metrics[best_key] = 0.0
                    risk_flags = [f for f in risk_flags if f != best_flag]
        if probe_alpha_active and probe_alpha_reason_ok:
            probe_follow_bonus_applied = max(0.0, float(self.cfg.probe_follow_whale_bonus))
            effective_base_score += probe_follow_bonus_applied
            if bool(self.cfg.probe_follow_waive_fake_breakout_once):
                already_used = bool(float(probe_alpha_state.get("waive_used", 0.0) or 0.0) >= 0.5)
                if not already_used:
                    waive_v = float(risk_metrics.get("risk_penalty_fake_breakout", 0.0) or 0.0)
                    if waive_v <= 0.0 and "fake_breakout" in set(risk_flags):
                        waive_v = float(self.cfg.risk_penalty_fake_breakout)
                    if waive_v > 0.0:
                        risk_penalty = max(0.0, float(risk_penalty) - waive_v)
                        probe_follow_waive_penalty_applied += waive_v
                        probe_follow_waived_flags.append("fake_breakout")
                        risk_metrics["risk_penalty_fake_breakout"] = 0.0
                        risk_flags = [f for f in risk_flags if f != "fake_breakout"]
                        self._mark_probe_alpha_waive_used(symbol=symbol, ts=ts)
        risk_metrics["whale_bonus_applied"] = float(whale_bonus_applied)
        risk_metrics["whale_waive_penalty_applied"] = float(whale_waive_penalty_applied)
        risk_metrics["whale_waived_flags_count"] = float(len(whale_waived_flags))
        risk_metrics["probe_follow_bonus_applied"] = float(probe_follow_bonus_applied)
        risk_metrics["probe_follow_waive_penalty_applied"] = float(probe_follow_waive_penalty_applied)
        risk_metrics["probe_follow_waived_flags_count"] = float(len(probe_follow_waived_flags))
        bayesian_prior = 0.0
        bayesian_posterior = 0.0
        bayesian_override_active = False
        bayesian_multiplier_applied = 1.0
        bayesian_waived_penalty = 0.0
        bayesian_waived_flags: List[str] = []
        bayesian_reason_override = ""
        bayesian_level_override = ""
        bayesian_meta: Dict[str, float] = {}
        if self.bayesian_scorer is not None:
            fake_hits_4h = float(details.get("fake_breakout_hits_4h", whale_state.get("hits", 0.0)) or 0.0)
            bayes_payload = {
                "pump_20s_pct": float(details.get("pump_20s_pct", details.get("ret_20s_pct", 0.0)) or 0.0),
                "funding_rate": float(funding_rate),
                "funding_rate_delta": float(funding_rate_delta),
                "oi_change_5m_pct": float(details.get("oi_change_5m_pct", details.get("oi5m_pct", 0.0)) or 0.0),
            }
            bayesian_meta = self.bayesian_scorer.calculate_posterior(
                event_payload=bayes_payload,
                fake_breakout_hits_4h=fake_hits_4h,
            )
            bayesian_prior = float(bayesian_meta.get("prior", 0.0) or 0.0)
            bayesian_posterior = float(bayesian_meta.get("posterior", 0.0) or 0.0)
            bayesian_override_active = bool(
                bayesian_posterior >= float(self.cfg.bayesian_override_posterior_threshold)
            )
            if bayesian_override_active:
                bayesian_reason_override = str(self.cfg.bayesian_reason_override or "").strip() or "short_squeeze_ignition"
                if bool(self.cfg.bayesian_block_fake_weak_oi_penalty):
                    weak_oi_hit = bool(float(details.get("fake_weak_oi", 0.0) or 0.0) >= 0.5)
                    fake_breakout_hit = bool(float(details.get("fake_breakout", 0.0) or 0.0) >= 0.5)
                    if weak_oi_hit or fake_breakout_hit or ("fake_breakout" in set(risk_flags)):
                        waive_v = float(risk_metrics.get("risk_penalty_fake_breakout", 0.0) or 0.0)
                        if waive_v <= 0.0 and "fake_breakout" in set(risk_flags):
                            waive_v = float(self.cfg.risk_penalty_fake_breakout)
                        if waive_v > 0.0:
                            risk_penalty = max(0.0, float(risk_penalty) - waive_v)
                            bayesian_waived_penalty += waive_v
                            bayesian_waived_flags.append("fake_breakout")
                            risk_metrics["risk_penalty_fake_breakout"] = 0.0
                            risk_flags = [f for f in risk_flags if f != "fake_breakout"]
                if bool(self.cfg.bayesian_block_funding_divergence_penalty):
                    funding_hit = bool(float(details.get("funding_divergence", 0.0) or 0.0) >= 0.5) or (
                        "funding_divergence" in set(risk_flags)
                    )
                    if funding_hit:
                        waive_v = float(risk_metrics.get("risk_penalty_funding_divergence", 0.0) or 0.0)
                        if waive_v <= 0.0 and "funding_divergence" in set(risk_flags):
                            waive_v = 4.0
                        if waive_v > 0.0:
                            risk_penalty = max(0.0, float(risk_penalty) - waive_v)
                            bayesian_waived_penalty += waive_v
                            bayesian_waived_flags.append("funding_divergence")
                            risk_metrics["risk_penalty_funding_divergence"] = 0.0
                            risk_flags = [f for f in risk_flags if f != "funding_divergence"]

        pre_ml_score = (
            effective_base_score
            * market_factor
            * symbol_factor
            * mtf_factor
            * flow_factor
            * regime_factor
            * sector_factor
            * sentiment_factor
            - risk_penalty
        )
        ml_prob = self._shadow_ml_win_prob(
            base_score=effective_base_score,
            market_factor=market_factor,
            symbol_factor=symbol_factor,
            mtf_factor=mtf_factor,
            flow_factor=flow_factor,
            risk_penalty=risk_penalty,
            funding_rate=funding_rate,
        )
        ml_adjust = 0.0
        if self.cfg.ml_shadow_enabled:
            if ml_prob < float(self.cfg.ml_shadow_downgrade_prob):
                ml_adjust -= float(self.cfg.ml_shadow_penalty)
            elif ml_prob > float(self.cfg.ml_shadow_upgrade_prob):
                ml_adjust += float(self.cfg.ml_shadow_bonus)

        final_score = max(0.0, pre_ml_score + ml_adjust)
        if bayesian_override_active:
            bayesian_multiplier_applied = max(1.0, float(self.cfg.bayesian_score_multiplier))
            final_score = max(0.0, float(final_score) * bayesian_multiplier_applied)
        tier = "TIER4"
        tier1_score, tier2_score, tier3_score = self._current_tier_thresholds()
        if final_score >= tier1_score:
            tier = "TIER1"
        elif final_score >= tier2_score:
            tier = "TIER2"
        elif final_score >= tier3_score:
            tier = "TIER3"

        if (not bayesian_override_active) and float(details.get("fake_breakout", 0.0) or 0.0) >= 0.5:
            if tier in {"TIER1", "TIER2"}:
                tier = "TIER3"
        if (not bayesian_override_active) and bool(details.get("watch_only", False)):
            if tier in {"TIER1", "TIER2"}:
                tier = "TIER3"

        level = str(result.get("level", "none") or "none")
        sniper_risk_action = "none"
        ignition_guard_action = "none"
        if level == "SNIPER" and (not bayesian_override_active):
            if bool(self.cfg.sniper_risk_downgrade_enabled):
                fake_breakout_hit = bool("fake_breakout" in set(risk_flags)) or (
                    float(details.get("fake_breakout", 0.0) or 0.0) >= 0.5
                )
                heavy_fake_breakout = bool(
                    self.cfg.sniper_risk_drop_on_heavy_fake_breakout
                    and fake_breakout_hit
                    and risk_penalty >= float(self.cfg.sniper_risk_drop_penalty)
                )
                if heavy_fake_breakout:
                    tier = "TIER4"
                    sniper_risk_action = "drop_tier4_heavy_fake_breakout"
                elif fake_breakout_hit or risk_penalty >= float(self.cfg.sniper_risk_downgrade_penalty):
                    tier = "TIER3"
                    sniper_risk_action = "downgrade_tier3_risk"
                else:
                    tier = "TIER1"
                    sniper_risk_action = "keep_tier1"
            else:
                tier = "TIER1"
                sniper_risk_action = "legacy_force_tier1"
        elif level == "SNIPER":
            sniper_risk_action = "skip_due_bayesian_override"

        if reason == "ignition_3of4":
            buy_imbalance = float(details.get("buy_imbalance", 0.0) or 0.0)
            fake_flags = float(
                details.get("fake_breakout_flags", details.get("fake_flags", 0.0)) or 0.0
            )
            pass_depth_gate = (
                buy_imbalance > float(self.cfg.ignition_min_buy_imbalance)
                and fake_flags <= float(self.cfg.ignition_max_fake_flags)
            )
            if pass_depth_gate:
                tier = "TIER2"
                ignition_guard_action = "allow_tier2_depth_gate_passed"
            else:
                tier = "TIER4"
                final_score = 0.0
                ignition_guard_action = "drop_depth_gate_failed"

        if bayesian_override_active and bool(self.cfg.bayesian_force_sniper_enabled):
            tier = "TIER1"
            bayesian_level_override = "SNIPER"

        risk_metrics["bayesian_waived_penalty"] = float(bayesian_waived_penalty)
        risk_metrics["bayesian_waived_flags_count"] = float(len(bayesian_waived_flags))
        delivery = "record_only"
        if tier == "TIER1":
            delivery = "realtime"
        elif tier == "TIER2":
            delivery = "confirm"
        elif tier == "TIER3":
            delivery = "summary" if self.cfg.tier3_discord_push_enabled else "record_only"

        return {
            "tier": tier,
            "delivery": delivery,
            "base_score": base_score,
            "base_score_effective": effective_base_score,
            "final_score": final_score,
            "pre_ml_score": pre_ml_score,
            "market_factor": market_factor,
            "symbol_factor": symbol_factor,
            "symbol_factor_struct": symbol_factor_struct,
            "symbol_quality_factor": symbol_quality_factor,
            "mtf_factor": mtf_factor,
            "flow_quality_factor": flow_factor,
            "regime_factor": regime_factor,
            "sector_cluster_factor": sector_factor,
            "sentiment_factor": sentiment_factor,
            "risk_penalty": risk_penalty,
            "risk_flags": risk_flags,
            "risk_metrics": risk_metrics,
            "ml_win_prob": ml_prob,
            "ml_adjust": ml_adjust,
            "symbol_meta": symbol_meta,
            "symbol_quality_meta": symbol_quality_meta,
            "mtf_meta": mtf_meta,
            "flow_meta": flow_meta,
            "regime_meta": regime_meta,
            "sector_meta": sector_meta,
            "sentiment_meta": sentiment_meta,
            "tier_thresholds": {
                "tier1_score": tier1_score,
                "tier2_score": tier2_score,
                "tier3_score": tier3_score,
            },
            "whale_testing_active": 1.0 if whale_active else 0.0,
            "whale_testing_reason_ok": 1.0 if whale_reason_ok else 0.0,
            "whale_testing_hits": float(whale_state.get("hits", 0.0) or 0.0),
            "whale_testing_drawdown_pct": float(whale_state.get("drawdown_pct", 0.0) or 0.0),
            "whale_bonus_applied": float(whale_bonus_applied),
            "whale_waive_penalty_applied": float(whale_waive_penalty_applied),
            "whale_waived_flags": list(whale_waived_flags),
            "probe_alpha_active": 1.0 if probe_alpha_active else 0.0,
            "probe_alpha_reason_ok": 1.0 if probe_alpha_reason_ok else 0.0,
            "probe_alpha_hits": float(probe_alpha_state.get("hits", 0.0) or 0.0),
            "probe_alpha_ret_1m_pct": float(probe_alpha_state.get("ret_1m_pct", 0.0) or 0.0),
            "probe_alpha_centroid_change_pct": float(probe_alpha_state.get("centroid_change_pct", 0.0) or 0.0),
            "probe_alpha_waive_used": float(probe_alpha_state.get("waive_used", 0.0) or 0.0),
            "probe_follow_bonus_applied": float(probe_follow_bonus_applied),
            "probe_follow_waive_penalty_applied": float(probe_follow_waive_penalty_applied),
            "probe_follow_waived_flags": list(probe_follow_waived_flags),
            "bayesian_prior": float(bayesian_prior),
            "bayesian_posterior": float(bayesian_posterior),
            "bayesian_override_active": 1.0 if bayesian_override_active else 0.0,
            "bayesian_multiplier_applied": float(bayesian_multiplier_applied),
            "bayesian_waived_penalty": float(bayesian_waived_penalty),
            "bayesian_waived_flags": list(bayesian_waived_flags),
            "bayesian_meta": bayesian_meta,
            "level_override": bayesian_level_override,
            "reason_override": bayesian_reason_override,
            "sniper_risk_action": sniper_risk_action,
            "ignition_guard_action": ignition_guard_action,
        }

    def _queue_tier2_confirmation(self, symbol: str, result: Dict[str, Any], meta: Dict[str, Any], decision: Dict[str, Any]) -> None:
        now = time.time()
        self.pending_tier2[symbol] = {
            "symbol": symbol,
            "queued_ts": now,
            "due_ts": now + max(10.0, float(self.cfg.tier2_confirm_k_sec)),
            "result": result,
            "meta": meta,
            "decision": decision,
            "entry_price": float(result.get("last_price", self.latest_price.get(symbol, 0.0)) or 0.0),
        }

    async def _confirm_tier2_signal(self, rec: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        symbol = str(rec.get("symbol", ""))
        meta = rec.get("meta", {}) if isinstance(rec.get("meta", {}), dict) else {}
        source = str(meta.get("source", "binance") or "binance").strip().lower() or "binance"
        entry = float(rec.get("entry_price", 0.0) or 0.0)
        now_price = float(self.latest_price.get(symbol, 0.0) or 0.0)
        side = str(((rec.get("result") or {}).get("side", "neutral")))
        pullback_limit = float(self.cfg.tier2_confirm_pullback_pct)
        price_ok = True
        if entry > 0 and now_price > 0:
            move = (now_price - entry) / entry * 100.0
            if side == "short":
                price_ok = move <= pullback_limit
            else:
                price_ok = move >= -pullback_limit

        micro = await self._get_micro_confirm_cached(symbol, time.time(), source=source)
        ret_1m = float(micro.get("ret_1m_pct", 0.0) or 0.0)
        flow_ok = float(micro.get("flow_ok", 0.0) or 0.0) >= 0.5
        cross_ok = float(micro.get("cross_ok", 0.0) or 0.0) >= 0.5
        delta_persist = float(micro.get("delta_persist_score", 0.0) or 0.0)

        checks = 0
        if side == "short":
            checks += int(ret_1m <= 0.0)
        else:
            checks += int(ret_1m >= 0.0)
        checks += int(flow_ok)
        checks += int(cross_ok)
        checks += int(delta_persist >= 0.4)
        ok = bool(price_ok and checks >= max(1, int(self.cfg.tier2_confirm_min_checks)))
        return ok, {"checks": checks, "price_ok": 1.0 if price_ok else 0.0, "ret_1m_pct": ret_1m}

    async def tier2_confirm_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            now = time.time()
            due = [k for k, v in self.pending_tier2.items() if float(v.get("due_ts", 0.0)) <= now]
            for symbol in due:
                rec = self.pending_tier2.pop(symbol, None)
                if not isinstance(rec, dict):
                    continue
                try:
                    ok, cmeta = await self._confirm_tier2_signal(rec)
                except Exception as exc:
                    print(f"[tier2] confirm error symbol={symbol}: {exc!r}")
                    continue
                decision = rec.get("decision", {}) if isinstance(rec.get("decision", {}), dict) else {}
                result = rec.get("result", {}) if isinstance(rec.get("result", {}), dict) else {}
                meta = rec.get("meta", {}) if isinstance(rec.get("meta", {}), dict) else {}
                drow = {
                    "ts": now,
                    "time_bjt": self._bjt_time_text(now),
                    "symbol": symbol,
                    "action": "tier2_confirm_pass" if ok else "tier2_confirm_fail",
                    "tier": str(decision.get("tier", "TIER2")),
                    "final_score": float(decision.get("final_score", 0.0) or 0.0),
                    "meta": cmeta,
                }
                self._append_tier_decision_row(drow)
                if not ok:
                    continue

                level = str(result.get("level", "none") or "none")
                if ALERT_LEVEL_RANK.get(level, 0) <= 0:
                    level = "WATCH"
                final_score = float(decision.get("final_score", result.get("score", 0.0)) or 0.0)
                if self._should_emit_alert(symbol, level, True, final_score):
                    last_price = float(result.get("last_price", 0.0) or 0.0)
                    try:
                        threshold_v = float(result.get("threshold", 60.0) or 60.0)
                    except Exception:
                        threshold_v = 60.0
                    reason = str((meta or {}).get("reason", "tier2_confirm") or "tier2_confirm")
                    source = str((meta or {}).get("source", "binance") or "binance").strip().lower() or "binance"
                    tracking = self._record_alert_tracking(symbol, last_price)
                    result = dict(result)
                    result["decision"] = decision
                    try:
                        self._register_live_pnl_entry(
                            symbol=symbol,
                            side=str(result.get("side", "neutral") or "neutral"),
                            level=level,
                            tier=str(decision.get("tier", "TIER2") or "TIER2"),
                            score=float(result.get("score", final_score) or final_score),
                            final_score=final_score,
                            threshold=threshold_v,
                            reason=reason,
                            entry_price=last_price,
                        )
                    except Exception as exc:
                        print(f"[live-pnl] tier2 open failed: {exc!r}")
                    msg = self._build_discord_message(
                        symbol=symbol,
                        level=level,
                        score=result.get("score", 0.0),
                        result=result,
                        tracking=tracking,
                        source=source,
                    )
                    if self._resolve_discord_webhook_url(source):
                        asyncio.create_task(self._send_discord_webhook(msg, source=source))

    @staticmethod
    def _side_direction(side: str) -> float:
        s = str(side or "").strip().lower()
        if s == "short":
            return -1.0
        return 1.0

    def _register_live_pnl_entry(
        self,
        *,
        symbol: str,
        side: str,
        level: str,
        tier: str,
        score: float,
        final_score: float,
        threshold: float,
        reason: str,
        entry_price: float,
    ) -> None:
        try:
            p = float(entry_price or 0.0)
        except Exception:
            p = 0.0
        if p <= 0:
            p = float(self.latest_price.get(symbol, 0.0) or 0.0)
        if p <= 0:
            return

        now = time.time()
        self.live_pnl_seq += 1
        signal_id = f"{symbol}_{int(now * 1000)}_{self.live_pnl_seq}"
        entry: Dict[str, Any] = {
            "signal_id": signal_id,
            "symbol": symbol,
            "time_bjt": self._bjt_time_text(now),
            "entry_ts": now,
            "entry_price": p,
            "side": side,
            "level": level,
            "tier": str(tier or ""),
            "score": float(score),
            "final_score": float(final_score),
            "threshold": float(threshold),
            "reason": reason,
            "direction": self._side_direction(side),
            "mfe_30m_pct": 0.0,
            "mae_30m_pct": 0.0,
            "close_ts": None,
            "close_time_bjt": None,
        }
        for _, label in LIVE_PNL_HORIZONS:
            entry[f"ret_{label}_pct"] = None
            entry[f"price_{label}"] = None

        self.live_pnl_open[signal_id] = entry
        self.live_pnl_open_by_symbol[symbol].add(signal_id)
        self._persist_live_pnl_open_entry(entry)
        self._save_live_pnl_summary_state()
        print(
            f"[live-pnl] open {signal_id} {symbol} side={side} level={level} "
            f"entry={p:.8f} score={float(score):.1f}"
        )

    def _update_symbol_max_profit(self, entry: Dict[str, Any]) -> None:
        symbol = str(entry.get("symbol", "")).upper()
        if not symbol:
            return
        s = self.live_pnl_summary_state
        sm = s.get("symbol_max_profit")
        if not isinstance(sm, dict):
            sm = {}
        prev = sm.get(symbol)
        if not isinstance(prev, dict):
            prev = self._new_symbol_profit_stats()
        else:
            base = self._new_symbol_profit_stats()
            base.update(prev)
            prev = base

        prev["closed_signals"] = int(prev.get("closed_signals", 0) or 0) + 1
        signal_id = str(entry.get("signal_id", "") or "")

        try:
            mfe = float(entry.get("mfe_30m_pct", 0.0) or 0.0)
            best_prev = prev.get("max_mfe_30m_pct")
            if best_prev is None or mfe > float(best_prev):
                prev["max_mfe_30m_pct"] = mfe
                prev["best_signal_id_by_mfe"] = signal_id
        except Exception:
            pass

        for _, label in LIVE_PNL_HORIZONS:
            k = f"max_ret_{label}_pct"
            v = entry.get(f"ret_{label}_pct")
            if v is None:
                continue
            try:
                rv = float(v)
            except Exception:
                continue
            cur = prev.get(k)
            if cur is None or rv > float(cur):
                prev[k] = rv

        prev["updated_at"] = str(entry.get("close_time_bjt") or self._bjt_time_text(time.time()))
        sm[symbol] = prev
        s["symbol_max_profit"] = sm

    def _finalize_live_pnl_entry(self, signal_id: str) -> None:
        entry = self.live_pnl_open.pop(signal_id, None)
        if not isinstance(entry, dict):
            return
        self._delete_live_pnl_open_entry(signal_id)
        symbol = str(entry.get("symbol", ""))
        ids = self.live_pnl_open_by_symbol.get(symbol)
        if ids is not None:
            ids.discard(signal_id)
            if not ids:
                self.live_pnl_open_by_symbol.pop(symbol, None)

        try:
            row = {k: entry.get(k, "") for k in LIVE_PNL_CSV_FIELDS}
            with self.live_pnl_file.open("a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(LIVE_PNL_CSV_FIELDS))
                w.writerow(row)
        except Exception as exc:
            print(f"[live-pnl] csv write failed: {exc!r}")

        s = self.live_pnl_summary_state
        s["rows"] = int(s.get("rows", 0) or 0) + 1
        horizons = s.get("horizons")
        if not isinstance(horizons, dict):
            horizons = {label: self._new_horizon_stats() for _, label in LIVE_PNL_HORIZONS}
        for _, label in LIVE_PNL_HORIZONS:
            key = f"ret_{label}_pct"
            ret_v = entry.get(key)
            if ret_v is None:
                continue
            try:
                rv = float(ret_v)
            except Exception:
                continue
            h = horizons.get(label)
            if not isinstance(h, dict):
                h = self._new_horizon_stats()
            h["count"] = int(h.get("count", 0) or 0) + 1
            if rv > 0:
                h["wins"] = int(h.get("wins", 0) or 0) + 1
            elif rv < 0:
                h["losses"] = int(h.get("losses", 0) or 0) + 1
            else:
                h["flats"] = int(h.get("flats", 0) or 0) + 1
            h["sum_ret_pct"] = float(h.get("sum_ret_pct", 0.0) or 0.0) + rv
            cnt = max(1, int(h.get("count", 1) or 1))
            h["avg_ret_pct"] = float(h.get("sum_ret_pct", 0.0) or 0.0) / float(cnt)
            best_prev = h.get("best_ret_pct")
            worst_prev = h.get("worst_ret_pct")
            h["best_ret_pct"] = rv if best_prev is None else max(float(best_prev), rv)
            h["worst_ret_pct"] = rv if worst_prev is None else min(float(worst_prev), rv)
            horizons[label] = h
        s["horizons"] = horizons
        s["last_closed"] = {
            "signal_id": signal_id,
            "symbol": symbol,
            "side": entry.get("side", "neutral"),
            "level": entry.get("level", "none"),
            "ret_5m_pct": entry.get("ret_5m_pct"),
            "ret_15m_pct": entry.get("ret_15m_pct"),
            "ret_30m_pct": entry.get("ret_30m_pct"),
            "closed_at": entry.get("close_time_bjt"),
        }
        self._update_symbol_max_profit(entry)
        self._save_live_pnl_summary_state()
        print(
            f"[live-pnl] close {signal_id} {symbol} "
            f"ret5={entry.get('ret_5m_pct')} ret15={entry.get('ret_15m_pct')} ret30={entry.get('ret_30m_pct')}"
        )

    def _update_live_pnl_on_tick(self, symbol: str, ts: float, price: float) -> None:
        ids = self.live_pnl_open_by_symbol.get(symbol)
        if not ids:
            return
        to_close: List[str] = []
        for signal_id in list(ids):
            entry = self.live_pnl_open.get(signal_id)
            if not isinstance(entry, dict):
                ids.discard(signal_id)
                continue
            entry_price = float(entry.get("entry_price", 0.0) or 0.0)
            if entry_price <= 0:
                to_close.append(signal_id)
                continue
            direction = float(entry.get("direction", 1.0) or 1.0)
            raw_ret = (float(price) - entry_price) / entry_price * 100.0
            side_ret = raw_ret * direction
            entry["mfe_30m_pct"] = max(float(entry.get("mfe_30m_pct", 0.0) or 0.0), side_ret)
            entry["mae_30m_pct"] = min(float(entry.get("mae_30m_pct", 0.0) or 0.0), side_ret)
            entry_ts = float(entry.get("entry_ts", ts) or ts)
            elapsed = float(ts) - entry_ts
            for sec, label in LIVE_PNL_HORIZONS:
                rk = f"ret_{label}_pct"
                pk = f"price_{label}"
                if entry.get(rk) is None and elapsed >= float(sec):
                    entry[rk] = float(side_ret)
                    entry[pk] = float(price)
            if entry.get("ret_30m_pct") is not None:
                entry["close_ts"] = float(ts)
                entry["close_time_bjt"] = self._bjt_time_text(ts)
                to_close.append(signal_id)

        for signal_id in to_close:
            self._finalize_live_pnl_entry(signal_id)

    def _reload_onchain_targets_if_due(self, *, force: bool = False) -> None:
        if not self.cfg.onchain_targets_file:
            return
        now = time.time()
        if not force and now < self.onchain_targets_next_reload_ts:
            return
        self.onchain_targets_next_reload_ts = now + max(1.0, self.cfg.onchain_targets_reload_sec)
        path = self.onchain_targets_path
        if not path.exists():
            return
        try:
            st = path.stat()
        except Exception:
            return
        if not force and st.st_mtime <= self.onchain_targets_mtime:
            return
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"[onchain-targets] load failed: {exc!r}")
            return
        if not isinstance(raw, dict):
            print("[onchain-targets] ignored non-object config")
            return

        self.onchain_file_exact = _parse_symbol_collection(raw.get("symbols", []))
        self.onchain_patterns = _parse_symbol_collection(raw.get("patterns", []))
        tau_map = raw.get("cvd_decay_tau_by_symbol", {})
        next_tau: Dict[str, float] = {}
        if isinstance(tau_map, dict):
            for k, v in tau_map.items():
                s = str(k).strip().upper()
                if not s:
                    continue
                try:
                    tau_v = float(v)
                except Exception:
                    continue
                if tau_v > 0:
                    next_tau[s] = tau_v
        self.onchain_tau_overrides = next_tau

        auto_cfg = raw.get("auto_add", {})
        if isinstance(auto_cfg, dict):
            if "enabled" in auto_cfg:
                self.onchain_auto_add_enabled_rt = bool(auto_cfg.get("enabled"))
            if "vol_ratio_1m" in auto_cfg:
                try:
                    self.onchain_auto_add_vol_ratio_rt = max(0.1, float(auto_cfg.get("vol_ratio_1m")))
                except Exception:
                    pass
            if "min_volume_1m" in auto_cfg:
                try:
                    self.onchain_auto_add_min_volume_rt = max(0.0, float(auto_cfg.get("min_volume_1m")))
                except Exception:
                    pass
            if "ttl_sec" in auto_cfg:
                try:
                    self.onchain_auto_add_ttl_rt = max(60.0, float(auto_cfg.get("ttl_sec")))
                except Exception:
                    pass
            if "discord_enabled" in auto_cfg:
                self.onchain_auto_add_discord_enabled_rt = bool(auto_cfg.get("discord_enabled"))
            if "discord_batch_window_sec" in auto_cfg:
                try:
                    self.onchain_auto_add_discord_batch_window_rt = max(
                        0.0,
                        float(auto_cfg.get("discord_batch_window_sec")),
                    )
                except Exception:
                    pass
            if "discord_max_rows" in auto_cfg:
                try:
                    self.onchain_auto_add_discord_max_rows_rt = max(
                        1,
                        int(auto_cfg.get("discord_max_rows")),
                    )
                except Exception:
                    pass

        self.onchain_targets_mtime = st.st_mtime
        print(
            f"[onchain-targets] reloaded symbols={len(self.onchain_file_exact)} "
            f"patterns={len(self.onchain_patterns)} tau_overrides={len(self.onchain_tau_overrides)}"
        )

    def _reload_onchain_sectors_if_due(self, *, force: bool = False) -> None:
        if not self.cfg.onchain_sector_file:
            return
        now = time.time()
        if not force and now < self.onchain_sector_next_reload_ts:
            return
        self.onchain_sector_next_reload_ts = now + max(1.0, self.cfg.onchain_sector_reload_sec)
        path = self.onchain_sector_path
        if not path.exists():
            return
        try:
            st = path.stat()
        except Exception:
            return
        if not force and st.st_mtime <= self.onchain_sector_mtime:
            return
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"[onchain-sectors] load failed: {exc!r}")
            return
        if not isinstance(raw, dict):
            print("[onchain-sectors] ignored non-object config")
            return

        symbol_to_sector: Dict[str, str] = {}
        sector_to_symbols: Dict[str, Set[str]] = defaultdict(set)
        patterns: List[Tuple[str, str]] = []

        raw_symbol_to_sector = raw.get("symbol_to_sector", {})
        if isinstance(raw_symbol_to_sector, dict):
            for sym, sec in raw_symbol_to_sector.items():
                s = str(sym).strip().upper()
                c = str(sec).strip().upper()
                if not s or not c:
                    continue
                symbol_to_sector[s] = c
                sector_to_symbols[c].add(s)

        raw_sector_to_symbols = raw.get("sector_to_symbols", {})
        if isinstance(raw_sector_to_symbols, dict):
            for sec, symbols in raw_sector_to_symbols.items():
                c = str(sec).strip().upper()
                if not c:
                    continue
                for sym in _parse_symbol_collection(symbols):
                    symbol_to_sector.setdefault(sym, c)
                    sector_to_symbols[c].add(sym)

        raw_patterns = raw.get("patterns", [])
        if isinstance(raw_patterns, list):
            for row in raw_patterns:
                if not isinstance(row, dict):
                    continue
                pat = str(row.get("pattern", "")).strip()
                sec = str(row.get("sector", "")).strip().upper()
                if not pat or not sec:
                    continue
                if pat.startswith("RE:"):
                    patterns.append((pat, sec))
                else:
                    patterns.append((pat.upper(), sec))

        self.onchain_sector_by_symbol = symbol_to_sector
        self.onchain_sector_symbols = dict(sector_to_symbols)
        self.onchain_sector_patterns = patterns
        self.onchain_sector_mtime = st.st_mtime
        print(
            f"[onchain-sectors] reloaded sectors={len(self.onchain_sector_symbols)} "
            f"symbols={len(self.onchain_sector_by_symbol)} patterns={len(self.onchain_sector_patterns)}"
        )

    def _prune_onchain_auto_expire(self, now: Optional[float] = None) -> None:
        ts = time.time() if now is None else now
        expired = [sym for sym, exp in self.onchain_auto_expire.items() if exp <= ts]
        for sym in expired:
            self.onchain_auto_expire.pop(sym, None)

    def _add_onchain_auto_symbol(
        self,
        symbol: str,
        ts: float,
        reason: str,
        ttl_sec: Optional[float] = None,
    ) -> None:
        s = str(symbol).upper().strip()
        if not s:
            return
        ttl = self.onchain_auto_add_ttl_rt if ttl_sec is None else max(60.0, float(ttl_sec))
        exp = ts + ttl
        prev = self.onchain_auto_expire.get(s, 0.0)
        self.onchain_auto_expire[s] = max(prev, exp)
        if prev <= 0:
            print(f"[onchain-targets] auto_add {s} reason={reason} ttl={int(ttl)}s")
            self._queue_onchain_auto_add_discord_event(s, reason, ttl, ts)

    @staticmethod
    def _fmt_bjt(ts: float) -> str:
        bjt = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(ts, tz=bjt).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _centroid_change_pct(prices: List[float]) -> float:
        vals = [float(x) for x in prices if float(x) > 0.0]
        if len(vals) < 4:
            return 0.0
        mid = max(1, len(vals) // 2)
        head = vals[:mid]
        tail = vals[mid:]
        if not tail:
            tail = vals[-1:]
        head_avg = float(statistics.mean(head)) if head else float(vals[0])
        tail_avg = float(statistics.mean(tail)) if tail else float(vals[-1])
        if head_avg <= 1e-12:
            return 0.0
        return (tail_avg - head_avg) / head_avg * 100.0

    def _build_liquidation_spike_watch_message(
        self,
        symbol: str,
        ts: float,
        pump_pct: float,
        vol_ratio_1m: float,
        oi5m_pct: float,
        retrace_need_pct: float,
        timer_sec: float,
    ) -> str:
        lines = [
            f"⚠️ **Liquidation_Spike** **{self._display_symbol(symbol)}**",
            f"time(BJT): {self._fmt_bjt(ts)}",
            f"1m涨幅/量比: {pump_pct:+.2f}% / {vol_ratio_1m:.2f}",
            f"OI(5m): {oi5m_pct:+.2f}%",
            f"观察窗口: {int(timer_sec)}s 后检查 | 回吐要求: {retrace_need_pct:.0f}%",
            "动作: 暂停追高，等待右侧二次确认",
        ]
        return "\n".join(lines)[:1900]

    def _handle_liquidation_spike(
        self,
        *,
        symbol: str,
        ts: float,
        last_price: float,
        pump_pct: float,
        pump_20s: float,
        vol_ratio_1m: float,
        fake_breakout: bool,
        fake_flags: float,
        micro: Optional[Dict[str, float]],
        source: str = "binance",
    ) -> Tuple[str, Dict[str, float]]:
        details: Dict[str, float] = {}
        if not bool(self.cfg.liquidation_spike_enabled) or float(last_price) <= 0.0:
            return "none", details

        m = micro or {}
        oi5m_pct = float(m.get("oi_change_5m_pct", 0.0) or 0.0)
        taker_buy_ratio = float(m.get("taker_buy_ratio", 0.0) or 0.0)
        buy_imbalance = float(m.get("buy_imbalance", 0.0) or 0.0)
        ret_1m_pct = float(m.get("ret_1m_pct", pump_pct) or pump_pct)
        delta_persist = float(m.get("delta_persist_score", 0.0) or 0.0)

        timer_sec = max(60.0, float(self.cfg.liquidation_spike_timer_sec))
        follow_window_sec = max(300.0, float(self.cfg.liquidation_spike_follow_window_sec))
        follow_until_default = float(ts) + timer_sec + follow_window_sec
        check_interval_sec = max(1.0, float(self.cfg.liquidation_spike_check_interval_sec))
        retrace_need_pct = max(5.0, float(self.cfg.liquidation_spike_retrace_pct))
        state = self.liquidation_spike_state.get(symbol)

        if state is not None:
            stage = str(state.get("stage", "watch") or "watch")
            peak_price = max(float(state.get("peak_price", last_price) or last_price), float(last_price))
            state["peak_price"] = peak_price
            state["last_price"] = float(last_price)
            state["updated_ts"] = float(ts)
            if peak_price > 1e-12:
                retrace_pct = (peak_price - float(last_price)) / peak_price * 100.0
            else:
                retrace_pct = 0.0
            state["retrace_pct"] = float(retrace_pct)
            state["max_retrace_pct"] = max(float(state.get("max_retrace_pct", 0.0) or 0.0), float(retrace_pct))

            arm_until_ts = float(state.get("arm_until_ts", float(ts)) or float(ts))
            follow_until_ts = float(state.get("follow_until_ts", follow_until_default) or follow_until_default)
            next_check_ts = float(state.get("next_check_ts", 0.0) or 0.0)
            if ts >= follow_until_ts:
                self.liquidation_spike_state.pop(symbol, None)
                return "none", details

            if ts < next_check_ts:
                return "block", {
                    "liquidation_spike_active": 1.0,
                    "liquidation_spike_stage_watch": 1.0,
                    "liquidation_spike_retrace_pct": float(retrace_pct),
                    "liquidation_spike_retrace_need_pct": float(retrace_need_pct),
                }
            state["next_check_ts"] = float(ts) + check_interval_sec

            if stage == "watch" and ts >= arm_until_ts and retrace_pct >= retrace_need_pct:
                stage = "retrace_ok"
                state["stage"] = stage
                state["retrace_ts"] = float(ts)
                state["stabilize_hits"] = 0.0

            if stage == "retrace_ok":
                stabilize_ret = abs(ret_1m_pct) <= float(self.cfg.liquidation_spike_stabilize_abs_ret_1m_pct)
                stabilize_vol = vol_ratio_1m <= float(self.cfg.liquidation_spike_stabilize_max_vol_ratio_1m)
                stabilize_oi = oi5m_pct >= float(self.cfg.liquidation_spike_confirm_min_oi5m_pct)
                stabilize_delta = delta_persist >= 0.35
                stable_now = bool(stabilize_ret and stabilize_vol and stabilize_oi and stabilize_delta)
                hits = float(state.get("stabilize_hits", 0.0) or 0.0)
                hits = hits + 1.0 if stable_now else max(0.0, hits - 1.0)
                state["stabilize_hits"] = hits

                right_ret_ok = (
                    pump_pct >= max(0.12, float(self.cfg.flow_pump_min_ret_1m_pct) * 0.5)
                    or pump_20s >= max(0.06, float(self.cfg.ignition_pump_20s) * 0.8)
                )
                right_flow_ok = (
                    taker_buy_ratio >= max(0.50, float(self.cfg.liquidation_spike_min_taker_buy_ratio) - 0.05)
                    or buy_imbalance >= max(0.0, float(self.cfg.liquidation_spike_min_buy_imbalance) - 0.02)
                )
                right_fake_ok = float(fake_flags) <= float(max(1, int(self.cfg.liquidation_spike_min_fake_flags) - 1))
                if hits >= 2.0 and right_ret_ok and right_flow_ok and right_fake_ok:
                    self.liquidation_spike_state.pop(symbol, None)
                    self.liquidation_spike_last_alert_ts[symbol] = float(ts)
                    return "confirm", {
                        "liquidation_spike_active": 1.0,
                        "liquidation_spike_stage_reentry": 1.0,
                        "liquidation_spike_retrace_pct": float(retrace_pct),
                        "liquidation_spike_retrace_need_pct": float(retrace_need_pct),
                        "liquidation_spike_stabilize_hits": float(hits),
                        "liquidation_spike_stable_now": 1.0 if stable_now else 0.0,
                        "liquidation_spike_ret_1m_pct": float(ret_1m_pct),
                        "liquidation_spike_oi5m_pct": float(oi5m_pct),
                        "liquidation_spike_vol_ratio_1m": float(vol_ratio_1m),
                    }

                return "block", {
                    "liquidation_spike_active": 1.0,
                    "liquidation_spike_stage_retrace_ok": 1.0,
                    "liquidation_spike_retrace_pct": float(retrace_pct),
                    "liquidation_spike_retrace_need_pct": float(retrace_need_pct),
                    "liquidation_spike_stabilize_hits": float(hits),
                    "liquidation_spike_ret_1m_pct": float(ret_1m_pct),
                    "liquidation_spike_oi5m_pct": float(oi5m_pct),
                    "liquidation_spike_vol_ratio_1m": float(vol_ratio_1m),
                }

            return "block", {
                "liquidation_spike_active": 1.0,
                "liquidation_spike_stage_watch": 1.0,
                "liquidation_spike_retrace_pct": float(retrace_pct),
                "liquidation_spike_retrace_need_pct": float(retrace_need_pct),
            }

        min_fake = max(1, int(self.cfg.liquidation_spike_min_fake_flags))
        start_ok = bool(
            float(pump_pct) >= float(self.cfg.liquidation_spike_ret_1m_pct)
            and float(vol_ratio_1m) >= float(self.cfg.liquidation_spike_min_vol_ratio_1m)
            and float(oi5m_pct) <= float(self.cfg.liquidation_spike_max_oi5m_pct)
            and (bool(fake_breakout) or float(fake_flags) >= float(min_fake))
            and (
                float(taker_buy_ratio) >= float(self.cfg.liquidation_spike_min_taker_buy_ratio)
                or float(buy_imbalance) >= float(self.cfg.liquidation_spike_min_buy_imbalance)
            )
        )
        if not start_ok:
            return "none", details

        cooldown_sec = max(300.0, float(self.cfg.liquidation_spike_alert_cooldown_sec))
        last_ts = float(self.liquidation_spike_last_alert_ts.get(symbol, 0.0) or 0.0)
        if last_ts > 0.0 and (float(ts) - last_ts) < cooldown_sec:
            return "none", details

        arm_until_ts = float(ts) + timer_sec
        follow_until_ts = arm_until_ts + follow_window_sec
        self.liquidation_spike_state[symbol] = {
            "stage": "watch",
            "detected_ts": float(ts),
            "updated_ts": float(ts),
            "entry_price": float(last_price),
            "last_price": float(last_price),
            "peak_price": float(last_price),
            "retrace_pct": 0.0,
            "max_retrace_pct": 0.0,
            "arm_until_ts": float(arm_until_ts),
            "follow_until_ts": float(follow_until_ts),
            "next_check_ts": float(ts) + check_interval_sec,
            "stabilize_hits": 0.0,
        }
        self.liquidation_spike_last_alert_ts[symbol] = float(ts)
        print(
            f"[liq] {symbol} liquidation_spike_watch ret_1m={pump_pct:.2f}% "
            f"vol_ratio={vol_ratio_1m:.2f} oi5m={oi5m_pct:.2f}%"
        )
        payload = {
            "type": "liquidation_spike_watch",
            "trigger_source": str(source or "binance").strip().lower() or "binance",
            "ret_1m_pct": float(pump_pct),
            "ret_20s_pct": float(pump_20s),
            "vol_ratio_1m": float(vol_ratio_1m),
            "oi5m_pct": float(oi5m_pct),
            "fake_flags": float(fake_flags),
            "taker_buy_ratio": float(taker_buy_ratio),
            "buy_imbalance": float(buy_imbalance),
            "timer_sec": float(timer_sec),
            "follow_window_sec": float(follow_window_sec),
            "retrace_need_pct": float(retrace_need_pct),
        }
        self._persist_signal_event(
            ts=float(ts),
            symbol=symbol,
            reason="liquidation_spike_watch",
            side="neutral",
            level="WATCH",
            alert=True,
            base_score=0.0,
            threshold=float(self.cfg.liquidation_spike_ret_1m_pct),
            tier="TIER4",
            delivery="realtime",
            final_score=0.0,
            hit_count=1,
            payload=payload,
        )
        if self._resolve_discord_webhook_url(source):
            msg = self._build_liquidation_spike_watch_message(
                symbol=symbol,
                ts=float(ts),
                pump_pct=float(pump_pct),
                vol_ratio_1m=float(vol_ratio_1m),
                oi5m_pct=float(oi5m_pct),
                retrace_need_pct=float(retrace_need_pct),
                timer_sec=float(timer_sec),
            )
            try:
                asyncio.create_task(self._send_discord_webhook(msg, source=source))
            except RuntimeError:
                pass
        return "block", {
            "liquidation_spike_active": 1.0,
            "liquidation_spike_stage_watch": 1.0,
            "liquidation_spike_retrace_pct": 0.0,
            "liquidation_spike_retrace_need_pct": float(retrace_need_pct),
            "liquidation_spike_ret_1m_pct": float(pump_pct),
            "liquidation_spike_oi5m_pct": float(oi5m_pct),
            "liquidation_spike_vol_ratio_1m": float(vol_ratio_1m),
        }

    @staticmethod
    def _ema_last(values: List[float], span: int) -> Tuple[float, float]:
        vals = [float(x) for x in values if float(x) > 0.0]
        if not vals:
            return 0.0, 0.0
        alpha = 2.0 / (float(max(1, span)) + 1.0)
        ema = vals[0]
        first = ema
        for v in vals[1:]:
            ema = alpha * v + (1.0 - alpha) * ema
        return first, ema

    @staticmethod
    def _weighted_avg(values: List[float], weights: List[float]) -> float:
        total_w = 0.0
        total_v = 0.0
        for v, w in zip(values, weights):
            vv = float(v)
            ww = max(0.0, float(w))
            if vv <= 0.0:
                continue
            total_v += vv * ww
            total_w += ww
        if total_w <= 1e-12:
            vals = [float(x) for x in values if float(x) > 0.0]
            if not vals:
                return 0.0
            return float(statistics.mean(vals))
        return total_v / total_w

    def _slow_accum_metrics(
        self,
        prices: List[float],
        volumes: List[float],
    ) -> Tuple[bool, Dict[str, float]]:
        vals = [float(x) for x in prices if float(x) > 0.0]
        if len(vals) < 16:
            return False, {"centroid_change_pct": 0.0, "vwap_change_pct": 0.0, "stair_up_ratio": 0.0}

        vols = [max(0.0, float(v)) for v in volumes[: len(vals)]]
        if len(vols) < len(vals):
            vols.extend([1.0] * (len(vals) - len(vols)))

        n = len(vals)
        q = max(1, n // 4)
        q1 = vals[:q]
        q2 = vals[q : 2 * q]
        q3 = vals[2 * q : 3 * q]
        q4 = vals[3 * q :] if (3 * q) < n else vals[-1:]
        w1 = vols[: len(q1)]
        w2 = vols[len(q1) : len(q1) + len(q2)]
        w3 = vols[len(q1) + len(q2) : len(q1) + len(q2) + len(q3)]
        w4 = vols[len(q1) + len(q2) + len(q3) : len(q1) + len(q2) + len(q3) + len(q4)]

        c1 = self._weighted_avg(q1, w1)
        c2 = self._weighted_avg(q2, w2)
        c3 = self._weighted_avg(q3, w3)
        c4 = self._weighted_avg(q4, w4)
        stairs = [c1, c2, c3, c4]
        up_cnt = 0
        for i in range(1, len(stairs)):
            if stairs[i] > stairs[i - 1]:
                up_cnt += 1
        stair_up_ratio = float(up_cnt) / 3.0
        stair_change_pct = 0.0
        if c1 > 1e-12:
            stair_change_pct = (c4 - c1) / c1 * 100.0

        edge_n = max(3, n // 10)
        vwap_head = self._weighted_avg(vals[:edge_n], vols[:edge_n])
        vwap_tail = self._weighted_avg(vals[-edge_n:], vols[-edge_n:])
        vwap_change_pct = 0.0
        if vwap_head > 1e-12:
            vwap_change_pct = (vwap_tail - vwap_head) / vwap_head * 100.0

        centroid_change_pct = self._centroid_change_pct(vals)
        ema_s_start, ema_s_end = self._ema_last(vals, span=12)
        ema_l_start, ema_l_end = self._ema_last(vals, span=36)
        ema_trend_ok = bool(ema_s_end > ema_l_end)
        min_step = max(0.0, float(self.cfg.slow_accum_min_stair_step_pct))
        stair_ok = bool(stair_up_ratio >= 2.0 / 3.0 and stair_change_pct >= min_step)
        centroid_ok = bool(centroid_change_pct >= min_step)
        vwap_ok = bool(vwap_change_pct >= min_step)
        trend_ok = bool(ema_trend_ok and (centroid_ok or vwap_ok))
        return trend_ok, {
            "centroid_change_pct": float(centroid_change_pct),
            "vwap_change_pct": float(vwap_change_pct),
            "stair_up_ratio": float(stair_up_ratio),
            "stair_change_pct": float(stair_change_pct),
            "stair_ok": 1.0 if stair_ok else 0.0,
            "ema_short_start": float(ema_s_start),
            "ema_short_end": float(ema_s_end),
            "ema_long_start": float(ema_l_start),
            "ema_long_end": float(ema_l_end),
        }

    def _build_slow_accum_discord_message(
        self,
        symbol: str,
        ts: float,
        hits: int,
        metrics: Dict[str, float],
        last_price: float,
        tier: str,
        quality_label: str,
    ) -> str:
        lines = [
            f"📈 **Slow_Accumulation** **{self._display_symbol(symbol)}**",
            f"time(BJT): {self._fmt_bjt(ts)}",
            f"1-2h微脉冲拦截: {int(hits)} 次 (fake_breakout)",
            f"分层: {tier} | 质量: {quality_label}",
            (
                f"重心/VWAP: {float(metrics.get('centroid_change_pct', 0.0)):+.2f}% / "
                f"{float(metrics.get('vwap_change_pct', 0.0)):+.2f}%"
            ),
            (
                f"阶梯抬升: {float(metrics.get('stair_up_ratio', 0.0)) * 100.0:.0f}% "
                f"| EMA12/36: {self._fmt_price(metrics.get('ema_short_end', 0.0))} / "
                f"{self._fmt_price(metrics.get('ema_long_end', 0.0))}"
            ),
            f"价格: {self._fmt_price(last_price)}",
            (
                "动作: WATCH 长线跟踪（实时推送）"
                if str(tier).upper() in {"TIER2", "TIER3"}
                else "动作: WATCH 长线跟踪（仅落盘）"
            ),
        ]
        return "\n".join(lines)[:1900]

    def _maybe_emit_slow_accumulation_signal(
        self,
        symbol: str,
        ts: float,
        last_price: float,
        pump_pct: float,
        vol_1m: float,
        fake_flags: float,
        source: str = "binance",
    ) -> bool:
        if not bool(self.cfg.slow_accum_enabled):
            return False
        if last_price <= 0.0:
            return False
        min_ret = float(self.cfg.slow_accum_min_ret_1m_pct)
        max_ret = max(min_ret, float(self.cfg.slow_accum_max_ret_1m_pct))
        if float(pump_pct) < min_ret or float(pump_pct) > max_ret:
            return False

        window_sec = max(3600.0, float(self.cfg.slow_accum_window_sec))
        dq = self.slow_accum_events[symbol]
        dq.append((float(ts), float(last_price), max(1.0, float(vol_1m)), float(pump_pct)))
        cutoff = float(ts) - window_sec
        while dq and float(dq[0][0]) < cutoff:
            dq.popleft()
        hits = len(dq)
        if hits < max(1, int(self.cfg.slow_accum_min_hits)):
            return False

        prices = [x[1] for x in dq]
        volumes = [x[2] for x in dq]
        trend_ok, metrics = self._slow_accum_metrics(prices, volumes)
        if not trend_ok:
            return False

        last_alert_ts = float(self.slow_accum_last_alert_ts.get(symbol, 0.0) or 0.0)
        cooldown_sec = max(
            300.0,
            float(self.cfg.slow_accum_alert_cooldown_sec),
            float(self.cfg.module_signal_cooldown_sec),
        )
        if last_alert_ts > 0.0 and float(ts) - last_alert_ts < cooldown_sec:
            return False
        self.slow_accum_last_alert_ts[symbol] = float(ts)

        alpha_ret = max(0.0, float(self.cfg.slow_accum_alpha_ret_1m_pct))
        alpha_hits = max(1, int(self.cfg.slow_accum_alpha_hits))
        alpha_bonus = 0.0
        alpha_label = "base"
        if float(pump_pct) >= alpha_ret:
            alpha_bonus = max(alpha_bonus, max(0.0, float(self.cfg.slow_accum_alpha_bonus)))
            alpha_label = "alpha"
            if hits >= alpha_hits:
                alpha_bonus = max(alpha_bonus, max(0.0, float(self.cfg.slow_accum_alpha_plus_bonus)))
                alpha_label = "alpha_plus"

        tier2_hits = max(1, int(self.cfg.slow_accum_tier2_min_hits))
        tier3_hits = max(1, int(self.cfg.slow_accum_tier3_min_hits))
        if tier2_hits < tier3_hits:
            tier2_hits = tier3_hits
        tier = "TIER4"
        delivery = "record_only"
        quality_label = "BASE"
        if hits >= tier2_hits:
            tier = "TIER2"
            delivery = "realtime"
            quality_label = "ALPHA_PLUS"
        elif hits >= tier3_hits:
            tier = "TIER3"
            delivery = "realtime" if bool(self.cfg.slow_accum_tier3_realtime) else "record_only"
            quality_label = "ALPHA"

        print(
            f"[accum] {symbol} slow_accumulation hits={hits} window={int(window_sec)}s "
            f"centroid={float(metrics.get('centroid_change_pct', 0.0)):+.2f}% "
            f"alpha={alpha_label} tier={tier} delivery={delivery}"
        )
        payload = {
            "type": "slow_accumulation",
            "trigger_source": str(source or "binance").strip().lower() or "binance",
            "window_sec": float(window_sec),
            "hits": int(hits),
            "last_price": float(last_price),
            "ret_1m_pct": float(pump_pct),
            "fake_flags": float(fake_flags),
            "alpha_label": alpha_label,
            "alpha_bonus": float(alpha_bonus),
            "alpha_ret_gate": float(alpha_ret),
            "alpha_hits_gate": int(alpha_hits),
            "tier2_hits_gate": int(tier2_hits),
            "tier3_hits_gate": int(tier3_hits),
            "quality_label": str(quality_label),
            **{k: float(v) for k, v in metrics.items()},
        }
        score = float(min(120.0, max(0.0, float(hits) * 0.5 + float(alpha_bonus))))
        self._persist_signal_event(
            ts=float(ts),
            symbol=symbol,
            reason="slow_accumulation",
            side="long",
            level="WATCH",
            alert=True,
            base_score=score,
            threshold=float(max(1, int(self.cfg.slow_accum_min_hits))),
            tier=tier,
            delivery=delivery,
            final_score=score,
            hit_count=int(hits),
            payload=payload,
        )
        if delivery != "realtime":
            return True
        if not self._resolve_discord_webhook_url(source):
            return True
        msg = self._build_slow_accum_discord_message(
            symbol=symbol,
            ts=float(ts),
            hits=int(hits),
            metrics=metrics,
            last_price=float(last_price),
            tier=tier,
            quality_label=quality_label,
        )
        try:
            asyncio.create_task(self._send_discord_webhook(msg, source=source))
        except RuntimeError:
            pass
        return True

    def _build_fake_breakout_probe_discord_message(
        self,
        symbol: str,
        ts: float,
        hits: int,
        centroid_change_pct: float,
        last_price: float,
        pump_pct: float,
        fake_flags: float,
        push_candidate: bool,
        probe_alpha: bool,
    ) -> str:
        lines = [
            f"🧪 **主力异动/试盘** **{self._display_symbol(symbol)}**",
            f"time(BJT): {self._fmt_bjt(ts)}",
            f"4h拦截次数: {int(hits)} 次 (fake_breakout)",
            (
                f"价格重心变化: {centroid_change_pct:+.2f}% "
                f"(阈值: 不低于 -{float(self.cfg.fake_breakout_probe_max_centroid_drop_pct):.2f}%)"
            ),
            f"价格: {self._fmt_price(last_price)} | 1m涨幅: {pump_pct:.2f}%",
            f"fake_flags(当前): {int(fake_flags)}",
        ]
        if push_candidate:
            lines.append("状态: PUSH_GATED ✅")
        if probe_alpha:
            lines.append("状态: PROBE_ALPHA 🐋")
        return "\n".join(lines)[:1900]

    def _maybe_emit_fake_breakout_probe_signal(
        self,
        symbol: str,
        ts: float,
        last_price: float,
        pump_pct: float,
        fake_flags: float,
        source: str = "binance",
    ) -> None:
        if not bool(self.cfg.fake_breakout_probe_enabled):
            return
        if last_price <= 0.0:
            return
        window_sec = max(300.0, float(self.cfg.fake_breakout_probe_window_sec))
        dq = self.fake_breakout_probe_events[symbol]
        dq.append((float(ts), float(last_price)))
        cutoff = float(ts) - window_sec
        while dq and float(dq[0][0]) < cutoff:
            dq.popleft()
        hits = len(dq)
        if hits < max(1, int(self.cfg.fake_breakout_probe_min_hits)):
            return
        centroid_change_pct = self._centroid_change_pct([x[1] for x in dq])
        max_drop = max(0.0, float(self.cfg.fake_breakout_probe_max_centroid_drop_pct))
        if centroid_change_pct < -max_drop:
            return

        push_min_hits = max(1, int(self.cfg.fake_breakout_probe_push_min_hits))
        push_max_ret = max(0.0, float(self.cfg.fake_breakout_probe_push_max_ret_1m_pct))
        push_max_centroid = float(self.cfg.fake_breakout_probe_push_max_centroid_change_pct)
        push_candidate = bool(hits >= push_min_hits and float(pump_pct) <= push_max_ret)
        probe_alpha = bool(push_candidate and float(centroid_change_pct) <= push_max_centroid)

        if push_candidate:
            last_alert_ts = float(self.fake_breakout_probe_last_alert_ts.get(symbol, 0.0) or 0.0)
            cooldown_sec = max(
                60.0,
                float(self.cfg.fake_breakout_probe_alert_cooldown_sec),
                float(self.cfg.module_signal_cooldown_sec),
            )
            if last_alert_ts > 0.0 and float(ts) - last_alert_ts < cooldown_sec:
                return
            self.fake_breakout_probe_last_alert_ts[symbol] = float(ts)
        else:
            last_record_ts = float(self.fake_breakout_probe_last_record_ts.get(symbol, 0.0) or 0.0)
            record_cooldown_sec = max(300.0, float(self.cfg.module_signal_cooldown_sec))
            if last_record_ts > 0.0 and float(ts) - last_record_ts < record_cooldown_sec:
                return
            self.fake_breakout_probe_last_record_ts[symbol] = float(ts)

        probe_bonus = 0.0
        if push_candidate:
            probe_bonus += max(0.0, float(self.cfg.fake_breakout_probe_bonus))
        if probe_alpha:
            probe_bonus += max(0.0, float(self.cfg.fake_breakout_probe_alpha_bonus))
            self._activate_probe_alpha_state(
                symbol=symbol,
                ts=float(ts),
                hits=int(hits),
                ret_1m_pct=float(pump_pct),
                centroid_change_pct=float(centroid_change_pct),
            )
            print(
                f"[probe-alpha] {symbol} active hits={hits} ret_1m={float(pump_pct):.3f}% "
                f"centroid={float(centroid_change_pct):+.2f}% window={int(self.cfg.probe_follow_breakout_window_sec)}s"
            )

        base_score = float(max(0.0, float(hits) + float(probe_bonus)))
        tier = "TIER3" if push_candidate else "TIER4"
        delivery = "record_only"
        if push_candidate and bool(self.cfg.fake_breakout_probe_tier3_realtime):
            delivery = "realtime"

        print(
            f"[probe] {symbol} fake_breakout_cluster hits={hits} window={int(window_sec)}s "
            f"centroid={centroid_change_pct:+.2f}% push={int(push_candidate)} alpha={int(probe_alpha)} "
            f"tier={tier} delivery={delivery}"
        )
        payload = {
            "type": "fake_breakout_probe",
            "trigger_source": str(source or "binance").strip().lower() or "binance",
            "window_sec": float(window_sec),
            "hits": int(hits),
            "centroid_change_pct": float(centroid_change_pct),
            "max_allowed_drop_pct": float(max_drop),
            "last_price": float(last_price),
            "ret_1m_pct": float(pump_pct),
            "fake_flags": float(fake_flags),
            "push_candidate": 1.0 if push_candidate else 0.0,
            "probe_alpha": 1.0 if probe_alpha else 0.0,
            "probe_bonus": float(probe_bonus),
            "push_min_hits": float(push_min_hits),
            "push_max_ret_1m_pct": float(push_max_ret),
            "push_max_centroid_change_pct": float(push_max_centroid),
        }
        self._persist_signal_event(
            ts=float(ts),
            symbol=symbol,
            reason="fake_breakout_probe",
            side="neutral",
            level="WATCH",
            alert=True,
            base_score=base_score,
            threshold=float(max(1, int(self.cfg.fake_breakout_probe_min_hits))),
            tier=tier,
            delivery=delivery,
            final_score=base_score,
            hit_count=int(hits),
            payload=payload,
        )
        if delivery != "realtime":
            return
        if not self._resolve_discord_webhook_url(source):
            return
        msg = self._build_fake_breakout_probe_discord_message(
            symbol=symbol,
            ts=float(ts),
            hits=int(hits),
            centroid_change_pct=float(centroid_change_pct),
            last_price=float(last_price),
            pump_pct=float(pump_pct),
            fake_flags=float(fake_flags),
            push_candidate=bool(push_candidate),
            probe_alpha=bool(probe_alpha),
        )
        try:
            asyncio.create_task(self._send_discord_webhook(msg, source=source))
        except RuntimeError:
            pass

    def _prune_whale_testing_windows(self, symbol: str, ts: float) -> Tuple[Deque[Tuple[float, float]], Deque[Tuple[float, float]]]:
        window_sec = max(300.0, float(self.cfg.whale_testing_window_sec))
        cutoff = float(ts) - window_sec
        dq = self.whale_testing_events[symbol]
        high_dq = self.whale_testing_price_high[symbol]
        while dq and float(dq[0][0]) < cutoff:
            dq.popleft()
        while high_dq and float(high_dq[0][0]) < cutoff:
            high_dq.popleft()
        if not dq:
            self.whale_testing_events.pop(symbol, None)
            self.whale_testing_price_high.pop(symbol, None)
        return dq, high_dq

    def _get_whale_testing_state(self, symbol: str, ts: float, last_price: float = 0.0) -> Dict[str, float]:
        if not bool(self.cfg.whale_testing_enabled):
            return {
                "active": 0.0,
                "hits": 0.0,
                "drawdown_pct": 0.0,
                "window_high": 0.0,
                "activated_ts": 0.0,
                "expires_ts": 0.0,
            }
        dq, high_dq = self._prune_whale_testing_windows(symbol, float(ts))
        hits = float(len(dq))
        window_high = float(high_dq[0][1]) if high_dq else 0.0
        current_price = float(last_price or 0.0)
        if current_price <= 0.0 and dq:
            current_price = float(dq[-1][1] or 0.0)
        drawdown_pct = 0.0
        if window_high > 1e-12 and current_price > 0.0:
            drawdown_pct = (current_price - window_high) / window_high * 100.0

        state = self.whale_testing_active.get(symbol)
        if isinstance(state, dict):
            expires_ts = float(state.get("expires_ts", 0.0) or 0.0)
            if float(ts) > expires_ts:
                self.whale_testing_active.pop(symbol, None)
                state = None
        active = bool(isinstance(state, dict))
        return {
            "active": 1.0 if active else 0.0,
            "hits": hits,
            "drawdown_pct": float(drawdown_pct),
            "window_high": float(window_high),
            "activated_ts": float((state or {}).get("activated_ts", 0.0) or 0.0),
            "expires_ts": float((state or {}).get("expires_ts", 0.0) or 0.0),
        }

    def _update_whale_testing_state(
        self,
        symbol: str,
        ts: float,
        last_price: float,
        fake_flags: float,
    ) -> Dict[str, float]:
        if not bool(self.cfg.whale_testing_enabled):
            return self._get_whale_testing_state(symbol, ts, last_price)
        px = float(last_price or 0.0)
        if px <= 0.0:
            return self._get_whale_testing_state(symbol, ts, last_price)

        ts_v = float(ts)
        dq = self.whale_testing_events[symbol]
        high_dq = self.whale_testing_price_high[symbol]
        dq.append((ts_v, px))
        while high_dq and float(high_dq[-1][1]) <= px:
            high_dq.pop()
        high_dq.append((ts_v, px))
        dq, high_dq = self._prune_whale_testing_windows(symbol, ts_v)
        hits = len(dq)
        window_high = float(high_dq[0][1]) if high_dq else float(px)
        drawdown_pct = 0.0
        if window_high > 1e-12:
            drawdown_pct = (px - window_high) / window_high * 100.0
        max_drawdown = max(0.0, float(self.cfg.whale_testing_max_drawdown_pct))
        not_crashed = bool(drawdown_pct >= -max_drawdown)
        min_hits = max(1, int(self.cfg.whale_testing_min_hits))
        now_active_state = self.whale_testing_active.get(symbol)
        was_active = bool(
            isinstance(now_active_state, dict)
            and ts_v <= float(now_active_state.get("expires_ts", 0.0) or 0.0)
        )

        if hits >= min_hits and not_crashed:
            ttl_sec = max(300.0, float(self.cfg.whale_testing_ttl_sec))
            self.whale_testing_active[symbol] = {
                "activated_ts": float((now_active_state or {}).get("activated_ts", ts_v) or ts_v),
                "expires_ts": ts_v + ttl_sec,
                "hits": float(hits),
                "drawdown_pct": float(drawdown_pct),
                "window_high": float(window_high),
                "last_fake_flags": float(fake_flags),
                "updated_ts": ts_v,
            }
            if not was_active:
                print(
                    f"[whale] {symbol} active hits={hits} window={int(max(300.0, float(self.cfg.whale_testing_window_sec)))}s "
                    f"drawdown={drawdown_pct:+.2f}%"
                )
        elif not not_crashed:
            if symbol in self.whale_testing_active:
                self.whale_testing_active.pop(symbol, None)
                print(
                    f"[whale] {symbol} deactivate drawdown={drawdown_pct:+.2f}% "
                    f"limit=-{max_drawdown:.2f}%"
                )
        return self._get_whale_testing_state(symbol, ts_v, px)

    def _get_probe_alpha_state(self, symbol: str, ts: float) -> Dict[str, float]:
        state = self.probe_alpha_active.get(symbol)
        if not isinstance(state, dict):
            return {
                "active": 0.0,
                "hits": 0.0,
                "ret_1m_pct": 0.0,
                "centroid_change_pct": 0.0,
                "activated_ts": 0.0,
                "expires_ts": 0.0,
                "waive_used": 0.0,
            }
        expires_ts = float(state.get("expires_ts", 0.0) or 0.0)
        if float(ts) > expires_ts:
            self.probe_alpha_active.pop(symbol, None)
            return {
                "active": 0.0,
                "hits": 0.0,
                "ret_1m_pct": 0.0,
                "centroid_change_pct": 0.0,
                "activated_ts": 0.0,
                "expires_ts": 0.0,
                "waive_used": 0.0,
            }
        return {
            "active": 1.0,
            "hits": float(state.get("hits", 0.0) or 0.0),
            "ret_1m_pct": float(state.get("ret_1m_pct", 0.0) or 0.0),
            "centroid_change_pct": float(state.get("centroid_change_pct", 0.0) or 0.0),
            "activated_ts": float(state.get("activated_ts", 0.0) or 0.0),
            "expires_ts": expires_ts,
            "waive_used": float(state.get("waive_used", 0.0) or 0.0),
        }

    def _activate_probe_alpha_state(
        self,
        symbol: str,
        ts: float,
        hits: int,
        ret_1m_pct: float,
        centroid_change_pct: float,
    ) -> None:
        now_ts = float(ts)
        ttl = max(300.0, float(self.cfg.probe_follow_breakout_window_sec))
        prev = self.probe_alpha_active.get(symbol, {})
        activated_ts = float(prev.get("activated_ts", now_ts) or now_ts)
        self.probe_alpha_active[symbol] = {
            "activated_ts": activated_ts,
            "expires_ts": now_ts + ttl,
            "hits": float(hits),
            "ret_1m_pct": float(ret_1m_pct),
            "centroid_change_pct": float(centroid_change_pct),
            "waive_used": float(prev.get("waive_used", 0.0) or 0.0),
            "updated_ts": now_ts,
        }

    def _mark_probe_alpha_waive_used(self, symbol: str, ts: float) -> None:
        state = self.probe_alpha_active.get(symbol)
        if not isinstance(state, dict):
            return
        state["waive_used"] = 1.0
        state["updated_ts"] = float(ts)
        self.probe_alpha_active[symbol] = state

    def _build_onchain_auto_add_discord_message(self, items: List[Tuple[str, str, float, float]]) -> str:
        rows = max(1, int(self.onchain_auto_add_discord_max_rows_rt))
        ts = items[-1][3] if items else time.time()
        lines = [
            f"🧭 **ONCHAIN AUTO_ADD** | count={len(items)} | time(BJT): {self._fmt_bjt(ts)}",
        ]
        for symbol, reason, ttl, _ in items[:rows]:
            lines.append(f"`{symbol}` | reason: `{reason}` | ttl: {int(ttl)}s")
        extra = len(items) - rows
        if extra > 0:
            lines.append(f"... +{extra} more")
        return "\n".join(lines)[:1900]

    def _queue_onchain_auto_add_discord_event(self, symbol: str, reason: str, ttl: float, ts: float) -> None:
        if not self.onchain_auto_add_discord_enabled_rt:
            return
        if not self.cfg.discord_webhook_url:
            return
        self.onchain_auto_add_discord_buffer.append((symbol, reason, ttl, ts))
        if len(self.onchain_auto_add_discord_buffer) > 200:
            self.onchain_auto_add_discord_buffer = self.onchain_auto_add_discord_buffer[-200:]

        flush_task = self.onchain_auto_add_discord_flush_task
        if flush_task is not None and not flush_task.done():
            return
        try:
            self.onchain_auto_add_discord_flush_task = asyncio.create_task(
                self._flush_onchain_auto_add_discord_events(),
            )
        except RuntimeError:
            # No running loop (e.g. unit tests calling sync helpers), skip notify.
            self.onchain_auto_add_discord_flush_task = None

    async def _flush_onchain_auto_add_discord_events(self) -> None:
        await asyncio.sleep(max(0.0, float(self.onchain_auto_add_discord_batch_window_rt)))
        items = list(self.onchain_auto_add_discord_buffer)
        self.onchain_auto_add_discord_buffer.clear()
        if not items:
            return
        msg = self._build_onchain_auto_add_discord_message(items)
        await self._send_discord_webhook(msg)

    def _maybe_auto_add_onchain_symbol(self, symbol: str, vol_ratio_1m: float, vol_1m: float, ts: float) -> None:
        if not self.onchain_auto_add_enabled_rt:
            return
        if vol_ratio_1m < self.onchain_auto_add_vol_ratio_rt:
            return
        if vol_1m < self.onchain_auto_add_min_volume_rt:
            return
        self._add_onchain_auto_symbol(symbol, ts, "vol_spike")

    def _match_onchain_pattern(self, symbol: str) -> bool:
        s = str(symbol).upper().strip()
        for pat in self.onchain_patterns:
            p = str(pat).strip()
            if not p:
                continue
            if p.startswith("RE:"):
                try:
                    if re.match(p[3:], s):
                        return True
                except re.error:
                    continue
            elif fnmatch.fnmatchcase(s, p):
                return True
        return False

    def _cvd_tau_for_symbol(self, symbol: str) -> float:
        s = str(symbol).upper().strip()
        tau = self.onchain_tau_overrides.get(s)
        if tau is not None and tau > 0:
            return tau
        return self.cfg.ignition_cvd_decay_tau

    def _sector_for_symbol(self, symbol: str) -> Optional[str]:
        s = str(symbol).upper().strip()
        if not s:
            return None
        exact = self.onchain_sector_by_symbol.get(s)
        if exact:
            return exact
        for pat, sec in self.onchain_sector_patterns:
            if pat.startswith("RE:"):
                try:
                    if re.match(pat[3:], s):
                        return sec
                except re.error:
                    continue
            elif fnmatch.fnmatchcase(s, pat):
                return sec
        return None

    def _expand_onchain_targets_by_sector(self, seed_symbol: str, ts: float, seed_reason: str) -> None:
        if not self.cfg.onchain_sector_auto_expand_enabled:
            return
        if str(seed_reason).startswith("sector_"):
            return
        self._reload_onchain_sectors_if_due()
        sector = self._sector_for_symbol(seed_symbol)
        if not sector:
            return
        peers = self.onchain_sector_symbols.get(sector, set())
        if not peers:
            return
        min_qvol = max(0.0, float(self.cfg.onchain_sector_min_quote_volume_24h))
        max_peers = max(0, int(self.cfg.onchain_sector_max_peers_per_seed))

        candidates: List[Tuple[float, str]] = []
        for sym in peers:
            if sym == seed_symbol:
                continue
            qvol_24h = float(self.latest_qvol24h.get(sym, 0.0))
            if qvol_24h < min_qvol:
                continue
            candidates.append((qvol_24h, sym))
        if not candidates:
            return
        candidates.sort(key=lambda x: x[0], reverse=True)
        if max_peers > 0:
            candidates = candidates[:max_peers]

        added: List[str] = []
        for _, sym in candidates:
            prev = self.onchain_auto_expire.get(sym, 0.0)
            self._add_onchain_auto_symbol(
                sym,
                ts,
                f"sector:{sector}:{seed_symbol}",
                ttl_sec=self.cfg.onchain_sector_expand_ttl_sec,
            )
            if self.onchain_auto_expire.get(sym, 0.0) > prev:
                added.append(sym)

        if added:
            compact = ",".join(added[:8])
            if len(added) > 8:
                compact = f"{compact}...+{len(added) - 8}"
            print(f"[onchain-sector] seed={seed_symbol} sector={sector} expanded={compact}")

    def _is_onchain_target(self, symbol: str) -> bool:
        self._reload_onchain_targets_if_due()
        self._reload_onchain_sectors_if_due()
        self._prune_onchain_auto_expire()
        s = str(symbol).upper().strip()
        if not s:
            return False
        if s in self.onchain_cli_exact or s in self.onchain_file_exact or s in self.onchain_auto_expire:
            return True
        return self._match_onchain_pattern(s)

    async def _discover_onchain_pool(self, symbol: str) -> Optional[Dict[str, Any]]:
        base_asset = self._base_asset(symbol)
        query = (
            "https://api.dexscreener.com/latest/dex/search?"
            + urllib.parse.urlencode({"q": base_asset})
        )
        raw = await asyncio.to_thread(self._http_get_json, query, 8.0)
        pairs = (raw or {}).get("pairs", []) if isinstance(raw, dict) else []
        if not isinstance(pairs, list):
            return None
        best: Optional[Dict[str, Any]] = None
        best_key: Tuple[int, float, float] = (-1, -1.0, -1.0)
        for p in pairs:
            if not isinstance(p, dict):
                continue
            b = str((p.get("baseToken") or {}).get("symbol", "")).upper()
            q = str((p.get("quoteToken") or {}).get("symbol", "")).upper()
            if b != base_asset:
                continue
            liq = float((p.get("liquidity") or {}).get("usd", 0.0) or 0.0)
            if liq < self.cfg.onchain_min_liquidity_usd:
                continue
            chain_id = str(p.get("chainId", "")).lower()
            network = CHAIN_ID_TO_GECKO_NETWORK.get(chain_id)
            if not network:
                continue
            vol_h24 = float((p.get("volume") or {}).get("h24", 0.0) or 0.0)
            quote_is_stable = 1 if q in STABLE_QUOTES else 0
            key = (quote_is_stable, liq, vol_h24)
            if key > best_key:
                best_key = key
                best = {
                    "network": network,
                    "chain_id": chain_id,
                    "pool_address": str(p.get("pairAddress", "")),
                    "dex_id": str(p.get("dexId", "")),
                    "pair_url": str(p.get("url", "")),
                    "base_symbol": b,
                    "quote_symbol": q,
                    "liquidity_usd": liq,
                    "volume_h24_usd": vol_h24,
                }
        return best

    async def _get_onchain_pool_cached(self, symbol: str, ts: float) -> Optional[Dict[str, Any]]:
        cached = self.onchain_pool_cache.get(symbol)
        if cached is not None:
            cached_ts, pool = cached
            if ts - cached_ts <= self.cfg.onchain_discovery_ttl_sec:
                return pool
        pool: Optional[Dict[str, Any]]
        try:
            pool = await self._discover_onchain_pool(symbol)
        except Exception:
            pool = None
        self.onchain_pool_cache[symbol] = (ts, pool)
        return pool

    async def _fetch_onchain_micro_confirm(self, symbol: str, ts: float) -> Dict[str, float]:
        if not self._is_onchain_target(symbol):
            return {"onchain_available": 0.0, "onchain_ok": 1.0, "onchain_ret_5m_pct": 0.0, "onchain_vol_ratio_5m": 0.0}
        pool = await self._get_onchain_pool_cached(symbol, ts)
        if not pool:
            return {"onchain_available": 0.0, "onchain_ok": 0.0, "onchain_ret_5m_pct": 0.0, "onchain_vol_ratio_5m": 0.0}

        url = (
            f"https://api.geckoterminal.com/api/v2/networks/{pool['network']}/pools/{pool['pool_address']}/ohlcv/minute?"
            + urllib.parse.urlencode({"aggregate": 5, "limit": 30})
        )
        raw = await asyncio.to_thread(self._http_get_json, url, 8.0)
        rows = ((((raw or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list", [])) if isinstance(raw, dict) else []
        if not isinstance(rows, list) or len(rows) < 6:
            return {"onchain_available": 0.0, "onchain_ok": 0.0, "onchain_ret_5m_pct": 0.0, "onchain_vol_ratio_5m": 0.0}

        # geckoterminal returns newest-first
        rows = sorted(rows, key=lambda x: int(x[0]) if isinstance(x, list) and x else 0)
        try:
            close_now = float(rows[-1][4])
            close_prev = float(rows[-2][4])
            ret_5m = (close_now - close_prev) / close_prev * 100.0 if close_prev > 0 else 0.0
            vols = [float(x[5]) for x in rows[-25:]]
            v_last = vols[-1]
            v_base = vols[:-1]
            v_mean = float(statistics.mean(v_base)) if v_base else 0.0
            vol_ratio = v_last / v_mean if v_mean > 1e-9 else 0.0
            c_ret = ret_5m >= self.cfg.onchain_ret_5m_pct
            c_vol = vol_ratio >= self.cfg.onchain_vol_ratio_5m
            hit = int(c_ret) + int(c_vol)
            ok = hit >= max(1, min(2, self.cfg.onchain_min_signals))
            return {
                "onchain_available": 1.0,
                "onchain_ok": 1.0 if ok else 0.0,
                "onchain_hit": float(hit),
                "onchain_ret_5m_pct": ret_5m,
                "onchain_vol_ratio_5m": vol_ratio,
            }
        except Exception:
            return {"onchain_available": 0.0, "onchain_ok": 0.0, "onchain_ret_5m_pct": 0.0, "onchain_vol_ratio_5m": 0.0}

    async def _get_onchain_micro_cached(self, symbol: str, ts: float) -> Dict[str, float]:
        cached = self.onchain_micro_cache.get(symbol)
        if cached is not None:
            cached_ts, data = cached
            if ts - cached_ts <= self.cfg.onchain_micro_ttl_sec:
                return data
        data = await self._fetch_onchain_micro_confirm(symbol, ts)
        self.onchain_micro_cache[symbol] = (ts, data)
        return data

    async def _fetch_micro_confirm(self, symbol: str, ts: float, source: str = "binance") -> Dict[str, float]:
        """
        Lightweight REST confirm used only for shortlisted candidates.
        Pulls:
        - source exchange 1m klines (ret / volume ratio / CVD z)
        - source exchange OI history 5m
        - source exchange 5m klines (S/R structure)
        - Bybit 1m kline (cross-exchange confirm)
        - Optional onchain micro confirm
        """
        source_l = str(source or "binance").strip().lower() or "binance"
        binance_base = "https://fapi.binance.com"
        rest_base = (
            str(self.cfg.aster_rest_base_url or "").rstrip("/")
            if source_l == "aster"
            else binance_base
        )
        if not rest_base:
            rest_base = binance_base
        kline_1m_url = (
            f"{rest_base}/fapi/v1/klines?"
            + urllib.parse.urlencode({"symbol": symbol, "interval": "1m", "limit": 45})
        )
        kline_5m_url = (
            f"{rest_base}/fapi/v1/klines?"
            + urllib.parse.urlencode({"symbol": symbol, "interval": "5m", "limit": 120})
        )
        kline_15m_url = (
            f"{rest_base}/fapi/v1/klines?"
            + urllib.parse.urlencode({"symbol": symbol, "interval": "15m", "limit": 120})
        )
        oi_hist_urls = self._build_oi_hist_urls(symbol, rest_base)
        oi_now_urls = self._build_oi_now_urls(symbol, rest_base)
        lsr_url = (
            f"{rest_base}/futures/data/globalLongShortAccountRatio?"
            + urllib.parse.urlencode({"symbol": symbol, "period": "5m", "limit": 30})
        )
        top_pos_lsr_url = (
            f"{rest_base}/futures/data/topLongShortPositionRatio?"
            + urllib.parse.urlencode({"symbol": symbol, "period": "5m", "limit": 30})
        )
        top_acc_lsr_url = (
            f"{rest_base}/futures/data/topLongShortAccountRatio?"
            + urllib.parse.urlencode({"symbol": symbol, "period": "5m", "limit": 30})
        )
        taker_lsr_url = (
            f"{rest_base}/futures/data/takerlongshortRatio?"
            + urllib.parse.urlencode({"symbol": symbol, "period": "5m", "limit": 30})
        )
        funding_hist_url = (
            f"{rest_base}/fapi/v1/fundingRate?"
            + urllib.parse.urlencode({"symbol": symbol, "limit": 16})
        )
        bybit_url = (
            "https://api.bybit.com/v5/market/kline?"
            + urllib.parse.urlencode({"category": "linear", "symbol": symbol, "interval": "1", "limit": 45})
        )
        mexc_url = (
            "https://api.mexc.com/api/v3/klines?"
            + urllib.parse.urlencode({"symbol": symbol, "interval": "1m", "limit": 45})
        )
        depth_url = (
            f"{rest_base}/fapi/v1/depth?"
            + urllib.parse.urlencode({"symbol": symbol, "limit": 20})
        )
        agg_trades_url = (
            f"{rest_base}/fapi/v1/aggTrades?"
            + urllib.parse.urlencode({"symbol": symbol, "limit": 120})
        )
        (
            kline_raw,
            kline5_raw,
            kline15_raw,
            oi_raw,
            oi_now_raw,
            lsr_raw,
            top_pos_lsr_raw,
            top_acc_lsr_raw,
            taker_lsr_raw,
            funding_hist_raw,
            bybit_raw,
            mexc_raw,
            depth_raw,
            agg_raw,
        ) = await asyncio.gather(
            asyncio.to_thread(self._http_get_json, kline_1m_url, 4.0),
            asyncio.to_thread(self._http_get_json, kline_5m_url, 4.0),
            asyncio.to_thread(self._http_get_json, kline_15m_url, 4.0),
            asyncio.to_thread(self._http_get_json_first, oi_hist_urls, 4.0),
            asyncio.to_thread(self._http_get_json_first, oi_now_urls, 4.0),
            asyncio.to_thread(self._http_get_json, lsr_url, 4.0),
            (
                asyncio.to_thread(self._http_get_json, top_pos_lsr_url, 4.0)
                if self.cfg.sentiment_enabled
                else asyncio.sleep(0.0, result=[])
            ),
            (
                asyncio.to_thread(self._http_get_json, top_acc_lsr_url, 4.0)
                if self.cfg.sentiment_enabled
                else asyncio.sleep(0.0, result=[])
            ),
            (
                asyncio.to_thread(self._http_get_json, taker_lsr_url, 4.0)
                if self.cfg.sentiment_enabled
                else asyncio.sleep(0.0, result=[])
            ),
            (
                asyncio.to_thread(self._http_get_json, funding_hist_url, 4.0)
                if self.cfg.sentiment_enabled
                else asyncio.sleep(0.0, result=[])
            ),
            asyncio.to_thread(self._http_get_json, bybit_url, 4.0),
            asyncio.to_thread(self._http_get_json, mexc_url, 4.0),
            asyncio.to_thread(self._http_get_json, depth_url, 4.0),
            asyncio.to_thread(self._http_get_json, agg_trades_url, 4.0),
            return_exceptions=True,
        )

        ret_1m_pct = 0.0
        vol_ratio_1m = 0.0
        cvd_z = 0.0
        cvd_z_simple = 0.0
        if isinstance(kline_raw, list) and len(kline_raw) >= 22:
            try:
                close_now = float(kline_raw[-1][4])
                close_prev = float(kline_raw[-2][4])
                if close_prev > 0:
                    ret_1m_pct = (close_now - close_prev) / close_prev * 100.0

                qv = [float(x[7]) for x in kline_raw[-22:]]
                last_qv = qv[-1]
                base_qv = qv[:-1]
                base_mean = float(statistics.mean(base_qv)) if base_qv else 0.0
                if base_mean > 1e-9:
                    vol_ratio_1m = last_qv / base_mean

                # CVD proxy using taker buy quote - taker sell quote
                cvd_series = []
                for x in kline_raw[-35:]:
                    q = float(x[7])
                    b = float(x[10])
                    cvd_series.append(2.0 * b - q)
                if len(cvd_series) >= 8:
                    cvd_z_simple, cvd_z = self._calc_cvd_zscores(
                        cvd_series,
                        self._cvd_tau_for_symbol(symbol),
                    )
            except Exception:
                pass

        ret_5m_pct = 0.0
        breakout_pullback_pct = 0.0
        if isinstance(kline5_raw, list) and len(kline5_raw) >= 4:
            try:
                c_now = float(kline5_raw[-1][4])
                c_prev = float(kline5_raw[-2][4])
                if c_prev > 0:
                    ret_5m_pct = (c_now - c_prev) / c_prev * 100.0
                h_recent = max(float(x[2]) for x in kline5_raw[-5:])
                if h_recent > 0:
                    breakout_pullback_pct = max(0.0, (h_recent - c_now) / h_recent * 100.0)
            except Exception:
                pass

        atr_15m_pct = 0.0
        slope_15m_pct = 0.0
        listing_age_days = 9999.0
        if isinstance(kline15_raw, list) and len(kline15_raw) >= 8:
            try:
                closes = np.asarray([float(x[4]) for x in kline15_raw[-40:]], dtype=np.float64)
                highs = np.asarray([float(x[2]) for x in kline15_raw[-40:]], dtype=np.float64)
                lows = np.asarray([float(x[3]) for x in kline15_raw[-40:]], dtype=np.float64)
                n15 = int(min(closes.size, highs.size, lows.size))
                listing_age_days = max(0.0, n15 * 15.0 / (24.0 * 60.0))
                if n15 >= 3:
                    prev_close = closes[:-1]
                    tr = np.maximum.reduce([
                        highs[1:] - lows[1:],
                        np.abs(highs[1:] - prev_close),
                        np.abs(lows[1:] - prev_close),
                    ])
                    if tr.size > 0 and closes[-1] > 0:
                        atr_15m_pct = float(np.mean(tr[-14:]) / closes[-1] * 100.0)
                if closes.size >= 8 and closes[-1] > 0:
                    y = closes[-24:] if closes.size >= 24 else closes
                    x = np.arange(y.size, dtype=np.float64)
                    xm = float(np.mean(x))
                    ym = float(np.mean(y))
                    den = float(np.sum((x - xm) ** 2))
                    if den > 1e-12:
                        slope_15m_pct = float(np.sum((x - xm) * (y - ym)) / den / closes[-1] * 100.0)
            except Exception:
                pass

        vol_1h_pct = 0.0
        delta_persist_score = 0.0
        delta_pos_streak = 0.0
        delta_neg_streak = 0.0
        if isinstance(kline_raw, list) and len(kline_raw) >= 16:
            try:
                closes = np.asarray([float(x[4]) for x in kline_raw[-65:]], dtype=np.float64)
                if closes.size >= 3:
                    rets = np.diff(closes) / np.maximum(closes[:-1], 1e-9) * 100.0
                    vol_1h_pct = float(np.std(rets[-60:])) if rets.size >= 2 else 0.0
                deltas = np.asarray([(2.0 * float(x[10]) - float(x[7])) for x in kline_raw[-10:]], dtype=np.float64)
                signs = np.sign(deltas)
                if signs.size > 0:
                    p = float(np.mean(signs > 0))
                    n = float(np.mean(signs < 0))
                    delta_persist_score = max(p, n)
                    pos = 0
                    neg = 0
                    for sgn in signs[::-1]:
                        if sgn > 0:
                            if neg == 0:
                                pos += 1
                            else:
                                break
                        elif sgn < 0:
                            if pos == 0:
                                neg += 1
                            else:
                                break
                        else:
                            break
                    delta_pos_streak = float(pos)
                    delta_neg_streak = float(neg)
            except Exception:
                pass

        oi_change_5m_pct = 0.0
        oi_hist_points: List[Tuple[float, float]] = []
        oi_now_value = 0.0
        if not isinstance(oi_raw, Exception):
            try:
                oi_hist_points = self._extract_oi_points(oi_raw, ts)
            except Exception:
                oi_hist_points = []
        if oi_hist_points:
            for point_ts, point_oi in oi_hist_points:
                self._record_oi_point(source_l, symbol, point_ts, point_oi)
            oi_change_5m_pct = self._calc_oi_change_from_points(oi_hist_points, ts, window_sec=300.0)

        if not isinstance(oi_now_raw, Exception):
            try:
                oi_now_value = self._extract_oi_now(oi_now_raw)
            except Exception:
                oi_now_value = 0.0
        if oi_now_value > 0.0:
            self._record_oi_point(source_l, symbol, ts, oi_now_value)

        if abs(oi_change_5m_pct) <= 1e-9:
            oi_change_5m_pct = self._calc_oi_change_from_cache(source_l, symbol, ts, window_sec=300.0)
        if abs(oi_change_5m_pct) <= 1e-9 and not oi_hist_points and oi_now_value <= 0.0:
            self._maybe_warn_oi_unavailable(source_l, symbol, ts, "hist_now_empty")

        lsr_5m_chg_pct = 0.0
        flow_has_lsr = 0.0
        if isinstance(lsr_raw, list) and len(lsr_raw) >= 2:
            try:
                prev = float(lsr_raw[-2].get("longShortRatio", 0.0) or 0.0)
                now = float(lsr_raw[-1].get("longShortRatio", 0.0) or 0.0)
                if prev > 0 and now > 0:
                    lsr_5m_chg_pct = (now - prev) / prev * 100.0
                    flow_has_lsr = 1.0
            except Exception:
                pass

        def _ratio_change_pct(raw_rows: Any, keys: Tuple[str, ...]) -> float:
            if not isinstance(raw_rows, list) or len(raw_rows) < 2:
                return 0.0
            try:
                prev = 0.0
                now = 0.0
                for k in keys:
                    if prev <= 0.0:
                        prev = self._safe_float(raw_rows[-2].get(k, 0.0), 0.0)
                    if now <= 0.0:
                        now = self._safe_float(raw_rows[-1].get(k, 0.0), 0.0)
                if prev > 0.0 and now > 0.0:
                    return (now - prev) / prev * 100.0
            except Exception:
                return 0.0
            return 0.0

        top_pos_lsr_5m_chg_pct = _ratio_change_pct(top_pos_lsr_raw, ("longShortRatio",))
        top_acc_lsr_5m_chg_pct = _ratio_change_pct(top_acc_lsr_raw, ("longShortRatio",))
        taker_lsr_5m_chg_pct = _ratio_change_pct(taker_lsr_raw, ("buySellRatio", "longShortRatio"))

        funding_hist_delta = 0.0
        funding_hist_std = 0.0
        if isinstance(funding_hist_raw, list) and len(funding_hist_raw) >= 2:
            try:
                rates = np.asarray([self._safe_float(x.get("fundingRate", 0.0), 0.0) for x in funding_hist_raw[-8:]], dtype=np.float64)
                if rates.size >= 2:
                    funding_hist_delta = float(rates[-1] - rates[0])
                    funding_hist_std = float(np.std(rates))
            except Exception:
                pass

        taker_buy_ratio = 0.0
        buy_imbalance = 0.0
        flow_available = 0.0
        flow_hit = 0
        flow_ok = 1.0 if self.cfg.flow_grace_no_data else 0.0
        if isinstance(kline_raw, list) and len(kline_raw) >= 1:
            try:
                q = float(kline_raw[-1][7])
                b = float(kline_raw[-1][10])
                if q > 1e-9:
                    taker_buy_ratio = b / q
                    buy_imbalance = (2.0 * b - q) / q
                    flow_available = 1.0
                    flow_hit = int(taker_buy_ratio >= self.cfg.flow_buy_ratio_min) + int(
                        buy_imbalance >= self.cfg.flow_imbalance_min
                    )
                    if flow_has_lsr >= 0.5:
                        flow_hit += int(lsr_5m_chg_pct >= self.cfg.flow_lsr_5m_min_pct)
                    required = max(1, min(3, self.cfg.flow_min_signals))
                    if flow_has_lsr < 0.5 and self.cfg.flow_grace_no_data:
                        required = min(required, 2)
                    flow_ok = 1.0 if flow_hit >= required else 0.0
            except Exception:
                pass

        # S/R micro structure
        sr_ok = 0.0
        sr_breakout = 0.0
        sr_support = 0.0
        sr_dist_res_pct = 999.0
        sr_dist_sup_pct = 999.0
        sr_compression_pct = 999.0
        if isinstance(kline5_raw, list) and len(kline5_raw) >= 24:
            try:
                lookback = min(max(24, self.cfg.sr_lookback_5m), len(kline5_raw) - 1)
                prev = kline5_raw[-lookback - 1 : -1]
                close5 = float(kline5_raw[-1][4])
                highs = [float(x[2]) for x in prev]
                lows = [float(x[3]) for x in prev]
                if highs and lows and close5 > 0:
                    res = max(highs)
                    sup = min(lows)
                    sr_dist_res_pct = (res - close5) / close5 * 100.0
                    sr_dist_sup_pct = (close5 - sup) / close5 * 100.0
                    comp_prev = kline5_raw[-13:-1] if len(kline5_raw) >= 13 else prev
                    comp_high = max(float(x[2]) for x in comp_prev)
                    comp_low = min(float(x[3]) for x in comp_prev)
                    sr_compression_pct = (comp_high - comp_low) / close5 * 100.0
                    brk = 0.0 <= sr_dist_res_pct <= self.cfg.sr_breakout_dist_pct and sr_compression_pct <= self.cfg.sr_max_compression_pct
                    sup_ok = 0.0 <= sr_dist_sup_pct <= self.cfg.sr_support_dist_pct and sr_compression_pct <= self.cfg.sr_max_compression_pct * 1.3
                    sr_breakout = 1.0 if brk else 0.0
                    sr_support = 1.0 if sup_ok else 0.0
                    sr_ok = 1.0 if (brk or sup_ok) else 0.0
            except Exception:
                pass

        # Bybit cross confirm
        cross_available = 0.0
        bybit_ret_1m_pct = 0.0
        bybit_vol_ratio_1m = 0.0
        mexc_ret_1m_pct = 0.0
        mexc_vol_ratio_1m = 0.0
        cross_ok = 1.0 if self.cfg.cross_grace_no_data else 0.0
        bybit_hit = False
        mexc_hit = False
        bybit_has_data = False
        mexc_has_data = False
        if isinstance(bybit_raw, dict):
            rows = (((bybit_raw.get("result") or {}).get("list")) or [])
            if isinstance(rows, list) and len(rows) >= 22:
                try:
                    rows = sorted(rows, key=lambda x: int(x[0]) if isinstance(x, list) and x else 0)
                    close_now = float(rows[-1][4])
                    close_prev = float(rows[-2][4])
                    if close_prev > 0:
                        bybit_ret_1m_pct = (close_now - close_prev) / close_prev * 100.0
                    qv = [float(x[6]) for x in rows[-22:]]
                    qv_last = qv[-1]
                    qv_mean = float(statistics.mean(qv[:-1])) if len(qv) > 1 else 0.0
                    if qv_mean > 1e-9:
                        bybit_vol_ratio_1m = qv_last / qv_mean
                    bybit_has_data = True
                    bybit_hit = (
                        bybit_ret_1m_pct >= self.cfg.cross_ret_1m_pct
                        and bybit_vol_ratio_1m >= self.cfg.cross_vol_ratio_1m
                    )
                except Exception:
                    pass
        if isinstance(mexc_raw, list) and len(mexc_raw) >= 22:
            try:
                rows = sorted(mexc_raw, key=lambda x: int(x[0]) if isinstance(x, list) and x else 0)
                close_now = float(rows[-1][4])
                close_prev = float(rows[-2][4])
                if close_prev > 0:
                    mexc_ret_1m_pct = (close_now - close_prev) / close_prev * 100.0
                qv = [float(x[7]) for x in rows[-22:]]
                qv_last = qv[-1]
                qv_mean = float(statistics.mean(qv[:-1])) if len(qv) > 1 else 0.0
                if qv_mean > 1e-9:
                    mexc_vol_ratio_1m = qv_last / qv_mean
                mexc_has_data = True
                mexc_hit = (
                    mexc_ret_1m_pct >= self.cfg.cross_ret_1m_pct
                    and mexc_vol_ratio_1m >= self.cfg.cross_vol_ratio_1m
                )
            except Exception:
                pass

        if bybit_has_data or mexc_has_data:
            cross_available = 1.0
            cross_ok = 1.0 if (bybit_hit or mexc_hit) else 0.0

        cross_ret_diff_pct = 0.0
        if bybit_has_data:
            cross_ret_diff_pct = abs(ret_1m_pct - bybit_ret_1m_pct)
        elif mexc_has_data:
            cross_ret_diff_pct = abs(ret_1m_pct - mexc_ret_1m_pct)

        spread_bps = 0.0
        depth_bid_usd_top5 = 0.0
        depth_ask_usd_top5 = 0.0
        depth_total_usd_top5 = 0.0
        book_ratio_top5 = 1.0
        slippage_est_pct = 0.0
        if isinstance(depth_raw, dict):
            try:
                bids_raw = depth_raw.get("bids", [])
                asks_raw = depth_raw.get("asks", [])
                bids = [(float(x[0]), float(x[1])) for x in bids_raw[:20] if isinstance(x, list) and len(x) >= 2]
                asks = [(float(x[0]), float(x[1])) for x in asks_raw[:20] if isinstance(x, list) and len(x) >= 2]
                if bids and asks:
                    best_bid = bids[0][0]
                    best_ask = asks[0][0]
                    mid = (best_bid + best_ask) * 0.5
                    if mid > 0:
                        spread_bps = max(0.0, (best_ask - best_bid) / mid * 10000.0)
                    depth_bid_usd_top5 = float(sum(p * q for p, q in bids[:5]))
                    depth_ask_usd_top5 = float(sum(p * q for p, q in asks[:5]))
                    depth_total_usd_top5 = depth_bid_usd_top5 + depth_ask_usd_top5
                    if depth_ask_usd_top5 > 1e-9:
                        book_ratio_top5 = depth_bid_usd_top5 / depth_ask_usd_top5

                    def _slip(side: str) -> float:
                        need = float(self.cfg.risk_slippage_notional_usd)
                        if need <= 0 or mid <= 0:
                            return 0.0
                        levels = asks if side == "buy" else bids
                        remain = need
                        notional = 0.0
                        base_qty_sum = 0.0
                        for px, qty in levels:
                            level_notional = px * qty
                            take = min(remain, level_notional)
                            if take <= 0:
                                continue
                            base_qty = take / max(px, 1e-9)
                            notional += base_qty * px
                            base_qty_sum += base_qty
                            remain -= take
                            if remain <= 1e-9:
                                break
                        if base_qty_sum <= 1e-12:
                            return 0.0
                        avg_px = notional / base_qty_sum
                        if side == "buy":
                            return max(0.0, (avg_px - mid) / mid * 100.0)
                        return max(0.0, (mid - avg_px) / mid * 100.0)

                    slippage_est_pct = max(_slip("buy"), _slip("sell"))
            except Exception:
                pass

        trade_rate_per_sec = 0.0
        iceberg_prob = 0.0
        iceberg_conf = 0.0
        whale_notional_1m = 0.0
        whale_trade_count_1m = 0.0
        trade_notional_top10_ratio = 0.0
        if isinstance(agg_raw, list) and len(agg_raw) >= 10:
            try:
                rows = sorted(agg_raw, key=lambda x: int(x.get("T", 0)) if isinstance(x, dict) else 0)
                t0 = float(rows[0].get("T", 0) or 0.0)
                t1 = float(rows[-1].get("T", 0) or 0.0)
                if t1 > t0:
                    trade_rate_per_sec = len(rows) / max((t1 - t0) / 1000.0, 1e-6)
                window_cutoff = t1 - 60_000.0
                whale_thr = max(1.0, float(self.cfg.sentiment_whale_notional_usd))
                notionals_all: List[float] = []
                qty_map: Dict[str, int] = defaultdict(int)
                maker_switch = 0
                prev_m: Optional[bool] = None
                for r in rows[-80:]:
                    q = float(r.get("q", 0.0) or 0.0)
                    p = float(r.get("p", 0.0) or 0.0)
                    t = float(r.get("T", 0.0) or 0.0)
                    notional = max(0.0, q * p)
                    notionals_all.append(notional)
                    if t >= window_cutoff and notional >= whale_thr:
                        whale_notional_1m += notional
                        whale_trade_count_1m += 1.0
                    key = f"{q:.3f}"
                    qty_map[key] += 1
                    m = bool(r.get("m", False))
                    if prev_m is not None and prev_m != m:
                        maker_switch += 1
                    prev_m = m
                repeats = [v for v in qty_map.values() if v >= 4]
                rep_score = float(sum(repeats)) / max(1.0, float(len(rows[-80:])))
                switch_score = 1.0 - self._clamp(maker_switch / max(1.0, float(len(rows[-80:]) - 1)), 0.0, 1.0)
                iceberg_prob = self._clamp(0.75 * rep_score + 0.25 * switch_score, 0.0, 1.0)
                iceberg_conf = self._clamp(min(1.0, len(rows) / 80.0), 0.2, 1.0)
                if notionals_all:
                    total_notional = float(sum(notionals_all))
                    if total_notional > 1e-9:
                        top10 = sorted(notionals_all, reverse=True)[:10]
                        trade_notional_top10_ratio = float(sum(top10) / total_notional)
            except Exception:
                pass

        onchain_data = await self._get_onchain_micro_cached(symbol, ts)

        return {
            "ret_1m_pct": ret_1m_pct,
            "ret_5m_pct": ret_5m_pct,
            "vol_ratio_1m": vol_ratio_1m,
            "cvd_z": cvd_z,
            "cvd_z_simple": cvd_z_simple,
            "cvd_z_decayed": cvd_z,
            "oi_change_5m_pct": oi_change_5m_pct,
            "flow_available": flow_available,
            "flow_ok": flow_ok,
            "flow_hit": float(flow_hit),
            "flow_has_lsr": flow_has_lsr,
            "taker_buy_ratio": taker_buy_ratio,
            "buy_imbalance": buy_imbalance,
            "lsr_5m_chg_pct": lsr_5m_chg_pct,
            "top_pos_lsr_5m_chg_pct": top_pos_lsr_5m_chg_pct,
            "top_acc_lsr_5m_chg_pct": top_acc_lsr_5m_chg_pct,
            "taker_lsr_5m_chg_pct": taker_lsr_5m_chg_pct,
            "funding_hist_delta": funding_hist_delta,
            "funding_hist_std": funding_hist_std,
            "delta_persist_score": delta_persist_score,
            "delta_pos_streak": delta_pos_streak,
            "delta_neg_streak": delta_neg_streak,
            "sr_ok": sr_ok,
            "sr_breakout": sr_breakout,
            "sr_support": sr_support,
            "sr_dist_res_pct": sr_dist_res_pct,
            "sr_dist_sup_pct": sr_dist_sup_pct,
            "sr_compression_pct": sr_compression_pct,
            "breakout_pullback_pct": breakout_pullback_pct,
            "cross_available": cross_available,
            "cross_ok": cross_ok,
            "bybit_ret_1m_pct": bybit_ret_1m_pct,
            "bybit_vol_ratio_1m": bybit_vol_ratio_1m,
            "mexc_ret_1m_pct": mexc_ret_1m_pct,
            "mexc_vol_ratio_1m": mexc_vol_ratio_1m,
            "cross_ret_diff_pct": cross_ret_diff_pct,
            "atr_15m_pct": atr_15m_pct,
            "vol_1h_pct": vol_1h_pct,
            "slope_15m_pct": slope_15m_pct,
            "listing_age_days": listing_age_days,
            "spread_bps": spread_bps,
            "depth_bid_usd_top5": depth_bid_usd_top5,
            "depth_ask_usd_top5": depth_ask_usd_top5,
            "depth_total_usd_top5": depth_total_usd_top5,
            "book_ratio_top5": book_ratio_top5,
            "slippage_est_pct": slippage_est_pct,
            "trade_rate_per_sec": trade_rate_per_sec,
            "iceberg_prob": iceberg_prob,
            "iceberg_conf": iceberg_conf,
            "whale_notional_1m": whale_notional_1m,
            "whale_trade_count_1m": whale_trade_count_1m,
            "trade_notional_top10_ratio": trade_notional_top10_ratio,
            **onchain_data,
        }

    async def _get_micro_confirm_cached(self, symbol: str, ts: float, source: str = "binance") -> Dict[str, float]:
        source_l = str(source or "binance").strip().lower() or "binance"
        cache_key = f"{source_l}:{symbol}"
        cached = self.micro_cache.get(cache_key)
        if cached is not None:
            cached_ts, cached_data = cached
            if ts - cached_ts <= self.cfg.micro_cache_ttl_sec:
                self._update_market_state_histories(symbol, cached_data)
                return cached_data
        data = await self._fetch_micro_confirm(symbol, ts, source=source_l)
        self.micro_cache[cache_key] = (ts, data)
        self._update_market_state_histories(symbol, data)
        return data

    def _passes_denoise_filters(
        self,
        symbol: str,
        micro: Optional[Dict[str, float]],
        *,
        n_hit: int = 0,
        ret_1m_pct: float = 0.0,
        oi5m_pct: float = 0.0,
    ) -> Tuple[bool, Dict[str, float]]:
        details: Dict[str, float] = {}

        # Free flow
        flow_ok = True
        if self.cfg.flow_filter_enabled:
            flow_available = bool(micro) and float((micro or {}).get("flow_available", 0.0)) >= 0.5
            flow_hit = int(float((micro or {}).get("flow_hit", 0.0)))
            flow_required = max(1, min(3, int(self.cfg.flow_min_signals)))
            flow_relaxed = 0.0
            if (
                self.cfg.flow_relax_enabled
                and (float(ret_1m_pct) >= self.cfg.flow_relax_ret_1m_pct or float(oi5m_pct) >= self.cfg.flow_relax_oi5m_pct)
            ):
                flow_required = min(flow_required, max(1, int(self.cfg.flow_relax_min_signals)))
                flow_relaxed = 1.0
            if flow_available:
                flow_ok = flow_hit >= flow_required
            else:
                flow_ok = self.cfg.flow_grace_no_data
            details["flow_ok"] = 1.0 if flow_ok else 0.0
            details["flow_available"] = 1.0 if flow_available else 0.0
            details["flow_hit"] = float(flow_hit)
            details["flow_required"] = float(flow_required)
            details["flow_relaxed"] = flow_relaxed
            details["flow_has_lsr"] = float((micro or {}).get("flow_has_lsr", 0.0))
            details["taker_buy_ratio"] = float((micro or {}).get("taker_buy_ratio", 0.0))
            details["buy_imbalance"] = float((micro or {}).get("buy_imbalance", 0.0))
            details["lsr_5m_chg_pct"] = float((micro or {}).get("lsr_5m_chg_pct", 0.0))

        # S/R
        sr_ok = True
        if self.cfg.sr_filter_enabled:
            sr_raw_ok = bool(micro) and float((micro or {}).get("sr_ok", 0.0)) >= 0.5
            sr_soft_pass = False
            if (
                (not sr_raw_ok)
                and self.cfg.sr_soft_pass_enabled
                and int(n_hit) >= int(self.cfg.sr_soft_min_hit)
                and float(oi5m_pct) >= float(self.cfg.sr_soft_min_oi5m_pct)
            ):
                sr_soft_pass = True
            sr_ok = bool(sr_raw_ok or sr_soft_pass)
            details["sr_ok"] = 1.0 if sr_ok else 0.0
            details["sr_soft_pass"] = 1.0 if sr_soft_pass else 0.0
            details["sr_soft_qualified"] = (
                1.0
                if (int(n_hit) >= int(self.cfg.sr_soft_min_hit) and float(oi5m_pct) >= float(self.cfg.sr_soft_min_oi5m_pct))
                else 0.0
            )
            details["sr_dist_res_pct"] = float((micro or {}).get("sr_dist_res_pct", 0.0))
            details["sr_dist_sup_pct"] = float((micro or {}).get("sr_dist_sup_pct", 0.0))
            details["sr_compression_pct"] = float((micro or {}).get("sr_compression_pct", 0.0))

        # Cross-exchange
        cross_ok = True
        if self.cfg.cross_filter_enabled:
            cross_available = bool(micro) and float((micro or {}).get("cross_available", 0.0)) >= 0.5
            if cross_available:
                cross_ok = float((micro or {}).get("cross_ok", 0.0)) >= 0.5
            else:
                cross_ok = self.cfg.cross_grace_no_data
            details["cross_ok"] = 1.0 if cross_ok else 0.0
            details["cross_available"] = 1.0 if cross_available else 0.0
            details["bybit_ret_1m_pct"] = float((micro or {}).get("bybit_ret_1m_pct", 0.0))
            details["bybit_vol_ratio_1m"] = float((micro or {}).get("bybit_vol_ratio_1m", 0.0))
            details["mexc_ret_1m_pct"] = float((micro or {}).get("mexc_ret_1m_pct", 0.0))
            details["mexc_vol_ratio_1m"] = float((micro or {}).get("mexc_vol_ratio_1m", 0.0))

        # Onchain
        onchain_ok = True
        onchain_target = self._is_onchain_target(symbol)
        if self.cfg.onchain_filter_enabled and onchain_target:
            available = bool(micro) and float((micro or {}).get("onchain_available", 0.0)) >= 0.5
            if available:
                onchain_ok = float((micro or {}).get("onchain_ok", 0.0)) >= 0.5
            else:
                onchain_ok = self.cfg.onchain_grace_no_data
            details["onchain_ok"] = 1.0 if onchain_ok else 0.0
            details["onchain_available"] = 1.0 if available else 0.0
            details["onchain_ret_5m_pct"] = float((micro or {}).get("onchain_ret_5m_pct", 0.0))
            details["onchain_vol_ratio_5m"] = float((micro or {}).get("onchain_vol_ratio_5m", 0.0))

        return bool(flow_ok and sr_ok and cross_ok and onchain_ok), details

    def _should_trigger(
        self,
        symbol: str,
        pump_pct: float,
        vol_1m: float,
        pump_20s: float,
        vol_20s: float,
        vol_ratio_1m: float,
        vol_z_1m: float,
        micro: Optional[Dict[str, float]],
        source: str = "binance",
    ) -> Tuple[bool, str, Dict[str, float]]:
        now_ts = time.time()
        dyn_hard_pump, dyn_flow_pump, dyn_scale, dyn_meta = self._adaptive_trigger_thresholds(symbol, micro, now_ts)
        base_details = {
            "pump_1m_pct": pump_pct,
            "pump_20s_pct": pump_20s,
            "vol_20s": vol_20s,
            "ret_1m_pct": float((micro or {}).get("ret_1m_pct", pump_pct) or pump_pct),
            "vol_ratio_1m": vol_ratio_1m,
            "vol_z_1m": vol_z_1m,
            "dyn_hard_pump_pct": dyn_hard_pump,
            "dyn_flow_pump_pct": dyn_flow_pump,
            "dyn_scale": dyn_scale,
            **dyn_meta,
        }
        if isinstance(micro, dict):
            for k, v in micro.items():
                if isinstance(v, (int, float)):
                    base_details[str(k)] = float(v)

        micro_oi_5m = float((micro or {}).get("oi_change_5m_pct", 0.0))
        fake_breakout, fake_details = self._is_fake_breakout(
            micro,
            ret_1m_pct=pump_pct,
            vol_ratio_1m=vol_ratio_1m,
            oi5m_pct=micro_oi_5m,
        )
        if fake_breakout:
            fake_flags_v = float(fake_details.get("fake_flags", 0.0) or 0.0)
            last_price_v = float(self.latest_price.get(symbol, 0.0) or 0.0)
            slow_emitted = self._maybe_emit_slow_accumulation_signal(
                symbol=symbol,
                ts=now_ts,
                last_price=last_price_v,
                pump_pct=float(pump_pct),
                vol_1m=float(vol_1m),
                fake_flags=float(fake_flags_v),
                source=source,
            )
            whale_state: Dict[str, float] = {}
            if not slow_emitted:
                whale_state = self._update_whale_testing_state(
                    symbol=symbol,
                    ts=now_ts,
                    last_price=last_price_v,
                    fake_flags=float(fake_flags_v),
                )
            bjt_text = self._fmt_bjt(now_ts)
            print(
                f"[denoise] {symbol} fake_breakout_watch time(BJT)={bjt_text} "
                f"ret_1m={pump_pct:.2f}% flags={int(fake_flags_v)}"
            )
            payload = {
                "type": "fake_breakout_watch",
                "trigger_source": str(source or "binance").strip().lower() or "binance",
                "last_price": float(last_price_v),
                "ret_1m_pct": float(pump_pct),
                "pump_20s_pct": float(pump_20s),
                "vol_1m": float(vol_1m),
                "vol_ratio_1m": float(vol_ratio_1m),
                "vol_z_1m": float(vol_z_1m),
                "oi5m_pct": float(micro_oi_5m),
                "fake_flags": float(fake_flags_v),
                **{k: float(v) for k, v in fake_details.items() if isinstance(v, (int, float))},
            }
            if whale_state:
                payload.update(
                    {
                        "whale_testing_active": float(whale_state.get("active", 0.0) or 0.0),
                        "whale_testing_hits": float(whale_state.get("hits", 0.0) or 0.0),
                        "whale_testing_drawdown_pct": float(whale_state.get("drawdown_pct", 0.0) or 0.0),
                    }
                )
            self._persist_signal_event(
                ts=float(now_ts),
                symbol=symbol,
                reason="fake_breakout_watch",
                side="neutral",
                level="WATCH",
                alert=False,
                base_score=0.0,
                threshold=float(max(1, int(self.cfg.fake_breakout_min_flags))),
                tier="TIER4",
                delivery="record_only",
                final_score=0.0,
                hit_count=int(fake_flags_v),
                payload=payload,
            )
            if slow_emitted:
                return False, "", {}
            if not slow_emitted:
                self._maybe_emit_fake_breakout_probe_signal(
                    symbol=symbol,
                    ts=now_ts,
                    last_price=last_price_v,
                    pump_pct=float(pump_pct),
                    fake_flags=float(fake_flags_v),
                    source=source,
                )
            if whale_state:
                base_details.update(
                    {
                        "whale_testing_active": float(whale_state.get("active", 0.0) or 0.0),
                        "whale_testing_hits": float(whale_state.get("hits", 0.0) or 0.0),
                        "whale_testing_drawdown_pct": float(whale_state.get("drawdown_pct", 0.0) or 0.0),
                    }
                )
        base_details.update(fake_details)

        liq_action, liq_details = self._handle_liquidation_spike(
            symbol=symbol,
            ts=now_ts,
            last_price=float(self.latest_price.get(symbol, 0.0) or 0.0),
            pump_pct=float(pump_pct),
            pump_20s=float(pump_20s),
            vol_ratio_1m=float(vol_ratio_1m),
            fake_breakout=bool(fake_breakout),
            fake_flags=float(fake_details.get("fake_flags", 0.0) or 0.0),
            micro=micro,
            source=source,
        )
        if liq_details:
            base_details.update({k: float(v) for k, v in liq_details.items()})
        if liq_action == "confirm":
            return True, "liquidation_right_side", base_details
        if liq_action == "block":
            return False, "", {}

        if pump_pct >= dyn_hard_pump:
            return True, "hard_pump", base_details
        flow_ret_gate = float(self.cfg.flow_pump_min_ret_1m_pct)
        base_details["flow_ret_gate_pct"] = flow_ret_gate
        base_details["flow_min_volume_1m"] = float(self.cfg.flow_pump_min_volume_1m)
        base_details["flow_min_vol_ratio_1m"] = float(self.cfg.flow_pump_min_vol_ratio_1m)
        if (
            pump_pct >= flow_ret_gate
            and vol_1m >= float(self.cfg.flow_pump_min_volume_1m)
            and vol_ratio_1m >= float(self.cfg.flow_pump_min_vol_ratio_1m)
        ):
            return True, "flow_pump", base_details

        # 慢拉突破补捉：1m 不够暴力但 5m 延续 + OI/流动性跟随
        if self.cfg.slow_breakout_enabled:
            ret_5m_pct = float((micro or {}).get("ret_5m_pct", 0.0) or 0.0)
            delta_persist = float((micro or {}).get("delta_persist_score", 0.0) or 0.0)
            cond_ret = (
                pump_pct >= float(self.cfg.slow_breakout_ret_1m_pct)
                and ret_5m_pct >= float(self.cfg.slow_breakout_ret_5m_pct)
            )
            cond_flow_or_oi = (
                vol_ratio_1m >= float(self.cfg.slow_breakout_min_vol_ratio_1m)
                or micro_oi_5m >= float(self.cfg.slow_breakout_min_oi5m_pct)
            )
            cond_quality = (
                (not fake_breakout)
                or delta_persist >= float(self.cfg.slow_breakout_min_delta_persist)
                or micro_oi_5m >= float(self.cfg.slow_breakout_min_oi5m_pct) * 1.6
            )
            if cond_ret and cond_flow_or_oi and cond_quality:
                return True, "slow_breakout", {
                    **base_details,
                    "ret_5m_pct": ret_5m_pct,
                    "delta_persist_score": delta_persist,
                    "slow_breakout": 1.0,
                }

        # ignition (3-of-4): return, volume, cvd, oi
        cond_ret = pump_pct >= self.cfg.ignition_pump_1m or pump_20s >= self.cfg.ignition_pump_20s
        cond_vol = vol_ratio_1m >= self.cfg.ignition_vol_ratio_1m
        micro_cvd_z = float((micro or {}).get("cvd_z_decayed", (micro or {}).get("cvd_z", 0.0)))
        micro_cvd_z_simple = float((micro or {}).get("cvd_z_simple", 0.0))
        cond_cvd = bool(micro) and micro_cvd_z >= self.cfg.ignition_cvd_z
        cond_oi = bool(micro) and micro_oi_5m >= self.cfg.ignition_oi5m_pct
        n_hit = int(cond_ret) + int(cond_vol) + int(cond_cvd) + int(cond_oi)
        if n_hit >= self.cfg.ignition_min_signals:
            if bool(self.cfg.ignition_3of4_blocked):
                return False, "", {}
            if (
                self.cfg.ignition_bear_veto_enabled
                and micro_oi_5m >= self.cfg.ignition_bear_veto_oi5m
                and micro_cvd_z <= self.cfg.ignition_bear_veto_cvd_z
            ):
                return False, "", {}
            passed, d = self._passes_denoise_filters(
                symbol,
                micro,
                n_hit=n_hit,
                ret_1m_pct=pump_pct,
                oi5m_pct=micro_oi_5m,
            )
            if passed:
                details = {
                    **base_details,
                    "n_hit": float(n_hit),
                    "vol_ratio_1m": vol_ratio_1m,
                    "vol_z_1m": vol_z_1m,
                    "micro_cvd_z": micro_cvd_z,
                    "micro_cvd_z_simple": micro_cvd_z_simple,
                    "micro_oi5m": micro_oi_5m,
                    **d,
                }
                return True, "ignition_3of4", details

        # 早筛：在明显拉升前捕捉启动迹象，减少8-10分钟级滞后
        if (
            pump_pct >= self.cfg.early_pump_pct_1m
            and vol_1m >= self.cfg.early_volume_1m
            and pump_20s >= self.cfg.early_pump_pct_20s
            and vol_20s >= self.cfg.early_volume_20s
        ):
            passed, d = self._passes_denoise_filters(
                symbol,
                micro,
                n_hit=n_hit,
                ret_1m_pct=pump_pct,
                oi5m_pct=micro_oi_5m,
            )
            if passed:
                return True, "early_momo", {
                    **base_details,
                    "vol_ratio_1m": vol_ratio_1m,
                    "vol_z_1m": vol_z_1m,
                    **d,
                }
        return False, "", {}

    async def _enqueue_symbol(
        self,
        symbol: str,
        pump_pct: float,
        vol_1m: float,
        reason: str,
        details: Optional[Dict[str, float]] = None,
        source: str = "binance",
    ) -> None:
        now = time.time()
        last_ts = self.last_enqueued_at.get(symbol, 0.0)
        is_high = bool(self.cfg.onchain_priority_enabled and self._is_onchain_target(symbol))
        q = self.analysis_queue_high if is_high else self.analysis_queue_normal
        if symbol in self.in_queue_or_running:
            return
        if now - last_ts < self.cfg.queue_symbol_cooldown_sec:
            return
        if q.full() and is_high and not self.analysis_queue_normal.full():
            is_high = False
            q = self.analysis_queue_normal
        if q.full():
            return
        self.last_enqueued_at[symbol] = now
        self.in_queue_or_running.add(symbol)
        self.trigger_meta[symbol] = {
            "ts": now,
            "reason": reason,
            "source": str(source or "binance").strip().lower() or "binance",
            "priority": "high" if is_high else "normal",
            "details": details or {},
        }
        await q.put(symbol)
        self.queue_activity_event.set()
        self.last_trigger_ts = now
        self.trigger_count_since_analysis += 1
        vol_ratio = float((details or {}).get("vol_ratio_1m", 0.0))
        n_hit = int((details or {}).get("n_hit", 0.0))
        print(
            f"[trigger:{str(source or 'binance').strip().lower() or 'binance'}] "
            f"{symbol} pri={'H' if is_high else 'N'} reason={reason} pump_1m={pump_pct:.2f}% "
            f"vol_1m={vol_1m:,.0f} vol_ratio={vol_ratio:.2f} hit={n_hit}"
        )

    async def _process_tickers(self, tickers: List[Dict[str, Any]], ts: float, source: str) -> None:
        source_l = str(source or "binance").strip().lower() or "binance"
        self._reload_onchain_targets_if_due()
        self._reload_onchain_sectors_if_due()
        self._prune_onchain_auto_expire(ts)
        if source_l == "aster":
            self._reload_aster_contract_symbols_if_due()
        for t in tickers:
            symbol = str(t.get("s", "")).strip().upper()
            if not symbol.endswith("USDT"):
                continue
            if source_l == "aster" and self.aster_contract_symbols and symbol not in self.aster_contract_symbols:
                continue
            try:
                price = float(t.get("c", 0.0) or 0.0)
                qvol = float(t.get("q", 0.0) or 0.0)
            except Exception:
                continue
            if price <= 0:
                continue
            self.latest_price[symbol] = price
            self.latest_qvol24h[symbol] = qvol
            self._update_live_pnl_on_tick(symbol, ts, price)

            pump_pct = self._realtime_pump_pct(symbol, ts, price)
            vol_1m = self._volume_1m(symbol, ts, qvol)
            self._record_ingress_tick(source_l, symbol, ts, vol_1m)
            self._update_vol1m_history(symbol, vol_1m)
            self._update_pump1m_history(symbol, pump_pct)
            pump_20s = self._price_change_pct_window(symbol, ts, price, 20.0)
            vol_20s = self._volume_window(symbol, ts, qvol, 20.0)
            vol_ratio_1m = self._volume_ratio_1m(symbol, vol_1m)
            vol_z_1m = self._volume_zscore_1m(symbol, vol_1m)
            if source_l == "binance":
                self._maybe_auto_add_onchain_symbol(symbol, vol_ratio_1m, vol_1m, ts)

            micro: Optional[Dict[str, float]] = None
            should_probe_micro = (
                (pump_pct >= self.cfg.ignition_pump_1m * 0.8 and vol_ratio_1m >= self.cfg.ignition_vol_ratio_1m * 0.7)
                or (pump_20s >= self.cfg.ignition_pump_20s and vol_20s >= self.cfg.early_volume_20s)
                or (symbol in self.liquidation_spike_state)
            )
            if should_probe_micro:
                try:
                    micro = await self._get_micro_confirm_cached(symbol, ts, source=source_l)
                except Exception:
                    micro = None

            ok, reason, details = self._should_trigger(
                symbol,
                pump_pct,
                vol_1m,
                pump_20s,
                vol_20s,
                vol_ratio_1m,
                vol_z_1m,
                micro,
                source=source_l,
            )
            if ok:
                if micro is None:
                    try:
                        micro = await self._get_micro_confirm_cached(symbol, ts, source=source_l)
                    except Exception:
                        micro = None
                if isinstance(details, dict) and isinstance(micro, dict):
                    for k, v in micro.items():
                        if isinstance(v, (int, float)) and k not in details:
                            details[k] = float(v)
            self._maybe_record_force_chain_audit(
                symbol=symbol,
                ts=ts,
                source=source_l,
                pump_pct=pump_pct,
                pump_20s=pump_20s,
                vol_1m=vol_1m,
                vol_ratio_1m=vol_ratio_1m,
                vol_z_1m=vol_z_1m,
                should_trigger=ok,
                trigger_reason=reason,
                details=details,
                micro=micro,
            )
            if ok:
                if source_l == "binance":
                    self._expand_onchain_targets_by_sector(symbol, ts, reason)
                await self._enqueue_symbol(symbol, pump_pct, vol_1m, reason, details, source=source_l)

    async def _stream_consumer_loop(self, ws_url: str, source: str) -> None:
        if websockets is None:
            raise RuntimeError(f"websockets import failed: {_WS_IMPORT_ERROR!r}")
        source_l = str(source or "binance").strip().lower() or "binance"
        tag = "ws" if source_l == "binance" else f"ws-{source_l}"
        delay = self.cfg.reconnect_base_sec
        while True:
            try:
                print(f"[{tag}] connecting {ws_url}")
                async with websockets.connect(
                    ws_url,
                    ping_interval=self.cfg.ping_interval,
                    ping_timeout=self.cfg.ping_timeout,
                    max_queue=4096,
                ) as ws:
                    print(f"[{tag}] connected")
                    delay = self.cfg.reconnect_base_sec
                    while True:
                        raw = await ws.recv()
                        tickers = json.loads(raw)
                        if not isinstance(tickers, list):
                            continue
                        ts = time.time()
                        await self._process_tickers(tickers, ts, source_l)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[{tag}] disconnected: {exc!r}, reconnect in {delay:.1f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2.0, self.cfg.reconnect_max_sec)

    async def stream_consumer(self) -> None:
        await self._stream_consumer_loop(self.cfg.ws_url, "binance")

    async def aster_stream_consumer(self) -> None:
        if not bool(self.cfg.aster_ws_enabled):
            return
        self._reload_aster_contract_symbols_if_due(force=True)
        await self._stream_consumer_loop(self.cfg.aster_ws_url, "aster")

    async def _dequeue_symbol(self) -> Tuple[str, asyncio.Queue[str], bool]:
        while True:
            if self.cfg.onchain_priority_enabled:
                try:
                    return self.analysis_queue_high.get_nowait(), self.analysis_queue_high, True
                except asyncio.QueueEmpty:
                    pass
                try:
                    return self.analysis_queue_normal.get_nowait(), self.analysis_queue_normal, False
                except asyncio.QueueEmpty:
                    pass
                self.queue_activity_event.clear()
                if (not self.analysis_queue_high.empty()) or (not self.analysis_queue_normal.empty()):
                    self.queue_activity_event.set()
                    continue
                await self.queue_activity_event.wait()
                continue
            try:
                return self.analysis_queue_normal.get_nowait(), self.analysis_queue_normal, False
            except asyncio.QueueEmpty:
                pass
            self.queue_activity_event.clear()
            if not self.analysis_queue_normal.empty():
                self.queue_activity_event.set()
                continue
            await self.queue_activity_event.wait()

    async def analysis_worker(self, worker_id: int) -> None:
        while True:
            symbol, q_used, high_priority = await self._dequeue_symbol()
            try:
                print(f"[worker-{worker_id}] analyze {symbol} pri={'H' if high_priority else 'N'}")
                self.last_analysis_ts = time.time()
                self.trigger_count_since_analysis = 0
                result = await self.analyzer(self.exchange, symbol)
                self._handle_alert_result(symbol, result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[worker-{worker_id}] error {symbol}: {exc!r}")
            finally:
                self.in_queue_or_running.discard(symbol)
                if q_used is not None:
                    q_used.task_done()
            cooldown = self.cfg.onchain_high_worker_cooldown_sec if high_priority else self.cfg.worker_cooldown_sec
            await asyncio.sleep(max(0.0, cooldown))

    async def worker_healthcheck_loop(self) -> None:
        if not self.cfg.worker_healthcheck_enabled:
            return
        interval = max(2.0, float(self.cfg.worker_healthcheck_interval_sec))
        stall_sec = max(10.0, float(self.cfg.worker_healthcheck_stall_sec))
        min_triggers = max(1, int(self.cfg.worker_healthcheck_min_triggers))
        while True:
            try:
                await asyncio.sleep(interval)
                now = time.time()
                total_q = self.analysis_queue_high.qsize() + self.analysis_queue_normal.qsize()
                has_recent_trigger = self.last_trigger_ts > 0 and (now - self.last_trigger_ts) <= stall_sec
                no_recent_analysis = self.last_analysis_ts <= 0 or (now - self.last_analysis_ts) >= stall_sec
                stalled = (
                    has_recent_trigger
                    and no_recent_analysis
                    and self.trigger_count_since_analysis >= min_triggers
                    and total_q > 0
                )
                if stalled:
                    should_alert = (not self.worker_stall_active) or (
                        now - self.last_worker_stall_alert_ts >= stall_sec
                    )
                    if should_alert:
                        msg = (
                            "⚠️ Worker疑似阻塞\n"
                            f"pending={total_q} (H={self.analysis_queue_high.qsize()}, N={self.analysis_queue_normal.qsize()})\n"
                            f"last_trigger_ago={now - self.last_trigger_ts:.1f}s\n"
                            f"last_analysis_ago={(now - self.last_analysis_ts) if self.last_analysis_ts > 0 else -1.0:.1f}s\n"
                            f"trigger_since_analysis={self.trigger_count_since_analysis}"
                        )
                        print(
                            f"[health] worker_stall pending={total_q} "
                            f"trigger_since_analysis={self.trigger_count_since_analysis} "
                            f"last_analysis_ago={(now - self.last_analysis_ts) if self.last_analysis_ts > 0 else -1.0:.1f}s"
                        )
                        if self.cfg.discord_webhook_url:
                            await self._send_discord_webhook(msg)
                        self.last_worker_stall_alert_ts = now
                    self.worker_stall_active = True
                else:
                    if self.worker_stall_active:
                        print(
                            f"[health] worker_recovered pending={total_q} "
                            f"trigger_since_analysis={self.trigger_count_since_analysis}"
                        )
                    self.worker_stall_active = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[health] worker_healthcheck error: {exc!r}")

    async def discord_heartbeat_loop(self) -> None:
        if not self.cfg.discord_webhook_url:
            return
        interval = max(60.0, float(self.cfg.discord_heartbeat_interval_sec))
        content = str(self.cfg.discord_heartbeat_content or "").strip() or "初号机运行中"
        while True:
            try:
                await self._send_discord_webhook(content)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[discord-heartbeat] send failed: {exc!r}")
            await asyncio.sleep(interval)

    def _query_reason_symbol_counts(
        self,
        *,
        reason: str,
        window_sec: float,
        top_n: int,
        source: str = "binance",
    ) -> Tuple[List[Tuple[str, int]], int]:
        conn = self._select_signal_event_conn(source)
        if (not self.signal_db_enabled_rt) or conn is None:
            return [], 0
        min_ts = float(time.time() - max(3600.0, float(window_sec)))
        limit_n = int(top_n)
        try:
            with self.signal_db_lock:
                if limit_n > 0:
                    rows = conn.execute(
                        """
                        SELECT symbol, COUNT(*) AS c
                        FROM signal_events
                        WHERE reason = ? AND event_ts >= ?
                        GROUP BY symbol
                        ORDER BY c DESC, symbol ASC
                        LIMIT ?
                        """,
                        (str(reason or ""), float(min_ts), int(limit_n)),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """
                        SELECT symbol, COUNT(*) AS c
                        FROM signal_events
                        WHERE reason = ? AND event_ts >= ?
                        GROUP BY symbol
                        ORDER BY c DESC, symbol ASC
                        """,
                        (str(reason or ""), float(min_ts)),
                    ).fetchall()
                total_row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM signal_events
                    WHERE reason = ? AND event_ts >= ?
                    """,
                    (str(reason or ""), float(min_ts)),
                ).fetchone()
        except Exception as exc:
            print(f"[module-stats] query failed source={source} reason={reason}: {exc!r}")
            return [], 0
        out: List[Tuple[str, int]] = []
        for row in rows:
            symbol = str(row[0] or "").strip().upper()
            if not symbol:
                continue
            try:
                cnt = int(row[1] or 0)
            except Exception:
                cnt = 0
            if cnt > 0:
                out.append((symbol, cnt))
        total = 0
        if isinstance(total_row, tuple) and total_row:
            try:
                total = int(total_row[0] or 0)
            except Exception:
                total = 0
        return out, total

    def _format_reason_symbol_counts(
        self,
        *,
        prefix: str,
        rows: List[Tuple[str, int]],
        total: int,
        top_n: int,
    ) -> str:
        if not rows:
            return f"{prefix}：无（0次）"
        selected_rows = rows if int(top_n) <= 0 else rows[: max(1, int(top_n))]
        parts: List[str] = []
        for symbol, cnt in selected_rows:
            parts.append(f"{self._display_symbol(symbol)}（{int(cnt)}次）")
        return f"{prefix}：" + " ".join(parts)

    def _format_reason_symbol_count_lines(
        self,
        *,
        prefix: str,
        rows: List[Tuple[str, int]],
        total: int,
        top_n: int,
        max_line_len: int = 1700,
    ) -> List[str]:
        if not rows:
            return [f"{prefix}：无（0次）"]
        selected_rows = rows if int(top_n) <= 0 else rows[: max(1, int(top_n))]
        tokens = [f"{self._display_symbol(symbol)}（{int(cnt)}次）" for symbol, cnt in selected_rows]
        first_prefix = f"{prefix}："
        cont_prefix = f"{prefix}(续)："
        lines: List[str] = []
        current = first_prefix
        for tok in tokens:
            piece = tok if current.endswith("：") else f" {tok}"
            if len(current) + len(piece) <= max(64, int(max_line_len)):
                current += piece
            else:
                lines.append(current)
                current = f"{cont_prefix}{tok}"
        lines.append(current)
        return lines

    def _build_module_stats_messages(
        self,
        *,
        ts: float,
        window_sec: float,
        top_n: int,
        probe_rows: List[Tuple[str, int]],
        probe_total: int,
        slow_rows: List[Tuple[str, int]],
        slow_total: int,
        source: str = "binance",
    ) -> List[str]:
        src = str(source or "binance").strip().lower() or "binance"
        src_label = "Aster" if src == "aster" else "Binance"
        if abs(float(window_sec) - 86400.0) < 1.0:
            prefix = "24小时内"
        else:
            prefix = f"{int(max(1, round(float(window_sec) / 3600.0)))}小时内"
        probe_lines = self._format_reason_symbol_count_lines(
            prefix=f"{prefix}主力试盘触发",
            rows=probe_rows,
            total=int(probe_total),
            top_n=int(top_n),
        )
        slow_lines = self._format_reason_symbol_count_lines(
            prefix=f"{prefix}Slow Accumulation触发",
            rows=slow_rows,
            total=int(slow_total),
            top_n=int(top_n),
        )
        lines = [
            f"📊 盘口模块统计({src_label}) | {self._fmt_bjt(float(ts))}",
            *probe_lines,
            *slow_lines,
        ]
        max_msg_len = 1900
        out: List[str] = []
        current = ""
        for line in lines:
            candidate = line if not current else f"{current}\n{line}"
            if len(candidate) <= max_msg_len:
                current = candidate
                continue
            if current:
                out.append(current)
            current = line
        if current:
            out.append(current)
        return out

    async def discord_module_stats_loop(self) -> None:
        if not bool(self.cfg.discord_module_stats_enabled):
            return
        interval = max(300.0, float(self.cfg.discord_module_stats_interval_sec))
        window_sec = max(3600.0, float(self.cfg.discord_module_stats_window_sec))
        top_n = int(self.cfg.discord_module_stats_top_n)
        targets: List[str] = []
        if bool(str(self.cfg.discord_webhook_url or "").strip()):
            targets.append("binance")
        if bool(self.cfg.aster_ws_enabled) and bool(str(self.cfg.aster_discord_webhook_url or "").strip()):
            targets.append("aster")
        if not targets:
            return
        while True:
            try:
                now_ts = time.time()
                for source in targets:
                    probe_rows, probe_total = self._query_reason_symbol_counts(
                        reason="fake_breakout_probe",
                        window_sec=window_sec,
                        top_n=top_n,
                        source=source,
                    )
                    slow_rows, slow_total = self._query_reason_symbol_counts(
                        reason="slow_accumulation",
                        window_sec=window_sec,
                        top_n=top_n,
                        source=source,
                    )
                    msgs = self._build_module_stats_messages(
                        ts=now_ts,
                        window_sec=window_sec,
                        top_n=top_n,
                        probe_rows=probe_rows,
                        probe_total=probe_total,
                        slow_rows=slow_rows,
                        slow_total=slow_total,
                        source=source,
                    )
                    for msg in msgs:
                        await self._send_discord_webhook(msg, source=source)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[module-stats] send failed: {exc!r}")
            await asyncio.sleep(interval)

    async def run_forever(self) -> None:
        tasks = [asyncio.create_task(self.stream_consumer(), name="ws_consumer")]
        if bool(self.cfg.aster_ws_enabled):
            tasks.append(asyncio.create_task(self.aster_stream_consumer(), name="ws_consumer_aster"))
        for i in range(self.cfg.workers):
            tasks.append(asyncio.create_task(self.analysis_worker(i + 1), name=f"analysis_worker_{i+1}"))
        if self.cfg.decision_tier_enabled:
            tasks.append(asyncio.create_task(self.tier2_confirm_loop(), name="tier2_confirm"))
        if self.cfg.auto_tune_enabled or self.cfg.symbol_quality_enabled or self.cfg.regime_pattern_enabled:
            tasks.append(asyncio.create_task(self.adaptive_maintenance_loop(), name="adaptive_maintenance"))
        if self.cfg.worker_healthcheck_enabled:
            tasks.append(asyncio.create_task(self.worker_healthcheck_loop(), name="worker_healthcheck"))
        if self.cfg.discord_heartbeat_enabled and self.cfg.discord_webhook_url:
            tasks.append(asyncio.create_task(self.discord_heartbeat_loop(), name="discord_heartbeat"))
        if self.cfg.discord_module_stats_enabled and (
            bool(str(self.cfg.discord_webhook_url or "").strip())
            or (bool(self.cfg.aster_ws_enabled) and bool(str(self.cfg.aster_discord_webhook_url or "").strip()))
        ):
            tasks.append(asyncio.create_task(self.discord_module_stats_loop(), name="discord_module_stats"))
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self.discord_summary_flush_task is not None:
                self.discord_summary_flush_task.cancel()
                await asyncio.gather(self.discord_summary_flush_task, return_exceptions=True)

    def _alert_cooldown_for_level(self, level: str) -> float:
        if level in {"WATCH", "normal"}:
            return max(1.0, float(self.cfg.alert_cooldown_low_sec))
        return max(1.0, float(self.cfg.alert_cooldown_sec))

    def _should_emit_alert(self, symbol: str, level: str, alert: bool, score: float) -> bool:
        if not alert:
            return False
        rank = ALERT_LEVEL_RANK.get(level, 0)
        if rank <= 0:
            return False

        now = time.time()
        prev = self.alert_state.get(symbol)
        if prev is None:
            self.alert_state[symbol] = (now, rank, float(score))
            return True

        prev_ts, prev_rank, prev_score = prev
        cooldown_sec = self._alert_cooldown_for_level(level)
        if now - prev_ts >= cooldown_sec:
            self.alert_state[symbol] = (now, rank, float(score))
            return True

        # Cooldown窗口内允许等级升级，或分数显著抬升后再次推送。
        if rank > prev_rank:
            self.alert_state[symbol] = (now, rank, float(score))
            return True
        if float(score) >= float(prev_score) + float(self.cfg.alert_score_breakthrough):
            self.alert_state[symbol] = (now, rank, float(score))
            return True
        return False

    def _record_alert_tracking(self, symbol: str, price: float) -> Dict[str, Any]:
        now = time.time()

        freq = self.alert_1h_timestamps[symbol]
        freq.append(now)
        cutoff = now - ALERT_FREQ_WINDOW_SEC
        while freq and freq[0] < cutoff:
            freq.popleft()
        count_1h = len(freq)

        prev = self.alert_wave_state.get(symbol)
        if prev is None or now - float(prev.get("last_ts", 0.0)) > ALERT_WAVE_RESET_SEC:
            state: Dict[str, Any] = {
                "first_ts": now,
                "first_price": price if price > 0 else 0.0,
                "count": 1,
                "last_ts": now,
            }
        else:
            state = prev
            state["count"] = int(state.get("count", 1)) + 1
            state["last_ts"] = now
            if float(state.get("first_price", 0.0)) <= 0 and price > 0:
                state["first_price"] = price

        self.alert_wave_state[symbol] = state
        return {
            "first_ts": float(state.get("first_ts", now)),
            "first_price": float(state.get("first_price", 0.0)),
            "count": int(state.get("count", 1)),
            "count_1h": count_1h,
        }

    @staticmethod
    def _display_symbol(symbol: str) -> str:
        s = str(symbol or "").upper().strip()
        for quote in ("USDT", "USDC", "FDUSD", "BUSD"):
            if s.endswith(quote) and len(s) > len(quote):
                return f"{s[:-len(quote)]}/{quote}"
        return s

    @staticmethod
    def _fmt_score(v: Any) -> str:
        try:
            x = float(v)
        except Exception:
            return "0"
        if abs(x - round(x)) < 1e-9:
            return str(int(round(x)))
        return f"{x:.1f}"

    @staticmethod
    def _fmt_price(v: Any) -> str:
        try:
            x = float(v)
        except Exception:
            return "-"
        if x <= 0:
            return "-"
        if x >= 1000:
            return f"{x:.2f}"
        if x >= 1:
            return f"{x:.4f}".rstrip("0").rstrip(".")
        if x >= 0.01:
            return f"{x:.6f}".rstrip("0").rstrip(".")
        return f"{x:.8f}".rstrip("0").rstrip(".")

    def _resolve_alert_title(
        self,
        level: str,
        score: float,
        threshold: float,
        count_1h: int,
        signals: Dict[str, Any],
    ) -> Tuple[str, bool]:
        if level == "SNIPER" or score >= 150.0 or bool(signals.get("sniper_a")) or bool(signals.get("sniper_b")):
            return "🎯 SNIPER 狙击", False
        if bool(signals.get("trigger_1m")):
            return "⚡ 1M_TRIGGER", False
        if level == "WATCH":
            return "👀 WATCH 预警", False
        hot = level == "HOT" or (count_1h >= 3 and score >= threshold)
        if hot:
            return "🚨 HOT 高频", True
        return "🚨 高分报警", False

    @staticmethod
    def _build_tags(signals: Dict[str, Any]) -> str:
        tags = []
        base_tags = 0
        if bool(signals.get("squeeze")):
            tags.append("✅")
            base_tags += 1
        if bool(signals.get("momentum")):
            tags.append("✅")
            base_tags += 1
        if bool(signals.get("acceleration")):
            tags.append("✅")
            base_tags += 1
        if (
            base_tags == 0
            and (
                bool(signals.get("essential_ok"))
                or bool(signals.get("volume_flow"))
                or bool(signals.get("order_flow_buy"))
                or bool(signals.get("order_flow_sell"))
            )
        ):
            tags.append("✅")
        if bool(signals.get("vol_surge")):
            tags.append("🔥")
        if bool(signals.get("breakout")):
            tags.append("🚀")
        if bool(signals.get("order_flow_buy")):
            tags.append("⬆️")
        if bool(signals.get("order_flow_sell")):
            tags.append("📉")
        if bool(signals.get("lurking")):
            tags.append("🕵️")
        if bool(signals.get("smart_money")):
            tags.append("🐳")
        if bool(signals.get("black_horse")):
            tags.append("🦄")
        if bool(signals.get("vp_vacuum")):
            tags.append("🌌")
        if bool(signals.get("volume_flow")):
            tags.append("💰")
        if not tags:
            tags.append("✅")
        return " ".join(tags[:14])

    def _discord_delivery_mode(self, score: Any, level: str, hit_count: int) -> str:
        if bool(self.cfg.disable_three_layer_filter):
            return "realtime"
        try:
            score_v = float(score or 0.0)
        except Exception:
            score_v = 0.0
        if score_v >= float(self.cfg.discord_realtime_score):
            return "realtime"
        if score_v >= float(self.cfg.discord_min_score):
            if level in {"WATCH", "normal"} and int(hit_count) >= int(self.cfg.discord_watch_min_hit):
                return "realtime"
            return "drop"
        if score_v >= float(self.cfg.discord_summary_score):
            return "summary"
        return "drop"

    async def _flush_discord_summary_after_window(self) -> None:
        await asyncio.sleep(max(1.0, float(self.cfg.discord_summary_interval_sec)))
        await self._flush_discord_summary_now()

    async def _flush_discord_summary_now(self) -> None:
        if not self.discord_summary_buffer:
            return
        if not self.cfg.discord_webhook_url:
            self.discord_summary_buffer.clear()
            return

        bjt = timezone(timedelta(hours=8))
        items = sorted(
            self.discord_summary_buffer.values(),
            key=lambda x: (float(x.get("score", 0.0)), float(x.get("ts", 0.0))),
            reverse=True,
        )
        self.discord_summary_buffer.clear()

        max_rows = 20
        lines = ["**🧾 5m低分信号汇总 (45-50)**"]
        for item in items[:max_rows]:
            ts = float(item.get("ts", 0.0))
            time_txt = datetime.fromtimestamp(ts, tz=bjt).strftime("%H:%M") if ts > 0 else "--:--"
            symbol = str(item.get("symbol", ""))
            score = float(item.get("score", 0.0))
            level = str(item.get("level", "none"))
            lines.append(
                f"`{time_txt}` **{self._display_symbol(symbol)}** | Score: **{self._fmt_score(score)}** | {level}"
            )
        if len(items) > max_rows:
            lines.append(f"... 其余 {len(items) - max_rows} 条已省略")
        await self._send_discord_webhook("\n".join(lines)[:1900])

    def _enqueue_discord_summary(self, symbol: str, level: str, score: float) -> None:
        now = time.time()
        prev = self.discord_summary_buffer.get(symbol)
        if prev is None or float(score) >= float(prev.get("score", 0.0)):
            self.discord_summary_buffer[symbol] = {
                "symbol": symbol,
                "score": float(score),
                "level": level,
                "ts": now,
            }
        else:
            prev["ts"] = now
            prev["level"] = level

        if self.discord_summary_flush_task is None or self.discord_summary_flush_task.done():
            self.discord_summary_flush_task = asyncio.create_task(self._flush_discord_summary_after_window())

    def _handle_alert_result(self, symbol: str, result: Any) -> None:
        if not isinstance(result, dict):
            return
        meta = self.trigger_meta.get(symbol, {})
        level = str(result.get("level", "none"))
        alert = bool(result.get("alert", False))
        score = result.get("score")
        threshold = result.get("threshold")
        side = str(result.get("side", "neutral"))

        try:
            score_v = float(score or 0.0)
        except Exception:
            score_v = 0.0
        try:
            threshold_v_common = float(threshold or 0.0)
        except Exception:
            threshold_v_common = 0.0
        details = meta.get("details", {}) if isinstance(meta.get("details", {}), dict) else {}
        hit_count = int(float(details.get("n_hit", 0.0) or 0.0))
        reason = str(meta.get("reason", "analysis"))
        source = str(meta.get("source", "binance") or "binance").strip().lower() or "binance"
        if bool(self.cfg.ignition_3of4_blocked) and reason == "ignition_3of4":
            if self.cfg.decision_tier_enabled:
                now = time.time()
                self._append_tier_decision_row(
                    {
                        "ts": now,
                        "time_bjt": self._bjt_time_text(now),
                        "symbol": symbol,
                        "reason": reason,
                        "side": side,
                        "level": level,
                        "action": "blocked_ignition_3of4",
                        "tier": "TIER4",
                        "delivery": "record_only",
                        "final_score": 0.0,
                    }
                )
            self._persist_signal_event(
                ts=time.time(),
                symbol=symbol,
                reason=reason,
                side=side,
                level=level,
                alert=alert,
                base_score=score_v,
                threshold=threshold_v_common,
                tier="TIER4",
                delivery="record_only",
                final_score=0.0,
                hit_count=hit_count,
                payload={"action": "blocked_ignition_3of4", "trigger_source": source},
            )
            return

        # Tier decision pipeline (target architecture)
        if self.cfg.decision_tier_enabled:
            decision = self._compute_tier_decision(symbol, result, meta)
            result = dict(result)
            result["decision"] = decision
            level = str(decision.get("level_override", level) or level)
            reason = str(decision.get("reason_override", reason) or reason)
            if isinstance(meta, dict):
                meta = dict(meta)
                meta["reason"] = reason
            result["level"] = level
            if bool(self.cfg.disable_three_layer_filter):
                decision["delivery"] = "realtime"

            drow = {
                "ts": time.time(),
                "time_bjt": self._bjt_time_text(time.time()),
                "symbol": symbol,
                "reason": reason,
                "side": side,
                "level": level,
                "base_score": score_v,
                "tier": str(decision.get("tier", "TIER4")),
                "delivery": str(decision.get("delivery", "record_only")),
                "final_score": float(decision.get("final_score", 0.0) or 0.0),
                "market_factor": float(decision.get("market_factor", 1.0) or 1.0),
                "symbol_factor": float(decision.get("symbol_factor", 1.0) or 1.0),
                "symbol_factor_struct": float(decision.get("symbol_factor_struct", 1.0) or 1.0),
                "symbol_quality_factor": float(decision.get("symbol_quality_factor", 1.0) or 1.0),
                "mtf_factor": float(decision.get("mtf_factor", 1.0) or 1.0),
                "flow_quality_factor": float(decision.get("flow_quality_factor", 1.0) or 1.0),
                "regime_factor": float(decision.get("regime_factor", 1.0) or 1.0),
                "sector_cluster_factor": float(decision.get("sector_cluster_factor", 1.0) or 1.0),
                "sentiment_factor": float(decision.get("sentiment_factor", 1.0) or 1.0),
                "risk_penalty": float(decision.get("risk_penalty", 0.0) or 0.0),
                "ml_win_prob": float(decision.get("ml_win_prob", 0.5) or 0.5),
                "whale_testing_active": float(decision.get("whale_testing_active", 0.0) or 0.0),
                "whale_testing_hits": float(decision.get("whale_testing_hits", 0.0) or 0.0),
                "whale_testing_drawdown_pct": float(decision.get("whale_testing_drawdown_pct", 0.0) or 0.0),
                "whale_bonus_applied": float(decision.get("whale_bonus_applied", 0.0) or 0.0),
                "whale_waive_penalty_applied": float(decision.get("whale_waive_penalty_applied", 0.0) or 0.0),
                "probe_alpha_active": float(decision.get("probe_alpha_active", 0.0) or 0.0),
                "probe_alpha_hits": float(decision.get("probe_alpha_hits", 0.0) or 0.0),
                "probe_alpha_ret_1m_pct": float(decision.get("probe_alpha_ret_1m_pct", 0.0) or 0.0),
                "probe_alpha_centroid_change_pct": float(
                    decision.get("probe_alpha_centroid_change_pct", 0.0) or 0.0
                ),
                "probe_follow_bonus_applied": float(decision.get("probe_follow_bonus_applied", 0.0) or 0.0),
                "probe_follow_waive_penalty_applied": float(
                    decision.get("probe_follow_waive_penalty_applied", 0.0) or 0.0
                ),
                "bayesian_prior": float(decision.get("bayesian_prior", 0.0) or 0.0),
                "bayesian_posterior": float(decision.get("bayesian_posterior", 0.0) or 0.0),
                "bayesian_override_active": float(decision.get("bayesian_override_active", 0.0) or 0.0),
                "bayesian_multiplier_applied": float(decision.get("bayesian_multiplier_applied", 1.0) or 1.0),
                "bayesian_waived_penalty": float(decision.get("bayesian_waived_penalty", 0.0) or 0.0),
                "sniper_risk_action": str(decision.get("sniper_risk_action", "none") or "none"),
                "ignition_guard_action": str(decision.get("ignition_guard_action", "none") or "none"),
                "risk_flags": decision.get("risk_flags", []),
                "whale_waived_flags": decision.get("whale_waived_flags", []),
                "bayesian_waived_flags": decision.get("bayesian_waived_flags", []),
                "bayesian_reason_override": str(decision.get("reason_override", "") or ""),
                "bayesian_level_override": str(decision.get("level_override", "") or ""),
                "tier_thresholds": decision.get("tier_thresholds", {}),
            }
            self._append_tier_decision_row(drow)
            self._persist_signal_event(
                ts=float(drow.get("ts", time.time()) or time.time()),
                symbol=symbol,
                reason=reason,
                side=side,
                level=level,
                alert=bool(alert),
                base_score=score_v,
                threshold=threshold_v_common,
                tier=str(drow.get("tier", "TIER4") or "TIER4"),
                delivery=str(drow.get("delivery", "record_only") or "record_only"),
                final_score=float(drow.get("final_score", score_v) or score_v),
                hit_count=hit_count,
                payload={
                    "trigger_ts": float(meta.get("ts", 0.0) or 0.0),
                    "trigger_source": source,
                    "details": details,
                    "risk_flags": decision.get("risk_flags", []),
                    "whale_testing": {
                        "active": float(decision.get("whale_testing_active", 0.0) or 0.0),
                        "hits": float(decision.get("whale_testing_hits", 0.0) or 0.0),
                        "drawdown_pct": float(decision.get("whale_testing_drawdown_pct", 0.0) or 0.0),
                        "bonus_applied": float(decision.get("whale_bonus_applied", 0.0) or 0.0),
                        "waive_penalty_applied": float(decision.get("whale_waive_penalty_applied", 0.0) or 0.0),
                        "waived_flags": decision.get("whale_waived_flags", []),
                    },
                    "probe_alpha": {
                        "active": float(decision.get("probe_alpha_active", 0.0) or 0.0),
                        "hits": float(decision.get("probe_alpha_hits", 0.0) or 0.0),
                        "ret_1m_pct": float(decision.get("probe_alpha_ret_1m_pct", 0.0) or 0.0),
                        "centroid_change_pct": float(
                            decision.get("probe_alpha_centroid_change_pct", 0.0) or 0.0
                        ),
                        "waive_used": float(decision.get("probe_alpha_waive_used", 0.0) or 0.0),
                        "bonus_applied": float(decision.get("probe_follow_bonus_applied", 0.0) or 0.0),
                        "waive_penalty_applied": float(
                            decision.get("probe_follow_waive_penalty_applied", 0.0) or 0.0
                        ),
                        "waived_flags": decision.get("probe_follow_waived_flags", []),
                    },
                    "bayesian": {
                        "prior": float(decision.get("bayesian_prior", 0.0) or 0.0),
                        "posterior": float(decision.get("bayesian_posterior", 0.0) or 0.0),
                        "override_active": float(decision.get("bayesian_override_active", 0.0) or 0.0),
                        "multiplier_applied": float(decision.get("bayesian_multiplier_applied", 1.0) or 1.0),
                        "waived_penalty": float(decision.get("bayesian_waived_penalty", 0.0) or 0.0),
                        "waived_flags": decision.get("bayesian_waived_flags", []),
                        "meta": decision.get("bayesian_meta", {}),
                        "reason_override": str(decision.get("reason_override", "") or ""),
                        "level_override": str(decision.get("level_override", "") or ""),
                    },
                },
            )

            delivery = str(decision.get("delivery", "record_only"))
            if delivery == "record_only":
                return
            if delivery == "summary":
                sum_level = level if ALERT_LEVEL_RANK.get(level, 0) > 0 else "WATCH"
                self._enqueue_discord_summary(symbol, sum_level, float(decision.get("final_score", score_v)))
                return
            if delivery == "confirm":
                self._queue_tier2_confirmation(symbol, result, meta, decision)
                return

            push_level = level if ALERT_LEVEL_RANK.get(level, 0) > 0 else "WATCH"
            push_score = float(decision.get("final_score", score_v) or score_v)
            if self._should_emit_alert(symbol, push_level, True, push_score):
                last_price = float(result.get("last_price", 0.0) or 0.0)
                try:
                    threshold_v = float(threshold or 60.0)
                except Exception:
                    threshold_v = 60.0
                tracking = self._record_alert_tracking(symbol, last_price)
                print(
                    f"[notify] {symbol} tier={decision.get('tier')} level={push_level} side={side} "
                    f"score={score_v:.1f} final={push_score:.1f} threshold={threshold_v:.1f} reason={reason}"
                )
                try:
                    self._register_live_pnl_entry(
                        symbol=symbol,
                        side=side,
                        level=push_level,
                        tier=str(decision.get("tier", "") or ""),
                        score=score_v,
                        final_score=push_score,
                        threshold=threshold_v,
                        reason=reason,
                        entry_price=last_price,
                    )
                except Exception as exc:
                    print(f"[live-pnl] open failed: {exc!r}")
                if self._resolve_discord_webhook_url(source):
                    try:
                        msg = self._build_discord_message(
                            symbol=symbol,
                            level=push_level,
                            score=score,
                            result=result,
                            tracking=tracking,
                            source=source,
                        )
                        asyncio.create_task(self._send_discord_webhook(msg, source=source))
                    except Exception as exc:
                        print(f"[discord] build message failed: {exc!r}")
            return

        # Legacy delivery (fallback)
        if bool(self.cfg.disable_three_layer_filter):
            alert = True
            if ALERT_LEVEL_RANK.get(level, 0) <= 0:
                level = "WATCH"
        if not alert and meta.get("reason") == "ignition_3of4":
            meta_ts = float(meta.get("ts", 0.0))
            if time.time() - meta_ts <= self.cfg.ignition_watch_ttl_sec:
                level = "WATCH"
                alert = True

        mode = self._discord_delivery_mode(score_v, level, hit_count)
        self._persist_signal_event(
            ts=time.time(),
            symbol=symbol,
            reason=reason,
            side=side,
            level=level,
            alert=bool(alert),
            base_score=score_v,
            threshold=threshold_v_common,
            tier="",
            delivery=mode,
            final_score=score_v,
            hit_count=hit_count,
            payload={
                "trigger_ts": float(meta.get("ts", 0.0) or 0.0),
                "trigger_source": source,
                "details": details,
            },
        )
        if mode == "drop":
            return
        if mode == "summary":
            self._enqueue_discord_summary(symbol, level, score_v)
            return
        if not self._should_emit_alert(symbol, level, alert, score_v):
            return
        last_price = float(result.get("last_price", 0.0) or 0.0)
        try:
            threshold_v = float(threshold or 60.0)
        except Exception:
            threshold_v = 60.0
        tracking = self._record_alert_tracking(symbol, last_price)
        print(
            f"[notify] {symbol} level={level} side={side} "
            f"score={score} threshold={threshold} reason={reason}"
        )
        try:
            self._register_live_pnl_entry(
                symbol=symbol,
                side=side,
                level=level,
                tier="",
                score=score_v,
                final_score=score_v,
                threshold=threshold_v,
                reason=reason,
                entry_price=last_price,
            )
        except Exception as exc:
            print(f"[live-pnl] open failed: {exc!r}")
        if self._resolve_discord_webhook_url(source):
            try:
                msg = self._build_discord_message(
                    symbol=symbol,
                    level=level,
                    score=score,
                    result=result,
                    tracking=tracking,
                    source=source,
                )
                asyncio.create_task(self._send_discord_webhook(msg, source=source))
            except Exception as exc:
                print(f"[discord] build message failed: {exc!r}")

    def _build_discord_message(
        self,
        symbol: str,
        level: str,
        score: Any,
        result: Any,
        tracking: Any,
        source: str = "binance",
    ) -> str:
        bjt = timezone(timedelta(hours=8))
        r = result if isinstance(result, dict) else {}
        t = tracking if isinstance(tracking, dict) else {}
        signals = r.get("signals", {}) if isinstance(r.get("signals", {}), dict) else {}
        metrics = r.get("metrics", {}) if isinstance(r.get("metrics", {}), dict) else {}
        decision = r.get("decision", {}) if isinstance(r.get("decision", {}), dict) else {}
        advanced = r.get("advanced", {}) if isinstance(r.get("advanced", {}), dict) else {}
        alert_digest = advanced.get("alert_digest", {}) if isinstance(advanced.get("alert_digest", {}), dict) else {}
        contradiction = (
            advanced.get("contradiction_guard", {})
            if isinstance(advanced.get("contradiction_guard", {}), dict)
            else {}
        )

        score_v = float(score or 0.0)
        threshold_v = float(r.get("threshold", 60.0) or 60.0)
        count_1h = int(t.get("count_1h", 1))
        first_ts = float(t.get("first_ts", time.time()) or time.time())
        first_price = float(t.get("first_price", 0.0) or 0.0)
        alert_count = int(t.get("count", 1))
        last_price = float(r.get("last_price", 0.0) or 0.0)
        oi_change = float(metrics.get("oi_change_pct", 0.0) or 0.0)
        funding_rate = float(metrics.get("funding_rate", 0.0) or 0.0)
        funding_rate_delta = float(metrics.get("funding_rate_delta", 0.0) or 0.0)
        final_score = float(decision.get("final_score", score_v) or score_v)
        tier = str(decision.get("tier", "") or "").strip()
        ml_win_prob = float(decision.get("ml_win_prob", 0.0) or 0.0)
        whale_testing_active = bool(float(decision.get("whale_testing_active", 0.0) or 0.0) >= 0.5)
        whale_bonus_applied = float(decision.get("whale_bonus_applied", 0.0) or 0.0)
        whale_waive_penalty_applied = float(decision.get("whale_waive_penalty_applied", 0.0) or 0.0)

        title, show_freq = self._resolve_alert_title(level, score_v, threshold_v, count_1h, signals)
        tags = self._build_tags(signals)

        first_ts_text = datetime.fromtimestamp(first_ts, tz=bjt).strftime("%Y-%m-%d %H:%M")
        from_first = 0.0
        if first_price > 0 and last_price > 0:
            from_first = (last_price - first_price) / first_price * 100.0

        source_tag = ""
        if str(source or "").strip().lower() == "aster":
            source_tag = " `ASTER`"
        parts = [
            f"**{title}** **{self._display_symbol(symbol)}**{source_tag} | Score: **{self._fmt_score(score_v)}**",
            f"**决策层级**: {tier or 'N/A'} | Final: **{self._fmt_score(final_score)}**",
            f"**价格**: {self._fmt_price(last_price)}",
            f"**OI变动**: {oi_change:+.2f}%",
            f"**资金费率**: {funding_rate * 100.0:+.4f}%",
            f"**费率变化**: {funding_rate_delta * 100.0:+.4f}%",
        ]
        if ml_win_prob > 0:
            parts.append(f"**Shadow胜率**: {ml_win_prob * 100.0:.1f}%")
        digest_txt = str(alert_digest.get("summary", "")).strip()
        if digest_txt:
            parts.append(f"**结构摘要**: {digest_txt}")
        conf = contradiction.get("confidence", None)
        if conf is not None:
            try:
                parts.append(f"**置信度**: {float(conf) * 100.0:.0f}%")
            except Exception:
                pass
        if show_freq:
            hot_suffix = " (🔥 高频)" if count_1h >= 3 else ""
            parts.append(f"**1h频次**: {count_1h} 次{hot_suffix}")
        parts.extend(
            [
                f"**首次报警**: {first_ts_text} (第 {alert_count} 次)",
                f"**首报价格**: {self._fmt_price(first_price)} ({from_first:+.2f}%)",
                "",
                f"**{tags}**",
            ]
        )
        if whale_testing_active and (whale_bonus_applied > 0.0 or whale_waive_penalty_applied > 0.0):
            parts.append("**🐋 主力试盘确认**")
        text = "\n".join(parts)
        return text[:1900]

    def _resolve_discord_webhook_url(self, source: str = "binance") -> str:
        src = str(source or "binance").strip().lower() or "binance"
        if src == "aster":
            aster_url = str(self.cfg.aster_discord_webhook_url or "").strip()
            if aster_url:
                return aster_url
        return str(self.cfg.discord_webhook_url or "").strip()

    async def _send_discord_webhook(self, content: str, source: str = "binance") -> None:
        webhook_url = self._resolve_discord_webhook_url(source)
        if not webhook_url:
            return
        payload = {
            "content": content,
            "username": self.cfg.discord_username,
        }
        async with self.discord_send_lock:
            now = time.time()
            wait_sec = self.discord_next_send_ts - now
            if wait_sec > 0:
                await asyncio.sleep(wait_sec)

            attempts = max(1, int(self.cfg.discord_retry_max))
            for i in range(attempts):
                try:
                    await asyncio.to_thread(self._http_post_json, webhook_url, payload, 8.0)
                    self._archive_discord_payload(content=content, status="sent")
                    self.discord_next_send_ts = time.time() + max(0.0, float(self.cfg.discord_min_interval_sec))
                    return
                except urllib.error.HTTPError as exc:
                    retry_after = 0.0
                    body = ""
                    try:
                        body = exc.read().decode("utf-8", errors="ignore")
                    except Exception:
                        body = ""
                    if body:
                        try:
                            data = json.loads(body)
                            retry_after = float(data.get("retry_after", 0.0) or 0.0)
                        except Exception:
                            retry_after = 0.0
                    if exc.code == 429 and i < attempts - 1:
                        wait = retry_after if retry_after > 0 else 1.0
                        wait = max(wait, float(self.cfg.discord_min_interval_sec))
                        print(f"[discord] rate_limited, retry in {wait:.2f}s")
                        await asyncio.sleep(wait)
                        continue
                    print(f"[discord] webhook send failed status={exc.code} body={body[:240]!r}")
                    self._archive_discord_payload(
                        content=content,
                        status="failed",
                        http_status=int(exc.code),
                        error=(body[:500] if body else f"HTTP {exc.code}"),
                    )
                    self.discord_next_send_ts = time.time() + max(0.0, float(self.cfg.discord_min_interval_sec))
                    return
                except Exception as exc:
                    if i < attempts - 1:
                        await asyncio.sleep(max(0.5, float(self.cfg.discord_min_interval_sec)))
                        continue
                    print(f"[discord] webhook send failed: {exc!r}")
                    self._archive_discord_payload(content=content, status="failed", error=repr(exc))
                    self.discord_next_send_ts = time.time() + max(0.0, float(self.cfg.discord_min_interval_sec))
                    return


def _build_default_analyzer(data_dir: Path, mode: str, cfg: Config) -> AnalyzerFunc:
    module_cfg = AdvancedModuleConfig(
        trend_segmentation=bool(cfg.mod_trend_seg_enabled),
        combo_classifier=bool(cfg.mod_combo_enabled),
        cvd_structure=bool(cfg.mod_cvd_structure_enabled),
        auxiliary_indicators=bool(cfg.mod_aux_indicators_enabled),
        capital_flow_mtf=bool(cfg.mod_flow_mtf_enabled),
        five_dim_scoring=bool(cfg.mod_scorecard_enabled),
        contradiction_guard=bool(cfg.mod_contradiction_enabled),
        alert_digest=bool(cfg.mod_alert_digest_enabled),
        backtest_struct_metrics=bool(cfg.mod_backtest_metrics_enabled),
        trade_suggestion=bool(cfg.mod_trade_plan_enabled),
    )

    async def analyzer(exchange: Optional[Any], symbol: str) -> Any:
        return await fetch_data_and_analyze(
            exchange,
            symbol,
            data_dir=data_dir,
            mode=mode,
            advanced_modules=module_cfg,
        )

    return analyzer


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Binance futures websocket event monitor")
    p.add_argument("--strategy-version", type=str, default="1.1")
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--aster-ws-enabled", type=_to_bool, default=False)
    p.add_argument("--aster-ws-url", type=str, default="wss://fstream.asterdex.com/ws/!ticker@arr")
    p.add_argument("--aster-exchange-info-url", type=str, default="https://fapi.asterdex.com/fapi/v1/exchangeInfo")
    p.add_argument("--aster-rest-base-url", type=str, default="https://fapi.asterdex.com")
    p.add_argument("--aster-symbols-reload-sec", type=float, default=1800.0)
    p.add_argument("--aster-only-usdt-contracts", type=_to_bool, default=True)
    p.add_argument("--min-pump-pct", type=float, default=1.5)
    p.add_argument("--min-pump-pct-with-volume", type=float, default=0.8)
    p.add_argument("--min-volume-1m", type=float, default=500000.0)
    p.add_argument("--ingress-audit-enabled", type=_to_bool, default=True)
    p.add_argument("--ingress-audit-discord-enabled", type=_to_bool, default=False)
    p.add_argument("--ingress-audit-baseline-minutes", type=int, default=20)
    p.add_argument("--ingress-audit-min-samples", type=int, default=6)
    p.add_argument("--ingress-audit-history-minutes", type=int, default=240)
    p.add_argument("--ingress-audit-min-baseline-count", type=float, default=8.0)
    p.add_argument("--ingress-audit-low-ratio", type=float, default=0.35)
    p.add_argument("--ingress-audit-volume-spike-ratio", type=float, default=2.5)
    p.add_argument("--ingress-audit-alert-cooldown-sec", type=float, default=900.0)
    p.add_argument("--ingress-force-record-enabled", type=_to_bool, default=True)
    p.add_argument("--ingress-force-symbols", type=str, default="BLURUSDT")
    p.add_argument("--ingress-force-min-ret-1m-pct", type=float, default=0.25)
    p.add_argument("--ingress-force-min-vol-ratio-1m", type=float, default=1.20)
    p.add_argument("--ingress-force-record-cooldown-sec", type=float, default=30.0)
    p.add_argument("--flow-pump-min-ret-1m-pct", type=float, default=0.6)
    p.add_argument("--flow-pump-min-volume-1m", type=float, default=100000.0)
    p.add_argument("--flow-pump-min-vol-ratio-1m", type=float, default=3.0)
    p.add_argument("--early-pump-pct-1m", type=float, default=0.35)
    p.add_argument("--early-volume-1m", type=float, default=120000.0)
    p.add_argument("--early-pump-pct-20s", type=float, default=0.18)
    p.add_argument("--early-volume-20s", type=float, default=40000.0)
    p.add_argument("--dynamic-trigger-enabled", type=_to_bool, default=True)
    p.add_argument("--dynamic-trigger-lookback", type=int, default=240)
    p.add_argument("--dynamic-trigger-quantile", type=float, default=0.90)
    p.add_argument("--dynamic-trigger-anchor-pct", type=float, default=0.25)
    p.add_argument("--dynamic-trigger-min-scale", type=float, default=0.65)
    p.add_argument("--dynamic-trigger-max-scale", type=float, default=1.35)
    p.add_argument("--dynamic-min-pump-pct", type=float, default=0.55)
    p.add_argument("--dynamic-min-pump-pct-with-volume", type=float, default=0.25)
    p.add_argument("--dynamic-trigger-use-micro", type=_to_bool, default=True)
    p.add_argument("--dynamic-trigger-event-factor-enabled", type=_to_bool, default=True)
    p.add_argument("--dynamic-trigger-event-major-factor", type=float, default=1.12)
    p.add_argument("--dynamic-trigger-event-minor-factor", type=float, default=1.05)
    p.add_argument("--slow-breakout-enabled", type=_to_bool, default=True)
    p.add_argument("--slow-breakout-ret-1m-pct", type=float, default=0.30)
    p.add_argument("--slow-breakout-ret-5m-pct", type=float, default=1.20)
    p.add_argument("--slow-breakout-min-vol-ratio-1m", type=float, default=0.80)
    p.add_argument("--slow-breakout-min-oi5m-pct", type=float, default=0.60)
    p.add_argument("--slow-breakout-min-delta-persist", type=float, default=0.40)
    p.add_argument("--fake-breakout-filter-enabled", type=_to_bool, default=True)
    p.add_argument("--fake-breakout-ret-1m-pct", type=float, default=0.30)
    p.add_argument("--fake-breakout-min-vol-ratio-1m", type=float, default=1.05)
    p.add_argument("--fake-breakout-min-taker-buy-ratio", type=float, default=0.50)
    p.add_argument("--fake-breakout-min-buy-imbalance", type=float, default=0.0)
    p.add_argument("--fake-breakout-max-oi5m-pct", type=float, default=0.2)
    p.add_argument("--fake-breakout-pullback-pct", type=float, default=0.45)
    p.add_argument("--fake-breakout-delta-persist-max", type=float, default=0.35)
    p.add_argument("--fake-breakout-min-flags", type=int, default=3)
    p.add_argument("--liquidation-spike-enabled", type=_to_bool, default=True)
    p.add_argument("--liquidation-spike-ret-1m-pct", type=float, default=1.80)
    p.add_argument("--liquidation-spike-min-vol-ratio-1m", type=float, default=3.00)
    p.add_argument("--liquidation-spike-max-oi5m-pct", type=float, default=0.30)
    p.add_argument("--liquidation-spike-min-fake-flags", type=int, default=2)
    p.add_argument("--liquidation-spike-timer-sec", type=float, default=900.0)
    p.add_argument("--liquidation-spike-retrace-pct", type=float, default=50.0)
    p.add_argument("--liquidation-spike-stabilize-abs-ret-1m-pct", type=float, default=0.35)
    p.add_argument("--liquidation-spike-stabilize-max-vol-ratio-1m", type=float, default=1.20)
    p.add_argument("--liquidation-spike-confirm-min-oi5m-pct", type=float, default=0.50)
    p.add_argument("--liquidation-spike-follow-window-sec", type=float, default=1800.0)
    p.add_argument("--liquidation-spike-check-interval-sec", type=float, default=5.0)
    p.add_argument("--liquidation-spike-alert-cooldown-sec", type=float, default=3600.0)
    p.add_argument("--liquidation-spike-min-taker-buy-ratio", type=float, default=0.58)
    p.add_argument("--liquidation-spike-min-buy-imbalance", type=float, default=0.08)
    p.add_argument("--slow-accum-enabled", type=_to_bool, default=True)
    p.add_argument("--slow-accum-window-sec", type=float, default=5400.0)
    p.add_argument("--slow-accum-min-hits", type=int, default=100)
    p.add_argument("--slow-accum-tier2-min-hits", type=int, default=200)
    p.add_argument("--slow-accum-tier3-min-hits", type=int, default=121)
    p.add_argument("--slow-accum-tier3-realtime", type=_to_bool, default=True)
    p.add_argument("--slow-accum-min-ret-1m-pct", type=float, default=0.25)
    p.add_argument("--slow-accum-max-ret-1m-pct", type=float, default=0.80)
    p.add_argument("--slow-accum-min-stair-step-pct", type=float, default=0.50)
    p.add_argument("--slow-accum-alert-cooldown-sec", type=float, default=3600.0)
    p.add_argument("--slow-accum-alpha-ret-1m-pct", type=float, default=0.406)
    p.add_argument("--slow-accum-alpha-hits", type=int, default=200)
    p.add_argument("--slow-accum-alpha-bonus", type=float, default=10.0)
    p.add_argument("--slow-accum-alpha-plus-bonus", type=float, default=18.0)
    p.add_argument("--fake-breakout-probe-enabled", type=_to_bool, default=True)
    p.add_argument("--fake-breakout-probe-window-sec", type=float, default=4 * 3600.0)
    p.add_argument("--fake-breakout-probe-min-hits", type=int, default=50)
    p.add_argument("--fake-breakout-probe-max-centroid-drop-pct", type=float, default=2.0)
    p.add_argument("--fake-breakout-probe-alert-cooldown-sec", type=float, default=3600.0)
    p.add_argument("--fake-breakout-probe-push-min-hits", type=int, default=251)
    p.add_argument("--fake-breakout-probe-push-max-ret-1m-pct", type=float, default=0.335)
    p.add_argument("--fake-breakout-probe-push-max-centroid-change-pct", type=float, default=-0.35)
    p.add_argument("--fake-breakout-probe-bonus", type=float, default=8.0)
    p.add_argument("--fake-breakout-probe-alpha-bonus", type=float, default=15.0)
    p.add_argument("--fake-breakout-probe-tier3-realtime", type=_to_bool, default=True)
    p.add_argument("--probe-follow-breakout-window-sec", type=float, default=3600.0)
    p.add_argument("--probe-follow-whale-bonus", type=float, default=12.0)
    p.add_argument("--probe-follow-waive-fake-breakout-once", type=_to_bool, default=True)
    p.add_argument("--module-signal-cooldown-sec", type=float, default=1200.0)
    p.add_argument("--whale-testing-enabled", type=_to_bool, default=True)
    p.add_argument("--whale-testing-window-sec", type=float, default=4 * 3600.0)
    p.add_argument("--whale-testing-min-hits", type=int, default=50)
    p.add_argument("--whale-testing-max-drawdown-pct", type=float, default=3.0)
    p.add_argument("--whale-testing-ttl-sec", type=float, default=4 * 3600.0)
    p.add_argument("--whale-bonus-enabled", type=_to_bool, default=True)
    p.add_argument("--whale-bonus-score", type=float, default=15.0)
    p.add_argument("--whale-waive-fake-breakout-penalty", type=_to_bool, default=False)
    p.add_argument("--whale-waive-top-risk-penalty", type=_to_bool, default=False)
    p.add_argument("--ignition-pump-1m", type=float, default=0.18)
    p.add_argument("--ignition-pump-20s", type=float, default=0.10)
    p.add_argument("--ignition-vol-ratio-1m", type=float, default=1.6)
    p.add_argument("--ignition-cvd-z", type=float, default=1.8)
    p.add_argument("--ignition-cvd-decay-tau", type=float, default=6.0)
    p.add_argument("--ignition-oi5m-pct", type=float, default=0.4)
    p.add_argument("--ignition-min-signals", type=int, default=2)
    p.add_argument("--ignition-bear-veto-enabled", type=_to_bool, default=True)
    p.add_argument("--ignition-bear-veto-cvd-z", type=float, default=-1.5)
    p.add_argument("--ignition-bear-veto-oi5m", type=float, default=0.5)
    p.add_argument("--ignition-3of4-blocked", type=_to_bool, default=False)
    p.add_argument("--ignition-min-buy-imbalance", type=float, default=0.20)
    p.add_argument("--ignition-max-fake-flags", type=int, default=1)
    p.add_argument("--micro-cache-ttl-sec", type=float, default=8.0)
    p.add_argument("--ignition-watch-ttl-sec", type=float, default=120.0)
    p.add_argument("--sr-filter-enabled", type=_to_bool, default=True)
    p.add_argument("--sr-lookback-5m", type=int, default=72)
    p.add_argument("--sr-breakout-dist-pct", type=float, default=1.8)
    p.add_argument("--sr-support-dist-pct", type=float, default=1.5)
    p.add_argument("--sr-max-compression-pct", type=float, default=4.5)
    p.add_argument("--sr-soft-pass-enabled", type=_to_bool, default=True)
    p.add_argument("--sr-soft-min-hit", type=int, default=3)
    p.add_argument("--sr-soft-min-oi5m-pct", type=float, default=1.0)
    p.add_argument("--cross-filter-enabled", type=_to_bool, default=True)
    p.add_argument("--cross-ret-1m-pct", type=float, default=0.08)
    p.add_argument("--cross-vol-ratio-1m", type=float, default=1.2)
    p.add_argument("--cross-grace-no-data", type=_to_bool, default=True)
    p.add_argument("--flow-filter-enabled", type=_to_bool, default=True)
    p.add_argument("--flow-buy-ratio-min", type=float, default=0.54)
    p.add_argument("--flow-imbalance-min", type=float, default=0.05)
    p.add_argument("--flow-lsr-5m-min-pct", type=float, default=0.3)
    p.add_argument("--flow-min-signals", type=int, default=2)
    p.add_argument("--flow-grace-no-data", type=_to_bool, default=True)
    p.add_argument("--flow-relax-enabled", type=_to_bool, default=True)
    p.add_argument("--flow-relax-ret-1m-pct", type=float, default=0.35)
    p.add_argument("--flow-relax-oi5m-pct", type=float, default=0.8)
    p.add_argument("--flow-relax-min-signals", type=int, default=1)
    p.add_argument("--onchain-filter-enabled", type=_to_bool, default=True)
    p.add_argument("--onchain-symbols", type=str, default="AINUSDT")
    p.add_argument("--onchain-targets-file", type=str, default="config/onchain_targets.json")
    p.add_argument("--onchain-targets-reload-sec", type=float, default=10.0)
    p.add_argument("--onchain-sector-file", type=str, default="config/onchain_sectors.json")
    p.add_argument("--onchain-sector-reload-sec", type=float, default=30.0)
    p.add_argument("--onchain-ret-5m-pct", type=float, default=0.25)
    p.add_argument("--onchain-vol-ratio-5m", type=float, default=1.8)
    p.add_argument("--onchain-min-signals", type=int, default=1)
    p.add_argument("--onchain-grace-no-data", type=_to_bool, default=True)
    p.add_argument("--onchain-auto-add-enabled", type=_to_bool, default=True)
    p.add_argument("--onchain-auto-add-vol-ratio-1m", type=float, default=5.0)
    p.add_argument("--onchain-auto-add-min-volume-1m", type=float, default=200000.0)
    p.add_argument("--onchain-auto-add-ttl-sec", type=float, default=86400.0)
    p.add_argument("--onchain-auto-add-discord-enabled", type=_to_bool, default=False)
    p.add_argument("--onchain-auto-add-discord-batch-window-sec", type=float, default=3.0)
    p.add_argument("--onchain-auto-add-discord-max-rows", type=int, default=15)
    p.add_argument("--onchain-sector-auto-expand-enabled", type=_to_bool, default=True)
    p.add_argument("--onchain-sector-expand-ttl-sec", type=float, default=86400.0)
    p.add_argument("--onchain-sector-min-quote-volume-24h", type=float, default=1000000.0)
    p.add_argument("--onchain-sector-max-peers-per-seed", type=int, default=5)
    p.add_argument("--onchain-priority-enabled", type=_to_bool, default=True)
    p.add_argument("--onchain-high-worker-cooldown-sec", type=float, default=0.05)
    p.add_argument("--onchain-discovery-ttl-sec", type=float, default=1800.0)
    p.add_argument("--onchain-micro-ttl-sec", type=float, default=20.0)
    p.add_argument("--onchain-min-liquidity-usd", type=float, default=20000.0)
    p.add_argument("--queue-symbol-cooldown-sec", type=float, default=10.0)
    p.add_argument("--worker-cooldown-sec", type=float, default=0.5)
    p.add_argument("--worker-healthcheck-enabled", type=_to_bool, default=True)
    p.add_argument("--worker-healthcheck-interval-sec", type=float, default=10.0)
    p.add_argument("--worker-healthcheck-stall-sec", type=float, default=60.0)
    p.add_argument("--worker-healthcheck-min-triggers", type=int, default=1)
    p.add_argument("--alert-cooldown-sec", type=float, default=180.0)
    p.add_argument("--alert-cooldown-low-sec", type=float, default=90.0)
    p.add_argument("--discord-min-score", type=float, default=50.0)
    p.add_argument("--discord-watch-min-hit", type=int, default=2)
    p.add_argument("--discord-min-interval-sec", type=float, default=0.35)
    p.add_argument("--discord-retry-max", type=int, default=2)
    p.add_argument("--disable-three-layer-filter", type=_to_bool, default=False)
    p.add_argument("--discord-heartbeat-enabled", type=_to_bool, default=False)
    p.add_argument("--discord-heartbeat-interval-sec", type=float, default=900.0)
    p.add_argument("--discord-heartbeat-content", type=str, default="初号机运行中")
    p.add_argument("--discord-module-stats-enabled", type=_to_bool, default=False)
    p.add_argument("--discord-module-stats-interval-sec", type=float, default=7200.0)
    p.add_argument("--discord-module-stats-window-sec", type=float, default=24 * 3600.0)
    p.add_argument("--discord-module-stats-top-n", type=int, default=10)
    p.add_argument("--discord-webhook-url", type=str, default=os.getenv("DISCORD_WEBHOOK_URL", ""))
    p.add_argument("--aster-discord-webhook-url", type=str, default=os.getenv("ASTER_DISCORD_WEBHOOK_URL", ""))
    p.add_argument(
        "--webhook-config-file",
        type=str,
        default=os.getenv("MONITOR_WS_WEBHOOK_CONFIG_FILE", "config/webhook_monitor_ws.json"),
    )
    p.add_argument("--discord-username", type=str, default="Futures Monitor")
    p.add_argument("--discord-archive-enabled", type=_to_bool, default=True)
    p.add_argument("--discord-archive-file", type=str, default="data/logs/monitor_ws_discord_push.jsonl")
    p.add_argument("--signal-db-enabled", type=_to_bool, default=True)
    p.add_argument("--signal-db-file", type=str, default="data/logs/monitor_ws_signals.db")
    p.add_argument("--aster-signal-db-file", type=str, default="data/logs/monitor_ws_signals_aster.db")
    p.add_argument("--decision-tier-enabled", type=_to_bool, default=True)
    p.add_argument("--tier1-score", type=float, default=85.0)
    p.add_argument("--tier2-score", type=float, default=65.0)
    p.add_argument("--tier3-score", type=float, default=45.0)
    p.add_argument("--tier2-confirm-k-sec", type=float, default=60.0)
    p.add_argument("--tier2-confirm-pullback-pct", type=float, default=0.45)
    p.add_argument("--tier2-confirm-min-checks", type=int, default=1)
    p.add_argument("--tier3-discord-push-enabled", type=_to_bool, default=False)
    p.add_argument("--sniper-risk-downgrade-enabled", type=_to_bool, default=False)
    p.add_argument("--sniper-risk-downgrade-penalty", type=float, default=20.0)
    p.add_argument("--sniper-risk-drop-on-heavy-fake-breakout", type=_to_bool, default=True)
    p.add_argument("--sniper-risk-drop-penalty", type=float, default=35.0)
    p.add_argument("--tier-decision-file", type=str, default="data/logs/monitor_ws_tier_decisions.jsonl")
    p.add_argument("--auto-tune-enabled", type=_to_bool, default=True)
    p.add_argument("--auto-tune-apply-enabled", type=_to_bool, default=True)
    p.add_argument("--auto-tune-persist-control", type=_to_bool, default=False)
    p.add_argument("--auto-tune-interval-sec", type=float, default=300.0)
    p.add_argument("--auto-tune-window-rows", type=int, default=180)
    p.add_argument("--auto-tune-min-samples", type=int, default=30)
    p.add_argument("--auto-tune-cooldown-sec", type=float, default=3600.0)
    p.add_argument("--auto-tune-win-rate-high", type=float, default=0.70)
    p.add_argument("--auto-tune-win-rate-low", type=float, default=0.40)
    p.add_argument("--auto-tune-tier-step-up", type=float, default=2.0)
    p.add_argument("--auto-tune-tier-step-down", type=float, default=3.0)
    p.add_argument("--auto-tune-tier2-min", type=float, default=55.0)
    p.add_argument("--auto-tune-tier2-max", type=float, default=80.0)
    p.add_argument("--auto-tune-log-file", type=str, default="data/logs/monitor_ws_auto_tune.jsonl")
    p.add_argument("--symbol-quality-enabled", type=_to_bool, default=True)
    p.add_argument("--symbol-quality-window-rows", type=int, default=500)
    p.add_argument("--symbol-quality-min-samples", type=int, default=20)
    p.add_argument("--symbol-quality-alpha", type=float, default=4.0)
    p.add_argument("--symbol-quality-beta", type=float, default=4.0)
    p.add_argument("--symbol-quality-floor", type=float, default=0.70)
    p.add_argument("--symbol-quality-cap", type=float, default=1.25)
    p.add_argument("--sector-cluster-enabled", type=_to_bool, default=True)
    p.add_argument("--sector-cluster-window-sec", type=float, default=180.0)
    p.add_argument("--sector-cluster-min-count", type=int, default=3)
    p.add_argument("--sector-cluster-boost-factor", type=float, default=1.30)
    p.add_argument("--sector-cluster-ttl-sec", type=float, default=900.0)
    p.add_argument("--sector-cluster-min-base-score", type=float, default=20.0)
    p.add_argument("--regime-pattern-enabled", type=_to_bool, default=True)
    p.add_argument("--regime-window-rows", type=int, default=500)
    p.add_argument("--regime-min-samples", type=int, default=20)
    p.add_argument("--regime-win-rate-high", type=float, default=0.62)
    p.add_argument("--regime-win-rate-low", type=float, default=0.42)
    p.add_argument("--regime-factor-boost", type=float, default=1.05)
    p.add_argument("--regime-factor-cut", type=float, default=0.92)
    p.add_argument("--regime-morning-factor", type=float, default=1.03)
    p.add_argument("--regime-friday-night-factor", type=float, default=0.92)
    p.add_argument("--sentiment-enabled", type=_to_bool, default=True)
    p.add_argument("--sentiment-whale-notional-usd", type=float, default=100000.0)
    p.add_argument("--sentiment-lsr-weight", type=float, default=0.06)
    p.add_argument("--sentiment-whale-weight", type=float, default=0.07)
    p.add_argument("--sentiment-concentration-weight", type=float, default=0.06)
    p.add_argument("--sentiment-funding-div-weight", type=float, default=0.05)
    p.add_argument("--sentiment-funding-div-streak-warn", type=int, default=3)
    p.add_argument("--symbol-adapt-enabled", type=_to_bool, default=True)
    p.add_argument("--symbol-liq-high-24h", type=float, default=200000000.0)
    p.add_argument("--symbol-liq-mid-24h", type=float, default=20000000.0)
    p.add_argument("--symbol-new-listing-days", type=int, default=7)
    p.add_argument("--mtf-resonance-enabled", type=_to_bool, default=True)
    p.add_argument("--risk-liquidity-enabled", type=_to_bool, default=True)
    p.add_argument("--risk-slippage-enabled", type=_to_bool, default=True)
    p.add_argument("--risk-cross-anomaly-enabled", type=_to_bool, default=True)
    p.add_argument("--risk-spread-bps-max", type=float, default=12.0)
    p.add_argument("--risk-depth-min-usd", type=float, default=8000.0)
    p.add_argument("--risk-slippage-notional-usd", type=float, default=5000.0)
    p.add_argument("--risk-slippage-max-pct", type=float, default=0.15)
    p.add_argument("--risk-cross-anomaly-pct", type=float, default=0.6)
    p.add_argument("--risk-penalty-liquidity", type=float, default=10.0)
    p.add_argument("--risk-penalty-slippage", type=float, default=10.0)
    p.add_argument("--risk-penalty-cross-anomaly", type=float, default=6.0)
    p.add_argument("--risk-penalty-fake-breakout", type=float, default=12.0)
    p.add_argument("--risk-penalty-iceberg", type=float, default=4.0)
    p.add_argument("--bayesian-scorer-enabled", type=_to_bool, default=True)
    p.add_argument("--bayesian-prior-max", type=float, default=0.50)
    p.add_argument("--bayesian-prior-min", type=float, default=0.15)
    p.add_argument("--bayesian-prior-decay-hits", type=float, default=100.0)
    p.add_argument("--bayesian-evidence-a-pump20s-pct", type=float, default=3.0)
    p.add_argument("--bayesian-evidence-b-funding-rate-lt", type=float, default=0.0)
    p.add_argument("--bayesian-evidence-b-funding-delta-lt", type=float, default=-0.0001)
    p.add_argument("--bayesian-evidence-c-oi5m-pct-le", type=float, default=0.0)
    p.add_argument("--bayesian-p-ea-given-h", type=float, default=0.90)
    p.add_argument("--bayesian-p-eb-given-h", type=float, default=0.85)
    p.add_argument("--bayesian-p-ec-given-h", type=float, default=0.90)
    p.add_argument("--bayesian-p-ea-given-not-h", type=float, default=0.10)
    p.add_argument("--bayesian-p-eb-given-not-h", type=float, default=0.20)
    p.add_argument("--bayesian-p-ec-given-not-h", type=float, default=0.20)
    p.add_argument("--bayesian-override-posterior-threshold", type=float, default=0.80)
    p.add_argument("--bayesian-score-multiplier", type=float, default=1.20)
    p.add_argument("--bayesian-block-fake-weak-oi-penalty", type=_to_bool, default=True)
    p.add_argument("--bayesian-block-funding-divergence-penalty", type=_to_bool, default=True)
    p.add_argument("--bayesian-force-sniper-enabled", type=_to_bool, default=False)
    p.add_argument("--bayesian-reason-override", type=str, default="short_squeeze_ignition")
    p.add_argument("--ml-shadow-enabled", type=_to_bool, default=True)
    p.add_argument("--ml-shadow-downgrade-prob", type=float, default=0.30)
    p.add_argument("--ml-shadow-upgrade-prob", type=float, default=0.75)
    p.add_argument("--ml-shadow-penalty", type=float, default=8.0)
    p.add_argument("--ml-shadow-bonus", type=float, default=5.0)
    p.add_argument("--control-file", type=str, default="config/monitor_ws_control.json")
    p.add_argument("--mod-trend-seg-enabled", type=_to_bool, default=True)
    p.add_argument("--mod-combo-enabled", type=_to_bool, default=True)
    p.add_argument("--mod-cvd-structure-enabled", type=_to_bool, default=True)
    p.add_argument("--mod-aux-indicators-enabled", type=_to_bool, default=True)
    p.add_argument("--mod-flow-mtf-enabled", type=_to_bool, default=True)
    p.add_argument("--mod-scorecard-enabled", type=_to_bool, default=True)
    p.add_argument("--mod-contradiction-enabled", type=_to_bool, default=True)
    p.add_argument("--mod-alert-digest-enabled", type=_to_bool, default=True)
    p.add_argument("--mod-backtest-metrics-enabled", type=_to_bool, default=True)
    p.add_argument("--mod-trade-plan-enabled", type=_to_bool, default=True)
    p.add_argument("--data-dir", type=str, default="data")
    p.add_argument("--mode", type=str, default="lob", choices=["lob", "atr", "fixed", "liquidity"])
    return p.parse_args()


async def amain() -> None:
    args = parse_args()
    control = _load_control_overrides(args.control_file)
    webhook_config_file = (
        str(getattr(args, "webhook_config_file", "") or "").strip() or "config/webhook_monitor_ws.json"
    )
    resolved_monitor_ws_webhook = _resolve_monitor_ws_webhook(
        control=control,
        cli_webhook_url=str(args.discord_webhook_url or "").strip(),
        webhook_file=webhook_config_file,
    )
    cfg = Config(
        strategy_version=str(control.get("strategy_version", args.strategy_version) or "1.1").strip() or "1.1",
        aster_ws_enabled=bool(control.get("aster_ws_enabled", args.aster_ws_enabled)),
        aster_ws_url=str(control.get("aster_ws_url", args.aster_ws_url) or "").strip()
        or "wss://fstream.asterdex.com/ws/!ticker@arr",
        aster_exchange_info_url=str(control.get("aster_exchange_info_url", args.aster_exchange_info_url) or "").strip()
        or "https://fapi.asterdex.com/fapi/v1/exchangeInfo",
        aster_rest_base_url=str(control.get("aster_rest_base_url", args.aster_rest_base_url) or "").strip()
        or "https://fapi.asterdex.com",
        aster_symbols_reload_sec=max(
            30.0,
            float(control.get("aster_symbols_reload_sec", args.aster_symbols_reload_sec)),
        ),
        aster_only_usdt_contracts=bool(control.get("aster_only_usdt_contracts", args.aster_only_usdt_contracts)),
        workers=max(1, args.workers),
        min_pump_pct=args.min_pump_pct,
        min_pump_pct_with_volume=args.min_pump_pct_with_volume,
        min_volume_1m=args.min_volume_1m,
        ingress_audit_enabled=bool(control.get("ingress_audit_enabled", args.ingress_audit_enabled)),
        ingress_audit_discord_enabled=bool(
            control.get("ingress_audit_discord_enabled", args.ingress_audit_discord_enabled)
        ),
        ingress_audit_baseline_minutes=max(
            5,
            int(control.get("ingress_audit_baseline_minutes", args.ingress_audit_baseline_minutes)),
        ),
        ingress_audit_min_samples=max(
            1,
            int(control.get("ingress_audit_min_samples", args.ingress_audit_min_samples)),
        ),
        ingress_audit_history_minutes=max(
            30,
            int(control.get("ingress_audit_history_minutes", args.ingress_audit_history_minutes)),
        ),
        ingress_audit_min_baseline_count=max(
            1.0,
            float(control.get("ingress_audit_min_baseline_count", args.ingress_audit_min_baseline_count)),
        ),
        ingress_audit_low_ratio=max(
            0.05,
            min(0.95, float(control.get("ingress_audit_low_ratio", args.ingress_audit_low_ratio))),
        ),
        ingress_audit_volume_spike_ratio=max(
            1.0,
            float(control.get("ingress_audit_volume_spike_ratio", args.ingress_audit_volume_spike_ratio)),
        ),
        ingress_audit_alert_cooldown_sec=max(
            60.0,
            float(control.get("ingress_audit_alert_cooldown_sec", args.ingress_audit_alert_cooldown_sec)),
        ),
        ingress_force_record_enabled=bool(
            control.get("ingress_force_record_enabled", args.ingress_force_record_enabled)
        ),
        ingress_force_symbols=tuple(
            sorted(
                _parse_symbol_collection(
                    control.get("ingress_force_symbols", _parse_symbols(args.ingress_force_symbols))
                )
            )
        ),
        ingress_force_min_ret_1m_pct=max(
            0.0,
            float(control.get("ingress_force_min_ret_1m_pct", args.ingress_force_min_ret_1m_pct)),
        ),
        ingress_force_min_vol_ratio_1m=max(
            0.0,
            float(control.get("ingress_force_min_vol_ratio_1m", args.ingress_force_min_vol_ratio_1m)),
        ),
        ingress_force_record_cooldown_sec=max(
            1.0,
            float(control.get("ingress_force_record_cooldown_sec", args.ingress_force_record_cooldown_sec)),
        ),
        flow_pump_min_ret_1m_pct=max(
            0.05,
            float(control.get("flow_pump_min_ret_1m_pct", args.flow_pump_min_ret_1m_pct)),
        ),
        flow_pump_min_volume_1m=max(
            0.0,
            float(control.get("flow_pump_min_volume_1m", args.flow_pump_min_volume_1m)),
        ),
        flow_pump_min_vol_ratio_1m=max(
            0.0,
            float(control.get("flow_pump_min_vol_ratio_1m", args.flow_pump_min_vol_ratio_1m)),
        ),
        early_pump_pct_1m=args.early_pump_pct_1m,
        early_volume_1m=args.early_volume_1m,
        early_pump_pct_20s=args.early_pump_pct_20s,
        early_volume_20s=args.early_volume_20s,
        dynamic_trigger_enabled=bool(control.get("dynamic_trigger_enabled", args.dynamic_trigger_enabled)),
        dynamic_trigger_lookback=max(30, int(control.get("dynamic_trigger_lookback", args.dynamic_trigger_lookback))),
        dynamic_trigger_quantile=min(
            0.99,
            max(0.5, float(control.get("dynamic_trigger_quantile", args.dynamic_trigger_quantile))),
        ),
        dynamic_trigger_anchor_pct=max(
            0.05,
            float(control.get("dynamic_trigger_anchor_pct", args.dynamic_trigger_anchor_pct)),
        ),
        dynamic_trigger_min_scale=max(
            0.2,
            float(control.get("dynamic_trigger_min_scale", args.dynamic_trigger_min_scale)),
        ),
        dynamic_trigger_max_scale=max(
            0.3,
            float(control.get("dynamic_trigger_max_scale", args.dynamic_trigger_max_scale)),
        ),
        dynamic_min_pump_pct=max(0.05, float(control.get("dynamic_min_pump_pct", args.dynamic_min_pump_pct))),
        dynamic_min_pump_pct_with_volume=max(
            0.03,
            float(control.get("dynamic_min_pump_pct_with_volume", args.dynamic_min_pump_pct_with_volume)),
        ),
        dynamic_trigger_use_micro=bool(control.get("dynamic_trigger_use_micro", args.dynamic_trigger_use_micro)),
        dynamic_trigger_event_factor_enabled=bool(
            control.get("dynamic_trigger_event_factor_enabled", args.dynamic_trigger_event_factor_enabled)
        ),
        dynamic_trigger_event_major_factor=max(
            1.0,
            float(control.get("dynamic_trigger_event_major_factor", args.dynamic_trigger_event_major_factor)),
        ),
        dynamic_trigger_event_minor_factor=max(
            1.0,
            float(control.get("dynamic_trigger_event_minor_factor", args.dynamic_trigger_event_minor_factor)),
        ),
        slow_breakout_enabled=bool(control.get("slow_breakout_enabled", args.slow_breakout_enabled)),
        slow_breakout_ret_1m_pct=max(
            0.05,
            float(control.get("slow_breakout_ret_1m_pct", args.slow_breakout_ret_1m_pct)),
        ),
        slow_breakout_ret_5m_pct=max(
            0.10,
            float(control.get("slow_breakout_ret_5m_pct", args.slow_breakout_ret_5m_pct)),
        ),
        slow_breakout_min_vol_ratio_1m=max(
            0.0,
            float(control.get("slow_breakout_min_vol_ratio_1m", args.slow_breakout_min_vol_ratio_1m)),
        ),
        slow_breakout_min_oi5m_pct=max(
            0.0,
            float(control.get("slow_breakout_min_oi5m_pct", args.slow_breakout_min_oi5m_pct)),
        ),
        slow_breakout_min_delta_persist=max(
            0.0,
            float(control.get("slow_breakout_min_delta_persist", args.slow_breakout_min_delta_persist)),
        ),
        fake_breakout_filter_enabled=bool(
            control.get("fake_breakout_filter_enabled", args.fake_breakout_filter_enabled)
        ),
        fake_breakout_ret_1m_pct=max(
            0.05,
            float(control.get("fake_breakout_ret_1m_pct", args.fake_breakout_ret_1m_pct)),
        ),
        fake_breakout_min_vol_ratio_1m=max(
            0.1,
            float(control.get("fake_breakout_min_vol_ratio_1m", args.fake_breakout_min_vol_ratio_1m)),
        ),
        fake_breakout_min_taker_buy_ratio=min(
            1.0,
            max(0.0, float(control.get("fake_breakout_min_taker_buy_ratio", args.fake_breakout_min_taker_buy_ratio))),
        ),
        fake_breakout_min_buy_imbalance=min(
            1.0,
            max(-1.0, float(control.get("fake_breakout_min_buy_imbalance", args.fake_breakout_min_buy_imbalance))),
        ),
        fake_breakout_max_oi5m_pct=float(
            control.get("fake_breakout_max_oi5m_pct", args.fake_breakout_max_oi5m_pct)
        ),
        fake_breakout_pullback_pct=max(
            0.0,
            float(control.get("fake_breakout_pullback_pct", args.fake_breakout_pullback_pct)),
        ),
        fake_breakout_delta_persist_max=max(
            0.0,
            min(1.0, float(control.get("fake_breakout_delta_persist_max", args.fake_breakout_delta_persist_max))),
        ),
        fake_breakout_min_flags=max(
            1,
            min(5, int(control.get("fake_breakout_min_flags", args.fake_breakout_min_flags))),
        ),
        liquidation_spike_enabled=bool(
            control.get("liquidation_spike_enabled", args.liquidation_spike_enabled)
        ),
        liquidation_spike_ret_1m_pct=max(
            0.1,
            float(control.get("liquidation_spike_ret_1m_pct", args.liquidation_spike_ret_1m_pct)),
        ),
        liquidation_spike_min_vol_ratio_1m=max(
            0.0,
            float(
                control.get(
                    "liquidation_spike_min_vol_ratio_1m",
                    args.liquidation_spike_min_vol_ratio_1m,
                )
            ),
        ),
        liquidation_spike_max_oi5m_pct=float(
            control.get("liquidation_spike_max_oi5m_pct", args.liquidation_spike_max_oi5m_pct)
        ),
        liquidation_spike_min_fake_flags=max(
            1,
            min(
                10,
                int(control.get("liquidation_spike_min_fake_flags", args.liquidation_spike_min_fake_flags)),
            ),
        ),
        liquidation_spike_timer_sec=max(
            60.0,
            float(control.get("liquidation_spike_timer_sec", args.liquidation_spike_timer_sec)),
        ),
        liquidation_spike_retrace_pct=max(
            5.0,
            min(95.0, float(control.get("liquidation_spike_retrace_pct", args.liquidation_spike_retrace_pct))),
        ),
        liquidation_spike_stabilize_abs_ret_1m_pct=max(
            0.01,
            float(
                control.get(
                    "liquidation_spike_stabilize_abs_ret_1m_pct",
                    args.liquidation_spike_stabilize_abs_ret_1m_pct,
                )
            ),
        ),
        liquidation_spike_stabilize_max_vol_ratio_1m=max(
            0.1,
            float(
                control.get(
                    "liquidation_spike_stabilize_max_vol_ratio_1m",
                    args.liquidation_spike_stabilize_max_vol_ratio_1m,
                )
            ),
        ),
        liquidation_spike_confirm_min_oi5m_pct=float(
            control.get("liquidation_spike_confirm_min_oi5m_pct", args.liquidation_spike_confirm_min_oi5m_pct)
        ),
        liquidation_spike_follow_window_sec=max(
            300.0,
            float(control.get("liquidation_spike_follow_window_sec", args.liquidation_spike_follow_window_sec)),
        ),
        liquidation_spike_check_interval_sec=max(
            1.0,
            float(control.get("liquidation_spike_check_interval_sec", args.liquidation_spike_check_interval_sec)),
        ),
        liquidation_spike_alert_cooldown_sec=max(
            60.0,
            float(control.get("liquidation_spike_alert_cooldown_sec", args.liquidation_spike_alert_cooldown_sec)),
        ),
        liquidation_spike_min_taker_buy_ratio=min(
            1.0,
            max(
                0.0,
                float(
                    control.get(
                        "liquidation_spike_min_taker_buy_ratio",
                        args.liquidation_spike_min_taker_buy_ratio,
                    )
                ),
            ),
        ),
        liquidation_spike_min_buy_imbalance=min(
            1.0,
            max(
                -1.0,
                float(
                    control.get(
                        "liquidation_spike_min_buy_imbalance",
                        args.liquidation_spike_min_buy_imbalance,
                    )
                ),
            ),
        ),
        slow_accum_enabled=bool(control.get("slow_accum_enabled", args.slow_accum_enabled)),
        slow_accum_window_sec=max(
            3600.0,
            float(control.get("slow_accum_window_sec", args.slow_accum_window_sec)),
        ),
        slow_accum_min_hits=max(
            1,
            int(control.get("slow_accum_min_hits", args.slow_accum_min_hits)),
        ),
        slow_accum_tier2_min_hits=max(
            1,
            int(control.get("slow_accum_tier2_min_hits", args.slow_accum_tier2_min_hits)),
        ),
        slow_accum_tier3_min_hits=max(
            1,
            int(control.get("slow_accum_tier3_min_hits", args.slow_accum_tier3_min_hits)),
        ),
        slow_accum_tier3_realtime=bool(
            control.get("slow_accum_tier3_realtime", args.slow_accum_tier3_realtime)
        ),
        slow_accum_min_ret_1m_pct=max(
            0.0,
            float(control.get("slow_accum_min_ret_1m_pct", args.slow_accum_min_ret_1m_pct)),
        ),
        slow_accum_max_ret_1m_pct=max(
            0.0,
            float(control.get("slow_accum_max_ret_1m_pct", args.slow_accum_max_ret_1m_pct)),
        ),
        slow_accum_min_stair_step_pct=max(
            0.0,
            float(control.get("slow_accum_min_stair_step_pct", args.slow_accum_min_stair_step_pct)),
        ),
        slow_accum_alert_cooldown_sec=max(
            300.0,
            float(control.get("slow_accum_alert_cooldown_sec", args.slow_accum_alert_cooldown_sec)),
        ),
        slow_accum_alpha_ret_1m_pct=max(
            0.0,
            float(control.get("slow_accum_alpha_ret_1m_pct", args.slow_accum_alpha_ret_1m_pct)),
        ),
        slow_accum_alpha_hits=max(
            1,
            int(control.get("slow_accum_alpha_hits", args.slow_accum_alpha_hits)),
        ),
        slow_accum_alpha_bonus=max(
            0.0,
            float(control.get("slow_accum_alpha_bonus", args.slow_accum_alpha_bonus)),
        ),
        slow_accum_alpha_plus_bonus=max(
            0.0,
            float(control.get("slow_accum_alpha_plus_bonus", args.slow_accum_alpha_plus_bonus)),
        ),
        fake_breakout_probe_enabled=bool(
            control.get("fake_breakout_probe_enabled", args.fake_breakout_probe_enabled)
        ),
        fake_breakout_probe_window_sec=max(
            300.0,
            float(control.get("fake_breakout_probe_window_sec", args.fake_breakout_probe_window_sec)),
        ),
        fake_breakout_probe_min_hits=max(
            1,
            int(control.get("fake_breakout_probe_min_hits", args.fake_breakout_probe_min_hits)),
        ),
        fake_breakout_probe_max_centroid_drop_pct=max(
            0.0,
            float(
                control.get(
                    "fake_breakout_probe_max_centroid_drop_pct",
                    args.fake_breakout_probe_max_centroid_drop_pct,
                )
            ),
        ),
        fake_breakout_probe_alert_cooldown_sec=max(
            60.0,
            float(
                control.get(
                    "fake_breakout_probe_alert_cooldown_sec",
                    args.fake_breakout_probe_alert_cooldown_sec,
                )
            ),
        ),
        fake_breakout_probe_push_min_hits=max(
            1,
            int(control.get("fake_breakout_probe_push_min_hits", args.fake_breakout_probe_push_min_hits)),
        ),
        fake_breakout_probe_push_max_ret_1m_pct=max(
            0.0,
            float(
                control.get(
                    "fake_breakout_probe_push_max_ret_1m_pct",
                    args.fake_breakout_probe_push_max_ret_1m_pct,
                )
            ),
        ),
        fake_breakout_probe_push_max_centroid_change_pct=float(
            control.get(
                "fake_breakout_probe_push_max_centroid_change_pct",
                args.fake_breakout_probe_push_max_centroid_change_pct,
            )
        ),
        fake_breakout_probe_bonus=max(
            0.0,
            float(control.get("fake_breakout_probe_bonus", args.fake_breakout_probe_bonus)),
        ),
        fake_breakout_probe_alpha_bonus=max(
            0.0,
            float(control.get("fake_breakout_probe_alpha_bonus", args.fake_breakout_probe_alpha_bonus)),
        ),
        fake_breakout_probe_tier3_realtime=bool(
            control.get("fake_breakout_probe_tier3_realtime", args.fake_breakout_probe_tier3_realtime)
        ),
        probe_follow_breakout_window_sec=max(
            60.0,
            float(control.get("probe_follow_breakout_window_sec", args.probe_follow_breakout_window_sec)),
        ),
        probe_follow_whale_bonus=max(
            0.0,
            float(control.get("probe_follow_whale_bonus", args.probe_follow_whale_bonus)),
        ),
        probe_follow_waive_fake_breakout_once=bool(
            control.get("probe_follow_waive_fake_breakout_once", args.probe_follow_waive_fake_breakout_once)
        ),
        module_signal_cooldown_sec=max(
            60.0,
            float(control.get("module_signal_cooldown_sec", args.module_signal_cooldown_sec)),
        ),
        whale_testing_enabled=bool(control.get("whale_testing_enabled", args.whale_testing_enabled)),
        whale_testing_window_sec=max(
            300.0,
            float(control.get("whale_testing_window_sec", args.whale_testing_window_sec)),
        ),
        whale_testing_min_hits=max(
            1,
            int(control.get("whale_testing_min_hits", args.whale_testing_min_hits)),
        ),
        whale_testing_max_drawdown_pct=max(
            0.0,
            float(control.get("whale_testing_max_drawdown_pct", args.whale_testing_max_drawdown_pct)),
        ),
        whale_testing_ttl_sec=max(
            300.0,
            float(control.get("whale_testing_ttl_sec", args.whale_testing_ttl_sec)),
        ),
        whale_bonus_enabled=bool(control.get("whale_bonus_enabled", args.whale_bonus_enabled)),
        whale_bonus_score=max(
            0.0,
            float(control.get("whale_bonus_score", args.whale_bonus_score)),
        ),
        whale_waive_fake_breakout_penalty=bool(
            control.get("whale_waive_fake_breakout_penalty", args.whale_waive_fake_breakout_penalty)
        ),
        whale_waive_top_risk_penalty=bool(
            control.get("whale_waive_top_risk_penalty", args.whale_waive_top_risk_penalty)
        ),
        ignition_pump_1m=args.ignition_pump_1m,
        ignition_pump_20s=args.ignition_pump_20s,
        ignition_vol_ratio_1m=args.ignition_vol_ratio_1m,
        ignition_cvd_z=args.ignition_cvd_z,
        ignition_cvd_decay_tau=max(0.1, float(args.ignition_cvd_decay_tau)),
        ignition_oi5m_pct=args.ignition_oi5m_pct,
        ignition_min_signals=max(2, min(4, args.ignition_min_signals)),
        ignition_bear_veto_enabled=bool(args.ignition_bear_veto_enabled),
        ignition_bear_veto_cvd_z=float(args.ignition_bear_veto_cvd_z),
        ignition_bear_veto_oi5m=float(args.ignition_bear_veto_oi5m),
        ignition_3of4_blocked=bool(control.get("ignition_3of4_blocked", args.ignition_3of4_blocked)),
        ignition_min_buy_imbalance=min(
            1.0,
            max(-1.0, float(control.get("ignition_min_buy_imbalance", args.ignition_min_buy_imbalance))),
        ),
        ignition_max_fake_flags=max(
            0,
            min(10, int(control.get("ignition_max_fake_flags", args.ignition_max_fake_flags))),
        ),
        micro_cache_ttl_sec=max(1.0, args.micro_cache_ttl_sec),
        ignition_watch_ttl_sec=max(10.0, args.ignition_watch_ttl_sec),
        sr_filter_enabled=bool(args.sr_filter_enabled),
        sr_lookback_5m=max(24, int(args.sr_lookback_5m)),
        sr_breakout_dist_pct=max(0.1, float(args.sr_breakout_dist_pct)),
        sr_support_dist_pct=max(0.1, float(args.sr_support_dist_pct)),
        sr_max_compression_pct=max(0.5, float(args.sr_max_compression_pct)),
        sr_soft_pass_enabled=bool(args.sr_soft_pass_enabled),
        sr_soft_min_hit=max(2, min(4, int(args.sr_soft_min_hit))),
        sr_soft_min_oi5m_pct=float(args.sr_soft_min_oi5m_pct),
        cross_filter_enabled=bool(args.cross_filter_enabled),
        cross_ret_1m_pct=float(args.cross_ret_1m_pct),
        cross_vol_ratio_1m=max(0.1, float(args.cross_vol_ratio_1m)),
        cross_grace_no_data=bool(args.cross_grace_no_data),
        flow_filter_enabled=bool(args.flow_filter_enabled),
        flow_buy_ratio_min=min(1.0, max(0.0, float(args.flow_buy_ratio_min))),
        flow_imbalance_min=min(1.0, max(-1.0, float(args.flow_imbalance_min))),
        flow_lsr_5m_min_pct=float(args.flow_lsr_5m_min_pct),
        flow_min_signals=max(1, min(3, int(args.flow_min_signals))),
        flow_grace_no_data=bool(args.flow_grace_no_data),
        flow_relax_enabled=bool(args.flow_relax_enabled),
        flow_relax_ret_1m_pct=float(args.flow_relax_ret_1m_pct),
        flow_relax_oi5m_pct=float(args.flow_relax_oi5m_pct),
        flow_relax_min_signals=max(1, min(3, int(args.flow_relax_min_signals))),
        onchain_filter_enabled=bool(args.onchain_filter_enabled),
        onchain_symbols=_parse_symbols(args.onchain_symbols),
        onchain_targets_file=str(args.onchain_targets_file or "").strip(),
        onchain_targets_reload_sec=max(1.0, float(args.onchain_targets_reload_sec)),
        onchain_sector_file=str(args.onchain_sector_file or "").strip(),
        onchain_sector_reload_sec=max(1.0, float(args.onchain_sector_reload_sec)),
        onchain_ret_5m_pct=float(args.onchain_ret_5m_pct),
        onchain_vol_ratio_5m=max(0.1, float(args.onchain_vol_ratio_5m)),
        onchain_min_signals=max(1, min(2, int(args.onchain_min_signals))),
        onchain_grace_no_data=bool(args.onchain_grace_no_data),
        onchain_auto_add_enabled=bool(args.onchain_auto_add_enabled),
        onchain_auto_add_vol_ratio_1m=max(0.1, float(args.onchain_auto_add_vol_ratio_1m)),
        onchain_auto_add_min_volume_1m=max(0.0, float(args.onchain_auto_add_min_volume_1m)),
        onchain_auto_add_ttl_sec=max(60.0, float(args.onchain_auto_add_ttl_sec)),
        onchain_auto_add_discord_enabled=bool(args.onchain_auto_add_discord_enabled),
        onchain_auto_add_discord_batch_window_sec=max(0.0, float(args.onchain_auto_add_discord_batch_window_sec)),
        onchain_auto_add_discord_max_rows=max(1, int(args.onchain_auto_add_discord_max_rows)),
        onchain_sector_auto_expand_enabled=bool(args.onchain_sector_auto_expand_enabled),
        onchain_sector_expand_ttl_sec=max(60.0, float(args.onchain_sector_expand_ttl_sec)),
        onchain_sector_min_quote_volume_24h=max(0.0, float(args.onchain_sector_min_quote_volume_24h)),
        onchain_sector_max_peers_per_seed=max(0, int(args.onchain_sector_max_peers_per_seed)),
        onchain_priority_enabled=bool(args.onchain_priority_enabled),
        onchain_high_worker_cooldown_sec=max(0.0, float(args.onchain_high_worker_cooldown_sec)),
        onchain_discovery_ttl_sec=max(60.0, float(args.onchain_discovery_ttl_sec)),
        onchain_micro_ttl_sec=max(5.0, float(args.onchain_micro_ttl_sec)),
        onchain_min_liquidity_usd=max(1000.0, float(args.onchain_min_liquidity_usd)),
        queue_symbol_cooldown_sec=max(1.0, args.queue_symbol_cooldown_sec),
        worker_cooldown_sec=max(0.0, args.worker_cooldown_sec),
        worker_healthcheck_enabled=bool(args.worker_healthcheck_enabled),
        worker_healthcheck_interval_sec=max(2.0, float(args.worker_healthcheck_interval_sec)),
        worker_healthcheck_stall_sec=max(10.0, float(args.worker_healthcheck_stall_sec)),
        worker_healthcheck_min_triggers=max(1, int(args.worker_healthcheck_min_triggers)),
        alert_cooldown_sec=max(1.0, args.alert_cooldown_sec),
        alert_cooldown_low_sec=max(1.0, args.alert_cooldown_low_sec),
        discord_min_score=float(control.get("discord_min_score", args.discord_min_score)),
        discord_watch_min_hit=max(1, int(control.get("discord_watch_min_hit", args.discord_watch_min_hit))),
        discord_min_interval_sec=max(0.0, float(args.discord_min_interval_sec)),
        discord_retry_max=max(1, int(args.discord_retry_max)),
        disable_three_layer_filter=bool(control.get("disable_three_layer_filter", args.disable_three_layer_filter)),
        discord_heartbeat_enabled=bool(args.discord_heartbeat_enabled),
        discord_heartbeat_interval_sec=max(60.0, float(args.discord_heartbeat_interval_sec)),
        discord_heartbeat_content=str(args.discord_heartbeat_content or "").strip() or "初号机运行中",
        discord_module_stats_enabled=bool(
            control.get("discord_module_stats_enabled", args.discord_module_stats_enabled)
        ),
        discord_module_stats_interval_sec=max(
            300.0,
            float(control.get("discord_module_stats_interval_sec", args.discord_module_stats_interval_sec)),
        ),
        discord_module_stats_window_sec=max(
            3600.0,
            float(control.get("discord_module_stats_window_sec", args.discord_module_stats_window_sec)),
        ),
        discord_module_stats_top_n=max(
            0,
            int(control.get("discord_module_stats_top_n", args.discord_module_stats_top_n)),
        ),
        discord_webhook_url=resolved_monitor_ws_webhook,
        aster_discord_webhook_url=str(
            control.get("aster_discord_webhook_url", args.aster_discord_webhook_url) or ""
        ).strip(),
        discord_username=str(args.discord_username or "Futures Monitor").strip() or "Futures Monitor",
        control_file=str(args.control_file or "").strip() or "config/monitor_ws_control.json",
        webhook_config_file=webhook_config_file,
        discord_archive_enabled=bool(control.get("discord_archive_enabled", args.discord_archive_enabled)),
        discord_archive_file=str(control.get("discord_archive_file", args.discord_archive_file) or "").strip()
        or "data/logs/monitor_ws_discord_push.jsonl",
        signal_db_enabled=bool(control.get("signal_db_enabled", args.signal_db_enabled)),
        signal_db_file=str(control.get("signal_db_file", args.signal_db_file) or "").strip()
        or "data/logs/monitor_ws_signals.db",
        aster_signal_db_file=str(control.get("aster_signal_db_file", args.aster_signal_db_file) or "").strip()
        or "data/logs/monitor_ws_signals_aster.db",
        decision_tier_enabled=bool(control.get("decision_tier_enabled", args.decision_tier_enabled)),
        tier1_score=max(0.0, float(control.get("tier1_score", args.tier1_score))),
        tier2_score=max(0.0, float(control.get("tier2_score", args.tier2_score))),
        tier3_score=max(0.0, float(control.get("tier3_score", args.tier3_score))),
        tier2_confirm_k_sec=max(10.0, float(control.get("tier2_confirm_k_sec", args.tier2_confirm_k_sec))),
        tier2_confirm_pullback_pct=max(
            0.0,
            float(control.get("tier2_confirm_pullback_pct", args.tier2_confirm_pullback_pct)),
        ),
        tier2_confirm_min_checks=max(
            1,
            min(4, int(control.get("tier2_confirm_min_checks", args.tier2_confirm_min_checks))),
        ),
        tier3_discord_push_enabled=bool(control.get("tier3_discord_push_enabled", args.tier3_discord_push_enabled)),
        sniper_risk_downgrade_enabled=bool(
            control.get("sniper_risk_downgrade_enabled", args.sniper_risk_downgrade_enabled)
        ),
        sniper_risk_downgrade_penalty=max(
            0.0,
            float(control.get("sniper_risk_downgrade_penalty", args.sniper_risk_downgrade_penalty)),
        ),
        sniper_risk_drop_on_heavy_fake_breakout=bool(
            control.get(
                "sniper_risk_drop_on_heavy_fake_breakout",
                args.sniper_risk_drop_on_heavy_fake_breakout,
            )
        ),
        sniper_risk_drop_penalty=max(
            0.0,
            float(control.get("sniper_risk_drop_penalty", args.sniper_risk_drop_penalty)),
        ),
        tier_decision_file=str(control.get("tier_decision_file", args.tier_decision_file) or "").strip()
        or "data/logs/monitor_ws_tier_decisions.jsonl",
        auto_tune_enabled=bool(control.get("auto_tune_enabled", args.auto_tune_enabled)),
        auto_tune_apply_enabled=bool(control.get("auto_tune_apply_enabled", args.auto_tune_apply_enabled)),
        auto_tune_persist_control=bool(control.get("auto_tune_persist_control", args.auto_tune_persist_control)),
        auto_tune_interval_sec=max(10.0, float(control.get("auto_tune_interval_sec", args.auto_tune_interval_sec))),
        auto_tune_window_rows=max(30, int(control.get("auto_tune_window_rows", args.auto_tune_window_rows))),
        auto_tune_min_samples=max(5, int(control.get("auto_tune_min_samples", args.auto_tune_min_samples))),
        auto_tune_cooldown_sec=max(30.0, float(control.get("auto_tune_cooldown_sec", args.auto_tune_cooldown_sec))),
        auto_tune_win_rate_high=min(1.0, max(0.0, float(control.get("auto_tune_win_rate_high", args.auto_tune_win_rate_high)))),
        auto_tune_win_rate_low=min(1.0, max(0.0, float(control.get("auto_tune_win_rate_low", args.auto_tune_win_rate_low)))),
        auto_tune_tier_step_up=max(0.0, float(control.get("auto_tune_tier_step_up", args.auto_tune_tier_step_up))),
        auto_tune_tier_step_down=max(0.0, float(control.get("auto_tune_tier_step_down", args.auto_tune_tier_step_down))),
        auto_tune_tier2_min=max(0.0, float(control.get("auto_tune_tier2_min", args.auto_tune_tier2_min))),
        auto_tune_tier2_max=max(0.0, float(control.get("auto_tune_tier2_max", args.auto_tune_tier2_max))),
        auto_tune_log_file=str(control.get("auto_tune_log_file", args.auto_tune_log_file) or "").strip()
        or "data/logs/monitor_ws_auto_tune.jsonl",
        symbol_quality_enabled=bool(control.get("symbol_quality_enabled", args.symbol_quality_enabled)),
        symbol_quality_window_rows=max(
            30,
            int(control.get("symbol_quality_window_rows", args.symbol_quality_window_rows)),
        ),
        symbol_quality_min_samples=max(
            5,
            int(control.get("symbol_quality_min_samples", args.symbol_quality_min_samples)),
        ),
        symbol_quality_alpha=max(0.1, float(control.get("symbol_quality_alpha", args.symbol_quality_alpha))),
        symbol_quality_beta=max(0.1, float(control.get("symbol_quality_beta", args.symbol_quality_beta))),
        symbol_quality_floor=max(0.1, float(control.get("symbol_quality_floor", args.symbol_quality_floor))),
        symbol_quality_cap=max(0.1, float(control.get("symbol_quality_cap", args.symbol_quality_cap))),
        sector_cluster_enabled=bool(control.get("sector_cluster_enabled", args.sector_cluster_enabled)),
        sector_cluster_window_sec=max(
            10.0,
            float(control.get("sector_cluster_window_sec", args.sector_cluster_window_sec)),
        ),
        sector_cluster_min_count=max(2, int(control.get("sector_cluster_min_count", args.sector_cluster_min_count))),
        sector_cluster_boost_factor=max(
            1.0,
            float(control.get("sector_cluster_boost_factor", args.sector_cluster_boost_factor)),
        ),
        sector_cluster_ttl_sec=max(30.0, float(control.get("sector_cluster_ttl_sec", args.sector_cluster_ttl_sec))),
        sector_cluster_min_base_score=max(
            0.0,
            float(control.get("sector_cluster_min_base_score", args.sector_cluster_min_base_score)),
        ),
        regime_pattern_enabled=bool(control.get("regime_pattern_enabled", args.regime_pattern_enabled)),
        regime_window_rows=max(30, int(control.get("regime_window_rows", args.regime_window_rows))),
        regime_min_samples=max(5, int(control.get("regime_min_samples", args.regime_min_samples))),
        regime_win_rate_high=min(1.0, max(0.0, float(control.get("regime_win_rate_high", args.regime_win_rate_high)))),
        regime_win_rate_low=min(1.0, max(0.0, float(control.get("regime_win_rate_low", args.regime_win_rate_low)))),
        regime_factor_boost=max(0.5, float(control.get("regime_factor_boost", args.regime_factor_boost))),
        regime_factor_cut=max(0.5, float(control.get("regime_factor_cut", args.regime_factor_cut))),
        regime_morning_factor=max(0.5, float(control.get("regime_morning_factor", args.regime_morning_factor))),
        regime_friday_night_factor=max(
            0.5,
            float(control.get("regime_friday_night_factor", args.regime_friday_night_factor)),
        ),
        sentiment_enabled=bool(control.get("sentiment_enabled", args.sentiment_enabled)),
        sentiment_whale_notional_usd=max(
            1.0,
            float(control.get("sentiment_whale_notional_usd", args.sentiment_whale_notional_usd)),
        ),
        sentiment_lsr_weight=max(0.0, float(control.get("sentiment_lsr_weight", args.sentiment_lsr_weight))),
        sentiment_whale_weight=max(0.0, float(control.get("sentiment_whale_weight", args.sentiment_whale_weight))),
        sentiment_concentration_weight=max(
            0.0,
            float(control.get("sentiment_concentration_weight", args.sentiment_concentration_weight)),
        ),
        sentiment_funding_div_weight=max(
            0.0,
            float(control.get("sentiment_funding_div_weight", args.sentiment_funding_div_weight)),
        ),
        sentiment_funding_div_streak_warn=max(
            1,
            int(control.get("sentiment_funding_div_streak_warn", args.sentiment_funding_div_streak_warn)),
        ),
        symbol_adapt_enabled=bool(control.get("symbol_adapt_enabled", args.symbol_adapt_enabled)),
        symbol_liq_high_24h=max(0.0, float(control.get("symbol_liq_high_24h", args.symbol_liq_high_24h))),
        symbol_liq_mid_24h=max(0.0, float(control.get("symbol_liq_mid_24h", args.symbol_liq_mid_24h))),
        symbol_new_listing_days=max(
            1,
            int(control.get("symbol_new_listing_days", args.symbol_new_listing_days)),
        ),
        mtf_resonance_enabled=bool(control.get("mtf_resonance_enabled", args.mtf_resonance_enabled)),
        risk_liquidity_enabled=bool(control.get("risk_liquidity_enabled", args.risk_liquidity_enabled)),
        risk_slippage_enabled=bool(control.get("risk_slippage_enabled", args.risk_slippage_enabled)),
        risk_cross_anomaly_enabled=bool(control.get("risk_cross_anomaly_enabled", args.risk_cross_anomaly_enabled)),
        risk_spread_bps_max=max(0.0, float(control.get("risk_spread_bps_max", args.risk_spread_bps_max))),
        risk_depth_min_usd=max(0.0, float(control.get("risk_depth_min_usd", args.risk_depth_min_usd))),
        risk_slippage_notional_usd=max(
            0.0,
            float(control.get("risk_slippage_notional_usd", args.risk_slippage_notional_usd)),
        ),
        risk_slippage_max_pct=max(0.0, float(control.get("risk_slippage_max_pct", args.risk_slippage_max_pct))),
        risk_cross_anomaly_pct=max(
            0.0,
            float(control.get("risk_cross_anomaly_pct", args.risk_cross_anomaly_pct)),
        ),
        risk_penalty_liquidity=max(
            0.0,
            float(control.get("risk_penalty_liquidity", args.risk_penalty_liquidity)),
        ),
        risk_penalty_slippage=max(
            0.0,
            float(control.get("risk_penalty_slippage", args.risk_penalty_slippage)),
        ),
        risk_penalty_cross_anomaly=max(
            0.0,
            float(control.get("risk_penalty_cross_anomaly", args.risk_penalty_cross_anomaly)),
        ),
        risk_penalty_fake_breakout=max(
            0.0,
            float(control.get("risk_penalty_fake_breakout", args.risk_penalty_fake_breakout)),
        ),
        risk_penalty_iceberg=max(0.0, float(control.get("risk_penalty_iceberg", args.risk_penalty_iceberg))),
        bayesian_scorer_enabled=bool(control.get("bayesian_scorer_enabled", args.bayesian_scorer_enabled)),
        bayesian_prior_max=min(
            0.99,
            max(0.01, float(control.get("bayesian_prior_max", args.bayesian_prior_max))),
        ),
        bayesian_prior_min=min(
            0.99,
            max(0.01, float(control.get("bayesian_prior_min", args.bayesian_prior_min))),
        ),
        bayesian_prior_decay_hits=max(
            1.0,
            float(control.get("bayesian_prior_decay_hits", args.bayesian_prior_decay_hits)),
        ),
        bayesian_evidence_a_pump20s_pct=float(
            control.get("bayesian_evidence_a_pump20s_pct", args.bayesian_evidence_a_pump20s_pct)
        ),
        bayesian_evidence_b_funding_rate_lt=float(
            control.get("bayesian_evidence_b_funding_rate_lt", args.bayesian_evidence_b_funding_rate_lt)
        ),
        bayesian_evidence_b_funding_delta_lt=float(
            control.get("bayesian_evidence_b_funding_delta_lt", args.bayesian_evidence_b_funding_delta_lt)
        ),
        bayesian_evidence_c_oi5m_pct_le=float(
            control.get("bayesian_evidence_c_oi5m_pct_le", args.bayesian_evidence_c_oi5m_pct_le)
        ),
        bayesian_p_ea_given_h=min(
            0.999,
            max(0.001, float(control.get("bayesian_p_ea_given_h", args.bayesian_p_ea_given_h))),
        ),
        bayesian_p_eb_given_h=min(
            0.999,
            max(0.001, float(control.get("bayesian_p_eb_given_h", args.bayesian_p_eb_given_h))),
        ),
        bayesian_p_ec_given_h=min(
            0.999,
            max(0.001, float(control.get("bayesian_p_ec_given_h", args.bayesian_p_ec_given_h))),
        ),
        bayesian_p_ea_given_not_h=min(
            0.999,
            max(0.001, float(control.get("bayesian_p_ea_given_not_h", args.bayesian_p_ea_given_not_h))),
        ),
        bayesian_p_eb_given_not_h=min(
            0.999,
            max(0.001, float(control.get("bayesian_p_eb_given_not_h", args.bayesian_p_eb_given_not_h))),
        ),
        bayesian_p_ec_given_not_h=min(
            0.999,
            max(0.001, float(control.get("bayesian_p_ec_given_not_h", args.bayesian_p_ec_given_not_h))),
        ),
        bayesian_override_posterior_threshold=min(
            0.999,
            max(
                0.0,
                float(
                    control.get(
                        "bayesian_override_posterior_threshold",
                        args.bayesian_override_posterior_threshold,
                    )
                ),
            ),
        ),
        bayesian_score_multiplier=max(
            1.0,
            float(control.get("bayesian_score_multiplier", args.bayesian_score_multiplier)),
        ),
        bayesian_block_fake_weak_oi_penalty=bool(
            control.get("bayesian_block_fake_weak_oi_penalty", args.bayesian_block_fake_weak_oi_penalty)
        ),
        bayesian_block_funding_divergence_penalty=bool(
            control.get(
                "bayesian_block_funding_divergence_penalty",
                args.bayesian_block_funding_divergence_penalty,
            )
        ),
        bayesian_force_sniper_enabled=bool(
            control.get("bayesian_force_sniper_enabled", args.bayesian_force_sniper_enabled)
        ),
        bayesian_reason_override=str(
            control.get("bayesian_reason_override", args.bayesian_reason_override) or ""
        ).strip()
        or "short_squeeze_ignition",
        ml_shadow_enabled=bool(control.get("ml_shadow_enabled", args.ml_shadow_enabled)),
        ml_shadow_downgrade_prob=min(
            1.0,
            max(0.0, float(control.get("ml_shadow_downgrade_prob", args.ml_shadow_downgrade_prob))),
        ),
        ml_shadow_upgrade_prob=min(
            1.0,
            max(0.0, float(control.get("ml_shadow_upgrade_prob", args.ml_shadow_upgrade_prob))),
        ),
        ml_shadow_penalty=max(0.0, float(control.get("ml_shadow_penalty", args.ml_shadow_penalty))),
        ml_shadow_bonus=max(0.0, float(control.get("ml_shadow_bonus", args.ml_shadow_bonus))),
        mod_trend_seg_enabled=bool(control.get("mod_trend_seg_enabled", args.mod_trend_seg_enabled)),
        mod_combo_enabled=bool(control.get("mod_combo_enabled", args.mod_combo_enabled)),
        mod_cvd_structure_enabled=bool(control.get("mod_cvd_structure_enabled", args.mod_cvd_structure_enabled)),
        mod_aux_indicators_enabled=bool(control.get("mod_aux_indicators_enabled", args.mod_aux_indicators_enabled)),
        mod_flow_mtf_enabled=bool(control.get("mod_flow_mtf_enabled", args.mod_flow_mtf_enabled)),
        mod_scorecard_enabled=bool(control.get("mod_scorecard_enabled", args.mod_scorecard_enabled)),
        mod_contradiction_enabled=bool(control.get("mod_contradiction_enabled", args.mod_contradiction_enabled)),
        mod_alert_digest_enabled=bool(control.get("mod_alert_digest_enabled", args.mod_alert_digest_enabled)),
        mod_backtest_metrics_enabled=bool(control.get("mod_backtest_metrics_enabled", args.mod_backtest_metrics_enabled)),
        mod_trade_plan_enabled=bool(control.get("mod_trade_plan_enabled", args.mod_trade_plan_enabled)),
    )
    print(f"[ws-monitor] strategy_version={cfg.strategy_version}")
    print(
        "[ws-monitor] ingress_audit "
        f"enabled={'true' if cfg.ingress_audit_enabled else 'false'} "
        f"force_symbols={','.join(cfg.ingress_force_symbols) if cfg.ingress_force_symbols else '-'} "
        f"low_ratio={cfg.ingress_audit_low_ratio:.2f} "
        f"vol_spike_ratio={cfg.ingress_audit_volume_spike_ratio:.2f}"
    )
    if cfg.aster_ws_enabled:
        print(
            "[ws-monitor] aster_ws_enabled=true "
            f"url={cfg.aster_ws_url} reload_sec={cfg.aster_symbols_reload_sec:.0f}"
        )
        if cfg.aster_discord_webhook_url:
            print(f"[ws-monitor] aster_discord_webhook_url={_mask_webhook(cfg.aster_discord_webhook_url)}")
    if cfg.signal_db_enabled:
        print(
            "[ws-monitor] signal_db_split "
            f"binance={cfg.signal_db_file} aster={cfg.aster_signal_db_file}"
        )
    analyzer = _build_default_analyzer(Path(args.data_dir), args.mode, cfg)
    try:
        exchange = await get_exchange()
    except Exception as exc:
        # Deep analyzer is REST-only today, so keep monitor alive even if ccxt init fails.
        print(f"[ws-monitor] get_exchange failed, fallback REST-only mode: {exc!r}")
        exchange = None
    monitor = WSMonitor(cfg, analyzer, exchange)
    try:
        await monitor.run_forever()
    finally:
        await close_exchange(exchange)


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
