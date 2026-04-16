#!/usr/bin/env python3
"""Perpetual explosive coin monitor based on 1m volume/price breakout radar."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import aiohttp
import pandas as pd

try:
    import ccxt.pro as ccxtpro  # type: ignore
except Exception as exc:  # pragma: no cover
    ccxtpro = None  # type: ignore[assignment]
    _CCXTPRO_IMPORT_ERROR = exc
else:
    _CCXTPRO_IMPORT_ERROR = None


BINANCE_FAPI_24H_TICKER_URL = "https://fapi.binance.com/fapi/v1/ticker/24hr"
BINANCE_FAPI_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_FAPI_PREMIUM_INDEX_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
BINANCE_FAPI_OPEN_INTEREST_HIST_URL = "https://fapi.binance.com/futures/data/openInterestHist"
BINANCE_FAPI_GLOBAL_LSR_URL = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
GECKOTERMINAL_POOL_OHLCV_TMPL = "https://api.geckoterminal.com/api/v2/networks/{network}/pools/{pool}/ohlcv/minute"
ALTERNATIVE_ME_FNG_URL = "https://api.alternative.me/fng/"
LEVERAGED_SUFFIXES: Tuple[str, ...] = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")
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
class RadarConfig:
    discord_webhook_cex: str = ""
    discord_webhook_exit: str = ""
    # v1.1 defaults: easier to trigger while still requiring multi-signal confirmation.
    vol_surge_threshold: float = 3.0
    score_threshold: float = 45.0
    gain_threshold: float = 15.0
    max_drawdown_before_breakout: float = -8.0
    trailing_stop_percent: float = 15.0
    min_24h_volume_usdt: float = 300_000.0
    max_symbols: int = 300
    websocket_concurrency: int = 15
    monitor_interval_sec: float = 3.0
    funding_threshold_rate: float = 0.0
    oi_surge_threshold: float = 1.2
    oi_hist_period: str = "5m"
    oi_hist_limit: int = 12
    whale_funding_reversal_rate: float = 0.0
    whale_oi_drop_ratio: float = 0.7
    onchain_filter_enabled: bool = True
    onchain_ret_5m_pct: float = 0.25
    onchain_vol_ratio_5m: float = 1.8
    onchain_txn_m5_min: float = 20.0
    onchain_buy_sell_ratio_min: float = 1.05
    onchain_min_signals: int = 1
    onchain_min_liquidity_usd: float = 20_000.0
    onchain_discovery_ttl_sec: float = 1800.0
    onchain_micro_ttl_sec: float = 20.0
    onchain_grace_no_data: bool = True
    sentiment_filter_enabled: bool = True
    sentiment_lsr_5m_min_pct: float = 0.30
    sentiment_fng_max: float = 85.0
    sentiment_min_signals: int = 1
    sentiment_ttl_sec: float = 20.0
    sentiment_fng_ttl_sec: float = 300.0
    sentiment_grace_no_data: bool = True
    reconnect_base_sec: float = 1.0
    reconnect_max_sec: float = 8.0
    request_timeout_sec: float = 10.0
    dry_run: bool = False


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _env_float(name: str, default: float) -> float:
    try:
        raw = os.getenv(name, str(default)).strip()
        return float(raw)
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        raw = os.getenv(name, str(default)).strip()
        return int(raw)
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_webhook(v: Any) -> str:
    return str(v or "").strip()


def _mask_webhook(v: str) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if len(s) <= 40:
        return "***"
    return f"{s[:34]}...{s[-6:]}"


def _load_webhook_overrides(path_s: str) -> Dict[str, Any]:
    path_text = str(path_s or "").strip()
    if not path_text:
        return {}
    path = Path(path_text).expanduser()
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"webhook config parse failed: path={path} err={exc!r}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f"webhook config must be a JSON object: path={path}")
    return raw


def _assert_webhook_match(
    field_name: str,
    file_value: str,
    runtime_value: str,
    webhook_file: str,
) -> None:
    if file_value and runtime_value and file_value != runtime_value:
        raise RuntimeError(
            f"startup rejected: {field_name} mismatch between webhook config and cli/env. "
            f"file={webhook_file} file_value={_mask_webhook(file_value)} "
            f"runtime_value={_mask_webhook(runtime_value)}"
        )


def _resolve_radar_webhooks(args: argparse.Namespace) -> Tuple[str, str]:
    webhook_file = str(getattr(args, "webhook_config_file", "") or "").strip()
    overrides = _load_webhook_overrides(webhook_file)

    file_cex = _normalize_webhook(
        overrides.get("discord_webhook_cex", overrides.get("discordWebhookCex", ""))
    )
    file_exit = _normalize_webhook(
        overrides.get("discord_webhook_exit", overrides.get("discordWebhookExit", ""))
    )
    runtime_cex = _normalize_webhook(getattr(args, "discord_webhook_cex", ""))
    runtime_exit = _normalize_webhook(getattr(args, "discord_webhook_exit", ""))

    _assert_webhook_match("discord_webhook_cex", file_cex, runtime_cex, webhook_file)
    _assert_webhook_match("discord_webhook_exit", file_exit, runtime_exit, webhook_file)

    return (file_cex or runtime_cex, file_exit or runtime_exit)


def _parse_bool_arg(v: str) -> bool:
    s = str(v or "").strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid bool value: {v}")


def _parse_symbols(raw: str) -> List[str]:
    out: List[str] = []
    for token in str(raw or "").split(","):
        symbol = token.strip().upper()
        if not symbol:
            continue
        if "/" in symbol:
            base = symbol.split("/")[0].strip().upper()
            if base:
                symbol = f"{base}/USDT:USDT"
        elif symbol.endswith("USDT"):
            symbol = f"{symbol[:-4]}/USDT:USDT"
        out.append(symbol)
    return list(dict.fromkeys(out))


def _to_raw_usdt_symbol(ccxt_symbol: str) -> str:
    symbol = str(ccxt_symbol or "").upper().strip()
    return symbol.replace("/", "").replace(":USDT", "")


def _score_breakout(vol_surge: float, gain: float, dd: float) -> float:
    # Keep the original weighted blend with mild capping for stability.
    return (
        0.40 * min(vol_surge, 50.0)
        + 0.35 * max(gain, 0.0)
        + 0.25 * (100.0 + max(dd, -20.0))
    )


async def send_discord_alert(
    session: aiohttp.ClientSession,
    webhook_url: str,
    message: str,
) -> None:
    if not webhook_url or not webhook_url.startswith("http"):
        return
    try:
        async with session.post(webhook_url, json={"content": message}) as resp:
            if resp.status >= 300:
                text = await resp.text()
                print(f"[{_now()}] discord push failed: status={resp.status} body={text[:200]}")
    except Exception as exc:
        print(f"[{_now()}] discord push exception: {exc!r}")


async def fetch_active_usdt_pairs(
    session: aiohttp.ClientSession,
    min_24h_volume_usdt: float,
    max_symbols: int,
) -> List[str]:
    symbols_with_volume: List[Tuple[float, str]] = []
    try:
        async with session.get(BINANCE_FAPI_EXCHANGE_INFO_URL) as resp_exchange_info:
            if resp_exchange_info.status != 200:
                print(f"[{_now()}] failed to fetch exchangeInfo: status={resp_exchange_info.status}")
                return []
            exchange_info = await resp_exchange_info.json()

        async with session.get(BINANCE_FAPI_24H_TICKER_URL) as resp_ticker:
            if resp_ticker.status != 200:
                print(f"[{_now()}] failed to fetch futures 24h ticker: status={resp_ticker.status}")
                return []
            data = await resp_ticker.json()
    except Exception as exc:
        print(f"[{_now()}] failed to fetch perpetual symbols: {exc!r}")
        return []

    tradable_perp_symbols: set[str] = set()
    rows = exchange_info.get("symbols", []) if isinstance(exchange_info, dict) else []
    if isinstance(rows, list):
        for item in rows:
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol", "")).upper()
            if not symbol.endswith("USDT"):
                continue
            if str(item.get("quoteAsset", "")).upper() != "USDT":
                continue
            if str(item.get("contractType", "")).upper() != "PERPETUAL":
                continue
            if str(item.get("status", "")).upper() != "TRADING":
                continue
            if any(symbol.endswith(suffix) for suffix in LEVERAGED_SUFFIXES):
                continue
            tradable_perp_symbols.add(symbol)

    if not isinstance(data, list):
        return []

    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).upper()
        if symbol not in tradable_perp_symbols:
            continue
        try:
            quote_volume = float(item.get("quoteVolume", 0.0) or 0.0)
        except Exception:
            continue
        if quote_volume < min_24h_volume_usdt:
            continue
        symbols_with_volume.append((quote_volume, f"{symbol[:-4]}/USDT:USDT"))

    symbols_with_volume.sort(key=lambda x: x[0], reverse=True)
    if max_symbols > 0:
        symbols_with_volume = symbols_with_volume[:max_symbols]
    return [symbol for _, symbol in symbols_with_volume]


async def fetch_funding_oi_snapshot(
    session: aiohttp.ClientSession,
    raw_symbol: str,
    oi_hist_period: str,
    oi_hist_limit: int,
) -> Optional[Dict[str, float]]:
    try:
        async with session.get(BINANCE_FAPI_PREMIUM_INDEX_URL, params={"symbol": raw_symbol}) as resp_funding:
            if resp_funding.status != 200:
                return None
            funding_data = await resp_funding.json()

        async with session.get(
            BINANCE_FAPI_OPEN_INTEREST_HIST_URL,
            params={"symbol": raw_symbol, "period": oi_hist_period, "limit": oi_hist_limit},
        ) as resp_oi:
            if resp_oi.status != 200:
                return None
            oi_data = await resp_oi.json()
    except Exception:
        return None

    try:
        funding_rate = float(funding_data.get("lastFundingRate", 0.0) or 0.0)
    except Exception:
        funding_rate = 0.0

    oi_values: List[float] = []
    if isinstance(oi_data, list):
        for row in oi_data:
            if not isinstance(row, dict):
                continue
            try:
                # Value-based OI is more comparable across symbols.
                oi_value = float(row.get("sumOpenInterestValue", 0.0) or 0.0)
            except Exception:
                continue
            if oi_value > 0.0:
                oi_values.append(oi_value)

    if len(oi_values) < 2:
        return {
            "funding_rate": funding_rate,
            "oi_latest": oi_values[-1] if oi_values else 0.0,
            "oi_ratio": 1.0,
        }

    oi_latest = oi_values[-1]
    oi_prev = oi_values[:-1]
    oi_prev_mean = sum(oi_prev) / max(1, len(oi_prev))
    oi_ratio = oi_latest / oi_prev_mean if oi_prev_mean > 0.0 else 1.0

    return {
        "funding_rate": funding_rate,
        "oi_latest": oi_latest,
        "oi_ratio": oi_ratio,
    }


async def _close_exchange_safely(exchange: Any) -> None:
    if exchange is None:
        return
    try:
        await exchange.close()
    except Exception as exc:
        print(f"[{_now()}] exchange close failed: {exc!r}")

    # Some network failures can leave an aiohttp session dangling in ccxt.
    session = getattr(exchange, "session", None)
    if session is not None:
        try:
            if not session.closed:
                await session.close()
        except Exception:
            pass


class ExplosiveSpotRadar:
    def __init__(self, config: RadarConfig) -> None:
        self.config = config
        self.active_positions: Dict[str, Dict[str, float]] = {}
        self.position_lock = asyncio.Lock()
        self.scan_semaphore = asyncio.Semaphore(max(1, int(config.websocket_concurrency)))
        self.stop_event = asyncio.Event()
        self.last_closed_ts: Dict[str, int] = {}
        self.onchain_pool_cache: Dict[str, Tuple[float, Optional[Dict[str, Any]]]] = {}
        self.onchain_micro_cache: Dict[str, Tuple[float, Dict[str, float]]] = {}
        self.sentiment_cache: Dict[str, Tuple[float, Dict[str, float]]] = {}
        self.fng_cache_ts: float = 0.0
        self.fng_cache_value: Optional[float] = None

    @staticmethod
    def _safe_float(v: Any, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return default

    @staticmethod
    def _base_asset(raw_symbol: str) -> str:
        s = str(raw_symbol or "").upper().strip()
        return s[:-4] if s.endswith("USDT") else s

    @staticmethod
    def _fng_label(v: Optional[float]) -> str:
        if v is None:
            return "unknown"
        if v <= 25:
            return "extreme_fear"
        if v <= 45:
            return "fear"
        if v <= 55:
            return "neutral"
        if v <= 75:
            return "greed"
        return "extreme_greed"

    def _default_onchain(self) -> Dict[str, float]:
        if not self.config.onchain_filter_enabled:
            return {
                "onchain_available": 0.0,
                "onchain_ok": 1.0,
                "onchain_hit": 0.0,
                "onchain_ret_5m_pct": 0.0,
                "onchain_vol_ratio_5m": 0.0,
                "onchain_txn_m5": 0.0,
                "onchain_buy_sell_ratio_m5": 0.0,
            }
        return {
            "onchain_available": 0.0,
            "onchain_ok": 1.0 if self.config.onchain_grace_no_data else 0.0,
            "onchain_hit": 0.0,
            "onchain_ret_5m_pct": 0.0,
            "onchain_vol_ratio_5m": 0.0,
            "onchain_txn_m5": 0.0,
            "onchain_buy_sell_ratio_m5": 0.0,
        }

    def _default_sentiment(self) -> Dict[str, float]:
        if not self.config.sentiment_filter_enabled:
            return {
                "sentiment_available": 0.0,
                "sentiment_ok": 1.0,
                "sentiment_hit": 0.0,
                "sentiment_lsr_5m_chg_pct": 0.0,
                "sentiment_fng_value": -1.0,
            }
        return {
            "sentiment_available": 0.0,
            "sentiment_ok": 1.0 if self.config.sentiment_grace_no_data else 0.0,
            "sentiment_hit": 0.0,
            "sentiment_lsr_5m_chg_pct": 0.0,
            "sentiment_fng_value": -1.0,
        }

    async def _discover_onchain_pool(
        self,
        session: aiohttp.ClientSession,
        raw_symbol: str,
    ) -> Optional[Dict[str, Any]]:
        base_asset = self._base_asset(raw_symbol)
        if not base_asset:
            return None
        query = DEXSCREENER_SEARCH_URL + "?" + urllib.parse.urlencode({"q": base_asset})
        try:
            async with session.get(query) as resp:
                if resp.status != 200:
                    return None
                raw = await resp.json()
        except Exception:
            return None

        pairs = (raw or {}).get("pairs", []) if isinstance(raw, dict) else []
        if not isinstance(pairs, list):
            return None

        best: Optional[Dict[str, Any]] = None
        best_key: Tuple[int, float, float, float] = (-1, -1.0, -1.0, -1.0)
        for p in pairs:
            if not isinstance(p, dict):
                continue
            base = str((p.get("baseToken") or {}).get("symbol", "")).upper().strip()
            quote = str((p.get("quoteToken") or {}).get("symbol", "")).upper().strip()
            if base != base_asset:
                continue
            liq = self._safe_float((p.get("liquidity") or {}).get("usd", 0.0), 0.0)
            if liq < self.config.onchain_min_liquidity_usd:
                continue
            chain_id = str(p.get("chainId", "")).lower().strip()
            network = CHAIN_ID_TO_GECKO_NETWORK.get(chain_id)
            if not network:
                continue
            tx_m5 = (p.get("txns") or {}).get("m5", {}) if isinstance(p.get("txns"), dict) else {}
            buys_m5 = self._safe_float((tx_m5 or {}).get("buys", 0.0), 0.0)
            sells_m5 = self._safe_float((tx_m5 or {}).get("sells", 0.0), 0.0)
            tx_total_m5 = max(0.0, buys_m5 + sells_m5)
            vol_h24 = self._safe_float((p.get("volume") or {}).get("h24", 0.0), 0.0)
            stable_score = 1 if quote in STABLE_QUOTES else 0
            key = (stable_score, liq, vol_h24, tx_total_m5)
            if key > best_key:
                best_key = key
                best = {
                    "network": network,
                    "pool_address": str(p.get("pairAddress", "")).strip(),
                    "pair_url": str(p.get("url", "")).strip(),
                    "liquidity_usd": liq,
                    "volume_h24_usd": vol_h24,
                    "tx_buys_m5": buys_m5,
                    "tx_sells_m5": sells_m5,
                    "tx_total_m5": tx_total_m5,
                }
        return best

    async def _get_onchain_pool_cached(
        self,
        session: aiohttp.ClientSession,
        raw_symbol: str,
        ts: float,
    ) -> Optional[Dict[str, Any]]:
        cached = self.onchain_pool_cache.get(raw_symbol)
        if cached is not None:
            cached_ts, pool = cached
            if ts - cached_ts <= self.config.onchain_discovery_ttl_sec:
                return pool
        try:
            pool = await self._discover_onchain_pool(session, raw_symbol)
        except Exception:
            pool = None
        self.onchain_pool_cache[raw_symbol] = (ts, pool)
        return pool

    async def _fetch_onchain_snapshot(
        self,
        session: aiohttp.ClientSession,
        raw_symbol: str,
        ts: float,
    ) -> Dict[str, float]:
        data = self._default_onchain()
        if not self.config.onchain_filter_enabled:
            return data

        pool = await self._get_onchain_pool_cached(session, raw_symbol, ts)
        if not pool:
            return data

        tx_total_m5 = self._safe_float(pool.get("tx_total_m5", 0.0), 0.0)
        tx_buys_m5 = self._safe_float(pool.get("tx_buys_m5", 0.0), 0.0)
        tx_sells_m5 = self._safe_float(pool.get("tx_sells_m5", 0.0), 0.0)
        buy_sell_ratio = (
            tx_buys_m5 / tx_sells_m5 if tx_sells_m5 > 1e-9 else (999.0 if tx_buys_m5 > 0.0 else 0.0)
        )

        ret_5m = 0.0
        vol_ratio = 0.0
        gecko_available = 0.0
        try:
            url = GECKOTERMINAL_POOL_OHLCV_TMPL.format(
                network=pool["network"],
                pool=pool["pool_address"],
            )
            query = url + "?" + urllib.parse.urlencode({"aggregate": 5, "limit": 30})
            async with session.get(query) as resp:
                if resp.status == 200:
                    raw = await resp.json()
                    rows = (
                        (((raw or {}).get("data") or {}).get("attributes") or {}).get("ohlcv_list", [])
                        if isinstance(raw, dict)
                        else []
                    )
                    if isinstance(rows, list) and len(rows) >= 6:
                        rows = sorted(rows, key=lambda x: int(x[0]) if isinstance(x, list) and x else 0)
                        close_now = self._safe_float(rows[-1][4], 0.0)
                        close_prev = self._safe_float(rows[-2][4], 0.0)
                        if close_prev > 0.0:
                            ret_5m = (close_now - close_prev) / close_prev * 100.0
                        vols = [self._safe_float(x[5], 0.0) for x in rows[-25:] if isinstance(x, list) and len(x) >= 6]
                        if len(vols) >= 2:
                            v_last = vols[-1]
                            v_base = vols[:-1]
                            v_mean = float(statistics.mean(v_base)) if v_base else 0.0
                            if v_mean > 1e-9:
                                vol_ratio = v_last / v_mean
                        gecko_available = 1.0
        except Exception:
            pass

        c_ret = gecko_available >= 0.5 and ret_5m >= self.config.onchain_ret_5m_pct
        c_vol = gecko_available >= 0.5 and vol_ratio >= self.config.onchain_vol_ratio_5m
        c_txn = tx_total_m5 >= self.config.onchain_txn_m5_min
        c_bias = buy_sell_ratio >= self.config.onchain_buy_sell_ratio_min
        hit = int(c_ret) + int(c_vol) + int(c_txn) + int(c_bias)

        available = 1.0 if (gecko_available >= 0.5 or tx_total_m5 > 0.0) else 0.0
        required = max(1, min(4, int(self.config.onchain_min_signals)))
        ok = 1.0 if hit >= required else 0.0
        if available < 0.5 and self.config.onchain_grace_no_data:
            ok = 1.0

        data.update(
            {
                "onchain_available": available,
                "onchain_ok": ok,
                "onchain_hit": float(hit),
                "onchain_ret_5m_pct": ret_5m,
                "onchain_vol_ratio_5m": vol_ratio,
                "onchain_txn_m5": tx_total_m5,
                "onchain_buy_sell_ratio_m5": buy_sell_ratio,
            }
        )
        return data

    async def _get_onchain_snapshot_cached(
        self,
        session: aiohttp.ClientSession,
        raw_symbol: str,
        ts: float,
    ) -> Dict[str, float]:
        cached = self.onchain_micro_cache.get(raw_symbol)
        if cached is not None:
            cached_ts, data = cached
            if ts - cached_ts <= self.config.onchain_micro_ttl_sec:
                return data
        data = await self._fetch_onchain_snapshot(session, raw_symbol, ts)
        self.onchain_micro_cache[raw_symbol] = (ts, data)
        return data

    async def _get_fng_cached(self, session: aiohttp.ClientSession, ts: float) -> Optional[float]:
        if (ts - self.fng_cache_ts) <= self.config.sentiment_fng_ttl_sec and self.fng_cache_value is not None:
            return self.fng_cache_value
        try:
            async with session.get(ALTERNATIVE_ME_FNG_URL, params={"limit": 1}) as resp:
                if resp.status == 200:
                    raw = await resp.json()
                    rows = raw.get("data", []) if isinstance(raw, dict) else []
                    if isinstance(rows, list) and rows:
                        value = self._safe_float(rows[0].get("value"), -1.0)
                        if value >= 0.0:
                            self.fng_cache_ts = ts
                            self.fng_cache_value = value
                            return value
        except Exception:
            pass
        return self.fng_cache_value

    async def _fetch_sentiment_snapshot(
        self,
        session: aiohttp.ClientSession,
        raw_symbol: str,
        ts: float,
    ) -> Dict[str, float]:
        data = self._default_sentiment()
        if not self.config.sentiment_filter_enabled:
            return data

        lsr_change_5m = 0.0
        has_lsr = 0.0
        try:
            async with session.get(
                BINANCE_FAPI_GLOBAL_LSR_URL,
                params={"symbol": raw_symbol, "period": "5m", "limit": 30},
            ) as resp:
                if resp.status == 200:
                    rows = await resp.json()
                    if isinstance(rows, list) and len(rows) >= 2:
                        prev = self._safe_float(rows[-2].get("longShortRatio"), 0.0)
                        now = self._safe_float(rows[-1].get("longShortRatio"), 0.0)
                        if prev > 0.0 and now > 0.0:
                            lsr_change_5m = (now - prev) / prev * 100.0
                            has_lsr = 1.0
        except Exception:
            pass

        fng_value = await self._get_fng_cached(session, ts)

        c_lsr = has_lsr >= 0.5 and lsr_change_5m >= self.config.sentiment_lsr_5m_min_pct
        c_fng = fng_value is not None and fng_value <= self.config.sentiment_fng_max
        hit = int(c_lsr) + int(c_fng)
        available_signals = int(has_lsr >= 0.5) + int(fng_value is not None)

        required = max(1, min(2, int(self.config.sentiment_min_signals)))
        ok = 1.0 if hit >= required else 0.0
        if available_signals == 0 and self.config.sentiment_grace_no_data:
            ok = 1.0

        data.update(
            {
                "sentiment_available": 1.0 if available_signals > 0 else 0.0,
                "sentiment_ok": ok,
                "sentiment_hit": float(hit),
                "sentiment_lsr_5m_chg_pct": lsr_change_5m,
                "sentiment_fng_value": float(fng_value) if fng_value is not None else -1.0,
                "sentiment_fng_label_code": float(
                    {
                        "unknown": -1.0,
                        "extreme_fear": 0.0,
                        "fear": 1.0,
                        "neutral": 2.0,
                        "greed": 3.0,
                        "extreme_greed": 4.0,
                    }.get(self._fng_label(fng_value), -1.0)
                ),
            }
        )
        return data

    async def _get_sentiment_snapshot_cached(
        self,
        session: aiohttp.ClientSession,
        raw_symbol: str,
        ts: float,
    ) -> Dict[str, float]:
        cached = self.sentiment_cache.get(raw_symbol)
        if cached is not None:
            cached_ts, data = cached
            if ts - cached_ts <= self.config.sentiment_ttl_sec:
                return data
        data = await self._fetch_sentiment_snapshot(session, raw_symbol, ts)
        self.sentiment_cache[raw_symbol] = (ts, data)
        return data

    def _calculate_metrics(self, candles: Sequence[Sequence[float]]) -> Optional[Dict[str, float]]:
        if len(candles) < 30:
            return None
        try:
            df = pd.DataFrame(
                candles[-100:],
                columns=["ts", "open", "high", "low", "close", "volume"],
            )
        except Exception:
            return None
        if len(df) < 30:
            return None

        # Use the latest fully closed candle to avoid intra-candle noise.
        closed = df.iloc[:-1].copy()
        if len(closed) < 30:
            return None

        ts = int(closed["ts"].iloc[-1])
        alert_price = float(closed["low"].rolling(20).min().iloc[-1])
        current_price = float(closed["close"].iloc[-1])
        high_window = float(closed["high"].max())

        if alert_price <= 0.0 or current_price <= 0.0:
            return None

        gain = (current_price / alert_price - 1.0) * 100.0
        dd = (current_price / high_window - 1.0) * 100.0 if high_window > 0.0 else 0.0

        vol_mean = float(closed["volume"].mean())
        recent_vol = float(closed["volume"].iloc[-3:].mean())
        vol_surge = recent_vol / vol_mean if vol_mean > 0.0 else 1.0
        score = _score_breakout(vol_surge, gain, dd)

        return {
            "ts": float(ts),
            "alert_price": alert_price,
            "current_price": current_price,
            "gain": gain,
            "dd": dd,
            "vol_surge": vol_surge,
            "score": score,
        }

    async def _open_breakout_position(
        self,
        symbol: str,
        metrics: Dict[str, float],
        session: aiohttp.ClientSession,
    ) -> None:
        async with self.position_lock:
            if symbol in self.active_positions:
                return
            entry_price = float(metrics["current_price"])
            self.active_positions[symbol] = {
                "alert_price": entry_price,
                "highest_price": entry_price,
                "entry_funding_rate": float(metrics.get("funding_rate", 0.0)),
                "entry_oi_ratio": float(metrics.get("oi_ratio", 1.0)),
                "entry_time": float(time.time()),
            }

        msg = (
            f"[PERP BREAKOUT] {symbol}\n"
            f"score={metrics['score']:.1f} | gain=+{metrics['gain']:.1f}% | "
            f"dd={metrics['dd']:.1f}% | vol={metrics['vol_surge']:.1f}x | "
            f"funding={float(metrics.get('funding_rate', 0.0)) * 100:.4f}% | "
            f"oi={float(metrics.get('oi_ratio', 1.0)):.2f}x | "
            f"onchain_tx_m5={float(metrics.get('onchain_txn_m5', 0.0)):.0f} | "
            f"onchain_hit={float(metrics.get('onchain_hit', 0.0)):.0f} | "
            f"lsr5m={float(metrics.get('sentiment_lsr_5m_chg_pct', 0.0)):+.2f}% | "
            f"fng={float(metrics.get('sentiment_fng_value', -1.0)):.0f}"
        )
        print(f"[{_now()}] {msg}")
        await send_discord_alert(session, self.config.discord_webhook_cex, msg)

    async def scan_symbol(self, exchange: Any, symbol: str, session: aiohttp.ClientSession) -> None:
        backoff = self.config.reconnect_base_sec
        while not self.stop_event.is_set():
            try:
                async with self.scan_semaphore:
                    candles = await exchange.watch_ohlcv(symbol, timeframe="1m", limit=100)
                metrics = self._calculate_metrics(candles)
                if not metrics:
                    backoff = self.config.reconnect_base_sec
                    continue

                candle_ts = int(metrics["ts"])
                if self.last_closed_ts.get(symbol) == candle_ts:
                    backoff = self.config.reconnect_base_sec
                    continue
                self.last_closed_ts[symbol] = candle_ts

                if (
                    metrics["score"] > self.config.score_threshold
                    and metrics["gain"] > self.config.gain_threshold
                    and metrics["dd"] > self.config.max_drawdown_before_breakout
                    and metrics["vol_surge"] > self.config.vol_surge_threshold
                ):
                    raw_symbol = _to_raw_usdt_symbol(symbol)
                    derivative = await fetch_funding_oi_snapshot(
                        session=session,
                        raw_symbol=raw_symbol,
                        oi_hist_period=self.config.oi_hist_period,
                        oi_hist_limit=self.config.oi_hist_limit,
                    )
                    if not derivative:
                        backoff = self.config.reconnect_base_sec
                        continue

                    funding_rate = float(derivative.get("funding_rate", 0.0) or 0.0)
                    oi_ratio = float(derivative.get("oi_ratio", 1.0) or 1.0)
                    funding_ok = funding_rate > self.config.funding_threshold_rate
                    oi_ok = oi_ratio > self.config.oi_surge_threshold

                    if funding_ok and oi_ok:
                        context_ts = time.time()
                        onchain_data, sentiment_data = await asyncio.gather(
                            self._get_onchain_snapshot_cached(session, raw_symbol, context_ts),
                            self._get_sentiment_snapshot_cached(session, raw_symbol, context_ts),
                        )

                        if float(onchain_data.get("onchain_ok", 0.0) or 0.0) < 0.5:
                            backoff = self.config.reconnect_base_sec
                            continue
                        if float(sentiment_data.get("sentiment_ok", 0.0) or 0.0) < 0.5:
                            backoff = self.config.reconnect_base_sec
                            continue

                        metrics["funding_rate"] = funding_rate
                        metrics["oi_ratio"] = oi_ratio
                        metrics.update(onchain_data)
                        metrics.update(sentiment_data)
                        await self._open_breakout_position(symbol, metrics, session)

                backoff = self.config.reconnect_base_sec
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[{_now()}] scan error symbol={symbol}: {exc!r}; reconnect in {backoff:.1f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self.config.reconnect_max_sec)

    async def monitor_active_positions(self, exchange: Any, session: aiohttp.ClientSession) -> None:
        while not self.stop_event.is_set():
            async with self.position_lock:
                tracked_symbols = list(self.active_positions.keys())

            for symbol in tracked_symbols:
                exit_payload: Optional[Tuple[str, str]] = None
                try:
                    ticker = await exchange.fetch_ticker(symbol)
                    current_price = float(ticker.get("last", 0.0) or 0.0)
                    if current_price <= 0.0:
                        continue

                    raw_symbol = _to_raw_usdt_symbol(symbol)
                    derivative = await fetch_funding_oi_snapshot(
                        session=session,
                        raw_symbol=raw_symbol,
                        oi_hist_period=self.config.oi_hist_period,
                        oi_hist_limit=self.config.oi_hist_limit,
                    )
                    funding_rate_now: Optional[float] = None
                    oi_ratio_now: Optional[float] = None
                    if derivative is not None:
                        try:
                            funding_rate_now = float(derivative.get("funding_rate", 0.0) or 0.0)
                        except Exception:
                            funding_rate_now = None
                        try:
                            oi_ratio_now = float(derivative.get("oi_ratio", 1.0) or 1.0)
                        except Exception:
                            oi_ratio_now = None

                    async with self.position_lock:
                        data = self.active_positions.get(symbol)
                        if not data:
                            continue

                        if current_price > data["highest_price"]:
                            data["highest_price"] = current_price

                        drawdown_from_peak = (current_price / data["highest_price"] - 1.0) * 100.0
                        current_profit = (current_price / data["alert_price"] - 1.0) * 100.0
                        max_profit = (data["highest_price"] / data["alert_price"] - 1.0) * 100.0

                        if drawdown_from_peak <= -self.config.trailing_stop_percent:
                            reason = f"drawdown from peak reached {drawdown_from_peak:.1f}%"
                            exit_msg = (
                                f"[PERP EXIT] {symbol}\n"
                                f"reason={reason}\n"
                                f"pnl={current_profit:+.1f}% (peak={max_profit:+.1f}%)"
                            )
                            del self.active_positions[symbol]
                            exit_payload = (symbol, exit_msg)
                            continue

                        # Funding/OI API failure should not force an emergency exit.
                        if funding_rate_now is None or oi_ratio_now is None:
                            continue

                        funding_reversal = funding_rate_now <= self.config.whale_funding_reversal_rate
                        oi_drop = oi_ratio_now <= self.config.whale_oi_drop_ratio
                        if funding_reversal or oi_drop:
                            reasons: List[str] = []
                            if funding_reversal:
                                reasons.append(
                                    f"funding reversed to {funding_rate_now * 100:.4f}% "
                                    f"(threshold {self.config.whale_funding_reversal_rate * 100:.4f}%)"
                                )
                            if oi_drop:
                                reasons.append(
                                    f"oi dropped to {oi_ratio_now:.2f}x "
                                    f"(threshold {self.config.whale_oi_drop_ratio:.2f}x)"
                                )
                            reason = "; ".join(reasons)
                            exit_msg = (
                                f"[PERP EXIT] {symbol}\n"
                                f"reason=whale-risk {reason}\n"
                                f"pnl={current_profit:+.1f}% (peak={max_profit:+.1f}%)"
                            )
                            del self.active_positions[symbol]
                            exit_payload = (symbol, exit_msg)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    continue

                if exit_payload:
                    _, message = exit_payload
                    print(f"[{_now()}] {message}")
                    await send_discord_alert(session, self.config.discord_webhook_exit, message)

            await asyncio.sleep(self.config.monitor_interval_sec)

    async def status_reporter(self) -> None:
        while not self.stop_event.is_set():
            await asyncio.sleep(60)
            async with self.position_lock:
                active_count = len(self.active_positions)
            print(f"[{_now()}] heartbeat: active_positions={active_count}")

    async def run(self, symbols_override: Optional[List[str]]) -> None:
        if ccxtpro is None:
            raise RuntimeError(
                f"ccxt.pro import failed: {_CCXTPRO_IMPORT_ERROR!r}. "
                "Install ccxtpro before running this radar."
            )

        timeout = aiohttp.ClientTimeout(total=self.config.request_timeout_sec)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            if symbols_override:
                symbols = symbols_override
            else:
                print(f"[{_now()}] loading active Binance USDT perpetual symbols...")
                symbols = await fetch_active_usdt_pairs(
                    session=session,
                    min_24h_volume_usdt=self.config.min_24h_volume_usdt,
                    max_symbols=self.config.max_symbols,
                )
                if not symbols:
                    symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]
                    print(f"[{_now()}] fallback symbols used: {', '.join(symbols)}")

            exchange = ccxtpro.binanceusdm(
                {
                    "enableRateLimit": True,
                    "options": {
                        "newUpdates": True,
                    },
                    "OHLCVLimit": 120,
                }
            )

            try:
                try:
                    await exchange.load_markets()
                except Exception as exc:
                    print(f"[{_now()}] exchange init failed: {exc!r}")
                    return

                print(
                    f"[{_now()}] radar started: symbols={len(symbols)}, "
                    f"websocket_concurrency={self.config.websocket_concurrency}"
                )
                tasks = [
                    asyncio.create_task(self.scan_symbol(exchange, symbol, session), name=f"scan:{symbol}")
                    for symbol in symbols
                ]
                tasks.append(asyncio.create_task(self.monitor_active_positions(exchange, session), name="trailing"))
                tasks.append(asyncio.create_task(self.status_reporter(), name="heartbeat"))

                try:
                    await asyncio.gather(*tasks)
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                await _close_exchange_safely(exchange)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Explosive perpetual radar monitor.")
    parser.add_argument(
        "--symbols",
        type=str,
        default=os.getenv("EXPLODE_SYMBOLS", "").strip(),
        help="Comma separated symbols. Example: BTC/USDT:USDT,ETH/USDT:USDT or BTCUSDT,ETHUSDT",
    )
    parser.add_argument(
        "--webhook-config-file",
        type=str,
        default=os.getenv("EXPLOSIVE_WEBHOOK_CONFIG_FILE", "config/webhook_explosive_spot_radar.json").strip()
        or "config/webhook_explosive_spot_radar.json",
    )
    parser.add_argument(
        "--discord-webhook-cex",
        type=str,
        default=os.getenv("DISCORD_WEBHOOK_CEX", "").strip(),
    )
    parser.add_argument(
        "--discord-webhook-exit",
        type=str,
        default=os.getenv("DISCORD_WEBHOOK_EXIT", "").strip(),
    )
    parser.add_argument("--vol-surge-threshold", type=float, default=_env_float("VOL_SURGE_THRESHOLD", 3.0))
    parser.add_argument("--score-threshold", type=float, default=_env_float("SCORE_THRESHOLD", 45.0))
    parser.add_argument("--gain-threshold", type=float, default=_env_float("GAIN_THRESHOLD", 15.0))
    parser.add_argument("--max-dd", type=float, default=_env_float("MAX_DD", -8.0))
    parser.add_argument("--trailing-stop-percent", type=float, default=_env_float("TRAILING_STOP_PERCENT", 15.0))
    parser.add_argument("--min-24h-volume-usdt", type=float, default=_env_float("MIN_24H_VOLUME_USDT", 300_000.0))
    parser.add_argument("--max-symbols", type=int, default=_env_int("MAX_SYMBOLS", 300))
    parser.add_argument("--websocket-concurrency", type=int, default=_env_int("SEMAPHORE_LIMIT", 15))
    parser.add_argument(
        "--funding-threshold-rate",
        type=float,
        default=_env_float("FUNDING_THRESHOLD_RATE", 0.0),
        help="Entry requires funding_rate > threshold. 0.0001 = 0.01%%",
    )
    parser.add_argument(
        "--oi-surge-threshold",
        type=float,
        default=_env_float("OI_SURGE_THRESHOLD", 1.2),
        help="Entry requires oi_ratio > threshold.",
    )
    parser.add_argument(
        "--whale-funding-reversal-rate",
        type=float,
        default=_env_float("WHALE_FUNDING_REVERSAL_RATE", 0.0),
        help="Exit when funding_rate <= threshold.",
    )
    parser.add_argument(
        "--whale-oi-drop-ratio",
        type=float,
        default=_env_float("WHALE_OI_DROP_RATIO", 0.7),
        help="Exit when oi_ratio <= threshold.",
    )
    parser.add_argument(
        "--onchain-filter-enabled",
        type=_parse_bool_arg,
        default=_env_bool("ONCHAIN_FILTER_ENABLED", True),
    )
    parser.add_argument("--onchain-ret-5m-pct", type=float, default=_env_float("ONCHAIN_RET_5M_PCT", 0.25))
    parser.add_argument("--onchain-vol-ratio-5m", type=float, default=_env_float("ONCHAIN_VOL_RATIO_5M", 1.8))
    parser.add_argument("--onchain-txn-m5-min", type=float, default=_env_float("ONCHAIN_TXN_M5_MIN", 20.0))
    parser.add_argument(
        "--onchain-buy-sell-ratio-min",
        type=float,
        default=_env_float("ONCHAIN_BUY_SELL_RATIO_MIN", 1.05),
    )
    parser.add_argument("--onchain-min-signals", type=int, default=_env_int("ONCHAIN_MIN_SIGNALS", 1))
    parser.add_argument("--onchain-min-liquidity-usd", type=float, default=_env_float("ONCHAIN_MIN_LIQUIDITY_USD", 20_000.0))
    parser.add_argument("--onchain-discovery-ttl-sec", type=float, default=_env_float("ONCHAIN_DISCOVERY_TTL_SEC", 1800.0))
    parser.add_argument("--onchain-micro-ttl-sec", type=float, default=_env_float("ONCHAIN_MICRO_TTL_SEC", 20.0))
    parser.add_argument(
        "--onchain-grace-no-data",
        type=_parse_bool_arg,
        default=_env_bool("ONCHAIN_GRACE_NO_DATA", True),
    )
    parser.add_argument(
        "--sentiment-filter-enabled",
        type=_parse_bool_arg,
        default=_env_bool("SENTIMENT_FILTER_ENABLED", True),
    )
    parser.add_argument("--sentiment-lsr-5m-min-pct", type=float, default=_env_float("SENTIMENT_LSR_5M_MIN_PCT", 0.30))
    parser.add_argument("--sentiment-fng-max", type=float, default=_env_float("SENTIMENT_FNG_MAX", 85.0))
    parser.add_argument("--sentiment-min-signals", type=int, default=_env_int("SENTIMENT_MIN_SIGNALS", 1))
    parser.add_argument("--sentiment-ttl-sec", type=float, default=_env_float("SENTIMENT_TTL_SEC", 20.0))
    parser.add_argument("--sentiment-fng-ttl-sec", type=float, default=_env_float("SENTIMENT_FNG_TTL_SEC", 300.0))
    parser.add_argument(
        "--sentiment-grace-no-data",
        type=_parse_bool_arg,
        default=_env_bool("SENTIMENT_GRACE_NO_DATA", True),
    )
    parser.add_argument("--oi-hist-period", type=str, default=os.getenv("OI_HIST_PERIOD", "5m").strip() or "5m")
    parser.add_argument("--oi-hist-limit", type=int, default=_env_int("OI_HIST_LIMIT", 12))
    parser.add_argument("--monitor-interval-sec", type=float, default=_env_float("MONITOR_INTERVAL_SEC", 3.0))
    parser.add_argument("--request-timeout-sec", type=float, default=_env_float("REQUEST_TIMEOUT_SEC", 10.0))
    parser.add_argument("--dry-run", action="store_true", default=_env_bool("DRY_RUN", False))
    return parser


def _config_from_args(args: argparse.Namespace) -> RadarConfig:
    resolved_cex, resolved_exit = _resolve_radar_webhooks(args)
    webhook_cex = "" if args.dry_run else resolved_cex
    webhook_exit = "" if args.dry_run else resolved_exit
    return RadarConfig(
        discord_webhook_cex=webhook_cex,
        discord_webhook_exit=webhook_exit,
        vol_surge_threshold=float(args.vol_surge_threshold),
        score_threshold=float(args.score_threshold),
        gain_threshold=float(args.gain_threshold),
        max_drawdown_before_breakout=float(args.max_dd),
        trailing_stop_percent=float(args.trailing_stop_percent),
        min_24h_volume_usdt=float(args.min_24h_volume_usdt),
        max_symbols=max(1, int(args.max_symbols)),
        websocket_concurrency=max(1, int(args.websocket_concurrency)),
        funding_threshold_rate=float(args.funding_threshold_rate),
        oi_surge_threshold=max(1.0, float(args.oi_surge_threshold)),
        whale_funding_reversal_rate=float(args.whale_funding_reversal_rate),
        whale_oi_drop_ratio=max(0.1, float(args.whale_oi_drop_ratio)),
        onchain_filter_enabled=bool(args.onchain_filter_enabled),
        onchain_ret_5m_pct=float(args.onchain_ret_5m_pct),
        onchain_vol_ratio_5m=max(0.1, float(args.onchain_vol_ratio_5m)),
        onchain_txn_m5_min=max(0.0, float(args.onchain_txn_m5_min)),
        onchain_buy_sell_ratio_min=max(0.1, float(args.onchain_buy_sell_ratio_min)),
        onchain_min_signals=max(1, int(args.onchain_min_signals)),
        onchain_min_liquidity_usd=max(0.0, float(args.onchain_min_liquidity_usd)),
        onchain_discovery_ttl_sec=max(60.0, float(args.onchain_discovery_ttl_sec)),
        onchain_micro_ttl_sec=max(1.0, float(args.onchain_micro_ttl_sec)),
        onchain_grace_no_data=bool(args.onchain_grace_no_data),
        sentiment_filter_enabled=bool(args.sentiment_filter_enabled),
        sentiment_lsr_5m_min_pct=float(args.sentiment_lsr_5m_min_pct),
        sentiment_fng_max=max(1.0, float(args.sentiment_fng_max)),
        sentiment_min_signals=max(1, int(args.sentiment_min_signals)),
        sentiment_ttl_sec=max(1.0, float(args.sentiment_ttl_sec)),
        sentiment_fng_ttl_sec=max(30.0, float(args.sentiment_fng_ttl_sec)),
        sentiment_grace_no_data=bool(args.sentiment_grace_no_data),
        oi_hist_period=str(args.oi_hist_period or "5m").strip() or "5m",
        oi_hist_limit=max(3, int(args.oi_hist_limit)),
        monitor_interval_sec=max(0.5, float(args.monitor_interval_sec)),
        request_timeout_sec=max(2.0, float(args.request_timeout_sec)),
        dry_run=bool(args.dry_run),
    )


async def _async_main(args: argparse.Namespace) -> None:
    config = _config_from_args(args)
    symbols_override = _parse_symbols(args.symbols)
    radar = ExplosiveSpotRadar(config=config)
    await radar.run(symbols_override=symbols_override)


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    print("=" * 64)
    print("Explosive Perp Radar v1.1")
    print("=" * 64)
    print(
        "thresholds: "
        f"score>{args.score_threshold}, gain>{args.gain_threshold}%, "
        f"vol_surge>{args.vol_surge_threshold}x, max_dd>{args.max_dd}%, "
        f"funding>{float(args.funding_threshold_rate) * 100:.4f}%, "
        f"oi>{args.oi_surge_threshold}x, "
        f"onchain={'on' if bool(args.onchain_filter_enabled) else 'off'}, "
        f"sentiment={'on' if bool(args.sentiment_filter_enabled) else 'off'}"
    )
    try:
        asyncio.run(_async_main(args))
    except KeyboardInterrupt:
        print(f"\n[{_now()}] radar stopped by user.")
    except Exception as exc:
        print(f"[{_now()}] fatal error: {exc!r}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
