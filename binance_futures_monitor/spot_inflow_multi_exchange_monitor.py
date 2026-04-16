#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Multi-exchange spot incremental inflow monitor using CCXT unified trades."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sqlite3
import statistics
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Sequence, Set, Tuple

import aiohttp
import urllib.error
import urllib.parse
import urllib.request

try:
    import ccxt.async_support as ccxt_async  # type: ignore
except Exception as exc:  # pragma: no cover
    ccxt_async = None  # type: ignore[assignment]
    _CCXT_IMPORT_ERROR = exc
else:
    _CCXT_IMPORT_ERROR = None


DEFAULT_EXCHANGES: Tuple[str, ...] = (
    "binance",
    "bybit",
    "okx",
    "coinbase",
    "kraken",
    "kucoin",
    "bitget",
    "gateio",
    "mexc",
    "htx",
)
KOREA_EXCHANGES: Tuple[str, ...] = ("upbit", "bithumb", "coinone", "korbit", "gopax")
DIRECT_HTTP_EXCHANGES: Set[str] = {"upbit", "bithumb"}
DEFAULT_SYMBOLS: Tuple[str, ...] = ("BTC/USDT", "ETH/USDT", "SOL/USDT")
ALL_SYMBOLS_TOKEN = "__ALL_SYMBOLS__"
DEFAULT_QUOTE_WHITELIST: Tuple[str, ...] = ("USDT", "USD", "USDC", "FDUSD", "KRW")
COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
BINANCE_FUTURES_EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
STABLECOIN_BASES: Set[str] = {
    "USDT",
    "USDC",
    "BUSD",
    "FDUSD",
    "TUSD",
    "USDP",
    "USDE",
    "DAI",
    "PYUSD",
    "FRAX",
    "LUSD",
    "GUSD",
    "SUSD",
    "MIM",
    "USDJ",
    "EURS",
}
RANKING_BOARD_GLOBAL = "GLOBAL"
RANKING_BOARD_KOREA = "KOREA"
HTTP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
KST = timezone(timedelta(hours=9))


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, str(default))).strip())
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default))).strip())
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_bool_arg(v: str) -> bool:
    s = str(v or "").strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid bool value: {v}")


def _fmt_signed(value: float) -> str:
    try:
        sign = "+" if value >= 0 else "-"
        x = abs(float(value))
    except Exception:
        return "N/A"
    if x == float("inf"):
        return f"{sign}INF"
    if x >= 1_000_000_000:
        return f"{sign}{x / 1_000_000_000:.2f}B"
    if x >= 1_000_000:
        return f"{sign}{x / 1_000_000:.2f}M"
    if x >= 1_000:
        return f"{sign}{x / 1_000:.2f}K"
    return f"{sign}{x:.2f}"


def _fmt_unsigned(value: float) -> str:
    try:
        x = max(0.0, float(value))
    except Exception:
        return "N/A"
    if x == float("inf"):
        return "INF"
    if x >= 1_000_000_000:
        return f"{x / 1_000_000_000:.2f}B"
    if x >= 1_000_000:
        return f"{x / 1_000_000:.2f}M"
    if x >= 1_000:
        return f"{x / 1_000:.2f}K"
    return f"{x:.2f}"


def _fmt_market_cap(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"${_fmt_unsigned(value)}"


def _zscore(value: float, series: Sequence[float]) -> float:
    if len(series) < 2:
        return 0.0
    try:
        mean = float(statistics.mean(series))
        std = float(statistics.pstdev(series))
        v = float(value)
    except Exception:
        return 0.0
    if std <= 1e-12:
        return 0.0
    return (v - mean) / std


def _normalize_pair_token(token: str) -> str:
    s = str(token or "").strip().upper().replace(" ", "")
    if not s:
        return ""
    if ":" in s:
        s = s.split(":", 1)[0]
    if "/" in s:
        parts = [x for x in s.split("/") if x]
        if len(parts) != 2:
            return ""
        base, quote = parts
        if not base or not quote:
            return ""
        return f"{base}/{quote}"
    if s.endswith("USDT") and len(s) > 4:
        return f"{s[:-4]}/USDT"
    if s.endswith("USD") and len(s) > 3:
        return f"{s[:-3]}/USD"
    return ""


def parse_symbols(raw: str) -> Tuple[str, ...]:
    tokens = [x.strip() for x in str(raw or "").split(",") if x.strip()]
    if len(tokens) == 1 and tokens[0].lower() in {"all", "*"}:
        return (ALL_SYMBOLS_TOKEN,)
    out: List[str] = []
    for token in tokens:
        norm = _normalize_pair_token(token)
        if norm:
            out.append(norm)
    if not out:
        out = list(DEFAULT_SYMBOLS)
    return tuple(dict.fromkeys(out))


def parse_quote_whitelist(raw: str) -> Tuple[str, ...]:
    text = str(raw or "").strip()
    if not text:
        return DEFAULT_QUOTE_WHITELIST
    tokens = [x.strip().upper() for x in text.split(",") if x.strip()]
    if len(tokens) == 1 and tokens[0] in {"ALL", "*"}:
        return tuple()
    return tuple(dict.fromkeys(tokens))


def parse_exchanges(raw: str, max_exchanges: int) -> Tuple[str, ...]:
    text = str(raw or "").strip().lower()
    if not text:
        return DEFAULT_EXCHANGES

    tokens = [x.strip().lower() for x in text.split(",") if x.strip()]
    if not tokens:
        return DEFAULT_EXCHANGES

    if len(tokens) == 1 and tokens[0] == "all":
        if ccxt_async is None:
            return DEFAULT_EXCHANGES
        exs = list(getattr(ccxt_async, "exchanges", []) or [])
        exs = [str(x).strip().lower() for x in exs if str(x).strip()]
        exs = list(dict.fromkeys(exs))
        if max_exchanges > 0:
            exs = exs[: max(1, int(max_exchanges))]
        return tuple(exs)
    expanded: List[str] = []
    for token in tokens:
        if token in {"korea", "kr"}:
            expanded.extend(KOREA_EXCHANGES)
        else:
            expanded.append(token)
    return tuple(dict.fromkeys(expanded))


def pair_to_compact(pair: str) -> str:
    p = _normalize_pair_token(pair)
    return p.replace("/", "")


@dataclass(frozen=True)
class MonitorConfig:
    exchanges: Tuple[str, ...] = DEFAULT_EXCHANGES
    symbols: Tuple[str, ...] = DEFAULT_SYMBOLS
    include_aggregate: bool = True
    include_per_exchange: bool = True
    include_korea_exchanges: bool = True
    quote_whitelist: Tuple[str, ...] = DEFAULT_QUOTE_WHITELIST
    all_symbols_mode: bool = False
    window_sec: float = 1800.0
    ranking_interval_sec: float = 1800.0
    ranking_push_top_n: int = 20
    ranking_db_top_n: int = 100
    ranking_db_file: str = "data/logs/spot_inflow_rankings.db"
    metadata_refresh_sec: float = 6 * 3600.0
    coingecko_pages: int = 4
    exclude_top_marketcap_n: int = 20
    exclude_stablecoins: bool = True
    require_binance_futures: bool = True
    poll_interval_sec: float = 2.5
    poll_limit: int = 120
    max_pairs_per_cycle: int = 50
    korea_max_pairs_per_cycle: int = 20
    exchange_timeout_ms: int = 30_000
    exchange_start_jitter_sec: float = 0.4
    request_gap_sec: float = 0.08
    snapshot_interval_sec: float = 2.0
    status_interval_sec: float = 30.0
    min_net_inflow_usdt: float = 300_000.0
    min_increment_usdt: float = 120_000.0
    min_quote_volume_usdt: float = 500_000.0
    min_buy_ratio: float = 0.55
    min_zscore: float = 1.6
    zscore_lookback: int = 120
    zscore_min_samples: int = 30
    alert_cooldown_sec: float = 180.0
    exchange_retry_base_sec: float = 2.0
    exchange_retry_max_sec: float = 45.0
    duplicate_cache_size: int = 2048
    krw_per_usdt: float = 1400.0
    discord_webhook_url: str = ""
    webhook_timeout_sec: float = 8.0
    discord_retry_max: int = 3
    discord_min_interval_sec: float = 0.25
    dry_run: bool = False


@dataclass(frozen=True)
class WindowStats:
    net_inflow: float
    inflow: float
    outflow: float
    quote_volume: float
    buy_ratio: float
    trades: int


@dataclass(frozen=True)
class InflowAlert:
    scope: str
    ts_ms: int
    stats: WindowStats
    increment: float
    zscore: float


@dataclass(frozen=True)
class RankingRow:
    rank: int
    base_symbol: str
    coin_name: str
    market_cap_usd: Optional[float]
    fdv_usd: Optional[float]
    net_inflow_usdt: float
    top_exchange: str
    top_exchange_net_inflow_usdt: float
    growth_multiple: Optional[float]
    history_points: int


class RollingTradeWindow:
    """Sliding window for buy/sell quote flow."""

    def __init__(self, window_sec: float) -> None:
        self.window_ms = max(1, int(float(window_sec) * 1000))
        self.rows: Deque[Tuple[int, float, bool]] = deque()
        self.inflow = 0.0
        self.outflow = 0.0
        self.quote_volume = 0.0

    def add(self, ts_ms: int, quote_value: float, is_taker_buy: bool) -> None:
        ts = int(ts_ms)
        qv = max(0.0, float(quote_value))
        if qv <= 0.0:
            return
        side = bool(is_taker_buy)
        self.rows.append((ts, qv, side))
        if side:
            self.inflow += qv
        else:
            self.outflow += qv
        self.quote_volume += qv
        self.prune(ts - self.window_ms)

    def prune(self, cutoff_ms: int) -> None:
        cutoff = int(cutoff_ms)
        while self.rows and self.rows[0][0] < cutoff:
            _, qv, side = self.rows.popleft()
            if side:
                self.inflow -= qv
            else:
                self.outflow -= qv
            self.quote_volume -= qv
        if self.inflow < 0:
            self.inflow = 0.0
        if self.outflow < 0:
            self.outflow = 0.0
        if self.quote_volume < 0:
            self.quote_volume = 0.0

    def snapshot(self, now_ms: int) -> WindowStats:
        self.prune(int(now_ms) - self.window_ms)
        quote = max(0.0, float(self.quote_volume))
        inflow = max(0.0, float(self.inflow))
        outflow = max(0.0, float(self.outflow))
        buy_ratio = inflow / quote if quote > 0 else 0.0
        return WindowStats(
            net_inflow=inflow - outflow,
            inflow=inflow,
            outflow=outflow,
            quote_volume=quote,
            buy_ratio=buy_ratio,
            trades=len(self.rows),
        )


@dataclass
class ScopeState:
    window: RollingTradeWindow
    net_history: Deque[float] = field(default_factory=deque)
    prev_net_inflow: Optional[float] = None
    last_alert_ms: int = 0
    last_seen_ms: int = 0


@dataclass
class TradeCursor:
    exchange_id: str
    requested_pair: str
    market_symbol: str
    market_quote: str
    last_ts_ms: int = 0
    seen_queue: Deque[str] = field(default_factory=deque)
    seen_set: Set[str] = field(default_factory=set)

    def remember(self, key: str, max_size: int) -> bool:
        if key in self.seen_set:
            return False
        self.seen_queue.append(key)
        self.seen_set.add(key)
        while len(self.seen_queue) > max_size:
            old = self.seen_queue.popleft()
            self.seen_set.discard(old)
        return True


def resolve_spot_pairs(
    exchange_id: str,
    exchange: Any,
    requested_pairs: Sequence[str],
    quote_whitelist: Sequence[str],
) -> Dict[str, Tuple[str, str]]:
    out: Dict[str, Tuple[str, str]] = {}
    ex_id = str(exchange_id or "").strip().lower()
    all_mode = ALL_SYMBOLS_TOKEN in set(requested_pairs)
    whitelist = {str(x).strip().upper() for x in quote_whitelist if str(x).strip()}
    markets = getattr(exchange, "markets", {}) or {}
    pair_to_market: Dict[str, Tuple[str, str]] = {}
    base_to_quotes: Dict[str, Dict[str, Tuple[str, str]]] = {}
    for market in markets.values():
        if not isinstance(market, dict):
            continue
        if not bool(market.get("spot", False)):
            continue
        if market.get("active") is False:
            continue
        base = str(market.get("base", "")).upper().strip()
        quote = str(market.get("quote", "")).upper().strip()
        symbol = str(market.get("symbol", "")).strip()
        if not base or not quote or not symbol:
            continue
        if whitelist and quote not in whitelist:
            continue
        pair = f"{base}/{quote}"
        if pair not in pair_to_market:
            pair_to_market[pair] = (symbol, quote)
        base_to_quotes.setdefault(base, {})
        if quote not in base_to_quotes[base]:
            base_to_quotes[base][quote] = (symbol, quote)
    if all_mode:
        for pair in sorted(pair_to_market.keys()):
            out[pair] = pair_to_market[pair]
        return out
    for pair in requested_pairs:
        norm = _normalize_pair_token(pair)
        if not norm:
            continue
        if norm in pair_to_market:
            out[norm] = pair_to_market[norm]
            continue
        if "/" not in norm:
            continue
        base, _ = norm.split("/", 1)
        quote_map = base_to_quotes.get(base, {})
        if not quote_map:
            continue
        fallback_quotes: List[str]
        if ex_id in KOREA_EXCHANGES:
            fallback_quotes = ["KRW", "USDT", "USD", "USDC", "FDUSD"]
        else:
            fallback_quotes = ["USDT", "USD", "USDC", "FDUSD", "KRW"]
        selected: Optional[Tuple[str, str]] = None
        for q in fallback_quotes:
            item = quote_map.get(q)
            if item is not None:
                selected = item
                break
        if selected is None:
            continue
        out[norm] = selected
    return out


class _StaticMarketsExchange:
    def __init__(self, markets: Dict[str, Dict[str, Any]]) -> None:
        self.markets = markets


def build_direct_markets(exchange_id: str, payload: Any) -> Dict[str, Dict[str, Any]]:
    ex_id = str(exchange_id or "").strip().lower()
    markets: Dict[str, Dict[str, Any]] = {}
    if ex_id == "upbit":
        if not isinstance(payload, list):
            return markets
        for item in payload:
            if not isinstance(item, dict):
                continue
            market = str(item.get("market", "")).upper().strip()
            if "-" not in market:
                continue
            quote, base = market.split("-", 1)
            if not base or not quote:
                continue
            pair = f"{base}/{quote}"
            markets[pair] = {
                "spot": True,
                "active": True,
                "base": base,
                "quote": quote,
                "symbol": market,
            }
        return markets
    if ex_id == "bithumb":
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return markets
        for raw_base in data.keys():
            base = str(raw_base or "").upper().strip()
            if not base or base == "DATE":
                continue
            pair = f"{base}/KRW"
            markets[pair] = {
                "spot": True,
                "active": True,
                "base": base,
                "quote": "KRW",
                "symbol": f"{base}_KRW",
            }
        return markets
    return markets


def parse_direct_trades(exchange_id: str, payload: Any) -> List[Dict[str, Any]]:
    ex_id = str(exchange_id or "").strip().lower()
    out: List[Dict[str, Any]] = []
    if ex_id == "upbit":
        if not isinstance(payload, list):
            return out
        for item in payload:
            if not isinstance(item, dict):
                continue
            ts = int(item.get("timestamp", 0) or 0)
            price = float(item.get("trade_price", 0.0) or 0.0)
            amount = float(item.get("trade_volume", 0.0) or 0.0)
            ask_bid = str(item.get("ask_bid", "")).upper().strip()
            side = "buy" if ask_bid == "BID" else ("sell" if ask_bid == "ASK" else "")
            if ts <= 0 or price <= 0.0 or amount <= 0.0 or not side:
                continue
            out.append(
                {
                    "id": str(item.get("sequential_id", "")).strip(),
                    "timestamp": ts,
                    "side": side,
                    "price": price,
                    "amount": amount,
                    "cost": price * amount,
                }
            )
        return out
    if ex_id == "bithumb":
        rows = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            return out
        for item in rows:
            if not isinstance(item, dict):
                continue
            ts = 0
            dt_text = str(item.get("transaction_date", "")).strip()
            if dt_text:
                try:
                    dt = datetime.strptime(dt_text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
                    ts = int(dt.timestamp() * 1000)
                except Exception:
                    ts = 0
            price = float(item.get("price", 0.0) or 0.0)
            amount = float(item.get("units_traded", 0.0) or 0.0)
            total = float(item.get("total", 0.0) or 0.0)
            side_key = str(item.get("type", "")).lower().strip()
            side = "buy" if side_key == "bid" else ("sell" if side_key == "ask" else "")
            if ts <= 0 or price <= 0.0 or amount <= 0.0 or not side:
                continue
            cost = total if total > 0.0 else price * amount
            out.append(
                {
                    "id": f"{dt_text}:{side_key}:{price:.10f}:{amount:.10f}",
                    "timestamp": ts,
                    "side": side,
                    "price": price,
                    "amount": amount,
                    "cost": cost,
                }
            )
        return out
    return out


class MultiExchangeSpotInflowMonitor:
    def __init__(self, config: MonitorConfig) -> None:
        self.config = config
        self.states: Dict[str, ScopeState] = {}
        self.coin_windows: Dict[str, RollingTradeWindow] = {}
        self.coin_exchange_windows: Dict[str, Dict[str, RollingTradeWindow]] = {}
        self.session: Optional[aiohttp.ClientSession] = None
        self._history_maxlen = max(2, int(config.zscore_lookback))
        self._coin_symbol_meta: Dict[str, Tuple[str, Optional[float], Optional[float]]] = {}
        self._top_marketcap_excluded: Set[str] = set()
        self._binance_futures_bases: Set[str] = set()
        self._last_meta_refresh_ts: float = 0.0
        self._korea_exchange_filter: Set[str] = {
            str(ex).upper().strip()
            for ex in self.config.exchanges
            if str(ex).strip().lower() in set(KOREA_EXCHANGES)
        }
        self._discord_webhook_candidates: List[str] = self._build_discord_webhook_candidates(
            str(self.config.discord_webhook_url or "").strip()
        )
        self._discord_preferred_idx: int = 0
        self._db_path = str(self.config.ranking_db_file).strip() or "data/logs/spot_inflow_rankings.db"
        self._init_db()

    def _ensure_scope_state(self, scope: str) -> ScopeState:
        s = str(scope or "").strip().upper()
        state = self.states.get(s)
        if state is not None:
            return state
        state = ScopeState(
            window=RollingTradeWindow(self.config.window_sec),
            net_history=deque(maxlen=self._history_maxlen),
        )
        self.states[s] = state
        return state

    def _init_db(self) -> None:
        db_dir = os.path.dirname(self._db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        conn = sqlite3.connect(self._db_path)
        try:
            self._ensure_coin_window_stats_table(conn)
            self._ensure_ranking_snapshots_table(conn)
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _table_info(conn: sqlite3.Connection, table_name: str) -> List[Tuple[Any, ...]]:
        return list(conn.execute(f"PRAGMA table_info({table_name})").fetchall())

    @staticmethod
    def _pk_columns(table_info: Sequence[Tuple[Any, ...]]) -> List[str]:
        rows = [row for row in table_info if int(row[5]) > 0]
        rows.sort(key=lambda x: int(x[5]))
        return [str(row[1]) for row in rows]

    def _ensure_coin_window_stats_table(self, conn: sqlite3.Connection) -> None:
        info = self._table_info(conn, "coin_window_stats")
        expected_pk = ["board", "snapshot_ts", "base_symbol"]
        if not info:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS coin_window_stats (
                    board TEXT NOT NULL,
                    snapshot_ts INTEGER NOT NULL,
                    base_symbol TEXT NOT NULL,
                    net_inflow_usdt REAL NOT NULL,
                    quote_volume_usdt REAL NOT NULL,
                    PRIMARY KEY (board, snapshot_ts, base_symbol)
                )
                """
            )
        else:
            columns = {str(row[1]) for row in info}
            pk_cols = self._pk_columns(info)
            if "board" not in columns or pk_cols != expected_pk:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS coin_window_stats_v2 (
                        board TEXT NOT NULL,
                        snapshot_ts INTEGER NOT NULL,
                        base_symbol TEXT NOT NULL,
                        net_inflow_usdt REAL NOT NULL,
                        quote_volume_usdt REAL NOT NULL,
                        PRIMARY KEY (board, snapshot_ts, base_symbol)
                    )
                    """
                )
                if "board" in columns:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO coin_window_stats_v2
                        (board, snapshot_ts, base_symbol, net_inflow_usdt, quote_volume_usdt)
                        SELECT UPPER(COALESCE(board, 'GLOBAL')), snapshot_ts, base_symbol, net_inflow_usdt, quote_volume_usdt
                        FROM coin_window_stats
                        """
                    )
                else:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO coin_window_stats_v2
                        (board, snapshot_ts, base_symbol, net_inflow_usdt, quote_volume_usdt)
                        SELECT 'GLOBAL', snapshot_ts, base_symbol, net_inflow_usdt, quote_volume_usdt
                        FROM coin_window_stats
                        """
                    )
                conn.execute("DROP TABLE coin_window_stats")
                conn.execute("ALTER TABLE coin_window_stats_v2 RENAME TO coin_window_stats")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_coin_window_stats_board_base_ts "
            "ON coin_window_stats(board, base_symbol, snapshot_ts)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_coin_window_stats_board_ts "
            "ON coin_window_stats(board, snapshot_ts)"
        )

    def _ensure_ranking_snapshots_table(self, conn: sqlite3.Connection) -> None:
        info = self._table_info(conn, "ranking_snapshots")
        expected_pk = ["board", "snapshot_ts", "rank"]
        if not info:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ranking_snapshots (
                    board TEXT NOT NULL,
                    snapshot_ts INTEGER NOT NULL,
                    rank INTEGER NOT NULL,
                    base_symbol TEXT NOT NULL,
                    coin_name TEXT NOT NULL,
                    market_cap_usd REAL,
                    net_inflow_usdt REAL NOT NULL,
                    top_exchange TEXT NOT NULL,
                    top_exchange_net_inflow_usdt REAL NOT NULL,
                    growth_multiple REAL,
                    history_points INTEGER NOT NULL,
                    PRIMARY KEY (board, snapshot_ts, rank)
                )
                """
            )
        else:
            columns = {str(row[1]) for row in info}
            pk_cols = self._pk_columns(info)
            if "board" not in columns or pk_cols != expected_pk:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ranking_snapshots_v2 (
                        board TEXT NOT NULL,
                        snapshot_ts INTEGER NOT NULL,
                        rank INTEGER NOT NULL,
                        base_symbol TEXT NOT NULL,
                        coin_name TEXT NOT NULL,
                        market_cap_usd REAL,
                        net_inflow_usdt REAL NOT NULL,
                        top_exchange TEXT NOT NULL,
                        top_exchange_net_inflow_usdt REAL NOT NULL,
                        growth_multiple REAL,
                        history_points INTEGER NOT NULL,
                        PRIMARY KEY (board, snapshot_ts, rank)
                    )
                    """
                )
                if "board" in columns:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO ranking_snapshots_v2
                        (board, snapshot_ts, rank, base_symbol, coin_name, market_cap_usd, net_inflow_usdt,
                         top_exchange, top_exchange_net_inflow_usdt, growth_multiple, history_points)
                        SELECT UPPER(COALESCE(board, 'GLOBAL')), snapshot_ts, rank, base_symbol, coin_name, market_cap_usd,
                               net_inflow_usdt, top_exchange, top_exchange_net_inflow_usdt, growth_multiple, history_points
                        FROM ranking_snapshots
                        """
                    )
                else:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO ranking_snapshots_v2
                        (board, snapshot_ts, rank, base_symbol, coin_name, market_cap_usd, net_inflow_usdt,
                         top_exchange, top_exchange_net_inflow_usdt, growth_multiple, history_points)
                        SELECT 'GLOBAL', snapshot_ts, rank, base_symbol, coin_name, market_cap_usd,
                               net_inflow_usdt, top_exchange, top_exchange_net_inflow_usdt, growth_multiple, history_points
                        FROM ranking_snapshots
                        """
                    )
                conn.execute("DROP TABLE ranking_snapshots")
                conn.execute("ALTER TABLE ranking_snapshots_v2 RENAME TO ranking_snapshots")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ranking_snapshots_board_base_ts "
            "ON ranking_snapshots(board, base_symbol, snapshot_ts)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ranking_snapshots_board_ts_rank "
            "ON ranking_snapshots(board, snapshot_ts, rank)"
        )

    async def _fetch_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if self.session is None:
            return None
        try:
            async with self.session.get(url, params=params) as resp:
                if resp.status >= 300:
                    return None
                return await resp.json()
        except Exception:
            return None

    async def _refresh_metadata_if_needed(self, *, force: bool = False) -> None:
        now = time.time()
        if (
            not force
            and self._coin_symbol_meta
            and self._binance_futures_bases
            and now - self._last_meta_refresh_ts < max(60.0, float(self.config.metadata_refresh_sec))
        ):
            return

        # CoinGecko market caps and names.
        symbol_meta: Dict[str, Tuple[str, Optional[float], Optional[float]]] = {}
        top_symbols: List[str] = []
        pages = max(1, int(self.config.coingecko_pages))
        for page in range(1, pages + 1):
            payload = await self._fetch_json(
                COINGECKO_MARKETS_URL,
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": 250,
                    "page": page,
                    "sparkline": "false",
                },
            )
            if not isinstance(payload, list):
                continue
            for i, row in enumerate(payload):
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol", "")).upper().strip()
                if not symbol:
                    continue
                name = str(row.get("name", symbol)).strip() or symbol
                market_cap_raw = row.get("market_cap")
                market_cap: Optional[float]
                try:
                    market_cap = float(market_cap_raw) if market_cap_raw is not None else None
                except Exception:
                    market_cap = None
                fdv_raw = row.get("fully_diluted_valuation")
                fdv: Optional[float]
                try:
                    fdv = float(fdv_raw) if fdv_raw is not None else None
                except Exception:
                    fdv = None

                prev = symbol_meta.get(symbol)
                prev_cap = prev[1] if prev else None
                if prev is None or (market_cap is not None and (prev_cap is None or market_cap > prev_cap)):
                    symbol_meta[symbol] = (name, market_cap, fdv)
                if page == 1 and i < max(0, int(self.config.exclude_top_marketcap_n)):
                    top_symbols.append(symbol)

        # Binance futures tradable base assets.
        futures_payload = await self._fetch_json(BINANCE_FUTURES_EXCHANGE_INFO_URL)
        futures_bases: Set[str] = set()
        if isinstance(futures_payload, dict):
            for row in futures_payload.get("symbols", []) or []:
                if not isinstance(row, dict):
                    continue
                status = str(row.get("status", "")).upper().strip()
                if status != "TRADING":
                    continue
                base_asset = str(row.get("baseAsset", "")).upper().strip()
                if base_asset:
                    futures_bases.add(base_asset)

        if symbol_meta:
            self._coin_symbol_meta = symbol_meta
        self._top_marketcap_excluded = set(top_symbols)
        if futures_bases:
            self._binance_futures_bases = futures_bases
        self._last_meta_refresh_ts = now

    def _should_exclude_base(self, base_symbol: str) -> bool:
        base = str(base_symbol or "").upper().strip()
        if not base:
            return True
        if bool(self.config.exclude_stablecoins) and base in STABLECOIN_BASES:
            return True
        if int(self.config.exclude_top_marketcap_n) > 0 and base in self._top_marketcap_excluded:
            return True
        if bool(self.config.require_binance_futures) and self._binance_futures_bases and base not in self._binance_futures_bases:
            return True
        return False

    def _load_recent_24h_means(
        self,
        snapshot_ts: int,
        board: str = RANKING_BOARD_GLOBAL,
    ) -> Dict[str, Tuple[float, int]]:
        lookback_ts = int(snapshot_ts) - 24 * 3600
        board_key = str(board or RANKING_BOARD_GLOBAL).upper().strip() or RANKING_BOARD_GLOBAL
        conn = sqlite3.connect(self._db_path)
        try:
            rows = conn.execute(
                """
                SELECT base_symbol, AVG(net_inflow_usdt) AS avg_net, COUNT(*) AS n
                FROM coin_window_stats
                WHERE board = ? AND snapshot_ts >= ? AND snapshot_ts < ?
                GROUP BY base_symbol
                """,
                (board_key, lookback_ts, int(snapshot_ts)),
            ).fetchall()
        finally:
            conn.close()
        out: Dict[str, Tuple[float, int]] = {}
        for row in rows:
            base = str(row[0]).upper().strip()
            try:
                avg_net = float(row[1])
                n = int(row[2])
            except Exception:
                continue
            out[base] = (avg_net, n)
        return out

    def _load_recent_24h_top_counts(
        self,
        snapshot_ts: int,
        top_n: int,
        board: str = RANKING_BOARD_GLOBAL,
    ) -> Dict[str, int]:
        lookback_ts = int(snapshot_ts) - 24 * 3600
        board_key = str(board or RANKING_BOARD_GLOBAL).upper().strip() or RANKING_BOARD_GLOBAL
        conn = sqlite3.connect(self._db_path)
        try:
            rows = conn.execute(
                """
                SELECT base_symbol, COUNT(*) AS n
                FROM ranking_snapshots
                WHERE board = ? AND snapshot_ts >= ? AND snapshot_ts <= ? AND rank <= ?
                GROUP BY base_symbol
                """,
                (board_key, lookback_ts, int(snapshot_ts), max(1, int(top_n))),
            ).fetchall()
        finally:
            conn.close()
        out: Dict[str, int] = {}
        for row in rows:
            base = str(row[0]).upper().strip()
            try:
                n = int(row[1])
            except Exception:
                n = 0
            out[base] = max(0, n)
        return out

    def _store_snapshot(
        self,
        snapshot_ts: int,
        coin_rows: Sequence[Tuple[str, float, float]],
        ranking_rows: Sequence[RankingRow],
        board: str = RANKING_BOARD_GLOBAL,
    ) -> None:
        board_key = str(board or RANKING_BOARD_GLOBAL).upper().strip() or RANKING_BOARD_GLOBAL
        conn = sqlite3.connect(self._db_path)
        try:
            conn.execute("BEGIN")
            conn.executemany(
                """
                INSERT OR REPLACE INTO coin_window_stats
                (board, snapshot_ts, base_symbol, net_inflow_usdt, quote_volume_usdt)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (board_key, int(snapshot_ts), str(base).upper(), float(net), float(qv))
                    for base, net, qv in coin_rows
                ],
            )
            conn.execute(
                "DELETE FROM ranking_snapshots WHERE board = ? AND snapshot_ts = ?",
                (board_key, int(snapshot_ts)),
            )
            conn.executemany(
                """
                INSERT INTO ranking_snapshots
                (board, snapshot_ts, rank, base_symbol, coin_name, market_cap_usd, net_inflow_usdt,
                 top_exchange, top_exchange_net_inflow_usdt, growth_multiple, history_points)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        board_key,
                        int(snapshot_ts),
                        int(row.rank),
                        row.base_symbol,
                        row.coin_name,
                        row.market_cap_usd,
                        float(row.net_inflow_usdt),
                        row.top_exchange,
                        float(row.top_exchange_net_inflow_usdt),
                        row.growth_multiple,
                        int(row.history_points),
                    )
                    for row in ranking_rows
                ],
            )
            conn.commit()
        finally:
            conn.close()

    def _ranking_boards(self) -> List[Tuple[str, Optional[Set[str]]]]:
        out: List[Tuple[str, Optional[Set[str]]]] = [(RANKING_BOARD_GLOBAL, None)]
        if self._korea_exchange_filter:
            out.append((RANKING_BOARD_KOREA, set(self._korea_exchange_filter)))
        return out

    def _pairs_per_cycle_for_exchange(self, exchange_id: str) -> int:
        base = max(1, int(self.config.max_pairs_per_cycle))
        ex_id = str(exchange_id or "").strip().lower()
        if ex_id in KOREA_EXCHANGES:
            korea_cap = max(1, int(self.config.korea_max_pairs_per_cycle))
            return min(base, korea_cap)
        return base

    @staticmethod
    def _board_display_name(board: str) -> str:
        b = str(board or "").upper().strip()
        if b == RANKING_BOARD_GLOBAL:
            return "总榜"
        if b == RANKING_BOARD_KOREA:
            return "韩国榜"
        return b or "榜单"

    def _format_ranking_discord(
        self,
        snapshot_ts: int,
        ranking_rows: Sequence[RankingRow],
        top_counts_24h: Optional[Dict[str, int]] = None,
        board: str = RANKING_BOARD_GLOBAL,
    ) -> str:
        board_name = self._board_display_name(board)
        title = f"【{board_name}Top20】"
        if not ranking_rows:
            return (
                f"{title}\n"
                f"现货净流入Top20为空 ({datetime.fromtimestamp(snapshot_ts).strftime('%Y-%m-%d %H:%M:%S')})"
            )
        counts = top_counts_24h or {}
        lines: List[str] = [title]
        for row in ranking_rows:
            growth = "N/A"
            if row.growth_multiple is not None:
                growth = f"{row.growth_multiple:.2f}".rstrip("0").rstrip(".")
            fdv_val = row.fdv_usd if row.fdv_usd is not None else row.market_cap_usd
            fdv_text = "N/A" if fdv_val is None else _fmt_unsigned(fdv_val)
            times = int(counts.get(row.base_symbol.upper(), 0))
            symbol_tag = f"{row.base_symbol.lower()}usdt"
            exchange_text = str(row.top_exchange or "N/A").lower()
            row_line = f"⭐️{symbol_tag}⭐️/fdv:{fdv_text}/{exchange_text}/{growth}倍/{times}次"
            if row.growth_multiple is not None and float(row.growth_multiple) > 10.0:
                row_line = f"**🔥{row_line}**"
            lines.append(row_line)
        return "\n".join(lines)

    def _format_cross_board_summary_discord(
        self,
        global_rows: Sequence[RankingRow],
        korea_rows: Sequence[RankingRow],
    ) -> str:
        lines: List[str] = ["【交叉汇总Top20】"]
        global_rank: Dict[str, int] = {str(row.base_symbol).upper(): int(row.rank) for row in global_rows}
        korea_rank: Dict[str, int] = {str(row.base_symbol).upper(): int(row.rank) for row in korea_rows}
        common = sorted(
            [x for x in global_rank.keys() if x in korea_rank],
            key=lambda x: (global_rank[x], korea_rank[x], x),
        )
        if not common:
            lines.append("无同时出现在总榜Top20和韩国榜Top20的币种")
            return "\n".join(lines)
        for base in common:
            symbol_tag = f"{base.lower()}usdt"
            lines.append(
                f"⭐️{symbol_tag}⭐️/总榜#{global_rank[base]}/韩国榜#{korea_rank[base]}"
            )
        return "\n".join(lines)

    @staticmethod
    def _next_boundary_ts(now_ts: float, interval_sec: float) -> int:
        interval = max(60, int(interval_sec))
        return int((int(now_ts) // interval + 1) * interval)

    @staticmethod
    def _scope_all(pair: str) -> str:
        return f"ALL:{pair_to_compact(pair)}"

    @staticmethod
    def _scope_exchange(exchange_id: str, pair: str) -> str:
        return f"{str(exchange_id).upper()}:{pair_to_compact(pair)}"

    def _normalize_quote_to_usdt(self, quote_value: float, quote: str) -> float:
        q = str(quote or "").upper().strip()
        v = max(0.0, float(quote_value))
        if v <= 0.0:
            return 0.0
        if q in {"USDT", "USDC", "FDUSD", "BUSD", "USD"}:
            return v
        if q == "KRW":
            rate = max(1.0, float(self.config.krw_per_usdt))
            return v / rate
        return v

    def ingest_trade(self, exchange_id: str, pair: str, ts_ms: int, quote_value: float, is_taker_buy: bool) -> None:
        base_symbol = str(pair).split("/", 1)[0].upper().strip()
        if base_symbol:
            coin_win = self.coin_windows.get(base_symbol)
            if coin_win is None:
                coin_win = RollingTradeWindow(self.config.window_sec)
                self.coin_windows[base_symbol] = coin_win
            coin_win.add(ts_ms=ts_ms, quote_value=quote_value, is_taker_buy=is_taker_buy)

            by_exchange = self.coin_exchange_windows.setdefault(base_symbol, {})
            ex_key = str(exchange_id or "").upper().strip()
            ex_win = by_exchange.get(ex_key)
            if ex_win is None:
                ex_win = RollingTradeWindow(self.config.window_sec)
                by_exchange[ex_key] = ex_win
            ex_win.add(ts_ms=ts_ms, quote_value=quote_value, is_taker_buy=is_taker_buy)

        if self.config.include_per_exchange:
            scope_ex = self._scope_exchange(exchange_id, pair)
            st_ex = self._ensure_scope_state(scope_ex)
            st_ex.window.add(ts_ms=ts_ms, quote_value=quote_value, is_taker_buy=is_taker_buy)
            st_ex.last_seen_ms = max(st_ex.last_seen_ms, int(ts_ms))
        if self.config.include_aggregate:
            scope_all = self._scope_all(pair)
            st_all = self._ensure_scope_state(scope_all)
            st_all.window.add(ts_ms=ts_ms, quote_value=quote_value, is_taker_buy=is_taker_buy)
            st_all.last_seen_ms = max(st_all.last_seen_ms, int(ts_ms))

    def evaluate_scope(self, scope: str, now_ms: int) -> Optional[InflowAlert]:
        state = self._ensure_scope_state(scope)
        stats = state.window.snapshot(now_ms)
        if stats.trades <= 0:
            state.prev_net_inflow = 0.0
            return None

        history = list(state.net_history)
        increment = (
            stats.net_inflow - float(state.prev_net_inflow)
            if state.prev_net_inflow is not None
            else 0.0
        )
        z = _zscore(stats.net_inflow, history) if history else 0.0
        state.prev_net_inflow = stats.net_inflow
        state.net_history.append(stats.net_inflow)

        if self.config.min_quote_volume_usdt > 0 and stats.quote_volume < self.config.min_quote_volume_usdt:
            return None
        if self.config.min_net_inflow_usdt > 0 and stats.net_inflow < self.config.min_net_inflow_usdt:
            return None
        if self.config.min_increment_usdt > 0 and increment < self.config.min_increment_usdt:
            return None
        if self.config.min_buy_ratio > 0 and stats.buy_ratio < self.config.min_buy_ratio:
            return None
        if self.config.min_zscore > 0:
            if len(history) < max(2, int(self.config.zscore_min_samples)):
                return None
            if z < self.config.min_zscore:
                return None

        cooldown_ms = int(max(0.0, self.config.alert_cooldown_sec) * 1000)
        if state.last_alert_ms > 0 and now_ms - state.last_alert_ms < cooldown_ms:
            return None
        state.last_alert_ms = now_ms

        return InflowAlert(
            scope=scope,
            ts_ms=int(now_ms),
            stats=stats,
            increment=increment,
            zscore=z,
        )

    @staticmethod
    def _scope_label(scope: str) -> str:
        s = str(scope or "").upper()
        if ":" not in s:
            return s
        left, right = s.split(":", 1)
        return f"{left} {right}"

    def _format_alert_compact(self, alert: InflowAlert) -> str:
        stats = alert.stats
        return (
            f"[{_now()}] SPOT INFLOW ALERT {self._scope_label(alert.scope)} "
            f"window={int(self.config.window_sec)}s "
            f"net={_fmt_signed(stats.net_inflow)} "
            f"inc={_fmt_signed(alert.increment)} "
            f"quote={_fmt_unsigned(stats.quote_volume)} "
            f"in={_fmt_unsigned(stats.inflow)} "
            f"out={_fmt_unsigned(stats.outflow)} "
            f"buy={stats.buy_ratio * 100.0:.1f}% "
            f"z={alert.zscore:.2f} "
            f"trades={stats.trades}"
        )

    def _format_alert_discord(self, alert: InflowAlert) -> str:
        stats = alert.stats
        return (
            "**现货流入告警 Spot Inflow**\n"
            f"标的: `{self._scope_label(alert.scope)}`\n"
            f"窗口: `{int(self.config.window_sec)}s`  买盘占比: `{stats.buy_ratio * 100.0:.1f}%`  z: `{alert.zscore:.2f}`\n"
            f"净流入: `{_fmt_signed(stats.net_inflow)}`  增量: `{_fmt_signed(alert.increment)}`\n"
            f"成交额: `{_fmt_unsigned(stats.quote_volume)}`  流入/流出: `{_fmt_unsigned(stats.inflow)} / {_fmt_unsigned(stats.outflow)}`\n"
            f"成交笔数: `{stats.trades}`"
        )

    @staticmethod
    def _default_http_headers(content_type: Optional[str] = None) -> Dict[str, str]:
        headers = {
            # Browser-like headers reduce the chance of CDN/WAF blocks on webhook and exchange routes.
            "User-Agent": HTTP_USER_AGENT,
            "Accept": "application/json, text/plain, */*",
            "Connection": "close",
        }
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    @classmethod
    def _http_get_json(cls, url: str, timeout: float = 8.0) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(2):
            try:
                req = urllib.request.Request(
                    url,
                    headers=cls._default_http_headers(),
                    method="GET",
                )
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    body = resp.read().decode("utf-8", errors="ignore")
                    if not body:
                        return {}
                    return json.loads(body)
            except Exception as exc:
                last_error = exc
                if attempt >= 1:
                    raise
                time.sleep(0.35)
        if last_error is not None:
            raise last_error
        return {}

    @classmethod
    def _http_post_json(cls, url: str, payload: Dict[str, Any], timeout: float = 8.0) -> Any:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=cls._default_http_headers("application/json"),
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            if not body:
                return {}
            try:
                return json.loads(body)
            except Exception:
                return {"raw": body}

    @staticmethod
    def _build_discord_webhook_candidates(webhook_url: str) -> List[str]:
        raw = str(webhook_url or "").strip()
        if not raw:
            return []
        out: List[str] = [raw]
        try:
            parts = urllib.parse.urlsplit(raw)
        except Exception:
            return out
        host = str(parts.netloc or "").lower().strip()
        if host != "discord.com":
            return out
        for alt_host in ("ptb.discord.com", "canary.discord.com"):
            alt_url = urllib.parse.urlunsplit(
                (
                    str(parts.scheme or "https"),
                    alt_host,
                    str(parts.path or ""),
                    str(parts.query or ""),
                    str(parts.fragment or ""),
                )
            )
            if alt_url not in out:
                out.append(alt_url)
        return out

    @staticmethod
    def _webhook_host(webhook_url: str) -> str:
        try:
            return str(urllib.parse.urlsplit(webhook_url).netloc or "unknown")
        except Exception:
            return "unknown"

    async def _send_discord(self, text: str) -> None:
        if self.config.dry_run:
            return
        if not self._discord_webhook_candidates:
            self._discord_webhook_candidates = self._build_discord_webhook_candidates(
                str(self.config.discord_webhook_url or "").strip()
            )
        if not self._discord_webhook_candidates:
            return
        attempts = max(1, int(self.config.discord_retry_max))
        timeout = max(2.0, float(self.config.webhook_timeout_sec))
        min_gap = max(0.0, float(self.config.discord_min_interval_sec))
        n_routes = len(self._discord_webhook_candidates)
        start_idx = self._discord_preferred_idx % n_routes
        route_order = [(start_idx + i) % n_routes for i in range(n_routes)]
        last_error = "unknown"
        for route_pos, route_idx in enumerate(route_order):
            webhook = self._discord_webhook_candidates[route_idx]
            host = self._webhook_host(webhook)
            for i in range(attempts):
                try:
                    await asyncio.to_thread(self._http_post_json, webhook, {"content": text}, timeout)
                    self._discord_preferred_idx = route_idx
                    if route_pos > 0:
                        print(f"[{_now()}] discord route switched to host={host}")
                    return
                except urllib.error.HTTPError as exc:
                    body = ""
                    try:
                        body = exc.read().decode("utf-8", errors="ignore")
                    except Exception:
                        body = ""
                    last_error = f"host={host} status={exc.code} body={body[:180]}"
                    if exc.code == 429 and i < attempts - 1:
                        await asyncio.sleep(max(1.0, min_gap))
                        continue
                    if exc.code in {403, 429, 500, 502, 503, 504}:
                        print(f"[{_now()}] discord route failed: host={host} status={exc.code}")
                        break
                    if i < attempts - 1:
                        await asyncio.sleep(max(0.5, min_gap))
                        continue
                    break
                except Exception as exc:
                    last_error = f"host={host} error={exc!r}"
                    if i < attempts - 1:
                        await asyncio.sleep(max(0.5, min_gap))
                        continue
                    print(f"[{_now()}] discord route exception: host={host} err={exc!r}")
                    break
            if route_pos < n_routes - 1:
                await asyncio.sleep(max(0.1, min_gap))
        print(f"[{_now()}] discord push failed after fallback: {last_error}")

    async def _emit_alert(self, alert: InflowAlert) -> None:
        console_text = self._format_alert_compact(alert)
        discord_text = self._format_alert_discord(alert)
        print(console_text)
        await self._send_discord(discord_text)

    async def _build_ranking_rows(
        self,
        snapshot_ts: int,
        board: str = RANKING_BOARD_GLOBAL,
        exchange_filter: Optional[Set[str]] = None,
    ) -> Tuple[List[Tuple[str, float, float]], List[RankingRow]]:
        await self._refresh_metadata_if_needed()
        board_key = str(board or RANKING_BOARD_GLOBAL).upper().strip() or RANKING_BOARD_GLOBAL
        history_map = self._load_recent_24h_means(snapshot_ts, board=board_key)
        ex_filter = None
        if exchange_filter:
            ex_filter = {str(x).upper().strip() for x in exchange_filter if str(x).strip()}

        coin_rows: List[Tuple[str, float, float]] = []
        ranking_data: List[RankingRow] = []
        now_ms = int(snapshot_ts) * 1000

        for base_symbol, ex_map in list(self.coin_exchange_windows.items()):
            inflow = 0.0
            outflow = 0.0
            quote_volume = 0.0
            top_exchange = "N/A"
            top_exchange_net = float("-inf")
            has_match = False
            for ex_name, ex_win in list(ex_map.items()):
                ex_key = str(ex_name).upper().strip()
                if ex_filter is not None and ex_key not in ex_filter:
                    continue
                has_match = True
                ex_stats = ex_win.snapshot(now_ms)
                inflow += float(ex_stats.inflow)
                outflow += float(ex_stats.outflow)
                quote_volume += float(ex_stats.quote_volume)
                ex_net = float(ex_stats.net_inflow)
                if ex_net > top_exchange_net:
                    top_exchange = ex_key
                    top_exchange_net = ex_net
            if not has_match:
                continue
            net = inflow - outflow
            if net <= 0:
                continue
            if self.config.min_net_inflow_usdt > 0 and net < self.config.min_net_inflow_usdt:
                continue
            if self.config.min_quote_volume_usdt > 0 and quote_volume < self.config.min_quote_volume_usdt:
                continue
            if self._should_exclude_base(base_symbol):
                continue

            if top_exchange_net == float("-inf"):
                top_exchange_net = 0.0

            coin_name, market_cap, fdv = self._coin_symbol_meta.get(base_symbol, (base_symbol, None, None))
            avg_24h, n_hist = history_map.get(base_symbol, (0.0, 0))
            growth_multiple: Optional[float] = None
            if n_hist > 0 and abs(float(avg_24h)) > 1e-9:
                growth_multiple = net / float(avg_24h)

            coin_rows.append((base_symbol, net, quote_volume))
            ranking_data.append(
                RankingRow(
                    rank=0,
                    base_symbol=base_symbol,
                    coin_name=coin_name,
                    market_cap_usd=market_cap,
                    fdv_usd=fdv,
                    net_inflow_usdt=net,
                    top_exchange=top_exchange,
                    top_exchange_net_inflow_usdt=top_exchange_net,
                    growth_multiple=growth_multiple,
                    history_points=n_hist,
                )
            )

        ranking_data.sort(key=lambda x: x.net_inflow_usdt, reverse=True)
        ranked: List[RankingRow] = []
        for i, row in enumerate(ranking_data, start=1):
            ranked.append(
                RankingRow(
                    rank=i,
                    base_symbol=row.base_symbol,
                    coin_name=row.coin_name,
                    market_cap_usd=row.market_cap_usd,
                    fdv_usd=row.fdv_usd,
                    net_inflow_usdt=row.net_inflow_usdt,
                    top_exchange=row.top_exchange,
                    top_exchange_net_inflow_usdt=row.top_exchange_net_inflow_usdt,
                    growth_multiple=row.growth_multiple,
                    history_points=row.history_points,
                )
            )
        return coin_rows, ranked

    async def _ranking_loop(self) -> None:
        while True:
            next_ts = self._next_boundary_ts(time.time(), self.config.ranking_interval_sec)
            wait_s = max(0.2, float(next_ts) - time.time())
            await asyncio.sleep(wait_s)

            snapshot_ts = int(next_ts)
            try:
                db_top_n = max(1, int(self.config.ranking_db_top_n))
                push_top_n = max(1, int(self.config.ranking_push_top_n))
                pushed_rows: Dict[str, List[RankingRow]] = {}
                for board, ex_filter in self._ranking_boards():
                    coin_rows, ranked_rows = await self._build_ranking_rows(
                        snapshot_ts,
                        board=board,
                        exchange_filter=ex_filter,
                    )
                    top_rows = list(ranked_rows[:push_top_n])
                    pushed_rows[str(board).upper().strip()] = top_rows
                    self._store_snapshot(snapshot_ts, coin_rows, ranked_rows[:db_top_n], board=board)
                    top_counts = self._load_recent_24h_top_counts(
                        snapshot_ts,
                        push_top_n,
                        board=board,
                    )
                    discord_msg = self._format_ranking_discord(
                        snapshot_ts,
                        top_rows,
                        top_counts_24h=top_counts,
                        board=board,
                    )
                    print(
                        f"[{_now()}] ranking snapshot saved: board={board.lower()} candidates={len(coin_rows)} "
                        f"db_top={min(len(ranked_rows), db_top_n)} push_top={min(len(ranked_rows), push_top_n)}"
                    )
                    await self._send_discord(discord_msg)
                if RANKING_BOARD_KOREA in pushed_rows:
                    global_rows = pushed_rows.get(RANKING_BOARD_GLOBAL, [])
                    korea_rows = pushed_rows.get(RANKING_BOARD_KOREA, [])
                    cross_msg = self._format_cross_board_summary_discord(
                        global_rows=global_rows,
                        korea_rows=korea_rows,
                    )
                    korea_symbols = {str(x.base_symbol).upper() for x in korea_rows}
                    overlap_n = sum(1 for r in global_rows if str(r.base_symbol).upper() in korea_symbols)
                    print(
                        f"[{_now()}] ranking cross summary: overlap={overlap_n}"
                    )
                    await self._send_discord(cross_msg)
            except Exception as exc:
                print(f"[{_now()}] ranking snapshot failed: {exc!r}")

    async def _evaluation_loop(self) -> None:
        interval = max(0.2, float(self.config.snapshot_interval_sec))
        while True:
            now_ms = int(time.time() * 1000)
            for scope in list(self.states.keys()):
                alert = self.evaluate_scope(scope, now_ms=now_ms)
                if alert is not None:
                    await self._emit_alert(alert)
            await asyncio.sleep(interval)

    def _top_coin_stats(self, now_ms: int, top_k: int) -> List[Tuple[str, WindowStats]]:
        rows: List[Tuple[str, WindowStats]] = []
        for base_symbol, win in list(self.coin_windows.items()):
            stats = win.snapshot(now_ms)
            if stats.quote_volume <= 0:
                continue
            rows.append((base_symbol, stats))
        rows.sort(key=lambda x: x[1].net_inflow, reverse=True)
        return rows[: max(1, int(top_k))]

    async def _status_loop(self) -> None:
        interval = max(5.0, float(self.config.status_interval_sec))
        while True:
            await asyncio.sleep(interval)
            now_ms = int(time.time() * 1000)
            top_rows = self._top_coin_stats(now_ms, 5)
            if top_rows:
                text = " | ".join(
                    (
                        f"{base} net={_fmt_signed(st.net_inflow)} "
                        f"quote={_fmt_unsigned(st.quote_volume)} buy={st.buy_ratio * 100:.1f}%"
                    )
                    for base, st in top_rows
                )
                print(f"[{_now()}] heartbeat top-coins: {text}")

    async def _create_exchange(self, exchange_id: str) -> Any:
        if ccxt_async is None:
            raise RuntimeError(f"ccxt.async_support import failed: {_CCXT_IMPORT_ERROR!r}")
        cls = getattr(ccxt_async, str(exchange_id), None)
        if cls is None:
            raise RuntimeError(f"exchange not found in ccxt: {exchange_id}")
        ex = cls(
            {
                "enableRateLimit": True,
                "timeout": max(5_000, int(self.config.exchange_timeout_ms)),
                "options": {
                    "defaultType": "spot",
                    # Reduce startup latency and timeout risk on exchanges that have slow currency endpoints.
                    "fetchCurrencies": False,
                },
            }
        )
        try:
            await ex.load_markets()
            return ex
        except Exception:
            try:
                await ex.close()
            except Exception:
                pass
            raise

    @staticmethod
    def _trade_key(trade: Dict[str, Any]) -> str:
        tid = str(trade.get("id", "")).strip()
        if tid:
            return f"id:{tid}"
        ts = int(trade.get("timestamp", 0) or 0)
        side = str(trade.get("side", "")).lower().strip()
        price = float(trade.get("price", 0.0) or 0.0)
        amount = float(trade.get("amount", 0.0) or 0.0)
        return f"raw:{ts}:{side}:{price:.10f}:{amount:.10f}"

    def _extract_trade(self, raw: Dict[str, Any]) -> Optional[Tuple[int, float, bool]]:
        try:
            ts = int(raw.get("timestamp", 0) or 0)
            side = str(raw.get("side", "")).strip().lower()
            if ts <= 0 or side not in {"buy", "sell"}:
                return None
            cost = float(raw.get("cost", 0.0) or 0.0)
            if cost <= 0:
                price = float(raw.get("price", 0.0) or 0.0)
                amount = float(raw.get("amount", 0.0) or 0.0)
                cost = price * amount
            if cost <= 0:
                return None
            is_taker_buy = side == "buy"
            return (ts, cost, is_taker_buy)
        except Exception:
            return None

    async def _load_direct_spot_pairs(self, exchange_id: str) -> Dict[str, Tuple[str, str]]:
        ex_id = str(exchange_id or "").strip().lower()
        timeout = max(2.0, float(self.config.exchange_timeout_ms) / 1000.0)
        if ex_id == "upbit":
            url = "https://api.upbit.com/v1/market/all?isDetails=false"
        elif ex_id == "bithumb":
            url = "https://api.bithumb.com/public/ticker/ALL_KRW"
        else:
            raise RuntimeError(f"direct market loader not supported: {exchange_id}")
        payload = await asyncio.to_thread(self._http_get_json, url, timeout)
        markets = build_direct_markets(ex_id, payload)
        exchange = _StaticMarketsExchange(markets)
        return resolve_spot_pairs(
            ex_id,
            exchange,
            self.config.symbols,
            self.config.quote_whitelist,
        )

    async def _fetch_direct_trades(self, exchange_id: str, market_symbol: str, limit: int) -> List[Dict[str, Any]]:
        ex_id = str(exchange_id or "").strip().lower()
        timeout = max(2.0, float(self.config.exchange_timeout_ms) / 1000.0)
        capped_limit = max(1, min(int(limit), 100))
        if ex_id == "upbit":
            capped_limit = min(capped_limit, 200)
            url = (
                "https://api.upbit.com/v1/trades/ticks?"
                f"market={urllib.parse.quote(str(market_symbol), safe='')}&count={capped_limit}"
            )
        elif ex_id == "bithumb":
            url = (
                "https://api.bithumb.com/public/transaction_history/"
                f"{urllib.parse.quote(str(market_symbol), safe='')}?count={capped_limit}"
            )
        else:
            raise RuntimeError(f"direct trade fetch not supported: {exchange_id}")
        payload = await asyncio.to_thread(self._http_get_json, url, timeout)
        return parse_direct_trades(ex_id, payload)

    async def _poll_direct_exchange(self, exchange_id: str) -> None:
        retry_count = 0
        ex_id = str(exchange_id or "").strip().lower()
        while True:
            try:
                resolved = await self._load_direct_spot_pairs(ex_id)
                if not resolved:
                    print(f"[{_now()}] exchange={ex_id} no requested spot pairs available, skip.")
                    return
                cursors = [
                    TradeCursor(
                        exchange_id=ex_id,
                        requested_pair=pair,
                        market_symbol=market_symbol,
                        market_quote=market_quote,
                    )
                    for pair, (market_symbol, market_quote) in resolved.items()
                ]
                cursor_idx = 0
                pairs_per_cycle = self._pairs_per_cycle_for_exchange(ex_id)
                print(
                    f"[{_now()}] exchange={ex_id} direct-http started with {len(cursors)} pairs: "
                    + (",".join(pair for pair, _ in list(resolved.items())[:8]) + ("..." if len(resolved) > 8 else ""))
                    + f" | scan={min(len(cursors), pairs_per_cycle)}/{len(cursors)} per cycle"
                )
                retry_count = 0
                while True:
                    loop_start = time.time()
                    take = min(len(cursors), pairs_per_cycle)
                    batch = [cursors[(cursor_idx + i) % len(cursors)] for i in range(take)]
                    cursor_idx = (cursor_idx + take) % len(cursors)
                    for cursor in batch:
                        try:
                            trades = await self._fetch_direct_trades(
                                ex_id,
                                cursor.market_symbol,
                                self.config.poll_limit,
                            )
                        except Exception as exc:
                            print(
                                f"[{_now()}] exchange={ex_id} pair={cursor.market_symbol} "
                                f"direct fetch failed: {exc!r}"
                            )
                            continue
                        if not trades:
                            continue
                        trades_sorted = sorted(
                            [x for x in trades if isinstance(x, dict)],
                            key=lambda x: int(x.get("timestamp", 0) or 0),
                        )
                        for trade in trades_sorted:
                            payload = self._extract_trade(trade)
                            if payload is None:
                                continue
                            ts, quote_value, is_taker_buy = payload
                            if ts < cursor.last_ts_ms:
                                continue
                            tkey = self._trade_key(trade)
                            if ts == cursor.last_ts_ms and not cursor.remember(
                                tkey, self.config.duplicate_cache_size
                            ):
                                continue
                            if ts > cursor.last_ts_ms:
                                cursor.last_ts_ms = ts
                                cursor.seen_queue.clear()
                                cursor.seen_set.clear()
                                cursor.remember(tkey, self.config.duplicate_cache_size)
                            self.ingest_trade(
                                exchange_id=ex_id,
                                pair=cursor.requested_pair,
                                ts_ms=ts,
                                quote_value=self._normalize_quote_to_usdt(quote_value, cursor.market_quote),
                                is_taker_buy=is_taker_buy,
                            )
                        await asyncio.sleep(max(0.0, float(self.config.request_gap_sec)))
                    elapsed = max(0.0, time.time() - loop_start)
                    sleep_s = max(0.0, float(self.config.poll_interval_sec) - elapsed)
                    await asyncio.sleep(sleep_s)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                retry_count += 1
                backoff = min(
                    float(self.config.exchange_retry_max_sec),
                    float(self.config.exchange_retry_base_sec) * (2 ** (retry_count - 1)),
                )
                wait_s = backoff + random.uniform(0.0, 0.6)
                print(
                    f"[{_now()}] exchange={ex_id} direct loop failed: {exc!r}, "
                    f"retry in {wait_s:.2f}s"
                )
                await asyncio.sleep(max(0.5, wait_s))

    async def _poll_exchange(self, exchange_id: str) -> None:
        if str(exchange_id or "").strip().lower() in DIRECT_HTTP_EXCHANGES:
            await self._poll_direct_exchange(exchange_id)
            return
        retry_count = 0
        while True:
            exchange = None
            try:
                exchange = await self._create_exchange(exchange_id)
                resolved = resolve_spot_pairs(
                    exchange_id,
                    exchange,
                    self.config.symbols,
                    self.config.quote_whitelist,
                )
                if not resolved:
                    print(
                        f"[{_now()}] exchange={exchange_id} no requested spot pairs available, skip."
                    )
                    return
                cursors = [
                    TradeCursor(
                        exchange_id=exchange_id,
                        requested_pair=pair,
                        market_symbol=market_symbol,
                        market_quote=market_quote,
                    )
                    for pair, (market_symbol, market_quote) in resolved.items()
                ]
                cursor_idx = 0
                pairs_per_cycle = self._pairs_per_cycle_for_exchange(exchange_id)
                print(
                    f"[{_now()}] exchange={exchange_id} started with {len(cursors)} pairs: "
                    + (",".join(pair for pair, _ in list(resolved.items())[:8]) + ("..." if len(resolved) > 8 else ""))
                    + f" | scan={min(len(cursors), pairs_per_cycle)}/{len(cursors)} per cycle"
                )
                retry_count = 0
                while True:
                    loop_start = time.time()
                    if not cursors:
                        await asyncio.sleep(float(self.config.poll_interval_sec))
                        continue
                    take = min(len(cursors), pairs_per_cycle)
                    batch = [cursors[(cursor_idx + i) % len(cursors)] for i in range(take)]
                    cursor_idx = (cursor_idx + take) % len(cursors)
                    for cursor in batch:
                        try:
                            trades = await exchange.fetch_trades(cursor.market_symbol, limit=self.config.poll_limit)
                        except Exception as exc:
                            print(
                                f"[{_now()}] exchange={exchange_id} pair={cursor.market_symbol} "
                                f"fetch_trades failed: {exc!r}"
                            )
                            continue
                        if not isinstance(trades, list) or not trades:
                            continue
                        trades_sorted = sorted(
                            [x for x in trades if isinstance(x, dict)],
                            key=lambda x: int(x.get("timestamp", 0) or 0),
                        )
                        for trade in trades_sorted:
                            payload = self._extract_trade(trade)
                            if payload is None:
                                continue
                            ts, quote_value, is_taker_buy = payload
                            if ts < cursor.last_ts_ms:
                                continue
                            tkey = self._trade_key(trade)
                            if ts == cursor.last_ts_ms and not cursor.remember(
                                tkey, self.config.duplicate_cache_size
                            ):
                                continue
                            if ts > cursor.last_ts_ms:
                                cursor.last_ts_ms = ts
                                cursor.seen_queue.clear()
                                cursor.seen_set.clear()
                                cursor.remember(tkey, self.config.duplicate_cache_size)
                            self.ingest_trade(
                                exchange_id=exchange_id,
                                pair=cursor.requested_pair,
                                ts_ms=ts,
                                quote_value=self._normalize_quote_to_usdt(quote_value, cursor.market_quote),
                                is_taker_buy=is_taker_buy,
                            )
                        await asyncio.sleep(max(0.0, float(self.config.request_gap_sec)))
                    elapsed = max(0.0, time.time() - loop_start)
                    sleep_s = max(0.0, float(self.config.poll_interval_sec) - elapsed)
                    await asyncio.sleep(sleep_s)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                retry_count += 1
                backoff = min(
                    float(self.config.exchange_retry_max_sec),
                    float(self.config.exchange_retry_base_sec) * (2 ** (retry_count - 1)),
                )
                wait_s = backoff + random.uniform(0.0, 0.6)
                print(
                    f"[{_now()}] exchange={exchange_id} loop failed: {exc!r}, "
                    f"retry in {wait_s:.2f}s"
                )
                await asyncio.sleep(max(0.5, wait_s))
            finally:
                if exchange is not None:
                    try:
                        await exchange.close()
                    except Exception:
                        pass

    async def _poll_exchange_with_start_delay(self, exchange_id: str, delay_sec: float) -> None:
        d = max(0.0, float(delay_sec))
        if d > 0:
            await asyncio.sleep(d)
        await self._poll_exchange(exchange_id)

    async def _run_guarded(self, task_name: str, worker: Callable[[], Awaitable[None]]) -> None:
        restart_count = 0
        while True:
            try:
                await worker()
                restart_count = 0
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                restart_count += 1
                backoff = min(15.0, 1.5 * float(restart_count))
                wait_s = backoff + random.uniform(0.0, 0.6)
                print(f"[{_now()}] task={task_name} crashed: {exc!r}, restart in {wait_s:.2f}s")
                await asyncio.sleep(wait_s)

    async def run(self) -> None:
        if ccxt_async is None:
            raise RuntimeError(f"ccxt.async_support import failed: {_CCXT_IMPORT_ERROR!r}")
        if not self.config.include_aggregate and not self.config.include_per_exchange:
            raise RuntimeError("at least one of include_aggregate/include_per_exchange must be true")
        if not self.config.exchanges:
            raise RuntimeError("empty exchange list")

        timeout = aiohttp.ClientTimeout(total=max(2.0, float(self.config.webhook_timeout_sec)))
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            tasks: List[asyncio.Task[Any]] = [
                asyncio.create_task(
                    self._run_guarded("multi_spot_ranking", self._ranking_loop),
                    name="multi_spot_ranking",
                ),
                asyncio.create_task(
                    self._run_guarded("multi_spot_status", self._status_loop),
                    name="multi_spot_status",
                ),
            ]
            start_gap = max(0.0, float(self.config.exchange_start_jitter_sec))
            for idx, exchange_id in enumerate(self.config.exchanges):
                start_delay = start_gap * float(idx)
                tasks.append(
                    asyncio.create_task(
                        self._run_guarded(
                            f"poll:{exchange_id}",
                            lambda ex=exchange_id, d=start_delay: self._poll_exchange_with_start_delay(ex, d),
                        ),
                        name=f"poll:{exchange_id}",
                    )
                )
            try:
                await asyncio.gather(*tasks)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self.session = None


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Multi-exchange spot incremental inflow monitor.")
    p.add_argument(
        "--exchanges",
        type=str,
        default=os.getenv("SPOT_INFLOW_EXCHANGES", ",".join(DEFAULT_EXCHANGES)).strip(),
        help="Comma separated ccxt exchange ids, or all",
    )
    p.add_argument("--max-exchanges", type=int, default=_env_int("SPOT_INFLOW_MAX_EXCHANGES", 30))
    p.add_argument(
        "--symbols",
        type=str,
        default=os.getenv("SPOT_INFLOW_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").strip(),
        help="Comma separated symbols, supports BTCUSDT or BTC/USDT, or use all.",
    )
    p.add_argument(
        "--quote-whitelist",
        type=str,
        default=os.getenv("SPOT_INFLOW_QUOTE_WHITELIST", ",".join(DEFAULT_QUOTE_WHITELIST)).strip(),
        help="Quotes to include when symbols=all, or all.",
    )
    p.add_argument(
        "--include-korea-exchanges",
        type=_parse_bool_arg,
        default=_env_bool("SPOT_INFLOW_INCLUDE_KOREA_EXCHANGES", True),
        help="Append Korean exchanges (upbit,bithumb,coinone,korbit,gopax).",
    )
    p.add_argument("--include-aggregate", type=_parse_bool_arg, default=_env_bool("SPOT_INFLOW_INCLUDE_AGGREGATE", True))
    p.add_argument("--include-per-exchange", type=_parse_bool_arg, default=_env_bool("SPOT_INFLOW_INCLUDE_PER_EXCHANGE", True))
    p.add_argument("--window-sec", type=float, default=_env_float("SPOT_INFLOW_WINDOW_SEC", 1800.0))
    p.add_argument("--ranking-interval-sec", type=float, default=_env_float("SPOT_INFLOW_RANKING_INTERVAL_SEC", 1800.0))
    p.add_argument("--ranking-push-top-n", type=int, default=_env_int("SPOT_INFLOW_RANKING_PUSH_TOP_N", 20))
    p.add_argument("--ranking-db-top-n", type=int, default=_env_int("SPOT_INFLOW_RANKING_DB_TOP_N", 100))
    p.add_argument(
        "--ranking-db-file",
        type=str,
        default=os.getenv("SPOT_INFLOW_RANKING_DB_FILE", "data/logs/spot_inflow_rankings.db").strip(),
    )
    p.add_argument("--metadata-refresh-sec", type=float, default=_env_float("SPOT_INFLOW_METADATA_REFRESH_SEC", 21600.0))
    p.add_argument("--coingecko-pages", type=int, default=_env_int("SPOT_INFLOW_COINGECKO_PAGES", 4))
    p.add_argument("--exclude-top-marketcap-n", type=int, default=_env_int("SPOT_INFLOW_EXCLUDE_TOP_MCAP_N", 20))
    p.add_argument("--exclude-stablecoins", type=_parse_bool_arg, default=_env_bool("SPOT_INFLOW_EXCLUDE_STABLECOINS", True))
    p.add_argument(
        "--require-binance-futures",
        type=_parse_bool_arg,
        default=_env_bool("SPOT_INFLOW_REQUIRE_BINANCE_FUTURES", True),
    )
    p.add_argument("--poll-interval-sec", type=float, default=_env_float("SPOT_INFLOW_POLL_INTERVAL_SEC", 2.5))
    p.add_argument("--poll-limit", type=int, default=_env_int("SPOT_INFLOW_POLL_LIMIT", 120))
    p.add_argument("--max-pairs-per-cycle", type=int, default=_env_int("SPOT_INFLOW_MAX_PAIRS_PER_CYCLE", 50))
    p.add_argument(
        "--korea-max-pairs-per-cycle",
        type=int,
        default=_env_int("SPOT_INFLOW_KOREA_MAX_PAIRS_PER_CYCLE", 20),
        help="Max pairs per cycle for Korean exchanges to reduce timeout pressure.",
    )
    p.add_argument(
        "--exchange-timeout-ms",
        type=int,
        default=_env_int("SPOT_INFLOW_EXCHANGE_TIMEOUT_MS", 30000),
        help="Per-request timeout for ccxt exchange REST calls.",
    )
    p.add_argument(
        "--exchange-start-jitter-sec",
        type=float,
        default=_env_float("SPOT_INFLOW_EXCHANGE_START_JITTER_SEC", 0.4),
        help="Stagger exchange startup to avoid thundering-herd timeouts.",
    )
    p.add_argument("--request-gap-sec", type=float, default=_env_float("SPOT_INFLOW_REQUEST_GAP_SEC", 0.08))
    p.add_argument("--snapshot-interval-sec", type=float, default=_env_float("SPOT_INFLOW_SNAPSHOT_INTERVAL_SEC", 2.0))
    p.add_argument("--status-interval-sec", type=float, default=_env_float("SPOT_INFLOW_STATUS_INTERVAL_SEC", 30.0))
    p.add_argument("--min-net-inflow-usdt", type=float, default=_env_float("SPOT_INFLOW_MIN_NET_USDT", 0.0))
    p.add_argument("--min-increment-usdt", type=float, default=_env_float("SPOT_INFLOW_MIN_INC_USDT", 0.0))
    p.add_argument("--min-quote-volume-usdt", type=float, default=_env_float("SPOT_INFLOW_MIN_QUOTE_USDT", 0.0))
    p.add_argument("--min-buy-ratio", type=float, default=_env_float("SPOT_INFLOW_MIN_BUY_RATIO", 0.55))
    p.add_argument("--min-zscore", type=float, default=_env_float("SPOT_INFLOW_MIN_ZSCORE", 1.6))
    p.add_argument("--zscore-lookback", type=int, default=_env_int("SPOT_INFLOW_ZSCORE_LOOKBACK", 120))
    p.add_argument("--zscore-min-samples", type=int, default=_env_int("SPOT_INFLOW_ZSCORE_MIN_SAMPLES", 30))
    p.add_argument("--alert-cooldown-sec", type=float, default=_env_float("SPOT_INFLOW_ALERT_COOLDOWN_SEC", 180.0))
    p.add_argument("--exchange-retry-base-sec", type=float, default=_env_float("SPOT_INFLOW_EX_RETRY_BASE_SEC", 2.0))
    p.add_argument("--exchange-retry-max-sec", type=float, default=_env_float("SPOT_INFLOW_EX_RETRY_MAX_SEC", 45.0))
    p.add_argument("--duplicate-cache-size", type=int, default=_env_int("SPOT_INFLOW_DUP_CACHE_SIZE", 2048))
    p.add_argument(
        "--krw-per-usdt",
        type=float,
        default=_env_float("SPOT_INFLOW_KRW_PER_USDT", 1400.0),
        help="KRW/USDT conversion used for KRW spot markets.",
    )
    p.add_argument("--webhook-timeout-sec", type=float, default=_env_float("SPOT_INFLOW_WEBHOOK_TIMEOUT_SEC", 8.0))
    p.add_argument("--discord-retry-max", type=int, default=_env_int("SPOT_INFLOW_DISCORD_RETRY_MAX", 3))
    p.add_argument(
        "--discord-min-interval-sec",
        type=float,
        default=_env_float("SPOT_INFLOW_DISCORD_MIN_INTERVAL_SEC", 0.25),
    )
    p.add_argument("--discord-webhook-url", type=str, default=os.getenv("DISCORD_WEBHOOK_URL", "").strip())
    p.add_argument("--dry-run", type=_parse_bool_arg, default=_env_bool("SPOT_INFLOW_DRY_RUN", False))
    return p


def _config_from_args(args: argparse.Namespace) -> MonitorConfig:
    exchanges = list(parse_exchanges(args.exchanges, max_exchanges=max(1, int(args.max_exchanges))))
    if bool(args.include_korea_exchanges):
        for ex in KOREA_EXCHANGES:
            if ex not in exchanges:
                exchanges.append(ex)
    if str(args.exchanges or "").strip().lower() == "all" and int(args.max_exchanges) > 0:
        exchanges = exchanges[: max(1, int(args.max_exchanges))]
    symbols = parse_symbols(args.symbols)
    quotes = parse_quote_whitelist(args.quote_whitelist)
    min_buy = max(0.0, min(1.0, float(args.min_buy_ratio)))
    webhook = "" if bool(args.dry_run) else str(args.discord_webhook_url or "").strip()
    return MonitorConfig(
        exchanges=tuple(exchanges),
        symbols=symbols,
        include_aggregate=bool(args.include_aggregate),
        include_per_exchange=bool(args.include_per_exchange),
        include_korea_exchanges=bool(args.include_korea_exchanges),
        quote_whitelist=quotes,
        all_symbols_mode=(ALL_SYMBOLS_TOKEN in set(symbols)),
        window_sec=max(60.0, float(args.window_sec)),
        ranking_interval_sec=max(300.0, float(args.ranking_interval_sec)),
        ranking_push_top_n=max(1, int(args.ranking_push_top_n)),
        ranking_db_top_n=max(1, int(args.ranking_db_top_n)),
        ranking_db_file=str(args.ranking_db_file or "data/logs/spot_inflow_rankings.db").strip()
        or "data/logs/spot_inflow_rankings.db",
        metadata_refresh_sec=max(600.0, float(args.metadata_refresh_sec)),
        coingecko_pages=max(1, min(8, int(args.coingecko_pages))),
        exclude_top_marketcap_n=max(0, int(args.exclude_top_marketcap_n)),
        exclude_stablecoins=bool(args.exclude_stablecoins),
        require_binance_futures=bool(args.require_binance_futures),
        poll_interval_sec=max(0.5, float(args.poll_interval_sec)),
        poll_limit=max(20, int(args.poll_limit)),
        max_pairs_per_cycle=max(1, int(args.max_pairs_per_cycle)),
        korea_max_pairs_per_cycle=max(1, int(args.korea_max_pairs_per_cycle)),
        exchange_timeout_ms=max(5_000, int(args.exchange_timeout_ms)),
        exchange_start_jitter_sec=max(0.0, float(args.exchange_start_jitter_sec)),
        request_gap_sec=max(0.0, float(args.request_gap_sec)),
        snapshot_interval_sec=max(0.2, float(args.snapshot_interval_sec)),
        status_interval_sec=max(5.0, float(args.status_interval_sec)),
        min_net_inflow_usdt=max(0.0, float(args.min_net_inflow_usdt)),
        min_increment_usdt=max(0.0, float(args.min_increment_usdt)),
        min_quote_volume_usdt=max(0.0, float(args.min_quote_volume_usdt)),
        min_buy_ratio=min_buy,
        min_zscore=max(0.0, float(args.min_zscore)),
        zscore_lookback=max(10, int(args.zscore_lookback)),
        zscore_min_samples=max(2, int(args.zscore_min_samples)),
        alert_cooldown_sec=max(0.0, float(args.alert_cooldown_sec)),
        exchange_retry_base_sec=max(0.5, float(args.exchange_retry_base_sec)),
        exchange_retry_max_sec=max(2.0, float(args.exchange_retry_max_sec)),
        duplicate_cache_size=max(100, int(args.duplicate_cache_size)),
        krw_per_usdt=max(100.0, float(args.krw_per_usdt)),
        discord_webhook_url=webhook,
        webhook_timeout_sec=max(2.0, float(args.webhook_timeout_sec)),
        discord_retry_max=max(1, int(args.discord_retry_max)),
        discord_min_interval_sec=max(0.0, float(args.discord_min_interval_sec)),
        dry_run=bool(args.dry_run),
    )


async def _amain(args: argparse.Namespace) -> None:
    cfg = _config_from_args(args)
    monitor = MultiExchangeSpotInflowMonitor(cfg)
    await monitor.run()


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    cfg = _config_from_args(args)
    korea_board_exchanges = [
        ex for ex in cfg.exchanges if str(ex).strip().lower() in set(KOREA_EXCHANGES)
    ]
    print("=" * 72)
    print("Spot Inflow Multi-Exchange Monitor")
    print("=" * 72)
    print(
        f"exchanges={len(cfg.exchanges)} "
        f"symbols={'ALL' if cfg.all_symbols_mode else ','.join(cfg.symbols)} "
        f"quotes={'ALL' if not cfg.quote_whitelist else ','.join(cfg.quote_whitelist)} "
        f"window={int(cfg.window_sec)}s "
        f"ranking_interval={int(cfg.ranking_interval_sec)}s "
        f"push_top={cfg.ranking_push_top_n} "
        f"db_top={cfg.ranking_db_top_n} "
        f"poll={cfg.poll_interval_sec:.2f}s "
        f"pairs_per_cycle={cfg.max_pairs_per_cycle} "
        f"korea_pairs_per_cycle={cfg.korea_max_pairs_per_cycle} "
        f"exchange_timeout_ms={cfg.exchange_timeout_ms} "
        f"start_jitter={cfg.exchange_start_jitter_sec:.2f}s "
        f"min_net={cfg.min_net_inflow_usdt:.0f} "
        f"min_inc={cfg.min_increment_usdt:.0f} "
        f"min_quote={cfg.min_quote_volume_usdt:.0f} "
        f"min_buy={cfg.min_buy_ratio:.2f} "
        f"min_z={cfg.min_zscore:.2f} "
        f"aggregate={'on' if cfg.include_aggregate else 'off'} "
        f"per_exchange={'on' if cfg.include_per_exchange else 'off'} "
        f"korea={'on' if cfg.include_korea_exchanges else 'off'} "
        f"korea_board={','.join(korea_board_exchanges) if korea_board_exchanges else 'off'} "
        f"exclude_top_mcap={cfg.exclude_top_marketcap_n} "
        f"exclude_stable={'on' if cfg.exclude_stablecoins else 'off'} "
        f"need_binance_futures={'on' if cfg.require_binance_futures else 'off'} "
        f"krw_per_usdt={cfg.krw_per_usdt:.2f} "
        f"discord={'on' if cfg.discord_webhook_url else 'off'} "
        f"dry_run={'on' if cfg.dry_run else 'off'}"
    )
    if len(cfg.exchanges) > 30:
        print(
            f"[{_now()}] warning: configured exchanges={len(cfg.exchanges)}. "
            "Huge fan-out may trigger rate limits and unstable connectivity."
        )
    try:
        asyncio.run(_amain(args))
    except KeyboardInterrupt:
        print(f"\n[{_now()}] monitor stopped by user.")
    except Exception as exc:
        print(f"[{_now()}] fatal error: {exc!r}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
