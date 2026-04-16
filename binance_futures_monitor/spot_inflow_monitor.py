#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Binance spot incremental inflow monitor based on aggTrade stream."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional, Sequence, Tuple

import aiohttp

try:
    import websockets
except Exception as exc:  # pragma: no cover
    websockets = None  # type: ignore[assignment]
    _WS_IMPORT_ERROR = exc
else:
    _WS_IMPORT_ERROR = None


DEFAULT_SYMBOLS: Tuple[str, ...] = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT")
WS_BASE_URL = "wss://stream.binance.com:9443/stream"


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
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_bool_arg(v: str) -> bool:
    s = str(v or "").strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid bool value: {v}")


def _parse_symbols(raw: str) -> Tuple[str, ...]:
    out: List[str] = []
    for token in str(raw or "").split(","):
        symbol = token.strip().upper().replace("/", "")
        if not symbol:
            continue
        if symbol.endswith("USDT"):
            out.append(symbol)
    if not out:
        out = list(DEFAULT_SYMBOLS)
    return tuple(dict.fromkeys(out))


def _fmt_signed(value: float) -> str:
    sign = "+" if value >= 0 else "-"
    x = abs(float(value))
    if x >= 1_000_000_000:
        return f"{sign}{x / 1_000_000_000:.2f}B"
    if x >= 1_000_000:
        return f"{sign}{x / 1_000_000:.2f}M"
    if x >= 1_000:
        return f"{sign}{x / 1_000:.2f}K"
    return f"{sign}{x:.2f}"


def _fmt_unsigned(value: float) -> str:
    x = max(0.0, float(value))
    if x >= 1_000_000_000:
        return f"{x / 1_000_000_000:.2f}B"
    if x >= 1_000_000:
        return f"{x / 1_000_000:.2f}M"
    if x >= 1_000:
        return f"{x / 1_000:.2f}K"
    return f"{x:.2f}"


def _zscore(value: float, series: Sequence[float]) -> float:
    if len(series) < 2:
        return 0.0
    mean = float(statistics.mean(series))
    std = float(statistics.pstdev(series))
    if std <= 1e-12:
        return 0.0
    return (float(value) - mean) / std


@dataclass(frozen=True)
class MonitorConfig:
    symbols: Tuple[str, ...] = DEFAULT_SYMBOLS
    window_sec: float = 60.0
    snapshot_interval_sec: float = 2.0
    status_interval_sec: float = 30.0
    min_net_inflow_usdt: float = 150_000.0
    min_increment_usdt: float = 60_000.0
    min_quote_volume_usdt: float = 280_000.0
    min_buy_ratio: float = 0.56
    min_zscore: float = 1.8
    zscore_lookback: int = 120
    zscore_min_samples: int = 30
    alert_cooldown_sec: float = 120.0
    reconnect_base_sec: float = 1.0
    reconnect_max_sec: float = 12.0
    discord_webhook_url: str = ""
    webhook_timeout_sec: float = 8.0
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
    symbol: str
    ts_ms: int
    stats: WindowStats
    increment: float
    zscore: float


class RollingTradeWindow:
    """Sliding window for spot taker flow."""

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
        quote_volume = max(0.0, float(self.quote_volume))
        inflow = max(0.0, float(self.inflow))
        outflow = max(0.0, float(self.outflow))
        buy_ratio = inflow / quote_volume if quote_volume > 0 else 0.0
        return WindowStats(
            net_inflow=inflow - outflow,
            inflow=inflow,
            outflow=outflow,
            quote_volume=quote_volume,
            buy_ratio=buy_ratio,
            trades=len(self.rows),
        )


@dataclass
class SymbolState:
    window: RollingTradeWindow
    net_history: Deque[float] = field(default_factory=deque)
    prev_net_inflow: Optional[float] = None
    last_seen_ms: int = 0
    last_alert_ms: int = 0


class SpotInflowMonitor:
    def __init__(self, config: MonitorConfig) -> None:
        self.config = config
        self.states: Dict[str, SymbolState] = {}
        self.session: Optional[aiohttp.ClientSession] = None
        self._history_maxlen = max(2, int(config.zscore_lookback))
        for symbol in config.symbols:
            self._ensure_state(symbol)

    def _ensure_state(self, symbol: str) -> SymbolState:
        s = str(symbol or "").strip().upper()
        state = self.states.get(s)
        if state is not None:
            return state
        state = SymbolState(
            window=RollingTradeWindow(self.config.window_sec),
            net_history=deque(maxlen=self._history_maxlen),
        )
        self.states[s] = state
        return state

    def ingest_agg_trade(self, event: Dict[str, Any]) -> None:
        symbol = str(event.get("s", "")).strip().upper()
        if not symbol:
            return
        state = self._ensure_state(symbol)
        try:
            price = float(event.get("p", 0.0) or 0.0)
            qty = float(event.get("q", 0.0) or 0.0)
            if price <= 0.0 or qty <= 0.0:
                return
            buyer_is_maker = bool(event.get("m", False))
            ts_ms = int(event.get("T", event.get("E", int(time.time() * 1000))) or 0)
        except Exception:
            return
        quote_value = price * qty
        is_taker_buy = not buyer_is_maker
        state.window.add(ts_ms=ts_ms, quote_value=quote_value, is_taker_buy=is_taker_buy)
        state.last_seen_ms = max(state.last_seen_ms, ts_ms)

    def evaluate_symbol(self, symbol: str, now_ms: int) -> Optional[InflowAlert]:
        state = self._ensure_state(symbol)
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
            symbol=str(symbol).upper(),
            ts_ms=int(now_ms),
            stats=stats,
            increment=increment,
            zscore=z,
        )

    def _stream_url(self) -> str:
        streams = "/".join(f"{symbol.lower()}@aggTrade" for symbol in self.config.symbols)
        return f"{WS_BASE_URL}?streams={streams}"

    def _format_alert(self, alert: InflowAlert) -> str:
        s = alert.stats
        return (
            f"[{_now()}] SPOT INFLOW ALERT {alert.symbol} "
            f"window={int(self.config.window_sec)}s "
            f"net={_fmt_signed(s.net_inflow)} "
            f"inc={_fmt_signed(alert.increment)} "
            f"quote={_fmt_unsigned(s.quote_volume)} "
            f"in={_fmt_unsigned(s.inflow)} "
            f"out={_fmt_unsigned(s.outflow)} "
            f"buy={s.buy_ratio * 100.0:.1f}% "
            f"z={alert.zscore:.2f} "
            f"trades={s.trades}"
        )

    async def _send_discord(self, text: str) -> None:
        webhook = str(self.config.discord_webhook_url or "").strip()
        if not webhook or self.config.dry_run:
            return
        if self.session is None:
            return
        try:
            async with self.session.post(webhook, json={"content": text}) as resp:
                if resp.status >= 300:
                    body = await resp.text()
                    print(f"[{_now()}] discord push failed: status={resp.status} body={body[:200]}")
        except Exception as exc:
            print(f"[{_now()}] discord push exception: {exc!r}")

    async def _emit_alert(self, alert: InflowAlert) -> None:
        text = self._format_alert(alert)
        print(text)
        await self._send_discord(text)

    async def _evaluation_loop(self) -> None:
        interval = max(0.2, float(self.config.snapshot_interval_sec))
        while True:
            now_ms = int(time.time() * 1000)
            for symbol in list(self.states.keys()):
                alert = self.evaluate_symbol(symbol, now_ms=now_ms)
                if alert is not None:
                    await self._emit_alert(alert)
            await asyncio.sleep(interval)

    async def _status_loop(self) -> None:
        interval = max(3.0, float(self.config.status_interval_sec))
        while True:
            await asyncio.sleep(interval)
            now_ms = int(time.time() * 1000)
            rows: List[Tuple[str, float, float, float]] = []
            for symbol, state in self.states.items():
                stats = state.window.snapshot(now_ms)
                if stats.quote_volume <= 0:
                    continue
                rows.append((symbol, stats.net_inflow, stats.quote_volume, stats.buy_ratio))
            rows.sort(key=lambda x: x[1], reverse=True)
            if not rows:
                print(f"[{_now()}] heartbeat: waiting for spot trades...")
                continue
            top = rows[:5]
            pieces = [
                f"{symbol} net={_fmt_signed(net)} quote={_fmt_unsigned(qv)} buy={buy_ratio * 100:.1f}%"
                for symbol, net, qv, buy_ratio in top
            ]
            print(f"[{_now()}] heartbeat({int(self.config.window_sec)}s): " + " | ".join(pieces))

    async def _consume_ws(self) -> None:
        url = self._stream_url()
        reconnect_tries = 0
        while True:
            try:
                print(f"[{_now()}] connecting: {url}")
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=10000,
                ) as ws:
                    reconnect_tries = 0
                    print(f"[{_now()}] websocket connected, monitoring symbols={len(self.config.symbols)}")
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(msg, dict):
                            continue
                        data: Any = msg.get("data")
                        if not isinstance(data, dict):
                            if str(msg.get("e", "")) == "aggTrade":
                                data = msg
                            else:
                                continue
                        if str(data.get("e", "")) != "aggTrade":
                            continue
                        self.ingest_agg_trade(data)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                reconnect_tries += 1
                backoff = min(
                    float(self.config.reconnect_max_sec),
                    float(self.config.reconnect_base_sec) * (2 ** (reconnect_tries - 1)),
                )
                jitter = random.uniform(0.0, 0.4)
                wait_s = max(0.2, backoff + jitter)
                print(f"[{_now()}] websocket error: {exc!r}, reconnect in {wait_s:.2f}s")
                await asyncio.sleep(wait_s)

    async def run(self) -> None:
        if websockets is None:
            raise RuntimeError(f"websockets import failed: {_WS_IMPORT_ERROR!r}")
        timeout = aiohttp.ClientTimeout(total=max(2.0, float(self.config.webhook_timeout_sec)))
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            tasks = [
                asyncio.create_task(self._evaluation_loop(), name="spot_inflow_eval"),
                asyncio.create_task(self._status_loop(), name="spot_inflow_status"),
            ]
            try:
                await self._consume_ws()
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self.session = None


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Binance spot incremental inflow monitor.")
    p.add_argument(
        "--symbols",
        type=str,
        default=os.getenv("SPOT_INFLOW_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT").strip(),
        help="Comma separated spot symbols, only USDT pairs are accepted.",
    )
    p.add_argument("--window-sec", type=float, default=_env_float("SPOT_INFLOW_WINDOW_SEC", 60.0))
    p.add_argument(
        "--snapshot-interval-sec",
        type=float,
        default=_env_float("SPOT_INFLOW_SNAPSHOT_INTERVAL_SEC", 2.0),
        help="How often to evaluate incremental inflow signals.",
    )
    p.add_argument("--status-interval-sec", type=float, default=_env_float("SPOT_INFLOW_STATUS_INTERVAL_SEC", 30.0))
    p.add_argument("--min-net-inflow-usdt", type=float, default=_env_float("SPOT_INFLOW_MIN_NET_USDT", 150_000.0))
    p.add_argument("--min-increment-usdt", type=float, default=_env_float("SPOT_INFLOW_MIN_INC_USDT", 60_000.0))
    p.add_argument("--min-quote-volume-usdt", type=float, default=_env_float("SPOT_INFLOW_MIN_QUOTE_USDT", 280_000.0))
    p.add_argument("--min-buy-ratio", type=float, default=_env_float("SPOT_INFLOW_MIN_BUY_RATIO", 0.56))
    p.add_argument("--min-zscore", type=float, default=_env_float("SPOT_INFLOW_MIN_ZSCORE", 1.8))
    p.add_argument("--zscore-lookback", type=int, default=_env_int("SPOT_INFLOW_ZSCORE_LOOKBACK", 120))
    p.add_argument("--zscore-min-samples", type=int, default=_env_int("SPOT_INFLOW_ZSCORE_MIN_SAMPLES", 30))
    p.add_argument("--alert-cooldown-sec", type=float, default=_env_float("SPOT_INFLOW_ALERT_COOLDOWN_SEC", 120.0))
    p.add_argument("--reconnect-base-sec", type=float, default=_env_float("SPOT_INFLOW_RECONNECT_BASE_SEC", 1.0))
    p.add_argument("--reconnect-max-sec", type=float, default=_env_float("SPOT_INFLOW_RECONNECT_MAX_SEC", 12.0))
    p.add_argument("--webhook-timeout-sec", type=float, default=_env_float("SPOT_INFLOW_WEBHOOK_TIMEOUT_SEC", 8.0))
    p.add_argument("--discord-webhook-url", type=str, default=os.getenv("DISCORD_WEBHOOK_URL", "").strip())
    p.add_argument("--dry-run", type=_parse_bool_arg, default=_env_bool("SPOT_INFLOW_DRY_RUN", False))
    return p


def _config_from_args(args: argparse.Namespace) -> MonitorConfig:
    symbols = _parse_symbols(args.symbols)
    min_buy_ratio = float(args.min_buy_ratio)
    min_buy_ratio = max(0.0, min(1.0, min_buy_ratio))
    webhook = "" if bool(args.dry_run) else str(args.discord_webhook_url or "").strip()
    return MonitorConfig(
        symbols=symbols,
        window_sec=max(5.0, float(args.window_sec)),
        snapshot_interval_sec=max(0.2, float(args.snapshot_interval_sec)),
        status_interval_sec=max(3.0, float(args.status_interval_sec)),
        min_net_inflow_usdt=max(0.0, float(args.min_net_inflow_usdt)),
        min_increment_usdt=max(0.0, float(args.min_increment_usdt)),
        min_quote_volume_usdt=max(0.0, float(args.min_quote_volume_usdt)),
        min_buy_ratio=min_buy_ratio,
        min_zscore=max(0.0, float(args.min_zscore)),
        zscore_lookback=max(10, int(args.zscore_lookback)),
        zscore_min_samples=max(2, int(args.zscore_min_samples)),
        alert_cooldown_sec=max(0.0, float(args.alert_cooldown_sec)),
        reconnect_base_sec=max(0.2, float(args.reconnect_base_sec)),
        reconnect_max_sec=max(1.0, float(args.reconnect_max_sec)),
        discord_webhook_url=webhook,
        webhook_timeout_sec=max(2.0, float(args.webhook_timeout_sec)),
        dry_run=bool(args.dry_run),
    )


async def _amain(args: argparse.Namespace) -> None:
    config = _config_from_args(args)
    monitor = SpotInflowMonitor(config)
    await monitor.run()


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    cfg = _config_from_args(args)
    print("=" * 64)
    print("Spot Inflow Incremental Monitor")
    print("=" * 64)
    print(
        f"symbols={','.join(cfg.symbols)} "
        f"window={int(cfg.window_sec)}s "
        f"min_net={cfg.min_net_inflow_usdt:.0f} "
        f"min_inc={cfg.min_increment_usdt:.0f} "
        f"min_quote={cfg.min_quote_volume_usdt:.0f} "
        f"min_buy_ratio={cfg.min_buy_ratio:.2f} "
        f"min_z={cfg.min_zscore:.2f} "
        f"cooldown={cfg.alert_cooldown_sec:.0f}s "
        f"discord={'on' if cfg.discord_webhook_url else 'off'} "
        f"dry_run={'on' if cfg.dry_run else 'off'}"
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
