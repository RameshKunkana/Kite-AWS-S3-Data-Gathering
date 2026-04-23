from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime, time as time_value, timedelta, timezone
from typing import Any

from kiteconnect import KiteTicker

from drify_ingestor.config import MarketSchedule
from drify_ingestor.instrument_selection import MarketBasket, SelectedInstrument


LOGGER = logging.getLogger(__name__)
IST = timezone(timedelta(hours=5, minutes=30))


TickHandler = Callable[[dict[str, Any]], None]


class PremarketReferenceCollector:
    def __init__(
        self,
        api_key: str,
        access_token: str,
        instruments_by_underlying: dict[str, SelectedInstrument],
        capture_until: time_value,
        reconnect_max_tries: int,
        reconnect_max_delay: int,
        connect_timeout: int,
    ) -> None:
        self.instruments_by_underlying = instruments_by_underlying
        self.capture_until = capture_until
        self.latest_prices: dict[str, float] = {}
        self._token_to_underlying = {
            instrument.instrument_token: underlying
            for underlying, instrument in instruments_by_underlying.items()
        }
        self._connected = threading.Event()
        self._closed = False
        self.ticker = KiteTicker(
            api_key,
            access_token,
            reconnect_max_tries=reconnect_max_tries,
            reconnect_max_delay=reconnect_max_delay,
            connect_timeout=connect_timeout,
        )
        self.ticker.on_ticks = self._on_ticks
        self.ticker.on_connect = self._on_connect
        self.ticker.on_close = self._on_close
        self.ticker.on_error = self._on_error

    def collect(self) -> dict[str, float]:
        LOGGER.info(
            "Starting pre-market index watcher for %s until %s",
            list(self.instruments_by_underlying),
            self.capture_until.isoformat(timespec="minutes"),
        )
        self.ticker.connect(threaded=True)
        if not self._connected.wait(timeout=30):
            self._close()
            raise TimeoutError("Timed out connecting to Kite pre-market watcher")
        while not self._capture_complete():
            time.sleep(1)

        self._close()
        deadline = time.time() + 5
        while not self._closed and time.time() < deadline:
            time.sleep(0.25)

        missing = [name for name in self.instruments_by_underlying if name not in self.latest_prices]
        if missing:
            raise ValueError(f"Did not capture pre-market LTP for: {', '.join(missing)}")

        LOGGER.info("Captured reference prices at basket time: %s", self.latest_prices)
        return dict(self.latest_prices)

    def _on_connect(self, ws: Any, response: Any) -> None:
        self._connected.set()
        tokens = [instrument.instrument_token for instrument in self.instruments_by_underlying.values()]
        ws.subscribe(tokens)
        ws.set_mode("ltp", tokens)
        LOGGER.info("Subscribed to pre-market LTP watchlist for %s tokens", len(tokens))

    def _on_ticks(self, ws: Any, ticks: list[dict[str, Any]]) -> None:
        for tick in ticks:
            token = tick.get("instrument_token")
            underlying = self._token_to_underlying.get(token)
            if underlying is None:
                continue
            last_price = tick.get("last_price")
            if last_price is None:
                continue
            self.latest_prices[underlying] = float(last_price)

        if self._capture_complete():
            self._close()

    def _on_close(self, ws: Any, code: int, reason: str) -> None:
        self._closed = True
        LOGGER.info("Pre-market watcher closed code=%s reason=%s", code, reason)

    def _on_error(self, ws: Any, code: int, reason: str) -> None:
        LOGGER.error("Pre-market watcher error code=%s reason=%s", code, reason)

    def _capture_complete(self) -> bool:
        return datetime.now(tz=IST).time() >= self.capture_until

    def _close(self) -> None:
        if self._closed:
            return
        if hasattr(self.ticker, "stop_retry"):
            self.ticker.stop_retry()
        self.ticker.close()


class KiteBasketStreamer:
    def __init__(
        self,
        api_key: str,
        access_token: str,
        market_basket: MarketBasket,
        schedule: MarketSchedule,
        market_close_grace_seconds: int,
        reconnect_max_tries: int,
        reconnect_max_delay: int,
        connect_timeout: int,
        tick_handler: TickHandler,
    ) -> None:
        self.market_basket = market_basket
        self.schedule = schedule
        self.market_close_grace_seconds = market_close_grace_seconds
        self.tick_handler = tick_handler
        self._closed_for_day = False
        self._close_lock = threading.Lock()
        self._ws: Any | None = None
        self.ticker = KiteTicker(
            api_key,
            access_token,
            reconnect_max_tries=reconnect_max_tries,
            reconnect_max_delay=reconnect_max_delay,
            connect_timeout=connect_timeout,
        )

        self.ticker.on_ticks = self._on_ticks
        self.ticker.on_connect = self._on_connect
        self.ticker.on_close = self._on_close
        self.ticker.on_error = self._on_error
        self.ticker.on_reconnect = self._on_reconnect
        self.ticker.on_noreconnect = self._on_noreconnect

    def connect(self) -> None:
        LOGGER.info(
            "Connecting to Kite WebSocket for %s instruments",
            len(self.market_basket.instruments_by_token),
        )
        self._start_market_close_watcher()
        self.ticker.connect(threaded=False)

    def _on_connect(self, ws: Any, response: Any) -> None:
        self._ws = ws
        if self._market_closed():
            LOGGER.info("Market close reached before subscription, closing WebSocket cleanly")
            self._close_for_day(ws)
            return

        grouped_tokens = self.market_basket.grouped_tokens_by_mode()
        total_tokens = sum(len(tokens) for tokens in grouped_tokens.values())
        LOGGER.info("Connected to Kite WebSocket, subscribing to %s instruments", total_tokens)
        for mode, tokens in grouped_tokens.items():
            ws.subscribe(tokens)
            ws.set_mode(mode, tokens)
            LOGGER.info("Set Kite mode=%s for %s instruments", mode, len(tokens))

    def _on_ticks(self, ws: Any, ticks: list[dict[str, Any]]) -> None:
        if self._market_closed():
            LOGGER.info("Market close reached during live stream, shutting down WebSocket")
            self._close_for_day(ws)
            return

        for tick in ticks:
            instrument_token = tick.get("instrument_token")
            if instrument_token not in self.market_basket.instruments_by_token:
                continue
            try:
                self.tick_handler(tick)
            except Exception:
                LOGGER.exception(
                    "Unhandled exception while processing tick instrument_token=%s",
                    instrument_token,
                )

    def _on_close(self, ws: Any, code: int, reason: str) -> None:
        if self._closed_for_day:
            LOGGER.info("Kite WebSocket closed after market end code=%s reason=%s", code, reason)
            return
        LOGGER.warning("Kite WebSocket closed code=%s reason=%s", code, reason)

    def _on_error(self, ws: Any, code: int, reason: str) -> None:
        LOGGER.error("Kite WebSocket error code=%s reason=%s", code, reason)

    def _on_reconnect(self, ws: Any, attempts_count: int) -> None:
        if self._closed_for_day:
            return
        LOGGER.warning("Reconnecting to Kite WebSocket attempt=%s", attempts_count)

    def _on_noreconnect(self, ws: Any) -> None:
        if self._closed_for_day:
            return
        LOGGER.error("Kite WebSocket stopped reconnecting")

    def stop(self) -> None:
        self._close_for_day(self._ws)

    def _close_for_day(self, ws: Any | None) -> None:
        with self._close_lock:
            if self._closed_for_day:
                return
            self._closed_for_day = True
            if hasattr(self.ticker, "stop_retry"):
                self.ticker.stop_retry()
            if ws is not None:
                ws.close()

    def _market_closed(self) -> bool:
        return datetime.now(tz=IST) >= self._market_close_deadline()

    def _start_market_close_watcher(self) -> None:
        watcher = threading.Thread(
            target=self._watch_for_market_close,
            name="market-close-watcher",
            daemon=True,
        )
        watcher.start()

    def _watch_for_market_close(self) -> None:
        while not self._closed_for_day:
            if self._market_closed():
                LOGGER.info(
                    "Market close watcher reached %s plus %ss grace, shutting down live stream",
                    self.schedule.market_end.isoformat(timespec="minutes"),
                    self.market_close_grace_seconds,
                )
                self._close_for_day(self._ws)
                return
            time.sleep(1)

    def _market_close_deadline(self) -> datetime:
        now = datetime.now(tz=IST)
        return datetime.combine(now.date(), self.schedule.market_end, tzinfo=IST) + timedelta(
            seconds=self.market_close_grace_seconds
        )
