from __future__ import annotations

import atexit
import logging
import signal
import time
from dataclasses import dataclass
from datetime import datetime, time as time_value, timedelta, timezone

from drify_ingestor.config import MarketSchedule, Settings
from drify_ingestor.instrument_selection import InstrumentSelector
from drify_ingestor.kinesis import KinesisPublisher
from drify_ingestor.kite_client import KiteBasketStreamer, PremarketReferenceCollector
from drify_ingestor.logging_config import configure_logging
from drify_ingestor.models import build_tick_event


LOGGER = logging.getLogger(__name__)
IST = timezone(timedelta(hours=5, minutes=30))


@dataclass(frozen=True)
class StartupPlan:
    mode: str
    should_wait_for_premarket: bool
    should_collect_premarket_references: bool
    should_wait_for_market_open: bool


def _wait_until(target_time: time_value, label: str) -> None:
    now = datetime.now(tz=IST)
    target = datetime.combine(now.date(), target_time, tzinfo=IST)

    if now >= target:
        return

    sleep_seconds = (target - now).total_seconds()
    LOGGER.info("Waiting %.0f seconds until %s at %s", sleep_seconds, label, target.isoformat())
    while sleep_seconds > 0:
        time.sleep(min(sleep_seconds, 30))
        now = datetime.now(tz=IST)
        sleep_seconds = (target - now).total_seconds()


def _has_market_closed(market_end: time_value, grace_seconds: int) -> bool:
    now = datetime.now(tz=IST)
    deadline = datetime.combine(now.date(), market_end, tzinfo=IST) + timedelta(seconds=grace_seconds)
    return now >= deadline


def _startup_plan(schedule: MarketSchedule) -> StartupPlan:
    current_time = datetime.now(tz=IST).time()
    if current_time < schedule.premarket_start:
        return StartupPlan(
            mode="premarket_capture",
            should_wait_for_premarket=True,
            should_collect_premarket_references=True,
            should_wait_for_market_open=True,
        )
    if current_time < schedule.basket_selection_time:
        return StartupPlan(
            mode="hybrid_capture_with_fallback",
            should_wait_for_premarket=False,
            should_collect_premarket_references=True,
            should_wait_for_market_open=current_time < schedule.market_start,
        )
    return StartupPlan(
        mode="late_start_live_snapshot",
        should_wait_for_premarket=False,
        should_collect_premarket_references=False,
        should_wait_for_market_open=current_time < schedule.market_start,
    )


def _collect_reference_prices(
    selector: InstrumentSelector,
    settings: Settings,
) -> dict[str, float]:
    reference_watchlist = selector.build_reference_watchlist()
    collector = PremarketReferenceCollector(
        api_key=settings.kite_api_key,
        access_token=settings.kite_access_token,
        instruments_by_underlying=reference_watchlist,
        capture_until=settings.schedule.basket_selection_time,
        reconnect_max_tries=settings.websocket_reconnect_max_tries,
        reconnect_max_delay=settings.websocket_reconnect_max_delay,
        connect_timeout=settings.websocket_connect_timeout,
    )
    try:
        return collector.collect()
    except (TimeoutError, ValueError):
        partial_prices = dict(collector.latest_prices)
        if partial_prices:
            LOGGER.warning(
                "Premarket reference collection incomplete; falling back to live quote snapshot for missing underlyings. captured=%s",
                partial_prices,
            )
            return partial_prices
        LOGGER.warning(
            "Premarket reference collection failed; falling back to live quote snapshot for basket construction",
            exc_info=True,
        )
        return {}


def main() -> None:
    streamer: KiteBasketStreamer | None = None
    publisher: KinesisPublisher | None = None
    try:
        settings = Settings.from_env()
        configure_logging(settings.log_level)
        if _has_market_closed(settings.schedule.market_end, settings.market_close_grace_seconds):
            LOGGER.info(
                "Market close plus grace window has already passed for today, exiting without starting the streamer"
            )
            return

        plan = _startup_plan(settings.schedule)
        LOGGER.info("Startup mode selected: %s", plan.mode)

        selector = InstrumentSelector(settings)

        if plan.should_wait_for_premarket:
            _wait_until(settings.schedule.premarket_start, "pre-market start")

        reference_prices: dict[str, float] = {}
        if plan.should_collect_premarket_references:
            reference_prices = _collect_reference_prices(selector, settings)
        else:
            LOGGER.info(
                "Late start detected after basket selection cutoff; building basket from live quote snapshot"
            )

        market_basket = selector.build_market_basket(reference_prices=reference_prices)

        if plan.should_wait_for_market_open:
            _wait_until(settings.schedule.market_start, "market open")

        publisher = KinesisPublisher(
            stream_name=settings.kinesis_stream_name,
            region_name=settings.aws_region,
            publish_retries=settings.kinesis_publish_retries,
            publish_backoff_seconds=settings.kinesis_publish_backoff_seconds,
            batch_size=settings.kinesis_batch_size,
            flush_interval_ms=settings.kinesis_flush_interval_ms,
            max_queue_size=settings.kinesis_max_queue_size,
        )
        atexit.register(publisher.close)

        def handle_tick(tick: dict) -> None:
            instrument_token = tick.get("instrument_token")
            instrument = market_basket.instruments_by_token.get(instrument_token)
            if instrument is None:
                LOGGER.debug("Skipping tick for unsubscribed instrument_token=%s", instrument_token)
                return

            if "last_price" not in tick and "ohlc" not in tick:
                LOGGER.debug("Skipping tick without last_price: %s", tick)
                return

            try:
                event = build_tick_event(tick, instrument)
                publisher.publish(
                    record=event.to_dict(),
                    partition_key=f"{settings.kinesis_partition_key}:{instrument.underlying}:{instrument.instrument_token}",
                )
            except Exception:
                LOGGER.exception(
                    "Failed to process/publish tick symbol=%s instrument_token=%s",
                    instrument.tradingsymbol,
                    instrument_token,
                )
                return

            LOGGER.debug(
                "Published tick symbol=%s timestamp=%s ltp=%s stream=%s",
                instrument.tradingsymbol,
                event.event_time,
                event.last_price,
                settings.kinesis_stream_name,
            )

        streamer = KiteBasketStreamer(
            api_key=settings.kite_api_key,
            access_token=settings.kite_access_token,
            market_basket=market_basket,
            schedule=settings.schedule,
            market_close_grace_seconds=settings.market_close_grace_seconds,
            reconnect_max_tries=settings.websocket_reconnect_max_tries,
            reconnect_max_delay=settings.websocket_reconnect_max_delay,
            connect_timeout=settings.websocket_connect_timeout,
            tick_handler=handle_tick,
        )

        def _handle_shutdown_signal(signum: int, frame: object) -> None:
            LOGGER.info("Received shutdown signal=%s, stopping data collection gracefully", signum)
            if streamer is not None:
                streamer.stop()

        signal.signal(signal.SIGINT, _handle_shutdown_signal)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, _handle_shutdown_signal)

        streamer.connect()
    except Exception:
        LOGGER.exception("Fatal error in live data collection service")
        raise
    finally:
        if streamer is not None:
            streamer.stop()
        if publisher is not None:
            publisher.close()


if __name__ == "__main__":
    main()
