from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import time
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class MarketSchedule:
    premarket_start: time
    premarket_end: time
    basket_selection_time: time
    market_start: time
    market_end: time


@dataclass(frozen=True)
class Settings:
    kite_api_key: str
    kite_access_token: str
    aws_region: str
    kinesis_stream_name: str
    kinesis_partition_key: str
    log_level: str
    option_strike_window: int
    nifty_strike_step: int
    sensex_strike_step: int
    index_mode: str
    derivative_mode: str
    basket_reference_mode: str
    websocket_reconnect_max_tries: int
    websocket_reconnect_max_delay: int
    websocket_connect_timeout: int
    kinesis_publish_retries: int
    kinesis_publish_backoff_seconds: float
    kinesis_batch_size: int
    kinesis_flush_interval_ms: int
    kinesis_max_queue_size: int
    market_close_grace_seconds: int
    schedule: MarketSchedule

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kite_api_key=_get_required("KITE_API_KEY"),
            kite_access_token=_get_required_secret("KITE_ACCESS_TOKEN", "KITE_ACCESS_TOKEN_FILE"),
            aws_region=_get_required("AWS_REGION"),
            kinesis_stream_name=_get_required("KINESIS_STREAM_NAME"),
            kinesis_partition_key=os.getenv("KINESIS_PARTITION_KEY", "market-ticks"),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            option_strike_window=int(os.getenv("OPTION_STRIKE_WINDOW", "15")),
            nifty_strike_step=int(os.getenv("NIFTY_STRIKE_STEP", "50")),
            sensex_strike_step=int(os.getenv("SENSEX_STRIKE_STEP", "100")),
            index_mode=os.getenv("KITE_INDEX_MODE", "quote"),
            derivative_mode=os.getenv("KITE_DERIVATIVE_MODE", "full"),
            basket_reference_mode=os.getenv("BASKET_REFERENCE_MODE", "last_price"),
            websocket_reconnect_max_tries=int(os.getenv("KITE_RECONNECT_MAX_TRIES", "50")),
            websocket_reconnect_max_delay=int(os.getenv("KITE_RECONNECT_MAX_DELAY", "60")),
            websocket_connect_timeout=int(os.getenv("KITE_CONNECT_TIMEOUT", "30")),
            kinesis_publish_retries=int(os.getenv("KINESIS_PUBLISH_RETRIES", "3")),
            kinesis_publish_backoff_seconds=float(os.getenv("KINESIS_PUBLISH_BACKOFF_SECONDS", "1.0")),
            kinesis_batch_size=int(os.getenv("KINESIS_BATCH_SIZE", "100")),
            kinesis_flush_interval_ms=int(os.getenv("KINESIS_FLUSH_INTERVAL_MS", "50")),
            kinesis_max_queue_size=int(os.getenv("KINESIS_MAX_QUEUE_SIZE", "10000")),
            market_close_grace_seconds=int(os.getenv("MARKET_CLOSE_GRACE_SECONDS", "120")),
            schedule=MarketSchedule(
                premarket_start=_parse_time(os.getenv("PREMARKET_START_TIME", "09:00")),
                premarket_end=_parse_time(os.getenv("PREMARKET_END_TIME", "09:08")),
                basket_selection_time=_parse_time(os.getenv("BASKET_SELECTION_TIME", "09:10")),
                market_start=_parse_time(os.getenv("MARKET_START_TIME", "09:15")),
                market_end=_parse_time(os.getenv("MARKET_END_TIME", "15:30")),
            ),
        )


def _parse_time(value: str) -> time:
    hour_str, minute_str = value.split(":", maxsplit=1)
    return time(hour=int(hour_str), minute=int(minute_str))


def _get_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _get_required_secret(name: str, file_name: str) -> str:
    direct_value = os.getenv(name)
    if direct_value:
        return direct_value

    file_path = os.getenv(file_name)
    if file_path:
        token = Path(file_path).read_text(encoding="utf-8").strip()
        if token:
            return token

    raise ValueError(
        f"Missing required environment variable: {name} or readable token file from {file_name}"
    )
