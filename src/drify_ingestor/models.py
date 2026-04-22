from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any

from drify_ingestor.instrument_selection import SelectedInstrument


IST = timezone(timedelta(hours=5, minutes=30))


@dataclass(frozen=True)
class TickEvent:
    event_time: str
    ingestion_time: str
    exchange_timestamp: str | None
    last_trade_time: str | None
    instrument_token: int
    tradingsymbol: str
    exchange: str
    segment: str
    name: str
    underlying: str
    basket_role: str
    instrument_type: str
    option_type: str | None
    expiry: str | None
    strike: float | None
    lot_size: int | None
    tick_size: float | None
    mode: str
    last_price: float | None
    last_quantity: int | None
    average_price: float | None
    volume: int | None
    buy_quantity: int | None
    sell_quantity: int | None
    change: float | None
    oi: int | None
    oi_day_high: int | None
    oi_day_low: int | None
    open_price: float | None
    high_price: float | None
    low_price: float | None
    close_price: float | None
    depth_buy: list[dict[str, Any]]
    depth_sell: list[dict[str, Any]]
    raw_tick_json: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["instrument_folder"] = _instrument_folder_key(
            underlying=self.underlying,
            basket_role=self.basket_role,
            instrument_type=self.instrument_type,
            expiry=self.expiry,
            strike=self.strike,
            tradingsymbol=self.tradingsymbol,
        )
        return payload


def build_tick_event(tick: dict[str, Any], instrument: SelectedInstrument) -> TickEvent:
    normalized_tick = _normalize_value(tick)
    exchange_timestamp = _to_iso8601(tick.get("exchange_timestamp") or tick.get("timestamp"))
    last_trade_time = _to_iso8601(tick.get("last_trade_time"))
    ingestion_time = _now_ist()
    event_time = exchange_timestamp if exchange_timestamp else ingestion_time
    ohlc = normalized_tick.get("ohlc") if isinstance(normalized_tick.get("ohlc"), dict) else {}
    depth = normalized_tick.get("depth") if isinstance(normalized_tick.get("depth"), dict) else {}

    return TickEvent(
        event_time=event_time,
        ingestion_time=ingestion_time,
        exchange_timestamp=exchange_timestamp,
        last_trade_time=last_trade_time,
        instrument_token=instrument.instrument_token,
        tradingsymbol=instrument.tradingsymbol,
        exchange=instrument.exchange,
        segment=instrument.segment,
        name=instrument.name,
        underlying=instrument.underlying,
        basket_role=instrument.basket_role,
        instrument_type=instrument.instrument_type,
        option_type=instrument.option_type,
        expiry=instrument.expiry,
        strike=instrument.strike,
        lot_size=instrument.lot_size,
        tick_size=instrument.tick_size,
        mode=instrument.mode,
        last_price=_to_float(tick.get("last_price")),
        last_quantity=_to_int(tick.get("last_quantity") or tick.get("last_traded_quantity")),
        average_price=_to_float(tick.get("average_price") or tick.get("average_traded_price")),
        volume=_to_int(tick.get("volume") or tick.get("volume_traded")),
        buy_quantity=_to_int(tick.get("buy_quantity") or tick.get("total_buy_quantity")),
        sell_quantity=_to_int(tick.get("sell_quantity") or tick.get("total_sell_quantity")),
        change=_to_float(tick.get("change")),
        oi=_to_int(tick.get("oi")),
        oi_day_high=_to_int(tick.get("oi_day_high")),
        oi_day_low=_to_int(tick.get("oi_day_low")),
        open_price=_to_float(ohlc.get("open")),
        high_price=_to_float(ohlc.get("high")),
        low_price=_to_float(ohlc.get("low")),
        close_price=_to_float(ohlc.get("close")),
        depth_buy=_to_depth(depth.get("buy")),
        depth_sell=_to_depth(depth.get("sell")),
        raw_tick_json=_to_json_string(normalized_tick),
    )


def _normalize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _normalize_value(nested) for key, nested in value.items()}
    if isinstance(value, list):
        return [_normalize_value(item) for item in value]
    if isinstance(value, datetime):
        return _to_iso8601(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


def _to_json_string(value: Any) -> str:
    import json

    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _instrument_folder(instrument: SelectedInstrument) -> str:
    return _instrument_folder_key(
        underlying=instrument.underlying,
        basket_role=instrument.basket_role,
        instrument_type=instrument.instrument_type,
        expiry=instrument.expiry,
        strike=instrument.strike,
        tradingsymbol=instrument.tradingsymbol,
    )


def _instrument_folder_key(
    *,
    underlying: str,
    basket_role: str,
    instrument_type: str,
    expiry: str | None,
    strike: float | None,
    tradingsymbol: str,
) -> str:
    if basket_role == "index":
        return f"{underlying}_SPOT"
    if basket_role == "volatility_index":
        return underlying
    if basket_role == "future":
        expiry_label = _format_expiry_label(expiry)
        return f"{underlying}_{expiry_label}_FUT"
    if basket_role == "option":
        expiry_label = _format_expiry_label(expiry)
        strike_label = _format_strike_label(strike)
        return f"{underlying}_{expiry_label}_{strike_label}_{instrument_type}"
    return tradingsymbol.replace(" ", "_").upper()


def _format_expiry_label(expiry: str | None) -> str:
    if not expiry:
        return "NOEXPIRY"
    expiry_date = date.fromisoformat(expiry)
    return expiry_date.strftime("%d%b").upper()


def _format_strike_label(strike: float | None) -> str:
    if strike is None:
        return "NA"
    if strike.is_integer():
        return str(int(strike))
    return str(strike).replace(".", "_")


def _to_depth(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [entry for entry in _normalize_value(value) if isinstance(entry, dict)]


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _to_iso8601(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(IST).replace(tzinfo=None).isoformat(timespec="milliseconds")
    return str(value)


def _now_ist() -> str:
    return datetime.now(tz=IST).replace(tzinfo=None).isoformat(timespec="milliseconds")
