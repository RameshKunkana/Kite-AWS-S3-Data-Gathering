from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from kiteconnect import KiteConnect

from drify_ingestor.config import Settings


LOGGER = logging.getLogger(__name__)
IST = timezone(timedelta(hours=5, minutes=30))


@dataclass(frozen=True)
class SelectedInstrument:
    instrument_token: int
    tradingsymbol: str
    exchange: str
    segment: str
    name: str
    instrument_type: str
    underlying: str
    basket_role: str
    mode: str
    expiry: str | None
    strike: float | None
    lot_size: int | None
    tick_size: float | None

    @property
    def option_type(self) -> str | None:
        if self.instrument_type in {"CE", "PE"}:
            return self.instrument_type
        return None


@dataclass(frozen=True)
class BasketReference:
    underlying: str
    reference_price: float
    atm_strike: int
    reference_source: str


@dataclass(frozen=True)
class MarketBasket:
    instruments_by_token: dict[int, SelectedInstrument]
    references: dict[str, BasketReference]

    def grouped_tokens_by_mode(self) -> dict[str, list[int]]:
        grouped: dict[str, list[int]] = {}
        for token, instrument in self.instruments_by_token.items():
            grouped.setdefault(instrument.mode, []).append(token)
        return grouped


class InstrumentSelector:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.kite = KiteConnect(api_key=settings.kite_api_key)
        self.kite.set_access_token(settings.kite_access_token)
        self._cached_instruments: list[dict[str, Any]] | None = None
        self._by_exchange_symbol: dict[tuple[str, str], dict[str, Any]] = {}
        self._by_exchange_name: dict[tuple[str, str], list[dict[str, Any]]] = {}

    def build_reference_watchlist(self) -> dict[str, SelectedInstrument]:
        self._instruments()
        return {
            "NIFTY": self._find_index("NSE", "NIFTY 50", "NIFTY"),
            "SENSEX": self._find_index("BSE", "SENSEX", "SENSEX"),
        }

    def build_market_basket(
        self,
        *,
        reference_prices: dict[str, float] | None = None,
    ) -> MarketBasket:
        self._instruments()
        index_quotes = self.kite.quote("NSE:NIFTY 50", "BSE:SENSEX", "NSE:INDIA VIX")
        reference_prices = reference_prices or {}

        basket: dict[int, SelectedInstrument] = {}
        references: dict[str, BasketReference] = {}

        nifty_reference = self._reference_for_underlying(
            "NIFTY",
            index_quotes["NSE:NIFTY 50"],
            self.settings.nifty_strike_step,
            explicit_reference_price=reference_prices.get("NIFTY"),
        )
        sensex_reference = self._reference_for_underlying(
            "SENSEX",
            index_quotes["BSE:SENSEX"],
            self.settings.sensex_strike_step,
            explicit_reference_price=reference_prices.get("SENSEX"),
        )
        references["NIFTY"] = nifty_reference
        references["SENSEX"] = sensex_reference

        for selected in (
            self._find_index("NSE", "NIFTY 50", "NIFTY"),
            self._find_index("BSE", "SENSEX", "SENSEX"),
            self._find_index("NSE", "INDIA VIX", "INDIA_VIX"),
            self._find_front_future("NIFTY", "NFO"),
            self._find_front_future("SENSEX", "BFO"),
        ):
            basket[selected.instrument_token] = selected

        for option in self._find_option_basket("NIFTY", "NFO", nifty_reference.atm_strike, self.settings.nifty_strike_step):
            basket[option.instrument_token] = option
        for option in self._find_option_basket("SENSEX", "BFO", sensex_reference.atm_strike, self.settings.sensex_strike_step):
            basket[option.instrument_token] = option

        LOGGER.info(
            "Built market basket with %s instruments: NIFTY ATM=%s, SENSEX ATM=%s",
            len(basket),
            nifty_reference.atm_strike,
            sensex_reference.atm_strike,
        )
        return MarketBasket(instruments_by_token=basket, references=references)

    def _reference_for_underlying(
        self,
        underlying: str,
        quote: dict[str, Any],
        strike_step: int,
        explicit_reference_price: float | None = None,
    ) -> BasketReference:
        if explicit_reference_price is not None:
            reference_price = explicit_reference_price
            reference_source = "captured_ltp"
        else:
            reference_price, reference_source = _resolve_reference_price(
                quote,
                mode=self.settings.basket_reference_mode,
            )
        atm_strike = round(reference_price / strike_step) * strike_step
        LOGGER.info(
            "Reference for %s: price=%s source=%s atm=%s",
            underlying,
            reference_price,
            reference_source,
            atm_strike,
        )
        return BasketReference(
            underlying=underlying,
            reference_price=reference_price,
            atm_strike=atm_strike,
            reference_source=reference_source,
        )

    def _find_index(
        self,
        exchange: str,
        tradingsymbol: str,
        underlying: str,
    ) -> SelectedInstrument:
        instrument = self._by_exchange_symbol.get((exchange, tradingsymbol))
        if instrument is None:
            for candidate in self._by_exchange_name.get((exchange, tradingsymbol), []):
                instrument = candidate
                break
        if instrument is None:
            raise ValueError(f"Unable to find index instrument {exchange}:{tradingsymbol}")
        return self._selected_instrument(
            instrument,
            underlying=underlying,
            basket_role="index" if underlying != "INDIA_VIX" else "volatility_index",
            mode="full",
        )

    def _instruments(self) -> list[dict[str, Any]]:
        if self._cached_instruments is None:
            LOGGER.info("Loading Kite instruments master")
            self._cached_instruments = self.kite.instruments()
            for instrument in self._cached_instruments:
                exchange = str(instrument.get("exchange") or "")
                symbol = str(instrument.get("tradingsymbol") or "")
                name = str(instrument.get("name") or "")
                self._by_exchange_symbol[(exchange, symbol)] = instrument
                self._by_exchange_name.setdefault((exchange, name), []).append(instrument)
        return self._cached_instruments

    def _find_front_future(
        self,
        underlying: str,
        exchange: str,
    ) -> SelectedInstrument:
        future_candidates = [
            instrument
            for instrument in self._by_exchange_name.get((exchange, underlying), [])
            if instrument.get("instrument_type") == "FUT"
            and _is_today_or_future(instrument.get("expiry"))
        ]
        if not future_candidates:
            raise ValueError(f"Unable to find a live future contract for {underlying}")
        front_future = min(future_candidates, key=lambda instrument: instrument["expiry"])
        return self._selected_instrument(
            front_future,
            underlying=underlying,
            basket_role="future",
            mode="full",
        )

    def _find_option_basket(
        self,
        underlying: str,
        exchange: str,
        atm_strike: int,
        strike_step: int,
    ) -> list[SelectedInstrument]:
        expiry = self._nearest_option_expiry(underlying, exchange)
        target_strikes = {
            atm_strike + offset * strike_step
            for offset in range(-self.settings.option_strike_window, self.settings.option_strike_window + 1)
        }
        selected = [
            self._selected_instrument(
                instrument,
                underlying=underlying,
                basket_role="option",
                mode="full",
            )
            for instrument in self._by_exchange_name.get((exchange, underlying), [])
            if instrument.get("instrument_type") in {"CE", "PE"}
            and instrument.get("expiry") == expiry
            and int(float(instrument.get("strike") or 0)) in target_strikes
        ]
        LOGGER.info(
            "Selected %s %s options for expiry %s using strikes %s to %s",
            len(selected),
            underlying,
            expiry,
            min(target_strikes),
            max(target_strikes),
        )
        expected_contracts = len(target_strikes) * 2
        if len(selected) != expected_contracts:
            LOGGER.warning(
                "Expected %s %s option contracts but found %s for expiry %s",
                expected_contracts,
                underlying,
                len(selected),
                expiry,
            )
        return selected

    def _nearest_option_expiry(
        self,
        underlying: str,
        exchange: str,
    ) -> date:
        expiries = sorted(
            {
                instrument["expiry"]
                for instrument in self._by_exchange_name.get((exchange, underlying), [])
                if instrument.get("instrument_type") in {"CE", "PE"}
                and _is_today_or_future(instrument.get("expiry"))
            }
        )
        if not expiries:
            raise ValueError(f"Unable to find a live option expiry for {underlying}")
        return expiries[0]

    def _selected_instrument(
        self,
        instrument: dict[str, Any],
        *,
        underlying: str,
        basket_role: str,
        mode: str,
    ) -> SelectedInstrument:
        expiry = instrument.get("expiry")
        expiry_value = expiry.isoformat() if hasattr(expiry, "isoformat") else None
        strike = instrument.get("strike")
        return SelectedInstrument(
            instrument_token=int(instrument["instrument_token"]),
            tradingsymbol=str(instrument["tradingsymbol"]),
            exchange=str(instrument["exchange"]),
            segment=str(instrument["segment"]),
            name=str(instrument.get("name") or instrument["tradingsymbol"]),
            instrument_type=str(instrument.get("instrument_type") or ""),
            underlying=underlying,
            basket_role=basket_role,
            mode=mode,
            expiry=expiry_value,
            strike=float(strike) if strike else None,
            lot_size=int(instrument["lot_size"]) if instrument.get("lot_size") else None,
            tick_size=float(instrument["tick_size"]) if instrument.get("tick_size") else None,
        )


def _resolve_reference_price(quote: dict[str, Any], mode: str) -> tuple[float, str]:
    if mode == "ohlc_open":
        open_price = (quote.get("ohlc") or {}).get("open")
        if open_price:
            return float(open_price), "ohlc_open"

    if quote.get("last_price"):
        return float(quote["last_price"]), "last_price"

    open_price = (quote.get("ohlc") or {}).get("open")
    if open_price:
        return float(open_price), "ohlc_open"

    close_price = (quote.get("ohlc") or {}).get("close")
    if close_price:
        return float(close_price), "ohlc_close"

    raise ValueError(f"Unable to resolve reference price from quote payload: {quote}")


def _is_today_or_future(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, datetime):
        expiry_date = value.date()
    elif isinstance(value, date):
        expiry_date = value
    else:
        expiry_date = date.fromisoformat(str(value))
    return expiry_date >= datetime.now(tz=IST).date()
