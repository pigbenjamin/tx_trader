"""Bounded projection of captured events into the legacy QuoteClient shape."""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from copy import deepcopy
from threading import Lock
from typing import Any

from .models import (
    CapturedAdapterDiagnostic,
    CapturedConnectionNotification,
    CapturedMarketDataEvent,
    CapturedQuoteSnapshot,
    CapturedServerTimeNotification,
    CapturedStockListNotification,
    CapturedTickNotification,
)


def _mutable_copy(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _mutable_copy(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_mutable_copy(item) for item in value]
    if isinstance(value, list):
        return [_mutable_copy(item) for item in value]
    if isinstance(value, (str, bytes, int, float, bool, type(None))):
        return value
    return deepcopy(value)


class LegacyQuoteSnapshotProjector:
    """Thread-safe, bounded compatibility view; never consumes ingress itself."""

    def __init__(
        self,
        *,
        quote_capacity: int = 256,
        tick_capacity: int = 1024,
        diagnostic_capacity: int = 64,
    ) -> None:
        for name, value in (
            ("quote_capacity", quote_capacity),
            ("tick_capacity", tick_capacity),
            ("diagnostic_capacity", diagnostic_capacity),
        ):
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        self._lock = Lock()
        self._server_time: dict[str, Any] | None = None
        self._stock_list: dict[str, Any] | None = None
        self._connection = {
            "stocks_ready": False,
            "last_kind": None,
            "last_code": None,
        }
        self._quotes: deque[dict[str, Any]] = deque(maxlen=quote_capacity)
        self._ticks: deque[dict[str, Any]] = deque(maxlen=tick_capacity)
        self._diagnostics: deque[dict[str, Any]] = deque(
            maxlen=diagnostic_capacity
        )

    def project(self, event: CapturedMarketDataEvent) -> None:
        if not isinstance(event, CapturedMarketDataEvent):
            raise TypeError("event must be CapturedMarketDataEvent")
        payload = event.payload
        with self._lock:
            if isinstance(payload, CapturedConnectionNotification):
                self._connection["last_kind"] = payload.broker_kind_raw
                self._connection["last_code"] = payload.broker_code_raw
                if payload.broker_kind_raw == 3003:
                    self._connection["stocks_ready"] = True
                elif payload.broker_kind_raw == 3002:
                    self._connection["stocks_ready"] = False
            elif isinstance(payload, CapturedServerTimeNotification):
                self._server_time = {
                    "hour": payload.hour_raw,
                    "minute": payload.minute_raw,
                    "second": payload.second_raw,
                    "total": payload.total_raw,
                }
            elif isinstance(payload, CapturedStockListNotification):
                self._stock_list = {
                    "market_no": payload.market_no_raw,
                    "product_data": _mutable_copy(payload.stock_list_raw),
                }
            elif isinstance(payload, CapturedQuoteSnapshot):
                quote = {
                    "market_no": payload.market_no_raw,
                    "stock_idx": payload.stock_idx_raw,
                }
                if payload.is_long_callback:
                    quote["long"] = True
                self._quotes.append(quote)
            elif isinstance(payload, CapturedTickNotification):
                tick = {
                    "market_no": payload.market_no_raw,
                    "stock_idx": payload.stock_idx_raw,
                    "ptr": payload.source_pointer_raw,
                    "date": payload.date_raw,
                    "timehms": payload.time_hms_raw,
                    "timemillismicros": payload.time_subsecond_raw,
                    "bid": payload.bid_raw,
                    "ask": payload.ask_raw,
                    "close": payload.close_raw,
                    "qty": payload.quantity_raw,
                    "simulate": payload.simulate_raw,
                }
                if payload.is_long_callback:
                    tick["long"] = True
                self._ticks.append(tick)
            elif isinstance(payload, CapturedAdapterDiagnostic):
                self._diagnostics.append(
                    {
                        "diagnostic_kind": payload.diagnostic_kind,
                        "market_no": payload.market_no_raw,
                        "stock_idx": payload.stock_idx_raw,
                        "error_code": payload.error_code_raw,
                        "message": payload.message,
                        "attempt": payload.attempt,
                        "raw_notification": _mutable_copy(
                            payload.raw_notification
                        ),
                    }
                )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            result = {
                "server_time": _mutable_copy(self._server_time),
                "stock_list": _mutable_copy(self._stock_list),
                "quotes": _mutable_copy(list(self._quotes)),
                "ticks": _mutable_copy(list(self._ticks)),
                "connection": _mutable_copy(self._connection),
                "diagnostics": _mutable_copy(list(self._diagnostics)),
            }
        return result

    def get_latest_event_data(self) -> dict[str, Any]:
        return self.snapshot()

    __call__ = snapshot
