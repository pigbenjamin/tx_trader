"""Side-effect-free contracts for the Capital quote adapter."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from tx_trade.market_data.models import ConnectionState


class CapitalAdapterError(RuntimeError):
    """Base class for live quote adapter failures."""


class LiveQuoteInitializationError(CapitalAdapterError):
    """The actual quote runtime could not be initialized."""


class AuthenticationError(CapitalAdapterError):
    """Authentication was rejected."""


class MonitorError(CapitalAdapterError):
    """The quote monitor could not be entered."""


class SubscriptionError(CapitalAdapterError):
    """A quote or tick subscription operation failed."""


class CommandQueueFullError(CapitalAdapterError):
    """The bounded command queue has no free slot."""


class AdapterStoppedError(CapitalAdapterError):
    """The adapter no longer accepts commands."""


class ReadyTimeoutError(CapitalAdapterError):
    """The quote service did not become ready before the deadline."""


def _strict_int(value: object, name: str, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")


@dataclass(frozen=True, slots=True)
class QuoteSnapshotRaw:
    bid_raw: int
    ask_raw: int
    last_raw: int
    bid_qty_raw: int
    ask_qty_raw: int
    last_qty_raw: int | None
    total_qty_raw: int
    stock_no: str | None = None
    stock_name: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "bid_raw",
            "ask_raw",
            "last_raw",
            "bid_qty_raw",
            "ask_qty_raw",
            "total_qty_raw",
        ):
            _strict_int(getattr(self, name), name)
        _strict_int(self.last_qty_raw, "last_qty_raw", optional=True)
        for name in ("stock_no", "stock_name"):
            value = getattr(self, name)
            if value is not None and type(value) is not str:
                raise TypeError(f"{name} must be a string or None")


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    max_attempts: int = 3
    backoff_seconds: tuple[float, ...] = (0.0, 0.1, 0.5)

    def __post_init__(self) -> None:
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        if type(self.backoff_seconds) is not tuple or not self.backoff_seconds:
            raise ValueError("backoff_seconds must be a non-empty tuple")
        if len(self.backoff_seconds) != self.max_attempts:
            raise ValueError("backoff_seconds length must equal max_attempts")
        for value in self.backoff_seconds:
            if type(value) not in (int, float):
                raise TypeError("backoff values must be numbers")
            if not math.isfinite(value) or value < 0:
                raise ValueError("backoff values must be non-negative and finite")


@dataclass(frozen=True, slots=True)
class AdapterSnapshot:
    state: ConnectionState
    generation: int
    callback_sequence: int
    last_kind: int | None
    last_code: int | None
    desired_quotes: frozenset[str]
    actual_quotes: frozenset[str]
    desired_ticks: frozenset[str]
    actual_ticks: frozenset[str]
    thread_id: int | None
    reconnect_attempts: int
    accepting_commands: bool


@runtime_checkable
class QuoteComBackend(Protocol):
    def co_initialize(self) -> None: ...

    def initialize(self, dll_path: str) -> None: ...

    def register_events(self, sink: object) -> None: ...

    def login(self, account: str, password: str) -> int: ...

    def enter_monitor(self) -> int: ...

    def leave_monitor(self) -> int: ...

    def request_quotes(self, symbols_csv: str) -> int: ...

    def request_ticks(self, symbols_csv: str) -> int: ...

    def cancel_quotes(self, symbols_csv: str) -> int: ...

    def cancel_ticks(self, symbols_csv: str) -> int: ...

    def lookup_quote(self, market_no: int, stock_idx: int) -> QuoteSnapshotRaw: ...

    def pump_waiting_messages(self) -> None: ...

    def release_events(self) -> None: ...

    def release_objects(self) -> None: ...

    def co_uninitialize(self) -> None: ...
