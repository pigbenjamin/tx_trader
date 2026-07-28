"""Side-effect-free ports for the Phase 1 market-data pipeline."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable
from uuid import UUID

from .models import (
    CapturedMarketDataEvent,
    ConnectionStatus,
    EventType,
    MarketDataEnvelope,
    SourceMode,
    TAIPEI,
)


def _require_uuid(value: object, name: str) -> None:
    if type(value) is not UUID:
        raise TypeError(f"{name} must be UUID")


def _require_nonempty(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not value.strip():
        raise ValueError(f"{name} must not be empty")


def _require_nonnegative(value: object, name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _require_taipei(value: object, name: str) -> None:
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    if getattr(value.tzinfo, "key", None) != TAIPEI.key:
        raise ValueError(f"{name} must use Asia/Taipei timezone")


class IngressDecision(StrEnum):
    """Result of a non-blocking attempt to publish a captured event."""

    ACCEPTED = "accepted"
    COALESCED = "coalesced"
    DROPPED = "dropped"
    DUPLICATE = "duplicate"


@dataclass(frozen=True, slots=True)
class RecordingSession:
    session_id: UUID
    schema_version: int
    source: str
    source_mode: SourceMode
    started_at: datetime
    trading_day: date | None
    config_fingerprint: str

    def __post_init__(self) -> None:
        _require_uuid(self.session_id, "session_id")
        if type(self.schema_version) is not int:
            raise TypeError("schema_version must be an integer")
        if self.schema_version < 1:
            raise ValueError("schema_version must be at least 1")
        _require_nonempty(self.source, "source")
        if type(self.source_mode) is not SourceMode:
            raise TypeError("source_mode must be SourceMode")
        _require_taipei(self.started_at, "started_at")
        if self.trading_day is not None and type(self.trading_day) is not date:
            raise TypeError("trading_day must be a date or None")
        _require_nonempty(self.config_fingerprint, "config_fingerprint")


@dataclass(frozen=True, slots=True)
class HealthSnapshot:
    is_degraded: bool
    reasons: tuple[str, ...]
    observed_at: datetime

    def __post_init__(self) -> None:
        if type(self.is_degraded) is not bool:
            raise TypeError("is_degraded must be bool")
        if type(self.reasons) is not tuple:
            raise TypeError("reasons must be a tuple")
        for reason in self.reasons:
            _require_nonempty(reason, "reason")
        if self.is_degraded != bool(self.reasons):
            raise ValueError("is_degraded must match whether reasons are present")
        _require_taipei(self.observed_at, "observed_at")


@dataclass(frozen=True, slots=True)
class ReadbackIntegrityReport:
    session_id: UUID
    event_count: int
    first_ingest_sequence: int | None
    last_ingest_sequence: int | None
    is_valid: bool
    errors: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_uuid(self.session_id, "session_id")
        _require_nonnegative(self.event_count, "event_count")
        for name in ("first_ingest_sequence", "last_ingest_sequence"):
            value = getattr(self, name)
            if value is not None:
                _require_nonnegative(value, name)
        if self.event_count == 0:
            if self.first_ingest_sequence is not None or self.last_ingest_sequence is not None:
                raise ValueError("empty reports must not have sequence bounds")
        elif self.first_ingest_sequence is None or self.last_ingest_sequence is None:
            raise ValueError("non-empty reports require sequence bounds")
        elif self.first_ingest_sequence > self.last_ingest_sequence:
            raise ValueError("first_ingest_sequence must not exceed last")
        if type(self.is_valid) is not bool:
            raise TypeError("is_valid must be bool")
        if type(self.errors) is not tuple:
            raise TypeError("errors must be a tuple")
        for error in self.errors:
            _require_nonempty(error, "error")
        if self.is_valid != (not self.errors):
            raise ValueError("is_valid must match whether errors are absent")


@runtime_checkable
class CapitalQuotePort(Protocol):
    def start(self) -> None: ...
    def login(self, account: str, password: str) -> None: ...
    def enter_monitor(self) -> None: ...
    def wait_until_ready(self, timeout_seconds: float) -> ConnectionStatus: ...
    def subscribe_quotes(self, symbols: Sequence[str]) -> None: ...
    def subscribe_ticks(self, symbols: Sequence[str]) -> None: ...
    def unsubscribe_quotes(self, symbols: Sequence[str]) -> None: ...
    def unsubscribe_ticks(self, symbols: Sequence[str]) -> None: ...
    def stop(self, timeout_seconds: float) -> None: ...


@runtime_checkable
class IngressSink(Protocol):
    def try_publish(self, event: CapturedMarketDataEvent) -> IngressDecision: ...


@runtime_checkable
class MarketDataSink(Protocol):
    def publish(self, envelope: MarketDataEnvelope) -> None: ...


@runtime_checkable
class MarketDataRepository(Protocol):
    def begin_session(self, session: RecordingSession) -> None: ...
    def append_batch(self, events: Sequence[MarketDataEnvelope]) -> None: ...
    def end_session(self, session_id: UUID, ended_at: datetime, status: str) -> None: ...
    def iter_events(
        self,
        session_id: UUID,
        *,
        after_ingest_sequence: int | None = None,
        event_types: set[EventType] | None = None,
    ) -> Iterator[MarketDataEnvelope]: ...


@runtime_checkable
class Clock(Protocol):
    def now(self) -> datetime: ...
    def monotonic(self) -> float: ...


@runtime_checkable
class HealthPort(Protocol):
    def record_status(self, status: ConnectionStatus) -> None: ...
    def degrade(self, reason: str, *, details: dict[str, object] | None = None) -> None: ...
    def snapshot(self) -> HealthSnapshot: ...


@runtime_checkable
class ReplaySource(Protocol):
    def open(self, session_id: UUID) -> None: ...
    def iter_events(
        self, *, after_ingest_sequence: int | None = None
    ) -> Iterator[MarketDataEnvelope]: ...
    def verify_integrity(self) -> ReadbackIntegrityReport: ...
