"""Immutable, deterministic Phase 1 market-data contracts."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum, StrEnum
from types import MappingProxyType
from typing import Any, TypeAlias
from uuid import UUID
from zoneinfo import ZoneInfo

SCHEMA_VERSION = 1
TAIPEI = ZoneInfo("Asia/Taipei")
JSONScalar: TypeAlias = str | int | bool | None
JSONValue: TypeAlias = JSONScalar | tuple["JSONValue", ...] | Mapping[str, "JSONValue"]


class SourceMode(StrEnum):
    OFFLINE = "offline"
    REPLAY = "replay"
    LIVE = "live"


class ConnectionState(StrEnum):
    NEW = "new"
    STARTING = "starting"
    COM_READY = "com_ready"
    LOGGING_IN = "logging_in"
    LOGGED_IN = "logged_in"
    ENTERING_MONITOR = "entering_monitor"
    CONNECTED = "connected"
    STOCKS_READY = "stocks_ready"
    SUBSCRIBED = "subscribed"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    STOPPING = "stopping"
    ERROR = "error"
    STOPPED = "stopped"


class EventType(StrEnum):
    CONNECTION_STATUS = "connection_status"
    SERVER_TIME = "server_time"
    INSTRUMENT = "instrument"
    QUOTE = "quote"
    TICK = "tick"
    ADAPTER_DIAGNOSTIC = "adapter_diagnostic"


class CapturedKind(StrEnum):
    QUOTE_SNAPSHOT = "quote_snapshot"
    TICK_NOTIFICATION = "tick_notification"
    CONNECTION_NOTIFICATION = "connection_notification"
    SERVER_TIME_NOTIFICATION = "server_time_notification"
    STOCK_LIST_NOTIFICATION = "stock_list_notification"
    ADAPTER_DIAGNOSTIC = "adapter_diagnostic"


def _require_taipei(value: datetime | None, name: str, *, optional: bool = False) -> None:
    if value is None:
        if optional:
            return
        raise ValueError(f"{name} is required")
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    if getattr(value.tzinfo, "key", None) != TAIPEI.key:
        raise ValueError(f"{name} must use Asia/Taipei timezone")


def _nonnegative(value: int, name: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _positive(value: int, name: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be at least 1")


def _strict_int(value: Any, name: str, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")


def _strict_bool(value: Any, name: str, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if type(value) is not bool:
        raise TypeError(f"{name} must be bool")


def _strict_str(value: Any, name: str, *, nonempty: bool = True) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if nonempty and not value.strip():
        raise ValueError(f"{name} must not be empty")


def _strict_date(value: Any, name: str, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if type(value) is not date:
        raise TypeError(f"{name} must be a date")


def _strict_enum(value: Any, enum_type: type[Enum], name: str) -> None:
    if type(value) is not enum_type:
        raise TypeError(f"{name} must be {enum_type.__name__}")


def _strict_uuid(value: Any, name: str) -> None:
    if type(value) is not UUID:
        raise TypeError(f"{name} must be UUID")


def _freeze_json(value: Any) -> JSONValue:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        raise TypeError("binary float is not permitted in raw JSON data")
    if isinstance(value, Mapping):
        frozen: dict[str, JSONValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("raw mapping keys must be strings")
            frozen[key] = _freeze_json(item)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    raise TypeError(f"unsupported raw JSON value: {type(value).__name__}")


def _freeze_optional_mapping(
    value: Mapping[str, Any] | None,
) -> Mapping[str, JSONValue] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("raw payload must be a mapping or None")
    frozen = _freeze_json(value)
    assert isinstance(frozen, Mapping)
    return frozen


def _freeze_required_mapping(value: Any, name: str) -> Mapping[str, JSONValue]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    frozen = _freeze_json(value)
    assert isinstance(frozen, Mapping)
    return frozen


def _validate_scale(scale: Decimal | None, name: str) -> None:
    if scale is not None:
        if not isinstance(scale, Decimal):
            raise TypeError(f"{name} must be Decimal or None")
        if not scale.is_finite():
            raise ValueError(f"{name} must be finite")
        if scale <= 0:
            raise ValueError(f"{name} must be greater than zero")


def _validate_normalized(
    raw: int,
    scale: Decimal | None,
    normalized: Decimal | None,
    name: str,
) -> None:
    if type(raw) is not int:
        raise TypeError(f"{name}_raw must be an integer")
    _validate_scale(scale, f"{name}_scale")
    if normalized is not None and not isinstance(normalized, Decimal):
        raise TypeError(f"{name}_normalized must be Decimal or None")
    if normalized is not None and not normalized.is_finite():
        raise ValueError(f"{name}_normalized must be finite")
    if scale is None:
        if normalized is not None:
            raise ValueError(f"{name}_normalized must be None when scale is unknown")
    elif normalized != Decimal(raw) * scale:
        raise ValueError(f"{name}_normalized does not equal raw * scale")


_DIAGNOSTIC_KINDS = {
    "quote_lookup_failure",
    "stock_list_parse_failure",
    "adapter_error",
}


def _validate_diagnostic(instance: Any) -> None:
    _strict_str(instance.diagnostic_kind, "diagnostic_kind")
    if instance.diagnostic_kind not in _DIAGNOSTIC_KINDS:
        raise ValueError("invalid diagnostic_kind")
    for name in ("market_no_raw", "stock_idx_raw"):
        value = getattr(instance, name)
        if value is not None:
            _nonnegative(value, name)
    _strict_int(instance.error_code_raw, "error_code_raw", optional=True)
    _strict_str(instance.message, "message")
    if len(instance.message) > 1024:
        raise ValueError("message must not exceed 1024 characters")
    _require_taipei(instance.received_at, "received_at")
    _positive(instance.attempt, "attempt")
    _nonnegative(instance.connection_generation, "connection_generation")
    _nonnegative(instance.callback_sequence, "callback_sequence")
    object.__setattr__(
        instance,
        "raw_notification",
        _freeze_required_mapping(instance.raw_notification, "raw_notification"),
    )


@dataclass(frozen=True, slots=True)
class ConnectionStatus:
    state: ConnectionState
    broker_kind_raw: int | None
    broker_code_raw: int | None
    message: str | None
    is_ready: bool
    changed_at: datetime
    connection_generation: int

    def __post_init__(self) -> None:
        _strict_enum(self.state, ConnectionState, "state")
        _strict_int(self.broker_kind_raw, "broker_kind_raw", optional=True)
        _strict_int(self.broker_code_raw, "broker_code_raw", optional=True)
        if self.message is not None:
            _strict_str(self.message, "message", nonempty=False)
        _strict_bool(self.is_ready, "is_ready")
        _require_taipei(self.changed_at, "changed_at")
        _nonnegative(self.connection_generation, "connection_generation")


@dataclass(frozen=True, slots=True)
class ServerTime:
    event_at: datetime | None
    hour_raw: int
    minute_raw: int
    second_raw: int
    total_raw: int
    received_at: datetime
    trading_day: date | None

    def __post_init__(self) -> None:
        _strict_int(self.hour_raw, "hour_raw")
        _strict_int(self.minute_raw, "minute_raw")
        _strict_int(self.second_raw, "second_raw")
        _nonnegative(self.total_raw, "total_raw")
        if not 0 <= self.hour_raw <= 23:
            raise ValueError("hour_raw must be between 0 and 23")
        if not 0 <= self.minute_raw <= 59 or not 0 <= self.second_raw <= 59:
            raise ValueError("minute_raw/second_raw must be between 0 and 59")
        _strict_date(self.trading_day, "trading_day", optional=True)
        _require_taipei(self.event_at, "event_at", optional=True)
        _require_taipei(self.received_at, "received_at")


@dataclass(frozen=True, slots=True)
class Instrument:
    instrument_id: str
    symbol: str
    venue: str
    market_no: int | None
    stock_idx: int | None
    display_name: str | None
    asset_class: str | None
    currency: str | None
    price_scale: Decimal | None
    quantity_scale: Decimal | None
    metadata_version: int
    updated_at: datetime
    raw_payload: Mapping[str, Any] | None

    def __post_init__(self) -> None:
        _strict_str(self.instrument_id, "instrument_id")
        _strict_str(self.symbol, "symbol")
        _strict_str(self.venue, "venue")
        for name in ("display_name", "asset_class", "currency"):
            value = getattr(self, name)
            if value is not None:
                _strict_str(value, name, nonempty=False)
        if self.market_no is not None:
            _nonnegative(self.market_no, "market_no")
        if self.stock_idx is not None:
            _nonnegative(self.stock_idx, "stock_idx")
        _validate_scale(self.price_scale, "price_scale")
        _validate_scale(self.quantity_scale, "quantity_scale")
        _positive(self.metadata_version, "metadata_version")
        _require_taipei(self.updated_at, "updated_at")
        object.__setattr__(self, "raw_payload", _freeze_optional_mapping(self.raw_payload))


@dataclass(frozen=True, slots=True)
class Quote:
    instrument_id: str
    market_no_raw: int
    stock_idx_raw: int
    bid_raw: int
    ask_raw: int
    last_raw: int
    bid_normalized: Decimal | None
    ask_normalized: Decimal | None
    last_normalized: Decimal | None
    bid_qty_raw: int | None
    ask_qty_raw: int | None
    last_qty_raw: int | None
    event_at: datetime | None
    received_at: datetime
    trading_day: date | None
    is_simulated: bool | None
    is_long_callback: bool
    price_scale: Decimal | None = None

    def __post_init__(self) -> None:
        _strict_str(self.instrument_id, "instrument_id")
        _nonnegative(self.market_no_raw, "market_no_raw")
        _nonnegative(self.stock_idx_raw, "stock_idx_raw")
        for prefix in ("bid", "ask", "last"):
            _validate_normalized(
                getattr(self, f"{prefix}_raw"),
                self.price_scale,
                getattr(self, f"{prefix}_normalized"),
                prefix,
            )
        for name in ("bid_qty_raw", "ask_qty_raw", "last_qty_raw"):
            value = getattr(self, name)
            if value is not None:
                _nonnegative(value, name)
        _strict_date(self.trading_day, "trading_day", optional=True)
        _strict_bool(self.is_simulated, "is_simulated", optional=True)
        _strict_bool(self.is_long_callback, "is_long_callback")
        _require_taipei(self.event_at, "event_at", optional=True)
        _require_taipei(self.received_at, "received_at")


@dataclass(frozen=True, slots=True)
class Tick:
    instrument_id: str
    market_no_raw: int
    stock_idx_raw: int
    source_pointer_raw: int
    date_raw: int
    time_hms_raw: int
    time_subsecond_raw: int
    bid_raw: int
    ask_raw: int
    close_raw: int
    bid_normalized: Decimal | None
    ask_normalized: Decimal | None
    close_normalized: Decimal | None
    quantity_raw: int
    quantity_normalized: Decimal | None
    simulate_raw: int
    is_simulated: bool | None
    event_at: datetime | None
    received_at: datetime
    trading_day: date | None
    is_long_callback: bool
    price_scale: Decimal | None = None
    quantity_scale: Decimal | None = None

    def __post_init__(self) -> None:
        _strict_str(self.instrument_id, "instrument_id")
        _nonnegative(self.market_no_raw, "market_no_raw")
        _nonnegative(self.stock_idx_raw, "stock_idx_raw")
        _nonnegative(self.source_pointer_raw, "source_pointer_raw")
        _nonnegative(self.quantity_raw, "quantity_raw")
        for name in ("date_raw", "time_hms_raw", "time_subsecond_raw"):
            _nonnegative(getattr(self, name), name)
        _strict_int(self.simulate_raw, "simulate_raw")
        for prefix in ("bid", "ask", "close"):
            _validate_normalized(
                getattr(self, f"{prefix}_raw"),
                self.price_scale,
                getattr(self, f"{prefix}_normalized"),
                prefix,
            )
        _validate_normalized(
            self.quantity_raw,
            self.quantity_scale,
            self.quantity_normalized,
            "quantity",
        )
        _require_taipei(self.event_at, "event_at", optional=True)
        _require_taipei(self.received_at, "received_at")
        _strict_date(self.trading_day, "trading_day", optional=True)
        _strict_bool(self.is_simulated, "is_simulated", optional=True)
        _strict_bool(self.is_long_callback, "is_long_callback")


@dataclass(frozen=True, slots=True)
class AdapterDiagnostic:
    diagnostic_kind: str
    market_no_raw: int | None
    stock_idx_raw: int | None
    error_code_raw: int | None
    message: str
    received_at: datetime
    attempt: int
    connection_generation: int
    callback_sequence: int
    raw_notification: Mapping[str, Any]

    def __post_init__(self) -> None:
        # Redaction is the mapper's responsibility; this boundary validates only
        # the stable category and bounded, non-empty message contract.
        _validate_diagnostic(self)


@dataclass(frozen=True, slots=True)
class StaLocalQuoteNotification:
    market_no_raw: int
    stock_idx_raw: int
    is_long_callback: bool
    callback_sequence: int
    received_at: datetime

    def __post_init__(self) -> None:
        _nonnegative(self.market_no_raw, "market_no_raw")
        _nonnegative(self.stock_idx_raw, "stock_idx_raw")
        _nonnegative(self.callback_sequence, "callback_sequence")
        _strict_bool(self.is_long_callback, "is_long_callback")
        _require_taipei(self.received_at, "received_at")


@dataclass(frozen=True, slots=True)
class CapturedQuoteSnapshot:
    market_no_raw: int
    stock_idx_raw: int
    bid_raw: int
    ask_raw: int
    last_raw: int
    bid_qty_raw: int | None
    ask_qty_raw: int | None
    last_qty_raw: int | None
    is_long_callback: bool
    callback_sequence: int
    received_at: datetime

    def __post_init__(self) -> None:
        _nonnegative(self.market_no_raw, "market_no_raw")
        _nonnegative(self.stock_idx_raw, "stock_idx_raw")
        _nonnegative(self.callback_sequence, "callback_sequence")
        for name in ("bid_raw", "ask_raw", "last_raw"):
            _strict_int(getattr(self, name), name)
        for name in ("bid_qty_raw", "ask_qty_raw", "last_qty_raw"):
            value = getattr(self, name)
            if value is not None:
                _nonnegative(value, name)
        _strict_bool(self.is_long_callback, "is_long_callback")
        _require_taipei(self.received_at, "received_at")


@dataclass(frozen=True, slots=True)
class CapturedTickNotification:
    market_no_raw: int
    stock_idx_raw: int
    source_pointer_raw: int
    date_raw: int
    time_hms_raw: int
    time_subsecond_raw: int
    bid_raw: int
    ask_raw: int
    close_raw: int
    quantity_raw: int
    simulate_raw: int
    is_long_callback: bool
    callback_sequence: int
    received_at: datetime

    def __post_init__(self) -> None:
        _nonnegative(self.market_no_raw, "market_no_raw")
        _nonnegative(self.stock_idx_raw, "stock_idx_raw")
        _nonnegative(self.source_pointer_raw, "source_pointer_raw")
        _nonnegative(self.quantity_raw, "quantity_raw")
        for name in ("date_raw", "time_hms_raw", "time_subsecond_raw"):
            _nonnegative(getattr(self, name), name)
        for name in ("bid_raw", "ask_raw", "close_raw", "simulate_raw"):
            _strict_int(getattr(self, name), name)
        _nonnegative(self.callback_sequence, "callback_sequence")
        _strict_bool(self.is_long_callback, "is_long_callback")
        _require_taipei(self.received_at, "received_at")


@dataclass(frozen=True, slots=True)
class CapturedConnectionNotification:
    broker_kind_raw: int
    broker_code_raw: int
    callback_sequence: int
    received_at: datetime

    def __post_init__(self) -> None:
        _strict_int(self.broker_kind_raw, "broker_kind_raw")
        _strict_int(self.broker_code_raw, "broker_code_raw")
        _nonnegative(self.callback_sequence, "callback_sequence")
        _require_taipei(self.received_at, "received_at")


@dataclass(frozen=True, slots=True)
class CapturedServerTimeNotification:
    hour_raw: int
    minute_raw: int
    second_raw: int
    total_raw: int
    callback_sequence: int
    received_at: datetime

    def __post_init__(self) -> None:
        _strict_int(self.hour_raw, "hour_raw")
        _strict_int(self.minute_raw, "minute_raw")
        _strict_int(self.second_raw, "second_raw")
        _nonnegative(self.total_raw, "total_raw")
        if not 0 <= self.hour_raw <= 23:
            raise ValueError("hour_raw must be between 0 and 23")
        if not 0 <= self.minute_raw <= 59 or not 0 <= self.second_raw <= 59:
            raise ValueError("minute_raw/second_raw must be between 0 and 59")
        _nonnegative(self.callback_sequence, "callback_sequence")
        _require_taipei(self.received_at, "received_at")


@dataclass(frozen=True, slots=True)
class CapturedStockListNotification:
    market_no_raw: int
    stock_list_raw: str | bytes
    callback_sequence: int
    received_at: datetime

    def __post_init__(self) -> None:
        _nonnegative(self.market_no_raw, "market_no_raw")
        _nonnegative(self.callback_sequence, "callback_sequence")
        if type(self.stock_list_raw) not in (str, bytes):
            raise TypeError("stock_list_raw must be str or bytes")
        _require_taipei(self.received_at, "received_at")


@dataclass(frozen=True, slots=True)
class CapturedAdapterDiagnostic:
    diagnostic_kind: str
    market_no_raw: int | None
    stock_idx_raw: int | None
    error_code_raw: int | None
    message: str
    received_at: datetime
    attempt: int
    connection_generation: int
    callback_sequence: int
    raw_notification: Mapping[str, Any]

    def __post_init__(self) -> None:
        _validate_diagnostic(self)


IngressCapturedPayload: TypeAlias = (
    CapturedQuoteSnapshot
    | CapturedTickNotification
    | CapturedConnectionNotification
    | CapturedServerTimeNotification
    | CapturedStockListNotification
    | CapturedAdapterDiagnostic
)

_CAPTURED_PAYLOAD_TYPES = {
    CapturedKind.QUOTE_SNAPSHOT: CapturedQuoteSnapshot,
    CapturedKind.TICK_NOTIFICATION: CapturedTickNotification,
    CapturedKind.CONNECTION_NOTIFICATION: CapturedConnectionNotification,
    CapturedKind.SERVER_TIME_NOTIFICATION: CapturedServerTimeNotification,
    CapturedKind.STOCK_LIST_NOTIFICATION: CapturedStockListNotification,
    CapturedKind.ADAPTER_DIAGNOSTIC: CapturedAdapterDiagnostic,
}


@dataclass(frozen=True, slots=True)
class CapturedMarketDataEvent:
    captured_kind: CapturedKind
    payload: IngressCapturedPayload
    raw_payload: Mapping[str, Any] | None
    source: str
    source_mode: SourceMode
    session_id: UUID
    connection_generation: int
    sequence: int
    broker_sequence: int | None
    received_at: datetime
    event_at: datetime | None
    trading_day: date | None
    metadata_version: int | None
    dedupe_candidate: str | None

    def __post_init__(self) -> None:
        _strict_enum(self.captured_kind, CapturedKind, "captured_kind")
        expected = _CAPTURED_PAYLOAD_TYPES.get(self.captured_kind)
        if expected is None or type(self.payload) is not expected:
            raise ValueError("captured_kind does not match payload")
        _strict_str(self.source, "source")
        _strict_enum(self.source_mode, SourceMode, "source_mode")
        _strict_uuid(self.session_id, "session_id")
        if self.source_mode is SourceMode.REPLAY:
            raise ValueError("replay is not a Phase 1 capture source")
        _nonnegative(self.connection_generation, "connection_generation")
        _nonnegative(self.sequence, "sequence")
        if self.broker_sequence is not None:
            _nonnegative(self.broker_sequence, "broker_sequence")
        if self.metadata_version is not None:
            _positive(self.metadata_version, "metadata_version")
        if self.dedupe_candidate is not None:
            _strict_str(self.dedupe_candidate, "dedupe_candidate")
        _require_taipei(self.received_at, "received_at")
        _require_taipei(self.event_at, "event_at", optional=True)
        _strict_date(self.trading_day, "trading_day", optional=True)
        if self.payload.received_at != self.received_at:
            raise ValueError("payload received_at must match captured event")
        if self.payload.callback_sequence != self.sequence:
            raise ValueError("payload callback_sequence must match captured sequence")
        if (
            isinstance(self.payload, CapturedAdapterDiagnostic)
            and self.payload.connection_generation != self.connection_generation
        ):
            raise ValueError("diagnostic generation must match captured event")
        object.__setattr__(self, "raw_payload", _freeze_optional_mapping(self.raw_payload))


DomainPayload: TypeAlias = (
    ConnectionStatus | ServerTime | Instrument | Quote | Tick | AdapterDiagnostic
)
_EVENT_PAYLOAD_TYPES = {
    EventType.CONNECTION_STATUS: ConnectionStatus,
    EventType.SERVER_TIME: ServerTime,
    EventType.INSTRUMENT: Instrument,
    EventType.QUOTE: Quote,
    EventType.TICK: Tick,
    EventType.ADAPTER_DIAGNOSTIC: AdapterDiagnostic,
}


@dataclass(frozen=True, slots=True)
class MarketDataEnvelope:
    schema_version: int
    event_type: EventType
    payload: DomainPayload
    source: str
    source_mode: SourceMode
    session_id: UUID
    ingest_sequence: int
    connection_generation: int
    sequence: int
    broker_sequence: int | None
    dedupe_key: str
    event_at: datetime | None
    received_at: datetime
    trading_day: date | None
    metadata_version: int | None
    raw_payload: Mapping[str, Any] | None

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
        _strict_enum(self.event_type, EventType, "event_type")
        expected = _EVENT_PAYLOAD_TYPES.get(self.event_type)
        if expected is None or type(self.payload) is not expected:
            raise ValueError("event_type does not match payload")
        _strict_str(self.source, "source")
        _strict_enum(self.source_mode, SourceMode, "source_mode")
        _strict_uuid(self.session_id, "session_id")
        _strict_str(self.dedupe_key, "dedupe_key")
        _nonnegative(self.ingest_sequence, "ingest_sequence")
        _nonnegative(self.connection_generation, "connection_generation")
        _nonnegative(self.sequence, "sequence")
        if self.broker_sequence is not None:
            _nonnegative(self.broker_sequence, "broker_sequence")
        if self.metadata_version is not None:
            _positive(self.metadata_version, "metadata_version")
        _require_taipei(self.event_at, "event_at", optional=True)
        _require_taipei(self.received_at, "received_at")
        _strict_date(self.trading_day, "trading_day", optional=True)
        self._validate_metadata_mapping()
        object.__setattr__(self, "raw_payload", _freeze_optional_mapping(self.raw_payload))

    def _validate_metadata_mapping(self) -> None:
        payload = self.payload
        if isinstance(payload, ConnectionStatus):
            # A broker connection callback may not carry an authoritative event
            # time. In that case event_at remains None while changed_at records
            # the local callback receipt time.
            if self.event_at is not None and self.event_at != payload.changed_at:
                raise ValueError(
                    "connection_status event_at must be None or equal changed_at"
                )
            if self.connection_generation != payload.connection_generation:
                raise ValueError("connection generation must match envelope")
        elif isinstance(payload, Instrument):
            if self.event_at != payload.updated_at:
                raise ValueError("instrument event_at must equal updated_at")
            if (
                self.metadata_version is None
                or self.metadata_version != payload.metadata_version
            ):
                raise ValueError("instrument metadata_version must match envelope")
        elif isinstance(payload, (ServerTime, Quote, Tick)):
            if self.received_at != payload.received_at:
                raise ValueError("payload received_at must match envelope")
            if self.event_at != payload.event_at:
                raise ValueError("payload event_at must match envelope")
            if self.trading_day != payload.trading_day:
                raise ValueError("payload trading_day must match envelope")
        elif isinstance(payload, AdapterDiagnostic):
            if self.received_at != payload.received_at:
                raise ValueError("diagnostic received_at must match envelope")
            if self.connection_generation != payload.connection_generation:
                raise ValueError("diagnostic generation must match envelope")
            if self.sequence != payload.callback_sequence:
                raise ValueError("diagnostic callback sequence must match envelope")
            if self.event_at is not None or self.trading_day is not None:
                raise ValueError("diagnostic event_at and trading_day must be None")


def build_adapter_diagnostic_dedupe_key(
    source: str,
    session_id: UUID,
    connection_generation: int,
    diagnostic_kind: str,
    callback_sequence: int,
    attempt: int,
) -> str:
    """Build the canonical stable diagnostic retry discriminator."""

    _strict_str(source, "source")
    _strict_uuid(session_id, "session_id")
    _nonnegative(connection_generation, "connection_generation")
    _strict_str(diagnostic_kind, "diagnostic_kind")
    if diagnostic_kind not in _DIAGNOSTIC_KINDS:
        raise ValueError("invalid diagnostic_kind")
    _nonnegative(callback_sequence, "callback_sequence")
    _positive(attempt, "attempt")
    canonical = json.dumps(
        [
            source,
            str(session_id),
            connection_generation,
            diagnostic_kind,
            callback_sequence,
            attempt,
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return f"adapter_diagnostic:sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def to_primitive(value: Any) -> Any:
    """Convert supported contracts to deterministic JSON-compatible values."""

    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: to_primitive(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return {"encoding": "base64", "data": base64.b64encode(value).decode("ascii")}
    if isinstance(value, Mapping):
        return {
            str(key): to_primitive(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (tuple, list)):
        return [to_primitive(item) for item in value]
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        raise TypeError("binary float is not supported")
    raise TypeError(f"unsupported serialization value: {type(value).__name__}")


def serialize_envelope(envelope: MarketDataEnvelope) -> str:
    """Serialize an envelope using canonical, reproducible JSON."""

    return json.dumps(
        to_primitive(envelope),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
