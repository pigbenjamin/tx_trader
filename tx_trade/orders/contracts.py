"""Immutable, deterministic contracts for paper order execution."""

from __future__ import annotations

import json
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum, StrEnum
from typing import Any, TypeAlias
from uuid import UUID
from zoneinfo import ZoneInfo

TAIPEI = ZoneInfo("Asia/Taipei")


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"


class TimeInForce(StrEnum):
    DAY = "day"


class OrderStatus(StrEnum):
    ACCEPTED = "accepted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"

    @property
    def is_terminal(self) -> bool:
        return self in {self.FILLED, self.CANCELLED, self.REJECTED}


class ExecutionProvenance(StrEnum):
    PAPER = "paper"


class RejectionCode(StrEnum):
    INVALID_INTENT = "invalid_intent"
    UNKNOWN_ORDER = "unknown_order"
    ORDER_TERMINAL = "order_terminal"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    INSTRUMENT_UNAVAILABLE = "instrument_unavailable"
    MARKET_DATA_UNAVAILABLE = "market_data_unavailable"
    CAPACITY_EXCEEDED = "capacity_exceeded"
    INTERNAL_REJECTED = "internal_rejected"

    @property
    def public_message(self) -> str:
        return _REJECTION_MESSAGES[self]


_REJECTION_MESSAGES = {
    RejectionCode.INVALID_INTENT: "paper order intent is invalid",
    RejectionCode.UNKNOWN_ORDER: "paper order was not found",
    RejectionCode.ORDER_TERMINAL: "paper order is already terminal",
    RejectionCode.IDEMPOTENCY_CONFLICT: "paper order idempotency conflict",
    RejectionCode.INSTRUMENT_UNAVAILABLE: "paper instrument is unavailable",
    RejectionCode.MARKET_DATA_UNAVAILABLE: "paper market data is unavailable",
    RejectionCode.CAPACITY_EXCEEDED: "paper broker capacity was exceeded",
    RejectionCode.INTERNAL_REJECTED: "paper order was rejected",
}


class MatchDisposition(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"


class MatchSkipReason(StrEnum):
    EVENT_NOT_QUOTE = "event_not_quote"
    METADATA_UNAVAILABLE = "metadata_unavailable"
    METADATA_MISMATCH = "metadata_mismatch"
    PRICE_SCALE_UNAVAILABLE = "price_scale_unavailable"
    QUANTITY_SCALE_UNAVAILABLE = "quantity_scale_unavailable"
    PRICE_UNAVAILABLE = "price_unavailable"
    QUANTITY_UNAVAILABLE = "quantity_unavailable"
    SIMULATED_QUOTE = "simulated_quote"
    INVALID_BOOK = "invalid_book"
    NO_LIQUIDITY = "no_liquidity"
    ORDER_NOT_ELIGIBLE = "order_not_eligible"
    LIMIT_NOT_CROSSED = "limit_not_crossed"


class PaperEventType(StrEnum):
    ORDER_ACCEPTED = "order_accepted"
    ORDER_PARTIALLY_FILLED = "order_partially_filled"
    ORDER_FILLED = "order_filled"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_REJECTED = "order_rejected"
    FILL_RECORDED = "fill_recorded"
    POSITION_CHANGED = "position_changed"


def _strict_string(value: Any, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not value.strip():
        raise ValueError(f"{name} must not be empty")


def _strict_uuid(value: Any, name: str, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if type(value) is not UUID:
        raise TypeError(f"{name} must be UUID")


def _strict_enum(value: Any, enum_type: type[Enum], name: str) -> None:
    if type(value) is not enum_type:
        raise TypeError(f"{name} must be {enum_type.__name__}")


def _strict_bool(value: Any, name: str) -> None:
    if type(value) is not bool:
        raise TypeError(f"{name} must be bool")


def _taipei_datetime(value: Any, name: str, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    if getattr(value.tzinfo, "key", None) != TAIPEI.key:
        raise ValueError(f"{name} must use Asia/Taipei timezone")


def _decimal(
    value: Any,
    name: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
    optional: bool = False,
) -> None:
    if value is None and optional:
        return
    if type(value) is not Decimal:
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    if positive and value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    if nonnegative and value < 0:
        raise ValueError(f"{name} must be non-negative")


def _positive_int(value: Any, name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 1:
        raise ValueError(f"{name} must be at least 1")


def _nonnegative_int(value: Any, name: str, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _source_pair(session_id: UUID | None, sequence: int | None) -> None:
    if (session_id is None) != (sequence is None):
        raise ValueError("source_session_id and source_ingest_sequence must be provided together")
    _strict_uuid(session_id, "source_session_id", optional=True)
    _nonnegative_int(sequence, "source_ingest_sequence", optional=True)


@dataclass(frozen=True, slots=True)
class OrderIntent:
    strategy_id: str
    client_order_id: str
    account_id: str
    instrument_id: str
    side: OrderSide
    quantity: Decimal
    order_type: OrderType
    limit_price: Decimal | None
    time_in_force: TimeInForce
    day_trade: bool
    created_at: datetime
    source_session_id: UUID | None = None
    source_ingest_sequence: int | None = None

    def __post_init__(self) -> None:
        for name in ("strategy_id", "client_order_id", "account_id", "instrument_id"):
            _strict_string(getattr(self, name), name)
        _strict_enum(self.side, OrderSide, "side")
        _decimal(self.quantity, "quantity", positive=True)
        _strict_enum(self.order_type, OrderType, "order_type")
        _strict_enum(self.time_in_force, TimeInForce, "time_in_force")
        _strict_bool(self.day_trade, "day_trade")
        _taipei_datetime(self.created_at, "created_at")
        _source_pair(self.source_session_id, self.source_ingest_sequence)
        if self.order_type is OrderType.MARKET:
            if self.limit_price is not None:
                raise ValueError("market orders must not have a limit_price")
        else:
            _decimal(self.limit_price, "limit_price", positive=True)


@dataclass(frozen=True, slots=True)
class CancelIntent:
    strategy_id: str
    client_order_id: str
    paper_order_id: UUID
    requested_at: datetime

    def __post_init__(self) -> None:
        _strict_string(self.strategy_id, "strategy_id")
        _strict_string(self.client_order_id, "client_order_id")
        _strict_uuid(self.paper_order_id, "paper_order_id")
        _taipei_datetime(self.requested_at, "requested_at")


@dataclass(frozen=True, slots=True)
class PaperOrder:
    paper_run_id: UUID
    paper_order_id: UUID
    intent: OrderIntent
    status: OrderStatus
    filled_quantity: Decimal
    remaining_quantity: Decimal
    average_fill_price: Decimal | None
    accepted_at: datetime | None
    updated_at: datetime
    provenance: ExecutionProvenance = ExecutionProvenance.PAPER

    def __post_init__(self) -> None:
        _strict_uuid(self.paper_run_id, "paper_run_id")
        _strict_uuid(self.paper_order_id, "paper_order_id")
        if type(self.intent) is not OrderIntent:
            raise TypeError("intent must be OrderIntent")
        _strict_enum(self.status, OrderStatus, "status")
        _decimal(self.filled_quantity, "filled_quantity", nonnegative=True)
        _decimal(self.remaining_quantity, "remaining_quantity", nonnegative=True)
        _decimal(
            self.average_fill_price,
            "average_fill_price",
            positive=True,
            optional=True,
        )
        _taipei_datetime(self.accepted_at, "accepted_at", optional=True)
        _taipei_datetime(self.updated_at, "updated_at")
        _strict_enum(self.provenance, ExecutionProvenance, "provenance")
        if self.filled_quantity + self.remaining_quantity != self.intent.quantity:
            raise ValueError("filled_quantity + remaining_quantity must equal intent quantity")
        if (self.filled_quantity == 0) != (self.average_fill_price is None):
            raise ValueError("average_fill_price must be present exactly when quantity is filled")
        if self.status in {OrderStatus.ACCEPTED, OrderStatus.REJECTED}:
            if self.filled_quantity != 0:
                raise ValueError("accepted/rejected orders must have zero filled_quantity")
        elif self.status is OrderStatus.PARTIALLY_FILLED:
            if not 0 < self.filled_quantity < self.intent.quantity:
                raise ValueError("partially filled orders require a partial filled_quantity")
        elif self.status is OrderStatus.FILLED:
            if self.filled_quantity != self.intent.quantity:
                raise ValueError("filled orders require the full intent quantity")
        elif self.status is OrderStatus.CANCELLED and self.remaining_quantity <= 0:
            raise ValueError("cancelled orders must have remaining quantity")
        if self.status is OrderStatus.REJECTED:
            if self.accepted_at is not None:
                raise ValueError("rejected orders must not have accepted_at")
        else:
            if self.accepted_at is None:
                raise ValueError("non-rejected orders require accepted_at")
            if self.accepted_at < self.intent.created_at:
                raise ValueError("accepted_at must not precede intent.created_at")
        if self.updated_at < self.intent.created_at:
            raise ValueError("updated_at must not precede intent.created_at")
        if self.accepted_at is not None and self.updated_at < self.accepted_at:
            raise ValueError("updated_at must not precede accepted_at")


@dataclass(frozen=True, slots=True)
class PaperFill:
    paper_run_id: UUID
    paper_fill_id: UUID
    paper_order_id: UUID
    strategy_id: str
    account_id: str
    instrument_id: str
    side: OrderSide
    quantity: Decimal
    execution_price: Decimal
    fee: Decimal
    source_session_id: UUID
    source_ingest_sequence: int
    occurred_at: datetime
    provenance: ExecutionProvenance = ExecutionProvenance.PAPER

    def __post_init__(self) -> None:
        for name in ("paper_run_id", "paper_fill_id", "paper_order_id"):
            _strict_uuid(getattr(self, name), name)
        for name in ("strategy_id", "account_id", "instrument_id"):
            _strict_string(getattr(self, name), name)
        _strict_enum(self.side, OrderSide, "side")
        _decimal(self.quantity, "quantity", positive=True)
        _decimal(self.execution_price, "execution_price", positive=True)
        _decimal(self.fee, "fee", nonnegative=True)
        _source_pair(self.source_session_id, self.source_ingest_sequence)
        _taipei_datetime(self.occurred_at, "occurred_at")
        _strict_enum(self.provenance, ExecutionProvenance, "provenance")


@dataclass(frozen=True, slots=True)
class PaperRejection:
    paper_run_id: UUID
    strategy_id: str
    client_order_id: str
    code: RejectionCode
    rejected_at: datetime
    paper_order_id: UUID | None = None
    provenance: ExecutionProvenance = ExecutionProvenance.PAPER

    def __post_init__(self) -> None:
        _strict_uuid(self.paper_run_id, "paper_run_id")
        _strict_uuid(self.paper_order_id, "paper_order_id", optional=True)
        _strict_string(self.strategy_id, "strategy_id")
        _strict_string(self.client_order_id, "client_order_id")
        _strict_enum(self.code, RejectionCode, "code")
        _taipei_datetime(self.rejected_at, "rejected_at")
        _strict_enum(self.provenance, ExecutionProvenance, "provenance")

    @property
    def message(self) -> str:
        return self.code.public_message


@dataclass(frozen=True, slots=True)
class PaperPosition:
    paper_run_id: UUID
    strategy_id: str
    account_id: str
    instrument_id: str
    net_quantity: Decimal
    average_open_price: Decimal | None
    cumulative_fees: Decimal
    version: int
    updated_at: datetime
    provenance: ExecutionProvenance = ExecutionProvenance.PAPER

    def __post_init__(self) -> None:
        _strict_uuid(self.paper_run_id, "paper_run_id")
        for name in ("strategy_id", "account_id", "instrument_id"):
            _strict_string(getattr(self, name), name)
        _decimal(self.net_quantity, "net_quantity")
        _decimal(
            self.average_open_price,
            "average_open_price",
            positive=True,
            optional=True,
        )
        _decimal(self.cumulative_fees, "cumulative_fees", nonnegative=True)
        _positive_int(self.version, "version")
        _taipei_datetime(self.updated_at, "updated_at")
        _strict_enum(self.provenance, ExecutionProvenance, "provenance")
        if (self.net_quantity == 0) != (self.average_open_price is None):
            raise ValueError(
                "average_open_price must be present exactly when net_quantity is non-zero"
            )


PaperEventPayload: TypeAlias = PaperOrder | PaperFill | PaperRejection | PaperPosition

_ORDER_EVENT_STATUSES = {
    PaperEventType.ORDER_ACCEPTED: OrderStatus.ACCEPTED,
    PaperEventType.ORDER_PARTIALLY_FILLED: OrderStatus.PARTIALLY_FILLED,
    PaperEventType.ORDER_FILLED: OrderStatus.FILLED,
    PaperEventType.ORDER_CANCELLED: OrderStatus.CANCELLED,
}
_MARKET_CAUSED_EVENT_TYPES = {
    PaperEventType.ORDER_PARTIALLY_FILLED,
    PaperEventType.ORDER_FILLED,
    PaperEventType.FILL_RECORDED,
    PaperEventType.POSITION_CHANGED,
}


@dataclass(frozen=True, slots=True)
class PaperEvent:
    paper_run_id: UUID
    paper_event_id: UUID
    paper_sequence: int
    event_type: PaperEventType
    payload: PaperEventPayload
    occurred_at: datetime
    source_session_id: UUID | None = None
    source_ingest_sequence: int | None = None
    provenance: ExecutionProvenance = ExecutionProvenance.PAPER

    def __post_init__(self) -> None:
        _strict_uuid(self.paper_run_id, "paper_run_id")
        _strict_uuid(self.paper_event_id, "paper_event_id")
        _positive_int(self.paper_sequence, "paper_sequence")
        _strict_enum(self.event_type, PaperEventType, "event_type")
        _taipei_datetime(self.occurred_at, "occurred_at")
        _source_pair(self.source_session_id, self.source_ingest_sequence)
        _strict_enum(self.provenance, ExecutionProvenance, "provenance")
        if type(self.payload) not in {
            PaperOrder,
            PaperFill,
            PaperRejection,
            PaperPosition,
        }:
            raise TypeError("payload must be a paper event payload")
        if self.payload.paper_run_id != self.paper_run_id:
            raise ValueError("payload paper_run_id must match event")
        expected_status = _ORDER_EVENT_STATUSES.get(self.event_type)
        if expected_status is not None:
            if type(self.payload) is not PaperOrder or self.payload.status is not expected_status:
                raise ValueError("order event type must match PaperOrder status")
        elif self.event_type is PaperEventType.ORDER_REJECTED:
            if type(self.payload) is not PaperRejection:
                raise ValueError("order_rejected payload must be PaperRejection")
        elif self.event_type is PaperEventType.FILL_RECORDED:
            if type(self.payload) is not PaperFill:
                raise ValueError("fill_recorded payload must be PaperFill")
        elif self.event_type is PaperEventType.POSITION_CHANGED:
            if type(self.payload) is not PaperPosition:
                raise ValueError("position_changed payload must be PaperPosition")
        if self.event_type in _MARKET_CAUSED_EVENT_TYPES and self.source_session_id is None:
            raise ValueError("fill-driven and position events require market-data source causation")
        expected_time = (
            self.payload.updated_at
            if isinstance(self.payload, (PaperOrder, PaperPosition))
            else self.payload.occurred_at
            if isinstance(self.payload, PaperFill)
            else self.payload.rejected_at
        )
        if self.occurred_at != expected_time:
            raise ValueError("payload time must match event occurred_at")
        if isinstance(self.payload, PaperFill):
            if (
                self.source_session_id != self.payload.source_session_id
                or self.source_ingest_sequence != self.payload.source_ingest_sequence
            ):
                raise ValueError("fill source causation must match event")


@dataclass(frozen=True, slots=True)
class InstrumentMetadataSnapshot:
    instrument_id: str
    metadata_version: int
    price_scale: Decimal | None
    quantity_scale: Decimal | None

    def __post_init__(self) -> None:
        _strict_string(self.instrument_id, "instrument_id")
        _positive_int(self.metadata_version, "metadata_version")
        _decimal(self.price_scale, "price_scale", positive=True, optional=True)
        _decimal(self.quantity_scale, "quantity_scale", positive=True, optional=True)


@dataclass(frozen=True, slots=True)
class PaperBrokerLimits:
    max_orders: int
    max_open_orders: int
    max_fills: int
    max_events: int
    max_market_data_records: int
    max_instrument_versions: int

    def __post_init__(self) -> None:
        _positive_int(self.max_orders, "max_orders")
        _positive_int(self.max_open_orders, "max_open_orders")
        _positive_int(self.max_fills, "max_fills")
        _positive_int(self.max_events, "max_events")
        _positive_int(self.max_market_data_records, "max_market_data_records")
        _positive_int(self.max_instrument_versions, "max_instrument_versions")
        if self.max_open_orders > self.max_orders:
            raise ValueError("max_open_orders must not exceed max_orders")


@dataclass(frozen=True, slots=True)
class MatchResult:
    paper_run_id: UUID
    disposition: MatchDisposition
    source_session_id: UUID
    source_ingest_sequence: int
    fills: tuple[PaperFill, ...]
    events: tuple[PaperEvent, ...]
    skip_reasons: tuple[MatchSkipReason, ...]
    snapshot_version: int

    def __post_init__(self) -> None:
        _strict_uuid(self.paper_run_id, "paper_run_id")
        _strict_enum(self.disposition, MatchDisposition, "disposition")
        _source_pair(self.source_session_id, self.source_ingest_sequence)
        _nonnegative_int(self.snapshot_version, "snapshot_version")
        _strict_contract_tuple(self.fills, PaperFill, "fills")
        _strict_contract_tuple(self.events, PaperEvent, "events")
        _strict_contract_tuple(self.skip_reasons, MatchSkipReason, "skip_reasons")
        if len(set(self.skip_reasons)) != len(self.skip_reasons):
            raise ValueError("skip_reasons must not contain duplicates")
        for fill in self.fills:
            if fill.paper_run_id != self.paper_run_id:
                raise ValueError("fill paper_run_id must match result")
            if (
                fill.source_session_id != self.source_session_id
                or fill.source_ingest_sequence != self.source_ingest_sequence
            ):
                raise ValueError("fill source causation must match result")
        for event in self.events:
            if event.paper_run_id != self.paper_run_id:
                raise ValueError("event paper_run_id must match result")
            if event.source_session_id is not None and (
                event.source_session_id != self.source_session_id
                or event.source_ingest_sequence != self.source_ingest_sequence
            ):
                raise ValueError("event source causation must match result")
        if self.disposition is MatchDisposition.DUPLICATE and (self.fills or self.events):
            raise ValueError("duplicate results must not contain fills or events")


@dataclass(frozen=True, slots=True)
class PaperBrokerSnapshot:
    paper_run_id: UUID
    bound_source_session_id: UUID | None
    last_committed_ingest_sequence: int | None
    next_paper_sequence: int
    snapshot_version: int
    orders: tuple[PaperOrder, ...]
    fills: tuple[PaperFill, ...]
    events: tuple[PaperEvent, ...]
    instruments: tuple[InstrumentMetadataSnapshot, ...]

    def __post_init__(self) -> None:
        _strict_uuid(self.paper_run_id, "paper_run_id")
        _source_pair(
            self.bound_source_session_id,
            self.last_committed_ingest_sequence,
        )
        _positive_int(self.next_paper_sequence, "next_paper_sequence")
        _nonnegative_int(self.snapshot_version, "snapshot_version")
        _strict_contract_tuple(self.orders, PaperOrder, "orders")
        _strict_contract_tuple(self.fills, PaperFill, "fills")
        _strict_contract_tuple(self.events, PaperEvent, "events")
        _strict_contract_tuple(
            self.instruments,
            InstrumentMetadataSnapshot,
            "instruments",
        )
        for collection_name, collection in (
            ("order", self.orders),
            ("fill", self.fills),
            ("event", self.events),
        ):
            if any(item.paper_run_id != self.paper_run_id for item in collection):
                raise ValueError(f"{collection_name} paper_run_id must match snapshot")
        expected_next_sequence = 1 if not self.events else self.events[-1].paper_sequence + 1
        if self.next_paper_sequence != expected_next_sequence:
            raise ValueError("next_paper_sequence must follow the event journal")
        if any(
            left.paper_sequence >= right.paper_sequence
            for left, right in zip(self.events, self.events[1:], strict=False)
        ):
            raise ValueError("events must have strictly increasing paper_sequence")


def _strict_contract_tuple(value: Any, item_type: type[Enum] | type[Any], name: str) -> None:
    if type(value) is not tuple:
        raise TypeError(f"{name} must be a tuple")
    if any(type(item) is not item_type for item in value):
        raise TypeError(f"{name} must contain only {item_type.__name__}")


def to_canonical_primitive(value: Any) -> Any:
    """Convert an order contract to deterministic JSON-compatible values."""

    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: to_canonical_primitive(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return _canonical_decimal(value)
    if isinstance(value, tuple):
        return [to_canonical_primitive(item) for item in value]
    if value is None or type(value) in {str, int, bool}:
        return value
    if isinstance(value, float):
        raise TypeError("binary float is not supported")
    raise TypeError(f"unsupported serialization value: {type(value).__name__}")


def _canonical_decimal(value: Decimal) -> str:
    """Return the shortest stable plain/scientific form without expanding exponents.

    Decimal scale is not business data, so coefficient trailing zeroes are
    removed. A plain representation is emitted only when it is no longer than
    normalized scientific notation. Consequently output size is bounded by the
    coefficient digit count plus the exponent's own digit count.
    """

    if not value.is_finite():
        raise ValueError("canonical Decimal values must be finite")
    if value == 0:
        return "0"

    parts = value.as_tuple()
    digits = list(parts.digits)
    exponent = parts.exponent
    assert isinstance(exponent, int)
    while digits[-1] == 0:
        digits.pop()
        exponent += 1

    coefficient = "".join(chr(48 + digit) for digit in digits)
    sign = "-" if parts.sign else ""
    adjusted_exponent = exponent + len(coefficient) - 1
    scientific_coefficient = (
        coefficient if len(coefficient) == 1 else f"{coefficient[0]}.{coefficient[1:]}"
    )
    scientific = (
        f"{sign}{scientific_coefficient}"
        if adjusted_exponent == 0
        else f"{sign}{scientific_coefficient}e{adjusted_exponent}"
    )

    point = len(coefficient) + exponent
    if point <= 0:
        plain_length = len(sign) + 2 - point + len(coefficient)
    elif point >= len(coefficient):
        plain_length = len(sign) + point
    else:
        plain_length = len(sign) + len(coefficient) + 1
    if plain_length > len(scientific):
        return scientific
    if point <= 0:
        return f"{sign}0.{('0' * -point)}{coefficient}"
    if point >= len(coefficient):
        return f"{sign}{coefficient}{('0' * exponent)}"
    return f"{sign}{coefficient[:point]}.{coefficient[point:]}"


def canonical_json(
    value: (
        PaperEventPayload
        | PaperEvent
        | OrderIntent
        | CancelIntent
        | InstrumentMetadataSnapshot
        | PaperBrokerLimits
        | MatchResult
        | PaperBrokerSnapshot
    ),
) -> str:
    """Serialize a Phase 2B contract using stable, compact canonical JSON."""

    return json.dumps(
        to_canonical_primitive(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
