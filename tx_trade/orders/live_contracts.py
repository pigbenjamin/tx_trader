"""Side-effect-free, broker-neutral contracts for live order execution.

This module deliberately contains no broker SDK imports, configuration reads, or
I/O.  Dispatch is transport evidence; only normalized broker events are
authoritative evidence of acceptance or execution.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum, StrEnum
from hashlib import sha256
from typing import Any, TypeAlias

MAX_IDENTIFIER_LENGTH = 128
CLIENT_ORDER_ID_UNIQUENESS_CONTRACT = "global:v1"
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_FAILURE_MESSAGES: dict["LiveFailureCode", str]


class LiveSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class LiveOrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"


class LiveTimeInForce(StrEnum):
    DAY = "day"
    IOC = "ioc"
    FOK = "fok"


class LiveOrderState(StrEnum):
    CREATED = "created"
    VALIDATED = "validated"
    SUBMITTING = "submitting"
    ACCEPTED = "accepted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCEL_PENDING = "cancel_pending"
    CANCELLED = "cancelled"
    SUBMISSION_UNKNOWN = "submission_unknown"
    RECONCILING = "reconciling"

    @property
    def is_terminal(self) -> bool:
        return self in {self.FILLED, self.REJECTED, self.CANCELLED}


class LiveCommandKind(StrEnum):
    NEW = "new"
    CANCEL = "cancel"
    AMEND = "amend"
    DECREASE = "decrease"


class DispatchState(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNKNOWN = "unknown"

    @property
    def is_authoritative_acceptance(self) -> bool:
        """Dispatch never proves that a broker accepted an order."""

        return False


class CommandDeduplication(StrEnum):
    FIRST_SEEN = "first_seen"
    EXACT_RETRY = "exact_retry"
    PAYLOAD_CONFLICT = "payload_conflict"


class BrokerOrderEventType(StrEnum):
    NEW_ACCEPTED = "new_accepted"
    NEW_REJECTED = "new_rejected"
    CANCEL_PENDING = "cancel_pending"
    CANCELLED = "cancelled"
    CANCEL_REJECTED = "cancel_rejected"
    DYNAMIC_CANCELLED = "dynamic_cancelled"
    PRICE_AMENDED = "price_amended"
    QUANTITY_DECREASED = "quantity_decreased"
    PRICE_AND_QUANTITY_AMENDED = "price_and_quantity_amended"
    AMEND_REJECTED = "amend_rejected"
    OUTCOME_UNKNOWN = "outcome_unknown"


class CorrelationStatus(StrEnum):
    CANDIDATE = "candidate"
    CONFIRMED = "confirmed"
    AMBIGUOUS = "ambiguous"


class AccountReadiness(StrEnum):
    READY = "ready"
    NOT_READY = "not_ready"
    UNKNOWN = "unknown"


class ReconciliationKind(StrEnum):
    MISSING_LOCAL_ORDER = "missing_local_order"
    MISSING_BROKER_ORDER = "missing_broker_order"
    ORDER_STATE_MISMATCH = "order_state_mismatch"
    QUANTITY_MISMATCH = "quantity_mismatch"
    POSITION_MISMATCH = "position_mismatch"
    FILL_MISMATCH = "fill_mismatch"
    CORRELATION_MISSING = "correlation_missing"


class LiveFailureCode(StrEnum):
    INVALID_COMMAND = "invalid_command"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    ACCOUNT_NOT_READY = "account_not_ready"
    RISK_REJECTED = "risk_rejected"
    DISPATCH_FAILED = "dispatch_failed"
    DISPATCH_OUTCOME_UNKNOWN = "dispatch_outcome_unknown"
    BROKER_REJECTED = "broker_rejected"
    CANCEL_REJECTED = "cancel_rejected"
    AMEND_REJECTED = "amend_rejected"
    BROKER_TIMEOUT = "broker_timeout"
    CORRELATION_CONFLICT = "correlation_conflict"
    BROKER_EVENT_INVALID = "broker_event_invalid"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    INTERNAL_FAILURE = "internal_failure"

    @property
    def public_message(self) -> str:
        return _FAILURE_MESSAGES[self]


_FAILURE_MESSAGES = {
    LiveFailureCode.INVALID_COMMAND: "live command is invalid",
    LiveFailureCode.IDEMPOTENCY_CONFLICT: "live command idempotency conflict",
    LiveFailureCode.ACCOUNT_NOT_READY: "live account is not ready",
    LiveFailureCode.RISK_REJECTED: "live command was rejected by risk controls",
    LiveFailureCode.DISPATCH_FAILED: "live command dispatch failed",
    LiveFailureCode.DISPATCH_OUTCOME_UNKNOWN: "live command dispatch outcome is unknown",
    LiveFailureCode.BROKER_REJECTED: "live command was rejected by broker",
    LiveFailureCode.CANCEL_REJECTED: "live cancel was rejected by broker",
    LiveFailureCode.AMEND_REJECTED: "live amendment was rejected by broker",
    LiveFailureCode.BROKER_TIMEOUT: "live broker outcome timed out",
    LiveFailureCode.CORRELATION_CONFLICT: "live broker correlation is ambiguous",
    LiveFailureCode.BROKER_EVENT_INVALID: "live broker event is invalid",
    LiveFailureCode.RECONCILIATION_REQUIRED: "live state requires reconciliation",
    LiveFailureCode.INTERNAL_FAILURE: "live operation failed",
}


class FingerprintDomain(StrEnum):
    NEW_COMMAND_V1 = "tx_trade.live.command.new.v1"
    CANCEL_COMMAND_V1 = "tx_trade.live.command.cancel.v1"
    AMEND_COMMAND_V1 = "tx_trade.live.command.amend.v1"
    DECREASE_COMMAND_V1 = "tx_trade.live.command.decrease.v1"
    BROKER_ORDER_EVENT_V1 = "tx_trade.live.broker.order-event.v1"
    BROKER_FILL_EVENT_V1 = "tx_trade.live.broker.fill-event.v1"


def _identifier(value: Any, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be 1..{MAX_IDENTIFIER_LENGTH} ASCII identifier characters")


def _enum(value: Any, expected: type[Enum], name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{name} must be {expected.__name__}")


def _utc(value: Any, name: str, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware UTC")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


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
    exponent = value.as_tuple().exponent
    if (
        len(value.as_tuple().digits) > 34
        or not isinstance(exponent, int)
        or not -6143 <= exponent <= 6144
    ):
        raise ValueError(f"{name} exceeds supported Decimal bounds")
    if positive and value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    if nonnegative and value < 0:
        raise ValueError(f"{name} must be non-negative")


def _optional_identifier(value: Any, name: str) -> None:
    if value is not None:
        _identifier(value, name)


def _positive_int(value: Any, name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 1:
        raise ValueError(f"{name} must be positive")


@dataclass(frozen=True, slots=True)
class LiveOrderIntent:
    strategy_id: str
    client_order_id: str
    account_id: str = field(repr=False)
    instrument_id: str
    side: LiveSide
    quantity: Decimal
    order_type: LiveOrderType
    limit_price: Decimal | None
    time_in_force: LiveTimeInForce
    day_trade: bool
    created_at: datetime

    def __post_init__(self) -> None:
        for name in ("strategy_id", "client_order_id", "account_id", "instrument_id"):
            _identifier(getattr(self, name), name)
        _enum(self.side, LiveSide, "side")
        _decimal(self.quantity, "quantity", positive=True)
        _enum(self.order_type, LiveOrderType, "order_type")
        _decimal(self.limit_price, "limit_price", positive=True, optional=True)
        if self.order_type is LiveOrderType.MARKET and self.limit_price is not None:
            raise ValueError("market order must not have limit_price")
        if self.order_type is LiveOrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit order requires limit_price")
        _enum(self.time_in_force, LiveTimeInForce, "time_in_force")
        if type(self.day_trade) is not bool:
            raise TypeError("day_trade must be bool")
        _utc(self.created_at, "created_at")


@dataclass(frozen=True, slots=True)
class NewOrderCommand:
    client_command_id: str
    intent: LiveOrderIntent
    requested_at: datetime
    kind: LiveCommandKind = field(default=LiveCommandKind.NEW, init=False)

    def __post_init__(self) -> None:
        _identifier(self.client_command_id, "client_command_id")
        if type(self.intent) is not LiveOrderIntent:
            raise TypeError("intent must be LiveOrderIntent")
        _utc(self.requested_at, "requested_at")


@dataclass(frozen=True, slots=True)
class CancelOrderCommand:
    client_command_id: str
    client_order_id: str
    requested_at: datetime
    kind: LiveCommandKind = field(default=LiveCommandKind.CANCEL, init=False)

    def __post_init__(self) -> None:
        _identifier(self.client_command_id, "client_command_id")
        _identifier(self.client_order_id, "client_order_id")
        _utc(self.requested_at, "requested_at")


@dataclass(frozen=True, slots=True)
class AmendOrderCommand:
    client_command_id: str
    client_order_id: str
    new_limit_price: Decimal
    requested_at: datetime
    kind: LiveCommandKind = field(default=LiveCommandKind.AMEND, init=False)

    def __post_init__(self) -> None:
        _identifier(self.client_command_id, "client_command_id")
        _identifier(self.client_order_id, "client_order_id")
        _decimal(self.new_limit_price, "new_limit_price", positive=True)
        _utc(self.requested_at, "requested_at")


@dataclass(frozen=True, slots=True)
class DecreaseOrderCommand:
    client_command_id: str
    client_order_id: str
    expected_total_quantity: Decimal
    new_total_quantity: Decimal
    requested_at: datetime
    kind: LiveCommandKind = field(default=LiveCommandKind.DECREASE, init=False)

    def __post_init__(self) -> None:
        _identifier(self.client_command_id, "client_command_id")
        _identifier(self.client_order_id, "client_order_id")
        _decimal(self.expected_total_quantity, "expected_total_quantity", positive=True)
        _decimal(self.new_total_quantity, "new_total_quantity", positive=True)
        if self.new_total_quantity >= self.expected_total_quantity:
            raise ValueError("new_total_quantity must be less than expected_total_quantity")
        _utc(self.requested_at, "requested_at")


LiveCommand: TypeAlias = (
    NewOrderCommand | CancelOrderCommand | AmendOrderCommand | DecreaseOrderCommand
)


@dataclass(frozen=True, slots=True)
class PendingCommandBinding:
    command: LiveCommand = field(repr=False)
    payload_fingerprint: str = field(repr=False)

    def __post_init__(self) -> None:
        _fingerprint(self.payload_fingerprint, "payload_fingerprint")
        domains = {
            NewOrderCommand: FingerprintDomain.NEW_COMMAND_V1,
            CancelOrderCommand: FingerprintDomain.CANCEL_COMMAND_V1,
            AmendOrderCommand: FingerprintDomain.AMEND_COMMAND_V1,
            DecreaseOrderCommand: FingerprintDomain.DECREASE_COMMAND_V1,
        }
        if type(self.command) not in domains:
            raise TypeError("command must be a concrete live command")
        expected = payload_fingerprint(self.command, domains[type(self.command)])
        if self.payload_fingerprint != expected:
            raise ValueError("payload_fingerprint must match the exact bound command")

    @property
    def client_command_id(self) -> str:
        return self.command.client_command_id

    @property
    def command_kind(self) -> LiveCommandKind:
        return self.command.kind

    @property
    def bound_at(self) -> datetime:
        return self.command.requested_at

    @property
    def expected_new_limit_price(self) -> Decimal | None:
        if type(self.command) is AmendOrderCommand:
            return self.command.new_limit_price
        return None

    @property
    def expected_current_total_quantity(self) -> Decimal | None:
        if type(self.command) is DecreaseOrderCommand:
            return self.command.expected_total_quantity
        return None

    @property
    def expected_new_total_quantity(self) -> Decimal | None:
        if type(self.command) is DecreaseOrderCommand:
            return self.command.new_total_quantity
        return None

    def matches_authoritative_working_change(
        self,
        event: NormalizedBrokerOrderEvent,
        *,
        current_total_quantity: Decimal,
    ) -> bool:
        """Return whether broker working changes prove this exact pending target."""

        if type(event) is not NormalizedBrokerOrderEvent:
            raise TypeError("event must be NormalizedBrokerOrderEvent")
        _decimal(current_total_quantity, "current_total_quantity", positive=True)
        if self.command_kind is LiveCommandKind.AMEND:
            if event.event_type is BrokerOrderEventType.PRICE_AMENDED:
                return event.new_limit_price == self.expected_new_limit_price
        if (
            self.command_kind is LiveCommandKind.DECREASE
            and event.event_type is BrokerOrderEventType.QUANTITY_DECREASED
            and event.decreased_quantity is not None
        ):
            return (
                current_total_quantity == self.expected_current_total_quantity
                and current_total_quantity - event.decreased_quantity
                == self.expected_new_total_quantity
            )
        return False


@dataclass(frozen=True, slots=True)
class LiveOrder:
    intent: LiveOrderIntent
    state: LiveOrderState
    total_quantity: Decimal
    filled_quantity: Decimal
    remaining_quantity: Decimal
    average_fill_price: Decimal | None
    working_limit_price: Decimal | None
    version: int
    updated_at: datetime
    accepted_at: datetime | None = None
    pending_command: PendingCommandBinding | None = None

    def __post_init__(self) -> None:
        if type(self.intent) is not LiveOrderIntent:
            raise TypeError("intent must be LiveOrderIntent")
        _enum(self.state, LiveOrderState, "state")
        _decimal(self.total_quantity, "total_quantity", positive=True)
        _decimal(self.filled_quantity, "filled_quantity", nonnegative=True)
        _decimal(self.remaining_quantity, "remaining_quantity", nonnegative=True)
        _decimal(self.average_fill_price, "average_fill_price", positive=True, optional=True)
        _decimal(self.working_limit_price, "working_limit_price", positive=True, optional=True)
        if self.intent.order_type is LiveOrderType.MARKET and self.working_limit_price is not None:
            raise ValueError("market order must not have working_limit_price")
        if self.intent.order_type is LiveOrderType.LIMIT and self.working_limit_price is None:
            raise ValueError("limit order requires working_limit_price")
        if self.filled_quantity + self.remaining_quantity != self.total_quantity:
            raise ValueError("filled_quantity + remaining_quantity must equal total_quantity")
        if self.total_quantity > self.intent.quantity:
            raise ValueError("total_quantity must not exceed intent quantity")
        if (self.filled_quantity == 0) != (self.average_fill_price is None):
            raise ValueError(
                "average_fill_price must exist exactly when filled_quantity is positive"
            )
        if type(self.version) is not int:
            raise TypeError("version must be an integer")
        if self.version < 1:
            raise ValueError("version must be at least 1")
        _utc(self.updated_at, "updated_at")
        _utc(self.accepted_at, "accepted_at", optional=True)
        if (
            self.state
            in {
                LiveOrderState.ACCEPTED,
                LiveOrderState.PARTIALLY_FILLED,
                LiveOrderState.FILLED,
                LiveOrderState.CANCEL_PENDING,
                LiveOrderState.CANCELLED,
            }
            and self.accepted_at is None
        ):
            raise ValueError("broker-authoritative state requires accepted_at")
        if self.accepted_at is not None and self.accepted_at > self.updated_at:
            raise ValueError("accepted_at must not be after updated_at")
        if self.pending_command is not None:
            if type(self.pending_command) is not PendingCommandBinding:
                raise TypeError("pending_command must be PendingCommandBinding")
            if self.pending_command.bound_at > self.updated_at:
                raise ValueError("pending command must not be bound after updated_at")
            pending = self.pending_command.command
            if isinstance(pending, NewOrderCommand):
                if pending.intent != self.intent:
                    raise ValueError("NEW pending command intent must equal order intent")
            elif pending.client_order_id != self.intent.client_order_id:
                raise ValueError("pending command must target this order client_order_id")
        binding_required = {
            LiveOrderState.SUBMITTING,
            LiveOrderState.SUBMISSION_UNKNOWN,
            LiveOrderState.RECONCILING,
            LiveOrderState.CANCEL_PENDING,
        }
        if (self.state in binding_required) != (self.pending_command is not None):
            if self.state in binding_required:
                raise ValueError(f"{self.state.value} order requires pending command binding")
            if self.state in {
                LiveOrderState.CREATED,
                LiveOrderState.VALIDATED,
                LiveOrderState.FILLED,
                LiveOrderState.REJECTED,
                LiveOrderState.CANCELLED,
            }:
                raise ValueError(
                    f"{self.state.value} order must not retain pending command binding"
                )
        if (
            self.state is LiveOrderState.CANCEL_PENDING
            and self.pending_command is not None
            and self.pending_command.command_kind is not LiveCommandKind.CANCEL
        ):
            raise ValueError("cancel_pending order requires CANCEL pending command")
        if self.state is LiveOrderState.PARTIALLY_FILLED and not (
            0 < self.filled_quantity < self.total_quantity
        ):
            raise ValueError("partially filled order requires a partial filled_quantity")
        if self.state is LiveOrderState.FILLED and self.remaining_quantity != 0:
            raise ValueError("filled order must have zero remaining_quantity")
        if (
            self.state
            in {
                LiveOrderState.CREATED,
                LiveOrderState.VALIDATED,
                LiveOrderState.SUBMITTING,
                LiveOrderState.ACCEPTED,
                LiveOrderState.REJECTED,
            }
            and self.filled_quantity != 0
        ):
            raise ValueError(f"{self.state.value} order must have zero filled_quantity")
        if self.state in {LiveOrderState.CANCEL_PENDING, LiveOrderState.CANCELLED} and (
            self.remaining_quantity == 0
        ):
            raise ValueError(f"{self.state.value} order must have remaining quantity")


@dataclass(frozen=True, slots=True)
class LiveFill:
    fill_id: str
    client_order_id: str
    strategy_id: str
    account_id: str = field(repr=False)
    instrument_id: str
    side: LiveSide
    quantity: Decimal
    execution_price: Decimal
    occurred_at: datetime

    def __post_init__(self) -> None:
        for name in ("fill_id", "client_order_id", "strategy_id", "account_id", "instrument_id"):
            _identifier(getattr(self, name), name)
        _enum(self.side, LiveSide, "side")
        _decimal(self.quantity, "quantity", positive=True)
        _decimal(self.execution_price, "execution_price", positive=True)
        _utc(self.occurred_at, "occurred_at")


@dataclass(frozen=True, slots=True)
class BrokerPosition:
    account_id: str = field(repr=False)
    instrument_id: str
    net_quantity: Decimal
    average_open_price: Decimal | None
    observed_at: datetime

    def __post_init__(self) -> None:
        _identifier(self.account_id, "account_id")
        _identifier(self.instrument_id, "instrument_id")
        _decimal(self.net_quantity, "net_quantity")
        _decimal(self.average_open_price, "average_open_price", positive=True, optional=True)
        if (self.net_quantity == 0) != (self.average_open_price is None):
            raise ValueError("average_open_price must exist exactly for a non-flat position")
        _utc(self.observed_at, "observed_at")


@dataclass(frozen=True, slots=True)
class StrategyPositionAttribution:
    strategy_id: str
    account_id: str = field(repr=False)
    instrument_id: str
    attributed_quantity: Decimal
    as_of: datetime

    def __post_init__(self) -> None:
        _identifier(self.strategy_id, "strategy_id")
        _identifier(self.account_id, "account_id")
        _identifier(self.instrument_id, "instrument_id")
        _decimal(self.attributed_quantity, "attributed_quantity")
        _utc(self.as_of, "as_of")


@dataclass(frozen=True, slots=True)
class DispatchReceipt:
    client_command_id: str
    payload_fingerprint: str
    state: DispatchState
    attempted_at: datetime
    completed_at: datetime | None
    failure_code: LiveFailureCode | None = None

    def __post_init__(self) -> None:
        _identifier(self.client_command_id, "client_command_id")
        _fingerprint(self.payload_fingerprint, "payload_fingerprint")
        _enum(self.state, DispatchState, "state")
        _utc(self.attempted_at, "attempted_at")
        _utc(self.completed_at, "completed_at", optional=True)
        if self.completed_at is not None and self.completed_at < self.attempted_at:
            raise ValueError("completed_at must not be before attempted_at")
        if self.failure_code is not None:
            _enum(self.failure_code, LiveFailureCode, "failure_code")
        if self.state is DispatchState.SUCCEEDED and self.failure_code is not None:
            raise ValueError("successful dispatch must not have failure_code")
        if self.state is not DispatchState.SUCCEEDED and self.failure_code is None:
            raise ValueError("non-successful dispatch requires failure_code")
        if self.state is not DispatchState.UNKNOWN and self.completed_at is None:
            raise ValueError("known dispatch outcome requires completed_at")

    @property
    def broker_accepted(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class BrokerCorrelation:
    broker_session_generation: int
    adapter_received_sequence: int
    status: CorrelationStatus
    correlated_at: datetime
    submission_attempt_id: str | None = field(default=None, repr=False)
    async_thread_id: str | None = field(default=None, repr=False)
    proxy_stamp_id: str | None = field(default=None, repr=False)
    broker_order_sequence: str | None = field(default=None, repr=False)
    broker_book_no: str | None = field(default=None, repr=False)
    broker_fill_id: str | None = field(default=None, repr=False)
    execution_no: str | None = field(default=None, repr=False)
    client_order_id: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        _positive_int(self.broker_session_generation, "broker_session_generation")
        _positive_int(self.adapter_received_sequence, "adapter_received_sequence")
        _enum(self.status, CorrelationStatus, "status")
        _utc(self.correlated_at, "correlated_at")
        for name in (
            "submission_attempt_id",
            "async_thread_id",
            "proxy_stamp_id",
            "broker_order_sequence",
            "broker_book_no",
            "broker_fill_id",
            "execution_no",
            "client_order_id",
        ):
            _optional_identifier(getattr(self, name), name)
        broker_clues = (
            self.async_thread_id,
            self.proxy_stamp_id,
            self.broker_order_sequence,
            self.broker_book_no,
            self.broker_fill_id,
            self.execution_no,
        )
        if all(clue is None for clue in broker_clues):
            raise ValueError("correlation requires at least one broker correlation clue")
        if self.status is CorrelationStatus.CONFIRMED and self.client_order_id is None:
            raise ValueError("confirmed correlation requires client_order_id")


@dataclass(frozen=True, slots=True)
class NormalizedBrokerOrderEvent:
    event_id: str = field(repr=False)
    account_id: str = field(repr=False)
    instrument_id: str
    event_type: BrokerOrderEventType
    received_at: datetime
    broker_session_generation: int
    adapter_received_sequence: int
    correlation: BrokerCorrelation
    occurred_at: datetime | None = None
    failure_code: LiveFailureCode | None = None
    decreased_quantity: Decimal | None = None
    new_limit_price: Decimal | None = None

    def __post_init__(self) -> None:
        _identifier(self.event_id, "event_id")
        _identifier(self.account_id, "account_id")
        _identifier(self.instrument_id, "instrument_id")
        _enum(self.event_type, BrokerOrderEventType, "event_type")
        _utc(self.received_at, "received_at")
        _positive_int(self.broker_session_generation, "broker_session_generation")
        _positive_int(self.adapter_received_sequence, "adapter_received_sequence")
        if type(self.correlation) is not BrokerCorrelation:
            raise TypeError("correlation must be BrokerCorrelation")
        if self.correlation.broker_session_generation != self.broker_session_generation:
            raise ValueError("correlation broker_session_generation must match event")
        if self.correlation.adapter_received_sequence != self.adapter_received_sequence:
            raise ValueError("correlation adapter_received_sequence must match event")
        _utc(self.occurred_at, "occurred_at", optional=True)
        if self.occurred_at is not None and self.occurred_at > self.received_at:
            raise ValueError("occurred_at must not be after received_at")
        _decimal(
            self.decreased_quantity,
            "decreased_quantity",
            positive=True,
            optional=True,
        )
        _decimal(self.new_limit_price, "new_limit_price", positive=True, optional=True)
        quantity_change_events = {
            BrokerOrderEventType.QUANTITY_DECREASED,
            BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED,
        }
        price_change_events = {
            BrokerOrderEventType.PRICE_AMENDED,
            BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED,
        }
        if (self.event_type in quantity_change_events) != (self.decreased_quantity is not None):
            raise ValueError("decreased_quantity is required exactly for quantity-change events")
        if (self.event_type in price_change_events) != (self.new_limit_price is not None):
            raise ValueError("new_limit_price is required exactly for price-change events")
        if self.failure_code is not None:
            _enum(self.failure_code, LiveFailureCode, "failure_code")
        expected_failures = {
            BrokerOrderEventType.NEW_REJECTED: LiveFailureCode.BROKER_REJECTED,
            BrokerOrderEventType.CANCEL_REJECTED: LiveFailureCode.CANCEL_REJECTED,
            BrokerOrderEventType.AMEND_REJECTED: LiveFailureCode.AMEND_REJECTED,
            BrokerOrderEventType.OUTCOME_UNKNOWN: LiveFailureCode.BROKER_TIMEOUT,
        }
        if self.failure_code is not expected_failures.get(self.event_type):
            raise ValueError("failure_code must match broker order event semantics")

    @property
    def is_authoritative_acceptance(self) -> bool:
        return self.event_type is BrokerOrderEventType.NEW_ACCEPTED

    @property
    def is_terminal(self) -> bool:
        return self.event_type in {
            BrokerOrderEventType.NEW_REJECTED,
            BrokerOrderEventType.CANCELLED,
            BrokerOrderEventType.DYNAMIC_CANCELLED,
        }


@dataclass(frozen=True, slots=True)
class NormalizedBrokerFillEvent:
    event_id: str = field(repr=False)
    account_id: str = field(repr=False)
    instrument_id: str
    side: LiveSide
    quantity: Decimal
    execution_price: Decimal
    received_at: datetime
    broker_session_generation: int
    adapter_received_sequence: int
    correlation: BrokerCorrelation
    occurred_at: datetime | None = None

    def __post_init__(self) -> None:
        _identifier(self.event_id, "event_id")
        _identifier(self.account_id, "account_id")
        _identifier(self.instrument_id, "instrument_id")
        _enum(self.side, LiveSide, "side")
        _decimal(self.quantity, "quantity", positive=True)
        _decimal(self.execution_price, "execution_price", positive=True)
        _utc(self.received_at, "received_at")
        _positive_int(self.broker_session_generation, "broker_session_generation")
        _positive_int(self.adapter_received_sequence, "adapter_received_sequence")
        if type(self.correlation) is not BrokerCorrelation:
            raise TypeError("correlation must be BrokerCorrelation")
        if self.correlation.broker_session_generation != self.broker_session_generation:
            raise ValueError("correlation broker_session_generation must match event")
        if self.correlation.adapter_received_sequence != self.adapter_received_sequence:
            raise ValueError("correlation adapter_received_sequence must match event")
        if self.correlation.broker_fill_id is None and self.correlation.execution_no is None:
            raise ValueError("fill evidence requires broker_fill_id or execution_no")
        _utc(self.occurred_at, "occurred_at", optional=True)
        if self.occurred_at is not None and self.occurred_at > self.received_at:
            raise ValueError("occurred_at must not be after received_at")


@dataclass(frozen=True, slots=True)
class BrokerOpenOrderObservation:
    observation_id: str = field(repr=False)
    account_id: str = field(repr=False)
    instrument_id: str
    side: LiveSide
    working_total_quantity: Decimal
    working_remaining_quantity: Decimal
    working_limit_price: Decimal | None
    correlation: BrokerCorrelation
    observed_at: datetime

    def __post_init__(self) -> None:
        _identifier(self.observation_id, "observation_id")
        _identifier(self.account_id, "account_id")
        _identifier(self.instrument_id, "instrument_id")
        _enum(self.side, LiveSide, "side")
        _decimal(self.working_total_quantity, "working_total_quantity", positive=True)
        _decimal(
            self.working_remaining_quantity,
            "working_remaining_quantity",
            positive=True,
        )
        if self.working_remaining_quantity > self.working_total_quantity:
            raise ValueError("working_remaining_quantity must not exceed total")
        _decimal(
            self.working_limit_price,
            "working_limit_price",
            positive=True,
            optional=True,
        )
        if type(self.correlation) is not BrokerCorrelation:
            raise TypeError("correlation must be BrokerCorrelation")
        _utc(self.observed_at, "observed_at")


@dataclass(frozen=True, slots=True)
class BrokerFillObservation:
    observation_id: str = field(repr=False)
    account_id: str = field(repr=False)
    instrument_id: str
    side: LiveSide
    quantity: Decimal
    execution_price: Decimal
    correlation: BrokerCorrelation
    observed_at: datetime
    occurred_at: datetime | None = None

    def __post_init__(self) -> None:
        _identifier(self.observation_id, "observation_id")
        _identifier(self.account_id, "account_id")
        _identifier(self.instrument_id, "instrument_id")
        _enum(self.side, LiveSide, "side")
        _decimal(self.quantity, "quantity", positive=True)
        _decimal(self.execution_price, "execution_price", positive=True)
        if type(self.correlation) is not BrokerCorrelation:
            raise TypeError("correlation must be BrokerCorrelation")
        if self.correlation.broker_fill_id is None and self.correlation.execution_no is None:
            raise ValueError("fill observation requires broker_fill_id or execution_no")
        _utc(self.observed_at, "observed_at")
        _utc(self.occurred_at, "occurred_at", optional=True)
        if self.occurred_at is not None and self.occurred_at > self.observed_at:
            raise ValueError("occurred_at must not be after observed_at")


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    account_id: str = field(repr=False)
    currency: str
    available_funds: Decimal
    observed_at: datetime

    def __post_init__(self) -> None:
        _identifier(self.account_id, "account_id")
        if type(self.currency) is not str:
            raise TypeError("currency must be a string")
        if not re.fullmatch(r"[A-Z]{3}", self.currency):
            raise ValueError("currency must be an uppercase 3-letter code")
        _decimal(self.available_funds, "available_funds", nonnegative=True)
        _utc(self.observed_at, "observed_at")


@dataclass(frozen=True, slots=True)
class ReadinessSnapshot:
    account_id: str = field(repr=False)
    readiness: AccountReadiness
    trading_session_id: str
    checked_at: datetime
    failure_code: LiveFailureCode | None = None

    def __post_init__(self) -> None:
        _identifier(self.account_id, "account_id")
        _enum(self.readiness, AccountReadiness, "readiness")
        _identifier(self.trading_session_id, "trading_session_id")
        _utc(self.checked_at, "checked_at")
        if self.failure_code is not None:
            _enum(self.failure_code, LiveFailureCode, "failure_code")
        if (self.readiness is AccountReadiness.READY) == (self.failure_code is not None):
            raise ValueError("failure_code is required exactly when account is not ready")


@dataclass(frozen=True, slots=True)
class ReconciliationDiscrepancy:
    discrepancy_id: str
    kind: ReconciliationKind
    account_id: str = field(repr=False)
    instrument_id: str
    observed_at: datetime
    client_order_id: str | None = None
    expected_quantity: Decimal | None = None
    actual_quantity: Decimal | None = None

    def __post_init__(self) -> None:
        _identifier(self.discrepancy_id, "discrepancy_id")
        _enum(self.kind, ReconciliationKind, "kind")
        _identifier(self.account_id, "account_id")
        _identifier(self.instrument_id, "instrument_id")
        _utc(self.observed_at, "observed_at")
        _optional_identifier(self.client_order_id, "client_order_id")
        _decimal(self.expected_quantity, "expected_quantity", optional=True)
        _decimal(self.actual_quantity, "actual_quantity", optional=True)
        if (self.expected_quantity is None) != (self.actual_quantity is None):
            raise ValueError("expected_quantity and actual_quantity must be provided together")


@dataclass(frozen=True, slots=True)
class CommandDeduplicationResult:
    client_command_id: str
    incoming_fingerprint: str
    recorded_fingerprint: str | None
    disposition: CommandDeduplication

    def __post_init__(self) -> None:
        _identifier(self.client_command_id, "client_command_id")
        _fingerprint(self.incoming_fingerprint, "incoming_fingerprint")
        if self.recorded_fingerprint is not None:
            _fingerprint(self.recorded_fingerprint, "recorded_fingerprint")
        _enum(self.disposition, CommandDeduplication, "disposition")
        if self.disposition is CommandDeduplication.FIRST_SEEN:
            if self.recorded_fingerprint is not None:
                raise ValueError("FIRST_SEEN must not have recorded_fingerprint")
        else:
            if self.recorded_fingerprint is None:
                raise ValueError("retry dispositions require recorded_fingerprint")
            equal = self.incoming_fingerprint == self.recorded_fingerprint
            if equal != (self.disposition is CommandDeduplication.EXACT_RETRY):
                raise ValueError("disposition must match fingerprint equality")


@dataclass(frozen=True, slots=True)
class LiveFailure:
    code: LiveFailureCode
    occurred_at: datetime
    client_command_id: str | None = None

    def __post_init__(self) -> None:
        _enum(self.code, LiveFailureCode, "code")
        _utc(self.occurred_at, "occurred_at")
        _optional_identifier(self.client_command_id, "client_command_id")

    @property
    def message(self) -> str:
        return self.code.public_message


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("canonical Decimal values must be finite")
    if value == 0:
        return "0"
    normalized = value.normalize()
    sign, digits, exponent = normalized.as_tuple()
    assert isinstance(exponent, int)
    coefficient = "".join(str(digit) for digit in digits)
    adjusted = exponent + len(coefficient) - 1
    scientific_coefficient = (
        coefficient if len(coefficient) == 1 else f"{coefficient[0]}.{coefficient[1:]}"
    )
    scientific = f"{scientific_coefficient}e{adjusted}" if adjusted else scientific_coefficient
    point = len(coefficient) + exponent
    if point <= 0:
        plain = f"0.{('0' * -point)}{coefficient}"
    elif point >= len(coefficient):
        plain = f"{coefficient}{('0' * exponent)}"
    else:
        plain = f"{coefficient[:point]}.{coefficient[point:]}"
    result = min(plain, scientific, key=len)
    return f"-{result}" if sign else result


def to_canonical_primitive(value: Any) -> Any:
    """Convert live contract values to deterministic JSON primitives."""

    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: to_canonical_primitive(getattr(value, item.name)) for item in fields(value)
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        _utc(value, "datetime")
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return _canonical_decimal(value)
    if isinstance(value, tuple):
        return [to_canonical_primitive(item) for item in value]
    if value is None or type(value) in {str, int, bool}:
        return value
    if isinstance(value, float):
        raise TypeError("binary float is not supported")
    raise TypeError(f"unsupported serialization value: {type(value).__name__}")


def canonical_bytes(value: Any) -> bytes:
    """Serialize a live contract as stable UTF-8 canonical JSON bytes."""

    return json.dumps(
        to_canonical_primitive(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _fingerprint(value: Any, name: str) -> None:
    if type(value) is not str or not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
        raise ValueError(f"{name} must be a sha256 fingerprint")


def payload_fingerprint(value: LiveCommand, domain: FingerprintDomain) -> str:
    """Return a domain-separated SHA-256 fingerprint for a command payload."""

    _enum(domain, FingerprintDomain, "domain")
    expected = {
        NewOrderCommand: FingerprintDomain.NEW_COMMAND_V1,
        CancelOrderCommand: FingerprintDomain.CANCEL_COMMAND_V1,
        AmendOrderCommand: FingerprintDomain.AMEND_COMMAND_V1,
        DecreaseOrderCommand: FingerprintDomain.DECREASE_COMMAND_V1,
    }
    if type(value) not in expected:
        raise TypeError("value must be a live command")
    if domain is not expected[type(value)]:
        raise ValueError("fingerprint domain does not match command type")
    digest = sha256(domain.value.encode("ascii") + b"\x00" + canonical_bytes(value)).hexdigest()
    return f"sha256:{digest}"


def broker_semantic_fingerprint(
    event: NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent,
) -> str:
    """Fingerprint authoritative broker semantics, excluding delivery metadata.

    Adapter generation, receive sequence/time, broker occurrence time, and all
    local correlation decisions are intentionally excluded.  Thus a redelivery
    in a later adapter session has the same fingerprint while the same event ID
    with changed authoritative content produces a conflict.
    """

    if type(event) is NormalizedBrokerOrderEvent:
        domain = FingerprintDomain.BROKER_ORDER_EVENT_V1
        payload = {
            "account_id": event.account_id,
            "event_id": event.event_id,
            "event_type": event.event_type.value,
            "failure_code": (event.failure_code.value if event.failure_code is not None else None),
            "instrument_id": event.instrument_id,
            "new_limit_price": (
                _canonical_decimal(event.new_limit_price)
                if event.new_limit_price is not None
                else None
            ),
            "decreased_quantity": (
                _canonical_decimal(event.decreased_quantity)
                if event.decreased_quantity is not None
                else None
            ),
        }
    elif type(event) is NormalizedBrokerFillEvent:
        domain = FingerprintDomain.BROKER_FILL_EVENT_V1
        payload = {
            "account_id": event.account_id,
            "event_id": event.event_id,
            "execution_price": _canonical_decimal(event.execution_price),
            "instrument_id": event.instrument_id,
            "quantity": _canonical_decimal(event.quantity),
            "side": event.side.value,
        }
    else:
        raise TypeError("event must be a normalized broker order or fill event")
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    digest = sha256(domain.value.encode("ascii") + b"\x00" + encoded).hexdigest()
    return f"sha256:{digest}"


__all__ = [
    "AccountReadiness",
    "AccountSnapshot",
    "AmendOrderCommand",
    "BrokerCorrelation",
    "BrokerFillObservation",
    "BrokerOpenOrderObservation",
    "BrokerOrderEventType",
    "BrokerPosition",
    "CLIENT_ORDER_ID_UNIQUENESS_CONTRACT",
    "CancelOrderCommand",
    "CommandDeduplication",
    "CommandDeduplicationResult",
    "CorrelationStatus",
    "DecreaseOrderCommand",
    "DispatchReceipt",
    "DispatchState",
    "FingerprintDomain",
    "LiveCommand",
    "LiveCommandKind",
    "LiveFailure",
    "LiveFailureCode",
    "LiveFill",
    "LiveOrder",
    "LiveOrderIntent",
    "LiveOrderState",
    "LiveOrderType",
    "LiveSide",
    "LiveTimeInForce",
    "MAX_IDENTIFIER_LENGTH",
    "NewOrderCommand",
    "NormalizedBrokerFillEvent",
    "NormalizedBrokerOrderEvent",
    "PendingCommandBinding",
    "ReadinessSnapshot",
    "ReconciliationDiscrepancy",
    "ReconciliationKind",
    "StrategyPositionAttribution",
    "canonical_bytes",
    "broker_semantic_fingerprint",
    "payload_fingerprint",
    "to_canonical_primitive",
]
