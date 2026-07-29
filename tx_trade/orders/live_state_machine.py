"""Pure, broker-neutral reducer for live-order state."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256

from .live_contracts import (
    AmendOrderCommand,
    BrokerOrderEventType,
    CancelOrderCommand,
    CorrelationStatus,
    DecreaseOrderCommand,
    DispatchReceipt,
    DispatchState,
    LiveCommandKind,
    LiveFailureCode,
    LiveFill,
    LiveOrder,
    LiveOrderIntent,
    LiveOrderState,
    NormalizedBrokerFillEvent,
    NormalizedBrokerOrderEvent,
    NewOrderCommand,
    PendingCommandBinding,
    broker_semantic_fingerprint,
    canonical_bytes,
)


class InvalidLiveOrderTransition(ValueError):
    """Raised for an invalid caller-controlled local transition."""


class ReductionDisposition(StrEnum):
    APPLIED = "applied"
    NO_CHANGE = "no_change"
    EXACT_DUPLICATE = "exact_duplicate"
    EVENT_CONFLICT = "event_conflict"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    STALE = "stale"
    TERMINAL_IMMUTABLE = "terminal_immutable"


@dataclass(frozen=True, slots=True)
class AppliedEvent:
    source: str
    event_id: str
    fingerprint: str


@dataclass(frozen=True, slots=True)
class AppliedEventLedger:
    events: tuple[AppliedEvent, ...] = ()

    def find(self, source: str, event_id: str) -> AppliedEvent | None:
        return next(
            (
                event
                for event in self.events
                if event.source == source and event.event_id == event_id
            ),
            None,
        )

    def append(self, event: AppliedEvent) -> AppliedEventLedger:
        return AppliedEventLedger((*self.events, event))


EMPTY_EVENT_LEDGER = AppliedEventLedger()


@dataclass(frozen=True, slots=True)
class ReductionResult:
    order: LiveOrder
    ledger: AppliedEventLedger
    disposition: ReductionDisposition
    fill: LiveFill | None = None
    failure_code: LiveFailureCode | None = None

    @property
    def reconciliation_required(self) -> bool:
        return self.disposition in {
            ReductionDisposition.EVENT_CONFLICT,
            ReductionDisposition.RECONCILIATION_REQUIRED,
        }


def create_live_order(intent: LiveOrderIntent) -> LiveOrder:
    if type(intent) is not LiveOrderIntent:
        raise TypeError("intent must be LiveOrderIntent")
    return LiveOrder(
        intent=intent,
        state=LiveOrderState.CREATED,
        total_quantity=intent.quantity,
        filled_quantity=Decimal(0),
        remaining_quantity=intent.quantity,
        average_fill_price=None,
        working_limit_price=intent.limit_price,
        version=1,
        updated_at=intent.created_at,
    )


def advance_local(
    order: LiveOrder,
    target: LiveOrderState,
    occurred_at: datetime,
    pending_command: PendingCommandBinding | None = None,
) -> LiveOrder:
    """Advance local lifecycle, binding the exact NEW command at SUBMITTING."""

    _validate_order_and_time(order, occurred_at)
    if type(target) is not LiveOrderState:
        raise TypeError("target must be LiveOrderState")
    if pending_command is not None and type(pending_command) is not PendingCommandBinding:
        raise TypeError("pending_command must be PendingCommandBinding")
    if order.state is LiveOrderState.CREATED and target is LiveOrderState.VALIDATED:
        if pending_command is not None:
            raise InvalidLiveOrderTransition("validation must not bind a command")
        return _snapshot(order, state=target, updated_at=occurred_at)
    if order.state is LiveOrderState.VALIDATED and target is LiveOrderState.SUBMITTING:
        _require_binding_for_order(
            order,
            pending_command,
            LiveCommandKind.NEW,
            occurred_at,
        )
        return _snapshot(
            order,
            state=target,
            updated_at=occurred_at,
            pending_command=pending_command,
        )
    if (
        order.state is LiveOrderState.SUBMISSION_UNKNOWN
        and target is LiveOrderState.RECONCILING
        and pending_command is None
    ):
        return _snapshot(
            order,
            state=target,
            updated_at=occurred_at,
            pending_command=order.pending_command,
        )
    raise InvalidLiveOrderTransition(
        f"transition from {order.state.value} to {target.value} is not allowed"
    )


def request_cancel(
    order: LiveOrder,
    pending_command: PendingCommandBinding,
    requested_at: datetime,
) -> LiveOrder:
    _validate_order_and_time(order, requested_at)
    _require_binding_for_order(
        order,
        pending_command,
        LiveCommandKind.CANCEL,
        requested_at,
    )
    if order.state not in {LiveOrderState.ACCEPTED, LiveOrderState.PARTIALLY_FILLED}:
        raise InvalidLiveOrderTransition(f"cancel request is not allowed from {order.state.value}")
    if order.pending_command is not None:
        raise InvalidLiveOrderTransition("order already has a pending command")
    return _snapshot(
        order,
        state=LiveOrderState.CANCEL_PENDING,
        updated_at=requested_at,
        pending_command=pending_command,
    )


def bind_operation(
    order: LiveOrder,
    pending_command: PendingCommandBinding,
    requested_at: datetime,
) -> LiveOrder:
    """Bind an AMEND or DECREASE operation without claiming broker success."""

    _validate_order_and_time(order, requested_at)
    if type(pending_command) is not PendingCommandBinding:
        raise TypeError("pending_command must be PendingCommandBinding")
    if pending_command.command_kind not in {
        LiveCommandKind.AMEND,
        LiveCommandKind.DECREASE,
    }:
        raise InvalidLiveOrderTransition("operation binding must be AMEND or DECREASE")
    _require_binding_for_order(
        order,
        pending_command,
        pending_command.command_kind,
        requested_at,
    )
    if order.state not in {LiveOrderState.ACCEPTED, LiveOrderState.PARTIALLY_FILLED}:
        raise InvalidLiveOrderTransition(f"operation is not allowed from {order.state.value}")
    if order.pending_command is not None:
        raise InvalidLiveOrderTransition("order already has a pending command")
    if (
        type(pending_command.command) is DecreaseOrderCommand
        and pending_command.command.expected_total_quantity != order.total_quantity
    ):
        raise InvalidLiveOrderTransition(
            "decrease expected_total_quantity does not match current order total"
        )
    return _snapshot(
        order,
        state=order.state,
        updated_at=requested_at,
        pending_command=pending_command,
    )


def reduce_dispatch(
    order: LiveOrder,
    receipt: DispatchReceipt,
    ledger: AppliedEventLedger = EMPTY_EVENT_LEDGER,
) -> ReductionResult:
    _validate_inputs(order, ledger)
    if type(receipt) is not DispatchReceipt:
        raise TypeError("receipt must be DispatchReceipt")
    binding = order.pending_command
    if (
        binding is None
        or receipt.client_command_id != binding.client_command_id
        or receipt.payload_fingerprint != binding.payload_fingerprint
    ):
        return ReductionResult(
            order,
            ledger,
            ReductionDisposition.EVENT_CONFLICT,
            failure_code=LiveFailureCode.IDEMPOTENCY_CONFLICT,
        )
    duplicate = _deduplicate(ledger, "dispatch", receipt.client_command_id, receipt)
    if duplicate is not None:
        return ReductionResult(order, ledger, duplicate)
    next_ledger = _record(ledger, "dispatch", receipt.client_command_id, receipt)
    evidence_at = receipt.completed_at or receipt.attempted_at
    if receipt.state is DispatchState.SUCCEEDED:
        return ReductionResult(order, next_ledger, ReductionDisposition.NO_CHANGE)
    target = (
        LiveOrderState.SUBMISSION_UNKNOWN
        if binding.command_kind is LiveCommandKind.NEW and receipt.state is DispatchState.UNKNOWN
        else LiveOrderState.RECONCILING
    )
    return ReductionResult(
        _snapshot(
            order,
            state=target,
            updated_at=max(order.updated_at, evidence_at),
            pending_command=binding,
        ),
        next_ledger,
        (
            ReductionDisposition.APPLIED
            if receipt.state is DispatchState.UNKNOWN
            else ReductionDisposition.RECONCILIATION_REQUIRED
        ),
        failure_code=receipt.failure_code,
    )


def reduce_broker_order_event(
    order: LiveOrder,
    event: NormalizedBrokerOrderEvent,
    ledger: AppliedEventLedger = EMPTY_EVENT_LEDGER,
) -> ReductionResult:
    _validate_inputs(order, ledger)
    if type(event) is not NormalizedBrokerOrderEvent:
        raise TypeError("event must be NormalizedBrokerOrderEvent")
    duplicate = _deduplicate(ledger, "broker-event", event.event_id, event)
    if duplicate is not None:
        return _broker_duplicate_result(order, ledger, duplicate)
    if not _is_confirmed_for_order(order, event):
        return _reconcile(order, ledger)
    if order.state is LiveOrderState.CANCELLED and event.event_type in {
        BrokerOrderEventType.CANCELLED,
        BrokerOrderEventType.DYNAMIC_CANCELLED,
    }:
        return ReductionResult(
            order,
            _record(ledger, "broker-event", event.event_id, event),
            ReductionDisposition.NO_CHANGE,
        )
    transition = _broker_transition(order, event)
    if transition is None:
        return _reconcile(order, ledger)
    next_ledger = _record(ledger, "broker-event", event.event_id, event)
    state, total, limit_price, pending = transition
    accepted_at = order.accepted_at
    if state not in {
        LiveOrderState.CREATED,
        LiveOrderState.VALIDATED,
        LiveOrderState.SUBMITTING,
        LiveOrderState.SUBMISSION_UNKNOWN,
        LiveOrderState.RECONCILING,
        LiveOrderState.REJECTED,
    }:
        accepted_at = accepted_at or event.occurred_at or event.received_at
    next_order = _snapshot(
        order,
        state=state,
        total_quantity=total,
        remaining_quantity=total - order.filled_quantity,
        working_limit_price=limit_price,
        updated_at=max(order.updated_at, event.received_at),
        accepted_at=accepted_at,
        pending_command=pending,
    )
    return ReductionResult(
        next_order,
        next_ledger,
        ReductionDisposition.APPLIED,
        failure_code=event.failure_code,
    )


def reduce_broker_fill_event(
    order: LiveOrder,
    event: NormalizedBrokerFillEvent,
    ledger: AppliedEventLedger = EMPTY_EVENT_LEDGER,
) -> ReductionResult:
    _validate_inputs(order, ledger)
    if type(event) is not NormalizedBrokerFillEvent:
        raise TypeError("event must be NormalizedBrokerFillEvent")
    duplicate = _deduplicate(ledger, "broker-event", event.event_id, event)
    if duplicate is not None:
        return _broker_duplicate_result(order, ledger, duplicate)
    if not _is_confirmed_for_order(order, event) or event.side is not order.intent.side:
        return _reconcile(order, ledger)
    if order.state in {LiveOrderState.REJECTED, LiveOrderState.FILLED}:
        return _reconcile(order, ledger)
    if event.quantity > order.remaining_quantity:
        return _reconcile(order, ledger)

    next_ledger = _record(ledger, "broker-event", event.event_id, event)
    new_filled = order.filled_quantity + event.quantity
    new_remaining = order.total_quantity - new_filled
    prior_notional = (
        order.filled_quantity * order.average_fill_price
        if order.average_fill_price is not None
        else Decimal(0)
    )
    average = (prior_notional + event.quantity * event.execution_price) / new_filled
    if new_remaining == 0:
        state = LiveOrderState.FILLED
        pending = None
    elif order.state is LiveOrderState.CANCELLED:
        state = LiveOrderState.CANCELLED
        pending = None
    elif order.state is LiveOrderState.RECONCILING:
        state = LiveOrderState.RECONCILING
        pending = order.pending_command
    elif order.state is LiveOrderState.CANCEL_PENDING:
        state = LiveOrderState.CANCEL_PENDING
        pending = order.pending_command
    else:
        state = LiveOrderState.PARTIALLY_FILLED
        pending = order.pending_command
        if pending is not None and pending.command_kind is LiveCommandKind.NEW:
            pending = None
    accepted_at = order.accepted_at or event.occurred_at or event.received_at
    next_order = _snapshot(
        order,
        state=state,
        filled_quantity=new_filled,
        remaining_quantity=new_remaining,
        average_fill_price=average,
        updated_at=max(order.updated_at, event.received_at),
        accepted_at=accepted_at,
        pending_command=pending,
    )
    fill_id = event.correlation.broker_fill_id or event.correlation.execution_no
    assert fill_id is not None
    fill = LiveFill(
        fill_id=fill_id,
        client_order_id=order.intent.client_order_id,
        strategy_id=order.intent.strategy_id,
        account_id=order.intent.account_id,
        instrument_id=order.intent.instrument_id,
        side=order.intent.side,
        quantity=event.quantity,
        execution_price=event.execution_price,
        occurred_at=event.occurred_at or event.received_at,
    )
    return ReductionResult(next_order, next_ledger, ReductionDisposition.APPLIED, fill)


def _broker_transition(
    order: LiveOrder,
    event: NormalizedBrokerOrderEvent,
) -> (
    tuple[
        LiveOrderState,
        Decimal,
        Decimal | None,
        PendingCommandBinding | None,
    ]
    | None
):
    kind = event.event_type
    open_state = (
        LiveOrderState.PARTIALLY_FILLED if order.filled_quantity > 0 else LiveOrderState.ACCEPTED
    )
    if order.state.is_terminal:
        return None
    if kind is BrokerOrderEventType.NEW_ACCEPTED:
        if order.accepted_at is not None:
            return (
                order.state,
                order.total_quantity,
                order.working_limit_price,
                order.pending_command,
            )
        return open_state, order.total_quantity, order.working_limit_price, None
    if kind is BrokerOrderEventType.NEW_REJECTED:
        if order.filled_quantity > 0 or order.accepted_at is not None:
            return None
        return LiveOrderState.REJECTED, order.total_quantity, order.working_limit_price, None
    if kind is BrokerOrderEventType.CANCEL_PENDING:
        if (
            order.pending_command is None
            or order.pending_command.command_kind is not LiveCommandKind.CANCEL
            or order.state is not LiveOrderState.CANCEL_PENDING
        ):
            return None
        return order.state, order.total_quantity, order.working_limit_price, order.pending_command
    if kind in {
        BrokerOrderEventType.CANCELLED,
        BrokerOrderEventType.DYNAMIC_CANCELLED,
    }:
        if order.remaining_quantity == 0 or order.state in {
            LiveOrderState.REJECTED,
            LiveOrderState.FILLED,
        }:
            return None
        if kind is BrokerOrderEventType.DYNAMIC_CANCELLED and order.state not in {
            LiveOrderState.ACCEPTED,
            LiveOrderState.PARTIALLY_FILLED,
            LiveOrderState.CANCEL_PENDING,
        }:
            return None
        return LiveOrderState.CANCELLED, order.total_quantity, order.working_limit_price, None
    if kind is BrokerOrderEventType.CANCEL_REJECTED:
        if (
            order.pending_command is None
            or order.pending_command.command_kind is not LiveCommandKind.CANCEL
        ):
            return None
        return open_state, order.total_quantity, order.working_limit_price, None
    if kind is BrokerOrderEventType.AMEND_REJECTED:
        if order.pending_command is None or order.pending_command.command_kind not in {
            LiveCommandKind.AMEND,
            LiveCommandKind.DECREASE,
        }:
            return None
        return open_state, order.total_quantity, order.working_limit_price, None
    if kind is BrokerOrderEventType.OUTCOME_UNKNOWN:
        binding = order.pending_command
        if binding is None:
            return None
        state = (
            LiveOrderState.SUBMISSION_UNKNOWN
            if binding.command_kind is LiveCommandKind.NEW
            else LiveOrderState.RECONCILING
        )
        return state, order.total_quantity, order.working_limit_price, binding
    if kind in {
        BrokerOrderEventType.PRICE_AMENDED,
        BrokerOrderEventType.QUANTITY_DECREASED,
        BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED,
    }:
        binding = order.pending_command
        if binding is None or not binding.matches_authoritative_working_change(
            event,
            current_total_quantity=order.total_quantity,
        ):
            return None
        if event.new_limit_price is not None and order.working_limit_price is None:
            return None
        total = (
            order.total_quantity - event.decreased_quantity
            if event.decreased_quantity is not None
            else order.total_quantity
        )
        if total < order.filled_quantity or total > order.total_quantity:
            return None
        limit_price = event.new_limit_price or order.working_limit_price
        resulting_state = LiveOrderState.FILLED if total == order.filled_quantity else open_state
        return resulting_state, total, limit_price, None
    return None


def _snapshot(
    order: LiveOrder,
    *,
    state: LiveOrderState,
    updated_at: datetime,
    total_quantity: Decimal | None = None,
    filled_quantity: Decimal | None = None,
    remaining_quantity: Decimal | None = None,
    average_fill_price: Decimal | None = None,
    working_limit_price: Decimal | None = None,
    accepted_at: datetime | None = None,
    pending_command: PendingCommandBinding | None = None,
) -> LiveOrder:
    if updated_at < order.updated_at:
        raise InvalidLiveOrderTransition("updated_at must not move backwards")
    return replace(
        order,
        state=state,
        total_quantity=order.total_quantity if total_quantity is None else total_quantity,
        filled_quantity=order.filled_quantity if filled_quantity is None else filled_quantity,
        remaining_quantity=(
            order.remaining_quantity if remaining_quantity is None else remaining_quantity
        ),
        average_fill_price=(
            order.average_fill_price if average_fill_price is None else average_fill_price
        ),
        working_limit_price=(
            order.working_limit_price if working_limit_price is None else working_limit_price
        ),
        version=order.version + 1,
        updated_at=updated_at,
        accepted_at=order.accepted_at if accepted_at is None else accepted_at,
        pending_command=pending_command,
    )


def _require_binding_for_order(
    order: LiveOrder,
    binding: PendingCommandBinding | None,
    kind: LiveCommandKind,
    occurred_at: datetime,
) -> None:
    if type(binding) is not PendingCommandBinding:
        raise InvalidLiveOrderTransition("exact pending command binding is required")
    if binding.command_kind is not kind:
        raise InvalidLiveOrderTransition(f"pending command must be {kind.value}")
    if binding.bound_at > occurred_at:
        raise InvalidLiveOrderTransition("pending command cannot be bound in the future")
    command = binding.command
    if type(command) is NewOrderCommand:
        if command.intent != order.intent:
            raise InvalidLiveOrderTransition("NEW command intent does not match order")
    elif isinstance(
        command,
        (CancelOrderCommand, AmendOrderCommand, DecreaseOrderCommand),
    ):
        if command.client_order_id != order.intent.client_order_id:
            raise InvalidLiveOrderTransition("command client_order_id does not match order")


def _validate_order_and_time(order: LiveOrder, occurred_at: datetime) -> None:
    if type(order) is not LiveOrder:
        raise TypeError("order must be LiveOrder")
    if type(occurred_at) is not datetime:
        raise TypeError("occurred_at must be a datetime")
    if occurred_at.tzinfo is None or occurred_at.utcoffset() != timedelta(0):
        raise ValueError("occurred_at must use UTC")
    if occurred_at < order.updated_at:
        raise InvalidLiveOrderTransition("updated_at must not move backwards")


def _validate_inputs(order: LiveOrder, ledger: AppliedEventLedger) -> None:
    if type(order) is not LiveOrder:
        raise TypeError("order must be LiveOrder")
    if type(ledger) is not AppliedEventLedger:
        raise TypeError("ledger must be AppliedEventLedger")


def _dispatch_fingerprint(value: DispatchReceipt) -> str:
    return f"sha256:{sha256(canonical_bytes(value)).hexdigest()}"


def _event_fingerprint(value: object) -> str:
    if type(value) in {NormalizedBrokerOrderEvent, NormalizedBrokerFillEvent}:
        return broker_semantic_fingerprint(value)  # type: ignore[arg-type]
    if type(value) is DispatchReceipt:
        return _dispatch_fingerprint(value)
    raise TypeError("unsupported event type")


def _deduplicate(
    ledger: AppliedEventLedger, source: str, event_id: str, value: object
) -> ReductionDisposition | None:
    recorded = ledger.find(source, event_id)
    if recorded is None:
        return None
    return (
        ReductionDisposition.EXACT_DUPLICATE
        if recorded.fingerprint == _event_fingerprint(value)
        else ReductionDisposition.EVENT_CONFLICT
    )


def _record(
    ledger: AppliedEventLedger, source: str, event_id: str, value: object
) -> AppliedEventLedger:
    return ledger.append(AppliedEvent(source, event_id, _event_fingerprint(value)))


def _is_confirmed_for_order(
    order: LiveOrder,
    event: NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent,
) -> bool:
    return (
        event.correlation.status is CorrelationStatus.CONFIRMED
        and event.correlation.client_order_id == order.intent.client_order_id
        and event.account_id == order.intent.account_id
        and event.instrument_id == order.intent.instrument_id
    )


def _reconcile(order: LiveOrder, ledger: AppliedEventLedger) -> ReductionResult:
    return ReductionResult(
        order,
        ledger,
        ReductionDisposition.RECONCILIATION_REQUIRED,
        failure_code=LiveFailureCode.RECONCILIATION_REQUIRED,
    )


def _broker_duplicate_result(
    order: LiveOrder,
    ledger: AppliedEventLedger,
    disposition: ReductionDisposition,
) -> ReductionResult:
    return ReductionResult(
        order,
        ledger,
        disposition,
        failure_code=(
            LiveFailureCode.CORRELATION_CONFLICT
            if disposition is ReductionDisposition.EVENT_CONFLICT
            else None
        ),
    )


__all__ = [
    "AppliedEvent",
    "AppliedEventLedger",
    "EMPTY_EVENT_LEDGER",
    "InvalidLiveOrderTransition",
    "ReductionDisposition",
    "ReductionResult",
    "advance_local",
    "bind_operation",
    "create_live_order",
    "reduce_broker_fill_event",
    "reduce_broker_order_event",
    "reduce_dispatch",
    "request_cancel",
]
