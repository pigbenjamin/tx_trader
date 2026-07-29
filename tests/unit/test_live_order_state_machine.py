from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from tx_trade.orders.live_contracts import (
    BrokerCorrelation,
    BrokerOrderEventType,
    AmendOrderCommand,
    CancelOrderCommand,
    CorrelationStatus,
    DecreaseOrderCommand,
    DispatchReceipt,
    DispatchState,
    LiveCommandKind,
    LiveFailureCode,
    LiveOrder,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    NormalizedBrokerFillEvent,
    NormalizedBrokerOrderEvent,
    PendingCommandBinding,
    FingerprintDomain,
    payload_fingerprint,
)
from tx_trade.orders.live_state_machine import (
    EMPTY_EVENT_LEDGER,
    InvalidLiveOrderTransition,
    ReductionDisposition,
    advance_local,
    bind_operation,
    create_live_order,
    reduce_broker_fill_event,
    reduce_broker_order_event,
    reduce_dispatch,
    request_cancel,
)

NOW = datetime(2026, 7, 29, 1, 0, tzinfo=timezone.utc)
FP = f"sha256:{'1' * 64}"


def intent() -> LiveOrderIntent:
    return LiveOrderIntent(
        strategy_id="strategy-a",
        client_order_id="client-1",
        account_id="account-1",
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=Decimal("4"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=True,
        created_at=NOW,
    )


def binding(
    kind: LiveCommandKind = LiveCommandKind.NEW,
    *,
    command_id: str = "command-1",
    expected_price: Decimal | None = None,
    expected_total: Decimal | None = None,
    expected_current: Decimal = Decimal("4"),
    target_intent: LiveOrderIntent | None = None,
    client_order_id: str = "client-1",
) -> PendingCommandBinding:
    command: NewOrderCommand | CancelOrderCommand | AmendOrderCommand | DecreaseOrderCommand
    domain: FingerprintDomain
    if kind is LiveCommandKind.AMEND and expected_price is None:
        expected_price = Decimal("22100")
    if kind is LiveCommandKind.DECREASE and expected_total is None:
        expected_total = Decimal("3")
    if kind is LiveCommandKind.NEW:
        command = NewOrderCommand(command_id, target_intent or intent(), NOW)
        domain = FingerprintDomain.NEW_COMMAND_V1
    elif kind is LiveCommandKind.CANCEL:
        command = CancelOrderCommand(command_id, client_order_id, NOW)
        domain = FingerprintDomain.CANCEL_COMMAND_V1
    elif kind is LiveCommandKind.AMEND:
        assert expected_price is not None
        command = AmendOrderCommand(
            command_id,
            client_order_id,
            expected_price,
            NOW,
        )
        domain = FingerprintDomain.AMEND_COMMAND_V1
    else:
        assert expected_total is not None
        command = DecreaseOrderCommand(
            command_id,
            client_order_id,
            expected_current,
            expected_total,
            NOW,
        )
        domain = FingerprintDomain.DECREASE_COMMAND_V1
    return PendingCommandBinding(command, payload_fingerprint(command, domain))


def order(
    state: LiveOrderState,
    *,
    filled: Decimal = Decimal("0"),
    pending: PendingCommandBinding | None = None,
    total: Decimal = Decimal("4"),
) -> LiveOrder:
    if pending is None:
        if state in {
            LiveOrderState.SUBMITTING,
            LiveOrderState.SUBMISSION_UNKNOWN,
            LiveOrderState.RECONCILING,
        }:
            pending = binding()
        elif state is LiveOrderState.CANCEL_PENDING:
            pending = binding(LiveCommandKind.CANCEL)
    accepted_at = (
        NOW
        if state
        in {
            LiveOrderState.ACCEPTED,
            LiveOrderState.PARTIALLY_FILLED,
            LiveOrderState.FILLED,
            LiveOrderState.CANCEL_PENDING,
            LiveOrderState.CANCELLED,
        }
        else None
    )
    return LiveOrder(
        intent=intent(),
        state=state,
        total_quantity=total,
        filled_quantity=filled,
        remaining_quantity=total - filled,
        average_fill_price=Decimal("22000") if filled else None,
        working_limit_price=Decimal("22000"),
        version=3,
        updated_at=NOW,
        accepted_at=accepted_at,
        pending_command=pending,
    )


def correlation(
    *,
    status: CorrelationStatus = CorrelationStatus.CONFIRMED,
    client_order_id: str | None = "client-1",
    generation: int = 1,
    sequence: int = 1,
    fill_id: str | None = None,
    correlated_at: datetime = NOW,
) -> BrokerCorrelation:
    return BrokerCorrelation(
        broker_session_generation=generation,
        adapter_received_sequence=sequence,
        status=status,
        correlated_at=correlated_at,
        broker_order_sequence="broker-order-1",
        broker_fill_id=fill_id,
        client_order_id=client_order_id,
    )


def order_event(
    event_type: BrokerOrderEventType,
    *,
    event_id: str = "event-1",
    correlated: BrokerCorrelation | None = None,
    decreased: Decimal | None = None,
    new_price: Decimal | None = None,
    received_at: datetime = NOW + timedelta(seconds=1),
) -> NormalizedBrokerOrderEvent:
    actual = correlated or correlation()
    failures = {
        BrokerOrderEventType.NEW_REJECTED: LiveFailureCode.BROKER_REJECTED,
        BrokerOrderEventType.CANCEL_REJECTED: LiveFailureCode.CANCEL_REJECTED,
        BrokerOrderEventType.AMEND_REJECTED: LiveFailureCode.AMEND_REJECTED,
        BrokerOrderEventType.OUTCOME_UNKNOWN: LiveFailureCode.BROKER_TIMEOUT,
    }
    return NormalizedBrokerOrderEvent(
        event_id=event_id,
        account_id="account-1",
        instrument_id="TXF-202608",
        event_type=event_type,
        received_at=received_at,
        broker_session_generation=actual.broker_session_generation,
        adapter_received_sequence=actual.adapter_received_sequence,
        correlation=actual,
        occurred_at=received_at - timedelta(milliseconds=1),
        failure_code=failures.get(event_type),
        decreased_quantity=decreased,
        new_limit_price=new_price,
    )


def fill_event(
    *,
    event_id: str = "fill-event-1",
    quantity: Decimal = Decimal("1"),
    price: Decimal = Decimal("22100"),
    correlated: BrokerCorrelation | None = None,
    received_at: datetime = NOW + timedelta(seconds=1),
) -> NormalizedBrokerFillEvent:
    actual = correlated or correlation(fill_id="broker-fill-1")
    return NormalizedBrokerFillEvent(
        event_id=event_id,
        account_id="account-1",
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=quantity,
        execution_price=price,
        received_at=received_at,
        broker_session_generation=actual.broker_session_generation,
        adapter_received_sequence=actual.adapter_received_sequence,
        correlation=actual,
        occurred_at=received_at - timedelta(milliseconds=1),
    )


def test_create_and_local_submission_bind_exact_new_command() -> None:
    created = create_live_order(intent())
    assert created.working_limit_price == Decimal("22000")
    assert created.pending_command is None
    validated = advance_local(created, LiveOrderState.VALIDATED, NOW + timedelta(seconds=1))
    pending = binding()
    submitting = advance_local(
        validated,
        LiveOrderState.SUBMITTING,
        NOW + timedelta(seconds=2),
        pending,
    )
    assert submitting.pending_command is pending
    with pytest.raises(InvalidLiveOrderTransition, match="binding"):
        advance_local(
            validated,
            LiveOrderState.SUBMITTING,
            NOW + timedelta(seconds=2),
        )


def test_submission_rejects_new_binding_for_different_intent() -> None:
    created = create_live_order(intent())
    validated = advance_local(
        created,
        LiveOrderState.VALIDATED,
        NOW + timedelta(seconds=1),
    )
    cross_order = binding(
        target_intent=replace(intent(), strategy_id="strategy-b"),
    )
    with pytest.raises(InvalidLiveOrderTransition, match="intent"):
        advance_local(
            validated,
            LiveOrderState.SUBMITTING,
            NOW + timedelta(seconds=2),
            cross_order,
        )


def test_cancel_amend_and_decrease_bindings_are_exact() -> None:
    cancel = request_cancel(
        order(LiveOrderState.ACCEPTED),
        binding(LiveCommandKind.CANCEL),
        NOW + timedelta(seconds=1),
    )
    amend = bind_operation(
        order(LiveOrderState.ACCEPTED),
        binding(LiveCommandKind.AMEND),
        NOW + timedelta(seconds=1),
    )
    decrease = bind_operation(
        order(LiveOrderState.PARTIALLY_FILLED, filled=Decimal("1")),
        binding(LiveCommandKind.DECREASE),
        NOW + timedelta(seconds=1),
    )
    assert cancel.state is LiveOrderState.CANCEL_PENDING
    assert amend.pending_command is not None
    assert decrease.pending_command is not None


def test_cancel_rejects_binding_for_different_order() -> None:
    with pytest.raises(InvalidLiveOrderTransition, match="client_order_id"):
        request_cancel(
            order(LiveOrderState.ACCEPTED),
            binding(LiveCommandKind.CANCEL, client_order_id="client-2"),
            NOW + timedelta(seconds=1),
        )


@pytest.mark.parametrize(
    "kind",
    [LiveCommandKind.AMEND, LiveCommandKind.DECREASE],
)
def test_working_operation_rejects_binding_for_different_order(
    kind: LiveCommandKind,
) -> None:
    with pytest.raises(InvalidLiveOrderTransition, match="client_order_id"):
        bind_operation(
            order(LiveOrderState.ACCEPTED),
            binding(kind, client_order_id="client-2"),
            NOW + timedelta(seconds=1),
        )


def test_dispatch_mismatch_fails_closed_without_poisoning_ledger() -> None:
    receipt = DispatchReceipt(
        "different-command",
        FP,
        DispatchState.SUCCEEDED,
        NOW,
        NOW + timedelta(seconds=1),
    )
    result = reduce_dispatch(order(LiveOrderState.SUBMITTING), receipt)
    assert result.disposition is ReductionDisposition.EVENT_CONFLICT
    assert result.failure_code is LiveFailureCode.IDEMPOTENCY_CONFLICT
    assert result.ledger == EMPTY_EVENT_LEDGER


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        (LiveCommandKind.NEW, LiveOrderState.SUBMISSION_UNKNOWN),
        (LiveCommandKind.CANCEL, LiveOrderState.RECONCILING),
        (LiveCommandKind.AMEND, LiveOrderState.RECONCILING),
        (LiveCommandKind.DECREASE, LiveOrderState.RECONCILING),
    ],
)
def test_unknown_dispatch_routes_by_bound_operation(
    kind: LiveCommandKind, expected: LiveOrderState
) -> None:
    pending = binding(kind)
    initial = (
        order(LiveOrderState.SUBMITTING, pending=pending)
        if kind is LiveCommandKind.NEW
        else (
            order(LiveOrderState.CANCEL_PENDING, pending=pending)
            if kind is LiveCommandKind.CANCEL
            else order(LiveOrderState.ACCEPTED, pending=pending)
        )
    )
    receipt = DispatchReceipt(
        pending.client_command_id,
        pending.payload_fingerprint,
        DispatchState.UNKNOWN,
        NOW,
        None,
        LiveFailureCode.DISPATCH_OUTCOME_UNKNOWN,
    )
    result = reduce_dispatch(initial, receipt)
    assert result.order.state is expected
    assert result.order.pending_command is pending


def test_successful_dispatch_preserves_pending_and_is_not_acceptance() -> None:
    original = order(LiveOrderState.SUBMITTING)
    pending = original.pending_command
    assert pending is not None
    receipt = DispatchReceipt(
        pending.client_command_id,
        pending.payload_fingerprint,
        DispatchState.SUCCEEDED,
        NOW,
        NOW + timedelta(seconds=1),
    )
    result = reduce_dispatch(original, receipt)
    assert result.disposition is ReductionDisposition.NO_CHANGE
    assert result.order == original
    assert result.order.pending_command is pending


def test_failed_dispatch_enters_reconciliation_without_rejecting() -> None:
    original = order(LiveOrderState.SUBMITTING)
    pending = original.pending_command
    assert pending is not None
    receipt = DispatchReceipt(
        pending.client_command_id,
        pending.payload_fingerprint,
        DispatchState.FAILED,
        NOW,
        NOW + timedelta(seconds=1),
        LiveFailureCode.DISPATCH_FAILED,
    )
    result = reduce_dispatch(original, receipt)
    assert result.reconciliation_required
    assert result.order.state is LiveOrderState.RECONCILING


@pytest.mark.parametrize(
    ("event_type", "expected"),
    [
        (BrokerOrderEventType.NEW_ACCEPTED, LiveOrderState.ACCEPTED),
        (BrokerOrderEventType.NEW_REJECTED, LiveOrderState.REJECTED),
        (BrokerOrderEventType.OUTCOME_UNKNOWN, LiveOrderState.SUBMISSION_UNKNOWN),
    ],
)
def test_new_order_results(event_type: BrokerOrderEventType, expected: LiveOrderState) -> None:
    result = reduce_broker_order_event(order(LiveOrderState.SUBMITTING), order_event(event_type))
    assert result.order.state is expected
    assert (result.order.pending_command is None) is (
        event_type is not BrokerOrderEventType.OUTCOME_UNKNOWN
    )


def test_cancel_results_and_dynamic_cancel() -> None:
    pending = order(LiveOrderState.CANCEL_PENDING, filled=Decimal("1"))
    rejected = reduce_broker_order_event(pending, order_event(BrokerOrderEventType.CANCEL_REJECTED))
    cancelled = reduce_broker_order_event(pending, order_event(BrokerOrderEventType.CANCELLED))
    dynamic = reduce_broker_order_event(
        order(LiveOrderState.ACCEPTED),
        order_event(BrokerOrderEventType.DYNAMIC_CANCELLED),
    )
    assert rejected.order.state is LiveOrderState.PARTIALLY_FILLED
    assert rejected.order.pending_command is None
    assert cancelled.order.state is LiveOrderState.CANCELLED
    assert dynamic.order.state is LiveOrderState.CANCELLED


def test_dynamic_cancel_is_compatible_with_local_cancel_pending() -> None:
    original = order(LiveOrderState.CANCEL_PENDING, filled=Decimal("1"))
    result = reduce_broker_order_event(
        original,
        order_event(BrokerOrderEventType.DYNAMIC_CANCELLED),
    )
    assert result.order.state is LiveOrderState.CANCELLED
    assert result.order.pending_command is None


@pytest.mark.parametrize(
    "equivalent",
    [BrokerOrderEventType.CANCELLED, BrokerOrderEventType.DYNAMIC_CANCELLED],
)
def test_equivalent_cancellation_fact_on_cancelled_order_is_recorded_no_change(
    equivalent: BrokerOrderEventType,
) -> None:
    original = order(LiveOrderState.CANCELLED, filled=Decimal("1"))
    result = reduce_broker_order_event(original, order_event(equivalent))
    assert result.disposition is ReductionDisposition.NO_CHANGE
    assert result.order == original
    assert len(result.ledger.events) == 1


@pytest.mark.parametrize(
    ("event_type", "decreased", "new_price", "kind"),
    [
        (
            BrokerOrderEventType.PRICE_AMENDED,
            None,
            Decimal("22100"),
            LiveCommandKind.AMEND,
        ),
        (
            BrokerOrderEventType.QUANTITY_DECREASED,
            Decimal("3"),
            None,
            LiveCommandKind.DECREASE,
        ),
    ],
)
def test_authoritative_amendments_update_working_values(
    event_type: BrokerOrderEventType,
    decreased: Decimal | None,
    new_price: Decimal | None,
    kind: LiveCommandKind,
) -> None:
    expected_total = Decimal("4") - decreased if decreased is not None else None
    original = order(
        LiveOrderState.ACCEPTED,
        pending=binding(kind, expected_total=expected_total),
    )
    result = reduce_broker_order_event(
        original,
        order_event(event_type, decreased=decreased, new_price=new_price),
    )
    expected_result_total = expected_total or Decimal("4")
    assert result.order.total_quantity == expected_result_total
    assert result.order.remaining_quantity == expected_result_total
    assert result.order.working_limit_price == (new_price or Decimal("22000"))
    assert result.order.pending_command is None


def test_decrease_cannot_cross_filled_quantity_or_mismatch_bound_target() -> None:
    below_original = order(
        LiveOrderState.PARTIALLY_FILLED,
        filled=Decimal("2"),
        pending=binding(LiveCommandKind.DECREASE, expected_total=Decimal("1")),
    )
    below = reduce_broker_order_event(
        below_original,
        order_event(
            BrokerOrderEventType.QUANTITY_DECREASED,
            decreased=Decimal("3"),
        ),
    )
    mismatch_original = replace(
        below_original,
        pending_command=binding(
            LiveCommandKind.DECREASE,
            expected_total=Decimal("2"),
        ),
    )
    mismatch = reduce_broker_order_event(
        mismatch_original,
        order_event(
            BrokerOrderEventType.QUANTITY_DECREASED,
            decreased=Decimal("1"),
        ),
    )
    assert below.reconciliation_required and mismatch.reconciliation_required
    assert below.ledger == mismatch.ledger == EMPTY_EVENT_LEDGER


def test_stale_decrease_compare_and_swap_is_rejected_before_dispatch() -> None:
    stale = binding(
        LiveCommandKind.DECREASE,
        expected_current=Decimal("4"),
        expected_total=Decimal("2"),
    )
    with pytest.raises(InvalidLiveOrderTransition, match="expected_total_quantity"):
        bind_operation(
            order(LiveOrderState.ACCEPTED, total=Decimal("3")),
            stale,
            NOW + timedelta(seconds=1),
        )


def test_decrease_to_exact_filled_quantity_completes_order() -> None:
    original = order(
        LiveOrderState.PARTIALLY_FILLED,
        filled=Decimal("2"),
        pending=binding(LiveCommandKind.DECREASE, expected_total=Decimal("2")),
    )
    result = reduce_broker_order_event(
        original,
        order_event(
            BrokerOrderEventType.QUANTITY_DECREASED,
            decreased=Decimal("2"),
        ),
    )
    assert result.order.state is LiveOrderState.FILLED
    assert result.order.remaining_quantity == 0


def test_amend_result_must_match_bound_command_kind() -> None:
    original = order(
        LiveOrderState.ACCEPTED,
        pending=binding(LiveCommandKind.DECREASE),
    )
    result = reduce_broker_order_event(
        original,
        order_event(
            BrokerOrderEventType.PRICE_AMENDED,
            new_price=Decimal("22100"),
        ),
    )
    assert result.reconciliation_required
    assert result.order == original
    assert result.ledger == EMPTY_EVENT_LEDGER


@pytest.mark.parametrize(
    ("event_type", "pending", "decreased", "new_price"),
    [
        (
            BrokerOrderEventType.PRICE_AMENDED,
            binding(LiveCommandKind.AMEND, expected_price=Decimal("22200")),
            None,
            Decimal("22100"),
        ),
        (
            BrokerOrderEventType.QUANTITY_DECREASED,
            binding(LiveCommandKind.DECREASE, expected_total=Decimal("2")),
            Decimal("1"),
            None,
        ),
        (
            BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED,
            binding(LiveCommandKind.AMEND),
            Decimal("1"),
            Decimal("22100"),
        ),
    ],
)
def test_working_change_must_exactly_match_bound_targets(
    event_type: BrokerOrderEventType,
    pending: PendingCommandBinding,
    decreased: Decimal | None,
    new_price: Decimal | None,
) -> None:
    original = order(LiveOrderState.ACCEPTED, pending=pending)
    result = reduce_broker_order_event(
        original,
        order_event(
            event_type,
            decreased=decreased,
            new_price=new_price,
        ),
    )
    assert result.reconciliation_required
    assert result.order == original
    assert result.ledger == EMPTY_EVENT_LEDGER


@pytest.mark.parametrize("kind", [LiveCommandKind.AMEND, LiveCommandKind.DECREASE])
def test_amend_rejected_returns_to_open_state(kind: LiveCommandKind) -> None:
    original = order(
        LiveOrderState.PARTIALLY_FILLED,
        filled=Decimal("1"),
        pending=binding(kind),
    )
    result = reduce_broker_order_event(original, order_event(BrokerOrderEventType.AMEND_REJECTED))
    assert result.order.state is LiveOrderState.PARTIALLY_FILLED
    assert result.order.pending_command is None


def test_operation_outcome_unknown_enters_reconciliation() -> None:
    original = order(
        LiveOrderState.ACCEPTED,
        pending=binding(LiveCommandKind.AMEND),
    )
    result = reduce_broker_order_event(original, order_event(BrokerOrderEventType.OUTCOME_UNKNOWN))
    assert result.order.state is LiveOrderState.RECONCILING
    assert result.order.pending_command is not None


def test_late_new_result_does_not_regress_accepted_or_cancel_pending() -> None:
    accepted = order(
        LiveOrderState.ACCEPTED,
        pending=binding(LiveCommandKind.AMEND),
    )
    cancel_pending = order(LiveOrderState.CANCEL_PENDING)
    late_accept = reduce_broker_order_event(
        accepted, order_event(BrokerOrderEventType.NEW_ACCEPTED)
    )
    late_cancel_accept = reduce_broker_order_event(
        cancel_pending, order_event(BrokerOrderEventType.NEW_ACCEPTED)
    )
    late_reject = reduce_broker_order_event(
        accepted, order_event(BrokerOrderEventType.NEW_REJECTED)
    )
    assert late_accept.order.state is LiveOrderState.ACCEPTED
    assert late_accept.order.pending_command is accepted.pending_command
    assert late_cancel_accept.order.state is LiveOrderState.CANCEL_PENDING
    assert late_cancel_accept.order.pending_command is cancel_pending.pending_command
    assert late_reject.reconciliation_required
    assert late_reject.order == accepted


def test_semantic_redelivery_ignores_delivery_and_correlation_metadata() -> None:
    event = order_event(BrokerOrderEventType.NEW_ACCEPTED)
    first = reduce_broker_order_event(order(LiveOrderState.SUBMITTING), event)
    later_correlation = correlation(
        generation=2,
        sequence=9,
        correlated_at=NOW + timedelta(seconds=3),
    )
    redelivery = replace(
        event,
        received_at=NOW + timedelta(seconds=3),
        occurred_at=NOW,
        broker_session_generation=2,
        adapter_received_sequence=9,
        correlation=later_correlation,
    )
    duplicate = reduce_broker_order_event(first.order, redelivery, first.ledger)
    assert duplicate.disposition is ReductionDisposition.EXACT_DUPLICATE
    conflict = reduce_broker_order_event(
        first.order,
        replace(event, event_type=BrokerOrderEventType.DYNAMIC_CANCELLED),
        first.ledger,
    )
    assert conflict.disposition is ReductionDisposition.EVENT_CONFLICT
    assert conflict.reconciliation_required
    assert conflict.failure_code is LiveFailureCode.CORRELATION_CONFLICT


def test_fill_semantic_redelivery_ignores_delivery_metadata_but_not_payload() -> None:
    event = fill_event()
    first = reduce_broker_fill_event(order(LiveOrderState.ACCEPTED), event)
    later_correlation = correlation(
        generation=2,
        sequence=9,
        fill_id="broker-fill-redelivered-clue",
        correlated_at=NOW + timedelta(seconds=3),
    )
    redelivery = replace(
        event,
        received_at=NOW + timedelta(seconds=3),
        occurred_at=NOW,
        broker_session_generation=2,
        adapter_received_sequence=9,
        correlation=later_correlation,
    )
    duplicate = reduce_broker_fill_event(first.order, redelivery, first.ledger)
    conflict = reduce_broker_fill_event(
        first.order,
        replace(event, execution_price=Decimal("22200")),
        first.ledger,
    )
    assert duplicate.disposition is ReductionDisposition.EXACT_DUPLICATE
    assert conflict.disposition is ReductionDisposition.EVENT_CONFLICT


def test_candidate_evidence_can_later_be_confirmed() -> None:
    candidate_event = order_event(
        BrokerOrderEventType.NEW_ACCEPTED,
        correlated=correlation(
            status=CorrelationStatus.CANDIDATE,
            client_order_id=None,
        ),
    )
    original = order(LiveOrderState.SUBMITTING)
    candidate = reduce_broker_order_event(original, candidate_event)
    confirmed = reduce_broker_order_event(
        candidate.order,
        replace(candidate_event, correlation=correlation()),
        candidate.ledger,
    )
    assert candidate.reconciliation_required
    assert candidate.ledger == EMPTY_EVENT_LEDGER
    assert confirmed.order.state is LiveOrderState.ACCEPTED


def test_fill_weighted_average_and_pending_cancel_race() -> None:
    original = order(LiveOrderState.CANCEL_PENDING)
    first = reduce_broker_fill_event(
        original, fill_event(quantity=Decimal("1"), price=Decimal("22000"))
    )
    second = reduce_broker_fill_event(
        first.order,
        fill_event(
            event_id="fill-event-2",
            quantity=Decimal("2"),
            price=Decimal("22150"),
            correlated=correlation(sequence=2, fill_id="broker-fill-2"),
            received_at=NOW + timedelta(seconds=2),
        ),
        first.ledger,
    )
    assert second.order.state is LiveOrderState.CANCEL_PENDING
    assert second.order.average_fill_price == Decimal("22100")


def test_late_confirmed_fill_updates_cancelled_and_can_complete_order() -> None:
    cancelled = order(LiveOrderState.CANCELLED)
    partial = reduce_broker_fill_event(cancelled, fill_event(quantity=Decimal("1")))
    completed = reduce_broker_fill_event(
        partial.order,
        fill_event(
            event_id="fill-event-2",
            quantity=Decimal("3"),
            correlated=correlation(sequence=2, fill_id="broker-fill-2"),
            received_at=NOW + timedelta(seconds=2),
        ),
        partial.ledger,
    )
    assert partial.order.state is LiveOrderState.CANCELLED
    assert partial.order.filled_quantity == Decimal("1")
    assert completed.order.state is LiveOrderState.FILLED


def test_partial_fill_while_reconciling_preserves_operation_uncertainty() -> None:
    original = order(
        LiveOrderState.RECONCILING,
        pending=binding(LiveCommandKind.CANCEL),
    )
    result = reduce_broker_fill_event(original, fill_event())
    assert result.order.state is LiveOrderState.RECONCILING
    assert result.order.pending_command is original.pending_command
    assert result.order.filled_quantity == Decimal("1")


@pytest.mark.parametrize("terminal", [LiveOrderState.REJECTED, LiveOrderState.FILLED])
def test_extra_fill_for_rejected_or_filled_requires_reconciliation(
    terminal: LiveOrderState,
) -> None:
    filled = Decimal("4") if terminal is LiveOrderState.FILLED else Decimal("0")
    result = reduce_broker_fill_event(order(terminal, filled=filled), fill_event())
    assert result.reconciliation_required
    assert result.ledger == EMPTY_EVENT_LEDGER


def test_ambiguous_fill_does_not_guess_identity_or_poison_ledger() -> None:
    event = fill_event(
        correlated=correlation(
            status=CorrelationStatus.AMBIGUOUS,
            client_order_id=None,
            fill_id="broker-fill-1",
        )
    )
    result = reduce_broker_fill_event(order(LiveOrderState.ACCEPTED), event)
    assert result.reconciliation_required
    assert result.fill is None
    assert result.ledger == EMPTY_EVENT_LEDGER

    confirmed = reduce_broker_fill_event(
        result.order,
        replace(event, correlation=correlation(fill_id="broker-fill-1")),
        result.ledger,
    )
    assert confirmed.disposition is ReductionDisposition.APPLIED
    assert confirmed.fill is not None
