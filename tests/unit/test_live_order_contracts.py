from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from hashlib import sha256

import pytest

from tx_trade.orders.live_contracts import (
    AccountReadiness,
    AccountSnapshot,
    AmendOrderCommand,
    BrokerCorrelation,
    BrokerFillObservation,
    BrokerOpenOrderObservation,
    BrokerOrderEventType,
    BrokerPosition,
    CLIENT_ORDER_ID_UNIQUENESS_CONTRACT,
    CancelOrderCommand,
    CommandDeduplication,
    CommandDeduplicationResult,
    CorrelationStatus,
    DecreaseOrderCommand,
    DispatchReceipt,
    DispatchState,
    FingerprintDomain,
    LiveCommand,
    LiveCommandKind,
    LiveFailure,
    LiveFailureCode,
    LiveFill,
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
    ReadinessSnapshot,
    ReconciliationDiscrepancy,
    ReconciliationKind,
    StrategyPositionAttribution,
    broker_semantic_fingerprint,
    canonical_bytes,
    payload_fingerprint,
    to_canonical_primitive,
)

NOW = datetime(2026, 7, 29, 1, 2, 3, 456789, tzinfo=timezone.utc)


def intent(**changes: object) -> LiveOrderIntent:
    values: dict[str, object] = {
        "strategy_id": "strategy-a",
        "client_order_id": "global-order-1",
        "account_id": "account-secret",
        "instrument_id": "TXF-202608",
        "side": LiveSide.BUY,
        "quantity": Decimal("2"),
        "order_type": LiveOrderType.LIMIT,
        "limit_price": Decimal("22100.50"),
        "time_in_force": LiveTimeInForce.DAY,
        "day_trade": True,
        "created_at": NOW,
    }
    values.update(changes)
    return LiveOrderIntent(**values)  # type: ignore[arg-type]


def new_command(**changes: object) -> NewOrderCommand:
    values: dict[str, object] = {
        "client_command_id": "new-command-1",
        "intent": intent(),
        "requested_at": NOW,
    }
    values.update(changes)
    return NewOrderCommand(**values)  # type: ignore[arg-type]


def broker_correlation(**changes: object) -> BrokerCorrelation:
    values: dict[str, object] = {
        "broker_session_generation": 3,
        "adapter_received_sequence": 17,
        "status": CorrelationStatus.CANDIDATE,
        "correlated_at": NOW,
        "async_thread_id": "thread-secret",
        "proxy_stamp_id": "stamp-secret",
    }
    values.update(changes)
    return BrokerCorrelation(**values)  # type: ignore[arg-type]


def order_event(
    event_type: BrokerOrderEventType = BrokerOrderEventType.NEW_ACCEPTED,
    **changes: object,
) -> NormalizedBrokerOrderEvent:
    failures = {
        BrokerOrderEventType.NEW_REJECTED: LiveFailureCode.BROKER_REJECTED,
        BrokerOrderEventType.CANCEL_REJECTED: LiveFailureCode.CANCEL_REJECTED,
        BrokerOrderEventType.AMEND_REJECTED: LiveFailureCode.AMEND_REJECTED,
        BrokerOrderEventType.OUTCOME_UNKNOWN: LiveFailureCode.BROKER_TIMEOUT,
    }
    values: dict[str, object] = {
        "event_id": "broker-event-17",
        "account_id": "account-secret",
        "instrument_id": "TXF-202608",
        "event_type": event_type,
        "received_at": NOW,
        "broker_session_generation": 3,
        "adapter_received_sequence": 17,
        "correlation": broker_correlation(),
        "failure_code": failures.get(event_type),
        "decreased_quantity": (
            Decimal("1")
            if event_type
            in {
                BrokerOrderEventType.QUANTITY_DECREASED,
                BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED,
            }
            else None
        ),
        "new_limit_price": (
            Decimal("22101")
            if event_type
            in {
                BrokerOrderEventType.PRICE_AMENDED,
                BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED,
            }
            else None
        ),
    }
    values.update(changes)
    return NormalizedBrokerOrderEvent(**values)  # type: ignore[arg-type]


def test_intent_is_frozen_slotted_strict_and_supports_market() -> None:
    source = intent()
    market = intent(order_type=LiveOrderType.MARKET, limit_price=None)

    assert not hasattr(source, "__dict__")
    assert market.limit_price is None
    assert CLIENT_ORDER_ID_UNIQUENESS_CONTRACT == "global:v1"
    with pytest.raises(FrozenInstanceError):
        source.quantity = Decimal("3")  # type: ignore[misc]
    with pytest.raises(TypeError, match="side"):
        intent(side="buy")
    with pytest.raises(TypeError, match="day_trade"):
        intent(day_trade=1)


@pytest.mark.parametrize("field", ["strategy_id", "client_order_id", "account_id", "instrument_id"])
@pytest.mark.parametrize("value", ["", "a" * 129, "has space", "é"])
def test_identifiers_are_bounded_and_sanitized(field: str, value: str) -> None:
    with pytest.raises(ValueError):
        intent(**{field: value})


@pytest.mark.parametrize(
    "value", [1, 1.0, True, "2", Decimal("NaN"), Decimal("Infinity"), Decimal("1e7000")]
)
def test_decimal_is_strict_finite_and_bounded(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        intent(quantity=value)


def test_quantity_price_and_utc_invariants() -> None:
    with pytest.raises(ValueError, match="greater than zero"):
        intent(quantity=Decimal("0"))
    with pytest.raises(ValueError, match="must not"):
        intent(order_type=LiveOrderType.MARKET)
    with pytest.raises(ValueError, match="requires"):
        intent(limit_price=None)
    with pytest.raises(ValueError, match="UTC"):
        intent(created_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="UTC"):
        intent(created_at=NOW.astimezone(timezone(timedelta(hours=8))))


def test_each_command_has_independent_id_and_payload() -> None:
    commands = (
        new_command(),
        CancelOrderCommand("cancel-command-1", "global-order-1", NOW),
        AmendOrderCommand("amend-command-1", "global-order-1", Decimal("22101"), NOW),
        DecreaseOrderCommand(
            "decrease-command-1",
            "global-order-1",
            Decimal("2"),
            Decimal("1"),
            NOW,
        ),
    )
    assert len({item.client_command_id for item in commands}) == 4
    assert [item.kind.value for item in commands] == ["new", "cancel", "amend", "decrease"]
    for item in commands:
        assert not hasattr(item, "__dict__")
    with pytest.raises(ValueError, match="less than"):
        DecreaseOrderCommand(
            "decrease-command-2",
            "global-order-1",
            Decimal("2"),
            Decimal("2"),
            NOW,
        )


def test_order_aggregate_conserves_quantity_and_authoritative_times() -> None:
    order = LiveOrder(
        intent=intent(),
        state=LiveOrderState.PARTIALLY_FILLED,
        total_quantity=Decimal("2"),
        filled_quantity=Decimal("1"),
        remaining_quantity=Decimal("1"),
        average_fill_price=Decimal("22100"),
        working_limit_price=Decimal("22100.5"),
        version=2,
        updated_at=NOW,
        accepted_at=NOW,
    )
    assert order.state.is_terminal is False
    with pytest.raises(ValueError, match="must equal"):
        replace(order, remaining_quantity=Decimal("0"))
    with pytest.raises(ValueError, match="exactly"):
        replace(order, average_fill_price=None)
    with pytest.raises(TypeError, match="integer"):
        replace(order, version=True)
    with pytest.raises(ValueError, match="requires accepted_at"):
        replace(order, accepted_at=None)
    with pytest.raises(ValueError, match="zero filled"):
        replace(order, state=LiveOrderState.ACCEPTED)


def test_pending_binding_is_durable_and_state_bound() -> None:
    command = new_command()
    binding = PendingCommandBinding(
        command=command,
        payload_fingerprint=payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
    )
    submitting = LiveOrder(
        intent=command.intent,
        state=LiveOrderState.SUBMITTING,
        total_quantity=Decimal("2"),
        filled_quantity=Decimal("0"),
        remaining_quantity=Decimal("2"),
        average_fill_price=None,
        working_limit_price=Decimal("22100.5"),
        version=1,
        updated_at=NOW,
        pending_command=binding,
    )
    unknown = replace(submitting, state=LiveOrderState.SUBMISSION_UNKNOWN)
    reconciling = replace(submitting, state=LiveOrderState.RECONCILING)
    assert unknown.pending_command == binding
    assert reconciling.pending_command == binding
    with pytest.raises(ValueError, match="requires pending"):
        replace(submitting, pending_command=None)

    amend_command = AmendOrderCommand(
        "amend-command-1",
        "global-order-1",
        Decimal("22101"),
        NOW,
    )
    amend = PendingCommandBinding(
        command=amend_command,
        payload_fingerprint=payload_fingerprint(amend_command, FingerprintDomain.AMEND_COMMAND_V1),
    )
    accepted = replace(
        submitting,
        state=LiveOrderState.ACCEPTED,
        accepted_at=NOW,
        pending_command=amend,
    )
    assert accepted.pending_command is amend
    with pytest.raises(ValueError, match="CANCEL"):
        replace(accepted, state=LiveOrderState.CANCEL_PENDING)
    with pytest.raises(ValueError, match="must not retain"):
        replace(accepted, state=LiveOrderState.CANCELLED)
    assert command.client_command_id not in repr(binding)
    assert binding.payload_fingerprint not in repr(binding)


def test_pending_binding_targets_are_kind_strict_and_match_exact_broker_changes() -> None:
    new = new_command()
    base = PendingCommandBinding(
        command=new,
        payload_fingerprint=payload_fingerprint(new, FingerprintDomain.NEW_COMMAND_V1),
    )
    wrong_domain = (
        "sha256:"
        + sha256(
            FingerprintDomain.CANCEL_COMMAND_V1.value.encode("ascii")
            + b"\x00"
            + canonical_bytes(new)
        ).hexdigest()
    )
    with pytest.raises(ValueError, match="exact bound command"):
        replace(base, payload_fingerprint=wrong_domain)
    with pytest.raises(ValueError, match="exact bound command"):
        replace(base, payload_fingerprint=f"sha256:{'0' * 64}")
    with pytest.raises(TypeError):
        replace(base, expected_new_limit_price=Decimal("22101"))  # type: ignore[call-arg]

    amend_command = AmendOrderCommand(
        "amend-command-1",
        "global-order-1",
        Decimal("22101"),
        NOW,
    )
    price = PendingCommandBinding(
        command=amend_command,
        payload_fingerprint=payload_fingerprint(amend_command, FingerprintDomain.AMEND_COMMAND_V1),
    )
    decrease_command = DecreaseOrderCommand(
        "decrease-command-1",
        "global-order-1",
        Decimal("2"),
        Decimal("1"),
        NOW,
    )
    decrease = PendingCommandBinding(
        command=decrease_command,
        payload_fingerprint=payload_fingerprint(
            decrease_command, FingerprintDomain.DECREASE_COMMAND_V1
        ),
    )
    assert base.client_command_id == new.client_command_id
    assert base.command_kind is LiveCommandKind.NEW
    assert base.bound_at == NOW
    assert base.expected_new_limit_price is None
    assert base.expected_current_total_quantity is None
    assert price.expected_new_limit_price == Decimal("22101")
    assert price.expected_new_total_quantity is None
    assert decrease.expected_current_total_quantity == Decimal("2")
    assert decrease.expected_new_total_quantity == Decimal("1")
    assert price.matches_authoritative_working_change(
        order_event(BrokerOrderEventType.PRICE_AMENDED),
        current_total_quantity=Decimal("2"),
    )
    assert not price.matches_authoritative_working_change(
        order_event(BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED),
        current_total_quantity=Decimal("2"),
    )
    assert decrease.matches_authoritative_working_change(
        order_event(BrokerOrderEventType.QUANTITY_DECREASED),
        current_total_quantity=Decimal("2"),
    )
    assert not price.matches_authoritative_working_change(
        order_event(
            BrokerOrderEventType.PRICE_AMENDED,
            new_limit_price=Decimal("22102"),
        ),
        current_total_quantity=Decimal("2"),
    )
    assert not decrease.matches_authoritative_working_change(
        order_event(BrokerOrderEventType.QUANTITY_DECREASED),
        current_total_quantity=Decimal("3"),
    )
    altered_amend = replace(amend_command, new_limit_price=Decimal("22102"))
    with pytest.raises(ValueError, match="exact bound command"):
        replace(price, command=altered_amend)
    assert not decrease.matches_authoritative_working_change(
        order_event(
            BrokerOrderEventType.QUANTITY_DECREASED,
            decreased_quantity=Decimal("0.5"),
        ),
        current_total_quantity=Decimal("2"),
    )
    with pytest.raises(TypeError, match="Decimal"):
        decrease.matches_authoritative_working_change(
            order_event(BrokerOrderEventType.QUANTITY_DECREASED),
            current_total_quantity=2,  # type: ignore[arg-type]
        )


def test_order_restore_rejects_pending_bindings_for_a_different_order() -> None:
    own_command = new_command()
    own_binding = PendingCommandBinding(
        command=own_command,
        payload_fingerprint=payload_fingerprint(own_command, FingerprintDomain.NEW_COMMAND_V1),
    )
    other_new = new_command(
        client_command_id="other-new-command",
        intent=intent(client_order_id="other-order"),
    )
    other_new_binding = PendingCommandBinding(
        command=other_new,
        payload_fingerprint=payload_fingerprint(other_new, FingerprintDomain.NEW_COMMAND_V1),
    )
    with pytest.raises(ValueError, match="intent must equal"):
        LiveOrder(
            intent=own_command.intent,
            state=LiveOrderState.SUBMITTING,
            total_quantity=Decimal("2"),
            filled_quantity=Decimal("0"),
            remaining_quantity=Decimal("2"),
            average_fill_price=None,
            working_limit_price=Decimal("22100.5"),
            version=1,
            updated_at=NOW,
            pending_command=other_new_binding,
        )

    own_order = LiveOrder(
        intent=own_command.intent,
        state=LiveOrderState.SUBMITTING,
        total_quantity=Decimal("2"),
        filled_quantity=Decimal("0"),
        remaining_quantity=Decimal("2"),
        average_fill_price=None,
        working_limit_price=Decimal("22100.5"),
        version=1,
        updated_at=NOW,
        pending_command=own_binding,
    )
    operations: tuple[tuple[LiveCommand, FingerprintDomain], ...] = (
        (
            CancelOrderCommand("cancel-other", "other-order", NOW),
            FingerprintDomain.CANCEL_COMMAND_V1,
        ),
        (
            AmendOrderCommand(
                "amend-other",
                "other-order",
                Decimal("22101"),
                NOW,
            ),
            FingerprintDomain.AMEND_COMMAND_V1,
        ),
        (
            DecreaseOrderCommand(
                "decrease-other",
                "other-order",
                Decimal("2"),
                Decimal("1"),
                NOW,
            ),
            FingerprintDomain.DECREASE_COMMAND_V1,
        ),
    )
    for operation, domain in operations:
        mismatched = PendingCommandBinding(
            command=operation,
            payload_fingerprint=payload_fingerprint(operation, domain),
        )
        with pytest.raises(ValueError, match="target this order"):
            replace(
                own_order,
                state=LiveOrderState.ACCEPTED,
                accepted_at=NOW,
                pending_command=mismatched,
            )


def test_all_required_states_are_present() -> None:
    assert {state.name for state in LiveOrderState} == {
        "CREATED",
        "VALIDATED",
        "SUBMITTING",
        "ACCEPTED",
        "PARTIALLY_FILLED",
        "FILLED",
        "REJECTED",
        "CANCEL_PENDING",
        "CANCELLED",
        "SUBMISSION_UNKNOWN",
        "RECONCILING",
    }


def test_dispatch_success_is_explicitly_not_broker_acceptance() -> None:
    receipt = DispatchReceipt(
        client_command_id="new-command-1",
        payload_fingerprint=payload_fingerprint(new_command(), FingerprintDomain.NEW_COMMAND_V1),
        state=DispatchState.SUCCEEDED,
        attempted_at=NOW,
        completed_at=NOW,
    )
    accepted = NormalizedBrokerOrderEvent(
        event_id="event-1",
        account_id="account-secret",
        instrument_id="TXF-202608",
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=NOW,
        broker_session_generation=3,
        adapter_received_sequence=17,
        correlation=broker_correlation(),
    )
    assert receipt.broker_accepted is False
    assert receipt.state.is_authoritative_acceptance is False
    assert accepted.is_authoritative_acceptance is True
    assert accepted.correlation.client_order_id is None


def test_dispatch_failure_contract_is_sanitized_and_consistent() -> None:
    fingerprint = payload_fingerprint(new_command(), FingerprintDomain.NEW_COMMAND_V1)
    failed = DispatchReceipt(
        "new-command-1",
        fingerprint,
        DispatchState.FAILED,
        NOW,
        NOW,
        LiveFailureCode.DISPATCH_FAILED,
    )
    assert failed.failure_code.public_message == "live command dispatch failed"
    with pytest.raises(ValueError, match="must not"):
        replace(failed, state=DispatchState.SUCCEEDED)
    with pytest.raises(ValueError, match="requires"):
        replace(failed, failure_code=None)


def test_domain_separated_fingerprints_are_deterministic_and_semantic() -> None:
    first = new_command()
    equivalent = new_command(intent=replace(first.intent, quantity=Decimal("2.00")))
    fingerprint = payload_fingerprint(first, FingerprintDomain.NEW_COMMAND_V1)

    assert fingerprint == payload_fingerprint(equivalent, FingerprintDomain.NEW_COMMAND_V1)
    assert fingerprint.startswith("sha256:")
    assert len(fingerprint) == 71
    with pytest.raises(ValueError, match="does not match"):
        payload_fingerprint(first, FingerprintDomain.CANCEL_COMMAND_V1)
    with pytest.raises(TypeError, match="FingerprintDomain"):
        payload_fingerprint(first, "tx_trade.live.command.new.v1")  # type: ignore[arg-type]


def test_canonical_bytes_are_sorted_compact_utf8_and_normalize_values() -> None:
    encoded = canonical_bytes(new_command())
    assert encoded == canonical_bytes(new_command())
    assert b" " not in encoded
    assert b'"quantity":"2"' in encoded
    assert b"2026-07-29T01:02:03.456789Z" in encoded
    assert encoded.startswith(b'{"client_command_id"')
    with pytest.raises(TypeError, match="binary float"):
        to_canonical_primitive(1.0)


def test_exact_retry_and_same_id_payload_conflict_are_representable() -> None:
    incoming = payload_fingerprint(new_command(), FingerprintDomain.NEW_COMMAND_V1)
    other = payload_fingerprint(
        new_command(intent=intent(quantity=Decimal("3"))),
        FingerprintDomain.NEW_COMMAND_V1,
    )
    exact = CommandDeduplicationResult(
        "new-command-1", incoming, incoming, CommandDeduplication.EXACT_RETRY
    )
    conflict = CommandDeduplicationResult(
        "new-command-1", incoming, other, CommandDeduplication.PAYLOAD_CONFLICT
    )
    assert exact.disposition is CommandDeduplication.EXACT_RETRY
    assert conflict.disposition is CommandDeduplication.PAYLOAD_CONFLICT
    with pytest.raises(ValueError, match="equality"):
        replace(conflict, recorded_fingerprint=incoming)


def test_fill_positions_and_attribution_are_distinct_strict_models() -> None:
    local_fill = LiveFill(
        "fill-1",
        "global-order-1",
        "strategy-a",
        "account-secret",
        "TXF-202608",
        LiveSide.BUY,
        Decimal("1"),
        Decimal("22100"),
        NOW,
    )
    broker = BrokerPosition("account-secret", "TXF-202608", Decimal("-1"), Decimal("22100"), NOW)
    attribution = StrategyPositionAttribution(
        "strategy-a", "account-secret", "TXF-202608", Decimal("-1"), NOW
    )
    event = NormalizedBrokerFillEvent(
        event_id="event-secret",
        account_id="account-secret",
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        execution_price=Decimal("22100"),
        received_at=NOW,
        broker_session_generation=3,
        adapter_received_sequence=17,
        correlation=broker_correlation(execution_no="execution-secret"),
    )
    assert event.quantity == local_fill.quantity
    assert event.correlation.client_order_id is None
    assert broker.net_quantity == attribution.attributed_quantity
    with pytest.raises(ValueError, match="exactly"):
        replace(broker, net_quantity=Decimal("0"))


def test_correlation_and_broker_sensitive_ids_do_not_leak_from_repr() -> None:
    correlation = broker_correlation(
        submission_attempt_id="attempt-secret",
        broker_order_sequence="order-sequence-secret",
        broker_book_no="book-secret",
        client_order_id="global-order-1",
        status=CorrelationStatus.CONFIRMED,
    )
    event = NormalizedBrokerFillEvent(
        event_id="event-secret",
        account_id="account-secret",
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        execution_price=Decimal("22100"),
        received_at=NOW,
        broker_session_generation=3,
        adapter_received_sequence=17,
        correlation=replace(correlation, execution_no="execution-secret"),
    )
    rendered = repr(event)
    for secret in (
        "account-secret",
        "event-secret",
        "attempt-secret",
        "thread-secret",
        "stamp-secret",
        "order-sequence-secret",
        "book-secret",
        "execution-secret",
        "global-order-1",
    ):
        assert secret not in rendered


def test_correlation_clues_remain_separate_and_sequences_are_strict() -> None:
    correlation = broker_correlation(
        async_thread_id="thread-7",
        proxy_stamp_id="stamp-7",
        broker_order_sequence="order-7",
    )
    assert correlation.async_thread_id != correlation.broker_order_sequence
    assert not hasattr(correlation, "broker_order_id")
    with pytest.raises(ValueError, match="at least one"):
        broker_correlation(async_thread_id=None, proxy_stamp_id=None)
    with pytest.raises(TypeError, match="integer"):
        broker_correlation(adapter_received_sequence=True)
    with pytest.raises(ValueError, match="positive"):
        broker_correlation(broker_session_generation=0)
    with pytest.raises(ValueError, match="requires client_order_id"):
        broker_correlation(status=CorrelationStatus.CONFIRMED)


def test_official_like_uncorrelated_order_observation_needs_no_local_identity() -> None:
    observation = NormalizedBrokerOrderEvent(
        event_id="adapter-event-17",
        account_id="account-secret",
        instrument_id="TXF-202608",
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=NOW,
        broker_session_generation=3,
        adapter_received_sequence=17,
        correlation=broker_correlation(),
        occurred_at=None,
    )
    assert observation.occurred_at is None
    assert observation.correlation.client_order_id is None
    assert not hasattr(observation, "client_order_id")


@pytest.mark.parametrize("event_type", list(BrokerOrderEventType))
def test_every_broker_operation_result_has_distinct_valid_semantics(
    event_type: BrokerOrderEventType,
) -> None:
    event = order_event(event_type)
    assert event.event_type is event_type
    assert event.is_terminal is (
        event_type
        in {
            BrokerOrderEventType.NEW_REJECTED,
            BrokerOrderEventType.CANCELLED,
            BrokerOrderEventType.DYNAMIC_CANCELLED,
        }
    )


def test_working_change_events_and_operation_failures_are_strict() -> None:
    decreased = order_event(BrokerOrderEventType.QUANTITY_DECREASED)
    amended = order_event(BrokerOrderEventType.PRICE_AMENDED)
    combined = order_event(BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED)
    assert decreased.decreased_quantity == Decimal("1")
    assert amended.new_limit_price == Decimal("22101")
    assert combined.decreased_quantity == Decimal("1")
    assert combined.new_limit_price == Decimal("22101")
    assert combined.failure_code is None
    assert combined.is_terminal is False
    with pytest.raises(ValueError, match="decreased_quantity"):
        order_event(BrokerOrderEventType.QUANTITY_DECREASED, decreased_quantity=None)
    with pytest.raises(ValueError, match="new_limit_price"):
        order_event(BrokerOrderEventType.NEW_ACCEPTED, new_limit_price=Decimal("1"))
    with pytest.raises(ValueError, match="decreased_quantity"):
        order_event(
            BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED,
            decreased_quantity=None,
        )
    with pytest.raises(ValueError, match="new_limit_price"):
        order_event(
            BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED,
            new_limit_price=None,
        )
    with pytest.raises(ValueError, match="semantics"):
        order_event(
            BrokerOrderEventType.CANCEL_REJECTED,
            failure_code=LiveFailureCode.AMEND_REJECTED,
        )


def test_outcome_unknown_is_nonterminal_timeout_evidence() -> None:
    timeout = order_event(BrokerOrderEventType.OUTCOME_UNKNOWN)
    assert timeout.failure_code is LiveFailureCode.BROKER_TIMEOUT
    assert timeout.is_terminal is False
    assert timeout.is_authoritative_acceptance is False
    with pytest.raises(ValueError, match="semantics"):
        order_event(
            BrokerOrderEventType.OUTCOME_UNKNOWN,
            failure_code=LiveFailureCode.BROKER_REJECTED,
        )


def test_broker_native_query_observations_need_no_local_identity() -> None:
    open_order = BrokerOpenOrderObservation(
        observation_id="open-observation-1",
        account_id="account-secret",
        instrument_id="TXF-202608",
        side=LiveSide.SELL,
        working_total_quantity=Decimal("3"),
        working_remaining_quantity=Decimal("2"),
        working_limit_price=Decimal("22100"),
        correlation=broker_correlation(broker_book_no="book-secret"),
        observed_at=NOW,
    )
    fill = BrokerFillObservation(
        observation_id="fill-observation-1",
        account_id="account-secret",
        instrument_id="TXF-202608",
        side=LiveSide.SELL,
        quantity=Decimal("1"),
        execution_price=Decimal("22100"),
        correlation=broker_correlation(execution_no="execution-secret"),
        observed_at=NOW,
    )
    assert open_order.correlation.client_order_id is None
    assert fill.correlation.client_order_id is None
    assert not hasattr(open_order, "strategy_id")
    assert not hasattr(fill, "client_order_id")
    assert "account-secret" not in repr(open_order)
    assert "execution-secret" not in repr(fill)


def test_broker_semantic_fingerprint_ignores_delivery_and_local_correlation() -> None:
    first = order_event(BrokerOrderEventType.PRICE_AND_QUANTITY_AMENDED)
    redelivered_correlation = broker_correlation(
        broker_session_generation=4,
        adapter_received_sequence=99,
        status=CorrelationStatus.CONFIRMED,
        client_order_id="global-order-1",
        broker_order_sequence="order-secret",
        correlated_at=NOW + timedelta(seconds=2),
    )
    redelivered = replace(
        first,
        received_at=NOW + timedelta(seconds=2),
        occurred_at=NOW - timedelta(seconds=1),
        broker_session_generation=4,
        adapter_received_sequence=99,
        correlation=redelivered_correlation,
    )
    price_conflict = replace(first, new_limit_price=Decimal("22102"))
    quantity_conflict = replace(first, decreased_quantity=Decimal("0.5"))
    assert broker_semantic_fingerprint(first) == broker_semantic_fingerprint(redelivered)
    assert broker_semantic_fingerprint(first) != broker_semantic_fingerprint(price_conflict)
    assert broker_semantic_fingerprint(first) != broker_semantic_fingerprint(quantity_conflict)


def test_fill_semantic_fingerprint_ignores_delivery_metadata() -> None:
    first = NormalizedBrokerFillEvent(
        event_id="fill-event-1",
        account_id="account-secret",
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        execution_price=Decimal("22100"),
        received_at=NOW,
        broker_session_generation=3,
        adapter_received_sequence=17,
        correlation=broker_correlation(execution_no="execution-secret"),
    )
    later_correlation = broker_correlation(
        broker_session_generation=4,
        adapter_received_sequence=18,
        execution_no="execution-secret",
    )
    later = replace(
        first,
        received_at=NOW + timedelta(seconds=1),
        broker_session_generation=4,
        adapter_received_sequence=18,
        correlation=later_correlation,
    )
    assert broker_semantic_fingerprint(first) == broker_semantic_fingerprint(later)
    assert broker_semantic_fingerprint(first) != broker_semantic_fingerprint(
        replace(first, quantity=Decimal("2"))
    )


def test_account_readiness_and_reconciliation_contracts() -> None:
    account = AccountSnapshot("account-secret", "TWD", Decimal("1000"), NOW)
    ready = ReadinessSnapshot("account-secret", AccountReadiness.READY, "session-1", NOW)
    discrepancy = ReconciliationDiscrepancy(
        "discrepancy-1",
        ReconciliationKind.QUANTITY_MISMATCH,
        "account-secret",
        "TXF-202608",
        NOW,
        "global-order-1",
        Decimal("2"),
        Decimal("1"),
    )
    assert account.currency == "TWD"
    assert ready.failure_code is None
    assert discrepancy.expected_quantity != discrepancy.actual_quantity
    with pytest.raises(ValueError, match="provided together"):
        replace(discrepancy, actual_quantity=None)
    with pytest.raises(ValueError, match="exactly"):
        replace(ready, readiness=AccountReadiness.NOT_READY)


def test_failures_expose_only_stable_sanitized_codes_and_messages() -> None:
    failure = LiveFailure(LiveFailureCode.INTERNAL_FAILURE, NOW, "new-command-1")
    assert failure.message == "live operation failed"
    assert len({code.public_message for code in LiveFailureCode}) == len(LiveFailureCode)
    assert "credential" not in repr(failure).lower()
    rejected = NormalizedBrokerOrderEvent(
        event_id="event-3",
        account_id="account-secret",
        instrument_id="TXF-202608",
        event_type=BrokerOrderEventType.NEW_REJECTED,
        received_at=NOW,
        broker_session_generation=3,
        adapter_received_sequence=17,
        correlation=broker_correlation(),
        failure_code=LiveFailureCode.BROKER_REJECTED,
    )
    assert rejected.failure_code is LiveFailureCode.BROKER_REJECTED
    with pytest.raises(ValueError, match="semantics"):
        replace(rejected, failure_code=None)


def test_module_has_no_forbidden_runtime_dependencies_or_raw_payload_fields() -> None:
    import tx_trade.orders.live_contracts as module

    source = module.__loader__.get_source(module.__name__)  # type: ignore[union-attr]
    assert source is not None
    lowered = source.lower()
    assert "import com" not in lowered
    assert "import os" not in lowered
    assert "getenv" not in lowered
    assert "credential" not in lowered
    assert "raw_payload" not in lowered
    assert "skcom" not in lowered
