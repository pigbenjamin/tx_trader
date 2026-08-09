from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import permutations

import pytest

from tx_trade.orders.live_contracts import (
    BrokerCorrelation,
    BrokerFillObservation,
    BrokerOpenOrderObservation,
    CancelOrderCommand,
    CorrelationStatus,
    FingerprintDomain,
    LiveOrder,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    PendingCommandBinding,
    payload_fingerprint,
)
from tx_trade.orders.live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    CompletenessEvidence,
    EvidenceCompleteness,
    EvidenceQueryKind,
    OpenOrdersSnapshot,
)
from tx_trade.orders.live_reconciliation import assess_reconciliation
from tx_trade.orders.live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    LocalReconciliationSnapshot,
    ReconciliationAssessment,
)
from tx_trade.orders.live_reconciliation_projection import project_authoritative_orders
from tx_trade.orders.live_reconciliation_projection_contracts import (
    OrderProjectionDisposition,
    OrderProjectionReason,
)

CREATED = datetime(2026, 8, 4, tzinfo=timezone.utc)
OBSERVED = CREATED + timedelta(seconds=1)
RECONCILED = OBSERVED + timedelta(seconds=1)
ACCOUNT = "account-1"
SNAPSHOT = "snapshot-1"


def _evidence(
    kind: EvidenceQueryKind,
    status: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
) -> CompletenessEvidence:
    return CompletenessEvidence(
        kind,
        ACCOUNT,
        status,
        OBSERVED,
        SNAPSHOT,
        None if status is EvidenceCompleteness.COMPLETE else "incomplete-query",
    )


def _intent(
    order_id: str,
    *,
    side: LiveSide = LiveSide.BUY,
    order_type: LiveOrderType = LiveOrderType.LIMIT,
    limit_price: Decimal | None = Decimal("22000"),
) -> LiveOrderIntent:
    return LiveOrderIntent(
        f"strategy-{order_id}",
        order_id,
        ACCOUNT,
        f"instrument-{order_id}",
        side,
        Decimal("2"),
        order_type,
        limit_price,
        LiveTimeInForce.DAY,
        False,
        CREATED,
    )


def _pending_order(
    order_id: str = "order-1",
    *,
    state: LiveOrderState = LiveOrderState.SUBMISSION_UNKNOWN,
    new_command: bool = True,
    version: int = 3,
    order_type: LiveOrderType = LiveOrderType.LIMIT,
    limit_price: Decimal | None = Decimal("22000"),
) -> LiveOrder:
    intent = _intent(order_id, order_type=order_type, limit_price=limit_price)
    if new_command:
        command = NewOrderCommand(f"new-{order_id}", intent, CREATED)
        domain = FingerprintDomain.NEW_COMMAND_V1
    else:
        command = CancelOrderCommand(f"cancel-{order_id}", order_id, CREATED)
        domain = FingerprintDomain.CANCEL_COMMAND_V1
    binding = PendingCommandBinding(command, payload_fingerprint(command, domain))
    return LiveOrder(
        intent,
        state,
        Decimal("2"),
        Decimal(0),
        Decimal("2"),
        None,
        intent.limit_price,
        version,
        CREATED,
        None,
        binding,
    )


def _accepted_order(order_id: str = "order-1") -> LiveOrder:
    pending = _pending_order(order_id)
    return replace(
        pending,
        state=LiveOrderState.ACCEPTED,
        accepted_at=CREATED,
        pending_command=None,
    )


def _correlation(
    order_id: str,
    *,
    status: CorrelationStatus = CorrelationStatus.CONFIRMED,
    correlated_at: datetime = CREATED,
    sequence: int = 1,
    fill: bool = False,
) -> BrokerCorrelation:
    return BrokerCorrelation(
        1,
        sequence,
        status,
        correlated_at,
        broker_order_sequence=None if fill else f"broker-{sequence}",
        broker_fill_id=f"fill-{sequence}" if fill else None,
        client_order_id=order_id,
    )


def _open_order(
    order_id: str = "order-1",
    *,
    observation_id: str | None = None,
    correlated_order_id: str | None = None,
    instrument_id: str | None = None,
    side: LiveSide = LiveSide.BUY,
    total: Decimal = Decimal("2"),
    remaining: Decimal = Decimal("2"),
    price: Decimal | None = Decimal("22000"),
    correlated_at: datetime = CREATED,
    observed_at: datetime = OBSERVED,
    sequence: int = 1,
) -> BrokerOpenOrderObservation:
    return BrokerOpenOrderObservation(
        observation_id or f"open-{order_id}-{sequence}",
        ACCOUNT,
        instrument_id or f"instrument-{order_id}",
        side,
        total,
        remaining,
        price,
        _correlation(
            correlated_order_id or order_id,
            correlated_at=correlated_at,
            sequence=sequence,
        ),
        observed_at,
    )


def _fill(order_id: str = "order-1") -> BrokerFillObservation:
    return BrokerFillObservation(
        f"fill-observation-{order_id}",
        ACCOUNT,
        f"instrument-{order_id}",
        LiveSide.BUY,
        Decimal("1"),
        Decimal("22000"),
        _correlation(order_id, fill=True),
        OBSERVED,
        OBSERVED,
    )


def _assessment(
    *,
    orders: tuple[LiveOrder, ...] = (),
    open_orders: tuple[BrokerOpenOrderObservation, ...] = (),
    fills: tuple[BrokerFillObservation, ...] = (),
    blockers: tuple[str, ...] = (),
    journal_sequence: int = 9,
    open_status: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
    fill_status: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
    local_as_of: datetime = CREATED,
) -> ReconciliationAssessment:
    local = LocalReconciliationSnapshot(
        ACCOUNT,
        orders,
        (),
        (),
        local_as_of,
        blockers,
        journal_sequence,
    )
    broker = BrokerReconciliationSnapshot(
        SNAPSHOT,
        ACCOUNT,
        OpenOrdersSnapshot(open_orders, _evidence(EvidenceQueryKind.OPEN_ORDERS, open_status)),
        BrokerFillsSnapshot(fills, _evidence(EvidenceQueryKind.FILLS, fill_status)),
        BrokerPositionsSnapshot((), _evidence(EvidenceQueryKind.POSITIONS)),
        OBSERVED,
    )
    return assess_reconciliation(local, broker, RECONCILED)


def _reasons(assessment: ReconciliationAssessment) -> set[OrderProjectionReason]:
    return set(project_authoritative_orders(assessment).reasons)


def test_supported_projection_preserves_intent_and_quantities() -> None:
    order = _pending_order()
    assessment = _assessment(orders=(order,), open_orders=(_open_order(),))

    plan = project_authoritative_orders(assessment)

    assert plan.disposition is OrderProjectionDisposition.READY
    assert plan.expected_journal_sequence == 9
    assert tuple((item.client_order_id, item.version) for item in plan.expected_order_versions) == (
        ("order-1", 3),
    )
    assert plan.consumed_discrepancy_ids == tuple(
        sorted(item.discrepancy_id for item in assessment.result.discrepancies)
    )
    assert len(plan.projected_orders) == 1
    projected = plan.projected_orders[0]
    assert projected.intent is order.intent
    assert projected.state is LiveOrderState.ACCEPTED
    assert projected.total_quantity == order.total_quantity
    assert projected.filled_quantity == 0
    assert projected.remaining_quantity == order.remaining_quantity
    assert projected.average_fill_price is None
    assert projected.working_limit_price == order.working_limit_price
    assert projected.version == order.version + 1
    assert projected.accepted_at == projected.updated_at == OBSERVED
    assert projected.pending_command is None
    assert plan.may_commit and not plan.may_dispatch
    assert assessment.local_snapshot.orders[0] is order
    assert order.pending_command is not None


def test_market_new_requires_market_broker_evidence() -> None:
    order = _pending_order(order_type=LiveOrderType.MARKET, limit_price=None)

    supported = project_authoritative_orders(
        _assessment(orders=(order,), open_orders=(_open_order(price=None),))
    )
    mismatched = project_authoritative_orders(
        _assessment(orders=(order,), open_orders=(_open_order(price=Decimal("22000")),))
    )

    assert supported.disposition is OrderProjectionDisposition.READY
    assert supported.projected_orders[0].working_limit_price is None
    assert mismatched.disposition is OrderProjectionDisposition.UNSUPPORTED
    assert OrderProjectionReason.BROKER_PRICE_MISMATCH in mismatched.reasons
    assert mismatched.projected_orders == ()


def test_multi_order_projection_and_input_permutations_are_canonical() -> None:
    orders = (_pending_order("order-2", version=7), _pending_order("order-1", version=2))
    broker_orders = (_open_order("order-2", sequence=2), _open_order("order-1"))
    plans = {
        project_authoritative_orders(
            _assessment(orders=tuple(order_perm), open_orders=tuple(broker_perm))
        )
        for order_perm in permutations(orders)
        for broker_perm in permutations(broker_orders)
    }

    assert len(plans) == 1
    plan = plans.pop()
    assert [item.client_order_id for item in plan.expected_order_versions] == [
        "order-1",
        "order-2",
    ]
    assert [item.intent.client_order_id for item in plan.projected_orders] == [
        "order-1",
        "order-2",
    ]
    assert plan == project_authoritative_orders(
        _assessment(orders=orders, open_orders=broker_orders)
    )


def test_authoritative_clean_assessment_is_no_change() -> None:
    assessment = _assessment(
        orders=(_accepted_order(),),
        open_orders=(_open_order(),),
    )

    plan = project_authoritative_orders(assessment)

    assert plan.disposition is OrderProjectionDisposition.NO_CHANGE
    assert not plan.may_commit
    assert not plan.may_dispatch


def test_clean_assessment_with_recovery_blocker_is_not_no_change() -> None:
    assessment = _assessment(blockers=("claim-1",))

    plan = project_authoritative_orders(assessment)

    assert plan.disposition is OrderProjectionDisposition.UNSUPPORTED
    assert plan.reasons == (OrderProjectionReason.UNSUPPORTED_DISCREPANCY,)


def test_forged_result_is_not_authoritative() -> None:
    assessment = _assessment(
        orders=(_pending_order(),),
        open_orders=(_open_order(),),
    )
    forged = ReconciliationAssessment(
        assessment.local_snapshot,
        assessment.broker_snapshot,
        replace(assessment.result, reconciled_at=RECONCILED + timedelta(seconds=1)),
    )

    plan = project_authoritative_orders(forged)

    assert plan.disposition is OrderProjectionDisposition.NOT_AUTHORITATIVE
    assert plan.reasons == (OrderProjectionReason.ASSESSMENT_MISMATCH,)


@pytest.mark.parametrize(
    ("status", "expected"),
    (
        (
            EvidenceCompleteness.INCOMPLETE,
            {
                OrderProjectionReason.INCOMPLETE_EVIDENCE,
                OrderProjectionReason.NOT_AUTHORITATIVE,
            },
        ),
        (
            EvidenceCompleteness.UNKNOWN,
            {
                OrderProjectionReason.INCOMPLETE_EVIDENCE,
                OrderProjectionReason.NOT_AUTHORITATIVE,
            },
        ),
    ),
)
def test_incomplete_evidence_is_not_authoritative(
    status: EvidenceCompleteness, expected: set[OrderProjectionReason]
) -> None:
    assessment = _assessment(open_status=status)

    plan = project_authoritative_orders(assessment)

    assert plan.disposition is OrderProjectionDisposition.NOT_AUTHORITATIVE
    assert set(plan.reasons) == expected


def test_ambiguous_raw_correlation_is_not_authoritative() -> None:
    pending = _pending_order()
    candidate = replace(
        _open_order(),
        correlation=_correlation("order-1", status=CorrelationStatus.CANDIDATE),
    )
    assessment = _assessment(orders=(pending,), open_orders=(candidate,))

    plan = project_authoritative_orders(assessment)

    assert plan.disposition is OrderProjectionDisposition.NOT_AUTHORITATIVE
    assert set(plan.reasons) == {
        OrderProjectionReason.AMBIGUOUS_EVIDENCE,
        OrderProjectionReason.NOT_AUTHORITATIVE,
    }


def test_duplicate_raw_matches_are_not_silently_deduplicated() -> None:
    first = _open_order(observation_id="open-1")
    duplicate = replace(first, observation_id="open-2")
    assessment = _assessment(
        orders=(_pending_order(),),
        open_orders=(first, duplicate),
    )

    plan = project_authoritative_orders(assessment)

    assert plan.disposition is OrderProjectionDisposition.UNSUPPORTED
    assert plan.reasons == (OrderProjectionReason.MULTIPLE_BROKER_MATCHES,)


@pytest.mark.parametrize(
    ("broker", "reason"),
    (
        (None, OrderProjectionReason.MISSING_BROKER_MATCH),
        (
            _open_order(instrument_id="wrong-instrument"),
            OrderProjectionReason.BROKER_IDENTITY_MISMATCH,
        ),
        (
            _open_order(side=LiveSide.SELL),
            OrderProjectionReason.BROKER_IDENTITY_MISMATCH,
        ),
        (
            _open_order(total=Decimal("3"), remaining=Decimal("3")),
            OrderProjectionReason.BROKER_QUANTITY_MISMATCH,
        ),
        (
            _open_order(remaining=Decimal("1")),
            OrderProjectionReason.BROKER_QUANTITY_MISMATCH,
        ),
        (
            _open_order(price=Decimal("22001")),
            OrderProjectionReason.BROKER_PRICE_MISMATCH,
        ),
        (
            _open_order(correlated_at=OBSERVED + timedelta(microseconds=1)),
            OrderProjectionReason.BROKER_TIME_MISMATCH,
        ),
    ),
)
def test_broker_match_dimensions_fail_closed(
    broker: BrokerOpenOrderObservation | None,
    reason: OrderProjectionReason,
) -> None:
    assessment = _assessment(
        orders=(_pending_order(),),
        open_orders=() if broker is None else (broker,),
    )

    plan = project_authoritative_orders(assessment)

    assert plan.disposition is OrderProjectionDisposition.UNSUPPORTED
    assert reason in plan.reasons
    assert plan.projected_orders == ()


def test_broker_observation_must_not_predate_local_snapshot_cut() -> None:
    stale_observation = _open_order(
        observed_at=CREATED + timedelta(milliseconds=500),
    )
    assessment = _assessment(
        orders=(_pending_order(),),
        open_orders=(stale_observation,),
        local_as_of=OBSERVED,
    )

    plan = project_authoritative_orders(assessment)

    assert plan.disposition is OrderProjectionDisposition.UNSUPPORTED
    assert plan.reasons == (OrderProjectionReason.BROKER_TIME_MISMATCH,)
    assert plan.projected_orders == ()


@pytest.mark.parametrize(
    ("order", "broker"),
    (
        (
            replace(
                _pending_order(),
                total_quantity=Decimal("1"),
                remaining_quantity=Decimal("1"),
            ),
            _open_order(total=Decimal("1"), remaining=Decimal("1")),
        ),
        (
            replace(_pending_order(), working_limit_price=Decimal("22001")),
            _open_order(price=Decimal("22001")),
        ),
    ),
)
def test_local_working_values_must_match_bound_new_intent(
    order: LiveOrder,
    broker: BrokerOpenOrderObservation,
) -> None:
    plan = project_authoritative_orders(_assessment(orders=(order,), open_orders=(broker,)))

    assert plan.disposition is OrderProjectionDisposition.UNSUPPORTED
    assert OrderProjectionReason.UNSUPPORTED_LOCAL_STATE in plan.reasons
    assert plan.projected_orders == ()


def test_wrong_client_id_is_never_guessed() -> None:
    assessment = _assessment(
        orders=(_pending_order(),),
        open_orders=(_open_order(correlated_order_id="other-order"),),
    )

    plan = project_authoritative_orders(assessment)

    assert plan.disposition is OrderProjectionDisposition.UNSUPPORTED
    assert OrderProjectionReason.MISSING_BROKER_MATCH in plan.reasons
    assert OrderProjectionReason.UNSUPPORTED_DISCREPANCY in plan.reasons


def test_any_correlated_fill_evidence_is_unsupported() -> None:
    assessment = _assessment(
        orders=(_pending_order(),),
        open_orders=(_open_order(),),
        fills=(_fill(),),
    )

    assert OrderProjectionReason.FILL_EVIDENCE_UNSUPPORTED in _reasons(assessment)


@pytest.mark.parametrize(
    ("order", "reason"),
    (
        (
            _pending_order(state=LiveOrderState.SUBMITTING),
            OrderProjectionReason.UNSUPPORTED_LOCAL_STATE,
        ),
        (
            _pending_order(state=LiveOrderState.RECONCILING, new_command=False),
            OrderProjectionReason.UNSUPPORTED_COMMAND,
        ),
    ),
)
def test_unsupported_local_state_or_command(
    order: LiveOrder, reason: OrderProjectionReason
) -> None:
    assessment = _assessment(orders=(order,), open_orders=(_open_order(),))

    plan = project_authoritative_orders(assessment)

    assert plan.disposition is OrderProjectionDisposition.UNSUPPORTED
    assert reason in plan.reasons


def test_broker_authoritative_order_is_never_regressed_by_pending_binding() -> None:
    pending = _pending_order()
    already_accepted = replace(
        pending,
        state=LiveOrderState.ACCEPTED,
        accepted_at=CREATED,
    )
    assessment = _assessment(
        orders=(already_accepted,),
        open_orders=(_open_order(),),
    )

    plan = project_authoritative_orders(assessment)

    assert plan.disposition is OrderProjectionDisposition.UNSUPPORTED
    assert OrderProjectionReason.UNSUPPORTED_LOCAL_STATE in plan.reasons


def test_unconsumed_non_correlation_discrepancy_is_unsupported() -> None:
    assessment = _assessment(orders=(_accepted_order(),))

    plan = project_authoritative_orders(assessment)

    assert plan.disposition is OrderProjectionDisposition.UNSUPPORTED
    assert plan.reasons == (OrderProjectionReason.UNSUPPORTED_DISCREPANCY,)
    assert plan.unsupported_discrepancy_ids == tuple(
        item.discrepancy_id for item in assessment.result.discrepancies
    )


def test_valid_unsupported_data_returns_a_plan_instead_of_raising() -> None:
    assessments = (
        _assessment(orders=(_pending_order(),)),
        _assessment(
            orders=(_pending_order(state=LiveOrderState.RECONCILING, new_command=False),),
            open_orders=(_open_order(),),
        ),
        _assessment(orders=(_accepted_order(),)),
    )

    assert all(
        project_authoritative_orders(item).disposition is OrderProjectionDisposition.UNSUPPORTED
        for item in assessments
    )
