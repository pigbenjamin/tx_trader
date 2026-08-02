from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Context, Decimal, localcontext
from itertools import permutations

import pytest

from tx_trade.orders.live_contracts import (
    BrokerCorrelation,
    BrokerFillObservation,
    BrokerOpenOrderObservation,
    BrokerPosition,
    CancelOrderCommand,
    CorrelationStatus,
    FingerprintDomain,
    LiveFill,
    LiveOrder,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    PendingCommandBinding,
    ReconciliationKind,
    StrategyPositionAttribution,
    payload_fingerprint,
)
from tx_trade.orders.live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    CompletenessEvidence,
    EvidenceCompleteness,
    EvidenceQueryKind,
    OpenOrdersSnapshot,
    ReconciliationPort,
    ReconciliationStatus,
)
from tx_trade.orders.live_reconciliation import (
    FakeOnlyReconciliationService,
    assess_reconciliation,
)
from tx_trade.orders.live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    LocalReconciliationSnapshot,
)

NOW = datetime(2026, 8, 2, 1, tzinfo=timezone.utc)
LATER = NOW + timedelta(seconds=1)


def evidence(
    kind: EvidenceQueryKind,
    status: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
) -> CompletenessEvidence:
    return CompletenessEvidence(
        kind,
        "account-1",
        status,
        NOW,
        "snapshot-1",
        None if status is EvidenceCompleteness.COMPLETE else "partial-query",
    )


def correlation(
    client_order_id: str | None = "order-1",
    status: CorrelationStatus = CorrelationStatus.CONFIRMED,
    *,
    fill_id: str | None = None,
    execution_no: str | None = None,
) -> BrokerCorrelation:
    return BrokerCorrelation(
        1,
        1,
        status,
        NOW,
        broker_order_sequence="sequence-1" if fill_id is None and execution_no is None else None,
        broker_fill_id=fill_id,
        execution_no=execution_no,
        client_order_id=client_order_id,
    )


def local_order(
    state: LiveOrderState = LiveOrderState.ACCEPTED,
    *,
    remaining: Decimal = Decimal("2"),
) -> LiveOrder:
    intent = LiveOrderIntent(
        "strategy-1",
        "order-1",
        "account-1",
        "TXF-202608",
        LiveSide.BUY,
        Decimal("2"),
        LiveOrderType.LIMIT,
        Decimal("22000"),
        LiveTimeInForce.DAY,
        False,
        NOW,
    )
    filled = Decimal("2") - remaining
    return LiveOrder(
        intent,
        state,
        Decimal("2"),
        filled,
        remaining,
        Decimal("22000") if filled else None,
        Decimal("22000"),
        1,
        NOW,
        NOW,
    )


def broker_order(
    *,
    corr: BrokerCorrelation | None = None,
    remaining: Decimal = Decimal("2"),
    observation_id: str = "open-1",
) -> BrokerOpenOrderObservation:
    return BrokerOpenOrderObservation(
        observation_id,
        "account-1",
        "TXF-202608",
        LiveSide.BUY,
        Decimal("2"),
        remaining,
        Decimal("22000"),
        corr or correlation(),
        NOW,
    )


def broker_fill(
    *,
    observation_id: str = "fill-observation-1",
    instrument_id: str = "TXF-202608",
    client_order_id: str = "order-1",
    fill_id: str | None = "fill-1",
    execution_no: str | None = None,
    quantity: Decimal = Decimal("1"),
    execution_price: Decimal = Decimal("22000"),
    occurred_at: datetime | None = NOW,
) -> BrokerFillObservation:
    return BrokerFillObservation(
        observation_id,
        "account-1",
        instrument_id,
        LiveSide.BUY,
        quantity,
        execution_price,
        correlation(
            client_order_id,
            fill_id=fill_id,
            execution_no=execution_no,
        ),
        NOW,
        occurred_at,
    )


def pending_order(state: LiveOrderState) -> LiveOrder:
    base = local_order()
    if state is LiveOrderState.CANCEL_PENDING:
        command = CancelOrderCommand("cancel-command-1", "order-1", NOW)
        domain = FingerprintDomain.CANCEL_COMMAND_V1
    else:
        command = NewOrderCommand("new-command-1", base.intent, NOW)
        domain = FingerprintDomain.NEW_COMMAND_V1
    binding = PendingCommandBinding(command, payload_fingerprint(command, domain))
    return replace(base, state=state, pending_command=binding)


def filled_bundle(
    strategy_id: str,
    order_id: str,
    fill_id: str,
    side: LiveSide,
    quantity: Decimal,
    *,
    occurred_at: datetime = NOW,
) -> tuple[LiveOrder, LiveFill, StrategyPositionAttribution]:
    intent = LiveOrderIntent(
        strategy_id,
        order_id,
        "account-1",
        "TXF-202608",
        side,
        quantity,
        LiveOrderType.LIMIT,
        Decimal("22000"),
        LiveTimeInForce.DAY,
        False,
        NOW,
    )
    order = LiveOrder(
        intent,
        LiveOrderState.FILLED,
        quantity,
        quantity,
        Decimal(0),
        Decimal("22000"),
        Decimal("22000"),
        1,
        NOW,
        NOW,
    )
    fill = LiveFill(
        fill_id,
        order_id,
        strategy_id,
        "account-1",
        "TXF-202608",
        side,
        quantity,
        Decimal("22000"),
        occurred_at,
    )
    attributed = quantity if side is LiveSide.BUY else quantity.copy_negate()
    attribution = StrategyPositionAttribution(
        strategy_id, "account-1", "TXF-202608", attributed, NOW
    )
    return order, fill, attribution


def snapshots(
    *,
    orders: tuple[LiveOrder, ...] = (),
    local_fills: tuple[LiveFill, ...] = (),
    attributions: tuple[StrategyPositionAttribution, ...] = (),
    open_orders: tuple[BrokerOpenOrderObservation, ...] = (),
    broker_fills: tuple[BrokerFillObservation, ...] = (),
    positions: tuple[BrokerPosition, ...] = (),
    open_status: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
    fill_status: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
    position_status: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
) -> tuple[LocalReconciliationSnapshot, BrokerReconciliationSnapshot]:
    local = LocalReconciliationSnapshot("account-1", orders, local_fills, attributions, NOW)
    broker = BrokerReconciliationSnapshot(
        "snapshot-1",
        "account-1",
        OpenOrdersSnapshot(open_orders, evidence(EvidenceQueryKind.OPEN_ORDERS, open_status)),
        BrokerFillsSnapshot(broker_fills, evidence(EvidenceQueryKind.FILLS, fill_status)),
        BrokerPositionsSnapshot(positions, evidence(EvidenceQueryKind.POSITIONS, position_status)),
        NOW,
    )
    return local, broker


def kinds(assessment: object) -> set[ReconciliationKind]:
    return {item.kind for item in assessment.result.discrepancies}  # type: ignore[attr-defined]


def test_matching_complete_snapshots_are_authoritative_without_granting_dispatch() -> None:
    local, broker = snapshots(orders=(local_order(),), open_orders=(broker_order(),))

    assessment = assess_reconciliation(local, broker, LATER)

    assert assessment.result.status is ReconciliationStatus.COMPLETE
    assert assessment.result.discrepancies == ()
    assert assessment.may_resume
    assert not assessment.may_dispatch
    assert assess_reconciliation(local, broker, LATER) == assessment


def test_incomplete_open_evidence_does_not_infer_absence() -> None:
    local, broker = snapshots(orders=(local_order(),), open_status=EvidenceCompleteness.INCOMPLETE)

    assessment = assess_reconciliation(local, broker, LATER)

    assert assessment.result.status is ReconciliationStatus.INCOMPLETE
    assert ReconciliationKind.MISSING_BROKER_ORDER not in kinds(assessment)
    assert not assessment.may_resume


def test_candidate_is_ambiguous_and_never_guesses_its_candidate_local_id() -> None:
    candidate = correlation("order-1", CorrelationStatus.CANDIDATE)
    local, broker = snapshots(orders=(local_order(),), open_orders=(broker_order(corr=candidate),))

    assessment = assess_reconciliation(local, broker, LATER)

    assert assessment.result.status is ReconciliationStatus.AMBIGUOUS
    discrepancy = next(
        item
        for item in assessment.result.discrepancies
        if item.kind is ReconciliationKind.CORRELATION_MISSING
    )
    assert discrepancy.client_order_id is None
    assert ReconciliationKind.MISSING_BROKER_ORDER not in kinds(assessment)


@pytest.mark.parametrize(
    "state",
    [
        LiveOrderState.SUBMITTING,
        LiveOrderState.SUBMISSION_UNKNOWN,
        LiveOrderState.RECONCILING,
        LiveOrderState.CANCEL_PENDING,
    ],
)
@pytest.mark.parametrize("broker_row_present", [False, True])
def test_pending_binding_never_allows_resume_from_read_only_assessment(
    state: LiveOrderState,
    broker_row_present: bool,
) -> None:
    open_orders = (broker_order(),) if broker_row_present else ()
    local, broker = snapshots(orders=(pending_order(state),), open_orders=open_orders)

    assessment = assess_reconciliation(local, broker, LATER)

    assert not assessment.may_resume
    assert tuple(item.kind for item in assessment.result.discrepancies) == (
        ReconciliationKind.CORRELATION_MISSING,
    )
    assert assessment.result.discrepancies[0].client_order_id == "order-1"


def test_positive_order_mismatch_and_complete_absence_are_reported() -> None:
    local, broker = snapshots(
        orders=(local_order(),),
        open_orders=(broker_order(remaining=Decimal("1")),),
    )
    quantity = assess_reconciliation(local, broker, LATER)
    assert ReconciliationKind.QUANTITY_MISMATCH in kinds(quantity)

    missing_local, missing_broker = snapshots(open_orders=(broker_order(),))
    assert ReconciliationKind.MISSING_LOCAL_ORDER in kinds(
        assess_reconciliation(missing_local, missing_broker, LATER)
    )

    absent_local, absent_broker = snapshots(orders=(local_order(),))
    assert ReconciliationKind.MISSING_BROKER_ORDER in kinds(
        assess_reconciliation(absent_local, absent_broker, LATER)
    )


def test_fill_price_conflict_is_positive_even_with_incomplete_fill_evidence() -> None:
    order, local_fill, attribution = filled_bundle(
        "strategy-1", "order-1", "fill-1", LiveSide.BUY, Decimal("1")
    )
    broker_fill = BrokerFillObservation(
        "fill-observation-1",
        "account-1",
        "TXF-202608",
        LiveSide.BUY,
        Decimal("1"),
        Decimal("22001"),
        correlation(fill_id="fill-1"),
        NOW,
        NOW,
    )
    local, broker = snapshots(
        orders=(order,),
        local_fills=(local_fill,),
        attributions=(attribution,),
        broker_fills=(broker_fill,),
        fill_status=EvidenceCompleteness.INCOMPLETE,
    )

    assert ReconciliationKind.FILL_MISMATCH in kinds(assess_reconciliation(local, broker, LATER))


def test_positions_are_summed_from_attributions_and_not_inferred_from_fills() -> None:
    buy = filled_bundle("strategy-1", "order-1", "fill-1", LiveSide.BUY, Decimal("2"))
    sell = filled_bundle("strategy-2", "order-2", "fill-2", LiveSide.SELL, Decimal("1"))
    position = BrokerPosition("account-1", "TXF-202608", Decimal("2"), Decimal("22000"), NOW)
    local, broker = snapshots(
        orders=(buy[0], sell[0]),
        local_fills=(buy[1], sell[1]),
        attributions=(buy[2], sell[2]),
        positions=(position,),
        fill_status=EvidenceCompleteness.INCOMPLETE,
    )

    mismatch = next(
        item
        for item in assess_reconciliation(local, broker, LATER).result.discrepancies
        if item.kind is ReconciliationKind.POSITION_MISMATCH
    )
    assert (mismatch.expected_quantity, mismatch.actual_quantity) == (
        Decimal("1"),
        Decimal("2"),
    )


def test_duplicates_permutations_are_stable_and_conflicting_identity_is_ambiguous() -> None:
    first = broker_order()
    duplicate = replace(first, observation_id="open-2")
    local, broker_a = snapshots(orders=(local_order(),), open_orders=(first, duplicate, first))
    _, broker_b = snapshots(orders=(local_order(),), open_orders=(first, duplicate))

    assert (
        assess_reconciliation(local, broker_a, LATER).result
        == assess_reconciliation(local, broker_b, LATER).result
    )

    conflict = replace(first, working_remaining_quantity=Decimal("1"))
    _, conflicted = snapshots(orders=(local_order(),), open_orders=(first, conflict))
    assessment = assess_reconciliation(local, conflicted, LATER)
    assert assessment.result.status is ReconciliationStatus.AMBIGUOUS


def test_decimal_canonicalization_ignores_trailing_zeros_and_context() -> None:
    one_a = filled_bundle("strategy-1", "order-1", "fill-1", LiveSide.BUY, Decimal("1.0"))
    one_b = filled_bundle("strategy-1", "order-1", "fill-1", LiveSide.BUY, Decimal("1.00"))
    position_a = BrokerPosition("account-1", "TXF-202608", Decimal("2.0"), Decimal("22000.0"), NOW)
    position_b = BrokerPosition(
        "account-1", "TXF-202608", Decimal("2.00"), Decimal("22000.00"), NOW
    )
    local_a, broker_a = snapshots(
        orders=(one_a[0],),
        local_fills=(one_a[1],),
        attributions=(one_a[2],),
        positions=(position_a,),
        fill_status=EvidenceCompleteness.INCOMPLETE,
    )
    local_b, broker_b = snapshots(
        orders=(one_b[0],),
        local_fills=(one_b[1],),
        attributions=(one_b[2],),
        positions=(position_b,),
        fill_status=EvidenceCompleteness.INCOMPLETE,
    )

    results = []
    for precision in (6, 28, 80):
        with localcontext(Context(prec=precision)):
            results.append(assess_reconciliation(local_a, broker_a, LATER).result)
            results.append(assess_reconciliation(local_b, broker_b, LATER).result)

    assert all(result == results[0] for result in results)
    assert len(results[0].discrepancies) == 1


def test_position_sum_is_exact_across_contexts_and_attribution_permutations() -> None:
    with localcontext(Context(prec=50)):
        buy = filled_bundle(
            "strategy-1",
            "order-1",
            "fill-1",
            LiveSide.BUY,
            Decimal("9999999999999999999999999999999999"),
        )
        sell = filled_bundle(
            "strategy-2",
            "order-2",
            "fill-2",
            LiveSide.SELL,
            Decimal("9999999999999999999999999999999998"),
        )
        position = BrokerPosition("account-1", "TXF-202608", Decimal("2"), Decimal("22000"), NOW)
        snapshot_pairs = [
            snapshots(
                orders=(buy[0], sell[0]),
                local_fills=(buy[1], sell[1]),
                attributions=attributions,
                positions=(position,),
                fill_status=EvidenceCompleteness.INCOMPLETE,
            )
            for attributions in permutations((buy[2], sell[2]))
        ]

    results = []
    for precision in (6, 28, 80):
        with localcontext(Context(prec=precision)):
            results.extend(
                assess_reconciliation(local, broker, LATER).result
                for local, broker in snapshot_pairs
            )

    assert all(result == results[0] for result in results)
    mismatch = next(
        item
        for item in results[0].discrepancies
        if item.kind is ReconciliationKind.POSITION_MISMATCH
    )
    assert mismatch.expected_quantity == Decimal("1")
    assert mismatch.actual_quantity == Decimal("2")


def test_position_sum_exceeding_supported_bound_fails_closed() -> None:
    with localcontext(Context(prec=50)):
        first = filled_bundle(
            "strategy-1",
            "order-1",
            "fill-1",
            LiveSide.BUY,
            Decimal("9999999999999999999999999999999999"),
        )
        second = filled_bundle(
            "strategy-2",
            "order-2",
            "fill-2",
            LiveSide.BUY,
            Decimal("9999999999999999999999999999999999"),
        )
        local, broker = snapshots(
            orders=(first[0], second[0]),
            local_fills=(first[1], second[1]),
            attributions=(first[2], second[2]),
            fill_status=EvidenceCompleteness.INCOMPLETE,
        )

    with pytest.raises(ValueError, match="exact sum exceeds supported Decimal bounds"):
        assess_reconciliation(local, broker, LATER)


def test_fill_identity_namespaces_aliases_conflicts_and_occurred_at() -> None:
    order, local_fill, attribution = filled_bundle(
        "strategy-1", "order-1", "shared", LiveSide.BUY, Decimal("1")
    )
    position = BrokerPosition("account-1", "TXF-202608", Decimal("1"), Decimal("22000"), NOW)
    common = dict(
        orders=(order,),
        local_fills=(local_fill,),
        attributions=(attribution,),
        positions=(position,),
    )

    shared_token_observations = (
        broker_fill(observation_id="fill-by-broker", fill_id="shared"),
        broker_fill(
            observation_id="fill-by-execution",
            fill_id=None,
            execution_no="shared",
        ),
    )
    collision_assessments = []
    for ordering in permutations(shared_token_observations):
        local, distinct_namespaces = snapshots(**common, broker_fills=ordering)
        collision_assessments.append(assess_reconciliation(local, distinct_namespaces, LATER))
    collision_results = [assessment.result for assessment in collision_assessments]
    assert all(result == collision_results[0] for result in collision_results)
    assert all(not assessment.may_resume for assessment in collision_assessments)
    assert collision_results[0].status is ReconciliationStatus.AMBIGUOUS
    assert {item.kind for item in collision_results[0].discrepancies} == {
        ReconciliationKind.FILL_MISMATCH
    }

    _, alias = snapshots(
        **common,
        broker_fills=(broker_fill(fill_id="broker-alias", execution_no="shared"),),
    )
    assert not assess_reconciliation(local, alias, LATER).result.discrepancies

    _, conflict = snapshots(
        **common,
        broker_fills=(
            broker_fill(observation_id="fill-a", fill_id="shared"),
            broker_fill(
                observation_id="fill-b",
                fill_id="shared",
                execution_price=Decimal("22001"),
            ),
        ),
    )
    assert assess_reconciliation(local, conflict, LATER).result.status is (
        ReconciliationStatus.AMBIGUOUS
    )

    _, unknown_time = snapshots(
        **common,
        broker_fills=(broker_fill(fill_id="shared", occurred_at=None),),
    )
    assert not assess_reconciliation(local, unknown_time, LATER).result.discrepancies

    _, wrong_time = snapshots(
        **common,
        broker_fills=(
            broker_fill(
                fill_id="shared",
                occurred_at=NOW - timedelta(microseconds=1),
            ),
        ),
    )
    assert kinds(assess_reconciliation(local, wrong_time, LATER)) == {
        ReconciliationKind.FILL_MISMATCH
    }


@pytest.mark.parametrize("source", ["open", "fill", "position"])
@pytest.mark.parametrize("case", ["normal", "duplicate", "conflict"])
def test_broker_observation_permutations_are_stable(source: str, case: str) -> None:
    open_first = broker_order()
    open_second = broker_order(corr=correlation("order-2"), observation_id="open-2")
    fill_first = broker_fill()
    fill_second = broker_fill(
        observation_id="fill-observation-2",
        instrument_id="MXF-202608",
        client_order_id="order-2",
        fill_id="fill-2",
    )
    position_first = BrokerPosition("account-1", "TXF-202608", Decimal("1"), Decimal("22000"), NOW)
    position_second = BrokerPosition("account-1", "MXF-202608", Decimal("2"), Decimal("12000"), NOW)
    values_by_source: dict[str, dict[str, tuple[object, ...]]] = {
        "open": {
            "normal": (open_first, open_second),
            "duplicate": (open_first, replace(open_first, observation_id="open-3"), open_first),
            "conflict": (
                open_first,
                replace(open_first, working_remaining_quantity=Decimal("1")),
                open_second,
            ),
        },
        "fill": {
            "normal": (fill_first, fill_second),
            "duplicate": (
                fill_first,
                replace(fill_first, observation_id="fill-observation-3"),
                fill_first,
            ),
            "conflict": (
                fill_first,
                replace(fill_first, execution_price=Decimal("22001")),
                fill_second,
            ),
        },
        "position": {
            "normal": (position_first, position_second),
            "duplicate": (position_first, position_first, position_second),
            "conflict": (
                position_first,
                replace(position_first, net_quantity=Decimal("2")),
                position_second,
            ),
        },
    }
    values = values_by_source[source][case]
    results = []
    for ordering in permutations(values):
        parameter = {
            "open": "open_orders",
            "fill": "broker_fills",
            "position": "positions",
        }[source]
        kwargs = {parameter: ordering}
        local, broker = snapshots(**kwargs)  # type: ignore[arg-type]
        results.append(assess_reconciliation(local, broker, LATER).result)

    assert all(result == results[0] for result in results)


def test_service_queries_each_source_once_and_implements_reconciliation_port() -> None:
    local, broker = snapshots()

    class LocalSource:
        calls = 0

        def load_account_snapshot(self, account_id: str) -> LocalReconciliationSnapshot:
            assert account_id == "account-1"
            self.calls += 1
            return local

    class BrokerSnapshotSource:
        calls = 0

        def query_reconciliation_snapshot(self, account_id: str) -> BrokerReconciliationSnapshot:
            assert account_id == "account-1"
            self.calls += 1
            return broker

    class Clock:
        calls = 0

        def now(self) -> datetime:
            self.calls += 1
            return LATER

    source = LocalSource()
    broker_source = BrokerSnapshotSource()
    clock = Clock()
    service = FakeOnlyReconciliationService(source, broker_source, clock)

    assert isinstance(service, ReconciliationPort)
    assert service.reconcile("account-1").status is ReconciliationStatus.COMPLETE
    assert source.calls == 1
    assert broker_source.calls == 1
    assert clock.calls == 1


@pytest.mark.parametrize(
    "bad_time",
    [NOW - timedelta(seconds=1), NOW.astimezone(timezone(timedelta(hours=8)))],
)
def test_explicit_reconciliation_time_must_be_utc_and_not_predate_snapshots(
    bad_time: datetime,
) -> None:
    local, broker = snapshots()
    with pytest.raises(ValueError):
        assess_reconciliation(local, broker, bad_time)
