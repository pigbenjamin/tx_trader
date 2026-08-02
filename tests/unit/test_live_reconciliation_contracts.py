from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from itertools import permutations

import pytest

from tx_trade.orders.live_contracts import (
    BrokerCorrelation,
    BrokerFillObservation,
    BrokerOpenOrderObservation,
    BrokerPosition,
    CorrelationStatus,
    LiveFill,
    LiveOrder,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    ReconciliationDiscrepancy,
    ReconciliationKind,
    StrategyPositionAttribution,
)
from tx_trade.orders.live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    CompletenessEvidence,
    EvidenceCompleteness,
    EvidenceQueryKind,
    OpenOrdersSnapshot,
    ReconciliationResult,
    ReconciliationStatus,
)
from tx_trade.orders.live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    BrokerReconciliationSnapshotSourcePort,
    LocalReconciliationSnapshot,
    LocalReconciliationSourcePort,
    ReconciliationAssessment,
    exact_decimal_sum,
)

NOW = datetime(2026, 8, 2, tzinfo=timezone.utc)


def local_order(
    *,
    account_id: str = "account-1",
    order_id: str = "order-1",
    strategy_id: str = "strategy-1",
    instrument_id: str = "TXF-202608",
    side: LiveSide = LiveSide.BUY,
    filled_quantity: Decimal = Decimal("1"),
) -> LiveOrder:
    intent = LiveOrderIntent(
        strategy_id,
        order_id,
        account_id,
        instrument_id,
        side,
        Decimal("2"),
        LiveOrderType.LIMIT,
        Decimal("22000"),
        LiveTimeInForce.DAY,
        False,
        NOW,
    )
    return LiveOrder(
        intent,
        LiveOrderState.PARTIALLY_FILLED,
        Decimal("2"),
        filled_quantity,
        Decimal("2") - filled_quantity,
        Decimal("22000"),
        Decimal("22000"),
        1,
        NOW,
        NOW,
    )


def local_fill(*, account_id: str = "account-1", fill_id: str = "fill-1") -> LiveFill:
    return LiveFill(
        fill_id,
        "order-1",
        "strategy-1",
        account_id,
        "TXF-202608",
        LiveSide.BUY,
        Decimal("1"),
        Decimal("22000"),
        NOW,
    )


def attribution(
    *,
    account_id: str = "account-1",
    strategy_id: str = "strategy-1",
    instrument_id: str = "TXF-202608",
) -> StrategyPositionAttribution:
    return StrategyPositionAttribution(
        strategy_id,
        account_id,
        instrument_id,
        Decimal("1"),
        NOW,
    )


def evidence(
    kind: EvidenceQueryKind,
    *,
    account_id: str = "account-1",
    source_cursor: str = "snapshot-1",
    observed_at: datetime = NOW,
) -> CompletenessEvidence:
    return CompletenessEvidence(
        kind,
        account_id,
        EvidenceCompleteness.COMPLETE,
        observed_at,
        source_cursor,
    )


def correlation() -> BrokerCorrelation:
    return BrokerCorrelation(
        1,
        1,
        CorrelationStatus.CANDIDATE,
        NOW,
        broker_fill_id="broker-fill-1",
    )


def broker_bundle(*, account_id: str = "account-1") -> BrokerReconciliationSnapshot:
    open_order = BrokerOpenOrderObservation(
        "open-1",
        account_id,
        "TXF-202608",
        LiveSide.BUY,
        Decimal("2"),
        Decimal("1"),
        Decimal("22000"),
        correlation(),
        NOW,
    )
    fill = BrokerFillObservation(
        "broker-fill-observation-1",
        account_id,
        "TXF-202608",
        LiveSide.BUY,
        Decimal("1"),
        Decimal("22000"),
        correlation(),
        NOW,
        NOW,
    )
    position = BrokerPosition(
        account_id,
        "TXF-202608",
        Decimal("1"),
        Decimal("22000"),
        NOW,
    )
    return BrokerReconciliationSnapshot(
        "snapshot-1",
        account_id,
        OpenOrdersSnapshot(
            (open_order,), evidence(EvidenceQueryKind.OPEN_ORDERS, account_id=account_id)
        ),
        BrokerFillsSnapshot((fill,), evidence(EvidenceQueryKind.FILLS, account_id=account_id)),
        BrokerPositionsSnapshot(
            (position,), evidence(EvidenceQueryKind.POSITIONS, account_id=account_id)
        ),
        NOW + timedelta(seconds=1),
    )


def local_bundle(*, account_id: str = "account-1") -> LocalReconciliationSnapshot:
    return LocalReconciliationSnapshot(
        account_id,
        (local_order(account_id=account_id),),
        (local_fill(account_id=account_id),),
        (attribution(account_id=account_id),),
        NOW,
    )


def result_for(
    broker: BrokerReconciliationSnapshot,
    *,
    status: ReconciliationStatus = ReconciliationStatus.COMPLETE,
    discrepancies: tuple[ReconciliationDiscrepancy, ...] = (),
) -> ReconciliationResult:
    return ReconciliationResult(
        broker.account_id,
        status,
        discrepancies,
        (
            broker.open_orders.evidence,
            broker.fills.evidence,
            broker.positions.evidence,
        ),
        NOW + timedelta(seconds=2),
    )


def test_valid_snapshots_are_frozen_slotted_and_port_is_runtime_checkable() -> None:
    local = local_bundle()
    broker = broker_bundle()

    class FakeSource:
        def load_account_snapshot(self, account_id: str) -> LocalReconciliationSnapshot:
            return local

    assert isinstance(FakeSource(), LocalReconciliationSourcePort)
    assert not hasattr(local, "__dict__")
    assert not hasattr(broker, "__dict__")
    with pytest.raises(FrozenInstanceError):
        local.as_of = NOW  # type: ignore[misc]


def test_broker_snapshot_source_port_requires_atomic_bundle_query() -> None:
    broker = broker_bundle()

    class AtomicSource:
        def query_reconciliation_snapshot(self, account_id: str) -> BrokerReconciliationSnapshot:
            return broker

    class LegacySplitSource:
        def query_open_orders(self, account_id: str) -> OpenOrdersSnapshot:
            return broker.open_orders

        def query_fills(self, account_id: str) -> BrokerFillsSnapshot:
            return broker.fills

        def query_positions(self, account_id: str) -> BrokerPositionsSnapshot:
            return broker.positions

    class WrongMethodName:
        def query_snapshot(self, account_id: str) -> BrokerReconciliationSnapshot:
            return broker

    assert isinstance(AtomicSource(), BrokerReconciliationSnapshotSourcePort)
    assert not isinstance(LegacySplitSource(), BrokerReconciliationSnapshotSourcePort)
    assert not isinstance(WrongMethodName(), BrokerReconciliationSnapshotSourcePort)


def test_exact_decimal_sum_is_context_independent_exact_and_permutation_stable() -> None:
    values = (
        Decimal("1234567890123456789012345678901234"),
        Decimal("-1234567890123456789012345678901233"),
        Decimal("0E-6143"),
    )
    for precision in (6, 28, 80):
        with localcontext() as context:
            context.prec = precision
            assert {exact_decimal_sum(ordering) for ordering in permutations(values)} == {
                Decimal("1")
            }


def test_exact_decimal_sum_rejects_invalid_inputs_and_unrepresentable_output() -> None:
    with pytest.raises(TypeError, match="Decimal"):
        exact_decimal_sum((Decimal("1"), 1))  # type: ignore[arg-type]
    for invalid in (Decimal("NaN"), Decimal("Infinity"), Decimal("1E-6144")):
        with pytest.raises(ValueError):
            exact_decimal_sum((invalid,))
    with pytest.raises(ValueError, match="exact sum"):
        exact_decimal_sum((Decimal("9999999999999999999999999999999999"), Decimal("2")))
    with pytest.raises(ValueError, match="exact sum"):
        exact_decimal_sum((Decimal("9E+6144"), Decimal("1E+6144")))


@pytest.mark.parametrize("field", ["orders", "fills", "position_attributions"])
def test_local_snapshot_requires_exact_tuples_and_item_types(field: str) -> None:
    values: dict[str, object] = {
        "account_id": "account-1",
        "orders": (local_order(),),
        "fills": (local_fill(),),
        "position_attributions": (attribution(),),
        "as_of": NOW,
    }
    values[field] = []
    with pytest.raises(TypeError, match="tuple"):
        LocalReconciliationSnapshot(**values)  # type: ignore[arg-type]
    values[field] = (object(),)
    with pytest.raises(TypeError, match="contain"):
        LocalReconciliationSnapshot(**values)  # type: ignore[arg-type]


def test_local_snapshot_rejects_cross_account_duplicate_keys_and_future_items() -> None:
    with pytest.raises(ValueError, match="account"):
        LocalReconciliationSnapshot(
            "account-1", (local_order(account_id="account-2"),), (), (), NOW
        )
    with pytest.raises(ValueError, match="client_order_ids"):
        LocalReconciliationSnapshot("account-1", (local_order(), local_order()), (), (), NOW)
    with pytest.raises(ValueError, match="fill_ids"):
        LocalReconciliationSnapshot("account-1", (), (local_fill(), local_fill()), (), NOW)
    with pytest.raises(ValueError, match="attribution keys"):
        LocalReconciliationSnapshot("account-1", (), (), (attribution(), attribution()), NOW)
    future_fill = LiveFill(
        "future-fill",
        "order-1",
        "strategy-1",
        "account-1",
        "TXF-202608",
        LiveSide.BUY,
        Decimal("1"),
        Decimal("22000"),
        NOW + timedelta(seconds=1),
    )
    with pytest.raises(ValueError, match="postdate"):
        LocalReconciliationSnapshot("account-1", (), (future_fill,), (), NOW)


def test_local_snapshot_rejects_bad_account_and_non_utc_boundary() -> None:
    with pytest.raises(ValueError, match="bounded ASCII"):
        LocalReconciliationSnapshot("bad account", (), (), (), NOW)
    with pytest.raises(ValueError, match="UTC"):
        LocalReconciliationSnapshot(
            "account-1", (), (), (), NOW.astimezone(timezone(timedelta(hours=8)))
        )


def test_local_snapshot_enforces_fill_references_aggregates_and_attributions() -> None:
    order = local_order()
    fill = local_fill()
    with pytest.raises(ValueError, match="reference an order"):
        LocalReconciliationSnapshot("account-1", (), (fill,), (), NOW)

    mismatches = (
        LiveFill(
            "strategy-mismatch",
            "order-1",
            "strategy-2",
            "account-1",
            "TXF-202608",
            LiveSide.BUY,
            Decimal("1"),
            Decimal("22000"),
            NOW,
        ),
        LiveFill(
            "side-mismatch",
            "order-1",
            "strategy-1",
            "account-1",
            "TXF-202608",
            LiveSide.SELL,
            Decimal("1"),
            Decimal("22000"),
            NOW,
        ),
    )
    for mismatch in mismatches:
        with pytest.raises(ValueError, match="order intent"):
            LocalReconciliationSnapshot("account-1", (order,), (mismatch,), (), NOW)

    with pytest.raises(ValueError, match="quantity sum"):
        LocalReconciliationSnapshot("account-1", (order,), (), (), NOW)
    with pytest.raises(ValueError, match="fill projection"):
        LocalReconciliationSnapshot(
            "account-1",
            (order,),
            (fill,),
            (attribution(strategy_id="strategy-2"),),
            NOW,
        )


def test_local_snapshot_validates_recovery_blockers() -> None:
    local = local_bundle()
    blocked = LocalReconciliationSnapshot(
        local.account_id,
        local.orders,
        local.fills,
        local.position_attributions,
        local.as_of,
        ("manual-review",),
    )
    assert blocked.recovery_blockers == ("manual-review",)
    with pytest.raises(ValueError, match="unique"):
        LocalReconciliationSnapshot("account-1", (), (), (), NOW, ("same", "same"))
    with pytest.raises(ValueError, match="bounded ASCII"):
        LocalReconciliationSnapshot("account-1", (), (), (), NOW, ("bad blocker",))


def test_broker_snapshot_rejects_type_identifier_account_and_time_mismatches() -> None:
    broker = broker_bundle()
    with pytest.raises(ValueError, match="snapshot_id"):
        BrokerReconciliationSnapshot(
            "bad snapshot",
            "account-1",
            broker.open_orders,
            broker.fills,
            broker.positions,
            broker.captured_at,
        )


def test_broker_snapshot_requires_one_exact_coherent_cut_token() -> None:
    broker = broker_bundle()
    mixed_fills = BrokerFillsSnapshot(
        broker.fills.fills,
        evidence(EvidenceQueryKind.FILLS, source_cursor="different-cut"),
    )
    with pytest.raises(ValueError, match="source_cursor"):
        BrokerReconciliationSnapshot(
            broker.snapshot_id,
            broker.account_id,
            broker.open_orders,
            mixed_fills,
            broker.positions,
            broker.captured_at,
        )
    with pytest.raises(TypeError, match="OpenOrdersSnapshot"):
        BrokerReconciliationSnapshot(
            "snapshot-1", "account-1", object(), broker.fills, broker.positions, broker.captured_at
        )  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="account"):
        BrokerReconciliationSnapshot(
            "snapshot-1",
            "account-2",
            broker.open_orders,
            broker.fills,
            broker.positions,
            broker.captured_at,
        )
    with pytest.raises(ValueError, match="evidence"):
        BrokerReconciliationSnapshot(
            "snapshot-1",
            "account-1",
            broker.open_orders,
            broker.fills,
            broker.positions,
            NOW - timedelta(seconds=1),
        )


def test_assessment_requires_exact_types_matching_accounts_and_snapshot_evidence() -> None:
    local = local_bundle()
    broker = broker_bundle()
    result = result_for(broker)
    assessment = ReconciliationAssessment(local, broker, result)

    assert assessment.may_resume
    assert not assessment.may_dispatch
    with pytest.raises(TypeError, match="LocalReconciliationSnapshot"):
        ReconciliationAssessment(object(), broker, result)  # type: ignore[arg-type]
    other_broker = broker_bundle(account_id="account-2")
    with pytest.raises(ValueError, match="accounts"):
        ReconciliationAssessment(local, other_broker, result_for(other_broker))
    unrelated = broker_bundle()
    unrelated = BrokerReconciliationSnapshot(
        "snapshot-2",
        unrelated.account_id,
        OpenOrdersSnapshot(
            (),
            CompletenessEvidence(
                EvidenceQueryKind.OPEN_ORDERS,
                "account-1",
                EvidenceCompleteness.COMPLETE,
                NOW,
                "snapshot-2",
            ),
        ),
        BrokerFillsSnapshot(
            unrelated.fills.fills,
            evidence(EvidenceQueryKind.FILLS, source_cursor="snapshot-2"),
        ),
        BrokerPositionsSnapshot(
            unrelated.positions.positions,
            evidence(EvidenceQueryKind.POSITIONS, source_cursor="snapshot-2"),
        ),
        unrelated.captured_at,
    )
    with pytest.raises(ValueError, match="evidence"):
        ReconciliationAssessment(local, unrelated, result)


def test_may_resume_requires_authoritative_result_without_discrepancies() -> None:
    local = local_bundle()
    broker = broker_bundle()
    incomplete = ReconciliationResult(
        "account-1",
        ReconciliationStatus.INCOMPLETE,
        (),
        (broker.open_orders.evidence, broker.fills.evidence, broker.positions.evidence),
        NOW + timedelta(seconds=2),
    )
    discrepancy = ReconciliationDiscrepancy(
        "difference-1",
        ReconciliationKind.POSITION_MISMATCH,
        "account-1",
        "TXF-202608",
        NOW,
        expected_quantity=Decimal("1"),
        actual_quantity=Decimal("2"),
    )

    assert not ReconciliationAssessment(local, broker, incomplete).may_resume
    assert not ReconciliationAssessment(
        local, broker, result_for(broker, discrepancies=(discrepancy,))
    ).may_resume
    blocked_local = LocalReconciliationSnapshot(
        local.account_id,
        local.orders,
        local.fills,
        local.position_attributions,
        local.as_of,
        ("manual-review",),
    )
    assert not ReconciliationAssessment(blocked_local, broker, result_for(broker)).may_resume


def test_assessment_rejects_broker_evidence_stale_against_local_snapshot() -> None:
    local = local_bundle()
    stale_time = NOW - timedelta(seconds=1)
    broker = broker_bundle()
    stale_broker = BrokerReconciliationSnapshot(
        broker.snapshot_id,
        broker.account_id,
        OpenOrdersSnapshot(
            (),
            evidence(EvidenceQueryKind.OPEN_ORDERS, observed_at=stale_time),
        ),
        broker.fills,
        broker.positions,
        broker.captured_at,
    )
    with pytest.raises(ValueError, match="predate local"):
        ReconciliationAssessment(local, stale_broker, result_for(stale_broker))


def test_contract_module_has_no_io_or_runtime_configuration_imports() -> None:
    import tx_trade.orders.live_reconciliation_contracts as module

    source = module.__loader__.get_source(module.__name__)  # type: ignore[union-attr]
    assert source is not None
    lowered = source.lower()
    for forbidden in ("skcom", "import os", "getenv", "open(", "connect(", "sqlite"):
        assert forbidden not in lowered
