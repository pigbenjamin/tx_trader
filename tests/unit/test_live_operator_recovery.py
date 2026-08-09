from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from inspect import signature

import pytest

from tx_trade.orders.live_contracts import (
    BrokerCorrelation,
    BrokerOpenOrderObservation,
    CorrelationStatus,
    FingerprintDomain,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    PendingCommandBinding,
    payload_fingerprint,
)
from tx_trade.orders.live_journal_contracts import (
    LiveJournalIdentity,
    LiveJournalRecoverySnapshot,
    OutstandingDispatchClaim,
)
from tx_trade.orders.live_operator_recovery import (
    build_operator_reconciliation_request,
    plan_operator_recovery,
)
from tx_trade.orders.live_operator_recovery_contracts import (
    ExplicitOperatorRecoverySelection,
    ExplicitOperatorRecoveryTargetSelection,
    OfflineOperatorRecoveryPlan,
    OperatorRecoveryDisposition,
    OperatorRecoveryReason,
    OperatorRecoveryResolution,
    OperatorRecoveryTargetKind,
)
from tx_trade.orders.live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    CompletenessEvidence,
    EvidenceCompleteness,
    EvidenceQueryKind,
    OpenOrdersSnapshot,
    RawBrokerObservation,
)
from tx_trade.orders.live_reconciliation import assess_reconciliation
from tx_trade.orders.live_reconciliation_commit_contracts import (
    DurableReconciliationCommitRequest,
)
from tx_trade.orders.live_reconciliation_projection_contracts import (
    AuthoritativeOrderProjectionPlan,
)
from tx_trade.orders.live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    LocalReconciliationSnapshot,
    ReconciliationAssessment,
)
from tx_trade.orders.live_state_machine import AppliedEventLedger, advance_local, create_live_order

NOW = datetime(2026, 8, 4, tzinfo=timezone.utc)
ACCOUNT = "account-secret"
FINGERPRINT = "sha256:" + "1" * 64


def _unknown_order(order_id: str = "order-1"):
    intent = LiveOrderIntent(
        strategy_id="strategy-1",
        client_order_id=order_id,
        account_id=ACCOUNT,
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=NOW,
    )
    command = NewOrderCommand(f"command-{order_id}", intent, NOW + timedelta(seconds=1))
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    order = advance_local(create_live_order(intent), LiveOrderState.VALIDATED, NOW)
    order = advance_local(
        order,
        LiveOrderState.SUBMITTING,
        command.requested_at,
        PendingCommandBinding(command, fingerprint),
    )
    return command, replace(order, state=LiveOrderState.SUBMISSION_UNKNOWN)


def _claim(order_id: str = "order-1", token: str | None = None):
    command, order = _unknown_order(order_id)
    return order, OutstandingDispatchClaim(
        command,
        token or f"token-{order_id}",
        "offline-worker",
        order.version,
        NOW + timedelta(seconds=2),
    )


def _snapshot(
    *,
    orders: tuple = (),
    claims: tuple = (),
    unresolved: tuple = (),
    sequence: int = 10,
) -> LiveJournalRecoverySnapshot:
    return LiveJournalRecoverySnapshot(
        LiveJournalIdentity("journal-1", 2, FINGERPRINT, NOW),
        orders,
        claims,
        unresolved,
        (),
        (),
        (),
        AppliedEventLedger(()),
        sequence,
    )


def _assessment(
    orders: tuple,
    *,
    include_orders: bool = True,
    complete: bool = True,
    sequence: int = 10,
) -> ReconciliationAssessment:
    as_of = NOW + timedelta(seconds=5)
    observed_at = NOW + timedelta(seconds=10)
    local = LocalReconciliationSnapshot(
        ACCOUNT,
        orders,
        (),
        (),
        as_of,
        ("recovery:outstanding",),
        sequence,
    )
    status = EvidenceCompleteness.COMPLETE if complete else EvidenceCompleteness.INCOMPLETE

    def evidence(kind: EvidenceQueryKind) -> CompletenessEvidence:
        return CompletenessEvidence(
            kind,
            ACCOUNT,
            status,
            observed_at,
            "snapshot-1",
            None if complete else "offline-incomplete",
        )

    broker_orders = tuple(
        BrokerOpenOrderObservation(
            f"broker-{order.intent.client_order_id}",
            ACCOUNT,
            order.intent.instrument_id,
            order.intent.side,
            order.total_quantity,
            order.remaining_quantity,
            order.working_limit_price,
            BrokerCorrelation(
                1,
                index,
                CorrelationStatus.CONFIRMED,
                observed_at,
                broker_order_sequence=f"broker-sequence-{index}",
                client_order_id=order.intent.client_order_id,
            ),
            observed_at,
        )
        for index, order in enumerate(orders, 1)
    )
    open_orders = OpenOrdersSnapshot(
        broker_orders if include_orders else (),
        evidence(EvidenceQueryKind.OPEN_ORDERS),
    )
    fills = BrokerFillsSnapshot((), evidence(EvidenceQueryKind.FILLS))
    positions = BrokerPositionsSnapshot((), evidence(EvidenceQueryKind.POSITIONS))
    broker = BrokerReconciliationSnapshot(
        "snapshot-1",
        ACCOUNT,
        open_orders,
        fills,
        positions,
        observed_at,
    )
    return assess_reconciliation(local, broker, observed_at)


def _ready_fixture(count: int = 1):
    values = tuple(_claim(f"order-{index}") for index in range(1, count + 1))
    orders = tuple(item[0] for item in values)
    claims = tuple(item[1] for item in values)
    snapshot = _snapshot(orders=orders, claims=claims)
    assessment = _assessment(orders)
    plan = plan_operator_recovery(snapshot, assessment)
    assert plan.disposition is OperatorRecoveryDisposition.READY_FOR_EXPLICIT_COMMIT
    return snapshot, assessment, plan


def _selection(plan: OfflineOperatorRecoveryPlan, commit_id: str = "commit-1"):
    return ExplicitOperatorRecoverySelection(
        commit_id,
        plan.account_id,
        plan.journal_sequence,
        plan.inspection_digest,
        tuple(
            ExplicitOperatorRecoveryTargetSelection(
                item.kind,
                item.target_id,
                OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,
            )
            for item in plan.targets
        ),
    )


def test_clean_snapshot_needs_no_action_and_is_deterministic() -> None:
    _, unknown = _unknown_order()
    clean = _snapshot(
        orders=(
            replace(unknown, state=LiveOrderState.ACCEPTED, pending_command=None, accepted_at=NOW),
        )
    )
    first = plan_operator_recovery(clean, None)
    assert first.disposition is OperatorRecoveryDisposition.READY_NO_ACTION
    assert first == plan_operator_recovery(clean, None)
    assert not first.commit_allowed
    assert not first.may_dispatch


def test_integrity_blocked_is_redacted_and_has_no_targets() -> None:
    order, claim = _claim(token="never-show-this-token")
    broken_claim = replace(claim, expected_order_version=order.version + 1)
    plan = plan_operator_recovery(
        _snapshot(orders=(order,), claims=(broken_claim,)),
        None,
    )
    assert plan.disposition is OperatorRecoveryDisposition.BLOCKED_INTEGRITY_FAILURE
    assert plan.reasons == (OperatorRecoveryReason.INTEGRITY_FAILURE,)
    assert plan.targets == ()
    assert "never-show-this-token" not in repr(plan)


def test_claim_without_assessment_needs_broker_evidence_and_digest_ignores_token() -> None:
    order, first_claim = _claim(token="first-secret-token")
    first_snapshot = _snapshot(orders=(order,), claims=(first_claim,))
    second_snapshot = replace(
        first_snapshot,
        outstanding_claims=(replace(first_claim, claim_token="second-secret-token"),),
    )
    first = plan_operator_recovery(first_snapshot, None)
    second = plan_operator_recovery(second_snapshot, None)
    assert first.disposition is OperatorRecoveryDisposition.NEEDS_BROKER_EVIDENCE
    assert first.inspection_digest == second.inspection_digest
    assert first.targets == ()
    assert "first-secret-token" not in repr(first)


def test_not_authoritative_projection_needs_evidence() -> None:
    order, claim = _claim()
    snapshot = _snapshot(orders=(order,), claims=(claim,))
    assessment = _assessment((order,), complete=False)
    plan = plan_operator_recovery(snapshot, assessment)
    assert plan.disposition is OperatorRecoveryDisposition.NEEDS_BROKER_EVIDENCE
    assert OperatorRecoveryReason.INCOMPLETE_EVIDENCE in plan.reasons


@pytest.mark.parametrize("unsupported", ("observation", "projection", "cross-account"))
def test_unsupported_global_mixed_or_projection_work_escalates(unsupported: str) -> None:
    order, claim = _claim()
    snapshot = _snapshot(orders=(order,), claims=(claim,))
    assessment = _assessment((order,))
    if unsupported == "observation":
        raw = RawBrokerObservation("raw-1", "broker", 1, 1, NOW, b"opaque")
        snapshot = replace(snapshot, unresolved_observations=(raw,))
    elif unsupported == "projection":
        assessment = _assessment((order,), include_orders=False)
    else:
        other_intent = replace(order.intent, account_id="account-other", client_order_id="other")
        other_command = NewOrderCommand("command-other", other_intent, NOW)
        other_order = replace(
            order,
            intent=other_intent,
            pending_command=PendingCommandBinding(
                other_command,
                payload_fingerprint(other_command, FingerprintDomain.NEW_COMMAND_V1),
            ),
        )
        other_claim = OutstandingDispatchClaim(
            other_command,
            "token-other",
            "worker",
            other_order.version,
            NOW,
        )
        snapshot = replace(
            snapshot,
            orders=(order, other_order),
            outstanding_claims=(claim, other_claim),
        )
    plan = plan_operator_recovery(snapshot, assessment)
    assert plan.disposition is OperatorRecoveryDisposition.UNSUPPORTED_REQUIRES_ESCALATION
    assert not plan.commit_allowed


def test_ready_plan_has_only_redacted_claim_target_and_projection() -> None:
    snapshot, assessment, plan = _ready_fixture()
    assert plan.projection_plan is not None and plan.projection_plan.may_commit
    assert len(plan.targets) == 1
    assert plan.targets[0].kind is OperatorRecoveryTargetKind.CLAIM
    assert plan.targets[0].allowed_resolutions == (
        OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,
    )
    assert snapshot.outstanding_claims[0].claim_token not in repr(plan)
    assert plan == plan_operator_recovery(snapshot, assessment)


@pytest.mark.parametrize(
    "change",
    ("accepted_at", "updated_at", "intent", "quantity", "price"),
)
def test_inspection_digest_binds_complete_projected_order(
    change: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot, assessment, original = _ready_fixture()
    projection = original.projection_plan
    assert projection is not None
    order = projection.projected_orders[0]
    if change == "accepted_at":
        assert order.accepted_at is not None
        changed_order = replace(order, accepted_at=order.accepted_at - timedelta(seconds=1))
    elif change == "updated_at":
        changed_order = replace(order, updated_at=order.updated_at + timedelta(seconds=1))
    elif change == "intent":
        changed_order = replace(
            order,
            intent=replace(order.intent, strategy_id="strategy-2"),
        )
    elif change == "quantity":
        changed_order = replace(
            order,
            intent=replace(order.intent, quantity=Decimal("2")),
            total_quantity=Decimal("2"),
            remaining_quantity=Decimal("2"),
        )
    else:
        changed_order = replace(
            order,
            intent=replace(order.intent, limit_price=Decimal("22001")),
            working_limit_price=Decimal("22001"),
        )
    changed_projection = replace(projection, projected_orders=(changed_order,))
    assert type(changed_projection) is AuthoritativeOrderProjectionPlan
    monkeypatch.setattr(
        "tx_trade.orders.live_operator_recovery.project_authoritative_orders",
        lambda _: changed_projection,
    )
    changed = plan_operator_recovery(snapshot, assessment)
    assert changed.inspection_digest != original.inspection_digest
    with pytest.raises(ValueError, match="selection does not match"):
        build_operator_reconciliation_request(
            changed,
            _selection(original),
            snapshot,
            assessment,
        )


def test_builder_produces_sorted_complete_request_without_executing() -> None:
    snapshot, assessment, plan = _ready_fixture(2)
    selection = _selection(plan)
    first = build_operator_reconciliation_request(plan, selection, snapshot, assessment)
    second = build_operator_reconciliation_request(plan, selection, snapshot, assessment)
    assert type(first) is DurableReconciliationCommitRequest
    assert first == second
    assert not plan.may_dispatch
    assert not assessment.may_dispatch
    assert tuple(item.client_order_id for item in first.expected_order_versions) == (
        "order-1",
        "order-2",
    )
    assert tuple(item.client_command_id for item in first.claim_resolutions) == (
        "command-order-1",
        "command-order-2",
    )
    assert tuple(item.intent.client_order_id for item in first.order_projections) == (
        "order-1",
        "order-2",
    )
    assert first.observation_resolutions == ()
    assert first.requirement_resolutions == ()


@pytest.mark.parametrize("failure", ("partial", "extra", "resolution"))
def test_builder_rejects_partial_extra_or_changed_selection(failure: str) -> None:
    snapshot, assessment, plan = _ready_fixture(2)
    selected = list(_selection(plan).selected_targets)
    if failure == "partial":
        selected = selected[:1]
    elif failure == "extra":
        selected.append(
            ExplicitOperatorRecoveryTargetSelection(
                OperatorRecoveryTargetKind.CLAIM,
                "claim-extra",
                OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,
            )
        )
        selected.sort(key=lambda item: (item.kind.value, item.target_id))
    else:
        selected[0] = replace(
            selected[0],
            resolution=OperatorRecoveryResolution.BROKER_FILL_CONFIRMED,
        )
    selection = ExplicitOperatorRecoverySelection(
        "commit-1",
        plan.account_id,
        plan.journal_sequence,
        plan.inspection_digest,
        tuple(selected),
    )
    with pytest.raises(ValueError, match="complete target set"):
        build_operator_reconciliation_request(plan, selection, snapshot, assessment)


@pytest.mark.parametrize("failure", ("not-ready", "stale", "account", "digest"))
def test_builder_rejects_wrong_plan_cut_account_or_digest(failure: str) -> None:
    snapshot, assessment, plan = _ready_fixture()
    selection = _selection(plan)
    if failure == "not-ready":
        plan = plan_operator_recovery(snapshot, None)
    elif failure == "stale":
        snapshot = replace(snapshot, journal_sequence=snapshot.journal_sequence + 1)
    elif failure == "account":
        selection = replace(selection, account_id="account-other")
    else:
        selection = replace(selection, inspection_digest="sha256:" + "2" * 64)
    with pytest.raises(ValueError):
        build_operator_reconciliation_request(plan, selection, snapshot, assessment)


def test_builder_rejects_assessment_or_projection_mismatch() -> None:
    snapshot, assessment, plan = _ready_fixture()
    selection = _selection(plan)
    mismatched = _assessment(snapshot.orders, include_orders=False)
    with pytest.raises(ValueError, match="no longer matches"):
        build_operator_reconciliation_request(plan, selection, snapshot, mismatched)


def test_public_api_accepts_only_hydrated_data_and_exposes_no_execution_ports() -> None:
    assert tuple(signature(plan_operator_recovery).parameters) == ("snapshot", "assessment")
    assert tuple(signature(build_operator_reconciliation_request).parameters) == (
        "plan",
        "selection",
        "snapshot",
        "assessment",
    )
    assert "commit" not in signature(plan_operator_recovery).parameters
    assert "broker" not in signature(plan_operator_recovery).parameters
    assert "dispatch" not in signature(build_operator_reconciliation_request).parameters
