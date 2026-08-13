from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import sqlite3

import pytest

from tests.support.live_authorization_audit_scenarios import commit_authorized

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
from tx_trade.orders.live_journal_contracts import JournalOpenMode, intent_fingerprint
from tx_trade.orders.live_operator_recovery import (
    build_operator_reconciliation_request,
    plan_operator_recovery,
)
from tx_trade.orders.live_operator_recovery_contracts import (
    ExplicitOperatorRecoverySelection,
    ExplicitOperatorRecoveryTargetSelection,
    OperatorRecoveryDisposition,
    OperatorRecoveryResolution,
)
from tx_trade.orders.live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    CompletenessEvidence,
    DispatchClaimDisposition,
    EvidenceCompleteness,
    EvidenceQueryKind,
    OpenOrdersSnapshot,
    RawBrokerObservation,
)
from tx_trade.orders.live_reconciliation import assess_reconciliation
from tx_trade.orders.live_reconciliation_commit_contracts import (
    ReconciliationCommitDisposition,
)
from tx_trade.orders.live_reconciliation_contracts import BrokerReconciliationSnapshot
from tx_trade.orders.live_state_machine import advance_local, create_live_order
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

NOW = datetime(2026, 8, 4, tzinfo=timezone.utc)
CUT = NOW + timedelta(seconds=20)
ACCOUNT_ID = "account-operator-integration"
ORDER_ID = "order-operator-integration"
COMMAND_ID = "command-operator-integration"


def _clock() -> datetime:
    return CUT


def _open(path: Path, mode: JournalOpenMode) -> SqliteLiveOrderJournal:
    return SqliteLiveOrderJournal(
        path,
        mode,
        journal_id="journal-operator-integration" if mode is JournalOpenMode.CREATE_NEW else None,
        clock=_clock,
        claim_token_factory=lambda: "claim-token-operator-integration",
    )


def _submission_unknown() -> tuple[NewOrderCommand, object]:
    intent = LiveOrderIntent(
        strategy_id="strategy-operator-integration",
        client_order_id=ORDER_ID,
        account_id=ACCOUNT_ID,
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=NOW,
    )
    command = NewOrderCommand(COMMAND_ID, intent, NOW + timedelta(seconds=1))
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    order = advance_local(create_live_order(intent), LiveOrderState.VALIDATED, NOW)
    order = advance_local(
        order,
        LiveOrderState.SUBMITTING,
        command.requested_at,
        PendingCommandBinding(command, fingerprint),
    )
    return command, replace(order, state=LiveOrderState.SUBMISSION_UNKNOWN)


def _create_claim(journal: SqliteLiveOrderJournal):
    command, order = _submission_unknown()
    journal.register_new_order(
        command,
        order,
        intent_fingerprint=intent_fingerprint(command.intent),
    )
    claim = journal.claim_dispatch(
        command.client_command_id,
        payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
        expected_order_version=order.version,
        claimant_id="offline-operator",
    )
    assert claim.disposition is DispatchClaimDisposition.ACQUIRED
    return command, order


def _broker_snapshot(
    order: object,
    *,
    completeness: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
    ambiguous: bool = False,
) -> BrokerReconciliationSnapshot:
    evidence_reason = None if completeness is EvidenceCompleteness.COMPLETE else "fake-incomplete"

    def evidence(kind: EvidenceQueryKind) -> CompletenessEvidence:
        return CompletenessEvidence(
            kind,
            ACCOUNT_ID,
            completeness,
            CUT,
            "snapshot-operator-integration",
            evidence_reason,
        )

    correlation = BrokerCorrelation(
        1,
        1,
        CorrelationStatus.CONFIRMED,
        CUT,
        broker_order_sequence="broker-order-operator-integration",
        client_order_id=ORDER_ID,
    )
    broker_order = BrokerOpenOrderObservation(
        "broker-open-operator-integration",
        ACCOUNT_ID,
        order.intent.instrument_id,
        order.intent.side,
        order.total_quantity,
        order.remaining_quantity,
        order.working_limit_price,
        correlation,
        CUT,
    )
    orders = (broker_order,)
    if ambiguous:
        orders = (
            broker_order,
            replace(
                broker_order,
                observation_id="broker-open-conflict",
                working_limit_price=Decimal("22001"),
            ),
        )
    return BrokerReconciliationSnapshot(
        "snapshot-operator-integration",
        ACCOUNT_ID,
        OpenOrdersSnapshot(orders, evidence(EvidenceQueryKind.OPEN_ORDERS)),
        BrokerFillsSnapshot((), evidence(EvidenceQueryKind.FILLS)),
        BrokerPositionsSnapshot((), evidence(EvidenceQueryKind.POSITIONS)),
        CUT,
    )


def _plan(journal: SqliteLiveOrderJournal, order: object, **broker_changes: object):
    recovery = journal.load_recovery_snapshot()
    local = journal.load_account_snapshot(ACCOUNT_ID)
    assessment = assess_reconciliation(
        local,
        _broker_snapshot(order, **broker_changes),
        CUT,
    )
    plan = plan_operator_recovery(recovery, assessment)
    return recovery, assessment, plan


def _selection(plan, commit_id: str = "commit-operator-integration"):
    return ExplicitOperatorRecoverySelection(
        commit_id,
        ACCOUNT_ID,
        plan.journal_sequence,
        plan.inspection_digest,
        tuple(
            ExplicitOperatorRecoveryTargetSelection(
                target.kind,
                target.target_id,
                OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,
            )
            for target in plan.targets
        ),
    )


def _row_counts(path: Path) -> tuple[int, int, int, int]:
    connection = sqlite3.connect(path)
    try:
        return tuple(
            connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "live_reconciliation_commits",
                "live_dispatch_claim_resolutions",
                "live_dispatch_receipts",
                "live_normalized_events",
            )
        )
    finally:
        connection.close()


def test_fake_full_projection_plan_build_commit_retry_and_restart(tmp_path: Path) -> None:
    path = tmp_path / "operator-full.sqlite3"
    journal = _open(path, JournalOpenMode.CREATE_NEW)
    command, order = _create_claim(journal)
    recovery, assessment, plan = _plan(journal, order)
    assert plan.disposition is OperatorRecoveryDisposition.READY_FOR_EXPLICIT_COMMIT
    assert not plan.may_dispatch
    assert not assessment.may_dispatch
    request = build_operator_reconciliation_request(
        plan,
        _selection(plan),
        recovery,
        assessment,
    )

    committed = commit_authorized(journal, request)
    assert committed.disposition is ReconciliationCommitDisposition.COMMITTED
    assert committed.order_projections == request.order_projections
    accepted = journal.get_order(ORDER_ID)
    assert accepted == request.order_projections[0]
    assert accepted is not None and accepted.state is LiveOrderState.ACCEPTED
    assert journal.load_account_snapshot(ACCOUNT_ID).recovery_blockers == ()
    after = journal.load_recovery_snapshot()
    assert after.outstanding_claims == ()
    assert after.applied_event_ledger.events == ()
    assert _row_counts(path) == (1, 1, 0, 0)

    retry = commit_authorized(journal, request)
    assert retry == replace(committed, disposition=ReconciliationCommitDisposition.EXACT_RETRY)
    assert journal.load_recovery_snapshot() == after
    reacquire = journal.claim_dispatch(
        command.client_command_id,
        payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
        expected_order_version=accepted.version,
        claimant_id="must-not-redispatch",
    )
    assert reacquire.disposition is not DispatchClaimDisposition.ACQUIRED
    journal.close()

    resumed = _open(path, JournalOpenMode.RESUME)
    assert resumed.load_recovery_snapshot() == after
    assert resumed.get_order(ORDER_ID) == accepted
    assert resumed.load_account_snapshot(ACCOUNT_ID).fills == ()
    assert _row_counts(path) == (1, 1, 0, 0)
    resumed.close()


def test_stale_request_after_sequence_advance_writes_no_commit(tmp_path: Path) -> None:
    path = tmp_path / "operator-stale.sqlite3"
    journal = _open(path, JournalOpenMode.CREATE_NEW)
    _, order = _create_claim(journal)
    recovery, assessment, plan = _plan(journal, order)
    request = build_operator_reconciliation_request(
        plan,
        _selection(plan, "commit-stale"),
        recovery,
        assessment,
    )
    journal.append_raw_observation(
        RawBrokerObservation("raw-advance", "fake-broker", 1, 1, CUT, b"advance")
    )
    before = journal.load_recovery_snapshot()
    counts = _row_counts(path)
    result = commit_authorized(journal, request)
    assert result.disposition is ReconciliationCommitDisposition.STALE_SNAPSHOT
    assert journal.load_recovery_snapshot() == before
    assert _row_counts(path) == counts == (0, 0, 0, 0)
    journal.close()


@pytest.mark.parametrize(
    "changes",
    (
        {"completeness": EvidenceCompleteness.INCOMPLETE},
        {"ambiguous": True},
    ),
)
def test_incomplete_or_ambiguous_evidence_never_becomes_commit_ready(
    tmp_path: Path,
    changes: dict[str, object],
) -> None:
    journal = _open(tmp_path / "operator-evidence.sqlite3", JournalOpenMode.CREATE_NEW)
    _, order = _create_claim(journal)
    _, _, plan = _plan(journal, order, **changes)
    assert plan.disposition is OperatorRecoveryDisposition.NEEDS_BROKER_EVIDENCE
    assert not plan.commit_allowed
    journal.close()


@pytest.mark.parametrize("changed", ("partial", "resolution"))
def test_partial_or_changed_selection_rejected_before_repository_call(
    tmp_path: Path,
    changed: str,
) -> None:
    path = tmp_path / "operator-selection.sqlite3"
    journal = _open(path, JournalOpenMode.CREATE_NEW)
    _, order = _create_claim(journal)
    recovery, assessment, plan = _plan(journal, order)
    selected = list(_selection(plan).selected_targets)
    if changed == "partial":
        selected = []
        with pytest.raises(ValueError, match="must not be empty"):
            ExplicitOperatorRecoverySelection(
                "commit-partial",
                ACCOUNT_ID,
                plan.journal_sequence,
                plan.inspection_digest,
                (),
            )
    else:
        selected[0] = replace(
            selected[0],
            resolution=OperatorRecoveryResolution.BROKER_FILL_CONFIRMED,
        )
        selection = replace(_selection(plan), selected_targets=tuple(selected))
        with pytest.raises(ValueError, match="complete target set"):
            build_operator_reconciliation_request(
                plan,
                selection,
                recovery,
                assessment,
            )
    assert _row_counts(path) == (0, 0, 0, 0)
    journal.close()
