from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from tx_trade.orders.live_contracts import (
    BrokerCorrelation,
    BrokerOpenOrderObservation,
    BrokerOrderEventType,
    CorrelationStatus,
    FingerprintDomain,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    NormalizedBrokerOrderEvent,
    PendingCommandBinding,
    ReconciliationKind,
    payload_fingerprint,
)
from tx_trade.orders.live_journal_contracts import (
    JournalOpenMode,
    RegistrationDisposition,
    intent_fingerprint,
)
from tx_trade.orders.live_journal_recovery import (
    PendingRecoveryKind,
    RecoveryReadiness,
    verify_recovery_snapshot,
)
from tx_trade.orders.live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    CompletenessEvidence,
    DispatchClaimDisposition,
    EvidenceCompleteness,
    EvidenceQueryKind,
    JournalAppendDisposition,
    OpenOrdersSnapshot,
    RawBrokerObservation,
    ReconciliationStatus,
)
from tx_trade.orders.live_reconciliation import FakeOnlyReconciliationService
from tx_trade.orders.live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    BrokerReconciliationSnapshotSourcePort,
)
from tx_trade.orders.live_state_machine import advance_local, create_live_order
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

CREATED_AT = datetime(2026, 8, 2, 1, tzinfo=timezone.utc)
ACCEPTED_AT = CREATED_AT + timedelta(seconds=2)
SNAPSHOT_AT = CREATED_AT + timedelta(seconds=10)
SNAPSHOT_ID = "snapshot-integration-fixed"
ACCOUNT_ID = "account-integration"
ORDER_ID = "order-integration-1"


@dataclass(slots=True)
class FakeBrokerReconciliationSource:
    snapshot: BrokerReconciliationSnapshot
    calls: list[str] = field(default_factory=list)

    def query_reconciliation_snapshot(
        self,
        account_id: str,
    ) -> BrokerReconciliationSnapshot:
        assert account_id == ACCOUNT_ID
        self.calls.append("reconciliation_snapshot")
        return self.snapshot


@dataclass(slots=True)
class SequentialThreeQueryFake:
    open_orders: OpenOrdersSnapshot
    fills: BrokerFillsSnapshot
    positions: BrokerPositionsSnapshot
    calls: list[str] = field(default_factory=list)

    def query_open_orders(self, account_id: str) -> OpenOrdersSnapshot:
        self.calls.append("open_orders")
        return self.open_orders

    def query_fills(self, account_id: str) -> BrokerFillsSnapshot:
        self.calls.append("fills")
        return self.fills

    def query_positions(self, account_id: str) -> BrokerPositionsSnapshot:
        self.calls.append("positions")
        return self.positions


class FixedClock:
    def now(self) -> datetime:
        return SNAPSHOT_AT


def _submission() -> tuple[NewOrderCommand, object]:
    intent = LiveOrderIntent(
        strategy_id="strategy-integration",
        client_order_id=ORDER_ID,
        account_id=ACCOUNT_ID,
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=Decimal("2"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=CREATED_AT,
    )
    command = NewOrderCommand("command-integration-1", intent, CREATED_AT + timedelta(seconds=1))
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    order = create_live_order(intent)
    order = advance_local(order, LiveOrderState.VALIDATED, CREATED_AT + timedelta(milliseconds=1))
    order = advance_local(
        order,
        LiveOrderState.SUBMITTING,
        command.requested_at,
        PendingCommandBinding(command, fingerprint),
    )
    return command, order


def _open_journal(path: Path, mode: JournalOpenMode) -> SqliteLiveOrderJournal:
    return SqliteLiveOrderJournal(
        path,
        mode,
        journal_id="journal-reconciliation" if mode is JournalOpenMode.CREATE_NEW else None,
        clock=FixedClock().now,
        claim_token_factory=lambda: "claim-integration-1",
    )


def _register(journal: SqliteLiveOrderJournal) -> tuple[NewOrderCommand, object]:
    command, order = _submission()
    registration = journal.register_new_order(
        command,
        order,
        intent_fingerprint=intent_fingerprint(command.intent),
    )
    assert registration.disposition is RegistrationDisposition.REGISTERED
    return command, order


def _persist_acceptance(journal: SqliteLiveOrderJournal, order: object) -> object:
    raw = RawBrokerObservation(
        "raw-accepted-integration-1",
        "fake-broker-reply",
        1,
        1,
        ACCEPTED_AT,
        b"accepted",
    )
    assert journal.append_raw_observation(raw).disposition is JournalAppendDisposition.APPENDED
    correlation = BrokerCorrelation(
        1,
        1,
        CorrelationStatus.CONFIRMED,
        ACCEPTED_AT,
        broker_order_sequence="broker-order-integration-1",
        client_order_id=ORDER_ID,
    )
    event = NormalizedBrokerOrderEvent(
        "accepted-integration-1",
        ACCOUNT_ID,
        "TXF-202608",
        BrokerOrderEventType.NEW_ACCEPTED,
        ACCEPTED_AT,
        1,
        1,
        correlation,
    )
    result = journal.apply_normalized_event(
        event,
        raw_observation_id=raw.observation_id,
        expected_order_version=order.version,  # type: ignore[attr-defined]
    )
    assert result.order is not None
    assert result.order.state is LiveOrderState.ACCEPTED
    assert result.order.pending_command is None
    return result.order


def _evidence(
    kind: EvidenceQueryKind,
    status: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
    *,
    observed_at: datetime = SNAPSHOT_AT,
    source_cursor: str = SNAPSHOT_ID,
) -> CompletenessEvidence:
    return CompletenessEvidence(
        kind,
        ACCOUNT_ID,
        status,
        observed_at,
        source_cursor,
        None if status is EvidenceCompleteness.COMPLETE else "fake-query-incomplete",
    )


def _broker_order(
    status: CorrelationStatus = CorrelationStatus.CONFIRMED,
) -> BrokerOpenOrderObservation:
    correlation = BrokerCorrelation(
        1,
        1,
        status,
        ACCEPTED_AT,
        broker_order_sequence="broker-order-integration-1",
        client_order_id=ORDER_ID,
    )
    return BrokerOpenOrderObservation(
        "broker-open-integration-1",
        ACCOUNT_ID,
        "TXF-202608",
        LiveSide.BUY,
        Decimal("2"),
        Decimal("2"),
        Decimal("22000"),
        correlation,
        SNAPSHOT_AT,
    )


def _broker_source(
    *orders: BrokerOpenOrderObservation,
    open_status: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
    evidence_observed_at: datetime = SNAPSHOT_AT,
) -> FakeBrokerReconciliationSource:
    snapshot = BrokerReconciliationSnapshot(
        SNAPSHOT_ID,
        ACCOUNT_ID,
        OpenOrdersSnapshot(
            orders,
            _evidence(
                EvidenceQueryKind.OPEN_ORDERS,
                open_status,
                observed_at=evidence_observed_at,
            ),
        ),
        BrokerFillsSnapshot(
            (),
            _evidence(EvidenceQueryKind.FILLS, observed_at=evidence_observed_at),
        ),
        BrokerPositionsSnapshot(
            (),
            _evidence(EvidenceQueryKind.POSITIONS, observed_at=evidence_observed_at),
        ),
        SNAPSHOT_AT,
    )
    return FakeBrokerReconciliationSource(snapshot)


def _service(
    journal: SqliteLiveOrderJournal,
    broker: FakeBrokerReconciliationSource,
) -> FakeOnlyReconciliationService:
    return FakeOnlyReconciliationService(journal, broker, FixedClock())


def _accepted_resumed_journal(path: Path) -> SqliteLiveOrderJournal:
    journal = _open_journal(path, JournalOpenMode.CREATE_NEW)
    _, order = _register(journal)
    _persist_acceptance(journal, order)
    journal.close()
    return _open_journal(path, JournalOpenMode.RESUME)


def test_create_persist_close_resume_assess_is_read_only_and_deterministic(
    tmp_path: Path,
) -> None:
    journal = _accepted_resumed_journal(tmp_path / "accepted.sqlite3")
    sequence_before = journal.load_recovery_snapshot().journal_sequence
    local_before = journal.load_account_snapshot(ACCOUNT_ID)
    broker = _broker_source(_broker_order())
    service = _service(journal, broker)

    first = service.assess(ACCOUNT_ID)
    second = service.assess(ACCOUNT_ID)

    assert first == second
    assert first.local_snapshot == local_before
    assert first.broker_snapshot is broker.snapshot
    assert second.broker_snapshot is broker.snapshot
    assert first.result.status is ReconciliationStatus.COMPLETE
    assert first.result.is_authoritative
    assert first.result.discrepancies == ()
    assert first.may_resume
    assert not first.may_dispatch
    assert len(first.result.evidence) == 3
    assert all(
        item.source_cursor == first.broker_snapshot.snapshot_id for item in first.result.evidence
    )
    assert all(item.observed_at >= first.local_snapshot.as_of for item in first.result.evidence)
    assert broker.calls == ["reconciliation_snapshot", "reconciliation_snapshot"]
    assert journal.load_recovery_snapshot().journal_sequence == sequence_before
    journal.close()


def test_incomplete_and_candidate_evidence_fail_closed_without_absence_guessing(
    tmp_path: Path,
) -> None:
    journal = _accepted_resumed_journal(tmp_path / "fail-closed.sqlite3")
    sequence_before = journal.load_recovery_snapshot().journal_sequence

    incomplete_broker = _broker_source(open_status=EvidenceCompleteness.INCOMPLETE)
    incomplete = _service(journal, incomplete_broker).assess(ACCOUNT_ID)
    incomplete_kinds = {item.kind for item in incomplete.result.discrepancies}
    assert incomplete.result.status is ReconciliationStatus.INCOMPLETE
    assert ReconciliationKind.MISSING_BROKER_ORDER not in incomplete_kinds
    assert not incomplete.may_resume
    assert not incomplete.may_dispatch
    assert incomplete_broker.calls == ["reconciliation_snapshot"]

    candidate_broker = _broker_source(_broker_order(CorrelationStatus.CANDIDATE))
    candidate = _service(journal, candidate_broker).assess(ACCOUNT_ID)
    correlation_issue = next(
        item
        for item in candidate.result.discrepancies
        if item.kind is ReconciliationKind.CORRELATION_MISSING
    )
    assert candidate.result.status is ReconciliationStatus.AMBIGUOUS
    assert correlation_issue.client_order_id is None
    assert ReconciliationKind.MISSING_BROKER_ORDER not in {
        item.kind for item in candidate.result.discrepancies
    }
    assert not candidate.may_resume
    assert not candidate.may_dispatch
    assert candidate_broker.calls == ["reconciliation_snapshot"]
    assert journal.load_recovery_snapshot().journal_sequence == sequence_before
    journal.close()


@pytest.mark.parametrize(
    ("with_claim", "pending_kind"),
    [
        (False, PendingRecoveryKind.REGISTERED_AWAITING_BROKER_EVIDENCE),
        (True, PendingRecoveryKind.CLAIMED_OUTCOME_UNKNOWN),
    ],
)
def test_pending_recovery_never_authorizes_dispatch_even_with_broker_evidence(
    tmp_path: Path,
    with_claim: bool,
    pending_kind: PendingRecoveryKind,
) -> None:
    path = tmp_path / f"pending-{with_claim}.sqlite3"
    journal = _open_journal(path, JournalOpenMode.CREATE_NEW)
    command, order = _register(journal)
    if with_claim:
        claim = journal.claim_dispatch(
            command.client_command_id,
            payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
            expected_order_version=order.version,  # type: ignore[attr-defined]
            claimant_id="fake-dispatcher",
        )
        assert claim.disposition is DispatchClaimDisposition.ACQUIRED
    journal.close()

    resumed = _open_journal(path, JournalOpenMode.RESUME)
    recovery_snapshot = resumed.load_recovery_snapshot()
    assert bool(recovery_snapshot.outstanding_claims) is with_claim
    verification = verify_recovery_snapshot(recovery_snapshot)
    assert verification.readiness is RecoveryReadiness.RECONCILIATION_REQUIRED
    assert verification.pending[0].kind is pending_kind
    assert not verification.pending[0].may_redispatch
    assert not verification.may_dispatch

    sequence_before = recovery_snapshot.journal_sequence
    broker = _broker_source(_broker_order())
    assessment = _service(resumed, broker).assess(ACCOUNT_ID)
    assert ReconciliationKind.CORRELATION_MISSING in {
        item.kind for item in assessment.result.discrepancies
    }
    assert not assessment.may_resume
    assert not assessment.may_dispatch
    assert broker.calls == ["reconciliation_snapshot"]
    assert resumed.load_recovery_snapshot().journal_sequence == sequence_before
    resumed.close()


def test_accepted_order_conflict_blocks_recovery_and_resume_durably(
    tmp_path: Path,
) -> None:
    path = tmp_path / "accepted-conflict.sqlite3"
    journal = _open_journal(path, JournalOpenMode.CREATE_NEW)
    _, submitting = _register(journal)
    accepted = _persist_acceptance(journal, submitting)
    assert accepted.pending_command is None  # type: ignore[attr-defined]

    conflict_at = ACCEPTED_AT + timedelta(seconds=1)
    raw = RawBrokerObservation(
        "raw-conflict-integration-1",
        "fake-broker-reply",
        1,
        2,
        conflict_at,
        b"conflicting-accepted",
    )
    assert journal.append_raw_observation(raw).disposition is (JournalAppendDisposition.APPENDED)
    conflicting_event = NormalizedBrokerOrderEvent(
        "conflict-integration-1",
        ACCOUNT_ID,
        "TXF-DIFFERENT",
        BrokerOrderEventType.NEW_ACCEPTED,
        conflict_at,
        1,
        2,
        BrokerCorrelation(
            1,
            2,
            CorrelationStatus.CONFIRMED,
            conflict_at,
            broker_order_sequence="broker-order-conflict-integration-1",
            client_order_id=ORDER_ID,
        ),
    )
    conflict = journal.apply_normalized_event(
        conflicting_event,
        raw_observation_id=raw.observation_id,
        expected_order_version=accepted.version,  # type: ignore[attr-defined]
    )
    assert conflict.disposition.value == "unresolved"

    recovery_before_restart = journal.load_recovery_snapshot()
    blockers_before_restart = journal.load_account_snapshot(ACCOUNT_ID).recovery_blockers
    assert recovery_before_restart.conflict_observations == (raw,)
    assert len(recovery_before_restart.reconciliation_requirements) == 1
    assert len(blockers_before_restart) == 2
    journal.close()

    resumed = _open_journal(path, JournalOpenMode.RESUME)
    recovery_after_restart = resumed.load_recovery_snapshot()
    verification = verify_recovery_snapshot(recovery_after_restart)
    assert verification.readiness is RecoveryReadiness.RECONCILIATION_REQUIRED
    assert verification.pending == ()
    assert recovery_after_restart.journal_sequence == (recovery_before_restart.journal_sequence)
    assert recovery_after_restart.conflict_observations == (raw,)
    local_after_restart = resumed.load_account_snapshot(ACCOUNT_ID)
    assert local_after_restart.orders[0].state is LiveOrderState.ACCEPTED
    assert local_after_restart.orders[0].pending_command is None
    assert local_after_restart.recovery_blockers == blockers_before_restart

    sequence_before = recovery_after_restart.journal_sequence
    broker = _broker_source(_broker_order())
    assessment = _service(resumed, broker).assess(ACCOUNT_ID)
    assert assessment.result.status is ReconciliationStatus.COMPLETE
    assert assessment.result.discrepancies == ()
    assert assessment.local_snapshot.recovery_blockers == blockers_before_restart
    assert not assessment.may_resume
    assert not assessment.may_dispatch
    assert broker.calls == ["reconciliation_snapshot"]
    assert resumed.load_recovery_snapshot().journal_sequence == sequence_before
    resumed.close()


def test_service_rejects_stale_broker_evidence_without_returning_assessment(
    tmp_path: Path,
) -> None:
    journal = _accepted_resumed_journal(tmp_path / "stale-evidence.sqlite3")
    local = journal.load_account_snapshot(ACCOUNT_ID)
    assert ACCEPTED_AT < local.as_of
    sequence_before = journal.load_recovery_snapshot().journal_sequence
    broker = _broker_source(evidence_observed_at=ACCEPTED_AT)

    with pytest.raises(ValueError, match="evidence must not predate local snapshot"):
        _service(journal, broker).assess(ACCOUNT_ID)

    assert broker.calls == ["reconciliation_snapshot"]
    assert journal.load_recovery_snapshot().journal_sequence == sequence_before
    journal.close()


def test_sequential_three_query_source_cannot_supply_atomic_reconciliation(
    tmp_path: Path,
) -> None:
    journal = _accepted_resumed_journal(tmp_path / "sequential-source.sqlite3")
    coherent = _broker_source(_broker_order()).snapshot
    sequential = SequentialThreeQueryFake(
        coherent.open_orders,
        coherent.fills,
        coherent.positions,
    )
    assert not isinstance(sequential, BrokerReconciliationSnapshotSourcePort)
    sequence_before = journal.load_recovery_snapshot().journal_sequence

    with pytest.raises((TypeError, AttributeError)):
        service = FakeOnlyReconciliationService(
            journal,
            sequential,  # type: ignore[arg-type]
            FixedClock(),
        )
        service.assess(ACCOUNT_ID)

    assert sequential.calls == []
    assert journal.load_recovery_snapshot().journal_sequence == sequence_before
    journal.close()
