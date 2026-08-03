from __future__ import annotations

from dataclasses import dataclass, field, replace
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
    LiveOrder,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    NormalizedBrokerOrderEvent,
    PendingCommandBinding,
    payload_fingerprint,
)
from tx_trade.orders.live_journal_contracts import JournalOpenMode, intent_fingerprint
from tx_trade.orders.live_journal_recovery import RecoveryReadiness, verify_recovery_snapshot
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
)
from tx_trade.orders.live_reconciliation import FakeOnlyReconciliationService
from tx_trade.orders.live_reconciliation_commit_contracts import (
    ClaimResolution,
    ClaimResolutionDirective,
    DurableReconciliationCommitRequest,
    ExpectedOrderVersion,
    ObservationResolution,
    ObservationResolutionDirective,
    ObservationStatus,
    ReconciliationCommitDisposition,
    RequirementResolution,
    RequirementResolutionDirective,
)
from tx_trade.orders.live_reconciliation_contracts import BrokerReconciliationSnapshot
from tx_trade.orders.live_state_machine import advance_local, create_live_order
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

CREATED_AT = datetime(2026, 8, 3, 1, tzinfo=timezone.utc)
ACCEPTED_AT = CREATED_AT + timedelta(seconds=2)
SNAPSHOT_AT = CREATED_AT + timedelta(seconds=10)
ACCOUNT_ID = "account-commit-integration"
ORDER_ID = "order-commit-integration"
COMMAND_ID = "command-commit-integration"
CLAIM_TOKEN = "claim-commit-integration"
SNAPSHOT_ID = "snapshot-commit-integration"


class FixedClock:
    def now(self) -> datetime:
        return SNAPSHOT_AT


@dataclass(slots=True)
class FakeBrokerSource:
    snapshot: BrokerReconciliationSnapshot
    calls: list[str] = field(default_factory=list)

    def query_reconciliation_snapshot(self, account_id: str) -> BrokerReconciliationSnapshot:
        assert account_id == self.snapshot.account_id
        self.calls.append(account_id)
        return self.snapshot


def _submission() -> tuple[NewOrderCommand, LiveOrder]:
    intent = LiveOrderIntent(
        strategy_id="strategy-commit-integration",
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
    command = NewOrderCommand(COMMAND_ID, intent, CREATED_AT + timedelta(seconds=1))
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


def _open(path: Path, mode: JournalOpenMode) -> SqliteLiveOrderJournal:
    return SqliteLiveOrderJournal(
        path,
        mode,
        journal_id="journal-commit-integration" if mode is JournalOpenMode.CREATE_NEW else None,
        clock=FixedClock().now,
        claim_token_factory=lambda: CLAIM_TOKEN,
    )


def _evidence(
    kind: EvidenceQueryKind,
    *,
    completeness: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
    observed_at: datetime = SNAPSHOT_AT,
    account_id: str = ACCOUNT_ID,
    snapshot_id: str = SNAPSHOT_ID,
) -> CompletenessEvidence:
    return CompletenessEvidence(
        kind,
        account_id,
        completeness,
        observed_at,
        snapshot_id,
        None if completeness is EvidenceCompleteness.COMPLETE else "fake-incomplete",
    )


def _broker_snapshot(
    *,
    include_order: bool = True,
    completeness: EvidenceCompleteness = EvidenceCompleteness.COMPLETE,
    observed_at: datetime = SNAPSHOT_AT,
    snapshot_id: str = SNAPSHOT_ID,
) -> BrokerReconciliationSnapshot:
    orders: tuple[BrokerOpenOrderObservation, ...] = ()
    if include_order:
        orders = (
            BrokerOpenOrderObservation(
                "broker-open-commit-integration",
                ACCOUNT_ID,
                "TXF-202608",
                LiveSide.BUY,
                Decimal("2"),
                Decimal("2"),
                Decimal("22000"),
                BrokerCorrelation(
                    1,
                    1,
                    CorrelationStatus.CONFIRMED,
                    ACCEPTED_AT,
                    broker_order_sequence="broker-sequence-commit-integration",
                    client_order_id=ORDER_ID,
                ),
                SNAPSHOT_AT,
            ),
        )
    return BrokerReconciliationSnapshot(
        snapshot_id,
        ACCOUNT_ID,
        OpenOrdersSnapshot(
            orders,
            _evidence(
                EvidenceQueryKind.OPEN_ORDERS,
                completeness=completeness,
                observed_at=observed_at,
                snapshot_id=snapshot_id,
            ),
        ),
        BrokerFillsSnapshot(
            (),
            _evidence(
                EvidenceQueryKind.FILLS,
                observed_at=observed_at,
                snapshot_id=snapshot_id,
            ),
        ),
        BrokerPositionsSnapshot(
            (),
            _evidence(
                EvidenceQueryKind.POSITIONS,
                observed_at=observed_at,
                snapshot_id=snapshot_id,
            ),
        ),
        SNAPSHOT_AT,
    )


def _register_claim_and_accept(
    journal: SqliteLiveOrderJournal,
) -> tuple[NewOrderCommand, LiveOrder]:
    command, submitting = _submission()
    journal.register_new_order(
        command,
        submitting,
        intent_fingerprint=intent_fingerprint(command.intent),
    )
    claim = journal.claim_dispatch(
        command.client_command_id,
        payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
        expected_order_version=submitting.version,
        claimant_id="fake-dispatcher",
    )
    assert claim.disposition is DispatchClaimDisposition.ACQUIRED
    assert claim.claim_token == CLAIM_TOKEN

    raw = RawBrokerObservation(
        "raw-accepted-commit-integration",
        "fake-broker-reply",
        1,
        1,
        ACCEPTED_AT,
        b"accepted-without-dispatch-receipt",
    )
    assert journal.append_raw_observation(raw).disposition is JournalAppendDisposition.APPENDED
    event = NormalizedBrokerOrderEvent(
        "event-accepted-commit-integration",
        ACCOUNT_ID,
        "TXF-202608",
        BrokerOrderEventType.NEW_ACCEPTED,
        ACCEPTED_AT,
        1,
        1,
        BrokerCorrelation(
            1,
            1,
            CorrelationStatus.CONFIRMED,
            ACCEPTED_AT,
            broker_order_sequence="broker-sequence-commit-integration",
            client_order_id=ORDER_ID,
        ),
    )
    applied = journal.apply_normalized_event(
        event,
        raw_observation_id=raw.observation_id,
        expected_order_version=submitting.version,
    )
    assert applied.order is not None
    assert applied.order.state is LiveOrderState.ACCEPTED
    assert applied.order.pending_command is None
    return command, applied.order


def _assessment(journal: SqliteLiveOrderJournal, snapshot: BrokerReconciliationSnapshot):
    broker = FakeBrokerSource(snapshot)
    assessment = FakeOnlyReconciliationService(journal, broker, FixedClock()).assess(ACCOUNT_ID)
    assert broker.calls == [ACCOUNT_ID]
    assert not assessment.may_dispatch
    return assessment


def _claim_request(journal: SqliteLiveOrderJournal, *, commit_id: str):
    assessment = _assessment(journal, _broker_snapshot())
    snapshot = journal.load_recovery_snapshot()
    claim = snapshot.outstanding_claims[0]
    order = assessment.local_snapshot.orders[0]
    return DurableReconciliationCommitRequest(
        commit_id,
        ACCOUNT_ID,
        assessment,
        assessment.local_snapshot.journal_sequence,
        expected_order_versions=(
            ExpectedOrderVersion(order.intent.client_order_id, order.version),
        ),
        claim_resolutions=(
            ClaimResolutionDirective(
                COMMAND_ID,
                claim.claim_token,
                ClaimResolution.BROKER_ORDER_CONFIRMED,
            ),
        ),
    )


def test_evidence_backed_claim_commit_survives_restart_without_dispatch_authority(
    tmp_path: Path,
) -> None:
    path = tmp_path / "claim-commit.sqlite3"
    journal = _open(path, JournalOpenMode.CREATE_NEW)
    command, accepted = _register_claim_and_accept(journal)
    before = journal.load_recovery_snapshot()
    before_verification = verify_recovery_snapshot(before)
    assert len(before.outstanding_claims) == 1
    assert not before_verification.may_dispatch

    request = _claim_request(journal, commit_id="commit-claim-integration")
    assert request.assessment.result.is_authoritative
    assert request.assessment.result.discrepancies == ()
    assert not request.assessment.may_resume
    committed = journal.commit_reconciliation(request)
    assert committed.disposition is ReconciliationCommitDisposition.COMMITTED
    assert committed.resolved_claim_ids == (COMMAND_ID,)
    assert committed.resolved_observation_ids == ()
    assert committed.resolved_requirement_ids == ()
    assert committed.order_projections == ()
    assert committed.resulting_journal_sequence is not None
    assert committed.resulting_journal_sequence > before.journal_sequence
    journal.close()

    resumed = _open(path, JournalOpenMode.RESUME)
    after = resumed.load_recovery_snapshot()
    after_verification = verify_recovery_snapshot(after)
    account = resumed.load_account_snapshot(ACCOUNT_ID)
    repeated_claim = resumed.claim_dispatch(
        command.client_command_id,
        payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
        expected_order_version=accepted.version,
        claimant_id="fake-dispatcher-after-restart",
    )
    exact_retry = resumed.commit_reconciliation(request)
    resumed.close()

    assert after.orders == (accepted,)
    assert after.outstanding_claims == ()
    assert after.unresolved_observations == ()
    assert after.reconciliation_requirements == ()
    assert after_verification.readiness is RecoveryReadiness.READY
    assert not after_verification.may_dispatch
    assert account.recovery_blockers == ()
    assert repeated_claim.disposition is DispatchClaimDisposition.ALREADY_CLAIMED
    assert repeated_claim.claim_token is None
    assert exact_retry.disposition is ReconciliationCommitDisposition.EXACT_RETRY
    assert exact_retry.committed_at == committed.committed_at
    assert exact_retry.resulting_journal_sequence == committed.resulting_journal_sequence
    assert all(item.source != "dispatch" for item in after.applied_event_ledger.events)


@pytest.mark.parametrize(
    ("include_order", "completeness"),
    [
        (True, EvidenceCompleteness.INCOMPLETE),
        (False, EvidenceCompleteness.COMPLETE),
    ],
)
def test_non_authoritative_or_discrepant_assessment_cannot_resolve_claim(
    tmp_path: Path,
    include_order: bool,
    completeness: EvidenceCompleteness,
) -> None:
    journal = _open(
        tmp_path / f"rejected-{include_order}-{completeness}.sqlite3", JournalOpenMode.CREATE_NEW
    )
    _, accepted = _register_claim_and_accept(journal)
    assessment = _assessment(
        journal,
        _broker_snapshot(include_order=include_order, completeness=completeness),
    )
    before = journal.load_recovery_snapshot()
    claim = before.outstanding_claims[0]
    request = DurableReconciliationCommitRequest(
        "commit-rejected-integration",
        ACCOUNT_ID,
        assessment,
        assessment.local_snapshot.journal_sequence,
        expected_order_versions=(ExpectedOrderVersion(ORDER_ID, accepted.version),),
        claim_resolutions=(
            ClaimResolutionDirective(
                COMMAND_ID,
                claim.claim_token,
                ClaimResolution.BROKER_ORDER_CONFIRMED,
            ),
        ),
    )

    result = journal.commit_reconciliation(request)
    after = journal.load_recovery_snapshot()
    journal.close()

    assert result.disposition is ReconciliationCommitDisposition.NOT_AUTHORITATIVE
    assert result.committed_at is None
    assert after == before
    assert not assessment.may_dispatch


def test_stale_snapshot_and_cross_account_request_fail_without_writes(tmp_path: Path) -> None:
    journal = _open(tmp_path / "stale.sqlite3", JournalOpenMode.CREATE_NEW)
    _register_claim_and_accept(journal)
    request = _claim_request(journal, commit_id="commit-stale-integration")
    raw = RawBrokerObservation(
        "raw-after-assessment-integration",
        "fake-broker-reply",
        1,
        2,
        SNAPSHOT_AT,
        b"durable-after-assessment",
    )
    journal.append_raw_observation(raw)
    before = journal.load_recovery_snapshot()

    stale = journal.commit_reconciliation(request)
    after = journal.load_recovery_snapshot()
    assert stale.disposition is ReconciliationCommitDisposition.STALE_SNAPSHOT
    assert after == before

    with pytest.raises(ValueError, match="assessment must match request account"):
        DurableReconciliationCommitRequest(
            "commit-cross-account-integration",
            "other-account",
            request.assessment,
            request.expected_journal_sequence,
            claim_resolutions=request.claim_resolutions,
        )
    assert journal.load_recovery_snapshot() == before
    journal.close()


def test_wrong_order_version_cas_fails_without_writes(tmp_path: Path) -> None:
    journal = _open(tmp_path / "wrong-order-version.sqlite3", JournalOpenMode.CREATE_NEW)
    _register_claim_and_accept(journal)
    request = _claim_request(journal, commit_id="commit-wrong-version-integration")
    expected = request.expected_order_versions[0]
    wrong_version = replace(
        request,
        expected_order_versions=(
            ExpectedOrderVersion(expected.client_order_id, expected.version - 1),
        ),
    )
    before = journal.load_recovery_snapshot()

    result = journal.commit_reconciliation(wrong_version)
    assert result.disposition is ReconciliationCommitDisposition.VERSION_MISMATCH
    assert journal.load_recovery_snapshot() == before
    journal.close()


def test_absence_only_outcome_unknown_claim_is_not_authoritative(tmp_path: Path) -> None:
    journal = _open(tmp_path / "absence-only.sqlite3", JournalOpenMode.CREATE_NEW)
    command, submitting = _submission()
    journal.register_new_order(
        command,
        submitting,
        intent_fingerprint=intent_fingerprint(command.intent),
    )
    claim = journal.claim_dispatch(
        command.client_command_id,
        payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
        expected_order_version=submitting.version,
        claimant_id="fake-dispatcher",
    )
    assessment = _assessment(journal, _broker_snapshot(include_order=False))
    before = journal.load_recovery_snapshot()
    request = DurableReconciliationCommitRequest(
        "commit-absence-only-integration",
        ACCOUNT_ID,
        assessment,
        assessment.local_snapshot.journal_sequence,
        expected_order_versions=(ExpectedOrderVersion(ORDER_ID, submitting.version),),
        claim_resolutions=(
            ClaimResolutionDirective(
                COMMAND_ID,
                claim.claim_token or CLAIM_TOKEN,
                ClaimResolution.BROKER_ORDER_CONFIRMED,
            ),
        ),
    )

    result = journal.commit_reconciliation(request)
    assert result.disposition is ReconciliationCommitDisposition.NOT_AUTHORITATIVE
    assert journal.load_recovery_snapshot() == before
    assert not assessment.may_dispatch
    journal.close()


def test_conflict_observation_and_requirement_must_be_resolved_together(tmp_path: Path) -> None:
    journal = _open(tmp_path / "conflict-resolution.sqlite3", JournalOpenMode.CREATE_NEW)
    _, accepted = _register_claim_and_accept(journal)
    claim_request = _claim_request(journal, commit_id="commit-precondition-claim")
    assert journal.commit_reconciliation(claim_request).disposition is (
        ReconciliationCommitDisposition.COMMITTED
    )

    conflict_at = ACCEPTED_AT + timedelta(seconds=1)
    raw = RawBrokerObservation(
        "raw-conflict-commit-integration",
        "fake-broker-reply",
        1,
        2,
        conflict_at,
        b"conflicting-instrument",
    )
    journal.append_raw_observation(raw)
    event = NormalizedBrokerOrderEvent(
        "event-conflict-commit-integration",
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
            broker_order_sequence="broker-conflict-commit-integration",
            client_order_id=ORDER_ID,
        ),
    )
    journal.apply_normalized_event(
        event,
        raw_observation_id=raw.observation_id,
        expected_order_version=accepted.version,
    )
    blocked = journal.load_recovery_snapshot()
    requirement = blocked.reconciliation_requirements[0]
    assessment = _assessment(
        journal,
        _broker_snapshot(snapshot_id="snapshot-conflict-commit-integration"),
    )
    observation_directive = ObservationResolutionDirective(
        raw.observation_id,
        ObservationStatus.CONFLICT,
        event.event_id,
        ObservationResolution.BROKER_ORDER_CONFIRMED,
    )
    partial = DurableReconciliationCommitRequest(
        "commit-partial-blockers-integration",
        ACCOUNT_ID,
        assessment,
        assessment.local_snapshot.journal_sequence,
        expected_order_versions=(ExpectedOrderVersion(ORDER_ID, accepted.version),),
        observation_resolutions=(observation_directive,),
    )

    rejected = journal.commit_reconciliation(partial)
    assert rejected.disposition is ReconciliationCommitDisposition.VERSION_MISMATCH
    assert journal.load_recovery_snapshot() == blocked

    complete = replace(
        partial,
        commit_id="commit-all-blockers-integration",
        requirement_resolutions=(
            RequirementResolutionDirective(
                requirement.requirement_id,
                RequirementResolution.SATISFIED,
            ),
        ),
    )
    committed = journal.commit_reconciliation(complete)
    assert committed.disposition is ReconciliationCommitDisposition.COMMITTED
    assert committed.resolved_observation_ids == (raw.observation_id,)
    assert committed.resolved_requirement_ids == (requirement.requirement_id,)
    journal.close()

    resumed = _open(tmp_path / "conflict-resolution.sqlite3", JournalOpenMode.RESUME)
    recovered = resumed.load_recovery_snapshot()
    verification = verify_recovery_snapshot(recovered)
    assert recovered.conflict_observations == ()
    assert recovered.reconciliation_requirements == ()
    assert verification.readiness is RecoveryReadiness.READY
    assert not verification.may_dispatch
    assert resumed.load_account_snapshot(ACCOUNT_ID).recovery_blockers == ()
    resumed.close()
