from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import count
from hashlib import sha256
import json
from pathlib import Path
import sqlite3

import pytest

from tests.support.live_authorization_audit_scenarios import commit_authorized

from tx_trade.orders import sqlite_live_order_journal as journal_module
from tx_trade.orders.live_contracts import (
    BrokerCorrelation,
    BrokerOrderEventType,
    BrokerOpenOrderObservation,
    CorrelationStatus,
    DispatchReceipt,
    DispatchState,
    FingerprintDomain,
    LiveFailureCode,
    LiveOrder,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    NormalizedBrokerOrderEvent,
    PendingCommandBinding,
    ReconciliationDiscrepancy,
    ReconciliationKind,
    payload_fingerprint,
)
from tx_trade.orders.live_journal_contracts import (
    JournalOpenMode,
    LiveJournalIntegrityError,
    ReceiptRecordDisposition,
    intent_fingerprint,
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
    ReconciliationResult,
    ReconciliationStatus,
)
from tx_trade.orders.live_reconciliation import assess_reconciliation
from tx_trade.orders.live_reconciliation_commit_contracts import (
    ClaimResolution,
    ClaimResolutionDirective,
    DurableReconciliationCommitRequest,
    ExpectedOrderVersion,
    ReconciliationCommitDisposition,
)
from tx_trade.orders.live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    ReconciliationAssessment,
)
from tx_trade.orders.live_state_machine import advance_local, create_live_order
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

NOW = datetime(2026, 8, 3, tzinfo=timezone.utc)


def _intent(order_id: str = "order-1", account_id: str = "account-1") -> LiveOrderIntent:
    return LiveOrderIntent(
        strategy_id="strategy-1",
        client_order_id=order_id,
        account_id=account_id,
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=NOW,
    )


def _submitting(intent: LiveOrderIntent) -> tuple[NewOrderCommand, LiveOrder]:
    command = NewOrderCommand(f"command-{intent.client_order_id}", intent, NOW)
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    order = advance_local(create_live_order(intent), LiveOrderState.VALIDATED, NOW)
    return command, advance_local(
        order,
        LiveOrderState.SUBMITTING,
        NOW,
        PendingCommandBinding(command, fingerprint),
    )


def _journal(path: Path, mode: JournalOpenMode, *, hour: int = 1):
    tokens = count(1)
    return SqliteLiveOrderJournal(
        path,
        mode,
        clock=lambda: NOW + timedelta(hours=hour),
        claim_token_factory=lambda: f"claim-token-{next(tokens)}",
        journal_id="journal-commit" if mode is JournalOpenMode.CREATE_NEW else None,
    )


def _register_claim(
    journal: SqliteLiveOrderJournal,
    *,
    order_id: str = "order-1",
    account_id: str = "account-1",
) -> tuple[NewOrderCommand, LiveOrder, str]:
    command, order = _submitting(_intent(order_id, account_id))
    journal.register_new_order(
        command, order, intent_fingerprint=intent_fingerprint(command.intent)
    )
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    claim = journal.claim_dispatch(
        command.client_command_id,
        fingerprint,
        expected_order_version=order.version,
        claimant_id="dispatcher-1",
    )
    assert claim.disposition is DispatchClaimDisposition.ACQUIRED
    assert claim.claim_token is not None
    return command, order, claim.claim_token


def _accept_claim(
    journal: SqliteLiveOrderJournal,
    order: LiveOrder,
    *,
    sequence: int = 1,
) -> LiveOrder:
    received_at = NOW + timedelta(minutes=1, seconds=sequence)
    raw = RawBrokerObservation(
        f"raw-accepted-{order.intent.client_order_id}",
        "capital-primary",
        1,
        sequence,
        received_at,
        b"accepted",
    )
    journal.append_raw_observation(raw)
    event = NormalizedBrokerOrderEvent(
        event_id=f"accepted-{order.intent.client_order_id}",
        account_id=order.intent.account_id,
        instrument_id=order.intent.instrument_id,
        event_type=BrokerOrderEventType.NEW_ACCEPTED,
        received_at=received_at,
        broker_session_generation=1,
        adapter_received_sequence=sequence,
        correlation=BrokerCorrelation(
            1,
            sequence,
            CorrelationStatus.CONFIRMED,
            received_at,
            broker_order_sequence=f"broker-{order.intent.client_order_id}",
            client_order_id=order.intent.client_order_id,
        ),
    )
    result = journal.apply_normalized_event(
        event,
        raw_observation_id=raw.observation_id,
        expected_order_version=order.version,
    )
    assert result.order is not None
    assert result.order.pending_command is None
    return result.order


def _evidence(
    kind: EvidenceQueryKind,
    snapshot_id: str,
    *,
    complete: bool = True,
) -> CompletenessEvidence:
    return CompletenessEvidence(
        kind,
        "account-1",
        EvidenceCompleteness.COMPLETE if complete else EvidenceCompleteness.INCOMPLETE,
        NOW + timedelta(hours=1),
        snapshot_id,
        None if complete else "offline fixture intentionally incomplete",
    )


def _assessment(
    journal: SqliteLiveOrderJournal,
    order: LiveOrder,
    *,
    snapshot_id: str = "snapshot-1",
    include_order: bool = True,
    complete: bool = True,
    discrepant: bool = False,
) -> ReconciliationAssessment:
    local = journal.load_account_snapshot("account-1")
    correlation = BrokerCorrelation(
        1,
        1,
        CorrelationStatus.CONFIRMED,
        NOW + timedelta(hours=1),
        broker_order_sequence="broker-order-1",
        client_order_id=order.intent.client_order_id,
    )
    broker_order = BrokerOpenOrderObservation(
        "broker-observation-1",
        "account-1",
        order.intent.instrument_id,
        order.intent.side,
        order.total_quantity,
        order.remaining_quantity,
        order.intent.limit_price,
        correlation,
        NOW + timedelta(hours=1),
    )
    open_evidence = _evidence(EvidenceQueryKind.OPEN_ORDERS, snapshot_id, complete=complete)
    fill_evidence = _evidence(EvidenceQueryKind.FILLS, snapshot_id, complete=complete)
    position_evidence = _evidence(EvidenceQueryKind.POSITIONS, snapshot_id, complete=complete)
    open_orders = OpenOrdersSnapshot((broker_order,) if include_order else (), open_evidence)
    fills = BrokerFillsSnapshot((), fill_evidence)
    positions = BrokerPositionsSnapshot((), position_evidence)
    captured_at = NOW + timedelta(hours=1)
    broker = BrokerReconciliationSnapshot(
        snapshot_id, "account-1", open_orders, fills, positions, captured_at
    )
    discrepancies = (
        (
            ReconciliationDiscrepancy(
                "discrepancy-1",
                ReconciliationKind.ORDER_STATE_MISMATCH,
                "account-1",
                order.intent.instrument_id,
                captured_at,
                order.intent.client_order_id,
            ),
        )
        if discrepant
        else ()
    )
    result = ReconciliationResult(
        "account-1",
        ReconciliationStatus.COMPLETE if complete else ReconciliationStatus.INCOMPLETE,
        discrepancies,
        (open_evidence, fill_evidence, position_evidence),
        NOW + timedelta(hours=1),
    )
    return ReconciliationAssessment(local, broker, result)


def _projection(order: LiveOrder) -> LiveOrder:
    accepted_at = NOW + timedelta(minutes=30)
    return replace(
        order,
        state=LiveOrderState.ACCEPTED,
        pending_command=None,
        accepted_at=accepted_at,
        updated_at=accepted_at,
        version=order.version + 1,
    )


def _request(
    journal: SqliteLiveOrderJournal,
    order: LiveOrder,
    claim_token: str,
    *,
    commit_id: str = "commit-1",
    snapshot_id: str = "snapshot-1",
    include_order: bool = True,
    complete: bool = True,
    discrepant: bool = False,
) -> DurableReconciliationCommitRequest:
    assessment = _assessment(
        journal,
        order,
        snapshot_id=snapshot_id,
        include_order=include_order,
        complete=complete,
        discrepant=discrepant,
    )
    return DurableReconciliationCommitRequest(
        commit_id,
        "account-1",
        assessment,
        assessment.local_snapshot.journal_sequence,
        (ExpectedOrderVersion(order.intent.client_order_id, order.version),),
        (
            ClaimResolutionDirective(
                f"command-{order.intent.client_order_id}",
                claim_token,
                ClaimResolution.BROKER_ORDER_CONFIRMED,
            ),
        ),
    )


_COUNTED_TABLES = (
    "live_journal_records",
    "live_orders",
    "live_order_history",
    "live_dispatch_claims",
    "live_dispatch_receipts",
    "live_reconciliation_commits",
    "live_dispatch_claim_resolutions",
    "live_observation_reconciliation_resolutions",
    "live_reconciliation_requirement_resolutions",
)


def _durable_state(path: Path) -> tuple[int, tuple[int, ...]]:
    connection = sqlite3.connect(path)
    try:
        sequence = connection.execute(
            "SELECT coalesce(max(journal_sequence), 0) FROM live_journal_records"
        ).fetchone()[0]
        counts = tuple(
            connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in _COUNTED_TABLES
        )
        return sequence, counts
    finally:
        connection.close()


def _scalar_fact_digest(domain: str, values: dict[str, str | int]) -> str:
    payload = json.dumps(
        values,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return f"sha256:{sha256(domain.encode('ascii') + bytes((0,)) + payload).hexdigest()}"


def test_commit_is_atomic_exact_and_survives_resume(tmp_path: Path) -> None:
    path = tmp_path / "commit.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    command, order, token = _register_claim(journal)
    order = _accept_claim(journal, order)
    request = _request(journal, order, token)
    assert request.assessment.result.is_authoritative
    assert request.assessment.local_snapshot.account_id == request.account_id
    assert request.assessment.broker_snapshot.account_id == request.account_id
    assert request.assessment.result.account_id == request.account_id
    assert request.assessment.local_snapshot.journal_sequence == request.expected_journal_sequence
    before = _durable_state(path)

    result = commit_authorized(journal, request)

    assert result.disposition is ReconciliationCommitDisposition.COMMITTED
    assert result.resulting_journal_sequence == before[0] + 3
    assert result.resolved_claim_ids == (command.client_command_id,)
    assert result.order_projections == ()
    assert journal.load_account_snapshot("account-1").recovery_blockers == ()
    assert journal.get_order(order.intent.client_order_id) == order
    committed_state = _durable_state(path)
    assert committed_state[0] == before[0] + 3
    assert committed_state[1][5:7] == (1, 1)

    journal.close()
    resumed = _journal(path, JournalOpenMode.RESUME, hour=2)
    assert commit_authorized(resumed, request) == replace(
        result, disposition=ReconciliationCommitDisposition.EXACT_RETRY
    )
    assert _durable_state(path) == committed_state
    redispatch = resumed.claim_dispatch(
        command.client_command_id,
        payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
        expected_order_version=order.version,
        claimant_id="late-dispatcher",
    )
    assert redispatch.disposition is DispatchClaimDisposition.ALREADY_CLAIMED
    late = resumed.record_dispatch_receipt(
        DispatchReceipt(
            command.client_command_id,
            payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
            DispatchState.UNKNOWN,
            NOW + timedelta(hours=2),
            None,
            LiveFailureCode.DISPATCH_OUTCOME_UNKNOWN,
        ),
        claim_token=token,
        expected_order_version=order.version,
    )
    assert late.disposition is not ReceiptRecordDisposition.RECORDED
    assert _durable_state(path) == committed_state
    resumed.close()


def test_exact_retry_after_unrelated_sequence_advance_writes_nothing(tmp_path: Path) -> None:
    path = tmp_path / "retry.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    _, order, token = _register_claim(journal)
    order = _accept_claim(journal, order)
    request = _request(journal, order, token)
    first = commit_authorized(journal, request)
    _register_claim(journal, order_id="order-2", account_id="account-2")
    advanced = _durable_state(path)

    retry = commit_authorized(journal, request)

    assert retry == replace(first, disposition=ReconciliationCommitDisposition.EXACT_RETRY)
    assert retry.committed_at == first.committed_at
    assert retry.resulting_journal_sequence == first.resulting_journal_sequence
    assert _durable_state(path) == advanced


def test_id_conflict_stale_sequence_wrong_version_and_token_are_no_write(
    tmp_path: Path,
) -> None:
    path = tmp_path / "cas.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    _, order, token = _register_claim(journal)
    order = _accept_claim(journal, order)
    original = _request(journal, order, token)
    committed = commit_authorized(journal, original)
    stable = _durable_state(path)

    conflict = _request(
        journal,
        order,
        token,
        commit_id=original.commit_id,
        snapshot_id="snapshot-conflict",
    )
    assert (
        commit_authorized(journal, conflict).disposition
        is ReconciliationCommitDisposition.ID_CONFLICT
    )
    assert _durable_state(path) == stable

    journal.close()
    stale_path = tmp_path / "stale.sqlite3"
    journal = _journal(stale_path, JournalOpenMode.CREATE_NEW)
    _, other_order, other_token = _register_claim(journal, order_id="order-2")
    other_order = _accept_claim(journal, other_order, sequence=2)
    current = _request(
        journal,
        other_order,
        other_token,
        commit_id="commit-2",
        snapshot_id="snapshot-2",
    )
    _register_claim(journal, order_id="order-other-account", account_id="account-2")
    before_rejections = _durable_state(stale_path)
    assert (
        commit_authorized(journal, current).disposition
        is ReconciliationCommitDisposition.STALE_SNAPSHOT
    )
    assert _durable_state(stale_path) == before_rejections

    current = _request(
        journal,
        other_order,
        other_token,
        commit_id="commit-3",
        snapshot_id="snapshot-3",
    )
    before_rejections = _durable_state(stale_path)

    wrong_version = replace(
        current,
        expected_order_versions=(
            ExpectedOrderVersion(other_order.intent.client_order_id, other_order.version - 1),
        ),
    )
    assert (
        commit_authorized(journal, wrong_version).disposition
        is ReconciliationCommitDisposition.VERSION_MISMATCH
    )
    assert _durable_state(stale_path) == before_rejections

    wrong_token = replace(
        current,
        claim_resolutions=(
            ClaimResolutionDirective(
                f"command-{other_order.intent.client_order_id}",
                "wrong-token",
                ClaimResolution.BROKER_ORDER_CONFIRMED,
            ),
        ),
    )
    assert (
        commit_authorized(journal, wrong_token).disposition
        is ReconciliationCommitDisposition.VERSION_MISMATCH
    )
    assert _durable_state(stale_path) == before_rejections
    assert committed.disposition is ReconciliationCommitDisposition.COMMITTED


@pytest.mark.parametrize(
    ("changes", "expected"),
    (
        ({"complete": False}, ReconciliationCommitDisposition.NOT_AUTHORITATIVE),
        ({"discrepant": True}, ReconciliationCommitDisposition.NOT_AUTHORITATIVE),
        ({"include_order": False}, ReconciliationCommitDisposition.NOT_AUTHORITATIVE),
    ),
)
def test_weak_or_absence_only_evidence_is_rejected_without_writes(
    tmp_path: Path,
    changes: dict[str, bool],
    expected: ReconciliationCommitDisposition,
) -> None:
    path = tmp_path / f"evidence-{next(iter(changes))}.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    _, order, token = _register_claim(journal)
    request = _request(journal, order, token, **changes)
    before = _durable_state(path)
    assert commit_authorized(journal, request).disposition is expected
    assert _durable_state(path) == before


@pytest.mark.parametrize("mismatch", ("quantity", "price"))
def test_forged_clean_result_cannot_hide_broker_order_mismatch(
    tmp_path: Path,
    mismatch: str,
) -> None:
    path = tmp_path / f"forged-clean-{mismatch}.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    _, order, token = _register_claim(journal)
    order = _accept_claim(journal, order)
    request = _request(journal, order, token)
    original = request.assessment.broker_snapshot.open_orders.orders[0]
    mismatched = (
        replace(
            original,
            working_total_quantity=Decimal("2"),
            working_remaining_quantity=Decimal("2"),
        )
        if mismatch == "quantity"
        else replace(original, working_limit_price=Decimal("22001"))
    )
    broker = replace(
        request.assessment.broker_snapshot,
        open_orders=OpenOrdersSnapshot(
            (mismatched,),
            request.assessment.broker_snapshot.open_orders.evidence,
        ),
    )
    canonical = assess_reconciliation(
        request.assessment.local_snapshot,
        broker,
        request.assessment.result.reconciled_at,
    )
    assert canonical.result.discrepancies
    forged = ReconciliationAssessment(
        request.assessment.local_snapshot,
        broker,
        request.assessment.result,
    )
    forged_request = replace(request, assessment=forged)
    before = _durable_state(path)

    result = commit_authorized(journal, forged_request)

    assert result.disposition is ReconciliationCommitDisposition.NOT_AUTHORITATIVE
    assert _durable_state(path) == before


def test_semantically_identical_broker_observations_are_deduplicated_for_commit(
    tmp_path: Path,
) -> None:
    path = tmp_path / "semantic-duplicate.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    _, order, token = _register_claim(journal)
    order = _accept_claim(journal, order)
    request = _request(journal, order, token)
    original = request.assessment.broker_snapshot.open_orders.orders[0]
    duplicate = replace(original, observation_id="broker-observation-alias")
    broker = replace(
        request.assessment.broker_snapshot,
        open_orders=OpenOrdersSnapshot(
            (original, duplicate),
            request.assessment.broker_snapshot.open_orders.evidence,
        ),
    )
    canonical = assess_reconciliation(
        request.assessment.local_snapshot,
        broker,
        request.assessment.result.reconciled_at,
    )
    assert canonical.result.discrepancies == ()
    duplicate_request = replace(request, assessment=canonical)

    committed = commit_authorized(journal, duplicate_request)

    assert committed.disposition is ReconciliationCommitDisposition.COMMITTED
    assert committed.resolved_claim_ids == (f"command-{order.intent.client_order_id}",)


def test_empty_and_multi_target_with_one_bad_target_are_atomic_no_write(tmp_path: Path) -> None:
    path = tmp_path / "atomic.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    _, first_order, first_token = _register_claim(journal)
    first_order = _accept_claim(journal, first_order)
    base = _request(journal, first_order, first_token)
    empty = replace(
        base,
        commit_id="commit-empty",
        expected_order_versions=(),
        claim_resolutions=(),
    )
    before = _durable_state(path)
    assert (
        commit_authorized(journal, empty).disposition
        is ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION
    )
    assert _durable_state(path) == before

    _, second_order, _ = _register_claim(journal, order_id="order-2")
    second_order = _accept_claim(journal, second_order, sequence=2)
    base = _request(journal, first_order, first_token)
    first_broker_order = base.assessment.broker_snapshot.open_orders.orders[0]
    second_broker_order = replace(
        first_broker_order,
        observation_id="broker-observation-2",
        correlation=replace(
            first_broker_order.correlation,
            adapter_received_sequence=2,
            broker_order_sequence="broker-order-2",
            client_order_id=second_order.intent.client_order_id,
        ),
    )
    broker = replace(
        base.assessment.broker_snapshot,
        open_orders=OpenOrdersSnapshot(
            (first_broker_order, second_broker_order),
            base.assessment.broker_snapshot.open_orders.evidence,
        ),
    )
    base = replace(
        base,
        assessment=assess_reconciliation(
            base.assessment.local_snapshot,
            broker,
            base.assessment.result.reconciled_at,
        ),
    )
    before = _durable_state(path)

    bad_second = replace(
        base,
        commit_id="commit-atomic",
        expected_order_versions=(
            ExpectedOrderVersion(first_order.intent.client_order_id, first_order.version),
            ExpectedOrderVersion(second_order.intent.client_order_id, second_order.version),
        ),
        claim_resolutions=(
            *base.claim_resolutions,
            ClaimResolutionDirective(
                "command-order-2",
                "wrong-token",
                ClaimResolution.BROKER_ORDER_CONFIRMED,
            ),
        ),
    )
    result = commit_authorized(journal, bad_second)
    assert result.disposition in {
        ReconciliationCommitDisposition.VERSION_MISMATCH,
        ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION,
    }
    assert _durable_state(path) == before


def test_order_projection_is_explicitly_unsupported_and_writes_nothing(tmp_path: Path) -> None:
    path = tmp_path / "projection.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    _, order, token = _register_claim(journal)
    order = _accept_claim(journal, order)
    base = _request(journal, order, token)
    projected = replace(base, order_projections=(_projection(order),))
    before = _durable_state(path)
    assert (
        commit_authorized(journal, projected).disposition
        is ReconciliationCommitDisposition.UNSUPPORTED_RESOLUTION
    )
    assert _durable_state(path) == before


@pytest.mark.parametrize(
    "tamper_sql",
    (
        "DELETE FROM live_journal_records WHERE record_kind = 'reconciliation-commit'",
        "UPDATE live_dispatch_claim_resolutions SET commit_id = 'orphan-commit'",
        "UPDATE live_reconciliation_commits SET resulting_journal_sequence = 999",
    ),
)
def test_commit_mapping_tamper_fails_closed(tmp_path: Path, tamper_sql: str) -> None:
    path = tmp_path / "tamper.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    _, order, token = _register_claim(journal)
    order = _accept_claim(journal, order)
    assert (
        commit_authorized(journal, _request(journal, order, token)).disposition
        is ReconciliationCommitDisposition.COMMITTED
    )
    journal.close()
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute(tamper_sql)
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(LiveJournalIntegrityError):
        _journal(path, JournalOpenMode.RESUME)


def test_missing_requested_overlay_fails_closed_despite_repaired_sequence_and_digest(
    tmp_path: Path,
) -> None:
    path = tmp_path / "missing-requested-overlay.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    command, order, token = _register_claim(journal)
    order = _accept_claim(journal, order)
    request = _request(journal, order, token)
    committed = commit_authorized(journal, request)
    assert committed.disposition is ReconciliationCommitDisposition.COMMITTED
    journal.close()

    connection = sqlite3.connect(path)
    try:
        row = connection.execute(
            """SELECT account_id, request_digest, base_journal_sequence,
                      resulting_journal_sequence, committed_at
               FROM live_reconciliation_commits WHERE commit_id = ?""",
            (request.commit_id,),
        ).fetchone()
        assert row is not None
        repaired_sequence = int(row[3]) - 1
        repaired_digest = _scalar_fact_digest(
            "tx_trade.live.journal.reconciliation-commit.v2",
            {
                "commit_id": request.commit_id,
                "account_id": row[0],
                "request_digest": row[1],
                "base_sequence": int(row[2]),
                "resulting_sequence": repaired_sequence,
                "committed_at": row[4],
            },
        )
        authorization_row = connection.execute(
            """SELECT authorization_id, authorization_digest, consumed_at
               FROM live_reconciliation_commit_authorizations WHERE commit_id = ?""",
            (request.commit_id,),
        ).fetchone()
        authorization_trigger = connection.execute(
            """SELECT sql FROM sqlite_master
               WHERE type = 'trigger'
                 AND name = 'live_reconciliation_commit_authorizations_no_update'"""
        ).fetchone()
        assert authorization_row is not None and authorization_trigger is not None
        repaired_authorization_fact = journal_module._authorization_fact_digest(
            authorization_row[1],
            request.commit_id,
            authorization_row[2],
            repaired_sequence,
        )
        connection.execute(
            "DELETE FROM live_dispatch_claim_resolutions WHERE client_command_id = ?",
            (command.client_command_id,),
        )
        connection.execute(
            """DELETE FROM live_journal_records
               WHERE record_kind = 'dispatch-claim-resolution' AND record_id = ?""",
            (command.client_command_id,),
        )
        connection.execute(
            """UPDATE live_journal_records
               SET journal_sequence = ?, payload_digest = ?
               WHERE record_kind = 'reconciliation-commit' AND record_id = ?""",
            (repaired_sequence, repaired_digest, request.commit_id),
        )
        connection.execute(
            """UPDATE live_reconciliation_commits SET resulting_journal_sequence = ?
               WHERE commit_id = ?""",
            (repaired_sequence, request.commit_id),
        )
        connection.execute(
            "UPDATE sqlite_sequence SET seq = ? WHERE name = 'live_journal_records'",
            (repaired_sequence,),
        )
        connection.execute("DROP TRIGGER live_reconciliation_commit_authorizations_no_update")
        connection.execute(
            """UPDATE live_reconciliation_commit_authorizations
               SET resulting_journal_sequence = ? WHERE commit_id = ?""",
            (repaired_sequence, request.commit_id),
        )
        connection.execute(authorization_trigger[0])
        connection.execute(
            """UPDATE live_journal_records SET payload_digest = ?
               WHERE record_kind = 'operator-authorization' AND record_id = ?""",
            (repaired_authorization_fact, authorization_row[0]),
        )
        connection.commit()

        assert connection.execute(
            """SELECT count(*), min(journal_sequence), max(journal_sequence)
               FROM live_journal_records"""
        ).fetchone() == (repaired_sequence, 1, repaired_sequence)
        assert connection.execute(
            """SELECT payload_digest FROM live_journal_records
               WHERE record_kind = 'reconciliation-commit' AND record_id = ?""",
            (request.commit_id,),
        ).fetchone() == (repaired_digest,)
        assert connection.execute(
            "SELECT count(*) FROM live_dispatch_claim_resolutions"
        ).fetchone() == (0,)
        assert connection.execute(
            """SELECT a.resulting_journal_sequence, r.journal_sequence, r.payload_digest
               FROM live_reconciliation_commit_authorizations a
               JOIN live_reconciliation_commits c ON c.commit_id = a.commit_id
               JOIN live_journal_records r
                 ON r.record_kind = 'operator-authorization'
                AND r.record_id = a.authorization_id
               WHERE a.commit_id = ?
                 AND a.request_digest = c.request_digest
                 AND a.authorization_digest = ?""",
            (request.commit_id, authorization_row[1]),
        ).fetchone() == (
            repaired_sequence,
            int(row[2]) + 1,
            repaired_authorization_fact,
        )
    finally:
        connection.close()

    with pytest.raises(
        LiveJournalIntegrityError,
        match="live journal reconciliation commit mapping is invalid",
    ):
        _journal(path, JournalOpenMode.RESUME)
