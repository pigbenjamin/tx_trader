from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from itertools import count
from pathlib import Path
import sqlite3

import pytest

from tests.support.live_authorization_audit_scenarios import commit_authorized

from tx_trade.orders import sqlite_live_order_journal as journal_module
from tx_trade.orders.live_contracts import (
    BrokerCorrelation,
    BrokerOpenOrderObservation,
    BrokerOrderEventType,
    CancelOrderCommand,
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
    payload_fingerprint,
)
from tx_trade.orders.live_journal_contracts import (
    JournalOpenMode,
    LiveJournalIntegrityError,
    OutstandingDispatchClaim,
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
)
from tx_trade.orders.live_reconciliation import assess_reconciliation
from tx_trade.orders.live_reconciliation_commit_contracts import (
    ClaimResolution,
    ClaimResolutionDirective,
    DurableReconciliationCommitRequest,
    ExpectedOrderVersion,
    ReconciliationCommitDisposition,
)
from tx_trade.orders.live_reconciliation_contracts import BrokerReconciliationSnapshot
from tx_trade.orders.live_reconciliation_projection import project_authoritative_orders
from tx_trade.orders.live_reconciliation_projection_contracts import (
    OrderProjectionDisposition,
)
from tx_trade.orders.live_state_machine import advance_local, create_live_order, request_cancel
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

NOW = datetime(2026, 8, 4, tzinfo=timezone.utc)


def _repair_authorization_binding(
    connection: sqlite3.Connection,
    *,
    commit_id: str,
    request_digest: str,
    resulting_sequence: int,
) -> None:
    row = connection.execute(
        """SELECT authorization_id, principal_id, authority_context_digest,
                  action_kind, journal_id, account_id, source_inspection_digest,
                  operator_plan_digest, broker_snapshot_id,
                  expected_journal_sequence, authorized_at, expires_at,
                  consumed_at, reason_code
           FROM live_reconciliation_commit_authorizations WHERE commit_id = ?""",
        (commit_id,),
    ).fetchone()
    trigger = connection.execute(
        """SELECT sql FROM sqlite_master
           WHERE type = 'trigger'
             AND name = 'live_reconciliation_commit_authorizations_no_update'"""
    ).fetchone()
    assert row is not None and trigger is not None
    authorization = journal_module.ReconciliationCommitAuthorization(
        authorization_id=row[0],
        principal_id=row[1],
        authority_context_digest=row[2],
        action=journal_module.ReconciliationAuthorizationAction(row[3]),
        journal_id=row[4],
        account_id=row[5],
        source_inspection_digest=row[6],
        operator_plan_digest=row[7],
        commit_id=commit_id,
        request_digest=request_digest,
        broker_snapshot_id=row[8],
        expected_journal_sequence=int(row[9]),
        authorized_at=datetime.fromisoformat(row[10]),
        expires_at=datetime.fromisoformat(row[11]),
        reason_code=row[13],
    )
    authorization_digest = journal_module.reconciliation_authorization_digest(authorization)
    authorization_fact = journal_module._authorization_fact_digest(
        authorization_digest,
        commit_id,
        row[12],
        resulting_sequence,
    )
    connection.execute("DROP TRIGGER live_reconciliation_commit_authorizations_no_update")
    connection.execute(
        """UPDATE live_reconciliation_commit_authorizations
           SET request_digest = ?, authorization_digest = ?,
               resulting_journal_sequence = ? WHERE commit_id = ?""",
        (request_digest, authorization_digest, resulting_sequence, commit_id),
    )
    connection.execute(trigger[0])
    connection.execute(
        """UPDATE live_journal_records SET payload_digest = ?
           WHERE record_kind = 'operator-authorization' AND record_id = ?""",
        (authorization_fact, row[0]),
    )
    assert connection.execute(
        """SELECT a.request_digest, a.authorization_digest,
                  a.resulting_journal_sequence, r.journal_sequence, r.payload_digest
           FROM live_reconciliation_commit_authorizations a
           JOIN live_journal_records r
             ON r.record_kind = 'operator-authorization'
            AND r.record_id = a.authorization_id
           WHERE a.commit_id = ?""",
        (commit_id,),
    ).fetchone() == (
        request_digest,
        authorization_digest,
        resulting_sequence,
        int(row[9]) + 1,
        authorization_fact,
    )


UNKNOWN_AT = NOW + timedelta(hours=1)
SNAPSHOT_AT = NOW + timedelta(hours=5)


def _journal(path: Path, mode: JournalOpenMode) -> SqliteLiveOrderJournal:
    tokens = count(1)
    return SqliteLiveOrderJournal(
        path,
        mode,
        clock=lambda: SNAPSHOT_AT,
        claim_token_factory=lambda: f"claim-token-{next(tokens)}",
        journal_id="journal-projection" if mode is JournalOpenMode.CREATE_NEW else None,
    )


def _intent(order_id: str, account_id: str = "account-1") -> LiveOrderIntent:
    return LiveOrderIntent(
        f"strategy-{order_id}",
        order_id,
        account_id,
        f"instrument-{order_id}",
        LiveSide.BUY,
        Decimal("2"),
        LiveOrderType.LIMIT,
        Decimal("22000"),
        LiveTimeInForce.DAY,
        False,
        NOW,
    )


def _register_unknown(
    journal: SqliteLiveOrderJournal,
    order_id: str,
    *,
    sequence: int,
    account_id: str = "account-1",
) -> tuple[NewOrderCommand, LiveOrder, str]:
    intent = _intent(order_id, account_id)
    command = NewOrderCommand(f"command-{order_id}", intent, NOW)
    fingerprint = payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1)
    validated = advance_local(create_live_order(intent), LiveOrderState.VALIDATED, NOW)
    submitting = advance_local(
        validated,
        LiveOrderState.SUBMITTING,
        NOW,
        PendingCommandBinding(command, fingerprint),
    )
    unknown = replace(submitting, state=LiveOrderState.SUBMISSION_UNKNOWN)
    journal.register_new_order(
        command,
        unknown,
        intent_fingerprint=intent_fingerprint(intent),
    )
    claim = journal.claim_dispatch(
        command.client_command_id,
        fingerprint,
        expected_order_version=unknown.version,
        claimant_id="dispatcher-1",
    )
    assert claim.disposition is DispatchClaimDisposition.ACQUIRED
    assert claim.claim_token is not None

    assert sequence > 0
    return command, unknown, claim.claim_token


def _evidence(kind: EvidenceQueryKind, snapshot_id: str) -> CompletenessEvidence:
    return CompletenessEvidence(
        kind,
        "account-1",
        EvidenceCompleteness.COMPLETE,
        SNAPSHOT_AT,
        snapshot_id,
    )


def _accept_unknown_order(
    journal: SqliteLiveOrderJournal,
    order: LiveOrder,
    *,
    sequence: int,
) -> LiveOrder:
    received_at = UNKNOWN_AT + timedelta(minutes=1)
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
        f"event-accepted-{order.intent.client_order_id}",
        order.intent.account_id,
        order.intent.instrument_id,
        BrokerOrderEventType.NEW_ACCEPTED,
        received_at,
        1,
        sequence,
        BrokerCorrelation(
            1,
            sequence,
            CorrelationStatus.CONFIRMED,
            received_at,
            broker_order_sequence=f"broker-accepted-{order.intent.client_order_id}",
            client_order_id=order.intent.client_order_id,
        ),
    )
    applied = journal.apply_normalized_event(
        event,
        raw_observation_id=raw.observation_id,
        expected_order_version=order.version,
    )
    assert applied.order is not None
    assert applied.order.state is LiveOrderState.ACCEPTED
    return applied.order


def _broker_order(order: LiveOrder, sequence: int) -> BrokerOpenOrderObservation:
    return BrokerOpenOrderObservation(
        f"open-{order.intent.client_order_id}",
        "account-1",
        order.intent.instrument_id,
        order.intent.side,
        order.total_quantity,
        order.remaining_quantity,
        order.working_limit_price,
        BrokerCorrelation(
            2,
            sequence,
            CorrelationStatus.CONFIRMED,
            SNAPSHOT_AT - timedelta(seconds=1),
            broker_order_sequence=f"broker-open-{order.intent.client_order_id}",
            client_order_id=order.intent.client_order_id,
        ),
        SNAPSHOT_AT,
    )


def _request(
    journal: SqliteLiveOrderJournal,
    commands_orders_tokens: tuple[tuple[NewOrderCommand, LiveOrder, str], ...],
    *,
    commit_id: str = "projection-commit-1",
    snapshot_id: str = "projection-snapshot-1",
) -> DurableReconciliationCommitRequest:
    local = journal.load_account_snapshot("account-1")
    by_id = {order.intent.client_order_id: order for _, order, _ in commands_orders_tokens}
    broker_orders = tuple(
        _broker_order(by_id[order_id], sequence)
        for sequence, order_id in enumerate(sorted(by_id), 1)
    )
    broker = BrokerReconciliationSnapshot(
        snapshot_id,
        "account-1",
        OpenOrdersSnapshot(
            broker_orders,
            _evidence(EvidenceQueryKind.OPEN_ORDERS, snapshot_id),
        ),
        BrokerFillsSnapshot((), _evidence(EvidenceQueryKind.FILLS, snapshot_id)),
        BrokerPositionsSnapshot((), _evidence(EvidenceQueryKind.POSITIONS, snapshot_id)),
        SNAPSHOT_AT,
    )
    assessment = assess_reconciliation(local, broker, SNAPSHOT_AT)
    plan = project_authoritative_orders(assessment)
    assert plan.disposition is OrderProjectionDisposition.READY
    directives = tuple(
        sorted(
            (
                ClaimResolutionDirective(
                    command.client_command_id,
                    token,
                    ClaimResolution.BROKER_ORDER_CONFIRMED,
                )
                for command, _, token in commands_orders_tokens
            ),
            key=lambda item: item.client_command_id,
        )
    )
    return DurableReconciliationCommitRequest(
        commit_id,
        "account-1",
        assessment,
        local.journal_sequence,
        plan.expected_order_versions,
        directives,
        order_projections=plan.projected_orders,
    )


def _no_projection_request(
    journal: SqliteLiveOrderJournal,
    command: NewOrderCommand,
    order: LiveOrder,
    token: str,
) -> DurableReconciliationCommitRequest:
    snapshot_id = "no-projection-snapshot"
    local = journal.load_account_snapshot("account-1")
    broker = BrokerReconciliationSnapshot(
        snapshot_id,
        "account-1",
        OpenOrdersSnapshot(
            (_broker_order(order, 1),),
            _evidence(EvidenceQueryKind.OPEN_ORDERS, snapshot_id),
        ),
        BrokerFillsSnapshot((), _evidence(EvidenceQueryKind.FILLS, snapshot_id)),
        BrokerPositionsSnapshot((), _evidence(EvidenceQueryKind.POSITIONS, snapshot_id)),
        SNAPSHOT_AT,
    )
    assessment = assess_reconciliation(local, broker, SNAPSHOT_AT)
    assert assessment.result.is_authoritative
    assert assessment.result.discrepancies == ()
    return DurableReconciliationCommitRequest(
        "no-projection-commit",
        "account-1",
        assessment,
        local.journal_sequence,
        (ExpectedOrderVersion(order.intent.client_order_id, order.version),),
        (
            ClaimResolutionDirective(
                command.client_command_id,
                token,
                ClaimResolution.BROKER_ORDER_CONFIRMED,
            ),
        ),
    )


def _state(path: Path) -> tuple[int, tuple[tuple[str, int, str], ...], tuple[int, ...]]:
    connection = sqlite3.connect(path)
    try:
        sequence = int(
            connection.execute(
                "SELECT coalesce(max(journal_sequence), 0) FROM live_journal_records"
            ).fetchone()[0]
        )
        orders = tuple(
            connection.execute(
                "SELECT client_order_id, version, state FROM live_orders ORDER BY client_order_id"
            )
        )
        counts = tuple(
            int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            for table in (
                "live_order_history",
                "live_dispatch_claim_resolutions",
                "live_reconciliation_commits",
                "live_dispatch_receipts",
                "live_fills",
                "live_normalized_events",
            )
        )
        return sequence, orders, counts
    finally:
        connection.close()


def test_projection_claim_commit_is_atomic_exact_and_survives_resume(
    tmp_path: Path,
) -> None:
    path = tmp_path / "projection.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    fixture = _register_unknown(journal, "order-1", sequence=1)
    command, pre_projection, token = fixture
    request = _request(journal, (fixture,))
    before = _state(path)

    result = commit_authorized(journal, request)

    assert result.disposition is ReconciliationCommitDisposition.COMMITTED
    assert result.resulting_journal_sequence == before[0] + 3
    assert result.resolved_claim_ids == (command.client_command_id,)
    assert result.order_projections == request.order_projections
    projected = request.order_projections[0]
    assert journal.get_order(pre_projection.intent.client_order_id) == projected
    assert projected.state is LiveOrderState.ACCEPTED
    assert projected.pending_command is None
    assert projected.accepted_at == projected.updated_at == SNAPSHOT_AT
    snapshot = journal.load_recovery_snapshot()
    assert snapshot.outstanding_claims == ()
    committed_state = _state(path)
    assert committed_state[2][3:] == (0, 0, 0)

    journal.close()
    resumed = _journal(path, JournalOpenMode.RESUME)
    retry = commit_authorized(resumed, request)
    assert retry == replace(result, disposition=ReconciliationCommitDisposition.EXACT_RETRY)
    assert retry.order_projections == request.order_projections
    assert _state(path) == committed_state

    duplicate_claim = resumed.claim_dispatch(
        command.client_command_id,
        payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
        expected_order_version=pre_projection.version,
        claimant_id="late-dispatcher",
    )
    assert duplicate_claim.disposition is not DispatchClaimDisposition.ACQUIRED
    late_receipt = resumed.record_dispatch_receipt(
        DispatchReceipt(
            command.client_command_id,
            payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
            DispatchState.UNKNOWN,
            SNAPSHOT_AT,
            None,
            LiveFailureCode.DISPATCH_OUTCOME_UNKNOWN,
        ),
        claim_token=token,
        expected_order_version=pre_projection.version,
    )
    assert late_receipt.disposition is not ReceiptRecordDisposition.RECORDED
    assert _state(path) == committed_state
    resumed.close()


def test_id_conflict_and_forged_projection_write_nothing(tmp_path: Path) -> None:
    path = tmp_path / "conflict.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    fixture = _register_unknown(journal, "order-1", sequence=1)
    request = _request(journal, (fixture,))
    committed = commit_authorized(journal, request)
    stable = _state(path)

    conflict = replace(
        request,
        claim_resolutions=(
            replace(
                request.claim_resolutions[0],
                resolution=ClaimResolution.BROKER_FILL_CONFIRMED,
            ),
        ),
    )
    assert (
        commit_authorized(journal, conflict).disposition
        is ReconciliationCommitDisposition.ID_CONFLICT
    )
    assert _state(path) == stable
    assert committed.disposition is ReconciliationCommitDisposition.COMMITTED

    journal.close()
    forged_path = tmp_path / "forged.sqlite3"
    journal = _journal(forged_path, JournalOpenMode.CREATE_NEW)
    fixture = _register_unknown(journal, "order-1", sequence=1)
    request = _request(journal, (fixture,))
    forged_order = replace(
        request.order_projections[0],
        accepted_at=SNAPSHOT_AT - timedelta(microseconds=1),
    )
    forged = replace(request, order_projections=(forged_order,))
    before = _state(forged_path)
    assert (
        commit_authorized(journal, forged).disposition
        is ReconciliationCommitDisposition.NOT_AUTHORITATIVE
    )
    assert _state(forged_path) == before


def test_projection_verifier_allows_a_legitimate_later_current_version(
    tmp_path: Path,
) -> None:
    path = tmp_path / "later-version.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    fixture = _register_unknown(journal, "order-1", sequence=1)
    request = _request(journal, (fixture,))
    result = commit_authorized(journal, request)
    assert result.disposition is ReconciliationCommitDisposition.COMMITTED
    projected = request.order_projections[0]

    cancel = CancelOrderCommand("cancel-order-1", "order-1", SNAPSHOT_AT)
    binding = PendingCommandBinding(
        cancel,
        payload_fingerprint(cancel, FingerprintDomain.CANCEL_COMMAND_V1),
    )
    later = request_cancel(projected, binding, SNAPSHOT_AT)
    registered = journal.register_command(
        cancel,
        later,
        expected_order_version=projected.version,
    )
    assert registered.order == later
    journal.close()

    resumed = _journal(path, JournalOpenMode.RESUME)
    assert resumed.get_order("order-1") == later
    assert commit_authorized(resumed, request) == replace(
        result,
        disposition=ReconciliationCommitDisposition.EXACT_RETRY,
    )
    resumed.close()


def test_projection_only_stale_sequence_version_and_token_are_no_write(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rejections.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    fixture = _register_unknown(journal, "order-1", sequence=1)
    request = _request(journal, (fixture,))

    projection_only = replace(request, commit_id="projection-only", claim_resolutions=())
    before = _state(path)
    assert (
        commit_authorized(journal, projection_only).disposition
        is ReconciliationCommitDisposition.VERSION_MISMATCH
    )
    assert _state(path) == before

    missing_projection = replace(
        request,
        commit_id="missing-projection",
        order_projections=(),
    )
    assert (
        commit_authorized(journal, missing_projection).disposition
        is ReconciliationCommitDisposition.NOT_AUTHORITATIVE
    )
    assert _state(path) == before

    wrong_version = replace(
        request,
        commit_id="wrong-version",
        expected_order_versions=(
            ExpectedOrderVersion(
                request.expected_order_versions[0].client_order_id,
                request.expected_order_versions[0].version - 1,
            ),
        ),
        order_projections=(
            replace(request.order_projections[0], version=request.order_projections[0].version - 1),
        ),
    )
    assert (
        commit_authorized(journal, wrong_version).disposition
        is ReconciliationCommitDisposition.NOT_AUTHORITATIVE
    )
    assert _state(path) == before

    wrong_token = replace(
        request,
        commit_id="wrong-token",
        claim_resolutions=(replace(request.claim_resolutions[0], claim_token="wrong-token"),),
    )
    assert (
        commit_authorized(journal, wrong_token).disposition
        is ReconciliationCommitDisposition.VERSION_MISMATCH
    )
    assert _state(path) == before

    stale = replace(request, commit_id="stale-sequence")
    journal.append_raw_observation(
        RawBrokerObservation("unrelated-raw", "capital-primary", 9, 9, SNAPSHOT_AT, b"raw")
    )
    stale_state = _state(path)
    assert (
        commit_authorized(journal, stale).disposition
        is ReconciliationCommitDisposition.STALE_SNAPSHOT
    )
    assert _state(path) == stale_state


def test_reordered_plan_and_mixed_multi_order_bad_target_roll_back_everything(
    tmp_path: Path,
) -> None:
    path = tmp_path / "multi.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    first = _register_unknown(journal, "order-1", sequence=1)
    second = _register_unknown(journal, "order-2", sequence=2)
    request = _request(journal, (first, second))
    before = _state(path)

    reordered = replace(
        request,
        commit_id="reordered",
        expected_order_versions=tuple(reversed(request.expected_order_versions)),
        order_projections=tuple(reversed(request.order_projections)),
    )
    assert (
        commit_authorized(journal, reordered).disposition
        is ReconciliationCommitDisposition.NOT_AUTHORITATIVE
    )
    assert _state(path) == before

    bad_second = replace(
        request,
        commit_id="bad-second",
        claim_resolutions=(
            request.claim_resolutions[0],
            replace(request.claim_resolutions[1], claim_token="wrong-second-token"),
        ),
    )
    assert (
        commit_authorized(journal, bad_second).disposition
        is ReconciliationCommitDisposition.VERSION_MISMATCH
    )
    assert _state(path) == before
    assert all(
        journal.get_order(item[1].intent.client_order_id) == item[1] for item in (first, second)
    )


@pytest.mark.parametrize("tamper", ("missing", "changed", "extra", "request"))
def test_projection_history_tamper_fails_closed_on_resume(tmp_path: Path, tamper: str) -> None:
    path = tmp_path / f"tamper-{tamper}.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    fixture = _register_unknown(journal, "order-1", sequence=1)
    request = _request(journal, (fixture,))
    assert (
        commit_authorized(journal, request).disposition is ReconciliationCommitDisposition.COMMITTED
    )
    projected = request.order_projections[0]
    journal.close()

    connection = sqlite3.connect(path)
    try:
        if tamper == "missing":
            connection.execute(
                "DELETE FROM live_order_history WHERE client_order_id = ? AND order_version = ?",
                (projected.intent.client_order_id, projected.version),
            )
        elif tamper == "changed":
            prior = connection.execute(
                "SELECT payload, payload_digest FROM live_order_history WHERE client_order_id = ? AND order_version = ?",
                (projected.intent.client_order_id, projected.version - 1),
            ).fetchone()
            assert prior is not None
            connection.execute(
                "UPDATE live_order_history SET payload = ?, payload_digest = ? WHERE client_order_id = ? AND order_version = ?",
                (*prior, projected.intent.client_order_id, projected.version),
            )
        elif tamper == "extra":
            row = connection.execute(
                "SELECT payload, payload_digest, recorded_at FROM live_order_history WHERE client_order_id = ? AND order_version = ?",
                (projected.intent.client_order_id, projected.version),
            ).fetchone()
            assert row is not None
            connection.execute(
                """INSERT INTO live_order_history(
                       client_order_id, order_version, payload, payload_digest, recorded_at
                   ) VALUES (?, ?, ?, ?, ?)""",
                (projected.intent.client_order_id, projected.version + 1, *row),
            )
        else:
            connection.execute(
                "UPDATE live_reconciliation_commits SET request_payload = ? WHERE commit_id = ?",
                (b"tampered-request", request.commit_id),
            )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(LiveJournalIntegrityError):
        _journal(path, JournalOpenMode.RESUME)


def test_repaired_projection_only_commit_mapping_fails_closed_on_resume(
    tmp_path: Path,
) -> None:
    path = tmp_path / "repaired-projection-only.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    fixture = _register_unknown(journal, "order-1", sequence=1)
    command, _, _ = fixture
    request = _request(journal, (fixture,))
    committed = commit_authorized(journal, request)
    assert committed.disposition is ReconciliationCommitDisposition.COMMITTED
    journal.close()

    repaired_request = replace(request, claim_resolutions=())
    repaired_payload, repaired_request_digest = journal_module._encode(
        repaired_request,
        journal_module._COMMIT_REQUEST_DOMAIN,
    )
    connection = sqlite3.connect(path)
    try:
        row = connection.execute(
            """SELECT account_id, base_journal_sequence,
                      resulting_journal_sequence, committed_at
               FROM live_reconciliation_commits WHERE commit_id = ?""",
            (request.commit_id,),
        ).fetchone()
        assert row is not None
        repaired_sequence = int(row[2]) - 1
        repaired_commit_digest = journal_module._scalar_digest(
            journal_module._COMMIT_FACT_DOMAIN,
            {
                "commit_id": request.commit_id,
                "account_id": row[0],
                "request_digest": repaired_request_digest,
                "base_sequence": int(row[1]),
                "resulting_sequence": repaired_sequence,
                "committed_at": row[3],
            },
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
            """UPDATE live_reconciliation_commits
               SET request_payload = ?, request_digest = ?,
                   resulting_journal_sequence = ?
               WHERE commit_id = ?""",
            (
                repaired_payload,
                repaired_request_digest,
                repaired_sequence,
                request.commit_id,
            ),
        )
        connection.execute(
            """UPDATE live_journal_records
               SET journal_sequence = ?, payload_digest = ?
               WHERE record_kind = 'reconciliation-commit' AND record_id = ?""",
            (repaired_sequence, repaired_commit_digest, request.commit_id),
        )
        connection.execute(
            "UPDATE sqlite_sequence SET seq = ? WHERE name = 'live_journal_records'",
            (repaired_sequence,),
        )
        _repair_authorization_binding(
            connection,
            commit_id=request.commit_id,
            request_digest=repaired_request_digest,
            resulting_sequence=repaired_sequence,
        )
        connection.commit()

        assert connection.execute(
            "SELECT count(*) FROM live_dispatch_claim_resolutions"
        ).fetchone() == (0,)
        assert connection.execute(
            """SELECT count(*), min(journal_sequence), max(journal_sequence)
               FROM live_journal_records"""
        ).fetchone() == (repaired_sequence, 1, repaired_sequence)
    finally:
        connection.close()

    with pytest.raises(
        LiveJournalIntegrityError,
        match="live journal reconciliation projection history is invalid",
    ):
        _journal(path, JournalOpenMode.RESUME)


def test_repaired_no_projection_non_authoritative_assessment_fails_closed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "repaired-nonauthoritative.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    command, unknown_order, token = _register_unknown(journal, "order-1", sequence=1)
    accepted_order = _accept_unknown_order(journal, unknown_order, sequence=2)
    request = _no_projection_request(journal, command, accepted_order, token)
    committed = commit_authorized(journal, request)
    assert committed.disposition is ReconciliationCommitDisposition.COMMITTED
    journal.close()

    original_open = request.assessment.broker_snapshot.open_orders
    incomplete_evidence = replace(
        original_open.evidence,
        status=EvidenceCompleteness.INCOMPLETE,
        reason="tampered incomplete evidence",
    )
    tampered_broker = replace(
        request.assessment.broker_snapshot,
        open_orders=OpenOrdersSnapshot(original_open.orders, incomplete_evidence),
    )
    tampered_assessment = assess_reconciliation(
        request.assessment.local_snapshot,
        tampered_broker,
        request.assessment.result.reconciled_at,
    )
    assert not tampered_assessment.result.is_authoritative
    repaired_request = replace(request, assessment=tampered_assessment)
    repaired_payload, repaired_request_digest = journal_module._encode(
        repaired_request,
        journal_module._COMMIT_REQUEST_DOMAIN,
    )

    connection = sqlite3.connect(path)
    try:
        row = connection.execute(
            """SELECT account_id, base_journal_sequence,
                      resulting_journal_sequence, committed_at
               FROM live_reconciliation_commits WHERE commit_id = ?""",
            (request.commit_id,),
        ).fetchone()
        assert row is not None
        repaired_commit_digest = journal_module._scalar_digest(
            journal_module._COMMIT_FACT_DOMAIN,
            {
                "commit_id": request.commit_id,
                "account_id": row[0],
                "request_digest": repaired_request_digest,
                "base_sequence": int(row[1]),
                "resulting_sequence": int(row[2]),
                "committed_at": row[3],
            },
        )
        connection.execute(
            """UPDATE live_reconciliation_commits
               SET request_payload = ?, request_digest = ? WHERE commit_id = ?""",
            (repaired_payload, repaired_request_digest, request.commit_id),
        )
        connection.execute(
            """UPDATE live_journal_records SET payload_digest = ?
               WHERE record_kind = 'reconciliation-commit' AND record_id = ?""",
            (repaired_commit_digest, request.commit_id),
        )
        _repair_authorization_binding(
            connection,
            commit_id=request.commit_id,
            request_digest=repaired_request_digest,
            resulting_sequence=int(row[2]),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(
        LiveJournalIntegrityError,
        match="live journal reconciliation assessment is invalid",
    ):
        _journal(path, JournalOpenMode.RESUME)


def test_repaired_projection_fill_claim_resolution_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "repaired-fill-resolution.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    fixture = _register_unknown(journal, "order-1", sequence=1)
    command, _, _ = fixture
    request = _request(journal, (fixture,))
    assert (
        commit_authorized(journal, request).disposition is ReconciliationCommitDisposition.COMMITTED
    )
    journal.close()

    repaired_directive = replace(
        request.claim_resolutions[0],
        resolution=ClaimResolution.BROKER_FILL_CONFIRMED,
    )
    repaired_request = replace(request, claim_resolutions=(repaired_directive,))
    repaired_payload, repaired_request_digest = journal_module._encode(
        repaired_request,
        journal_module._COMMIT_REQUEST_DOMAIN,
    )
    connection = sqlite3.connect(path)
    try:
        commit_row = connection.execute(
            """SELECT account_id, base_journal_sequence,
                      resulting_journal_sequence, committed_at
               FROM live_reconciliation_commits WHERE commit_id = ?""",
            (request.commit_id,),
        ).fetchone()
        resolution_row = connection.execute(
            """SELECT expected_precondition_digest, resolved_at
               FROM live_dispatch_claim_resolutions WHERE client_command_id = ?""",
            (command.client_command_id,),
        ).fetchone()
        assert commit_row is not None and resolution_row is not None
        repaired_resolution_digest = journal_module._scalar_digest(
            journal_module._CLAIM_RESOLUTION_DOMAIN,
            {
                "commit_id": request.commit_id,
                "client_command_id": command.client_command_id,
                "precondition": resolution_row[0],
                "resolution": ClaimResolution.BROKER_FILL_CONFIRMED.value,
                "resolved_at": resolution_row[1],
            },
        )
        repaired_commit_digest = journal_module._scalar_digest(
            journal_module._COMMIT_FACT_DOMAIN,
            {
                "commit_id": request.commit_id,
                "account_id": commit_row[0],
                "request_digest": repaired_request_digest,
                "base_sequence": int(commit_row[1]),
                "resulting_sequence": int(commit_row[2]),
                "committed_at": commit_row[3],
            },
        )
        connection.execute(
            """UPDATE live_reconciliation_commits
               SET request_payload = ?, request_digest = ? WHERE commit_id = ?""",
            (repaired_payload, repaired_request_digest, request.commit_id),
        )
        connection.execute(
            """UPDATE live_dispatch_claim_resolutions
               SET resolution_kind = ?, resolution_digest = ?
               WHERE client_command_id = ?""",
            (
                ClaimResolution.BROKER_FILL_CONFIRMED.value,
                repaired_resolution_digest,
                command.client_command_id,
            ),
        )
        connection.execute(
            """UPDATE live_journal_records SET payload_digest = ?
               WHERE record_kind = 'dispatch-claim-resolution' AND record_id = ?""",
            (repaired_resolution_digest, command.client_command_id),
        )
        connection.execute(
            """UPDATE live_journal_records SET payload_digest = ?
               WHERE record_kind = 'reconciliation-commit' AND record_id = ?""",
            (repaired_commit_digest, request.commit_id),
        )
        _repair_authorization_binding(
            connection,
            commit_id=request.commit_id,
            request_digest=repaired_request_digest,
            resulting_sequence=int(commit_row[2]),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(
        LiveJournalIntegrityError,
        match="live journal reconciliation projection history is invalid",
    ):
        _journal(path, JournalOpenMode.RESUME)


def test_repaired_projection_version_audit_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "repaired-version-audit.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    fixture = _register_unknown(journal, "order-1", sequence=1)
    command, prior_order, token = fixture
    request = _request(journal, (fixture,))
    assert (
        commit_authorized(journal, request).disposition is ReconciliationCommitDisposition.COMMITTED
    )
    journal.close()

    repaired_version = prior_order.version + 1
    connection = sqlite3.connect(path)
    try:
        claim_row = connection.execute(
            """SELECT claimant_id, claimed_at FROM live_dispatch_claims
               WHERE client_command_id = ?""",
            (command.client_command_id,),
        ).fetchone()
        resolution_row = connection.execute(
            """SELECT resolved_at, resolution_kind FROM live_dispatch_claim_resolutions
               WHERE client_command_id = ?""",
            (command.client_command_id,),
        ).fetchone()
        assert claim_row is not None and resolution_row is not None
        repaired_claim = OutstandingDispatchClaim(
            command,
            token,
            claim_row[0],
            repaired_version,
            datetime.fromisoformat(claim_row[1]),
        )
        _, repaired_claim_digest = journal_module._encode(
            repaired_claim,
            journal_module._CLAIM_DOMAIN,
        )
        repaired_precondition = journal_module._scalar_digest(
            journal_module._CLAIM_RESOLUTION_DOMAIN,
            {
                "client_command_id": command.client_command_id,
                "claim_token": token,
                "claim_version": repaired_version,
                "order_version": repaired_version,
            },
        )
        repaired_resolution_digest = journal_module._scalar_digest(
            journal_module._CLAIM_RESOLUTION_DOMAIN,
            {
                "commit_id": request.commit_id,
                "client_command_id": command.client_command_id,
                "precondition": repaired_precondition,
                "resolution": resolution_row[1],
                "resolved_at": resolution_row[0],
            },
        )
        connection.execute(
            """UPDATE live_dispatch_claims
               SET expected_order_version = ?, claim_version = ?
               WHERE client_command_id = ?""",
            (repaired_version, repaired_version, command.client_command_id),
        )
        connection.execute(
            """UPDATE live_dispatch_claim_resolutions
               SET expected_claim_version = ?, expected_order_version = ?,
                   expected_precondition_digest = ?, resolution_digest = ?
               WHERE client_command_id = ?""",
            (
                repaired_version,
                repaired_version,
                repaired_precondition,
                repaired_resolution_digest,
                command.client_command_id,
            ),
        )
        connection.execute(
            """UPDATE live_journal_records SET payload_digest = ?
               WHERE record_kind = 'dispatch-claim' AND record_id = ?""",
            (repaired_claim_digest, command.client_command_id),
        )
        connection.execute(
            """UPDATE live_journal_records SET payload_digest = ?
               WHERE record_kind = 'dispatch-claim-resolution' AND record_id = ?""",
            (repaired_resolution_digest, command.client_command_id),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(LiveJournalIntegrityError):
        _journal(path, JournalOpenMode.RESUME)


def test_order_history_recorded_at_tamper_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "tampered-history-time.sqlite3"
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    fixture = _register_unknown(journal, "order-1", sequence=1)
    request = _request(journal, (fixture,))
    assert (
        commit_authorized(journal, request).disposition is ReconciliationCommitDisposition.COMMITTED
    )
    projected = request.order_projections[0]
    journal.close()

    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """UPDATE live_order_history SET recorded_at = ?
               WHERE client_order_id = ? AND order_version = ?""",
            (
                journal_module._timestamp(projected.updated_at + timedelta(microseconds=1)),
                projected.intent.client_order_id,
                projected.version,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(LiveJournalIntegrityError):
        _journal(path, JournalOpenMode.RESUME)
