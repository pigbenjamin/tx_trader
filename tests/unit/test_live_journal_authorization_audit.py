from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
import sqlite3

import pytest

from tests.support.live_authorization_audit_scenarios import authorize
from tests.unit.test_live_journal_reconciliation_commit import (
    _accept_claim,
    _journal,
    _register_claim,
    _request,
)
from tx_trade.orders.live_journal_contracts import (
    JournalOpenMode,
    LiveJournalIntegrityError,
)
from tx_trade.orders.live_journal_codec import encode_journal_value
from tx_trade.orders.live_reconciliation_authorization import (
    reconciliation_authorization_digest,
)
from tx_trade.orders.live_reconciliation_authorization_contracts import (
    AuthorizedReconciliationCommitRequest,
    ReconciliationAuthorizationError,
    ReconciliationAuthorizationFailureCode,
)
from tx_trade.orders.live_reconciliation_commit_contracts import (
    ReconciliationCommitDisposition,
)


def _authorized_fixture(path: Path):
    journal = _journal(path, JournalOpenMode.CREATE_NEW)
    _, order, token = _register_claim(journal)
    order = _accept_claim(journal, order)
    request = _request(journal, order, token)
    return journal, request, authorize(journal, request)


def test_authorization_audit_is_atomic_exact_and_verified_on_reopen(
    tmp_path: Path,
) -> None:
    path = tmp_path / "authorization.sqlite3"
    journal, request, authorized = _authorized_fixture(path)

    result = journal.commit_authorized_reconciliation(authorized)

    assert result.disposition is ReconciliationCommitDisposition.COMMITTED
    connection = sqlite3.connect(path)
    try:
        audit = connection.execute(
            """SELECT authorization_id, commit_id, consumed_at, authorization_digest,
                      resulting_journal_sequence
               FROM live_reconciliation_commit_authorizations"""
        ).fetchone()
        records = connection.execute(
            """SELECT journal_sequence, record_kind, record_id
               FROM live_journal_records
               WHERE record_kind IN ('operator-authorization', 'reconciliation-commit')
               ORDER BY journal_sequence"""
        ).fetchall()
    finally:
        connection.close()
    assert audit == (
        authorized.authorization.authorization_id,
        request.commit_id,
        result.committed_at.isoformat().replace("+00:00", "Z"),
        reconciliation_authorization_digest(authorized.authorization),
        result.resulting_journal_sequence,
    )
    assert records == [
        (
            request.expected_journal_sequence + 1,
            "operator-authorization",
            authorized.authorization.authorization_id,
        ),
        (
            result.resulting_journal_sequence,
            "reconciliation-commit",
            request.commit_id,
        ),
    ]
    journal.close()
    resumed = _journal(path, JournalOpenMode.RESUME)
    retry = resumed.commit_authorized_reconciliation(authorized)
    assert retry.disposition is ReconciliationCommitDisposition.EXACT_RETRY
    resumed.close()


def test_expired_and_not_yet_valid_authorizations_write_nothing(tmp_path: Path) -> None:
    path = tmp_path / "authorization-time.sqlite3"
    journal, _, authorized = _authorized_fixture(path)
    now = journal._now()
    before = journal.load_recovery_snapshot().journal_sequence

    expired = replace(
        authorized.authorization,
        authorized_at=now - timedelta(minutes=2),
        expires_at=now,
    )
    with pytest.raises(ReconciliationAuthorizationError) as expired_error:
        journal.commit_authorized_reconciliation(
            AuthorizedReconciliationCommitRequest(expired, authorized.request)
        )
    assert expired_error.value.code is ReconciliationAuthorizationFailureCode.AUTHORIZATION_EXPIRED

    future = replace(
        authorized.authorization,
        authorized_at=now + timedelta(seconds=1),
        expires_at=now + timedelta(minutes=1),
    )
    with pytest.raises(ReconciliationAuthorizationError) as future_error:
        journal.commit_authorized_reconciliation(
            AuthorizedReconciliationCommitRequest(future, authorized.request)
        )
    assert future_error.value.code is ReconciliationAuthorizationFailureCode.INVALID_AUTHORIZATION
    assert journal.load_recovery_snapshot().journal_sequence == before
    journal.close()


def test_clock_callback_cannot_switch_request_after_canonical_validation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "authorization-request-snapshot.sqlite3"
    journal, request, authorized = _authorized_fixture(path)
    canonical_payload = encode_journal_value(request)
    canonical_authorization_digest = reconciliation_authorization_digest(authorized.authorization)
    expected_command_id = request.claim_resolutions[0].client_command_id
    expected_sequence = request.expected_journal_sequence
    now = journal._now()
    callback_count = 0

    def mutate_caller_request():
        nonlocal callback_count
        callback_count += 1
        if callback_count == 1:
            object.__setattr__(request, "commit_id", "commit-mutated")
            object.__setattr__(request, "account_id", "account-mutated")
            object.__setattr__(request, "expected_journal_sequence", 999)
            object.__setattr__(request, "claim_resolutions", ())
            object.__setattr__(
                request,
                "order_projections",
                (request.assessment.local_snapshot.orders[0],),
            )
        return now

    journal._clock = mutate_caller_request
    result = journal.commit_authorized_reconciliation(authorized)

    assert callback_count == 1
    assert result.disposition is ReconciliationCommitDisposition.COMMITTED
    assert result.commit_id == "commit-1"
    assert result.account_id == "account-1"
    assert result.resolved_claim_ids == (expected_command_id,)
    assert result.order_projections == ()
    connection = sqlite3.connect(path)
    try:
        commit = connection.execute(
            """SELECT commit_id, account_id, expected_journal_sequence, request_payload
               FROM live_reconciliation_commits"""
        ).fetchone()
        audit = connection.execute(
            """SELECT authorization_id, commit_id, account_id, expected_journal_sequence,
                      authorization_digest
               FROM live_reconciliation_commit_authorizations"""
        ).fetchone()
        resolution = connection.execute(
            """SELECT client_command_id, commit_id
               FROM live_dispatch_claim_resolutions"""
        ).fetchone()
    finally:
        connection.close()
    assert commit == ("commit-1", "account-1", expected_sequence, canonical_payload)
    assert audit == (
        "auth-commit-1",
        "commit-1",
        "account-1",
        expected_sequence,
        canonical_authorization_digest,
    )
    assert resolution == (expected_command_id, "commit-1")
    journal.close()
    resumed = _journal(path, JournalOpenMode.RESUME)
    resumed.close()


def test_clock_callback_cannot_switch_authorization_after_canonical_validation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "authorization-attestation-snapshot.sqlite3"
    journal, request, authorized = _authorized_fixture(path)
    authorization = authorized.authorization
    expected_digest = reconciliation_authorization_digest(authorization)
    expected_fields = (
        authorization.authorization_id,
        authorization.principal_id,
        authorization.account_id,
        authorization.commit_id,
        authorization.request_digest,
        authorization.expected_journal_sequence,
        expected_digest,
    )
    now = journal._now()

    def mutate_caller_authorization():
        object.__setattr__(authorization, "authorization_id", "auth-mutated")
        object.__setattr__(authorization, "principal_id", "principal-mutated")
        object.__setattr__(authorization, "account_id", "account-mutated")
        object.__setattr__(authorization, "commit_id", "commit-mutated")
        object.__setattr__(authorization, "request_digest", f"sha256:{'f' * 64}")
        object.__setattr__(authorization, "expected_journal_sequence", 999)
        return now

    journal._clock = mutate_caller_authorization
    result = journal.commit_authorized_reconciliation(authorized)

    assert result.disposition is ReconciliationCommitDisposition.COMMITTED
    connection = sqlite3.connect(path)
    try:
        audit = connection.execute(
            """SELECT authorization_id, principal_id, account_id, commit_id,
                      request_digest, expected_journal_sequence, authorization_digest
               FROM live_reconciliation_commit_authorizations"""
        ).fetchone()
        commit = connection.execute(
            "SELECT commit_id, account_id FROM live_reconciliation_commits"
        ).fetchone()
    finally:
        connection.close()
    assert audit == expected_fields
    assert commit == (request.commit_id, request.account_id)
    journal.close()
    resumed = _journal(path, JournalOpenMode.RESUME)
    resumed.close()


@pytest.mark.parametrize(
    ("column", "forged"),
    [
        ("authorization_id", "authorization-forged"),
        ("commit_id", "commit-forged"),
        ("journal_id", "journal-forged"),
        ("account_id", "account-forged"),
        ("action_kind", "generic"),
        ("principal_id", "principal-forged"),
        ("authority_context_digest", f"sha256:{'b' * 64}"),
        ("source_inspection_digest", f"sha256:{'b' * 64}"),
        ("operator_plan_digest", f"sha256:{'b' * 64}"),
        ("request_digest", f"sha256:{'b' * 64}"),
        ("broker_snapshot_id", "snapshot-forged"),
        ("expected_journal_sequence", 999),
        ("authorized_at", "2026-08-13T01:59:59Z"),
        ("expires_at", "2026-08-13T02:00:01Z"),
        ("consumed_at", "2026-08-13T02:00:01Z"),
        ("reason_code", "reason-forged"),
        ("authorization_digest", f"sha256:{'b' * 64}"),
        ("resulting_journal_sequence", 999),
    ],
)
def test_public_bare_commit_is_absent_and_every_tampered_audit_column_fails_closed(
    tmp_path: Path, column: str, forged: str | int
) -> None:
    path = tmp_path / f"authorization-tamper-{column}.sqlite3"
    journal, _, authorized = _authorized_fixture(path)
    journal.commit_authorized_reconciliation(authorized)
    assert not hasattr(journal, "commit_reconciliation")
    journal.close()

    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute("DROP TRIGGER live_reconciliation_commit_authorizations_no_update")
        connection.execute(
            f"UPDATE live_reconciliation_commit_authorizations SET {column} = ?",
            (forged,),
        )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(LiveJournalIntegrityError):
        _journal(path, JournalOpenMode.RESUME)
