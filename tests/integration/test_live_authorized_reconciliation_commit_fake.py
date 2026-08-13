from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
import sqlite3

import pytest

from tests.support.live_authorization_audit_scenarios import (
    ACCOUNT_ID,
    database_state,
    create_sealed_authorization_flow,
    prepare_authorized_flow,
)
from tests.support.trusted_assessment_source_scenarios import directory_snapshot
from tx_trade.orders.live_journal_contracts import JournalOpenMode
from tx_trade.orders.live_journal_recovery import verify_recovery_snapshot
from tx_trade.orders.live_reconciliation_authorization_contracts import (
    AuthorizedReconciliationCommitRequest,
    ReconciliationAuthorizationError,
    ReconciliationAuthorizationFailureCode,
)
from tx_trade.orders.live_reconciliation_commit_contracts import (
    ReconciliationCommitDisposition,
)
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal


def _audit_rows(path: Path) -> tuple[tuple[object, ...], tuple[tuple[object, ...], ...]]:
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    try:
        authorization = connection.execute(
            """SELECT authorization_id, commit_id, request_digest,
                      resulting_journal_sequence
               FROM live_reconciliation_commit_authorizations"""
        ).fetchone()
        facts = tuple(
            connection.execute(
                """SELECT journal_sequence, record_kind, record_id
                   FROM live_journal_records
                   WHERE record_kind IN (
                       'operator-authorization', 'dispatch-claim-resolution',
                       'reconciliation-commit'
                   ) ORDER BY journal_sequence"""
            )
        )
        assert authorization is not None
        return tuple(authorization), facts
    finally:
        connection.close()


def test_full_trusted_authorized_flow_is_durable_offline_and_exact(tmp_path: Path) -> None:
    path = tmp_path / "authorized-full-flow.sqlite3"
    create_sealed_authorization_flow(path)
    flow = prepare_authorized_flow(path)
    request = flow.authorized.request
    before_sequence = request.expected_journal_sequence

    assert flow.journal.identity.schema_version == 3
    assert flow.broker_calls == (ACCOUNT_ID,)
    assert request.assessment.result.is_authoritative
    assert not request.assessment.may_dispatch
    assert not flow.authorized.may_dispatch
    assert not hasattr(flow.journal, "dispatch")
    assert not hasattr(flow.journal, "query_reconciliation_snapshot")

    committed = flow.journal.commit_authorized_reconciliation(flow.authorized)
    assert committed.disposition is ReconciliationCommitDisposition.COMMITTED
    assert committed.resulting_journal_sequence == before_sequence + 3
    assert committed.resolved_claim_ids == (request.claim_resolutions[0].client_command_id,)
    assert committed.order_projections == request.order_projections
    after = flow.journal.load_recovery_snapshot()
    assert after.journal_sequence == committed.resulting_journal_sequence
    assert after.outstanding_claims == ()
    assert not verify_recovery_snapshot(after).may_dispatch
    flow.journal.close()

    audit, facts = _audit_rows(path)
    assert audit == (
        flow.authorized.authorization.authorization_id,
        request.commit_id,
        flow.authorized.authorization.request_digest,
        committed.resulting_journal_sequence,
    )
    assert facts == (
        (before_sequence + 1, "operator-authorization", "auth-authorization-flow"),
        (
            before_sequence + 2,
            "dispatch-claim-resolution",
            request.claim_resolutions[0].client_command_id,
        ),
        (before_sequence + 3, "reconciliation-commit", request.commit_id),
    )

    reopened = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: flow.authorized.authorization.expires_at + timedelta(days=1),
        claim_token_factory=lambda: "must-not-be-used",
    )
    before_retry = database_state(path)
    retry = reopened.commit_authorized_reconciliation(flow.authorized)
    assert retry == replace(committed, disposition=ReconciliationCommitDisposition.EXACT_RETRY)
    assert reopened.load_recovery_snapshot() == after
    reopened.close()
    assert database_state(path) == before_retry


@pytest.mark.parametrize("variant", ("expired", "before", "journal", "digest", "account"))
def test_invalid_authorization_variants_are_exact_zero_write(tmp_path: Path, variant: str) -> None:
    path = tmp_path / f"invalid-{variant}.sqlite3"
    create_sealed_authorization_flow(path)
    flow = prepare_authorized_flow(path)
    authorization = flow.authorized.authorization
    if variant == "expired":
        authorization = replace(
            authorization,
            authorized_at=authorization.authorized_at - timedelta(minutes=4),
            expires_at=authorization.authorized_at,
        )
    elif variant == "before":
        authorization = replace(
            authorization,
            authorized_at=authorization.expires_at - timedelta(seconds=1),
            expires_at=authorization.expires_at,
        )
    elif variant == "journal":
        authorization = replace(authorization, journal_id="journal-wrong")
    elif variant == "digest":
        authorization = replace(authorization, request_digest=f"sha256:{'b' * 64}")
    else:
        authorization = replace(authorization, account_id="account-wrong")

    flow.journal.close()
    before_rows = database_state(path)
    before_files = directory_snapshot(tmp_path)
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: flow.authorized.authorization.authorized_at,
        claim_token_factory=lambda: "unused-invalid-token",
    )
    with pytest.raises((ReconciliationAuthorizationError, ValueError)) as captured:
        journal.commit_authorized_reconciliation(
            AuthorizedReconciliationCommitRequest(authorization, flow.authorized.request)
        )
    if variant == "expired":
        assert captured.value.code is ReconciliationAuthorizationFailureCode.AUTHORIZATION_EXPIRED
    journal.close()
    assert database_state(path) == before_rows
    after_files = directory_snapshot(tmp_path)
    assert set(after_files) == set(before_files) == {path.name}
    assert after_files[path.name][0] == before_files[path.name][0]
