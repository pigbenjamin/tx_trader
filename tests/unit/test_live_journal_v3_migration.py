from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import sqlite3

from tests.support.live_authorization_audit_scenarios import (
    create_sealed_authorization_flow,
    database_state,
    prepare_authorized_flow,
)

from tx_trade.orders.live_journal_contracts import JournalOpenMode, LiveJournalIdentity
from tx_trade.orders.sqlite_live_order_journal import (
    SqliteLiveOrderJournal,
    _IDENTITY_DOMAIN,
    _COMMIT_FACT_DOMAIN,
    _COMMIT_REQUEST_DOMAIN,
    _SCHEMA_MIGRATION_FACT_DOMAIN,
    _decode,
    _encode,
    _scalar_digest,
    _timestamp,
    _v1_schema_material,
    _v2_schema_material,
)
from tx_trade.orders.live_reconciliation_commit_contracts import (
    DurableReconciliationCommitRequest,
)


NOW = datetime(2026, 8, 13, 2, 0, tzinfo=timezone.utc)


def _create_v2(path: Path) -> None:
    schema, v2_fingerprint = _v2_schema_material()
    _, v1_fingerprint = _v1_schema_material()
    identity = LiveJournalIdentity("journal-v2", 2, v2_fingerprint, NOW)
    _, identity_digest = _encode(identity, _IDENTITY_DOMAIN)
    migration_digest = _scalar_digest(
        _SCHEMA_MIGRATION_FACT_DOMAIN,
        {
            "version": 2,
            "from_fingerprint": v1_fingerprint,
            "to_fingerprint": v2_fingerprint,
            "recorded_at": _timestamp(NOW),
        },
    )
    connection = sqlite3.connect(path)
    try:
        connection.executescript(schema)
        connection.execute("INSERT INTO live_journal_migrations VALUES (1, ?)", (v1_fingerprint,))
        connection.execute("INSERT INTO live_journal_migrations VALUES (2, ?)", (v2_fingerprint,))
        connection.execute(
            "INSERT INTO live_journal_identity VALUES (1, ?, 2, ?, ?)",
            (identity.journal_id, v2_fingerprint, _timestamp(NOW)),
        )
        connection.execute(
            """INSERT INTO live_journal_records(
                   record_kind, record_id, payload_digest, recorded_at
               ) VALUES ('identity', ?, ?, ?)""",
            (identity.journal_id, identity_digest, _timestamp(NOW)),
        )
        connection.execute(
            """INSERT INTO live_journal_records(
                   record_kind, record_id, payload_digest, recorded_at
               ) VALUES ('schema-migration', '2', ?, ?)""",
            (migration_digest, _timestamp(NOW)),
        )
        connection.commit()
    finally:
        connection.close()


def _create_v2_with_real_reconciliation_commit(path: Path) -> None:
    create_sealed_authorization_flow(path)
    flow = prepare_authorized_flow(path)
    result = flow.journal.commit_authorized_reconciliation(flow.authorized)
    flow.journal.close()

    connection = sqlite3.connect(path)
    try:
        migration_sequence = int(
            connection.execute(
                """SELECT journal_sequence FROM live_journal_records
                   WHERE record_kind = 'schema-migration' AND record_id = '3'"""
            ).fetchone()[0]
        )
        authorization_sequence = int(
            connection.execute(
                """SELECT journal_sequence FROM live_journal_records
                   WHERE record_kind = 'operator-authorization'"""
            ).fetchone()[0]
        )
        commit_row = connection.execute(
            """SELECT request_payload, request_digest, base_journal_sequence,
                      resulting_journal_sequence, committed_at
               FROM live_reconciliation_commits WHERE commit_id = ?""",
            (flow.authorized.request.commit_id,),
        ).fetchone()
        request = _decode(
            commit_row[0],
            commit_row[1],
            DurableReconciliationCommitRequest,
            _COMMIT_REQUEST_DOMAIN,
        )
        assert isinstance(request, DurableReconciliationCommitRequest)
        base_sequence = request.expected_journal_sequence - 1
        legacy_request = replace(
            request,
            assessment=replace(
                request.assessment,
                local_snapshot=replace(
                    request.assessment.local_snapshot,
                    journal_sequence=base_sequence,
                ),
            ),
            expected_journal_sequence=base_sequence,
        )
        request_payload, request_digest = _encode(legacy_request, _COMMIT_REQUEST_DOMAIN)
        assert base_sequence == int(commit_row[2]) - 1
        resulting_sequence = int(commit_row[3]) - 2
        commit_digest = _scalar_digest(
            _COMMIT_FACT_DOMAIN,
            {
                "commit_id": legacy_request.commit_id,
                "account_id": legacy_request.account_id,
                "request_digest": request_digest,
                "base_sequence": base_sequence,
                "resulting_sequence": resulting_sequence,
                "committed_at": commit_row[4],
            },
        )
        records = connection.execute(
            """SELECT journal_sequence, record_kind, record_id, payload_digest, recorded_at
               FROM live_journal_records
               WHERE journal_sequence NOT IN (?, ?)
               ORDER BY journal_sequence""",
            (migration_sequence, authorization_sequence),
        ).fetchall()
        connection.execute("DELETE FROM live_journal_records")
        connection.executemany(
            """INSERT INTO live_journal_records VALUES (?, ?, ?, ?, ?)""",
            [
                (
                    index,
                    row[1],
                    row[2],
                    commit_digest if row[1] == "reconciliation-commit" else row[3],
                    row[4],
                )
                for index, row in enumerate(records, 1)
            ],
        )
        connection.execute(
            """UPDATE live_reconciliation_commits
               SET expected_journal_sequence = ?, base_journal_sequence = ?,
                   request_payload = ?, request_digest = ?, resulting_journal_sequence = ?
               WHERE commit_id = ?""",
            (
                base_sequence,
                base_sequence,
                request_payload,
                request_digest,
                resulting_sequence,
                legacy_request.commit_id,
            ),
        )
        connection.execute("DROP TRIGGER live_reconciliation_commit_authorizations_no_delete")
        connection.execute("DROP TRIGGER live_reconciliation_commit_authorizations_no_update")
        connection.execute("DROP TABLE live_reconciliation_commit_authorizations")
        identity_row = connection.execute("SELECT * FROM live_journal_identity").fetchone()
        connection.execute("DROP TABLE live_journal_identity")
        v2_schema, v2_fingerprint = _v2_schema_material()
        identity_start = v2_schema.index('CREATE TABLE "live_journal_identity" (')
        identity_end = v2_schema.index(";", identity_start) + 1
        connection.execute(v2_schema[identity_start:identity_end])
        legacy_identity = LiveJournalIdentity(
            identity_row[1], 2, v2_fingerprint, datetime.fromisoformat(identity_row[4])
        )
        _, identity_digest = _encode(legacy_identity, _IDENTITY_DOMAIN)
        connection.execute(
            "INSERT INTO live_journal_identity VALUES (?, ?, ?, ?, ?)",
            (identity_row[0], identity_row[1], 2, v2_fingerprint, identity_row[4]),
        )
        connection.execute(
            """UPDATE live_journal_records SET payload_digest = ?
               WHERE record_kind = 'identity'""",
            (identity_digest,),
        )
        connection.execute("DELETE FROM live_journal_migrations WHERE version = 3")
        connection.execute(
            "UPDATE sqlite_sequence SET seq = ? WHERE name = 'live_journal_records'",
            (resulting_sequence,),
        )
        connection.execute("PRAGMA user_version = 2")
        connection.commit()
        assert result.resulting_journal_sequence == resulting_sequence + 2
    finally:
        connection.close()


def test_v2_resumes_through_v3_and_preserves_creation_identity(tmp_path: Path) -> None:
    path = tmp_path / "v2.sqlite3"
    _create_v2(path)

    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: NOW,
        claim_token_factory=lambda: "unused",
    )

    assert journal.identity.schema_version == 2
    assert journal.load_recovery_snapshot().journal_sequence == 3
    journal.close()
    connection = sqlite3.connect(path)
    try:
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)
        assert connection.execute(
            "SELECT version FROM live_journal_migrations ORDER BY version"
        ).fetchall() == [(1,), (2,), (3,)]
        assert connection.execute(
            """SELECT record_id FROM live_journal_records
               WHERE record_kind = 'schema-migration'
               ORDER BY journal_sequence"""
        ).fetchall() == [("2",), ("3",)]
    finally:
        connection.close()


def test_v2_real_reconciliation_commit_migrates_and_reopens(tmp_path: Path) -> None:
    path = tmp_path / "v2-with-commit.sqlite3"
    _create_v2_with_real_reconciliation_commit(path)

    before = dict(database_state(path))
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: NOW,
        claim_token_factory=lambda: "unused",
    )
    journal.close()
    reopened = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: NOW,
        claim_token_factory=lambda: "unused",
    )
    try:
        assert reopened.load_recovery_snapshot().journal_sequence == len(
            dict(database_state(path))["live_journal_records"]
        )
    finally:
        reopened.close()

    after = dict(database_state(path))
    assert after["live_reconciliation_commits"] == before["live_reconciliation_commits"]
    assert after["live_dispatch_claim_resolutions"] == before["live_dispatch_claim_resolutions"]
    assert len(after["live_reconciliation_commit_authorizations"]) == 0
