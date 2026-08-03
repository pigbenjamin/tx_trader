from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import re
import sqlite3

import pytest


ORDERS_DIR = Path(__file__).parents[2] / "tx_trade" / "orders"
CURRENT_SCHEMA = ORDERS_DIR / "live_journal_schema.sql"
V1_SCHEMA = ORDERS_DIR / "live_journal_schema_v1.sql"
V1_TO_V2 = ORDERS_DIR / "live_journal_migration_v1_to_v2.sql"
V1_SHA256 = "2e2a378b3babf61c7458f7354e875a733eba803ffc9d7bf460a9644db5c724c1"


def _connect() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _signature(connection: sqlite3.Connection) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        connection.execute(
            """SELECT type, name, sql FROM sqlite_master
               WHERE name NOT LIKE 'sqlite_%'
                 AND type IN ('table', 'index', 'view', 'trigger')
               ORDER BY type, name"""
        )
    )


def _fresh_v2() -> sqlite3.Connection:
    connection = _connect()
    connection.executescript(CURRENT_SCHEMA.read_text(encoding="utf-8"))
    return connection


def _migrated_v1() -> sqlite3.Connection:
    connection = _connect()
    connection.executescript(V1_SCHEMA.read_text(encoding="utf-8"))
    connection.execute(
        "INSERT INTO live_journal_migrations VALUES (1, ?)",
        (f"sha256:{V1_SHA256}",),
    )
    connection.execute(
        "INSERT INTO live_journal_identity VALUES (1, ?, 1, ?, ?)",
        ("journal-1", f"sha256:{V1_SHA256}", "2026-08-03T00:00:00.000000Z"),
    )
    connection.execute(
        "INSERT INTO live_journal_records(record_kind, record_id, payload_digest, recorded_at) "
        "VALUES ('identity', 'journal-1', 'sha256:identity', '2026-08-03T00:00:00Z')"
    )
    identity_before = connection.execute("SELECT * FROM live_journal_identity").fetchone()
    record_before = connection.execute("SELECT * FROM live_journal_records").fetchone()

    connection.executescript(V1_TO_V2.read_text(encoding="utf-8"))

    assert connection.execute("SELECT * FROM live_journal_identity").fetchone() == identity_before
    assert connection.execute("SELECT * FROM live_journal_records").fetchone() == record_before
    return connection


def _seed_resolution_targets(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO live_order_id_reservations VALUES ('order-1', 'intent-1', '2026-08-03T00:00:00Z')"
    )
    connection.execute(
        "INSERT INTO live_orders VALUES "
        "('order-1', 'account-1', 'submitting', 1, X'01', 'sha256:order', '2026-08-03T00:00:00Z')"
    )
    connection.execute(
        "INSERT INTO live_commands VALUES "
        "('command-1', 'order-1', 'new', 'fp-1', X'01', 'sha256:command', "
        "'2026-08-03T00:00:00Z')"
    )
    connection.execute(
        "INSERT INTO live_dispatch_claims VALUES "
        "('command-1', 'claim-token-1', 'worker-1', 1, 1, '2026-08-03T00:00:00Z')"
    )
    connection.execute(
        "INSERT INTO live_raw_observations VALUES "
        "('observation-1', 'broker-1', 1, 1, '2026-08-03T00:00:00Z', X'01', "
        "'sha256:observation', 'ambiguous')"
    )
    connection.execute(
        "INSERT INTO live_reconciliation_requirements"
        "(client_order_id, observation_id, reason_code, created_at) VALUES "
        "('order-1', 'observation-1', 'outcome_unknown', '2026-08-03T00:00:00Z')"
    )
    connection.execute(
        """INSERT INTO live_reconciliation_commits(
               commit_id, account_id, expected_journal_sequence, base_journal_sequence,
               snapshot_id, request_payload, request_digest, committed_at,
               resulting_journal_sequence
           ) VALUES ('commit-1', 'account-1', 0, 0, 'snapshot-1', X'01',
                     'sha256:a-request1', '2026-08-03T01:00:00Z', 1)"""
    )
    connection.execute(
        "INSERT INTO live_journal_records(record_kind, record_id, payload_digest, recorded_at) "
        "VALUES ('reconciliation_commit', 'commit-1', 'sha256:record1', '2026-08-03T01:00:00Z')"
    )


def test_v1_schema_is_byte_for_byte_frozen() -> None:
    assert sha256(V1_SCHEMA.read_bytes()).hexdigest() == V1_SHA256


def test_fresh_v2_has_expected_version_and_overlay_tables() -> None:
    connection = _fresh_v2()
    try:
        assert connection.execute("PRAGMA application_id").fetchone() == (1415074890,)
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        assert {
            "live_reconciliation_commits",
            "live_dispatch_claim_resolutions",
            "live_observation_reconciliation_resolutions",
            "live_reconciliation_requirement_resolutions",
        } <= tables
        connection.execute(
            "INSERT INTO live_journal_identity VALUES "
            "(1, 'journal-2', 2, 'sha256:v2', '2026-08-03T00:00:00Z')"
        )
    finally:
        connection.close()


def test_v1_migration_matches_fresh_v2_schema_and_preserves_creation_identity() -> None:
    fresh = _fresh_v2()
    migrated = _migrated_v1()
    try:
        assert _signature(migrated) == _signature(fresh)
        assert migrated.execute("PRAGMA user_version").fetchone() == (2,)
        assert migrated.execute(
            "SELECT version FROM live_journal_migrations ORDER BY version"
        ).fetchall() == [(1,), (2,)]
        with pytest.raises(sqlite3.IntegrityError):
            migrated.execute(
                "UPDATE live_journal_identity SET schema_version = 3 WHERE singleton = 1"
            )
    finally:
        fresh.close()
        migrated.close()


def test_migration_leaves_transaction_management_to_caller() -> None:
    for sql_path in (CURRENT_SCHEMA, V1_TO_V2):
        script = sql_path.read_text(encoding="utf-8")
        assert re.search(r"\b(?:BEGIN|COMMIT|ROLLBACK)\b", script, re.IGNORECASE) is None

    connection = _connect()
    fresh = _fresh_v2()
    try:
        connection.executescript(V1_SCHEMA.read_text(encoding="utf-8"))
        connection.execute("BEGIN IMMEDIATE")
        for statement in V1_TO_V2.read_text(encoding="utf-8").split(";"):
            if statement.strip():
                connection.execute(statement)
        assert connection.in_transaction
        assert _signature(connection) == _signature(fresh)
        connection.rollback()
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
    finally:
        fresh.close()
        connection.close()


def test_overlay_foreign_keys_checks_and_target_uniqueness() -> None:
    connection = _fresh_v2()
    try:
        _seed_resolution_targets(connection)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """INSERT INTO live_dispatch_claim_resolutions VALUES
                   ('missing-command', 'commit-1', 'token', 1, 1, 'sha256:a-precondition',
                    'broker_order_confirmed', '2026-08-03T01:00:00Z',
                    'sha256:a-resolution')"""
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """INSERT INTO live_observation_reconciliation_resolutions VALUES
                   ('observation-1', 'commit-1', 'resolved', 'sha256:a-precondition',
                    'event-1', 'broker_order_confirmed', '2026-08-03T01:00:00Z',
                    'sha256:a-resolution')"""
            )

        connection.execute(
            """INSERT INTO live_dispatch_claim_resolutions VALUES
               ('command-1', 'commit-1', 'claim-token-1', 1, 1,
                'sha256:a-claim-precondition', 'broker_order_confirmed',
                '2026-08-03T01:00:00Z', 'sha256:a-claim-resolution')"""
        )
        connection.execute(
            """INSERT INTO live_observation_reconciliation_resolutions VALUES
               ('observation-1', 'commit-1', 'ambiguous', 'sha256:a-observation-precondition',
                'event-1', 'broker_order_confirmed', '2026-08-03T01:00:00Z',
                'sha256:a-observation-resolution')"""
        )
        connection.execute(
            """INSERT INTO live_reconciliation_requirement_resolutions VALUES
               (1, 'commit-1', 'sha256:a-requirement-precondition', 'satisfied',
                '2026-08-03T01:00:00Z', 'sha256:a-requirement-resolution')"""
        )

        for table in (
            "live_dispatch_claim_resolutions",
            "live_observation_reconciliation_resolutions",
            "live_reconciliation_requirement_resolutions",
        ):
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(f"INSERT INTO {table} SELECT * FROM {table}")

        assert connection.execute(
            "SELECT claim_token, claim_version FROM live_dispatch_claims WHERE client_command_id = 'command-1'"
        ).fetchone() == ("claim-token-1", 1)
        assert connection.execute(
            "SELECT resolution_status FROM live_raw_observations WHERE observation_id = 'observation-1'"
        ).fetchone() == ("ambiguous",)
        assert connection.execute(
            "SELECT resolved_at FROM live_reconciliation_requirements WHERE requirement_id = 1"
        ).fetchone() == (None,)
    finally:
        connection.close()
