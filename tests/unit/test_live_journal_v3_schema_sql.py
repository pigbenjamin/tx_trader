from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import re
import sqlite3
from typing import Any

import pytest


ORDERS_DIR = Path(__file__).parents[2] / "tx_trade" / "orders"
CURRENT_SCHEMA = ORDERS_DIR / "live_journal_schema.sql"
V2_SCHEMA = ORDERS_DIR / "live_journal_schema_v2.sql"
V2_TO_V3 = ORDERS_DIR / "live_journal_migration_v2_to_v3.sql"
V2_SHA256 = "d9c6c23fdce811b9a85efafa8eadd6083842c0d1d9007c33943d028a6d103b3b"
V3_SHA256 = "9150866af5822cc4bfb4e889791e82bac84fac59fce321c7667897eed223b761"
DIGEST = f"sha256:{'a' * 64}"
SECOND_DIGEST = f"sha256:{'b' * 64}"
NEW_OBJECTS = {
    "live_reconciliation_commit_authorizations",
    "live_reconciliation_commit_authorizations_account_idx",
    "live_reconciliation_commit_authorizations_principal_idx",
    "live_reconciliation_commit_authorizations_no_update",
    "live_reconciliation_commit_authorizations_no_delete",
}


def _connect() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _execute_script(connection: sqlite3.Connection, script: str) -> None:
    statement = ""
    for line in script.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            if statement.strip():
                connection.execute(statement)
            statement = ""
    assert not statement.strip()


def _signature(connection: sqlite3.Connection) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        connection.execute(
            """SELECT type, name, sql FROM sqlite_master
               WHERE name NOT LIKE 'sqlite_%'
                 AND type IN ('table', 'index', 'view', 'trigger')
               ORDER BY type, name"""
        )
    )


def _fresh(path: Path) -> sqlite3.Connection:
    connection = _connect()
    connection.executescript(path.read_text(encoding="utf-8"))
    return connection


def _seed_v2(connection: sqlite3.Connection) -> None:
    connection.execute(
        "INSERT INTO live_journal_migrations VALUES (2, ?)",
        (f"sha256:{V2_SHA256}",),
    )
    connection.execute(
        "INSERT INTO live_journal_identity VALUES (1, 'journal-1', 2, ?, 'created')",
        (f"sha256:{V2_SHA256}",),
    )
    connection.execute(
        "INSERT INTO live_journal_records(record_kind, record_id, payload_digest, recorded_at) "
        "VALUES ('identity', 'journal-1', 'sha256:identity', 'created')"
    )
    connection.execute(
        "INSERT INTO live_reconciliation_commits VALUES "
        "('commit-1', 'account-1', 4, 4, 'snapshot-1', X'01', "
        "?, 'committed', 5)",
        (DIGEST,),
    )
    connection.execute(
        "INSERT INTO live_reconciliation_commits VALUES "
        "('commit-2', 'account-1', 5, 5, 'snapshot-2', X'01', "
        "?, 'committed', 6)",
        (SECOND_DIGEST,),
    )


def _migrated_v2() -> sqlite3.Connection:
    connection = _fresh(V2_SCHEMA)
    _seed_v2(connection)
    before = {
        "identity": connection.execute("SELECT * FROM live_journal_identity").fetchall(),
        "records": connection.execute("SELECT * FROM live_journal_records").fetchall(),
        "commits": connection.execute("SELECT * FROM live_reconciliation_commits").fetchall(),
    }
    _execute_script(connection, V2_TO_V3.read_text(encoding="utf-8"))
    assert (
        connection.execute("SELECT * FROM live_journal_identity").fetchall() == before["identity"]
    )
    assert connection.execute("SELECT * FROM live_journal_records").fetchall() == before["records"]
    assert (
        connection.execute("SELECT * FROM live_reconciliation_commits").fetchall()
        == before["commits"]
    )
    return connection


def _authorization(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "authorization_id": "authorization-1",
        "commit_id": "commit-1",
        "journal_id": "journal-1",
        "account_id": "account-1",
        "action_kind": "reconciliation_commit",
        "principal_id": "principal-1",
        "authority_context_digest": DIGEST,
        "source_inspection_digest": DIGEST,
        "operator_plan_digest": DIGEST,
        "request_digest": DIGEST,
        "broker_snapshot_id": "snapshot-1",
        "expected_journal_sequence": 4,
        "authorized_at": "authorized",
        "expires_at": "expires",
        "consumed_at": "consumed",
        "reason_code": "operator_approved",
        "authorization_digest": DIGEST,
        "resulting_journal_sequence": 5,
    }
    values.update(overrides)
    return values


def _insert_authorization(connection: sqlite3.Connection, **overrides: Any) -> sqlite3.Cursor:
    values = _authorization(**overrides)
    columns = ", ".join(values)
    placeholders = ", ".join(f":{column}" for column in values)
    return connection.execute(
        f"INSERT INTO live_reconciliation_commit_authorizations ({columns}) "
        f"VALUES ({placeholders})",
        values,
    )


def test_v2_schema_is_byte_for_byte_frozen_and_v3_fingerprint_is_exact() -> None:
    assert sha256(V2_SCHEMA.read_bytes()).hexdigest() == V2_SHA256
    assert sha256(CURRENT_SCHEMA.read_bytes()).hexdigest() == V3_SHA256
    assert f"sha256:{V3_SHA256}" in V2_TO_V3.read_text(encoding="utf-8")


def test_fresh_v3_and_migrated_v2_have_identical_schema_signatures() -> None:
    fresh = _fresh(CURRENT_SCHEMA)
    migrated = _migrated_v2()
    try:
        assert _signature(migrated) == _signature(fresh)
        assert fresh.execute("PRAGMA application_id").fetchone() == (1415074890,)
        assert fresh.execute("PRAGMA user_version").fetchone() == (3,)
        assert migrated.execute("PRAGMA user_version").fetchone() == (3,)
        assert migrated.execute(
            "SELECT version, schema_fingerprint FROM live_journal_migrations ORDER BY version"
        ).fetchall() == [
            (2, f"sha256:{V2_SHA256}"),
            (3, f"sha256:{V3_SHA256}"),
        ]
    finally:
        fresh.close()
        migrated.close()


def test_v2_objects_are_unchanged_except_identity_check_widening() -> None:
    v2 = _fresh(V2_SCHEMA)
    v3 = _fresh(CURRENT_SCHEMA)
    try:
        v2_objects = {name: (kind, sql) for kind, name, sql in _signature(v2)}
        v3_objects = {name: (kind, sql) for kind, name, sql in _signature(v3)}
        assert set(v3_objects) - set(v2_objects) == NEW_OBJECTS
        assert set(v2_objects) - set(v3_objects) == set()

        for name, signature in v2_objects.items():
            if name != "live_journal_identity":
                assert v3_objects[name] == signature
        v2_identity = v2_objects["live_journal_identity"][1]
        v3_identity = v3_objects["live_journal_identity"][1]
        assert v3_identity == v2_identity.replace(
            "schema_version IN (1, 2)", "schema_version IN (1, 2, 3)"
        )

        for version in (1, 2, 3):
            v3.execute(
                "INSERT INTO live_journal_identity VALUES (1, ?, ?, ?, ?)",
                (f"journal-{version}", version, DIGEST, "created"),
            )
            v3.execute("DELETE FROM live_journal_identity")
        with pytest.raises(sqlite3.IntegrityError):
            v3.execute(
                "INSERT INTO live_journal_identity VALUES (1, 'journal-4', 4, ?, 'created')",
                (DIGEST,),
            )
    finally:
        v2.close()
        v3.close()


@pytest.mark.parametrize(
    "column",
    [
        "authority_context_digest",
        "source_inspection_digest",
        "operator_plan_digest",
        "request_digest",
        "authorization_digest",
    ],
)
@pytest.mark.parametrize(
    "invalid_digest",
    [
        f"sha256:{'a' * 63}",
        f"sha256:{'a' * 65}",
        f"sha256:{'A' * 64}",
        f"sha256:{'a' * 63}g",
        f"sha257:{'a' * 64}",
    ],
)
def test_every_new_digest_requires_exact_lowercase_sha256(column: str, invalid_digest: str) -> None:
    connection = _migrated_v2()
    try:
        with pytest.raises(sqlite3.IntegrityError):
            _insert_authorization(connection, **{column: invalid_digest})
    finally:
        connection.close()


def test_authorization_constraints_indexes_and_append_only_triggers() -> None:
    connection = _migrated_v2()
    try:
        _insert_authorization(connection)
        indexes = {
            row[1]: tuple(
                column[2] for column in connection.execute(f"PRAGMA index_info('{row[1]}')")
            )
            for row in connection.execute(
                "PRAGMA index_list('live_reconciliation_commit_authorizations')"
            )
        }
        assert indexes["live_reconciliation_commit_authorizations_account_idx"] == (
            "account_id",
            "consumed_at",
            "authorization_id",
        )
        assert indexes["live_reconciliation_commit_authorizations_principal_idx"] == (
            "principal_id",
            "consumed_at",
            "authorization_id",
        )

        with pytest.raises(sqlite3.IntegrityError, match="append-only authorization audit"):
            connection.execute(
                "UPDATE live_reconciliation_commit_authorizations SET reason_code = 'changed'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only authorization audit"):
            connection.execute("DELETE FROM live_reconciliation_commit_authorizations")

        for values in (
            {"authorization_id": "authorization-2", "commit_id": "missing"},
            {"authorization_id": "authorization-2"},
            {
                "authorization_id": "authorization-2",
                "commit_id": "commit-2",
                "action_kind": "generic",
            },
            {
                "authorization_id": "authorization-2",
                "commit_id": "commit-2",
                "expected_journal_sequence": -1,
            },
            {
                "authorization_id": "authorization-2",
                "commit_id": "commit-2",
                "resulting_journal_sequence": 4,
            },
        ):
            with pytest.raises(sqlite3.IntegrityError):
                _insert_authorization(connection, **values)
    finally:
        connection.close()


def test_migration_composes_in_caller_transaction_and_rolls_back_failure() -> None:
    script = V2_TO_V3.read_text(encoding="utf-8")
    assert (
        re.search(
            r"^\s*(?:BEGIN(?:\s+(?:DEFERRED|IMMEDIATE|EXCLUSIVE|TRANSACTION))?"
            r"|COMMIT|ROLLBACK)\s*;",
            script,
            re.IGNORECASE | re.MULTILINE,
        )
        is None
    )

    connection = _fresh(V2_SCHEMA)
    try:
        _seed_v2(connection)
        connection.commit()
        signature_before = _signature(connection)
        connection.execute("BEGIN IMMEDIATE")
        _execute_script(connection, script)
        assert connection.in_transaction
        with pytest.raises(sqlite3.IntegrityError):
            _insert_authorization(connection, action_kind="generic")
        connection.rollback()

        assert _signature(connection) == signature_before
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        assert connection.execute(
            "SELECT version FROM live_journal_migrations ORDER BY version"
        ).fetchall() == [(2,)]
        assert connection.execute("SELECT record_id FROM live_journal_records").fetchall() == [
            ("journal-1",)
        ]
    finally:
        connection.close()
