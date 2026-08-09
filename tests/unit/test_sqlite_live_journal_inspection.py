from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import os
from pathlib import Path
import sqlite3
from typing import Callable

import pytest

import tx_trade.orders.sqlite_live_journal_inspection as inspection_module
from tests.support.live_journal_inspection_scenarios import (
    create_frozen_v1,
    create_multi_account_foreign_secrets,
    create_semantically_blocked_v2,
    create_v2,
    create_v2_with_claim,
    database_rows,
    schema_signature,
)
from tx_trade.orders.live_journal_inspection_contracts import (
    LiveJournalInspectionDisposition,
    LiveJournalInspectionError,
    LiveJournalInspectionFailureCode,
    LiveJournalInspectionIssueCode,
    LiveJournalInspectionTargetKind,
)
from tx_trade.orders.live_journal_contracts import JournalOpenMode
from tx_trade.orders.live_ports import RawBrokerObservation
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal
from tx_trade.orders.sqlite_live_journal_inspection import (
    inspect_sqlite_live_order_journal,
)

Inspect = Callable[[str | Path], object]
SECRETS = (
    "account-a",
    "order-a",
    "command-a",
    "claim-token-secret",
    "claimant-secret",
    "payload-secret",
)


def _inspect(path: str | Path, account_id: str = "account-a"):
    return inspect_sqlite_live_order_journal(path, account_id=account_id)


def _artifact_snapshot(path: Path) -> dict[str, tuple[bytes, int, int]]:
    result: dict[str, tuple[bytes, int, int]] = {}
    for artifact in (path, Path(f"{path}-wal"), Path(f"{path}-shm"), Path(f"{path}-journal")):
        if artifact.exists():
            stat = artifact.stat()
            result[artifact.name] = (artifact.read_bytes(), stat.st_size, stat.st_mtime_ns)
    return result


def _assert_sanitized(error: BaseException) -> None:
    rendered = f"{error!s} {error!r}"
    assert not any(secret in rendered for secret in SECRETS)
    assert ".sqlite" not in rendered


def _assert_failure(
    path: str | Path,
    code: LiveJournalInspectionFailureCode,
    *,
    account_id: str = "account-a",
) -> LiveJournalInspectionError:
    with pytest.raises(LiveJournalInspectionError) as raised:
        _inspect(path, account_id)
    assert raised.value.code is code
    _assert_sanitized(raised.value)
    return raised.value


def test_clean_v2_is_deterministic_and_bitwise_read_only(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path, orders=(("account-a", "order-a", "command-a"),))
    before_artifacts = _artifact_snapshot(path)
    before_rows = database_rows(path)
    before_schema = schema_signature(path)
    connection = sqlite3.connect(path)
    try:
        before_user_version = connection.execute("PRAGMA user_version").fetchone()
    finally:
        connection.close()

    first = _inspect(path)
    second = _inspect(path)

    assert first == second
    assert first.inspection_digest == second.inspection_digest
    assert first.database_schema_version == 2
    assert _artifact_snapshot(path) == before_artifacts
    assert database_rows(path) == before_rows
    assert schema_signature(path) == before_schema
    connection = sqlite3.connect(path)
    try:
        assert connection.execute("PRAGMA user_version").fetchone() == before_user_version
    finally:
        connection.close()
    assert set(_artifact_snapshot(path)) == {path.name}
    assert sha256(path.read_bytes()).digest() == sha256(before_artifacts[path.name][0]).digest()


@pytest.mark.parametrize("with_claim", (False, True))
def test_frozen_v1_requests_upgrade_without_migration(tmp_path: Path, with_claim: bool) -> None:
    path = tmp_path / "journal-v1.sqlite3"
    create_frozen_v1(path, with_claim=with_claim)
    before = _artifact_snapshot(path)
    rows = database_rows(path)
    signature = schema_signature(path)

    report = _inspect(path)

    assert report.disposition is LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED
    assert report.issue_codes == (LiveJournalInspectionIssueCode.SCHEMA_UPGRADE_REQUIRED,)
    assert report.database_schema_version == 1
    assert _artifact_snapshot(path) == before
    assert database_rows(path) == rows
    assert schema_signature(path) == signature
    assert "live_reconciliation_commits" not in rows


@pytest.mark.parametrize("suffix", ("-wal", "-shm", "-journal"))
def test_partial_or_rollback_sidecar_fails_before_mutation(tmp_path: Path, suffix: str) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path)
    sidecar = Path(f"{path}{suffix}")
    sidecar.write_bytes(b"attacker-sidecar-secret")
    before = _artifact_snapshot(path)

    _assert_failure(path, LiveJournalInspectionFailureCode.ACTIVE_OR_UNCLEAN_SOURCE)

    assert _artifact_snapshot(path) == before


def test_existing_wal_pair_is_rejected_without_touching_any_artifact(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path, orders=(("account-a", "order-a", "command-a"),))
    writer = sqlite3.connect(path)
    try:
        assert writer.execute("PRAGMA journal_mode = WAL").fetchone() == ("wal",)
        writer.execute("BEGIN IMMEDIATE")
        writer.execute(
            "UPDATE live_orders SET updated_at = updated_at WHERE client_order_id = 'order-a'"
        )
        writer.commit()
        before = _artifact_snapshot(path)
        _assert_failure(path, LiveJournalInspectionFailureCode.ACTIVE_OR_UNCLEAN_SOURCE)
        assert _artifact_snapshot(path) == before
    finally:
        writer.close()


@pytest.mark.parametrize(
    ("make_path", "code"),
    (
        (
            lambda root: root / "missing.sqlite3",
            LiveJournalInspectionFailureCode.SOURCE_UNAVAILABLE,
        ),
        (lambda root: root, LiveJournalInspectionFailureCode.INVALID_REQUEST),
    ),
)
def test_unavailable_source_shapes_are_typed_and_sanitized(
    tmp_path: Path,
    make_path: Callable[[Path], Path],
    code: LiveJournalInspectionFailureCode,
) -> None:
    _assert_failure(make_path(tmp_path), code)


def test_nul_path_is_invalid_before_filesystem_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nul_path = "bad\x00secret.sqlite3"

    def unexpected_lstat(_path: object) -> os.stat_result:
        pytest.fail("NUL path reached filesystem validation")

    monkeypatch.setattr(inspection_module.os, "lstat", unexpected_lstat)

    error = _assert_failure(nul_path, LiveJournalInspectionFailureCode.INVALID_REQUEST)

    assert "bad" not in str(error)
    assert "secret" not in repr(error)


def test_symlink_and_hardlink_are_rejected_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path)
    links: list[Path] = []
    symbolic = tmp_path / "symbolic.sqlite3"
    try:
        symbolic.symlink_to(path)
    except OSError:
        pass
    else:
        links.append(symbolic)
    hard = tmp_path / "hard.sqlite3"
    try:
        os.link(path, hard)
    except OSError:
        pass
    else:
        links.append(hard)
    if not links:
        pytest.skip("filesystem does not support test links")
    before = path.read_bytes()
    for link in links:
        _assert_failure(link, LiveJournalInspectionFailureCode.INVALID_REQUEST)
    assert path.read_bytes() == before


@pytest.mark.parametrize("payload", (b"not sqlite", b"SQLite format 3\x00broken"))
def test_malformed_database_is_integrity_failure(tmp_path: Path, payload: bytes) -> None:
    path = tmp_path / "secret.sqlite3"
    path.write_bytes(payload)
    before = _artifact_snapshot(path)
    _assert_failure(path, LiveJournalInspectionFailureCode.INTEGRITY_FAILURE)
    assert _artifact_snapshot(path) == before


@pytest.mark.parametrize("header_pair", ((1, 2), (2, 1)))
def test_hybrid_sqlite_header_pair_fails_closed_without_mutation(
    tmp_path: Path,
    header_pair: tuple[int, int],
) -> None:
    path = tmp_path / "hybrid-header.sqlite3"
    create_v2(path)
    tampered = bytearray(path.read_bytes())
    tampered[18], tampered[19] = header_pair
    path.write_bytes(tampered)
    before = _artifact_snapshot(path)

    error = _assert_failure(path, LiveJournalInspectionFailureCode.INTEGRITY_FAILURE)

    _assert_sanitized(error)
    assert _artifact_snapshot(path) == before


@pytest.mark.parametrize(
    "statement",
    (
        "PRAGMA application_id = 7",
        "PRAGMA user_version = 77",
        "UPDATE live_journal_migrations SET schema_fingerprint = 'sha256:' || printf('%064d', 1) WHERE version = 2",
        "UPDATE live_journal_records SET payload_digest = 'sha256:' || printf('%064d', 2) WHERE journal_sequence = 1",
        "UPDATE live_journal_records SET journal_sequence = journal_sequence + 100 WHERE journal_sequence = 1",
    ),
)
def test_header_schema_digest_and_sequence_tamper_fail_closed(
    tmp_path: Path, statement: str
) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path, orders=(("account-a", "order-a", "command-a"),))
    connection = sqlite3.connect(path)
    try:
        connection.execute(statement)
        connection.commit()
    finally:
        connection.close()
    before = _artifact_snapshot(path)

    try:
        report = _inspect(path)
    except LiveJournalInspectionError as error:
        assert error.code is LiveJournalInspectionFailureCode.INTEGRITY_FAILURE
        _assert_sanitized(error)
    else:
        assert report.disposition is LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE
        assert report.issue_codes == (LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,)
        assert report.targets == ()
    assert _artifact_snapshot(path) == before


def test_pending_and_claim_targets_are_canonical_opaque_and_redacted(tmp_path: Path) -> None:
    pending_path = tmp_path / "pending.sqlite3"
    claim_path = tmp_path / "claim.sqlite3"
    create_v2(pending_path, orders=(("account-a", "order-a", "command-a"),))
    create_v2_with_claim(claim_path)

    pending = _inspect(pending_path)
    claimed = _inspect(claim_path)

    assert pending.disposition is LiveJournalInspectionDisposition.RECOVERY_REQUIRED
    assert claimed.disposition is LiveJournalInspectionDisposition.RECOVERY_REQUIRED
    assert any(
        target.kind is LiveJournalInspectionTargetKind.PENDING_COMMAND for target in pending.targets
    )
    assert any(target.kind is LiveJournalInspectionTargetKind.CLAIM for target in claimed.targets)
    for report in (pending, claimed, _inspect(claim_path)):
        assert all(target.target_id not in SECRETS for target in report.targets)
        assert all(len(target.target_id) <= 128 for target in report.targets)
        rendered = repr(report)
        assert not any(secret in rendered for secret in SECRETS)
    assert claimed == _inspect(claim_path)


def test_other_account_identifiers_do_not_affect_selected_account(tmp_path: Path) -> None:
    first_path = tmp_path / "first.sqlite3"
    second_path = tmp_path / "second.sqlite3"
    create_v2(
        first_path,
        orders=(
            ("account-a", "order-a", "command-a"),
            ("account-b", "order-b-one", "command-b-one"),
        ),
    )
    create_v2(
        second_path,
        orders=(
            ("account-a", "order-a", "command-a"),
            ("account-b", "order-b-two", "command-b-two"),
        ),
    )

    first = _inspect(first_path)
    second = _inspect(second_path)

    assert first.disposition == second.disposition
    assert first.issue_codes == second.issue_codes
    assert first.targets == second.targets
    assert first.inspection_digest == second.inspection_digest
    for output in (repr(first), repr(second), first.inspection_digest, second.inspection_digest):
        assert "account-b" not in output
        assert "order-b" not in output
        assert "command-b" not in output


def test_foreign_claim_token_and_raw_payload_do_not_affect_selected_report(
    tmp_path: Path,
) -> None:
    first_path = tmp_path / "first.sqlite3"
    second_path = tmp_path / "second.sqlite3"
    create_multi_account_foreign_secrets(
        first_path,
        foreign_claim_token="foreign-token-one",
        foreign_raw_payload=b"foreign-raw-payload-one",
    )
    create_multi_account_foreign_secrets(
        second_path,
        foreign_claim_token="foreign-token-two",
        foreign_raw_payload=b"foreign-raw-payload-two",
    )

    first = _inspect(first_path)
    second = _inspect(second_path)

    assert first == second
    rendered = f"{first!r} {second!r}"
    for secret in (
        "account-b",
        "order-b",
        "command-b",
        "foreign-token",
        "foreign-claimant",
        "foreign-raw-payload",
    ):
        assert secret not in rendered


def test_large_valid_aggregate_snapshot_is_inspected_without_false_block(
    tmp_path: Path,
) -> None:
    path = tmp_path / "large-aggregate.sqlite3"
    create_v2(path, orders=(("account-a", "order-a", "command-a"),))
    secret = b"sqlite-large-aggregate-secret" + b"x" * 20_000
    observed_at = datetime(2026, 8, 9, 1, tzinfo=timezone.utc)
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: observed_at,
        claim_token_factory=lambda: "unused-token",
    )
    try:
        for index in range(64):
            journal.append_raw_observation(
                RawBrokerObservation(
                    f"large-observation-{index}",
                    "reply",
                    1,
                    index + 1,
                    observed_at,
                    secret,
                )
            )
    finally:
        journal.close()

    report = _inspect(path)

    assert path.stat().st_size < inspection_module.MAX_INSPECTION_MAIN_DATABASE_BYTES
    assert report.disposition is LiveJournalInspectionDisposition.RECOVERY_REQUIRED
    assert LiveJournalInspectionIssueCode.GLOBAL_RECOVERY_BLOCKER in report.issue_codes
    assert report.disposition is not LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE
    assert secret.decode("ascii") not in repr(report)


def test_missing_account_returns_only_redacted_status(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path, orders=(("account-b", "order-b", "command-b"),))
    report = _inspect(path, "account-a")
    assert report.disposition is LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND
    assert report.issue_codes == (LiveJournalInspectionIssueCode.ACCOUNT_NOT_FOUND,)
    assert report.targets == ()
    assert "account-a" not in repr(report)
    assert "account-b" not in repr(report)


def test_semantic_integrity_failure_returns_only_generic_blocked_report(
    tmp_path: Path,
) -> None:
    path = tmp_path / "journal.sqlite3"
    create_semantically_blocked_v2(path)
    before = _artifact_snapshot(path)

    report = _inspect(path)

    assert report.disposition is LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE
    assert report.issue_codes == (LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,)
    assert report.targets == ()
    assert not any(secret in repr(report) for secret in SECRETS)
    assert _artifact_snapshot(path) == before


@pytest.mark.parametrize("account_id", ("", "contains space", "a" * 129, "帳戶"))
def test_invalid_account_request_is_sanitized(tmp_path: Path, account_id: str) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path)
    error = _assert_failure(
        path,
        LiveJournalInspectionFailureCode.INVALID_REQUEST,
        account_id=account_id,
    )
    if account_id:
        assert account_id not in str(error)


def test_sqlite_authorizer_observes_only_read_operations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path, orders=(("account-a", "order-a", "command-a"),))
    original_connect = sqlite3.connect
    actions: list[int] = []
    statements: list[str] = []

    class AuditedConnection(sqlite3.Connection):
        def set_authorizer(self, authorizer):  # type: ignore[no-untyped-def]
            def audited_authorizer(
                action: int,
                first: str | None,
                second: str | None,
                database: str | None,
                trigger: str | None,
            ) -> int:
                actions.append(action)
                return authorizer(action, first, second, database, trigger)

            return super().set_authorizer(audited_authorizer)

        def execute(self, sql, parameters=()):  # type: ignore[no-untyped-def]
            statements.append(sql)
            return super().execute(sql, parameters)

    def audited_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        kwargs["factory"] = AuditedConnection
        connection = original_connect(*args, **kwargs)
        return connection

    monkeypatch.setattr(sqlite3, "connect", audited_connect)
    report = _inspect(path)
    assert report.database_schema_version == 2
    forbidden = {
        sqlite3.SQLITE_INSERT,
        sqlite3.SQLITE_UPDATE,
        sqlite3.SQLITE_DELETE,
        sqlite3.SQLITE_CREATE_INDEX,
        sqlite3.SQLITE_CREATE_TABLE,
        sqlite3.SQLITE_CREATE_TRIGGER,
        sqlite3.SQLITE_CREATE_VIEW,
        sqlite3.SQLITE_DROP_INDEX,
        sqlite3.SQLITE_DROP_TABLE,
        sqlite3.SQLITE_DROP_TRIGGER,
        sqlite3.SQLITE_DROP_VIEW,
        sqlite3.SQLITE_ALTER_TABLE,
        sqlite3.SQLITE_ATTACH,
        sqlite3.SQLITE_DETACH,
    }
    assert not forbidden.intersection(actions)
    sql = "\n".join(statements).upper()
    assert "INSERT " not in sql
    assert "UPDATE " not in sql
    assert "DELETE " not in sql
    assert "CHECKPOINT" not in sql
    assert "ATTACH " not in sql
    assert "BEGIN IMMEDIATE" not in sql
    assert "BEGIN EXCLUSIVE" not in sql


def test_connection_redirect_to_different_valid_database_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.sqlite3"
    replacement = tmp_path / "replacement.sqlite3"
    create_v2(source, orders=(("account-a", "order-a", "command-a"),))
    create_v2(replacement, orders=(("account-a", "order-b", "command-b"),))
    source_before = _artifact_snapshot(source)
    replacement_before = _artifact_snapshot(replacement)
    original_connect = sqlite3.connect

    def redirected_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        redirected_uri = f"{replacement.resolve().as_uri()}?mode=ro&immutable=1&cache=private"
        return original_connect(redirected_uri, uri=True, isolation_level=None)

    monkeypatch.setattr(sqlite3, "connect", redirected_connect)

    error = _assert_failure(source, LiveJournalInspectionFailureCode.SOURCE_CHANGED)

    _assert_sanitized(error)
    assert _artifact_snapshot(source) == source_before
    assert _artifact_snapshot(replacement) == replacement_before


def test_same_size_same_mtime_main_content_change_is_detected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path, orders=(("account-a", "order-a", "command-a"),))
    original_bytes = path.read_bytes()
    original_stat = path.stat()
    original_builder = inspection_module._build_live_journal_inspection_report

    def mutate_after_report(*args: object, **kwargs: object):
        report = original_builder(*args, **kwargs)
        changed = bytearray(original_bytes)
        changed[-1] ^= 1
        path.write_bytes(changed)
        os.utime(path, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
        return report

    monkeypatch.setattr(
        inspection_module,
        "_build_live_journal_inspection_report",
        mutate_after_report,
    )

    try:
        error = _assert_failure(path, LiveJournalInspectionFailureCode.SOURCE_CHANGED)
        _assert_sanitized(error)
        changed_stat = path.stat()
        assert changed_stat.st_size == original_stat.st_size
        assert changed_stat.st_mtime_ns == original_stat.st_mtime_ns
    finally:
        path.write_bytes(original_bytes)
        os.utime(path, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))


def test_post_bind_queries_cannot_observe_substituted_live_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.sqlite3"
    replacement = tmp_path / "replacement.sqlite3"
    create_v2(source)
    create_v2(replacement, orders=(("account-a", "order-a", "command-a"),))
    original_connect = sqlite3.connect
    replacement_connection = original_connect(
        f"{replacement.resolve().as_uri()}?mode=ro&immutable=1&cache=private",
        uri=True,
        isolation_level=None,
    )
    replacement_connection.row_factory = sqlite3.Row
    post_bind_queries: list[str] = []

    class SubstitutingConnection(sqlite3.Connection):
        sealed = False

        def serialize(self, *args: object, **kwargs: object) -> bytes:
            serialized = super().serialize(*args, **kwargs)
            self.sealed = True
            return serialized

        def execute(self, sql, parameters=()):  # type: ignore[no-untyped-def]
            if self.sealed and sql.strip().upper() != "ROLLBACK":
                post_bind_queries.append(sql)
                return replacement_connection.execute(sql, parameters)
            return super().execute(sql, parameters)

    def substituting_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        kwargs["factory"] = SubstitutingConnection
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", substituting_connect)
    try:
        report = _inspect(source)
    finally:
        replacement_connection.close()

    assert report.disposition is LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND
    assert report.issue_codes == (LiveJournalInspectionIssueCode.ACCOUNT_NOT_FOUND,)
    assert post_bind_queries == []
