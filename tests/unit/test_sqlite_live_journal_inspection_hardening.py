from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import sqlite3
from typing import Any

import pytest

import tx_trade.orders.sqlite_live_journal_inspection as inspection
from tests.support.live_journal_inspection_scenarios import create_v2
from tx_trade.orders.live_journal_inspection_contracts import (
    LiveJournalInspectionError,
    LiveJournalInspectionFailureCode,
)


_SECRET = "hardening-account-secret"


def _snapshot(path: Path) -> dict[str, tuple[bytes, int, int, str]]:
    artifacts = (path, *(Path(f"{path}{suffix}") for suffix in ("-wal", "-shm", "-journal")))
    result = {}
    for artifact in artifacts:
        if artifact.exists():
            stat = artifact.stat()
            payload = artifact.read_bytes()
            result[artifact.name] = (
                payload,
                stat.st_size,
                stat.st_mtime_ns,
                sha256(payload).hexdigest(),
            )
    return result


def _assert_sanitized_capacity(path: Path) -> LiveJournalInspectionError:
    before = _snapshot(path)
    with pytest.raises(LiveJournalInspectionError) as raised:
        inspection.inspect_sqlite_live_order_journal(path, account_id=_SECRET)
    assert raised.value.code is LiveJournalInspectionFailureCode.CAPACITY_EXCEEDED
    rendered = f"{raised.value!s} {raised.value!r}"
    assert _SECRET not in rendered
    assert path.name not in rendered
    assert ".sqlite" not in rendered
    assert _snapshot(path) == before

    # This is intentionally an OS-level lifetime check.  On platforms with
    # restrictive SQLite/file handles, a leaked connection or descriptor makes
    # the rename fail; the round trip also proves that no artifact was created.
    moved = path.with_suffix(".moved")
    path.rename(moved)
    moved.rename(path)
    assert _snapshot(path) == before
    return raised.value


def _authorizer_callback() -> Any:
    class Capture:
        callback: Any = None

        def set_authorizer(self, callback: Any) -> None:
            self.callback = callback

    connection = Capture()
    inspection._install_authorizer(connection)  # type: ignore[arg-type]
    assert connection.callback is not None
    return connection.callback


@pytest.mark.parametrize(
    "action_name",
    (
        "SQLITE_INSERT",
        "SQLITE_UPDATE",
        "SQLITE_DELETE",
        "SQLITE_CREATE_INDEX",
        "SQLITE_CREATE_TABLE",
        "SQLITE_CREATE_TEMP_INDEX",
        "SQLITE_CREATE_TEMP_TABLE",
        "SQLITE_CREATE_TEMP_TRIGGER",
        "SQLITE_CREATE_TEMP_VIEW",
        "SQLITE_CREATE_TRIGGER",
        "SQLITE_CREATE_VIEW",
        "SQLITE_CREATE_VTABLE",
        "SQLITE_DROP_INDEX",
        "SQLITE_DROP_TABLE",
        "SQLITE_DROP_TEMP_INDEX",
        "SQLITE_DROP_TEMP_TABLE",
        "SQLITE_DROP_TEMP_TRIGGER",
        "SQLITE_DROP_TEMP_VIEW",
        "SQLITE_DROP_TRIGGER",
        "SQLITE_DROP_VIEW",
        "SQLITE_DROP_VTABLE",
        "SQLITE_ALTER_TABLE",
        "SQLITE_REINDEX",
        "SQLITE_ANALYZE",
        "SQLITE_ATTACH",
        "SQLITE_DETACH",
        "SQLITE_SAVEPOINT",
    ),
)
def test_authorizer_directly_denies_mutating_and_ambient_actions(action_name: str) -> None:
    action = getattr(sqlite3, action_name, None)
    if action is None:
        pytest.skip(f"{action_name} is unavailable in this SQLite build")
    assert _authorizer_callback()(action, "target", None, "main", None) == sqlite3.SQLITE_DENY


@pytest.mark.parametrize("verb", ("BEGIN", "begin", "ROLLBACK", "rollback"))
def test_authorizer_allows_only_safe_transaction_verbs(verb: str) -> None:
    callback = _authorizer_callback()
    assert callback(sqlite3.SQLITE_TRANSACTION, verb, None, None, None) == sqlite3.SQLITE_OK


@pytest.mark.parametrize("verb", (None, "COMMIT", "END", "RELEASE"))
def test_authorizer_denies_other_transaction_verbs(verb: str | None) -> None:
    callback = _authorizer_callback()
    assert callback(sqlite3.SQLITE_TRANSACTION, verb, None, None, None) == sqlite3.SQLITE_DENY


@pytest.mark.parametrize(
    "pragma",
    (
        "application_id",
        "foreign_key_check",
        "quick_check",
        "page_count",
        "page_size",
        "user_version",
    ),
)
def test_authorizer_allows_exact_pragma_getter_allowlist_without_value(pragma: str) -> None:
    callback = _authorizer_callback()
    assert callback(sqlite3.SQLITE_PRAGMA, pragma.upper(), None, None, None) == sqlite3.SQLITE_OK
    assert callback(sqlite3.SQLITE_PRAGMA, pragma, "1", None, None) == sqlite3.SQLITE_DENY


@pytest.mark.parametrize("pragma", ("journal_mode", "query_only", "trusted_schema", "cache_size"))
def test_authorizer_denies_nonallowlisted_pragmas(pragma: str) -> None:
    callback = _authorizer_callback()
    assert callback(sqlite3.SQLITE_PRAGMA, pragma, None, None, None) == sqlite3.SQLITE_DENY


@pytest.mark.parametrize("function", ("load_extension", "LOAD_EXTENSION", "LoAd_ExTeNsIoN"))
def test_authorizer_denies_load_extension_case_insensitively(function: str) -> None:
    callback = _authorizer_callback()
    assert callback(sqlite3.SQLITE_FUNCTION, None, function, None, None) == sqlite3.SQLITE_DENY


def test_installed_authorizer_rejects_sql_on_source_and_isolated_connections() -> None:
    source = sqlite3.connect(":memory:", isolation_level=None)
    isolated: sqlite3.Connection | None = None
    try:
        source.execute("CREATE TABLE guarded(value INTEGER)")
        source.execute("INSERT INTO guarded VALUES (1)")
        serialized = source.serialize()
        inspection._install_authorizer(source)
        isolated = inspection._open_isolated_inspection_connection(serialized)

        attempts = (
            (source, "UPDATE guarded SET value = 2"),
            (source, "CREATE TEMP TABLE forbidden(value INTEGER)"),
            (source, "PRAGMA user_version = 7"),
            (isolated, "DELETE FROM guarded"),
            (isolated, "SAVEPOINT forbidden"),
        )
        for connection, statement in attempts:
            with pytest.raises(sqlite3.DatabaseError) as raised:
                connection.execute(statement)
            assert (
                getattr(raised.value, "sqlite_errorcode", sqlite3.SQLITE_AUTH)
                == sqlite3.SQLITE_AUTH
            )

        with pytest.raises(sqlite3.DatabaseError) as raised:
            isolated.execute("SELECT LoAd_ExTeNsIoN('not-a-real-library')")
        # Some SQLite builds reject the function before the authorizer and
        # expose SQLITE_ERROR instead of SQLITE_AUTH.  Either is fail-closed.
        assert getattr(raised.value, "sqlite_errorcode", sqlite3.SQLITE_ERROR) in {
            sqlite3.SQLITE_AUTH,
            sqlite3.SQLITE_ERROR,
        }
    finally:
        if isolated is not None:
            isolated.close()
        source.close()


def test_frozen_capacity_constants_and_interrupt_mapping() -> None:
    assert inspection.MAX_INSPECTION_MAIN_DATABASE_BYTES == 64 * 1024 * 1024
    assert inspection.MAX_INSPECTION_SERIALIZED_DATABASE_BYTES == 64 * 1024 * 1024
    assert inspection.MAX_INSPECTION_TOTAL_ROWS == 25_000
    assert inspection.INSPECTION_PROGRESS_OPCODE_INTERVAL == 1_000
    assert inspection.MAX_INSPECTION_PROGRESS_CALLBACKS == 100_000

    interrupted = sqlite3.OperationalError("private detail")
    interrupted.sqlite_errorcode = sqlite3.SQLITE_INTERRUPT  # type: ignore[attr-defined]
    assert (
        inspection._sqlite_failure_code(interrupted)
        is LiveJournalInspectionFailureCode.CAPACITY_EXCEEDED
    )


@pytest.mark.parametrize(
    "limit_name", ("MAX_INSPECTION_MAIN_DATABASE_BYTES", "MAX_INSPECTION_SERIALIZED_DATABASE_BYTES")
)
def test_main_and_serialized_size_limits_accept_exact_boundary_and_reject_plus_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    limit_name: str,
) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path)
    size = path.stat().st_size

    monkeypatch.setattr(inspection, limit_name, size)
    inspection.inspect_sqlite_live_order_journal(path, account_id=_SECRET)

    monkeypatch.setattr(inspection, limit_name, size - 1)
    _assert_sanitized_capacity(path)


def _durable_row_count(path: Path) -> int:
    connection = sqlite3.connect(path)
    try:
        return sum(
            connection.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
            for table in inspection._V2_DURABLE_TABLES
        )
    finally:
        connection.close()


def test_total_row_limit_accepts_exact_boundary_and_rejects_plus_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path, orders=((_SECRET, "order", "command"),))
    row_count = _durable_row_count(path)

    monkeypatch.setattr(inspection, "MAX_INSPECTION_TOTAL_ROWS", row_count)
    inspection.inspect_sqlite_live_order_journal(path, account_id=_SECRET)

    monkeypatch.setattr(inspection, "MAX_INSPECTION_TOTAL_ROWS", row_count - 1)
    _assert_sanitized_capacity(path)


def test_progress_budget_abort_is_sanitized_capacity_and_zero_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path, orders=((_SECRET, "order", "command"),))
    monkeypatch.setattr(inspection, "INSPECTION_PROGRESS_OPCODE_INTERVAL", 1)
    monkeypatch.setattr(inspection, "MAX_INSPECTION_PROGRESS_CALLBACKS", 0)
    _assert_sanitized_capacity(path)


def test_progress_budget_aborts_on_the_maximum_callback_invocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Capture:
        callback: Any = None
        opcode_interval: int | None = None

        def set_progress_handler(self, callback: Any, opcode_interval: int) -> None:
            self.callback = callback
            self.opcode_interval = opcode_interval

    connection = Capture()
    monkeypatch.setattr(inspection, "MAX_INSPECTION_PROGRESS_CALLBACKS", 3)
    inspection._install_progress_budget(connection)  # type: ignore[arg-type]

    assert connection.opcode_interval == inspection.INSPECTION_PROGRESS_OPCODE_INTERVAL
    assert [connection.callback() for _ in range(3)] == [0, 0, 1]


def test_snapshot_file_owned_descriptor_close_memory_error_is_not_swallowed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "source.bin"
    path.write_bytes(b"content")
    real_close = inspection.os.close

    def close_with_memory_error(descriptor: int) -> None:
        real_close(descriptor)
        raise MemoryError("close secret")

    monkeypatch.setattr(inspection.os, "close", close_with_memory_error)

    with pytest.raises(MemoryError, match="close secret"):
        inspection._snapshot_file(path, maximum_bytes=path.stat().st_size)


class _TrackedConnection(sqlite3.Connection):
    instances: list[_TrackedConnection] = []

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.closed_by_inspector = False
        type(self).instances.append(self)

    def close(self) -> None:
        self.closed_by_inspector = True
        super().close()


class _SerializeMemoryConnection(_TrackedConnection):
    def serialize(self, *args: Any, **kwargs: Any) -> bytes:
        raise MemoryError("serialize secret")


class _DeserializeMemoryConnection(_TrackedConnection):
    def deserialize(self, *args: Any, **kwargs: Any) -> None:
        raise MemoryError("deserialize secret")


def _install_tracking_factory(
    monkeypatch: pytest.MonkeyPatch,
    connection_type: type[_TrackedConnection],
) -> list[_TrackedConnection]:
    connection_type.instances = []
    original = sqlite3.connect

    def connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        kwargs["factory"] = connection_type
        return original(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", connect)
    monkeypatch.setattr(inspection, "_SQLITE_CONNECT", connect)
    return connection_type.instances


@pytest.mark.parametrize("stage", ("serialize", "deserialize", "reader", "report"))
def test_memory_error_paths_are_capacity_sanitized_zero_write_and_close_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path, orders=((_SECRET, "order", "command"),))
    connection_type = (
        _SerializeMemoryConnection if stage == "serialize" else _DeserializeMemoryConnection
    )
    if stage in {"reader", "report"}:
        connection_type = _TrackedConnection
    instances = _install_tracking_factory(monkeypatch, connection_type)

    if stage == "reader":
        real_reader = inspection._ConnectionBoundLiveJournalReader

        class MemoryReader(real_reader):
            def load(self, *args: Any, **kwargs: Any) -> Any:
                raise MemoryError("reader secret")

        monkeypatch.setattr(inspection, "_ConnectionBoundLiveJournalReader", MemoryReader)
    elif stage == "report":

        def raise_memory(*args: Any, **kwargs: Any) -> Any:
            raise MemoryError("report secret")

        monkeypatch.setattr(inspection, "_build_live_journal_inspection_report", raise_memory)

    _assert_sanitized_capacity(path)
    assert instances
    assert all(connection.closed_by_inspector for connection in instances)


def test_final_source_change_overrides_earlier_capacity_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path, orders=((_SECRET, "order", "command"),))
    before_artifacts = _snapshot(path)
    real_snapshot = inspection._snapshot_source
    calls = 0

    def changing_final_snapshot(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        snapshot = real_snapshot(*args, **kwargs)
        if calls >= 3:
            return (*snapshot[:-1], object())
        return snapshot

    class MemoryReader(inspection._ConnectionBoundLiveJournalReader):
        def load(self, *args: Any, **kwargs: Any) -> Any:
            raise MemoryError("capacity secret")

    monkeypatch.setattr(inspection, "_snapshot_source", changing_final_snapshot)
    monkeypatch.setattr(inspection, "_ConnectionBoundLiveJournalReader", MemoryReader)

    with pytest.raises(LiveJournalInspectionError) as raised:
        inspection.inspect_sqlite_live_order_journal(path, account_id=_SECRET)
    assert raised.value.code is LiveJournalInspectionFailureCode.SOURCE_CHANGED
    assert _SECRET not in f"{raised.value!s} {raised.value!r}"
    assert _snapshot(path) == before_artifacts


@pytest.mark.parametrize("stage", ("cleanup", "final_snapshot"))
def test_memory_error_during_cleanup_overrides_primary_integrity_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    path = tmp_path / "journal.sqlite3"
    create_v2(path, orders=((_SECRET, "order", "command"),))

    class IntegrityReader(inspection._ConnectionBoundLiveJournalReader):
        def load(self, *args: Any, **kwargs: Any) -> Any:
            raise ValueError("integrity secret")

    monkeypatch.setattr(inspection, "_ConnectionBoundLiveJournalReader", IntegrityReader)
    if stage == "cleanup":
        original = sqlite3.connect

        class CleanupMemoryConnection(sqlite3.Connection):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                self.is_isolated = bool(args and args[0] == ":memory:")

            def close(self) -> None:
                super().close()
                if self.is_isolated:
                    raise MemoryError("cleanup secret")

        def connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
            kwargs["factory"] = CleanupMemoryConnection
            return original(*args, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", connect)
        monkeypatch.setattr(inspection, "_SQLITE_CONNECT", connect)
    else:
        real_snapshot = inspection._snapshot_source
        calls = 0

        def final_snapshot_memory_error(*args: Any, **kwargs: Any) -> Any:
            nonlocal calls
            calls += 1
            if calls >= 3:
                raise MemoryError("snapshot secret")
            return real_snapshot(*args, **kwargs)

        monkeypatch.setattr(inspection, "_snapshot_source", final_snapshot_memory_error)

    _assert_sanitized_capacity(path)


def test_query_growth_has_a_deterministic_statement_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths: dict[int, Path] = {}
    for order_count in (1, 10, 50):
        path = tmp_path / f"journal-{order_count}.sqlite3"
        orders = tuple(
            (_SECRET, f"order-{index}", f"command-{index}") for index in range(order_count)
        )
        create_v2(path, orders=orders)
        paths[order_count] = path

    traces: list[str] = []
    original = sqlite3.connect

    class TracedConnection(sqlite3.Connection):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.set_trace_callback(traces.append)

    def traced_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        kwargs["factory"] = TracedConnection
        return original(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", traced_connect)
    monkeypatch.setattr(inspection, "_SQLITE_CONNECT", traced_connect)
    counts = {}
    for order_count, path in paths.items():
        traces.clear()
        inspection.inspect_sqlite_live_order_journal(path, account_id=_SECRET)
        counts[order_count] = sum(
            statement.lstrip().upper().startswith(("SELECT", "WITH", "PRAGMA"))
            for statement in traces
        )

    # Frozen implementation budget: schema and row-budget validation costs at
    # most 90 fixed reads, while inspection adds no more than three reads per
    # order.  This guards query growth without depending on timing or opcodes.
    assert all(counts[size] <= 90 + 3 * size for size in counts)
    assert counts[50] - counts[10] <= 3 * (50 - 10)
    assert counts[10] - counts[1] <= 3 * (10 - 1)
