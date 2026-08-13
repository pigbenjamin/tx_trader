from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
import sqlite3

import pytest

import tx_trade.orders.sqlite_live_journal_inspection as inspection_module
from tests.support.live_journal_inspection_scenarios import (
    create_frozen_v1,
    create_frozen_v2,
    create_v2 as create_current,
)
from tx_trade.orders.live_journal_contracts import LiveJournalIntegrityError
from tx_trade.orders.live_journal_inspection_contracts import (
    LiveJournalInspectionError,
    LiveJournalInspectionFailureCode,
)
from tx_trade.orders.sqlite_live_journal_inspection import _install_authorizer
from tx_trade.orders.sqlite_live_order_journal import (
    DATABASE_SCHEMA_VERSION,
    _ConnectionBoundLiveJournalReader,
    _ReadOnlyLiveJournalPayload,
)


@pytest.mark.parametrize("function_name", ("load_extension", "LOAD_EXTENSION", "LoAd_ExTeNsIoN"))
def test_inspection_authorizer_denies_load_extension_case_insensitively(
    function_name: str,
) -> None:
    captured: list[object] = []

    class CapturingConnection(sqlite3.Connection):
        def set_authorizer(self, authorizer):  # type: ignore[no-untyped-def]
            captured.append(authorizer)

    connection = sqlite3.connect(":memory:", factory=CapturingConnection)
    try:
        _install_authorizer(connection)
        authorizer = captured[0]
        assert callable(authorizer)
        assert (
            authorizer(sqlite3.SQLITE_FUNCTION, None, function_name, None, None)
            == sqlite3.SQLITE_DENY
        )
        assert authorizer(sqlite3.SQLITE_FUNCTION, None, None, None, None) == sqlite3.SQLITE_OK
        assert authorizer(sqlite3.SQLITE_FUNCTION, None, "lower", None, None) == sqlite3.SQLITE_OK
    finally:
        connection.close()


def test_isolated_begin_progress_interrupt_is_capacity_exceeded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "journal.sqlite3"
    create_current(path)
    monkeypatch.setattr(inspection_module, "INSPECTION_PROGRESS_OPCODE_INTERVAL", 1)
    monkeypatch.setattr(inspection_module, "MAX_INSPECTION_PROGRESS_CALLBACKS", 0)

    with pytest.raises(LiveJournalInspectionError) as raised:
        inspection_module._open_isolated_inspection_connection(path.read_bytes())

    assert raised.value.code is LiveJournalInspectionFailureCode.CAPACITY_EXCEEDED


def _connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"{path.resolve().as_uri()}?mode=ro&immutable=1&cache=private",
        uri=True,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    return connection


def test_reader_loads_v3_without_owning_transaction_or_connection(tmp_path: Path) -> None:
    path = tmp_path / "journal.sqlite3"
    create_current(path, orders=(("account-a", "order-a", "command-a"),))
    connection = _connection(path)
    try:
        reader = _ConnectionBoundLiveJournalReader(connection)

        payload = reader.load("account-a", DATABASE_SCHEMA_VERSION)

        assert type(payload) is _ReadOnlyLiveJournalPayload
        assert payload.snapshot is not None
        assert payload.snapshot.identity == payload.identity
        assert payload.snapshot.journal_sequence == payload.journal_sequence
        assert connection.in_transaction is False
        assert connection.execute("SELECT 1").fetchone()[0] == 1
        with pytest.raises((FrozenInstanceError, AttributeError)):
            payload.journal_sequence = 0  # type: ignore[misc]
        assert not hasattr(payload, "__dict__")
    finally:
        connection.close()


@pytest.mark.parametrize(
    ("version", "create_legacy"),
    ((1, create_frozen_v1), (2, create_frozen_v2)),
)
def test_reader_loads_legacy_schema_for_upgrade_only(
    tmp_path: Path,
    version: int,
    create_legacy,
) -> None:
    path = tmp_path / f"journal-v{version}.sqlite3"
    create_legacy(path)
    connection = _connection(path)
    try:
        payload = _ConnectionBoundLiveJournalReader(connection).load("account-a", version)

        assert payload.identity.schema_version == version
        assert payload.snapshot is None
        assert payload.issue_codes == ()
        assert connection.in_transaction is False
    finally:
        connection.close()


def test_reader_integrity_failure_does_not_rollback_or_close_caller(
    tmp_path: Path,
) -> None:
    path = tmp_path / "journal.sqlite3"
    create_current(path, orders=(("account-a", "order-a", "command-a"),))
    writable = sqlite3.connect(path)
    try:
        writable.execute(
            "UPDATE live_journal_records SET payload_digest = ? WHERE journal_sequence = 1",
            ("sha256:" + "0" * 64,),
        )
        writable.commit()
    finally:
        writable.close()

    connection = _connection(path)
    try:
        connection.execute("BEGIN")
        reader = _ConnectionBoundLiveJournalReader(connection)

        with pytest.raises(LiveJournalIntegrityError):
            reader.load("account-a", DATABASE_SCHEMA_VERSION)

        assert connection.in_transaction is True
        assert connection.execute("SELECT 1").fetchone()[0] == 1
        connection.execute("ROLLBACK")
    finally:
        connection.close()
