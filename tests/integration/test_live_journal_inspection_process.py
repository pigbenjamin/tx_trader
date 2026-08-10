from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile

import pytest

from tx_trade.orders.live_journal_contracts import JournalOpenMode
from tx_trade.orders.live_ports import RawBrokerObservation
from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

from tests.support.live_journal_inspection_scenarios import (
    NOW,
    create_frozen_v1,
    create_v2_with_claim,
)


ACCOUNT_CANARY = "account-process-canary"
CLAIM_CANARY = "claim-token-process-canary"
DURABLE_ID_CANARY = "durable-id-process-canary"
RAW_PAYLOAD_CANARY = "raw-payload-process-canary"
PATH_CANARY = "path-process-canary"
FROZEN_V1_IDENTITY_CANARY = "journal-v1"
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

_CHILD = r"""
import importlib.abc
import json
import os
import sys

if sys.argv[3] == "poison":
    blocked = (
        "pythoncom", "comtypes", "win32com", "dotenv", "keyring", "config",
        "tx_trade.broker",
    )

    class BlockedImportFinder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if any(fullname == name or fullname.startswith(name + ".") for name in blocked):
                raise AssertionError("forbidden integration import")
            return None

    class PoisonEnvironment:
        def __getitem__(self, key):
            raise AssertionError("environment read attempted")
        def __iter__(self):
            raise AssertionError("environment iteration attempted")
        def __len__(self):
            raise AssertionError("environment length attempted")
        def get(self, key, default=None):
            raise AssertionError("environment read attempted")
        def __contains__(self, key):
            raise AssertionError("environment membership attempted")

    sys.meta_path.insert(0, BlockedImportFinder())
    os.getenv = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("environment read attempted")
    )
    os.environ = PoisonEnvironment()

from tx_trade.orders.live_journal_inspection_contracts import LiveJournalInspectionError
from tx_trade.orders.sqlite_live_journal_inspection import inspect_sqlite_live_order_journal

if sys.argv[3] == "poison":
    from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

    def forbidden_runtime_path(*args, **kwargs):
        raise AssertionError("dispatch or commit path attempted")

    for method_name in (
        "claim_dispatch",
        "record_dispatch_receipt",
        "commit_reconciliation",
    ):
        setattr(SqliteLiveOrderJournal, method_name, forbidden_runtime_path)

try:
    report = inspect_sqlite_live_order_journal(sys.argv[1], account_id=sys.argv[2])
except LiveJournalInspectionError as error:
    output = {"error": error.code.value}
else:
    output = {
        "database_schema_version": report.database_schema_version,
        "journal_sequence": report.journal_sequence,
        "disposition": report.disposition.value,
        "issues": [item.value for item in report.issue_codes],
        "targets": [
            [item.kind.value, item.issue_code.value] for item in report.targets
        ],
        "inspection_digest": report.inspection_digest,
        "may_dispatch": report.may_dispatch,
        "commit_allowed": report.commit_allowed,
        "schema_upgrade_required": report.schema_upgrade_required,
    }
print(json.dumps(output, sort_keys=True, separators=(",", ":")))
"""


def _run_inspection(path: Path, *, poison: bool = False) -> tuple[dict[str, object], str]:
    child_environment = os.environ.copy()
    child_environment["PYTHONPATH"] = str(REPOSITORY_ROOT)
    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            _CHILD,
            str(path),
            ACCOUNT_CANARY,
            "poison" if poison else "plain",
        ],
        cwd=REPOSITORY_ROOT,
        env=child_environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout), completed.stdout + completed.stderr


def _snapshot(directory: Path) -> dict[str, tuple[bytes, int, int, str]]:
    return {
        path.name: (
            path.read_bytes(),
            path.stat().st_size,
            path.stat().st_mtime_ns,
            sha256(path.read_bytes()).hexdigest(),
        )
        for path in sorted(directory.iterdir())
        if path.is_file()
    }


def _assert_no_canaries(output: str, path: Path) -> None:
    for canary in (
        ACCOUNT_CANARY,
        CLAIM_CANARY,
        DURABLE_ID_CANARY,
        RAW_PAYLOAD_CANARY,
        PATH_CANARY,
        str(path),
    ):
        assert canary not in output


def _create_v2(path: Path) -> None:
    create_v2_with_claim(
        path,
        account_id=ACCOUNT_CANARY,
        order_id=DURABLE_ID_CANARY,
        command_id=f"command-{DURABLE_ID_CANARY}",
        claim_token=CLAIM_CANARY,
        claimant_id="claimant-process-canary",
    )
    journal = SqliteLiveOrderJournal(
        path,
        JournalOpenMode.RESUME,
        clock=lambda: datetime(2026, 8, 9, tzinfo=timezone.utc),
        claim_token_factory=lambda: "unused-process-claim",
    )
    try:
        journal.append_raw_observation(
            RawBrokerObservation(
                f"observation-{DURABLE_ID_CANARY}",
                "process-fake-reply",
                1,
                1,
                NOW,
                RAW_PAYLOAD_CANARY.encode(),
            )
        )
    finally:
        journal.close()


def _create_v1(path: Path) -> None:
    create_frozen_v1(path)


def test_fresh_process_inspection_is_independent_of_parent_cwd(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with tempfile.TemporaryDirectory() as temporary_directory:
        directory = Path(temporary_directory).resolve()
        assert directory != REPOSITORY_ROOT
        assert REPOSITORY_ROOT not in directory.parents
        v1_path = directory / "fresh-process-v1.sqlite3"
        v2_path = directory / "fresh-process-v2.sqlite3"
        _create_v1(v1_path)
        _create_v2(v2_path)
        before = _snapshot(directory)
        with monkeypatch.context() as cwd_patch:
            cwd_patch.chdir(directory)
            v1_result, v1_output = _run_inspection(v1_path, poison=True)
            v2_result, v2_output = _run_inspection(v2_path, poison=True)

        assert v1_result["database_schema_version"] == 1
        assert v1_result["disposition"] == "schema_upgrade_required"
        assert v2_result["database_schema_version"] == 2
        assert v2_result["disposition"] == "recovery_required"
        assert _snapshot(directory) == before
        assert set(before) == {v1_path.name, v2_path.name}
        _assert_no_canaries(v1_output, v1_path)
        assert FROZEN_V1_IDENTITY_CANARY not in v1_output
        _assert_no_canaries(v2_output, v2_path)


def test_fresh_process_clean_v2_is_deterministic_and_read_only(tmp_path: Path) -> None:
    path = tmp_path / f"{PATH_CANARY}.sqlite3"
    _create_v2(path)
    before = _snapshot(tmp_path)

    first, first_output = _run_inspection(path, poison=True)
    middle = _snapshot(tmp_path)
    second, second_output = _run_inspection(path, poison=True)

    assert first == second
    assert first["database_schema_version"] == 2
    assert first["disposition"] == "recovery_required"
    assert first["may_dispatch"] is False
    assert first["commit_allowed"] is False
    assert _snapshot(tmp_path) == middle == before
    assert set(before) == {path.name}
    _assert_no_canaries(first_output + second_output, path)


def test_fresh_process_v1_requires_upgrade_without_migrating(tmp_path: Path) -> None:
    path = tmp_path / f"{PATH_CANARY}-v1.sqlite3"
    _create_v1(path)
    with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True) as connection:
        rows_before = tuple(connection.execute("SELECT * FROM live_journal_migrations"))
        version_before = int(connection.execute("PRAGMA user_version").fetchone()[0])
    before = _snapshot(tmp_path)

    result, output = _run_inspection(path)

    assert result["database_schema_version"] == 1
    assert result["disposition"] == "schema_upgrade_required"
    assert result["issues"] == ["schema_upgrade_required"]
    assert result["schema_upgrade_required"] is True
    assert _snapshot(tmp_path) == before
    with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True) as connection:
        assert int(connection.execute("PRAGMA user_version").fetchone()[0]) == version_before
        assert tuple(connection.execute("SELECT * FROM live_journal_migrations")) == rows_before
        assert not connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'live_reconciliation_commits'"
        ).fetchone()
    _assert_no_canaries(output, path)
    assert FROZEN_V1_IDENTITY_CANARY not in output


def test_quiescent_complete_wal_pair_fails_closed_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / f"{PATH_CANARY}-wal.sqlite3"
    _create_v2(path)
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("UPDATE live_journal_identity SET journal_id = 'wal-transient-id'")
        connection.execute("COMMIT")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("UPDATE live_journal_identity SET journal_id = ?", (DURABLE_ID_CANARY,))
        connection.execute("COMMIT")
        wal = path.with_name(path.name + "-wal")
        shm = path.with_name(path.name + "-shm")
        assert wal.stat().st_size > 32
        assert shm.stat().st_size > 0
        before = _snapshot(tmp_path)

        result, output = _run_inspection(path)

        assert result == {"error": "active_or_unclean_source"}
        assert _snapshot(tmp_path) == before
        _assert_no_canaries(output, path)
    finally:
        connection.close()


@pytest.mark.parametrize("sidecar_suffix", ["-wal", "-shm"])
def test_partial_sidecar_fails_before_mutation(tmp_path: Path, sidecar_suffix: str) -> None:
    path = tmp_path / f"{PATH_CANARY}-partial.sqlite3"
    _create_v2(path)
    sidecar = path.with_name(path.name + sidecar_suffix)
    sidecar.write_bytes(b"deliberately-incomplete-sidecar")
    before = _snapshot(tmp_path)

    result, output = _run_inspection(path)

    assert result == {"error": "active_or_unclean_source"}
    assert _snapshot(tmp_path) == before
    _assert_no_canaries(output, path)
