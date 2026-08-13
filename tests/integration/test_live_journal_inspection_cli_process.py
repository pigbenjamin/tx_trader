from __future__ import annotations

from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys

import pytest

from tests.support.live_journal_inspection_scenarios import (
    AttributionBlocker,
    AttributionState,
    create_attribution_scenario,
    create_frozen_v1,
    create_frozen_v2,
    create_semantically_blocked_v2,
    create_v2,
    create_v2_with_claim,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
ACCOUNT_CANARY = "account-cli-secret"
PATH_CANARY = "journal-cli-path-secret"
DURABLE_CANARIES = (
    "order-cli-secret",
    "command-cli-secret",
    "claim-cli-secret",
    "claimant-cli-secret",
    "selected-account",
    "foreign-account-secret",
    "foreign-order-secret",
    "foreign-command-secret",
)
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
OPAQUE_TARGET = re.compile(r"^[a-z_]+-[0-9a-f]{64}$")
REPORT_KEYS = {
    "commit_allowed",
    "database_schema_version",
    "disposition",
    "inspection_digest",
    "issue_codes",
    "journal_sequence",
    "may_dispatch",
    "output_schema_version",
    "requires_reconciliation",
    "schema_upgrade_required",
    "targets",
}
TARGET_KEYS = {"issue_code", "kind", "target_id"}
ARTIFACT_SUFFIXES = ("", "-wal", "-shm", "-journal")


def _artifact_snapshot(path: Path) -> dict[str, tuple[bool, bytes, int, int, str]]:
    snapshot: dict[str, tuple[bool, bytes, int, int, str]] = {}
    for suffix in ARTIFACT_SUFFIXES:
        artifact = Path(f"{path}{suffix}")
        if not artifact.exists():
            snapshot[suffix] = (False, b"", 0, 0, "")
            continue
        payload = artifact.read_bytes()
        stat = artifact.stat()
        snapshot[suffix] = (
            True,
            payload,
            stat.st_size,
            stat.st_mtime_ns,
            sha256(payload).hexdigest(),
        )
    return snapshot


def _file_digest(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _directory_artifacts(
    *directories: Path,
) -> dict[tuple[int, str], tuple[str, int, str]]:
    snapshot: dict[tuple[int, str], tuple[str, int, str]] = {}

    def visit(root_index: int, directory: Path, relative_parent: Path) -> None:
        with os.scandir(directory) as entries:
            for entry in entries:
                relative_path = relative_parent / entry.name
                metadata = entry.stat(follow_symlinks=False)
                attributes = getattr(metadata, "st_file_attributes", 0)
                reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
                if attributes & reparse_flag:
                    entry_type = "reparse_point"
                elif stat.S_ISLNK(metadata.st_mode):
                    entry_type = "symlink"
                elif stat.S_ISDIR(metadata.st_mode):
                    entry_type = "directory"
                elif stat.S_ISREG(metadata.st_mode):
                    entry_type = "regular_file"
                elif stat.S_ISFIFO(metadata.st_mode):
                    entry_type = "fifo"
                elif stat.S_ISSOCK(metadata.st_mode):
                    entry_type = "socket"
                elif stat.S_ISCHR(metadata.st_mode):
                    entry_type = "character_device"
                elif stat.S_ISBLK(metadata.st_mode):
                    entry_type = "block_device"
                else:
                    entry_type = "other"

                digest = _file_digest(Path(entry.path)) if entry_type == "regular_file" else ""
                snapshot[(root_index, relative_path.as_posix())] = (
                    entry_type,
                    metadata.st_size,
                    digest,
                )
                if entry_type == "directory":
                    visit(root_index, Path(entry.path), relative_path)

    for root_index, directory in enumerate(directories):
        visit(root_index, directory, Path())
    return snapshot


def _write_poison_sitecustomize(directory: Path) -> None:
    directory.mkdir()
    (directory / "sitecustomize.py").write_text(
        r"""import importlib.abc
import os
import sys

blocked = (
    "pythoncom", "pywintypes", "win32com", "comtypes", "dotenv", "keyring",
    "tx_trade.broker", "tx_trade.config", "tx_trade.credentials",
    "tx_trade.app.config", "tx_trade.app.phase2_config",
    "tx_trade.app.research_paper_config",
)

class BlockedImportFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == name or fullname.startswith(name + ".") for name in blocked):
            raise AssertionError("forbidden integration import")
        if fullname.lower().endswith((".dll", "_dll")):
            raise AssertionError("forbidden DLL import")
        return None

sys.meta_path.insert(0, BlockedImportFinder())

from tx_trade.orders.sqlite_live_order_journal import SqliteLiveOrderJournal

def forbidden_runtime_path(*args, **kwargs):
    raise AssertionError("writable journal runtime path attempted")

for method_name in (
    "claim_dispatch",
    "record_dispatch_receipt",
    "commit_authorized_reconciliation",
):
    setattr(SqliteLiveOrderJournal, method_name, forbidden_runtime_path)

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

class PoisonStdin:
    def read(self, *args, **kwargs):
        raise AssertionError("stdin read attempted")
    def readline(self, *args, **kwargs):
        raise AssertionError("stdin read attempted")
    def readlines(self, *args, **kwargs):
        raise AssertionError("stdin read attempted")
    def __iter__(self):
        raise AssertionError("stdin iteration attempted")
    def isatty(self):
        return False

os.getenv = lambda *args, **kwargs: (_ for _ in ()).throw(
    AssertionError("environment read attempted")
)
os.environ = PoisonEnvironment()
sys.stdin = PoisonStdin()
""",
        encoding="ascii",
    )


def _run_cli(
    cwd: Path,
    *arguments: str,
    poison_directory: Path | None = None,
) -> subprocess.CompletedProcess[bytes]:
    environment = os.environ.copy()
    python_paths = [str(REPOSITORY_ROOT)]
    if poison_directory is not None:
        python_paths.insert(0, str(poison_directory.resolve()))
    environment["PYTHONPATH"] = os.pathsep.join(python_paths)
    return subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "tx_trade.live_journal_inspection_cli",
            *arguments,
        ],
        cwd=cwd,
        env=environment,
        input=b"stdin-must-not-be-read-secret",
        capture_output=True,
        check=False,
        timeout=30,
    )


def _assert_canonical_json_line(stream: bytes) -> dict[str, object]:
    assert stream.endswith(b"\n")
    assert not stream.endswith(b"\n\n")
    assert stream.count(b"\n") == 1
    assert stream.isascii()
    document = json.loads(stream)
    assert stream == (
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
            "ascii"
        )
        + b"\n"
    )
    assert isinstance(document, dict)
    return document


def _assert_redacted(completed: subprocess.CompletedProcess[bytes], path: Path) -> None:
    rendered = completed.stdout + completed.stderr
    for canary in (ACCOUNT_CANARY, PATH_CANARY, path.name, *DURABLE_CANARIES):
        assert canary.encode("ascii") not in rendered
    assert str(path).encode("utf-8") not in rendered
    canonical_path = json.dumps(str(path), ensure_ascii=True)[1:-1].encode("ascii")
    assert canonical_path not in rendered


def _assert_report(
    completed: subprocess.CompletedProcess[bytes],
    *,
    exit_code: int,
    disposition: str,
    schema_version: int,
) -> dict[str, object]:
    assert completed.returncode == exit_code, completed.stderr.decode("utf-8", "replace")
    assert completed.stderr == b""
    document = _assert_canonical_json_line(completed.stdout)
    assert set(document) == REPORT_KEYS
    assert document["output_schema_version"] == 1
    assert document["database_schema_version"] == schema_version
    assert document["disposition"] == disposition
    assert document["may_dispatch"] is False
    assert document["commit_allowed"] is False
    assert DIGEST.fullmatch(str(document["inspection_digest"]))
    assert isinstance(document["journal_sequence"], int)
    assert document["journal_sequence"] >= 0
    assert isinstance(document["issue_codes"], list)
    assert document["issue_codes"] == sorted(set(document["issue_codes"]))
    assert isinstance(document["targets"], list)
    for target in document["targets"]:
        assert isinstance(target, dict)
        assert set(target) == TARGET_KEYS
        assert OPAQUE_TARGET.fullmatch(str(target["target_id"]))
        assert target["issue_code"] in document["issue_codes"]
    return document


def _create_ready(path: Path) -> str:
    selected_account, _ = create_attribution_scenario(
        path,
        blocker=AttributionBlocker.UNRESOLVED,
        state=AttributionState.FOREIGN,
    )
    return selected_account


@pytest.mark.parametrize(
    ("fixture", "exit_code", "disposition", "schema_version"),
    (
        ("ready", 0, "ready_no_action", 3),
        ("recovery", 10, "recovery_required", 3),
        ("upgrade", 11, "schema_upgrade_required", 1),
        ("upgrade_v2", 11, "schema_upgrade_required", 2),
        ("account_missing", 12, "account_not_found", 3),
        ("blocked", 13, "blocked_integrity_failure", 3),
    ),
)
def test_real_module_cli_status_exit_matrix_is_deterministic_and_read_only(
    tmp_path: Path,
    fixture: str,
    exit_code: int,
    disposition: str,
    schema_version: int,
) -> None:
    data_directory = tmp_path / "資料-data"
    data_directory.mkdir()
    external_cwd = tmp_path / "外部-external-cwd"
    external_cwd.mkdir()
    poison_directory = tmp_path / "poison-bootstrap"
    _write_poison_sitecustomize(poison_directory)
    path = (data_directory / f"{PATH_CANARY}-{fixture}.sqlite3").resolve()

    account_id = ACCOUNT_CANARY
    if fixture == "ready":
        account_id = _create_ready(path)
    elif fixture == "recovery":
        create_v2_with_claim(
            path,
            account_id=ACCOUNT_CANARY,
            order_id=DURABLE_CANARIES[0],
            command_id=DURABLE_CANARIES[1],
            claim_token=DURABLE_CANARIES[2],
            claimant_id=DURABLE_CANARIES[3],
        )
    elif fixture == "upgrade":
        create_frozen_v1(path)
    elif fixture == "upgrade_v2":
        create_frozen_v2(path)
    elif fixture == "account_missing":
        create_v2(path, orders=(("foreign-account-secret", "foreign-order", "foreign-command"),))
    else:
        create_semantically_blocked_v2(path)

    before = _artifact_snapshot(path)
    directory_artifacts_before = _directory_artifacts(data_directory, external_cwd)
    arguments = ("--journal", str(path), "--account-id", account_id)
    first = _run_cli(external_cwd, *arguments, poison_directory=poison_directory)
    middle = _artifact_snapshot(path)
    second = _run_cli(external_cwd, *arguments, poison_directory=poison_directory)

    first_document = _assert_report(
        first,
        exit_code=exit_code,
        disposition=disposition,
        schema_version=schema_version,
    )
    second_document = _assert_report(
        second,
        exit_code=exit_code,
        disposition=disposition,
        schema_version=schema_version,
    )
    assert first.stdout == second.stdout
    assert first_document == second_document
    assert _artifact_snapshot(path) == middle == before
    assert _directory_artifacts(data_directory, external_cwd) == directory_artifacts_before
    assert {suffix for suffix, value in before.items() if value[0]} == {""}
    _assert_redacted(first, path)
    _assert_redacted(second, path)


@pytest.mark.parametrize(
    ("failure", "failure_code"),
    (
        ("missing", "source_unavailable"),
        ("active_sidecar", "active_or_unclean_source"),
        ("corrupt", "integrity_failure"),
        ("capacity", "capacity_exceeded"),
    ),
)
def test_real_module_cli_typed_failures_are_canonical_sanitized_and_read_only(
    tmp_path: Path,
    failure: str,
    failure_code: str,
) -> None:
    data_directory = tmp_path / "資料-data"
    data_directory.mkdir()
    external_cwd = tmp_path / "外部-external-cwd"
    external_cwd.mkdir()
    poison_directory = tmp_path / "poison-bootstrap"
    _write_poison_sitecustomize(poison_directory)
    path = (data_directory / f"{PATH_CANARY}-{failure}.sqlite3").resolve()
    account_id = ACCOUNT_CANARY

    if failure == "active_sidecar":
        create_v2(path)
        Path(f"{path}-wal").write_bytes(b"active-sidecar-secret")
    elif failure == "corrupt":
        path.write_bytes(b"not-a-sqlite-database-secret")
    elif failure == "capacity":
        with path.open("wb") as stream:
            stream.truncate(64 * 1024 * 1024 + 1)

    before = _artifact_snapshot(path)
    directory_artifacts_before = _directory_artifacts(data_directory, external_cwd)
    completed = _run_cli(
        external_cwd,
        "--journal",
        str(path),
        "--account-id",
        account_id,
        poison_directory=poison_directory,
    )

    assert completed.returncode == 20
    assert completed.stdout == b""
    error = _assert_canonical_json_line(completed.stderr)
    assert error == {
        "failure_code": failure_code,
        "output_schema_version": 1,
        "status": "inspection_failure",
    }
    assert _artifact_snapshot(path) == before
    assert _directory_artifacts(data_directory, external_cwd) == directory_artifacts_before
    _assert_redacted(completed, path)


@pytest.mark.parametrize(
    "arguments",
    (
        ("--unknown-secret-option", "argv-secret-value"),
        ("--journal", "argv-secret-journal", "--account-id"),
        ("--account-id", "argv-secret-account"),
        (
            "--journal",
            "argv-secret-journal",
            "--account-id",
            "invalid account secret",
        ),
    ),
)
def test_hostile_or_incomplete_argv_is_usage_error_without_echoing_secrets(
    tmp_path: Path,
    arguments: tuple[str, ...],
) -> None:
    data_directory = tmp_path / "資料-data"
    data_directory.mkdir()
    external_cwd = tmp_path / "外部-external-cwd"
    external_cwd.mkdir()
    poison_directory = tmp_path / "poison-bootstrap"
    _write_poison_sitecustomize(poison_directory)
    directory_artifacts_before = _directory_artifacts(data_directory, external_cwd)

    completed = _run_cli(
        external_cwd,
        *arguments,
        poison_directory=poison_directory,
    )

    assert completed.returncode == 2
    assert completed.stdout == b""
    assert _assert_canonical_json_line(completed.stderr) == {
        "failure_code": "invalid_cli_request",
        "output_schema_version": 1,
        "status": "cli_failure",
    }
    assert b"secret" not in completed.stderr
    assert _directory_artifacts(data_directory, external_cwd) == directory_artifacts_before


def test_help_works_from_external_cwd_without_loading_live_integrations(
    tmp_path: Path,
) -> None:
    external_cwd = tmp_path / "外部-external-cwd"
    external_cwd.mkdir()
    poison_directory = tmp_path / "poison-bootstrap"
    _write_poison_sitecustomize(poison_directory)
    directory_artifacts_before = _directory_artifacts(external_cwd)

    completed = _run_cli(external_cwd, "--help", poison_directory=poison_directory)

    assert completed.returncode == 0, completed.stderr.decode("utf-8", "replace")
    assert completed.stderr == b""
    assert b"--journal" in completed.stdout
    assert b"--account-id" in completed.stdout
    assert b"forbidden integration import" not in completed.stdout
    assert _directory_artifacts(external_cwd) == directory_artifacts_before
