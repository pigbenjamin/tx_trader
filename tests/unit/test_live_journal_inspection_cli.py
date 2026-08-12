from __future__ import annotations

import ast
from io import StringIO
import json
from pathlib import Path

import pytest

import tx_trade.live_journal_inspection_cli as cli
from tx_trade.orders.live_journal_inspection_contracts import (
    LiveJournalInspectionDisposition,
    LiveJournalInspectionError,
    LiveJournalInspectionFailureCode,
    LiveJournalInspectionIssueCode,
    LiveJournalInspectionReport,
    LiveJournalInspectionTargetKind,
    RedactedLiveJournalInspectionTarget,
)


ACCOUNT_CANARY = "account-canary"
PATH_CANARY = "path-canary.sqlite3"
DURABLE_CANARY = "raw-durable-order-canary"
DIGEST = "sha256:" + "a" * 64
TARGET_ID = "pending_command:sha256:" + "b" * 64


def _report(
    disposition: LiveJournalInspectionDisposition,
) -> LiveJournalInspectionReport:
    if disposition is LiveJournalInspectionDisposition.READY_NO_ACTION:
        version = 2
        issues: tuple[LiveJournalInspectionIssueCode, ...] = ()
        targets: tuple[RedactedLiveJournalInspectionTarget, ...] = ()
    elif disposition is LiveJournalInspectionDisposition.RECOVERY_REQUIRED:
        version = 2
        issues = (LiveJournalInspectionIssueCode.OUTSTANDING_DISPATCH,)
        targets = (
            RedactedLiveJournalInspectionTarget(
                kind=LiveJournalInspectionTargetKind.PENDING_COMMAND,
                target_id=TARGET_ID,
                issue_code=LiveJournalInspectionIssueCode.OUTSTANDING_DISPATCH,
            ),
        )
    elif disposition is LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED:
        version = 1
        issues = (LiveJournalInspectionIssueCode.SCHEMA_UPGRADE_REQUIRED,)
        targets = ()
    elif disposition is LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND:
        version = 2
        issues = (LiveJournalInspectionIssueCode.ACCOUNT_NOT_FOUND,)
        targets = ()
    else:
        version = 2
        issues = (LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,)
        targets = ()
    return LiveJournalInspectionReport(
        account_id=ACCOUNT_CANARY,
        database_schema_version=version,
        journal_sequence=7,
        disposition=disposition,
        issue_codes=issues,
        targets=targets,
        inspection_digest=DIGEST,
    )


def _invoke(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    outcome: LiveJournalInspectionReport | BaseException,
) -> tuple[int, str, str]:
    def inspect(_path: object, *, account_id: str) -> LiveJournalInspectionReport:
        assert account_id == "account-id"
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(cli, "inspect_sqlite_live_order_journal", inspect)
    exit_code = cli.main(["--journal", "journal.sqlite3", "--account-id", "account-id"])
    captured = capsys.readouterr()
    return exit_code, captured.out, captured.err


def test_public_contract_constants_are_frozen() -> None:
    assert cli.OUTPUT_SCHEMA_VERSION == 1
    assert cli.MAX_CLI_OUTPUT_BYTES == 256 * 1024
    assert {member.name: member.value for member in cli.LiveJournalInspectionCliExitCode} == {
        "READY_NO_ACTION": 0,
        "USAGE_ERROR": 2,
        "RECOVERY_REQUIRED": 10,
        "SCHEMA_UPGRADE_REQUIRED": 11,
        "ACCOUNT_NOT_FOUND": 12,
        "BLOCKED_INTEGRITY_FAILURE": 13,
        "INSPECTION_FAILURE": 20,
    }


def test_serializer_has_exact_golden_schema_and_canonical_encoding() -> None:
    report = _report(LiveJournalInspectionDisposition.RECOVERY_REQUIRED)
    expected = (
        '{"commit_allowed":false,"database_schema_version":2,'
        '"disposition":"recovery_required","inspection_digest":"sha256:'
        + "a"
        * 64
        + '","issue_codes":["outstanding_dispatch"],"journal_sequence":7,'
        '"may_dispatch":false,"output_schema_version":1,"requires_reconciliation":true,'
        '"schema_upgrade_required":false,"targets":[{"issue_code":"outstanding_dispatch",'
        '"kind":"pending_command","target_id":"pending_command:sha256:' + "b" * 64 + '"}]}\n'
    )

    first = cli.serialize_live_journal_inspection_report(report)
    second = cli.serialize_live_journal_inspection_report(report)

    assert first == expected == second
    assert first.encode("ascii").decode("ascii") == first
    assert first.endswith("\n") and not first.endswith("\n\n")
    assert " " not in first
    assert list(json.loads(first)) == sorted(json.loads(first))
    for canary in (ACCOUNT_CANARY, PATH_CANARY, DURABLE_CANARY):
        assert canary not in first


@pytest.mark.parametrize("disposition", tuple(LiveJournalInspectionDisposition))
def test_serializer_supports_every_disposition(
    disposition: LiveJournalInspectionDisposition,
) -> None:
    rendered = cli.serialize_live_journal_inspection_report(_report(disposition))
    payload = json.loads(rendered)
    assert payload["disposition"] == disposition.value
    assert set(payload) == {
        "output_schema_version",
        "database_schema_version",
        "journal_sequence",
        "disposition",
        "issue_codes",
        "targets",
        "inspection_digest",
        "may_dispatch",
        "commit_allowed",
        "requires_reconciliation",
        "schema_upgrade_required",
    }


@pytest.mark.parametrize(
    ("disposition", "expected_exit"),
    (
        (LiveJournalInspectionDisposition.READY_NO_ACTION, 0),
        (LiveJournalInspectionDisposition.RECOVERY_REQUIRED, 10),
        (LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED, 11),
        (LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND, 12),
        (LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE, 13),
    ),
)
def test_main_maps_dispositions_to_exit_codes_and_one_stdout_line(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    disposition: LiveJournalInspectionDisposition,
    expected_exit: int,
) -> None:
    exit_code, stdout, stderr = _invoke(monkeypatch, capsys, _report(disposition))
    assert exit_code == expected_exit
    assert stdout == cli.serialize_live_journal_inspection_report(_report(disposition))
    assert stdout.count("\n") == 1
    assert stderr == ""


@pytest.mark.parametrize("failure_code", tuple(LiveJournalInspectionFailureCode))
def test_typed_inspection_failures_are_public_canonical_and_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure_code: LiveJournalInspectionFailureCode,
) -> None:
    exit_code, stdout, stderr = _invoke(
        monkeypatch,
        capsys,
        LiveJournalInspectionError(failure_code),
    )
    assert exit_code == 20
    assert stdout == ""
    assert stderr == (
        '{"failure_code":"'
        + failure_code.value
        + '","output_schema_version":1,"status":"inspection_failure"}\n'
    )
    assert PATH_CANARY not in stderr


@pytest.mark.parametrize(
    "exception_type",
    (MemoryError, ValueError, OSError, KeyboardInterrupt),
)
def test_unexpected_failures_are_fixed_and_do_not_leak_exception_text(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    exception_type: type[BaseException],
) -> None:
    exception = exception_type("unexpected-exception-canary")
    exit_code, stdout, stderr = _invoke(monkeypatch, capsys, exception)
    assert exit_code == 20
    assert stdout == ""
    assert stderr == (
        '{"failure_code":"internal_failure","output_schema_version":1,'
        '"status":"internal_failure"}\n'
    )
    assert "unexpected-exception-canary" not in stderr
    assert "Traceback" not in stderr


def test_inspector_system_exit_is_fixed_and_does_not_leak_sensitive_text(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code, stdout, stderr = _invoke(
        monkeypatch,
        capsys,
        SystemExit("sensitive-inspector-system-exit-canary"),
    )

    assert exit_code == 20
    assert stdout == ""
    assert stderr == (
        '{"failure_code":"internal_failure","output_schema_version":1,'
        '"status":"internal_failure"}\n'
    )
    assert "sensitive-inspector-system-exit-canary" not in stderr
    assert "Traceback" not in stderr


def test_serializer_system_exit_fails_before_any_stdout_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report(LiveJournalInspectionDisposition.READY_NO_ACTION)
    stdout = _RecordingStream()
    stderr = StringIO()
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    monkeypatch.setattr(
        cli,
        "inspect_sqlite_live_order_journal",
        lambda _path, *, account_id: report,
    )
    monkeypatch.setattr(
        cli,
        "serialize_live_journal_inspection_report",
        lambda _report: (_ for _ in ()).throw(
            SystemExit("sensitive-serializer-system-exit-canary")
        ),
    )

    exit_code = cli.main(["--journal", "journal.sqlite3", "--account-id", "account-id"])

    assert exit_code == 20
    assert stdout.write_calls == []
    assert stderr.getvalue() == (
        '{"failure_code":"internal_failure","output_schema_version":1,'
        '"status":"internal_failure"}\n'
    )
    assert "sensitive-serializer-system-exit-canary" not in stderr.getvalue()
    assert "Traceback" not in stderr.getvalue()


@pytest.mark.parametrize(
    "argv",
    (
        [],
        ["--journal", "attacker-missing-account"],
        ["--account-id", "attacker-missing-journal"],
        ["--journal", "a", "--journal", "attacker-duplicate", "--account-id", "id"],
        ["--journal", "a", "--account-id", "id", "--account-id", "attacker-duplicate"],
        ["--journal", "a", "--account-id", "id", "--attacker-unknown"],
        ["--journal", "a", "--account-id", "id", "attacker-extra"],
        ["--journal", "", "--account-id", "id"],
        ["--journal", "a" * 4097, "--account-id", "id"],
        ["--journal", "a", "--account-id", ""],
        ["--journal", "a", "--account-id", "a" * 129],
        ["--journal", "a", "--account-id", "contains space"],
        ["--journal", "a", "--account-id", "non-ascii-攻擊"],
    ),
)
def test_hostile_parser_inputs_return_sanitized_usage_error(
    capsys: pytest.CaptureFixture[str], argv: list[str]
) -> None:
    exit_code = cli.main(argv)
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err == (
        '{"failure_code":"invalid_cli_request","output_schema_version":1,"status":"cli_failure"}\n'
    )
    for value in argv:
        if value.startswith("attacker") or len(value) > 128 or "攻擊" in value:
            assert value not in combined
    assert "usage:" not in combined.lower()
    assert "Traceback" not in combined


@pytest.mark.parametrize(
    "stderr_options",
    (
        {"short_write": True},
        {"fail_write": True},
        {"fail_flush": True},
        {"interrupt_write": True},
        {"interrupt_flush": True},
        {"system_exit_write": True},
        {"system_exit_flush": True},
    ),
    ids=(
        "short-write",
        "oserror-write",
        "oserror-flush",
        "keyboard-interrupt-write",
        "keyboard-interrupt-flush",
        "system-exit-write",
        "system-exit-flush",
    ),
)
def test_invalid_request_with_stderr_failure_returns_20_not_usage_error(
    monkeypatch: pytest.MonkeyPatch,
    stderr_options: dict[str, bool],
) -> None:
    stdout = _RecordingStream()
    stderr = _RecordingStream(**stderr_options)
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    monkeypatch.setattr(cli.sys, "stderr", stderr)

    exit_code = cli.main(["--attacker-invalid-system-exit-canary"])

    assert exit_code == 20
    assert exit_code != 2
    assert stdout.write_calls == []
    assert len(stderr.write_calls) == 1
    assert all("canary" not in value for value in stderr.write_calls)
    assert all("Traceback" not in value for value in stderr.write_calls)


def test_help_is_safe_and_contains_no_environment_specific_values(
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = cli.main(["--help"])
    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.err == ""
    assert "--journal" in captured.out
    assert "--account-id" in captured.out
    for canary in (ACCOUNT_CANARY, PATH_CANARY, DURABLE_CANARY):
        assert canary not in captured.out


def test_output_cap_counts_the_complete_ascii_line_including_lf(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report(LiveJournalInspectionDisposition.READY_NO_ACTION)
    baseline = cli.serialize_live_journal_inspection_report(report)
    encoded_size = len(baseline.encode("ascii"))
    assert baseline.endswith("\n")

    monkeypatch.setattr(cli, "MAX_CLI_OUTPUT_BYTES", encoded_size)
    assert cli.serialize_live_journal_inspection_report(report) == baseline

    monkeypatch.setattr(cli, "MAX_CLI_OUTPUT_BYTES", encoded_size - 1)
    with pytest.raises(ValueError, match="public bound"):
        cli.serialize_live_journal_inspection_report(report)


def test_oversized_report_is_rejected_before_any_stdout_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report(LiveJournalInspectionDisposition.READY_NO_ACTION)
    encoded_size = len(cli.serialize_live_journal_inspection_report(report).encode("ascii"))
    stdout = _RecordingStream()
    stderr = StringIO()
    monkeypatch.setattr(cli, "MAX_CLI_OUTPUT_BYTES", encoded_size - 1)
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    monkeypatch.setattr(
        cli,
        "inspect_sqlite_live_order_journal",
        lambda _path, *, account_id: report,
    )

    exit_code = cli.main(["--journal", "journal.sqlite3", "--account-id", "account-id"])

    assert exit_code == 20
    assert stdout.write_calls == []
    assert json.loads(stderr.getvalue()) == {
        "failure_code": "internal_failure",
        "output_schema_version": 1,
        "status": "internal_failure",
    }


class _RecordingStream:
    def __init__(
        self,
        *,
        short_write: bool = False,
        fail_write: bool = False,
        fail_flush: bool = False,
        interrupt_write: bool = False,
        interrupt_flush: bool = False,
        system_exit_write: bool = False,
        system_exit_flush: bool = False,
    ) -> None:
        self.short_write = short_write
        self.fail_write = fail_write
        self.fail_flush = fail_flush
        self.interrupt_write = interrupt_write
        self.interrupt_flush = interrupt_flush
        self.system_exit_write = system_exit_write
        self.system_exit_flush = system_exit_flush
        self.write_calls: list[str] = []
        self.flush_calls = 0

    def write(self, value: str) -> int:
        self.write_calls.append(value)
        if self.interrupt_write:
            raise KeyboardInterrupt("text-write-keyboard-interrupt-canary")
        if self.system_exit_write:
            raise SystemExit("text-write-system-exit-canary")
        if self.fail_write:
            raise OSError("stream-write-canary")
        return len(value) - 1 if self.short_write else len(value)

    def flush(self) -> None:
        self.flush_calls += 1
        if self.interrupt_flush:
            raise KeyboardInterrupt("text-flush-keyboard-interrupt-canary")
        if self.system_exit_flush:
            raise SystemExit("text-flush-system-exit-canary")
        if self.fail_flush:
            raise OSError("stream-flush-canary")


class _RecordingBinaryBuffer:
    def __init__(
        self,
        *,
        short_write: bool = False,
        interrupt_write: bool = False,
        interrupt_flush: bool = False,
        system_exit_write: bool = False,
        system_exit_flush: bool = False,
    ) -> None:
        self.short_write = short_write
        self.interrupt_write = interrupt_write
        self.interrupt_flush = interrupt_flush
        self.system_exit_write = system_exit_write
        self.system_exit_flush = system_exit_flush
        self.write_calls: list[bytes] = []
        self.flush_calls = 0

    def write(self, value: bytes) -> int:
        self.write_calls.append(value)
        if self.system_exit_write:
            raise SystemExit("binary-write-system-exit-canary")
        if self.interrupt_write:
            raise KeyboardInterrupt("binary-write-canary")
        return len(value) - 1 if self.short_write else len(value)

    def flush(self) -> None:
        self.flush_calls += 1
        if self.system_exit_flush:
            raise SystemExit("binary-flush-system-exit-canary")
        if self.interrupt_flush:
            raise KeyboardInterrupt("binary-flush-canary")


class _BinaryBackedStream:
    def __init__(self, buffer: _RecordingBinaryBuffer) -> None:
        self.buffer = buffer

    def write(self, _value: str) -> int:
        pytest.fail("binary-backed stream used its text writer")

    def flush(self) -> None:
        pytest.fail("binary-backed stream used its text flusher")


def test_binary_stdout_is_one_ascii_write_with_exact_lf_and_flush(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report(LiveJournalInspectionDisposition.READY_NO_ACTION)
    buffer = _RecordingBinaryBuffer()
    stdout = _BinaryBackedStream(buffer)
    stderr = StringIO()
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    monkeypatch.setattr(
        cli,
        "inspect_sqlite_live_order_journal",
        lambda _path, *, account_id: report,
    )

    exit_code = cli.main(["--journal", "journal.sqlite3", "--account-id", "account-id"])

    assert exit_code == 0
    assert buffer.write_calls == [
        cli.serialize_live_journal_inspection_report(report).encode("ascii")
    ]
    assert buffer.flush_calls == 1
    assert buffer.write_calls[0].endswith(b"\n")
    assert b"\r" not in buffer.write_calls[0]
    assert stderr.getvalue() == ""


@pytest.mark.parametrize(
    "buffer_options",
    (
        {"short_write": True},
        {"interrupt_write": True},
        {"interrupt_flush": True},
    ),
    ids=("short-write", "write-keyboard-interrupt", "flush-keyboard-interrupt"),
)
def test_binary_stdout_faults_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    buffer_options: dict[str, bool],
) -> None:
    report = _report(LiveJournalInspectionDisposition.READY_NO_ACTION)
    buffer = _RecordingBinaryBuffer(**buffer_options)
    stderr = StringIO()
    monkeypatch.setattr(cli.sys, "stdout", _BinaryBackedStream(buffer))
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    monkeypatch.setattr(
        cli,
        "inspect_sqlite_live_order_journal",
        lambda _path, *, account_id: report,
    )

    exit_code = cli.main(["--journal", "journal.sqlite3", "--account-id", "account-id"])

    assert exit_code == 20
    assert len(buffer.write_calls) == 1
    assert json.loads(stderr.getvalue()) == {
        "failure_code": "internal_failure",
        "output_schema_version": 1,
        "status": "internal_failure",
    }
    assert "Traceback" not in stderr.getvalue()
    assert "canary" not in stderr.getvalue()


@pytest.mark.parametrize(
    "buffer_options",
    (
        {"system_exit_write": True},
        {"system_exit_flush": True},
    ),
    ids=("write-system-exit", "flush-system-exit"),
)
def test_binary_stdout_system_exit_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    buffer_options: dict[str, bool],
) -> None:
    report = _report(LiveJournalInspectionDisposition.READY_NO_ACTION)
    buffer = _RecordingBinaryBuffer(**buffer_options)
    stderr = StringIO()
    monkeypatch.setattr(cli.sys, "stdout", _BinaryBackedStream(buffer))
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    monkeypatch.setattr(
        cli,
        "inspect_sqlite_live_order_journal",
        lambda _path, *, account_id: report,
    )

    exit_code = cli.main(["--journal", "journal.sqlite3", "--account-id", "account-id"])

    assert exit_code == 20
    assert len(buffer.write_calls) == 1
    assert stderr.getvalue() == (
        '{"failure_code":"internal_failure","output_schema_version":1,'
        '"status":"internal_failure"}\n'
    )
    assert "system-exit-canary" not in stderr.getvalue()
    assert "Traceback" not in stderr.getvalue()


@pytest.mark.parametrize(
    "buffer_options",
    (
        {"short_write": True},
        {"interrupt_write": True},
        {"interrupt_flush": True},
    ),
    ids=("short-write", "write-keyboard-interrupt", "flush-keyboard-interrupt"),
)
def test_binary_stderr_faults_are_swallowed(
    monkeypatch: pytest.MonkeyPatch,
    buffer_options: dict[str, bool],
) -> None:
    buffer = _RecordingBinaryBuffer(**buffer_options)
    stdout = StringIO()
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    monkeypatch.setattr(cli.sys, "stderr", _BinaryBackedStream(buffer))
    monkeypatch.setattr(
        cli,
        "inspect_sqlite_live_order_journal",
        lambda _path, *, account_id: (_ for _ in ()).throw(
            LiveJournalInspectionError(LiveJournalInspectionFailureCode.SOURCE_UNAVAILABLE)
        ),
    )

    exit_code = cli.main(["--journal", "journal.sqlite3", "--account-id", "account-id"])

    assert exit_code == 20
    assert stdout.getvalue() == ""
    assert buffer.write_calls == [
        b'{"failure_code":"source_unavailable","output_schema_version":1,'
        b'"status":"inspection_failure"}\n'
    ]
    assert b"\r" not in buffer.write_calls[0]
    assert b"Traceback" not in buffer.write_calls[0]
    assert b"canary" not in buffer.write_calls[0]


@pytest.mark.parametrize(
    "stdout",
    (
        _RecordingStream(short_write=True),
        _RecordingStream(fail_write=True),
        _RecordingStream(fail_flush=True),
    ),
    ids=("short-write", "write-error", "flush-error"),
)
def test_stdout_failure_returns_internal_failure_without_traceback(
    monkeypatch: pytest.MonkeyPatch,
    stdout: _RecordingStream,
) -> None:
    report = _report(LiveJournalInspectionDisposition.READY_NO_ACTION)
    stderr = StringIO()
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    monkeypatch.setattr(
        cli,
        "inspect_sqlite_live_order_journal",
        lambda _path, *, account_id: report,
    )

    exit_code = cli.main(["--journal", "journal.sqlite3", "--account-id", "account-id"])

    assert exit_code == 20
    assert json.loads(stderr.getvalue()) == {
        "failure_code": "internal_failure",
        "output_schema_version": 1,
        "status": "internal_failure",
    }
    assert "Traceback" not in stderr.getvalue()
    assert "canary" not in stderr.getvalue()


@pytest.mark.parametrize(
    "stdout",
    (
        _RecordingStream(system_exit_write=True),
        _RecordingStream(system_exit_flush=True),
    ),
    ids=("write-system-exit", "flush-system-exit"),
)
def test_text_stdout_system_exit_returns_sanitized_internal_failure(
    monkeypatch: pytest.MonkeyPatch,
    stdout: _RecordingStream,
) -> None:
    report = _report(LiveJournalInspectionDisposition.READY_NO_ACTION)
    stderr = StringIO()
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    monkeypatch.setattr(
        cli,
        "inspect_sqlite_live_order_journal",
        lambda _path, *, account_id: report,
    )

    exit_code = cli.main(["--journal", "journal.sqlite3", "--account-id", "account-id"])

    assert exit_code == 20
    assert len(stdout.write_calls) == 1
    assert stderr.getvalue() == (
        '{"failure_code":"internal_failure","output_schema_version":1,'
        '"status":"internal_failure"}\n'
    )
    assert "system-exit-canary" not in stderr.getvalue()
    assert "Traceback" not in stderr.getvalue()


@pytest.mark.parametrize(
    "stderr",
    (
        _RecordingStream(short_write=True),
        _RecordingStream(fail_write=True),
        _RecordingStream(fail_flush=True),
    ),
    ids=("short-write", "write-error", "flush-error"),
)
def test_stderr_failure_is_swallowed_without_stdout_or_traceback(
    monkeypatch: pytest.MonkeyPatch,
    stderr: _RecordingStream,
) -> None:
    stdout = _RecordingStream()
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    monkeypatch.setattr(
        cli,
        "inspect_sqlite_live_order_journal",
        lambda _path, *, account_id: (_ for _ in ()).throw(
            LiveJournalInspectionError(LiveJournalInspectionFailureCode.SOURCE_UNAVAILABLE)
        ),
    )

    exit_code = cli.main(["--journal", "journal.sqlite3", "--account-id", "account-id"])

    assert exit_code == 20
    assert stdout.write_calls == []
    assert all("Traceback" not in value for value in stderr.write_calls)
    assert all("canary" not in value for value in stderr.write_calls)


@pytest.mark.parametrize(
    "stderr",
    (
        _RecordingStream(system_exit_write=True),
        _RecordingStream(system_exit_flush=True),
    ),
    ids=("write-system-exit", "flush-system-exit"),
)
def test_stderr_system_exit_is_swallowed_and_preserves_exit_20(
    monkeypatch: pytest.MonkeyPatch,
    stderr: _RecordingStream,
) -> None:
    stdout = _RecordingStream()
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    monkeypatch.setattr(
        cli,
        "inspect_sqlite_live_order_journal",
        lambda _path, *, account_id: (_ for _ in ()).throw(
            SystemExit("sensitive-stderr-emission-system-exit-canary")
        ),
    )

    exit_code = cli.main(["--journal", "journal.sqlite3", "--account-id", "account-id"])

    assert exit_code == 20
    assert stdout.write_calls == []
    assert stderr.write_calls == [
        '{"failure_code":"internal_failure","output_schema_version":1,'
        '"status":"internal_failure"}\n'
    ]
    assert all(
        "sensitive-stderr-emission-system-exit-canary" not in value for value in stderr.write_calls
    )
    assert all("Traceback" not in value for value in stderr.write_calls)


def test_typed_failure_flush_system_exit_does_not_retry_internal_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdout = _RecordingStream()
    stderr = _RecordingStream(system_exit_flush=True)
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    monkeypatch.setattr(
        cli,
        "inspect_sqlite_live_order_journal",
        lambda _path, *, account_id: (_ for _ in ()).throw(
            LiveJournalInspectionError(LiveJournalInspectionFailureCode.SOURCE_UNAVAILABLE)
        ),
    )

    exit_code = cli.main(["--journal", "journal.sqlite3", "--account-id", "account-id"])

    assert exit_code == 20
    assert stdout.write_calls == []
    assert stderr.write_calls == [
        '{"failure_code":"source_unavailable","output_schema_version":1,'
        '"status":"inspection_failure"}\n'
    ]
    assert "internal_failure" not in stderr.write_calls[0]
    assert "system-exit-canary" not in stderr.write_calls[0]
    assert "Traceback" not in stderr.write_calls[0]


def test_failure_line_construction_system_exit_uses_at_most_one_fallback_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdout = _RecordingStream()
    stderr = _RecordingStream()
    monkeypatch.setattr(cli.sys, "stdout", stdout)
    monkeypatch.setattr(cli.sys, "stderr", stderr)
    monkeypatch.setattr(
        cli,
        "_failure_line",
        lambda **_kwargs: (_ for _ in ()).throw(
            SystemExit("failure-line-construction-system-exit-canary")
        ),
    )

    exit_code = cli.main(["--attacker-invalid"])

    assert exit_code == 20
    assert stdout.write_calls == []
    assert len(stderr.write_calls) <= 1
    assert stderr.write_calls == [
        '{"failure_code":"internal_failure","output_schema_version":1,'
        '"status":"internal_failure"}\n'
    ]
    assert "failure-line-construction-system-exit-canary" not in stderr.write_calls[0]
    assert "Traceback" not in stderr.write_calls[0]


def test_cli_source_has_no_privileged_imports_or_reflective_serialization() -> None:
    source_path = Path(cli.__file__)
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imports: set[str] = set()
    called_names: set[str] = set()
    attributes: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module or "")
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                called_names.add(node.func.id)
            elif isinstance(node.func, ast.Attribute):
                called_names.add(node.func.attr)
        elif isinstance(node, ast.Attribute):
            attributes.add(node.attr)

    forbidden_import_parts = {
        "broker",
        "capital",
        "config",
        "credential",
        "credentials",
        "ctypes",
        "dll",
        "dotenv",
        "socket",
    }
    assert not {
        name for name in imports if forbidden_import_parts.intersection(name.lower().split("."))
    }
    assert "asdict" not in called_names
    assert not {"dispatch", "commit", "migrate", "migration"}.intersection(called_names)
    assert not {"stdin", "environ", "getenv"}.intersection(attributes)
