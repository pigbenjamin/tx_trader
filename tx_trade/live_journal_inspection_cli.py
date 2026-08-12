"""Output-only command-line composition for live-journal inspection."""

from __future__ import annotations

from enum import IntEnum
import json
import re
import sys
from typing import Sequence, TextIO

from tx_trade.orders.live_journal_inspection_contracts import (
    LiveJournalInspectionDisposition,
    LiveJournalInspectionError,
    LiveJournalInspectionReport,
)
from tx_trade.orders.sqlite_live_journal_inspection import (
    inspect_sqlite_live_order_journal,
)

OUTPUT_SCHEMA_VERSION = 1
MAX_CLI_OUTPUT_BYTES = 262_144
_ACCOUNT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_HELP = "usage: python -B -m tx_trade.live_journal_inspection_cli --journal PATH --account-id ID\n"
_INTERNAL_FAILURE_LINE = (
    '{"failure_code":"internal_failure","output_schema_version":1,"status":"internal_failure"}\n'
)


class LiveJournalInspectionCliExitCode(IntEnum):
    """Stable process exit codes for the inspection CLI."""

    READY_NO_ACTION = 0
    USAGE_ERROR = 2
    RECOVERY_REQUIRED = 10
    SCHEMA_UPGRADE_REQUIRED = 11
    ACCOUNT_NOT_FOUND = 12
    BLOCKED_INTEGRITY_FAILURE = 13
    INSPECTION_FAILURE = 20


_DISPOSITION_EXIT_CODES = {
    LiveJournalInspectionDisposition.READY_NO_ACTION: (
        LiveJournalInspectionCliExitCode.READY_NO_ACTION
    ),
    LiveJournalInspectionDisposition.RECOVERY_REQUIRED: (
        LiveJournalInspectionCliExitCode.RECOVERY_REQUIRED
    ),
    LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED: (
        LiveJournalInspectionCliExitCode.SCHEMA_UPGRADE_REQUIRED
    ),
    LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND: (
        LiveJournalInspectionCliExitCode.ACCOUNT_NOT_FOUND
    ),
    LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE: (
        LiveJournalInspectionCliExitCode.BLOCKED_INTEGRITY_FAILURE
    ),
}


class _InvalidCliRequest(ValueError):
    pass


def _parse_request(arguments: Sequence[str]) -> tuple[str, str] | None:
    if len(arguments) == 1 and arguments[0] in {"-h", "--help"}:
        return None

    values: dict[str, str] = {}
    index = 0
    while index < len(arguments):
        flag = arguments[index]
        if flag not in {"--journal", "--account-id"} or flag in values:
            raise _InvalidCliRequest
        index += 1
        if index >= len(arguments):
            raise _InvalidCliRequest
        value = arguments[index]
        if not value or value.startswith("-"):
            raise _InvalidCliRequest
        values[flag] = value
        index += 1

    if set(values) != {"--journal", "--account-id"}:
        raise _InvalidCliRequest
    journal = values["--journal"]
    account_id = values["--account-id"]
    if len(journal) > 4096 or _ACCOUNT_ID.fullmatch(account_id) is None:
        raise _InvalidCliRequest
    return journal, account_id


def _canonical_json_line(payload: dict[str, object]) -> str:
    line = (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    )
    encoded = line.encode("ascii")
    if len(encoded) > MAX_CLI_OUTPUT_BYTES:
        raise ValueError("CLI output exceeds its public bound")
    return line


def _write_once(stream: TextIO, line: str) -> bool:
    try:
        encoded = line.encode("ascii")
        if len(encoded) > MAX_CLI_OUTPUT_BYTES:
            return False
        binary_stream = getattr(stream, "buffer", None)
        if binary_stream is None:
            written = stream.write(line)
            if written != len(line):
                return False
            stream.flush()
        else:
            written = binary_stream.write(encoded)
            if written != len(encoded):
                return False
            binary_stream.flush()
    except (Exception, KeyboardInterrupt, SystemExit):
        return False
    return True


def _failure_line(*, failure_code: str, status: str) -> str:
    return _canonical_json_line(
        {
            "failure_code": failure_code,
            "output_schema_version": OUTPUT_SCHEMA_VERSION,
            "status": status,
        }
    )


def _emit_failure(*, failure_code: str, status: str) -> bool:
    try:
        line = _failure_line(failure_code=failure_code, status=status)
    except (Exception, KeyboardInterrupt, SystemExit):
        _write_once(sys.stderr, _INTERNAL_FAILURE_LINE)
        return False
    return _write_once(sys.stderr, line)


def _report_payload(report: LiveJournalInspectionReport) -> dict[str, object]:
    return {
        "commit_allowed": report.commit_allowed,
        "database_schema_version": report.database_schema_version,
        "disposition": report.disposition.value,
        "inspection_digest": report.inspection_digest,
        "issue_codes": [issue.value for issue in report.issue_codes],
        "journal_sequence": report.journal_sequence,
        "may_dispatch": report.may_dispatch,
        "output_schema_version": OUTPUT_SCHEMA_VERSION,
        "requires_reconciliation": report.requires_reconciliation,
        "schema_upgrade_required": report.schema_upgrade_required,
        "targets": [
            {
                "issue_code": target.issue_code.value,
                "kind": target.kind.value,
                "target_id": target.target_id,
            }
            for target in report.targets
        ],
    }


def serialize_live_journal_inspection_report(report: LiveJournalInspectionReport) -> str:
    """Serialize the explicit public report allowlist as one canonical JSON line."""

    return _canonical_json_line(_report_payload(report))


def main(argv: Sequence[str] | None = None) -> int:
    """Inspect one journal and emit only a bounded, redacted JSON report."""

    try:
        arguments = tuple(sys.argv[1:] if argv is None else argv)
        request = _parse_request(arguments)
    except _InvalidCliRequest:
        if _emit_failure(failure_code="invalid_cli_request", status="cli_failure"):
            return LiveJournalInspectionCliExitCode.USAGE_ERROR
        return LiveJournalInspectionCliExitCode.INSPECTION_FAILURE
    except (Exception, KeyboardInterrupt, SystemExit):
        _emit_failure(failure_code="internal_failure", status="internal_failure")
        return LiveJournalInspectionCliExitCode.INSPECTION_FAILURE

    if request is None:
        if _write_once(sys.stdout, _HELP):
            return LiveJournalInspectionCliExitCode.READY_NO_ACTION
        _emit_failure(failure_code="internal_failure", status="internal_failure")
        return LiveJournalInspectionCliExitCode.INSPECTION_FAILURE

    journal, account_id = request
    try:
        report = inspect_sqlite_live_order_journal(journal, account_id=account_id)
    except LiveJournalInspectionError as exc:
        _emit_failure(failure_code=exc.code.value, status="inspection_failure")
        return LiveJournalInspectionCliExitCode.INSPECTION_FAILURE
    except (Exception, KeyboardInterrupt, SystemExit):
        _emit_failure(failure_code="internal_failure", status="internal_failure")
        return LiveJournalInspectionCliExitCode.INSPECTION_FAILURE

    try:
        exit_code = _DISPOSITION_EXIT_CODES[report.disposition]
        output = serialize_live_journal_inspection_report(report)
    except (Exception, KeyboardInterrupt, SystemExit):
        _emit_failure(failure_code="internal_failure", status="internal_failure")
        return LiveJournalInspectionCliExitCode.INSPECTION_FAILURE

    if not _write_once(sys.stdout, output):
        _emit_failure(failure_code="internal_failure", status="internal_failure")
        return LiveJournalInspectionCliExitCode.INSPECTION_FAILURE
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
