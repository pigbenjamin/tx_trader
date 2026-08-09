"""Pure redacted contracts for read-only live-journal inspection."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import re
from typing import TypeVar

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_T = TypeVar("_T")

MAX_INSPECTION_TARGETS = 1024


def _identifier(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be a bounded ASCII identifier")


def _digest(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _DIGEST.fullmatch(value):
        raise ValueError(f"{name} must be a canonical SHA-256 digest")


def _nonnegative_int(value: object, name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be nonnegative")


def _positive_int(value: object, name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 1:
        raise ValueError(f"{name} must be positive")


def _exact_tuple(values: object, expected: type[_T], name: str) -> tuple[_T, ...]:
    if type(values) is not tuple:
        raise TypeError(f"{name} must be a tuple")
    if any(type(item) is not expected for item in values):
        raise TypeError(f"{name} must contain exact {expected.__name__} values")
    return values


class LiveJournalInspectionDisposition(StrEnum):
    READY_NO_ACTION = "ready_no_action"
    RECOVERY_REQUIRED = "recovery_required"
    SCHEMA_UPGRADE_REQUIRED = "schema_upgrade_required"
    ACCOUNT_NOT_FOUND = "account_not_found"
    BLOCKED_INTEGRITY_FAILURE = "blocked_integrity_failure"


class LiveJournalInspectionIssueCode(StrEnum):
    ACCOUNT_NOT_FOUND = "account_not_found"
    SCHEMA_UPGRADE_REQUIRED = "schema_upgrade_required"
    INTEGRITY_FAILURE = "integrity_failure"
    OUTSTANDING_DISPATCH = "outstanding_dispatch"
    PENDING_BROKER_EVIDENCE = "pending_broker_evidence"
    UNRESOLVED_OBSERVATION = "unresolved_observation"
    CONFLICT_OBSERVATION = "conflict_observation"
    AMBIGUOUS_OBSERVATION = "ambiguous_observation"
    DURABLE_RECONCILIATION_REQUIREMENT = "durable_reconciliation_requirement"
    GLOBAL_RECOVERY_BLOCKER = "global_recovery_blocker"
    REPORT_LIMIT_EXCEEDED = "report_limit_exceeded"


class LiveJournalInspectionTargetKind(StrEnum):
    PENDING_COMMAND = "pending_command"
    CLAIM = "claim"
    OBSERVATION = "observation"
    REQUIREMENT = "requirement"


@dataclass(frozen=True, slots=True)
class RedactedLiveJournalInspectionTarget:
    kind: LiveJournalInspectionTargetKind
    target_id: str = field(repr=False)
    issue_code: LiveJournalInspectionIssueCode

    def __post_init__(self) -> None:
        if type(self.kind) is not LiveJournalInspectionTargetKind:
            raise TypeError("kind must be LiveJournalInspectionTargetKind")
        _identifier(self.target_id, "target_id")
        if type(self.issue_code) is not LiveJournalInspectionIssueCode:
            raise TypeError("issue_code must be LiveJournalInspectionIssueCode")


_SPECIAL_ISSUES = {
    LiveJournalInspectionIssueCode.ACCOUNT_NOT_FOUND,
    LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,
    LiveJournalInspectionIssueCode.SCHEMA_UPGRADE_REQUIRED,
}


@dataclass(frozen=True, slots=True)
class LiveJournalInspectionReport:
    account_id: str = field(repr=False)
    database_schema_version: int
    journal_sequence: int
    disposition: LiveJournalInspectionDisposition
    issue_codes: tuple[LiveJournalInspectionIssueCode, ...]
    targets: tuple[RedactedLiveJournalInspectionTarget, ...] = field(repr=False)
    inspection_digest: str = field(repr=False)

    def __post_init__(self) -> None:
        _identifier(self.account_id, "account_id")
        _positive_int(self.database_schema_version, "database_schema_version")
        _nonnegative_int(self.journal_sequence, "journal_sequence")
        if type(self.disposition) is not LiveJournalInspectionDisposition:
            raise TypeError("disposition must be LiveJournalInspectionDisposition")
        issues = _exact_tuple(
            self.issue_codes,
            LiveJournalInspectionIssueCode,
            "issue_codes",
        )
        targets = _exact_tuple(
            self.targets,
            RedactedLiveJournalInspectionTarget,
            "targets",
        )
        _digest(self.inspection_digest, "inspection_digest")

        issue_order = tuple(item.value for item in issues)
        target_order = tuple(
            (item.kind.value, item.target_id, item.issue_code.value) for item in targets
        )
        if len(set(issues)) != len(issues) or tuple(sorted(issue_order)) != issue_order:
            raise ValueError("issue_codes must be unique and canonically sorted")
        if len(targets) > MAX_INSPECTION_TARGETS:
            raise ValueError("targets exceed the inspection report limit")
        if (
            len(set(target_order)) != len(target_order)
            or tuple(sorted(target_order)) != target_order
        ):
            raise ValueError("targets must be unique and canonically sorted")
        if any(target.issue_code not in issues for target in targets):
            raise ValueError("target issue codes must be present in issue_codes")

        if self.disposition is LiveJournalInspectionDisposition.READY_NO_ACTION:
            if issues or targets:
                raise ValueError("READY_NO_ACTION must not contain issues or targets")
        elif self.disposition is LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED:
            if (
                self.database_schema_version != 1
                or issues != (LiveJournalInspectionIssueCode.SCHEMA_UPGRADE_REQUIRED,)
                or targets
            ):
                raise ValueError(
                    "SCHEMA_UPGRADE_REQUIRED requires database schema v1 and only its issue"
                )
        elif self.disposition is LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND:
            if issues != (LiveJournalInspectionIssueCode.ACCOUNT_NOT_FOUND,) or targets:
                raise ValueError("ACCOUNT_NOT_FOUND requires only its issue and no targets")
        elif self.disposition is LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE:
            if issues != (LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,) or targets:
                raise ValueError("BLOCKED_INTEGRITY_FAILURE requires only its issue and no targets")
        elif not issues or set(issues) & _SPECIAL_ISSUES:
            raise ValueError("RECOVERY_REQUIRED requires non-status recovery issues")

    @property
    def may_dispatch(self) -> bool:
        return False

    @property
    def commit_allowed(self) -> bool:
        return False

    @property
    def requires_reconciliation(self) -> bool:
        return self.disposition in {
            LiveJournalInspectionDisposition.RECOVERY_REQUIRED,
            LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE,
        }

    @property
    def schema_upgrade_required(self) -> bool:
        return self.disposition is LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED


class LiveJournalInspectionFailureCode(StrEnum):
    INVALID_REQUEST = "invalid_request"
    SOURCE_UNAVAILABLE = "source_unavailable"
    ACTIVE_OR_UNCLEAN_SOURCE = "active_or_unclean_source"
    INTEGRITY_FAILURE = "integrity_failure"
    CAPACITY_EXCEEDED = "capacity_exceeded"
    SOURCE_CHANGED = "source_changed"


_FAILURE_MESSAGES = {
    LiveJournalInspectionFailureCode.INVALID_REQUEST: "live journal inspection request is invalid",
    LiveJournalInspectionFailureCode.SOURCE_UNAVAILABLE: "live journal inspection source is unavailable",
    LiveJournalInspectionFailureCode.ACTIVE_OR_UNCLEAN_SOURCE: (
        "live journal inspection source is active or unclean"
    ),
    LiveJournalInspectionFailureCode.INTEGRITY_FAILURE: (
        "live journal inspection failed integrity validation"
    ),
    LiveJournalInspectionFailureCode.CAPACITY_EXCEEDED: (
        "live journal inspection capacity was exceeded"
    ),
    LiveJournalInspectionFailureCode.SOURCE_CHANGED: ("live journal inspection source changed"),
}


class LiveJournalInspectionError(RuntimeError):
    """Inspection failure with deliberately stable, non-sensitive public text."""

    def __init__(self, code: LiveJournalInspectionFailureCode) -> None:
        if type(code) is not LiveJournalInspectionFailureCode:
            raise TypeError("code must be LiveJournalInspectionFailureCode")
        self.code = code
        super().__init__(_FAILURE_MESSAGES[code])


__all__ = [
    "LiveJournalInspectionDisposition",
    "LiveJournalInspectionError",
    "LiveJournalInspectionFailureCode",
    "LiveJournalInspectionIssueCode",
    "LiveJournalInspectionReport",
    "LiveJournalInspectionTargetKind",
    "MAX_INSPECTION_TARGETS",
    "RedactedLiveJournalInspectionTarget",
]
