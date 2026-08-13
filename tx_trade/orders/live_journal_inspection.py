"""Pure, redacted construction of live-journal inspection reports."""

from __future__ import annotations

from hashlib import sha256
import json
import re

from .live_contracts import LiveCommand, NewOrderCommand
from .live_journal_contracts import LiveJournalIdentity, LiveJournalRecoverySnapshot
from .live_journal_inspection_contracts import (
    MAX_INSPECTION_TARGETS,
    LiveJournalInspectionDisposition,
    LiveJournalInspectionIssueCode,
    LiveJournalInspectionReport,
    LiveJournalInspectionTargetKind,
    RedactedLiveJournalInspectionTarget,
)
from .live_journal_recovery import RecoveryReadiness, verify_recovery_snapshot

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_DIGEST_DOMAIN = b"tx_trade.live.journal.inspection.report.v1\x00"
_TARGET_DOMAIN = b"tx_trade.live.journal.inspection.target.v1\x00"
_SPECIAL_SCOPED_ISSUES = frozenset(
    {
        LiveJournalInspectionIssueCode.ACCOUNT_NOT_FOUND,
        LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,
        LiveJournalInspectionIssueCode.SCHEMA_UPGRADE_REQUIRED,
    }
)


def _validate_account_id(account_id: object) -> None:
    if type(account_id) is not str:
        raise TypeError("account_id must be a string")
    if not _IDENTIFIER.fullmatch(account_id):
        raise ValueError("account_id must be a bounded ASCII identifier")


def _validate_schema_version(database_schema_version: object, expected: int) -> None:
    if type(database_schema_version) is not int:
        raise TypeError("database_schema_version must be an integer")
    if database_schema_version != expected:
        raise ValueError("database schema version is not supported by this report builder")


def _validate_identity(
    identity: object, supported_schema_versions: frozenset[int]
) -> LiveJournalIdentity:
    if type(identity) is not LiveJournalIdentity:
        raise TypeError("identity must be LiveJournalIdentity")
    if identity.schema_version not in supported_schema_versions:
        raise ValueError("journal identity schema version is not supported by this report builder")
    return identity


def _validate_scoped_issues(
    scoped_issue_codes: object,
) -> tuple[LiveJournalInspectionIssueCode, ...]:
    if type(scoped_issue_codes) is not tuple:
        raise TypeError("scoped_issue_codes must be a tuple")
    if any(type(item) is not LiveJournalInspectionIssueCode for item in scoped_issue_codes):
        raise TypeError(
            "scoped_issue_codes must contain exact LiveJournalInspectionIssueCode values"
        )
    if any(item in _SPECIAL_SCOPED_ISSUES for item in scoped_issue_codes):
        raise ValueError("scoped_issue_codes must not contain report status issue codes")
    return tuple(sorted(set(scoped_issue_codes), key=lambda item: item.value))


def _canonical_json(document: object) -> bytes:
    return json.dumps(
        document,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


def _opaque_target_id(
    identity: LiveJournalIdentity,
    account_id: str,
    database_schema_version: int,
    journal_sequence: int,
    kind: LiveJournalInspectionTargetKind,
    durable_id: str,
) -> str:
    material = {
        "account_id": account_id,
        "database_schema_version": database_schema_version,
        "durable_id": durable_id,
        "journal_id": identity.journal_id,
        "journal_sequence": journal_sequence,
        "kind": kind.value,
        "schema_fingerprint": identity.schema_fingerprint,
        "version": 1,
    }
    digest = sha256(_TARGET_DOMAIN + _canonical_json(material)).hexdigest()
    return f"{kind.value}-{digest}"


def _inspection_digest(
    identity: LiveJournalIdentity,
    account_id: str,
    database_schema_version: int,
    journal_sequence: int,
    disposition: LiveJournalInspectionDisposition,
    issues: tuple[LiveJournalInspectionIssueCode, ...],
    targets: tuple[RedactedLiveJournalInspectionTarget, ...],
) -> str:
    # This is an integrity fingerprint over a deliberately narrow allowlist, not
    # authentication and not a serialization of the recovery snapshot.
    material = {
        "account_id": account_id,
        "database_schema_version": database_schema_version,
        "disposition": disposition.value,
        "identity": {
            "journal_id": identity.journal_id,
            "schema_fingerprint": identity.schema_fingerprint,
            "schema_version": identity.schema_version,
        },
        "issue_codes": [item.value for item in issues],
        "journal_sequence": journal_sequence,
        "targets": [[item.kind.value, item.target_id, item.issue_code.value] for item in targets],
        "version": 1,
    }
    return f"sha256:{sha256(_DIGEST_DOMAIN + _canonical_json(material)).hexdigest()}"


def _report(
    identity: LiveJournalIdentity,
    account_id: str,
    database_schema_version: int,
    journal_sequence: int,
    disposition: LiveJournalInspectionDisposition,
    issues: tuple[LiveJournalInspectionIssueCode, ...],
    targets: tuple[RedactedLiveJournalInspectionTarget, ...] = (),
) -> LiveJournalInspectionReport:
    digest = _inspection_digest(
        identity,
        account_id,
        database_schema_version,
        journal_sequence,
        disposition,
        issues,
        targets,
    )
    return LiveJournalInspectionReport(
        account_id,
        database_schema_version,
        journal_sequence,
        disposition,
        issues,
        targets,
        digest,
    )


def _command_order_id(command: LiveCommand) -> str:
    if isinstance(command, NewOrderCommand):
        return command.intent.client_order_id
    # Exact command types have already been checked by their contracts and the
    # semantic snapshot verifier.
    return command.client_order_id


def build_live_journal_inspection_report(
    snapshot: LiveJournalRecoverySnapshot,
    account_id: str,
    *,
    database_schema_version: int,
    scoped_issue_codes: tuple[LiveJournalInspectionIssueCode, ...] = (),
) -> LiveJournalInspectionReport:
    """Build a deterministic account-scoped report without performing I/O."""

    if type(snapshot) is not LiveJournalRecoverySnapshot:
        raise TypeError("snapshot must be LiveJournalRecoverySnapshot")
    _validate_account_id(account_id)
    identity = _validate_identity(snapshot.identity, frozenset({1, 2, 3}))
    _validate_schema_version(database_schema_version, 3)
    scoped_issues = _validate_scoped_issues(scoped_issue_codes)

    verification = verify_recovery_snapshot(snapshot)
    if verification.readiness is RecoveryReadiness.BLOCKED:
        return _report(
            identity,
            account_id,
            database_schema_version,
            snapshot.journal_sequence,
            LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE,
            (LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,),
        )

    selected_orders = tuple(
        order for order in snapshot.orders if order.intent.account_id == account_id
    )
    if not selected_orders and not scoped_issues:
        return _report(
            identity,
            account_id,
            database_schema_version,
            snapshot.journal_sequence,
            LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND,
            (LiveJournalInspectionIssueCode.ACCOUNT_NOT_FOUND,),
        )

    selected_order_ids = {order.intent.client_order_id for order in selected_orders}
    selected_claims = tuple(
        claim
        for claim in snapshot.outstanding_claims
        if _command_order_id(claim.command) in selected_order_ids
    )
    targets: list[RedactedLiveJournalInspectionTarget] = []
    issues = set(scoped_issues)

    for order in selected_orders:
        binding = order.pending_command
        if binding is None:
            continue
        issues.add(LiveJournalInspectionIssueCode.PENDING_BROKER_EVIDENCE)
        targets.append(
            RedactedLiveJournalInspectionTarget(
                LiveJournalInspectionTargetKind.PENDING_COMMAND,
                _opaque_target_id(
                    identity,
                    account_id,
                    database_schema_version,
                    snapshot.journal_sequence,
                    LiveJournalInspectionTargetKind.PENDING_COMMAND,
                    binding.client_command_id,
                ),
                LiveJournalInspectionIssueCode.PENDING_BROKER_EVIDENCE,
            )
        )

    for claim in selected_claims:
        issues.add(LiveJournalInspectionIssueCode.OUTSTANDING_DISPATCH)
        targets.append(
            RedactedLiveJournalInspectionTarget(
                LiveJournalInspectionTargetKind.CLAIM,
                _opaque_target_id(
                    identity,
                    account_id,
                    database_schema_version,
                    snapshot.journal_sequence,
                    LiveJournalInspectionTargetKind.CLAIM,
                    claim.command.client_command_id,
                ),
                LiveJournalInspectionIssueCode.OUTSTANDING_DISPATCH,
            )
        )

    if len(targets) > MAX_INSPECTION_TARGETS:
        return _report(
            identity,
            account_id,
            database_schema_version,
            snapshot.journal_sequence,
            LiveJournalInspectionDisposition.RECOVERY_REQUIRED,
            (LiveJournalInspectionIssueCode.REPORT_LIMIT_EXCEEDED,),
        )

    canonical_targets = tuple(
        sorted(
            set(targets),
            key=lambda item: (item.kind.value, item.target_id, item.issue_code.value),
        )
    )
    canonical_issues = tuple(sorted(issues, key=lambda item: item.value))
    disposition = (
        LiveJournalInspectionDisposition.RECOVERY_REQUIRED
        if canonical_issues
        else LiveJournalInspectionDisposition.READY_NO_ACTION
    )
    return _report(
        identity,
        account_id,
        database_schema_version,
        snapshot.journal_sequence,
        disposition,
        canonical_issues,
        canonical_targets,
    )


def build_schema_upgrade_required_inspection_report(
    identity: LiveJournalIdentity,
    account_id: str,
    *,
    database_schema_version: int,
    journal_sequence: int,
) -> LiveJournalInspectionReport:
    """Build the only permitted inspection result for a valid legacy journal."""

    checked_identity = _validate_identity(identity, frozenset({1, 2}))
    _validate_account_id(account_id)
    if type(database_schema_version) is not int:
        raise TypeError("database_schema_version must be an integer")
    if database_schema_version not in {1, 2}:
        raise ValueError("database schema version is not supported by this report builder")
    if (database_schema_version, checked_identity.schema_version) not in {
        (1, 1),
        (2, 1),
        (2, 2),
    }:
        raise ValueError("identity and database schema versions are incompatible")
    if type(journal_sequence) is not int:
        raise TypeError("journal_sequence must be an integer")
    if journal_sequence < 0:
        raise ValueError("journal_sequence must be nonnegative")
    return _report(
        checked_identity,
        account_id,
        database_schema_version,
        journal_sequence,
        LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED,
        (LiveJournalInspectionIssueCode.SCHEMA_UPGRADE_REQUIRED,),
    )


__all__ = [
    "build_live_journal_inspection_report",
    "build_schema_upgrade_required_inspection_report",
]
