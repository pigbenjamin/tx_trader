from dataclasses import FrozenInstanceError, fields

import pytest

from tx_trade.orders import live_journal_inspection_contracts as contracts
from tx_trade.orders.live_journal_inspection_contracts import (
    LiveJournalInspectionDisposition,
    LiveJournalInspectionError,
    LiveJournalInspectionFailureCode,
    LiveJournalInspectionIssueCode,
    LiveJournalInspectionReport,
    LiveJournalInspectionTargetKind,
    RedactedLiveJournalInspectionTarget,
)

DIGEST = "sha256:" + "1" * 64
ACCOUNT = "account-secret"


def _target(
    target_id: str = "claim-secret",
    issue_code: LiveJournalInspectionIssueCode = (
        LiveJournalInspectionIssueCode.OUTSTANDING_DISPATCH
    ),
) -> RedactedLiveJournalInspectionTarget:
    return RedactedLiveJournalInspectionTarget(
        LiveJournalInspectionTargetKind.CLAIM,
        target_id,
        issue_code,
    )


def _report(**changes: object) -> LiveJournalInspectionReport:
    values: dict[str, object] = {
        "account_id": ACCOUNT,
        "database_schema_version": 3,
        "journal_sequence": 7,
        "disposition": LiveJournalInspectionDisposition.RECOVERY_REQUIRED,
        "issue_codes": (LiveJournalInspectionIssueCode.OUTSTANDING_DISPATCH,),
        "targets": (_target(),),
        "inspection_digest": DIGEST,
    }
    values.update(changes)
    return LiveJournalInspectionReport(**values)  # type: ignore[arg-type]


def test_public_surface_is_exact() -> None:
    assert contracts.__all__ == [
        "LiveJournalInspectionDisposition",
        "LiveJournalInspectionError",
        "LiveJournalInspectionFailureCode",
        "LiveJournalInspectionIssueCode",
        "LiveJournalInspectionReport",
        "LiveJournalInspectionTargetKind",
        "MAX_INSPECTION_TARGETS",
        "RedactedLiveJournalInspectionTarget",
    ]
    assert contracts.MAX_INSPECTION_TARGETS == 1024


def test_target_is_frozen_slotted_exact_and_redacted() -> None:
    target = _target()
    assert not hasattr(target, "__dict__")
    assert "claim-secret" not in repr(target)
    assert {item.name for item in fields(target)} == {"kind", "target_id", "issue_code"}
    with pytest.raises(FrozenInstanceError):
        target.target_id = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError, match="kind"):
        RedactedLiveJournalInspectionTarget("claim", "claim-1", target.issue_code)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="issue_code"):
        RedactedLiveJournalInspectionTarget(target.kind, "claim-1", "issue")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "target_id",
    ("", "a" * 129, "contains space", "café", "-leading"),
)
def test_target_rejects_noncanonical_identifier(target_id: str) -> None:
    with pytest.raises(ValueError, match="bounded ASCII"):
        _target(target_id)


@pytest.mark.parametrize(
    "report",
    (
        LiveJournalInspectionReport(
            ACCOUNT,
            2,
            7,
            LiveJournalInspectionDisposition.READY_NO_ACTION,
            (),
            (),
            DIGEST,
        ),
        _report(),
        *(
            LiveJournalInspectionReport(
                ACCOUNT,
                version,
                7,
                LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED,
                (LiveJournalInspectionIssueCode.SCHEMA_UPGRADE_REQUIRED,),
                (),
                DIGEST,
            )
            for version in (1, 2)
        ),
        LiveJournalInspectionReport(
            ACCOUNT,
            2,
            7,
            LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND,
            (LiveJournalInspectionIssueCode.ACCOUNT_NOT_FOUND,),
            (),
            DIGEST,
        ),
        LiveJournalInspectionReport(
            ACCOUNT,
            2,
            7,
            LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE,
            (LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,),
            (),
            DIGEST,
        ),
    ),
)
def test_every_disposition_is_read_only_and_has_exact_properties(
    report: LiveJournalInspectionReport,
) -> None:
    assert not report.may_dispatch
    assert not report.commit_allowed
    assert report.requires_reconciliation == (
        report.disposition
        in {
            LiveJournalInspectionDisposition.RECOVERY_REQUIRED,
            LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE,
        }
    )
    assert report.schema_upgrade_required == (
        report.disposition is LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED
    )
    assert ACCOUNT not in repr(report)
    assert DIGEST not in repr(report)
    assert "claim-secret" not in repr(report)


def test_recovery_may_have_no_targets_for_global_blocker() -> None:
    report = _report(
        issue_codes=(LiveJournalInspectionIssueCode.GLOBAL_RECOVERY_BLOCKER,),
        targets=(),
    )
    assert report.requires_reconciliation
    assert report.targets == ()


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"database_schema_version": True}, "integer"),
        ({"journal_sequence": True}, "integer"),
        ({"inspection_digest": "sha256:" + "A" * 64}, "SHA-256"),
        ({"issue_codes": [LiveJournalInspectionIssueCode.OUTSTANDING_DISPATCH]}, "tuple"),
        ({"targets": [_target()]}, "tuple"),
        (
            {
                "issue_codes": (
                    LiveJournalInspectionIssueCode.PENDING_BROKER_EVIDENCE,
                    LiveJournalInspectionIssueCode.OUTSTANDING_DISPATCH,
                )
            },
            "sorted",
        ),
        (
            {
                "issue_codes": (
                    LiveJournalInspectionIssueCode.OUTSTANDING_DISPATCH,
                    LiveJournalInspectionIssueCode.OUTSTANDING_DISPATCH,
                )
            },
            "unique",
        ),
        (
            {"targets": (_target("claim-2"), _target("claim-1"))},
            "sorted",
        ),
    ),
)
def test_report_rejects_coercion_and_noncanonical_collections(
    changes: dict[str, object], message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        _report(**changes)


def test_report_rejects_targets_beyond_fixed_capacity() -> None:
    targets = tuple(_target(f"claim-{index:04d}") for index in range(1025))
    with pytest.raises(ValueError, match="limit"):
        _report(targets=targets)


def test_report_requires_target_issue_to_be_reported() -> None:
    with pytest.raises(ValueError, match="present"):
        _report(
            issue_codes=(LiveJournalInspectionIssueCode.GLOBAL_RECOVERY_BLOCKER,),
        )


@pytest.mark.parametrize(
    "report",
    (
        lambda: _report(
            disposition=LiveJournalInspectionDisposition.READY_NO_ACTION,
        ),
        lambda: _report(
            disposition=LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED,
            database_schema_version=3,
            issue_codes=(LiveJournalInspectionIssueCode.SCHEMA_UPGRADE_REQUIRED,),
            targets=(),
        ),
        lambda: _report(
            disposition=LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND,
        ),
        lambda: _report(
            disposition=LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE,
        ),
        lambda: _report(issue_codes=(), targets=()),
        lambda: _report(
            issue_codes=(LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,), targets=()
        ),
    ),
)
def test_disposition_invariants_reject_ambiguous_reports(report: object) -> None:
    with pytest.raises(ValueError):
        report()  # type: ignore[operator]


def test_report_fields_expose_no_sensitive_source_or_mutation_data() -> None:
    names = {item.name for item in fields(LiveJournalInspectionReport)}
    assert names == {
        "account_id",
        "database_schema_version",
        "journal_sequence",
        "disposition",
        "issue_codes",
        "targets",
        "inspection_digest",
    }
    forbidden = {
        "path",
        "snapshot",
        "order",
        "command",
        "token",
        "payload",
        "evidence",
        "credential",
        "broker",
        "dispatch",
    }
    assert not names & forbidden


@pytest.mark.parametrize("code", tuple(LiveJournalInspectionFailureCode))
def test_inspection_error_is_stable_and_sanitized(
    code: LiveJournalInspectionFailureCode,
) -> None:
    error = LiveJournalInspectionError(code)
    assert error.code is code
    assert code.value not in str(error)
    for attacker_value in (
        "C:/secret/journal.sqlite",
        ACCOUNT,
        "SQLite says malformed page 42",
        "broker-token-secret",
    ):
        assert attacker_value not in str(error)
        assert attacker_value not in repr(error)
    with pytest.raises(TypeError, match="code"):
        LiveJournalInspectionError(code.value)  # type: ignore[arg-type]


def test_sensitive_invalid_values_are_never_echoed() -> None:
    secret = "account secret must not leak"
    with pytest.raises(ValueError) as raised:
        LiveJournalInspectionReport(
            secret,
            2,
            7,
            LiveJournalInspectionDisposition.READY_NO_ACTION,
            (),
            (),
            DIGEST,
        )
    assert secret not in str(raised.value)
