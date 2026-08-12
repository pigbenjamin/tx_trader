from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError, fields
from datetime import datetime, timedelta, timezone
import inspect
from pathlib import Path
from typing import get_type_hints

import pytest

from tx_trade.orders import live_reconciliation_assessment_contracts as contracts
from tx_trade.orders.live_journal_inspection_contracts import (
    LiveJournalInspectionDisposition,
    LiveJournalInspectionIssueCode,
    LiveJournalInspectionReport,
)
from tx_trade.orders.live_ports import (
    BrokerFillsSnapshot,
    BrokerPositionsSnapshot,
    CompletenessEvidence,
    EvidenceCompleteness,
    EvidenceQueryKind,
    OpenOrdersSnapshot,
    ReconciliationResult,
    ReconciliationStatus,
)
from tx_trade.orders.live_reconciliation_assessment_contracts import (
    InspectedReconciliationAssessment,
    TrustedAssessmentSourceError,
    TrustedAssessmentSourceFailureCode,
)
from tx_trade.orders.live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    LocalReconciliationSnapshot,
    ReconciliationAssessment,
)

NOW = datetime(2026, 8, 13, tzinfo=timezone.utc)
ACCOUNT = "account-secret"
DIGEST = "sha256:" + "1" * 64


def _evidence(kind: EvidenceQueryKind, account_id: str = ACCOUNT) -> CompletenessEvidence:
    return CompletenessEvidence(
        kind,
        account_id,
        EvidenceCompleteness.COMPLETE,
        NOW + timedelta(seconds=1),
        "snapshot-1",
    )


def _assessment(account_id: str = ACCOUNT) -> ReconciliationAssessment:
    evidence = tuple(
        _evidence(kind, account_id)
        for kind in (
            EvidenceQueryKind.OPEN_ORDERS,
            EvidenceQueryKind.FILLS,
            EvidenceQueryKind.POSITIONS,
        )
    )
    local = LocalReconciliationSnapshot(
        account_id,
        (),
        (),
        (),
        NOW,
        journal_sequence=7,
    )
    broker = BrokerReconciliationSnapshot(
        "snapshot-1",
        account_id,
        OpenOrdersSnapshot((), evidence[0]),
        BrokerFillsSnapshot((), evidence[1]),
        BrokerPositionsSnapshot((), evidence[2]),
        NOW + timedelta(seconds=1),
    )
    result = ReconciliationResult(
        account_id,
        ReconciliationStatus.COMPLETE,
        (),
        evidence,
        NOW + timedelta(seconds=2),
    )
    return ReconciliationAssessment(local, broker, result)


def _inspection(**changes: object) -> LiveJournalInspectionReport:
    values: dict[str, object] = {
        "account_id": ACCOUNT,
        "database_schema_version": 2,
        "journal_sequence": 7,
        "disposition": LiveJournalInspectionDisposition.READY_NO_ACTION,
        "issue_codes": (),
        "targets": (),
        "inspection_digest": DIGEST,
    }
    values.update(changes)
    return LiveJournalInspectionReport(**values)  # type: ignore[arg-type]


def _pair(
    inspection: LiveJournalInspectionReport | None = None,
    assessment: ReconciliationAssessment | None = None,
) -> InspectedReconciliationAssessment:
    return InspectedReconciliationAssessment(
        inspection if inspection is not None else _inspection(),
        assessment if assessment is not None else _assessment(),
    )


def test_public_surface_constant_enum_and_signatures_are_frozen() -> None:
    assert contracts.__all__ == [
        "InspectedReconciliationAssessment",
        "MAX_TRUSTED_BROKER_OBSERVATIONS",
        "TrustedAssessmentSourceError",
        "TrustedAssessmentSourceFailureCode",
    ]
    assert contracts.MAX_TRUSTED_BROKER_OBSERVATIONS == 25_000
    assert {item.name: item.value for item in TrustedAssessmentSourceFailureCode} == {
        "INVALID_REQUEST": "invalid_request",
        "SOURCE_UNAVAILABLE": "source_unavailable",
        "ACTIVE_OR_UNCLEAN_SOURCE": "active_or_unclean_source",
        "SOURCE_CHANGED": "source_changed",
        "SCHEMA_UPGRADE_REQUIRED": "schema_upgrade_required",
        "ACCOUNT_NOT_FOUND": "account_not_found",
        "BROKER_SOURCE_FAILURE": "broker_source_failure",
        "ACCOUNT_SCOPE_MISMATCH": "account_scope_mismatch",
        "MALFORMED_EVIDENCE": "malformed_evidence",
        "CAPACITY_EXCEEDED": "capacity_exceeded",
        "INTEGRITY_FAILURE": "integrity_failure",
        "INTERNAL_FAILURE": "internal_failure",
    }

    error_signature = inspect.signature(TrustedAssessmentSourceError)
    assert tuple(error_signature.parameters) == ("code",)
    assert error_signature.parameters["code"].default is inspect.Parameter.empty
    assert get_type_hints(TrustedAssessmentSourceError.__init__) == {
        "code": TrustedAssessmentSourceFailureCode,
        "return": type(None),
    }
    pair_signature = inspect.signature(InspectedReconciliationAssessment)
    assert tuple(pair_signature.parameters) == ("inspection", "assessment")
    assert get_type_hints(InspectedReconciliationAssessment) == {
        "inspection": LiveJournalInspectionReport,
        "assessment": ReconciliationAssessment,
    }


def test_pair_is_frozen_slotted_redacted_and_never_authorizes_writes() -> None:
    pair = _pair()
    assert not hasattr(pair, "__dict__")
    assert [item.name for item in fields(pair)] == ["inspection", "assessment"]
    assert ACCOUNT not in repr(pair)
    assert DIGEST not in repr(pair)
    assert repr(pair) == "InspectedReconciliationAssessment()"
    assert pair.may_dispatch is False
    assert pair.commit_allowed is False
    assert not hasattr(pair, "may_resume")
    with pytest.raises(FrozenInstanceError):
        pair.inspection = _inspection()  # type: ignore[misc]


def test_ready_and_recovery_inspections_are_accepted_without_digest_authentication() -> None:
    ready = _pair(_inspection(inspection_digest="sha256:" + "2" * 64))
    recovery = _pair(
        _inspection(
            disposition=LiveJournalInspectionDisposition.RECOVERY_REQUIRED,
            issue_codes=(LiveJournalInspectionIssueCode.GLOBAL_RECOVERY_BLOCKER,),
        )
    )
    assert ready.inspection.inspection_digest != DIGEST
    assert recovery.inspection.disposition is LiveJournalInspectionDisposition.RECOVERY_REQUIRED
    assert not hasattr(ready, "digest_authenticated")
    assert not hasattr(ready, "inspection_authenticated")


def test_pair_requires_exact_nested_types_not_subclasses() -> None:
    class InspectionSubclass(LiveJournalInspectionReport):
        pass

    class AssessmentSubclass(ReconciliationAssessment):
        pass

    inspection = InspectionSubclass(
        ACCOUNT,
        2,
        7,
        LiveJournalInspectionDisposition.READY_NO_ACTION,
        (),
        (),
        DIGEST,
    )
    assessment = _assessment()
    assessment_subclass = AssessmentSubclass(
        assessment.local_snapshot,
        assessment.broker_snapshot,
        assessment.result,
    )
    with pytest.raises(TypeError, match="inspection"):
        InspectedReconciliationAssessment(inspection, assessment)
    with pytest.raises(TypeError, match="assessment"):
        InspectedReconciliationAssessment(_inspection(), assessment_subclass)
    with pytest.raises(TypeError, match="inspection"):
        InspectedReconciliationAssessment(object(), assessment)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="assessment"):
        InspectedReconciliationAssessment(_inspection(), object())  # type: ignore[arg-type]


def test_pair_requires_schema_version_two() -> None:
    inspection = _inspection(
        database_schema_version=1,
        disposition=LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED,
        issue_codes=(LiveJournalInspectionIssueCode.SCHEMA_UPGRADE_REQUIRED,),
    )
    with pytest.raises(ValueError, match="schema version 2"):
        _pair(inspection)


@pytest.mark.parametrize(
    "disposition",
    (
        LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED,
        LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND,
        LiveJournalInspectionDisposition.BLOCKED_INTEGRITY_FAILURE,
    ),
)
def test_pair_rejects_non_assessment_dispositions(
    disposition: LiveJournalInspectionDisposition,
) -> None:
    if disposition is LiveJournalInspectionDisposition.SCHEMA_UPGRADE_REQUIRED:
        inspection = _inspection(
            database_schema_version=1,
            disposition=disposition,
            issue_codes=(LiveJournalInspectionIssueCode.SCHEMA_UPGRADE_REQUIRED,),
        )
        expected = "schema version 2"
    elif disposition is LiveJournalInspectionDisposition.ACCOUNT_NOT_FOUND:
        inspection = _inspection(
            disposition=disposition,
            issue_codes=(LiveJournalInspectionIssueCode.ACCOUNT_NOT_FOUND,),
        )
        expected = "disposition"
    else:
        inspection = _inspection(
            disposition=disposition,
            issue_codes=(LiveJournalInspectionIssueCode.INTEGRITY_FAILURE,),
        )
        expected = "disposition"
    with pytest.raises(ValueError, match=expected):
        _pair(inspection)


@pytest.mark.parametrize("nested_field", ("local_snapshot", "broker_snapshot", "result"))
def test_pair_rejects_every_account_scope_mismatch(nested_field: str) -> None:
    assessment = _assessment()
    nested = getattr(assessment, nested_field)
    object.__setattr__(nested, "account_id", "different-account")
    with pytest.raises(ValueError, match="accounts must match"):
        _pair(assessment=assessment)


def test_pair_rejects_inspection_account_and_sequence_mismatches() -> None:
    with pytest.raises(ValueError, match="accounts must match"):
        _pair(_inspection(account_id="different-account"))
    with pytest.raises(ValueError, match="journal sequences must match"):
        _pair(_inspection(journal_sequence=8))


_ERROR_MESSAGES = {
    TrustedAssessmentSourceFailureCode.INVALID_REQUEST: (
        "trusted assessment source request is invalid"
    ),
    TrustedAssessmentSourceFailureCode.SOURCE_UNAVAILABLE: (
        "trusted assessment source is unavailable"
    ),
    TrustedAssessmentSourceFailureCode.ACTIVE_OR_UNCLEAN_SOURCE: (
        "trusted assessment source is active or unclean"
    ),
    TrustedAssessmentSourceFailureCode.SOURCE_CHANGED: (
        "trusted assessment source changed during inspection"
    ),
    TrustedAssessmentSourceFailureCode.SCHEMA_UPGRADE_REQUIRED: (
        "trusted assessment source requires a schema upgrade"
    ),
    TrustedAssessmentSourceFailureCode.ACCOUNT_NOT_FOUND: (
        "trusted assessment source account was not found"
    ),
    TrustedAssessmentSourceFailureCode.BROKER_SOURCE_FAILURE: (
        "trusted assessment broker source failed"
    ),
    TrustedAssessmentSourceFailureCode.ACCOUNT_SCOPE_MISMATCH: (
        "trusted assessment account scope does not match"
    ),
    TrustedAssessmentSourceFailureCode.MALFORMED_EVIDENCE: (
        "trusted assessment evidence is malformed"
    ),
    TrustedAssessmentSourceFailureCode.CAPACITY_EXCEEDED: (
        "trusted assessment source capacity was exceeded"
    ),
    TrustedAssessmentSourceFailureCode.INTEGRITY_FAILURE: (
        "trusted assessment source failed integrity validation"
    ),
    TrustedAssessmentSourceFailureCode.INTERNAL_FAILURE: (
        "trusted assessment source failed internally"
    ),
}


@pytest.mark.parametrize(("code", "message"), tuple(_ERROR_MESSAGES.items()))
def test_error_messages_are_exact_stable_and_sanitized(
    code: TrustedAssessmentSourceFailureCode,
    message: str,
) -> None:
    error = TrustedAssessmentSourceError(code)
    assert error.code is code
    assert str(error) == message
    for secret in (
        "C:/secret/journal.sqlite",
        ACCOUNT,
        "cursor-secret",
        "broker exception payload",
    ):
        assert secret not in str(error)
        assert secret not in repr(error)
    with pytest.raises(TypeError, match="code"):
        TrustedAssessmentSourceError(code.value)  # type: ignore[arg-type]


def test_module_imports_are_pure_and_bounded() -> None:
    module_path = Path(contracts.__file__)
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imports = {
        (node.level, node.module) for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }
    assert imports == {
        (0, "__future__"),
        (0, "dataclasses"),
        (0, "enum"),
        (1, "live_journal_inspection_contracts"),
        (1, "live_reconciliation_contracts"),
    }
    assert not any(isinstance(node, ast.Import) for node in ast.walk(tree))
