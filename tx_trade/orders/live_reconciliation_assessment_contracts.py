"""Pure contracts for assessments loaded from a trusted inspection source."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from .live_journal_inspection_contracts import (
    LiveJournalInspectionDisposition,
    LiveJournalInspectionReport,
)
from .live_reconciliation_contracts import ReconciliationAssessment

MAX_TRUSTED_BROKER_OBSERVATIONS = 25_000


class TrustedAssessmentSourceFailureCode(StrEnum):
    INVALID_REQUEST = "invalid_request"
    SOURCE_UNAVAILABLE = "source_unavailable"
    ACTIVE_OR_UNCLEAN_SOURCE = "active_or_unclean_source"
    SOURCE_CHANGED = "source_changed"
    SCHEMA_UPGRADE_REQUIRED = "schema_upgrade_required"
    ACCOUNT_NOT_FOUND = "account_not_found"
    BROKER_SOURCE_FAILURE = "broker_source_failure"
    ACCOUNT_SCOPE_MISMATCH = "account_scope_mismatch"
    MALFORMED_EVIDENCE = "malformed_evidence"
    CAPACITY_EXCEEDED = "capacity_exceeded"
    INTEGRITY_FAILURE = "integrity_failure"
    INTERNAL_FAILURE = "internal_failure"


_FAILURE_MESSAGES = {
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


class TrustedAssessmentSourceError(RuntimeError):
    """Trusted-source failure with stable, deliberately sanitized text."""

    def __init__(self, code: TrustedAssessmentSourceFailureCode) -> None:
        if type(code) is not TrustedAssessmentSourceFailureCode:
            raise TypeError("code must be TrustedAssessmentSourceFailureCode")
        self.code = code
        super().__init__(_FAILURE_MESSAGES[code])


@dataclass(frozen=True, slots=True)
class InspectedReconciliationAssessment:
    """A reconciliation assessment paired with its read-only inspection."""

    inspection: LiveJournalInspectionReport = field(repr=False)
    assessment: ReconciliationAssessment = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.inspection) is not LiveJournalInspectionReport:
            raise TypeError("inspection must be LiveJournalInspectionReport")
        if type(self.assessment) is not ReconciliationAssessment:
            raise TypeError("assessment must be ReconciliationAssessment")
        if self.inspection.database_schema_version != 2:
            raise ValueError("inspection must use database schema version 2")
        if self.inspection.disposition not in {
            LiveJournalInspectionDisposition.READY_NO_ACTION,
            LiveJournalInspectionDisposition.RECOVERY_REQUIRED,
        }:
            raise ValueError("inspection disposition cannot produce an assessment")

        local_snapshot = self.assessment.local_snapshot
        broker_snapshot = self.assessment.broker_snapshot
        result = self.assessment.result
        account_id = self.inspection.account_id
        if (
            local_snapshot.account_id != account_id
            or broker_snapshot.account_id != account_id
            or result.account_id != account_id
        ):
            raise ValueError("inspection, local, broker, and result accounts must match")
        if self.inspection.journal_sequence != local_snapshot.journal_sequence:
            raise ValueError("inspection and local journal sequences must match")

    @property
    def may_dispatch(self) -> bool:
        return False

    @property
    def commit_allowed(self) -> bool:
        return False


__all__ = [
    "InspectedReconciliationAssessment",
    "MAX_TRUSTED_BROKER_OBSERVATIONS",
    "TrustedAssessmentSourceError",
    "TrustedAssessmentSourceFailureCode",
]
