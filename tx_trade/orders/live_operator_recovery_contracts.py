"""Pure redacted contracts for explicit offline operator recovery decisions."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import re
from typing import TypeVar

from .live_reconciliation_projection_contracts import (
    AuthoritativeOrderProjectionPlan,
    OrderProjectionDisposition,
)

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_T = TypeVar("_T")


def _identifier(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be a bounded ASCII identifier")


def _digest(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _DIGEST.fullmatch(value):
        raise ValueError(f"{name} must be a canonical SHA-256 fingerprint")


def _nonnegative_int(value: object, name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be nonnegative")


def _exact_tuple(values: object, expected: type[_T], name: str) -> tuple[_T, ...]:
    if type(values) is not tuple:
        raise TypeError(f"{name} must be a tuple")
    if any(type(item) is not expected for item in values):
        raise TypeError(f"{name} must contain exact {expected.__name__} values")
    return values


class OperatorRecoveryDisposition(StrEnum):
    READY_NO_ACTION = "ready_no_action"
    NEEDS_BROKER_EVIDENCE = "needs_broker_evidence"
    READY_FOR_EXPLICIT_COMMIT = "ready_for_explicit_commit"
    UNSUPPORTED_REQUIRES_ESCALATION = "unsupported_requires_escalation"
    BLOCKED_INTEGRITY_FAILURE = "blocked_integrity_failure"


class OperatorRecoveryTargetKind(StrEnum):
    CLAIM = "claim"
    OBSERVATION = "observation"
    REQUIREMENT = "requirement"


class OperatorRecoveryResolution(StrEnum):
    BROKER_ORDER_CONFIRMED = "broker_order_confirmed"
    BROKER_FILL_CONFIRMED = "broker_fill_confirmed"
    SATISFIED = "satisfied"


class OperatorRecoveryReason(StrEnum):
    BROKER_EVIDENCE_REQUIRED = "broker_evidence_required"
    INCOMPLETE_EVIDENCE = "incomplete_evidence"
    AMBIGUOUS_EVIDENCE = "ambiguous_evidence"
    UNSUPPORTED_ISSUE = "unsupported_issue"
    UNSUPPORTED_TARGET = "unsupported_target"
    PROJECTION_NOT_READY = "projection_not_ready"
    INTEGRITY_FAILURE = "integrity_failure"


_BROKER_RESOLUTIONS = {
    OperatorRecoveryResolution.BROKER_FILL_CONFIRMED,
    OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,
}


@dataclass(frozen=True, slots=True)
class RedactedOperatorRecoveryTarget:
    kind: OperatorRecoveryTargetKind
    target_id: str = field(repr=False)
    allowed_resolutions: tuple[OperatorRecoveryResolution, ...]

    def __post_init__(self) -> None:
        if type(self.kind) is not OperatorRecoveryTargetKind:
            raise TypeError("kind must be OperatorRecoveryTargetKind")
        _identifier(self.target_id, "target_id")
        values = _exact_tuple(
            self.allowed_resolutions,
            OperatorRecoveryResolution,
            "allowed_resolutions",
        )
        canonical = tuple(item.value for item in values)
        if not values:
            raise ValueError("allowed_resolutions must not be empty")
        if len(set(values)) != len(values) or tuple(sorted(canonical)) != canonical:
            raise ValueError("allowed_resolutions must be unique and canonically sorted")
        allowed = (
            {OperatorRecoveryResolution.SATISFIED}
            if self.kind is OperatorRecoveryTargetKind.REQUIREMENT
            else _BROKER_RESOLUTIONS
        )
        if not set(values).issubset(allowed):
            raise ValueError("resolution is not allowed for target kind")


@dataclass(frozen=True, slots=True)
class OfflineOperatorRecoveryPlan:
    account_id: str = field(repr=False)
    journal_sequence: int
    inspection_digest: str = field(repr=False)
    disposition: OperatorRecoveryDisposition
    issue_codes: tuple[str, ...] = ()
    reasons: tuple[OperatorRecoveryReason, ...] = ()
    targets: tuple[RedactedOperatorRecoveryTarget, ...] = field(default=(), repr=False)
    projection_plan: AuthoritativeOrderProjectionPlan | None = field(
        default=None,
        repr=False,
    )

    def __post_init__(self) -> None:
        _identifier(self.account_id, "account_id")
        _nonnegative_int(self.journal_sequence, "journal_sequence")
        _digest(self.inspection_digest, "inspection_digest")
        if type(self.disposition) is not OperatorRecoveryDisposition:
            raise TypeError("disposition must be OperatorRecoveryDisposition")
        issues = _exact_tuple(self.issue_codes, str, "issue_codes")
        reasons = _exact_tuple(self.reasons, OperatorRecoveryReason, "reasons")
        targets = _exact_tuple(self.targets, RedactedOperatorRecoveryTarget, "targets")
        for issue in issues:
            _identifier(issue, "issue_code")
        issue_order = tuple(issues)
        reason_order = tuple(item.value for item in reasons)
        target_order = tuple((item.kind.value, item.target_id) for item in targets)
        for values in (issue_order, reason_order, target_order):
            if len(set(values)) != len(values) or tuple(sorted(values)) != values:
                raise ValueError("plan collections must be unique and canonically sorted")
        if self.projection_plan is not None:
            if type(self.projection_plan) is not AuthoritativeOrderProjectionPlan:
                raise TypeError("projection_plan must be AuthoritativeOrderProjectionPlan or None")
            if (
                self.projection_plan.account_id != self.account_id
                or self.projection_plan.expected_journal_sequence != self.journal_sequence
            ):
                raise ValueError("projection plan must match recovery account and journal cut")

        if self.disposition is OperatorRecoveryDisposition.READY_NO_ACTION:
            if issues or reasons or targets or self.projection_plan is not None:
                raise ValueError("READY_NO_ACTION must not contain recovery actions")
        elif self.disposition is OperatorRecoveryDisposition.READY_FOR_EXPLICIT_COMMIT:
            if not issues or not targets or reasons:
                raise ValueError("explicit commit readiness requires issues and targets only")
            if (
                self.projection_plan is None
                or self.projection_plan.disposition is not OrderProjectionDisposition.READY
            ):
                raise ValueError("explicit commit requires a READY projection decision")
        elif self.disposition is OperatorRecoveryDisposition.BLOCKED_INTEGRITY_FAILURE:
            if (
                reasons != (OperatorRecoveryReason.INTEGRITY_FAILURE,)
                or targets
                or self.projection_plan is not None
            ):
                raise ValueError("integrity failure plans must contain only the integrity reason")
        else:
            if not reasons or targets:
                raise ValueError("non-committable plans require reasons and no targets")

    @property
    def commit_allowed(self) -> bool:
        return self.disposition is OperatorRecoveryDisposition.READY_FOR_EXPLICIT_COMMIT

    @property
    def may_dispatch(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class ExplicitOperatorRecoveryTargetSelection:
    kind: OperatorRecoveryTargetKind
    target_id: str = field(repr=False)
    resolution: OperatorRecoveryResolution

    def __post_init__(self) -> None:
        if type(self.kind) is not OperatorRecoveryTargetKind:
            raise TypeError("kind must be OperatorRecoveryTargetKind")
        _identifier(self.target_id, "target_id")
        if type(self.resolution) is not OperatorRecoveryResolution:
            raise TypeError("resolution must be OperatorRecoveryResolution")
        if self.kind is OperatorRecoveryTargetKind.REQUIREMENT:
            if self.resolution is not OperatorRecoveryResolution.SATISFIED:
                raise ValueError("requirement selections must be satisfied")
        elif self.resolution not in _BROKER_RESOLUTIONS:
            raise ValueError("broker targets require a broker-confirmed resolution")


@dataclass(frozen=True, slots=True)
class ExplicitOperatorRecoverySelection:
    commit_id: str
    account_id: str = field(repr=False)
    journal_sequence: int
    inspection_digest: str = field(repr=False)
    selected_targets: tuple[ExplicitOperatorRecoveryTargetSelection, ...] = field(repr=False)

    def __post_init__(self) -> None:
        _identifier(self.commit_id, "commit_id")
        _identifier(self.account_id, "account_id")
        _nonnegative_int(self.journal_sequence, "journal_sequence")
        _digest(self.inspection_digest, "inspection_digest")
        targets = _exact_tuple(
            self.selected_targets,
            ExplicitOperatorRecoveryTargetSelection,
            "selected_targets",
        )
        if not targets:
            raise ValueError("selected_targets must not be empty")
        canonical = tuple((item.kind.value, item.target_id) for item in targets)
        if len(set(canonical)) != len(canonical) or tuple(sorted(canonical)) != canonical:
            raise ValueError("selected_targets must be unique and canonically sorted")


__all__ = [
    "ExplicitOperatorRecoverySelection",
    "ExplicitOperatorRecoveryTargetSelection",
    "OfflineOperatorRecoveryPlan",
    "OperatorRecoveryDisposition",
    "OperatorRecoveryReason",
    "OperatorRecoveryResolution",
    "OperatorRecoveryTargetKind",
    "RedactedOperatorRecoveryTarget",
]
