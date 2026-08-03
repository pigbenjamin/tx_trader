"""Pure contracts for atomically committing a reconciliation assessment.

This module defines data and port boundaries only.  In particular, it has no
persistence knowledge and does not manufacture commit timestamps.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
import re
from typing import Protocol, TypeVar, runtime_checkable

from .live_contracts import LiveOrder
from .live_reconciliation_contracts import ReconciliationAssessment

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_T = TypeVar("_T")


def _identifier(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be a bounded ASCII identifier")


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


def _utc(value: object, name: str) -> None:
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware UTC")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _exact_tuple(values: object, expected: type[_T], name: str) -> tuple[_T, ...]:
    if type(values) is not tuple:
        raise TypeError(f"{name} must be a tuple")
    if any(type(item) is not expected for item in values):
        raise TypeError(f"{name} must contain {expected.__name__} values")
    return values


class ClaimResolution(StrEnum):
    """Supported terminal handling for an outstanding dispatch claim."""

    BROKER_ORDER_CONFIRMED = "broker_order_confirmed"
    BROKER_FILL_CONFIRMED = "broker_fill_confirmed"


class ObservationStatus(StrEnum):
    """Durable pre-state required by an observation compare-and-swap."""

    UNRESOLVED = "unresolved"
    CONFLICT = "conflict"
    AMBIGUOUS = "ambiguous"


class ObservationResolution(StrEnum):
    """Supported terminal handling for a durable broker observation."""

    BROKER_ORDER_CONFIRMED = "broker_order_confirmed"
    BROKER_FILL_CONFIRMED = "broker_fill_confirmed"


class RequirementResolution(StrEnum):
    """Supported terminal handling for a durable reconciliation requirement."""

    SATISFIED = "satisfied"


@dataclass(frozen=True, slots=True)
class ExpectedOrderVersion:
    client_order_id: str
    version: int

    def __post_init__(self) -> None:
        _identifier(self.client_order_id, "client_order_id")
        _positive_int(self.version, "version")


@dataclass(frozen=True, slots=True)
class ClaimResolutionDirective:
    client_command_id: str
    claim_token: str = field(repr=False)
    resolution: ClaimResolution

    def __post_init__(self) -> None:
        _identifier(self.client_command_id, "client_command_id")
        _identifier(self.claim_token, "claim_token")
        if type(self.resolution) is not ClaimResolution:
            raise TypeError("resolution must be ClaimResolution")


@dataclass(frozen=True, slots=True)
class ObservationResolutionDirective:
    observation_id: str
    expected_status: ObservationStatus
    normalized_event_id: str
    resolution: ObservationResolution

    def __post_init__(self) -> None:
        _identifier(self.observation_id, "observation_id")
        if type(self.expected_status) is not ObservationStatus:
            raise TypeError("expected_status must be ObservationStatus")
        _identifier(self.normalized_event_id, "normalized_event_id")
        if type(self.resolution) is not ObservationResolution:
            raise TypeError("resolution must be ObservationResolution")


@dataclass(frozen=True, slots=True)
class RequirementResolutionDirective:
    requirement_id: int
    resolution: RequirementResolution

    def __post_init__(self) -> None:
        _positive_int(self.requirement_id, "requirement_id")
        if type(self.resolution) is not RequirementResolution:
            raise TypeError("resolution must be RequirementResolution")


@dataclass(frozen=True, slots=True)
class DurableReconciliationCommitRequest:
    """Caller-identified compare-and-swap request for one account journal."""

    commit_id: str
    account_id: str = field(repr=False)
    assessment: ReconciliationAssessment
    expected_journal_sequence: int
    expected_order_versions: tuple[ExpectedOrderVersion, ...] = ()
    claim_resolutions: tuple[ClaimResolutionDirective, ...] = ()
    observation_resolutions: tuple[ObservationResolutionDirective, ...] = ()
    requirement_resolutions: tuple[RequirementResolutionDirective, ...] = ()
    order_projections: tuple[LiveOrder, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.commit_id, "commit_id")
        _identifier(self.account_id, "account_id")
        if type(self.assessment) is not ReconciliationAssessment:
            raise TypeError("assessment must be ReconciliationAssessment")
        if self.assessment.result.account_id != self.account_id:
            raise ValueError("assessment must match request account")
        _nonnegative_int(self.expected_journal_sequence, "expected_journal_sequence")
        if self.expected_journal_sequence != self.assessment.local_snapshot.journal_sequence:
            raise ValueError("expected_journal_sequence must match assessment durable cut")

        versions = _exact_tuple(
            self.expected_order_versions, ExpectedOrderVersion, "expected_order_versions"
        )
        claims = _exact_tuple(self.claim_resolutions, ClaimResolutionDirective, "claim_resolutions")
        observations = _exact_tuple(
            self.observation_resolutions,
            ObservationResolutionDirective,
            "observation_resolutions",
        )
        requirements = _exact_tuple(
            self.requirement_resolutions,
            RequirementResolutionDirective,
            "requirement_resolutions",
        )
        projections = _exact_tuple(self.order_projections, LiveOrder, "order_projections")

        unique_groups: tuple[tuple[object, ...], ...] = (
            tuple(item.client_order_id for item in versions),
            tuple(item.client_command_id for item in claims),
            tuple(item.claim_token for item in claims),
            tuple(item.observation_id for item in observations),
            tuple(item.normalized_event_id for item in observations),
            tuple(item.requirement_id for item in requirements),
            tuple(item.intent.client_order_id for item in projections),
        )
        if any(len(set(targets)) != len(targets) for targets in unique_groups):
            raise ValueError("resolution and order targets must be unique within each kind")

        expected_by_order = {item.client_order_id: item.version for item in versions}
        for order in projections:
            if order.intent.account_id != self.account_id:
                raise ValueError("projected orders must match request account")
            expected = expected_by_order.get(order.intent.client_order_id)
            if expected is None:
                raise ValueError("each projected order requires an expected order version")
            if order.version != expected + 1:
                raise ValueError("projected order version must advance expected version by one")


class ReconciliationCommitDisposition(StrEnum):
    COMMITTED = "committed"
    EXACT_RETRY = "exact_retry"
    ID_CONFLICT = "id_conflict"
    STALE_SNAPSHOT = "stale_snapshot"
    VERSION_MISMATCH = "version_mismatch"
    NOT_AUTHORITATIVE = "not_authoritative"
    UNSUPPORTED_RESOLUTION = "unsupported_resolution"


@dataclass(frozen=True, slots=True)
class DurableReconciliationCommitResult:
    """Durable outcome; exact retries reproduce the originally committed data."""

    commit_id: str
    account_id: str = field(repr=False)
    disposition: ReconciliationCommitDisposition
    committed_at: datetime | None = None
    resulting_journal_sequence: int | None = None
    resolved_claim_ids: tuple[str, ...] = ()
    resolved_observation_ids: tuple[str, ...] = ()
    resolved_requirement_ids: tuple[int, ...] = ()
    order_projections: tuple[LiveOrder, ...] = ()

    def __post_init__(self) -> None:
        _identifier(self.commit_id, "commit_id")
        _identifier(self.account_id, "account_id")
        if type(self.disposition) is not ReconciliationCommitDisposition:
            raise TypeError("disposition must be ReconciliationCommitDisposition")
        claims = _exact_tuple(self.resolved_claim_ids, str, "resolved_claim_ids")
        observations = _exact_tuple(self.resolved_observation_ids, str, "resolved_observation_ids")
        requirements = _exact_tuple(self.resolved_requirement_ids, int, "resolved_requirement_ids")
        projections = _exact_tuple(self.order_projections, LiveOrder, "order_projections")
        for claim_id in claims:
            _identifier(claim_id, "resolved_claim_id")
        for observation_id in observations:
            _identifier(observation_id, "resolved_observation_id")
        for requirement_id in requirements:
            _positive_int(requirement_id, "resolved_requirement_id")
        target_groups: tuple[tuple[object, ...], ...] = (
            claims,
            observations,
            requirements,
            tuple(item.intent.client_order_id for item in projections),
        )
        if any(len(set(targets)) != len(targets) for targets in target_groups):
            raise ValueError("resolved targets and projected orders must be unique")
        if any(order.intent.account_id != self.account_id for order in projections):
            raise ValueError("projected orders must match result account")

        durable = self.disposition in {
            ReconciliationCommitDisposition.COMMITTED,
            ReconciliationCommitDisposition.EXACT_RETRY,
        }
        if durable:
            if self.committed_at is None or self.resulting_journal_sequence is None:
                raise ValueError("durable outcomes require committed_at and journal sequence")
            _utc(self.committed_at, "committed_at")
            _nonnegative_int(self.resulting_journal_sequence, "resulting_journal_sequence")
        elif (
            self.committed_at is not None
            or self.resulting_journal_sequence is not None
            or claims
            or observations
            or requirements
            or projections
        ):
            raise ValueError("rejected outcomes must not contain committed data")


@runtime_checkable
class DurableReconciliationCommitPort(Protocol):
    def commit_reconciliation(
        self, request: DurableReconciliationCommitRequest
    ) -> DurableReconciliationCommitResult: ...
