"""Pure verification and classification for live-journal recovery.

This module deliberately performs no persistence or broker operation.  It
turns a hydrated recovery snapshot into a fail-closed, deterministic decision;
in particular, an outstanding dispatch claim is never evidence that retrying
the command is safe.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
import re

from .live_contracts import LiveCommand, LiveOrder, NewOrderCommand
from .live_journal_codec import (
    LiveJournalCodecError,
    decode_journal_value,
    encode_journal_value,
    journal_digest,
)
from .live_journal_contracts import (
    DurableReconciliationRequirement,
    LiveJournalIdentity,
    LiveJournalRecoverySnapshot,
    OutstandingDispatchClaim,
)
from .live_ports import AmbiguousObservation, RawBrokerObservation
from .live_state_machine import AppliedEvent, AppliedEventLedger

_FINGERPRINT = re.compile(r"^sha256:[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_PROJECTION_DOMAIN = "tx_trade.live.order.projection.v1"


class RecoveryReadiness(StrEnum):
    """Whether orchestration may proceed after hydrating a journal."""

    BLOCKED = "blocked"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    READY = "ready"


class PendingRecoveryKind(StrEnum):
    """Conservative classification of a durable pending command."""

    REGISTERED_AWAITING_BROKER_EVIDENCE = "registered_awaiting_broker_evidence"
    CLAIMED_OUTCOME_UNKNOWN = "claimed_outcome_unknown"


class RecoveryIssueCode(StrEnum):
    """Non-sensitive reasons for a fail-closed recovery result."""

    PROJECTION_INVALID = "projection_invalid"
    DUPLICATE_ORDER_ID = "duplicate_order_id"
    DUPLICATE_COMMAND_ID = "duplicate_command_id"
    DUPLICATE_CLAIM_TOKEN = "duplicate_claim_token"
    COMMAND_ORDER_MISMATCH = "command_order_mismatch"
    CLAIM_ORDER_MISMATCH = "claim_order_mismatch"
    CLAIM_COMMAND_MISMATCH = "claim_command_mismatch"
    CLAIM_VERSION_MISMATCH = "claim_version_mismatch"
    DUPLICATE_EVENT_IDENTITY = "duplicate_event_identity"
    INVALID_EVENT_FINGERPRINT = "invalid_event_fingerprint"
    DUPLICATE_OBSERVATION_ID = "duplicate_observation_id"
    DUPLICATE_OBSERVATION_SEQUENCE = "duplicate_observation_sequence"
    OBSERVATION_RESOLUTION_CONFLICT = "observation_resolution_conflict"
    UNKNOWN_AMBIGUITY_CANDIDATE = "unknown_ambiguity_candidate"
    DUPLICATE_REQUIREMENT_ID = "duplicate_requirement_id"
    REQUIREMENT_ORDER_MISMATCH = "requirement_order_mismatch"
    REQUIREMENT_OBSERVATION_MISMATCH = "requirement_observation_mismatch"
    OUTSTANDING_DISPATCH = "outstanding_dispatch"
    PENDING_BROKER_EVIDENCE = "pending_broker_evidence"
    UNRESOLVED_OBSERVATION = "unresolved_observation"
    CONFLICT_OBSERVATION = "conflict_observation"
    AMBIGUOUS_OBSERVATION = "ambiguous_observation"
    DURABLE_RECONCILIATION_REQUIREMENT = "durable_reconciliation_requirement"


_BLOCKING_ISSUES = frozenset(
    {
        RecoveryIssueCode.PROJECTION_INVALID,
        RecoveryIssueCode.DUPLICATE_ORDER_ID,
        RecoveryIssueCode.DUPLICATE_COMMAND_ID,
        RecoveryIssueCode.DUPLICATE_CLAIM_TOKEN,
        RecoveryIssueCode.COMMAND_ORDER_MISMATCH,
        RecoveryIssueCode.CLAIM_ORDER_MISMATCH,
        RecoveryIssueCode.CLAIM_COMMAND_MISMATCH,
        RecoveryIssueCode.CLAIM_VERSION_MISMATCH,
        RecoveryIssueCode.DUPLICATE_EVENT_IDENTITY,
        RecoveryIssueCode.INVALID_EVENT_FINGERPRINT,
        RecoveryIssueCode.DUPLICATE_OBSERVATION_ID,
        RecoveryIssueCode.DUPLICATE_OBSERVATION_SEQUENCE,
        RecoveryIssueCode.OBSERVATION_RESOLUTION_CONFLICT,
        RecoveryIssueCode.UNKNOWN_AMBIGUITY_CANDIDATE,
        RecoveryIssueCode.DUPLICATE_REQUIREMENT_ID,
        RecoveryIssueCode.REQUIREMENT_ORDER_MISMATCH,
        RecoveryIssueCode.REQUIREMENT_OBSERVATION_MISMATCH,
    }
)


@dataclass(frozen=True, slots=True)
class PendingRecovery:
    """A pending command requiring explicit orchestration after recovery."""

    client_order_id: str = field(repr=False)
    client_command_id: str = field(repr=False)
    kind: PendingRecoveryKind

    @property
    def may_redispatch(self) -> bool:
        """Recovery verification never authorizes a broker side effect."""

        return False


@dataclass(frozen=True, slots=True)
class ProjectionVerification:
    """Canonical digest of one successfully round-tripped order projection."""

    client_order_id: str = field(repr=False)
    order_version: int
    fingerprint: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class RecoveryVerification:
    """Deterministic, immutable output of recovery verification."""

    readiness: RecoveryReadiness
    journal_sequence: int
    pending: tuple[PendingRecovery, ...]
    issues: tuple[RecoveryIssueCode, ...]
    projections: tuple[ProjectionVerification, ...]
    conflict_observation_ids: tuple[str, ...] = field(repr=False)
    reconciliation_requirement_ids: tuple[int, ...]

    @property
    def may_dispatch(self) -> bool:
        """A verifier cannot grant dispatch authority."""

        return False


def _command_order_id(command: LiveCommand) -> str:
    if isinstance(command, NewOrderCommand):
        return command.intent.client_order_id
    return command.client_order_id


def _append_once(issues: list[RecoveryIssueCode], issue: RecoveryIssueCode) -> None:
    if issue not in issues:
        issues.append(issue)


def _verify_projection(order: LiveOrder) -> ProjectionVerification | None:
    try:
        payload = encode_journal_value(order)
        decoded = decode_journal_value(payload, LiveOrder)
        if decoded != order or encode_journal_value(decoded) != payload:
            return None
        return ProjectionVerification(
            client_order_id=order.intent.client_order_id,
            order_version=order.version,
            fingerprint=journal_digest(_PROJECTION_DOMAIN, payload),
        )
    except (LiveJournalCodecError, TypeError, ValueError):
        return None


def _round_trips_as_single_record(value: object) -> bool:
    """Validate one durable component without imposing a limit on its aggregate view."""

    try:
        payload = encode_journal_value(value)
        decoded = decode_journal_value(payload, type(value))
        return decoded == value and encode_journal_value(decoded) == payload
    except (LiveJournalCodecError, TypeError, ValueError):
        return False


def _ambiguous_components_are_canonical(ambiguous: AmbiguousObservation) -> bool:
    candidates = ambiguous.candidate_client_order_ids
    return (
        type(ambiguous.observation) is RawBrokerObservation
        and _round_trips_as_single_record(ambiguous.observation)
        and type(candidates) is tuple
        and len(candidates) >= 2
        and all(
            type(candidate) is str and _IDENTIFIER.fullmatch(candidate) for candidate in candidates
        )
        and len(set(candidates)) == len(candidates)
        and candidates == tuple(sorted(candidates))
    )


def _snapshot_components_are_canonical(snapshot: LiveJournalRecoverySnapshot) -> bool:
    collections: tuple[tuple[object, type[object]], ...] = (
        (snapshot.orders, LiveOrder),
        (snapshot.outstanding_claims, OutstandingDispatchClaim),
        (snapshot.unresolved_observations, RawBrokerObservation),
        (snapshot.conflict_observations, RawBrokerObservation),
        (snapshot.ambiguous_observations, AmbiguousObservation),
        (snapshot.reconciliation_requirements, DurableReconciliationRequirement),
    )
    if (
        type(snapshot.identity) is not LiveJournalIdentity
        or type(snapshot.applied_event_ledger) is not AppliedEventLedger
        or type(snapshot.applied_event_ledger.events) is not tuple
        or any(type(event) is not AppliedEvent for event in snapshot.applied_event_ledger.events)
        or type(snapshot.journal_sequence) is not int
        or snapshot.journal_sequence < 1
        or any(
            type(values) is not tuple or any(type(item) is not expected for item in values)
            for values, expected in collections
        )
    ):
        return False
    components = (
        snapshot.identity,
        *snapshot.outstanding_claims,
        *snapshot.unresolved_observations,
        *snapshot.conflict_observations,
        *snapshot.reconciliation_requirements,
        *snapshot.applied_event_ledger.events,
    )
    return all(_round_trips_as_single_record(item) for item in components) and all(
        _ambiguous_components_are_canonical(item) for item in snapshot.ambiguous_observations
    )


def verify_recovery_snapshot(snapshot: LiveJournalRecoverySnapshot) -> RecoveryVerification:
    """Validate a hydrated snapshot and classify all recovery work.

    Input tuple order is retained where it represents journal order (the
    applied-event ledger).  Derived collections are sorted by opaque durable
    identifiers so repeated verification is deterministic regardless of SQL
    query-plan ordering.
    """

    if type(snapshot) is not LiveJournalRecoverySnapshot:
        raise TypeError("snapshot must be LiveJournalRecoverySnapshot")

    issues: list[RecoveryIssueCode] = []
    projections: list[ProjectionVerification] = []
    orders_by_id: dict[str, LiveOrder] = {}
    pending_by_command: dict[str, tuple[LiveOrder, LiveCommand]] = {}

    for order in snapshot.orders:
        order_id = order.intent.client_order_id
        if order_id in orders_by_id:
            _append_once(issues, RecoveryIssueCode.DUPLICATE_ORDER_ID)
        else:
            orders_by_id[order_id] = order
        projection = _verify_projection(order)
        if projection is None:
            _append_once(issues, RecoveryIssueCode.PROJECTION_INVALID)
        else:
            projections.append(projection)
        binding = order.pending_command
        if binding is None:
            continue
        command = binding.command
        if _command_order_id(command) != order_id:
            _append_once(issues, RecoveryIssueCode.COMMAND_ORDER_MISMATCH)
        command_id = command.client_command_id
        if command_id in pending_by_command:
            _append_once(issues, RecoveryIssueCode.DUPLICATE_COMMAND_ID)
        else:
            pending_by_command[command_id] = (order, command)

    claims_by_command: dict[str, OutstandingDispatchClaim] = {}
    claim_tokens: set[str] = set()
    for claim in snapshot.outstanding_claims:
        command_id = claim.command.client_command_id
        if command_id in claims_by_command:
            _append_once(issues, RecoveryIssueCode.DUPLICATE_COMMAND_ID)
        else:
            claims_by_command[command_id] = claim
        if claim.claim_token in claim_tokens:
            _append_once(issues, RecoveryIssueCode.DUPLICATE_CLAIM_TOKEN)
        claim_tokens.add(claim.claim_token)

        order_id = _command_order_id(claim.command)
        mapped_order = orders_by_id.get(order_id)
        if mapped_order is None:
            _append_once(issues, RecoveryIssueCode.CLAIM_ORDER_MISMATCH)
            continue
        pending_entry = pending_by_command.get(command_id)
        if pending_entry is None or pending_entry[0] is not mapped_order:
            _append_once(issues, RecoveryIssueCode.CLAIM_COMMAND_MISMATCH)
        elif pending_entry[1] != claim.command:
            _append_once(issues, RecoveryIssueCode.CLAIM_COMMAND_MISMATCH)
        if claim.expected_order_version != mapped_order.version:
            _append_once(issues, RecoveryIssueCode.CLAIM_VERSION_MISMATCH)

    event_identities: set[tuple[str, str]] = set()
    for event in snapshot.applied_event_ledger.events:
        identity = (event.source, event.event_id)
        if identity in event_identities:
            _append_once(issues, RecoveryIssueCode.DUPLICATE_EVENT_IDENTITY)
        event_identities.add(identity)
        if type(event.fingerprint) is not str or not _FINGERPRINT.fullmatch(event.fingerprint):
            _append_once(issues, RecoveryIssueCode.INVALID_EVENT_FINGERPRINT)

    unresolved_ids: set[str] = set()
    observation_sequences: set[tuple[str, int, int]] = set()
    for observation in snapshot.unresolved_observations:
        if observation.observation_id in unresolved_ids:
            _append_once(issues, RecoveryIssueCode.DUPLICATE_OBSERVATION_ID)
        unresolved_ids.add(observation.observation_id)
        sequence = (
            observation.source,
            observation.broker_session_generation,
            observation.adapter_received_sequence,
        )
        if sequence in observation_sequences:
            _append_once(issues, RecoveryIssueCode.DUPLICATE_OBSERVATION_SEQUENCE)
        observation_sequences.add(sequence)

    conflict_ids: set[str] = set()
    for observation in snapshot.conflict_observations:
        if observation.observation_id in conflict_ids:
            _append_once(issues, RecoveryIssueCode.DUPLICATE_OBSERVATION_ID)
        conflict_ids.add(observation.observation_id)
        if observation.observation_id in unresolved_ids:
            _append_once(issues, RecoveryIssueCode.OBSERVATION_RESOLUTION_CONFLICT)
        sequence = (
            observation.source,
            observation.broker_session_generation,
            observation.adapter_received_sequence,
        )
        if sequence in observation_sequences:
            _append_once(issues, RecoveryIssueCode.DUPLICATE_OBSERVATION_SEQUENCE)
        observation_sequences.add(sequence)

    ambiguous_ids: set[str] = set()
    for ambiguous in snapshot.ambiguous_observations:
        observation = ambiguous.observation
        if observation.observation_id in ambiguous_ids:
            _append_once(issues, RecoveryIssueCode.DUPLICATE_OBSERVATION_ID)
        ambiguous_ids.add(observation.observation_id)
        if (
            observation.observation_id in unresolved_ids
            or observation.observation_id in conflict_ids
        ):
            _append_once(issues, RecoveryIssueCode.OBSERVATION_RESOLUTION_CONFLICT)
        sequence = (
            observation.source,
            observation.broker_session_generation,
            observation.adapter_received_sequence,
        )
        if sequence in observation_sequences:
            _append_once(issues, RecoveryIssueCode.DUPLICATE_OBSERVATION_SEQUENCE)
        observation_sequences.add(sequence)
        if any(candidate not in orders_by_id for candidate in ambiguous.candidate_client_order_ids):
            _append_once(issues, RecoveryIssueCode.UNKNOWN_AMBIGUITY_CANDIDATE)

    known_observation_ids = unresolved_ids | conflict_ids | ambiguous_ids
    requirement_ids: set[int] = set()
    for requirement in snapshot.reconciliation_requirements:
        if requirement.requirement_id in requirement_ids:
            _append_once(issues, RecoveryIssueCode.DUPLICATE_REQUIREMENT_ID)
        requirement_ids.add(requirement.requirement_id)
        if (
            requirement.client_order_id is not None
            and requirement.client_order_id not in orders_by_id
        ):
            _append_once(issues, RecoveryIssueCode.REQUIREMENT_ORDER_MISMATCH)
        if (
            requirement.observation_id is not None
            and requirement.observation_id not in known_observation_ids
        ):
            _append_once(issues, RecoveryIssueCode.REQUIREMENT_OBSERVATION_MISMATCH)

    # Validate each durable record independently.  The codec's 1 MiB ceiling is
    # a per-record persistence limit, not a limit on this aggregate recovery view.
    if not _snapshot_components_are_canonical(snapshot):
        _append_once(issues, RecoveryIssueCode.PROJECTION_INVALID)

    pending_results: list[PendingRecovery] = []
    for command_id, (order, _) in pending_by_command.items():
        kind = (
            PendingRecoveryKind.CLAIMED_OUTCOME_UNKNOWN
            if command_id in claims_by_command
            else PendingRecoveryKind.REGISTERED_AWAITING_BROKER_EVIDENCE
        )
        pending_results.append(PendingRecovery(order.intent.client_order_id, command_id, kind))

    if snapshot.outstanding_claims:
        _append_once(issues, RecoveryIssueCode.OUTSTANDING_DISPATCH)
    if pending_by_command:
        _append_once(issues, RecoveryIssueCode.PENDING_BROKER_EVIDENCE)
    if snapshot.unresolved_observations:
        _append_once(issues, RecoveryIssueCode.UNRESOLVED_OBSERVATION)
    if snapshot.conflict_observations:
        _append_once(issues, RecoveryIssueCode.CONFLICT_OBSERVATION)
    if snapshot.ambiguous_observations:
        _append_once(issues, RecoveryIssueCode.AMBIGUOUS_OBSERVATION)
    if snapshot.reconciliation_requirements:
        _append_once(issues, RecoveryIssueCode.DURABLE_RECONCILIATION_REQUIREMENT)

    if any(issue in _BLOCKING_ISSUES for issue in issues):
        readiness = RecoveryReadiness.BLOCKED
    elif issues:
        readiness = RecoveryReadiness.RECONCILIATION_REQUIRED
    else:
        readiness = RecoveryReadiness.READY

    return RecoveryVerification(
        readiness=readiness,
        journal_sequence=snapshot.journal_sequence,
        pending=tuple(
            sorted(
                pending_results,
                key=lambda item: (item.client_order_id, item.client_command_id),
            )
        ),
        issues=tuple(sorted(issues, key=lambda issue: issue.value)),
        projections=tuple(
            sorted(
                projections,
                key=lambda item: (item.client_order_id, item.order_version),
            )
        ),
        conflict_observation_ids=tuple(sorted(conflict_ids)),
        reconciliation_requirement_ids=tuple(sorted(requirement_ids)),
    )


__all__ = [
    "PendingRecovery",
    "PendingRecoveryKind",
    "ProjectionVerification",
    "RecoveryIssueCode",
    "RecoveryReadiness",
    "RecoveryVerification",
    "verify_recovery_snapshot",
]
