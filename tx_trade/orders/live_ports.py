"""Side-effect-free ports for Phase 3A live-order orchestration.

The interfaces in this module are broker and persistence agnostic.  In
particular, a successful dispatch receipt is only evidence that a transport
call completed; broker acceptance must arrive through a normalized reply or a
complete broker query.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
import re
from typing import Protocol, TypeAlias, runtime_checkable

from .live_contracts import (
    AccountSnapshot,
    AmendOrderCommand,
    BrokerFillObservation,
    BrokerOpenOrderObservation,
    BrokerPosition,
    CancelOrderCommand,
    DecreaseOrderCommand,
    DispatchReceipt,
    LiveCommand,
    LiveFailure,
    LiveFailureCode,
    LiveOrder,
    NewOrderCommand,
    NormalizedBrokerFillEvent,
    NormalizedBrokerOrderEvent,
    ReadinessSnapshot,
    ReconciliationDiscrepancy,
)

NormalizedLiveOrderEvent: TypeAlias = NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent
OrderServiceResult: TypeAlias = LiveOrder | LiveFailure
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_FINGERPRINT = re.compile(r"^sha256:[0-9a-f]{64}$")


def _require_nonempty(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not value:
        raise ValueError(f"{name} must not be empty")


def _require_identifier(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be a bounded ASCII identifier")


def _require_fingerprint(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _FINGERPRINT.fullmatch(value):
        raise ValueError(f"{name} must be a canonical SHA-256 fingerprint")


def _require_positive(value: object, name: str) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if value < 1:
        raise ValueError(f"{name} must be positive")


def _require_utc(value: object, name: str) -> None:
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware UTC")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


class EvidenceCompleteness(StrEnum):
    """Whether a broker result is safe to treat as an exhaustive snapshot."""

    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    UNKNOWN = "unknown"


class EvidenceQueryKind(StrEnum):
    OPEN_ORDERS = "open_orders"
    FILLS = "fills"
    POSITIONS = "positions"
    REPLY_BACKFILL = "reply_backfill"


@dataclass(frozen=True, slots=True)
class CompletenessEvidence:
    """Evidence attached to every broker snapshot and reply-stream read."""

    query_kind: EvidenceQueryKind
    account_id: str = field(repr=False)
    status: EvidenceCompleteness
    observed_at: datetime
    source_cursor: str
    reason: str | None = None

    def __post_init__(self) -> None:
        if type(self.query_kind) is not EvidenceQueryKind:
            raise TypeError("query_kind must be EvidenceQueryKind")
        _require_identifier(self.account_id, "account_id")
        if type(self.status) is not EvidenceCompleteness:
            raise TypeError("status must be EvidenceCompleteness")
        _require_utc(self.observed_at, "observed_at")
        _require_nonempty(self.source_cursor, "source_cursor")
        if self.reason is not None:
            _require_nonempty(self.reason, "reason")
        if (self.status is EvidenceCompleteness.COMPLETE) == (self.reason is not None):
            raise ValueError("reason is required exactly when evidence is not complete")

    @property
    def permits_absence_inference(self) -> bool:
        """Only complete evidence may prove that an item is absent."""

        return self.status is EvidenceCompleteness.COMPLETE


@dataclass(frozen=True, slots=True)
class RawBrokerObservation:
    """Opaque callback evidence captured before parsing or correlation."""

    observation_id: str
    source: str
    broker_session_generation: int
    adapter_received_sequence: int
    received_at: datetime
    payload: bytes = field(repr=False)

    def __post_init__(self) -> None:
        _require_identifier(self.observation_id, "observation_id")
        _require_identifier(self.source, "source")
        _require_positive(self.broker_session_generation, "broker_session_generation")
        _require_positive(self.adapter_received_sequence, "adapter_received_sequence")
        _require_utc(self.received_at, "received_at")
        if type(self.payload) is not bytes:
            raise TypeError("payload must be bytes")
        if not self.payload:
            raise ValueError("payload must not be empty")


@dataclass(frozen=True, slots=True)
class BrokerReplyBatch:
    observations: tuple[RawBrokerObservation, ...]
    evidence: CompletenessEvidence

    def __post_init__(self) -> None:
        if type(self.observations) is not tuple:
            raise TypeError("observations must be a tuple")
        if any(type(item) is not RawBrokerObservation for item in self.observations):
            raise TypeError("observations must contain RawBrokerObservation values")
        if type(self.evidence) is not CompletenessEvidence:
            raise TypeError("evidence must be CompletenessEvidence")
        if self.evidence.query_kind is not EvidenceQueryKind.REPLY_BACKFILL:
            raise ValueError("reply batch evidence must cover REPLY_BACKFILL")
        if any(item.received_at > self.evidence.observed_at for item in self.observations):
            raise ValueError("reply observations must not postdate completeness evidence")


@dataclass(frozen=True, slots=True)
class OpenOrdersSnapshot:
    orders: tuple[BrokerOpenOrderObservation, ...]
    evidence: CompletenessEvidence

    def __post_init__(self) -> None:
        _validate_snapshot(
            self.orders,
            BrokerOpenOrderObservation,
            self.evidence,
            EvidenceQueryKind.OPEN_ORDERS,
        )


@dataclass(frozen=True, slots=True)
class BrokerFillsSnapshot:
    fills: tuple[BrokerFillObservation, ...]
    evidence: CompletenessEvidence

    def __post_init__(self) -> None:
        _validate_snapshot(
            self.fills,
            BrokerFillObservation,
            self.evidence,
            EvidenceQueryKind.FILLS,
        )


@dataclass(frozen=True, slots=True)
class BrokerPositionsSnapshot:
    positions: tuple[BrokerPosition, ...]
    evidence: CompletenessEvidence

    def __post_init__(self) -> None:
        _validate_snapshot(
            self.positions,
            BrokerPosition,
            self.evidence,
            EvidenceQueryKind.POSITIONS,
        )


def _validate_snapshot(
    values: tuple[object, ...],
    expected_type: type[object],
    evidence: object,
    expected_kind: EvidenceQueryKind,
) -> None:
    if type(values) is not tuple:
        raise TypeError("snapshot values must be a tuple")
    if any(type(item) is not expected_type for item in values):
        raise TypeError(f"snapshot values must contain {expected_type.__name__} values")
    if type(evidence) is not CompletenessEvidence:
        raise TypeError("evidence must be CompletenessEvidence")
    if evidence.query_kind is not expected_kind:
        raise ValueError(f"evidence must cover {expected_kind.name}")
    for item in values:
        if getattr(item, "account_id") != evidence.account_id:
            raise ValueError("snapshot observations must match evidence account")
        item_observed_at = getattr(item, "observed_at")
        if item_observed_at > evidence.observed_at:
            raise ValueError("snapshot observations must not postdate evidence")


class ReservationDisposition(StrEnum):
    RESERVED = "reserved"
    EXACT_RETRY = "exact_retry"
    PAYLOAD_CONFLICT = "payload_conflict"


@dataclass(frozen=True, slots=True)
class ClientOrderIdReservation:
    """Result of atomically reserving a globally unique client order id."""

    client_order_id: str
    incoming_fingerprint: str
    recorded_fingerprint: str | None
    disposition: ReservationDisposition

    def __post_init__(self) -> None:
        _require_identifier(self.client_order_id, "client_order_id")
        _require_fingerprint(self.incoming_fingerprint, "incoming_fingerprint")
        if self.recorded_fingerprint is not None:
            _require_fingerprint(self.recorded_fingerprint, "recorded_fingerprint")
        if type(self.disposition) is not ReservationDisposition:
            raise TypeError("disposition must be ReservationDisposition")
        if self.disposition is ReservationDisposition.RESERVED:
            if self.recorded_fingerprint is not None:
                raise ValueError("RESERVED must not have a recorded fingerprint")
            return
        if self.recorded_fingerprint is None:
            raise ValueError("retry and conflict results require a recorded fingerprint")
        fingerprints_match = self.incoming_fingerprint == self.recorded_fingerprint
        if fingerprints_match != (self.disposition is ReservationDisposition.EXACT_RETRY):
            raise ValueError("disposition must match fingerprint equality")

    @property
    def may_continue(self) -> bool:
        return self.disposition is not ReservationDisposition.PAYLOAD_CONFLICT


class DispatchClaimDisposition(StrEnum):
    ACQUIRED = "acquired"
    ALREADY_CLAIMED = "already_claimed"
    VERSION_MISMATCH = "version_mismatch"
    PAYLOAD_CONFLICT = "payload_conflict"


@dataclass(frozen=True, slots=True)
class DispatchClaim:
    """Compare-and-swap result fencing concurrent or stale dispatchers."""

    client_command_id: str
    payload_fingerprint: str
    disposition: DispatchClaimDisposition
    version: int
    claim_token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        _require_identifier(self.client_command_id, "client_command_id")
        _require_fingerprint(self.payload_fingerprint, "payload_fingerprint")
        if type(self.disposition) is not DispatchClaimDisposition:
            raise TypeError("disposition must be DispatchClaimDisposition")
        _require_positive(self.version, "version")
        if self.claim_token is not None:
            _require_identifier(self.claim_token, "claim_token")
        if (self.disposition is DispatchClaimDisposition.ACQUIRED) != (
            self.claim_token is not None
        ):
            raise ValueError("claim_token is required exactly for an acquired claim")

    @property
    def acquired(self) -> bool:
        return self.disposition is DispatchClaimDisposition.ACQUIRED


class JournalAppendDisposition(StrEnum):
    APPENDED = "appended"
    EXACT_DUPLICATE = "exact_duplicate"
    ID_CONFLICT = "id_conflict"


@dataclass(frozen=True, slots=True)
class JournalAppendResult:
    observation_id: str
    disposition: JournalAppendDisposition

    def __post_init__(self) -> None:
        _require_identifier(self.observation_id, "observation_id")
        if type(self.disposition) is not JournalAppendDisposition:
            raise TypeError("disposition must be JournalAppendDisposition")


class EventApplicationDisposition(StrEnum):
    APPLIED = "applied"
    EXACT_DUPLICATE = "exact_duplicate"
    EVENT_CONFLICT = "event_conflict"
    UNRESOLVED = "unresolved"
    AMBIGUOUS = "ambiguous"
    VERSION_MISMATCH = "version_mismatch"


@dataclass(frozen=True, slots=True)
class EventApplicationResult:
    event_id: str
    disposition: EventApplicationDisposition
    order: LiveOrder | None
    failure_code: LiveFailureCode | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.event_id, "event_id")
        if type(self.disposition) is not EventApplicationDisposition:
            raise TypeError("disposition must be EventApplicationDisposition")
        if self.order is not None and type(self.order) is not LiveOrder:
            raise TypeError("order must be LiveOrder or None")
        if (self.disposition is EventApplicationDisposition.APPLIED) != (self.order is not None):
            raise ValueError("order is required exactly when an event is applied")
        if self.failure_code is not None and type(self.failure_code) is not LiveFailureCode:
            raise TypeError("failure_code must be LiveFailureCode or None")
        expected_failure = (
            LiveFailureCode.CORRELATION_CONFLICT
            if self.disposition is EventApplicationDisposition.EVENT_CONFLICT
            else None
        )
        if self.failure_code is not expected_failure:
            raise ValueError("CORRELATION_CONFLICT is required exactly for an event conflict")

    @property
    def requires_reconciliation(self) -> bool:
        return self.disposition is EventApplicationDisposition.EVENT_CONFLICT


@dataclass(frozen=True, slots=True)
class AmbiguousObservation:
    observation: RawBrokerObservation
    candidate_client_order_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.observation) is not RawBrokerObservation:
            raise TypeError("observation must be RawBrokerObservation")
        if type(self.candidate_client_order_ids) is not tuple:
            raise TypeError("candidate_client_order_ids must be a tuple")
        for candidate in self.candidate_client_order_ids:
            _require_identifier(candidate, "candidate_client_order_id")
        if len(self.candidate_client_order_ids) < 2:
            raise ValueError("ambiguous observation requires at least two candidates")
        if len(set(self.candidate_client_order_ids)) != len(self.candidate_client_order_ids):
            raise ValueError("candidate client order ids must be unique")


class ReconciliationStatus(StrEnum):
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    account_id: str = field(repr=False)
    status: ReconciliationStatus
    discrepancies: tuple[ReconciliationDiscrepancy, ...]
    evidence: tuple[CompletenessEvidence, ...]
    reconciled_at: datetime

    def __post_init__(self) -> None:
        _require_identifier(self.account_id, "account_id")
        if type(self.status) is not ReconciliationStatus:
            raise TypeError("status must be ReconciliationStatus")
        if type(self.discrepancies) is not tuple:
            raise TypeError("discrepancies must be a tuple")
        if any(type(item) is not ReconciliationDiscrepancy for item in self.discrepancies):
            raise TypeError("discrepancies must contain ReconciliationDiscrepancy values")
        if any(item.account_id != self.account_id for item in self.discrepancies):
            raise ValueError("discrepancies must match reconciliation account")
        if type(self.evidence) is not tuple:
            raise TypeError("evidence must be a tuple")
        if any(type(item) is not CompletenessEvidence for item in self.evidence):
            raise TypeError("evidence must contain CompletenessEvidence values")
        _require_utc(self.reconciled_at, "reconciled_at")
        if any(item.account_id != self.account_id for item in self.evidence):
            raise ValueError("evidence must match reconciliation account")
        if any(item.observed_at > self.reconciled_at for item in self.evidence):
            raise ValueError("evidence must not postdate reconciliation")
        kinds = tuple(item.query_kind for item in self.evidence)
        if len(set(kinds)) != len(kinds):
            raise ValueError("reconciliation evidence kinds must be unique")
        required = {
            EvidenceQueryKind.OPEN_ORDERS,
            EvidenceQueryKind.FILLS,
            EvidenceQueryKind.POSITIONS,
        }
        if self.status is ReconciliationStatus.COMPLETE:
            if not required.issubset(kinds):
                raise ValueError("complete reconciliation requires all broker query evidence")
            if any(item.status is not EvidenceCompleteness.COMPLETE for item in self.evidence):
                raise ValueError("complete reconciliation requires complete evidence")

    @property
    def is_authoritative(self) -> bool:
        if self.status is not ReconciliationStatus.COMPLETE or not self.evidence:
            return False
        required = {
            EvidenceQueryKind.OPEN_ORDERS,
            EvidenceQueryKind.FILLS,
            EvidenceQueryKind.POSITIONS,
        }
        return required.issubset(item.query_kind for item in self.evidence) and all(
            item.permits_absence_inference for item in self.evidence
        )


@runtime_checkable
class AccountCatalogPort(Protocol):
    def list_accounts(self) -> tuple[AccountSnapshot, ...]: ...

    def get_readiness(self, account_id: str) -> ReadinessSnapshot: ...


@runtime_checkable
class LiveOrderDispatchPort(Protocol):
    def dispatch(self, command: LiveCommand) -> DispatchReceipt: ...


@runtime_checkable
class BrokerReplySourcePort(Protocol):
    def read_replies(
        self,
        *,
        after_cursor: str | None = None,
        limit: int = 100,
    ) -> BrokerReplyBatch: ...


@runtime_checkable
class BrokerOrderQueryPort(Protocol):
    def query_open_orders(self, account_id: str) -> OpenOrdersSnapshot: ...

    def query_fills(self, account_id: str) -> BrokerFillsSnapshot: ...

    def query_positions(self, account_id: str) -> BrokerPositionsSnapshot: ...


@runtime_checkable
class OrderJournalPort(Protocol):
    def reserve_client_order_id(
        self,
        client_order_id: str,
        payload_fingerprint: str,
    ) -> ClientOrderIdReservation: ...

    def claim_dispatch(
        self,
        client_command_id: str,
        payload_fingerprint: str,
        *,
        expected_version: int,
        claimant_id: str,
    ) -> DispatchClaim: ...

    def record_dispatch_receipt(
        self,
        receipt: DispatchReceipt,
        *,
        claim_token: str,
        expected_version: int,
    ) -> bool: ...

    def append_raw_observation(
        self,
        observation: RawBrokerObservation,
    ) -> JournalAppendResult: ...

    def apply_normalized_event(
        self,
        event: NormalizedLiveOrderEvent,
        *,
        expected_order_version: int | None,
    ) -> EventApplicationResult: ...

    def load_unresolved_observations(self) -> tuple[RawBrokerObservation, ...]: ...

    def load_ambiguous_observations(self) -> tuple[AmbiguousObservation, ...]: ...


@runtime_checkable
class OrderServicePort(Protocol):
    def submit(self, command: NewOrderCommand) -> OrderServiceResult: ...

    def cancel(self, command: CancelOrderCommand) -> OrderServiceResult: ...

    def amend(self, command: AmendOrderCommand) -> OrderServiceResult: ...

    def decrease(self, command: DecreaseOrderCommand) -> OrderServiceResult: ...

    def get_order(self, client_order_id: str) -> LiveOrder | None: ...


@runtime_checkable
class ReconciliationPort(Protocol):
    def reconcile(self, account_id: str) -> ReconciliationResult: ...


@runtime_checkable
class LiveOrderEventSink(Protocol):
    def publish(self, event: NormalizedLiveOrderEvent) -> None: ...
