"""Durable live-order journal contracts.

These contracts define persistence and recovery semantics only.  Importing
this module performs no filesystem, SQLite, environment, network, or broker
operation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from hashlib import sha256
import re
from typing import Protocol, runtime_checkable

from .live_contracts import (
    DispatchReceipt,
    LiveCommand,
    LiveOrder,
    LiveOrderIntent,
    NormalizedBrokerFillEvent,
    NormalizedBrokerOrderEvent,
    canonical_bytes,
)
from .live_ports import (
    AmbiguousObservation,
    DispatchClaim,
    EventApplicationResult,
    JournalAppendResult,
    RawBrokerObservation,
)
from .live_state_machine import AppliedEventLedger

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_FINGERPRINT = re.compile(r"^sha256:[0-9a-f]{64}$")
_INTENT_FINGERPRINT_DOMAIN = b"tx_trade.live.order.intent.v1"


def _identifier(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{name} must be a bounded ASCII identifier")


def _fingerprint(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not _FINGERPRINT.fullmatch(value):
        raise ValueError(f"{name} must be a canonical SHA-256 fingerprint")


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


def intent_fingerprint(intent: LiveOrderIntent) -> str:
    """Fingerprint immutable order identity independently of command retries."""

    if type(intent) is not LiveOrderIntent:
        raise TypeError("intent must be LiveOrderIntent")
    digest = sha256(_INTENT_FINGERPRINT_DOMAIN + b"\x00" + canonical_bytes(intent)).hexdigest()
    return f"sha256:{digest}"


class JournalOpenMode(StrEnum):
    CREATE_NEW = "create_new"
    RESUME = "resume"


@dataclass(frozen=True, slots=True)
class LiveJournalIdentity:
    journal_id: str
    schema_version: int
    schema_fingerprint: str
    created_at: datetime

    def __post_init__(self) -> None:
        _identifier(self.journal_id, "journal_id")
        _positive_int(self.schema_version, "schema_version")
        _fingerprint(self.schema_fingerprint, "schema_fingerprint")
        _utc(self.created_at, "created_at")


class RegistrationDisposition(StrEnum):
    REGISTERED = "registered"
    EXACT_RETRY = "exact_retry"
    ID_CONFLICT = "id_conflict"
    VERSION_MISMATCH = "version_mismatch"


@dataclass(frozen=True, slots=True)
class CommandRegistrationResult:
    client_command_id: str = field(repr=False)
    disposition: RegistrationDisposition
    order: LiveOrder | None

    def __post_init__(self) -> None:
        _identifier(self.client_command_id, "client_command_id")
        if type(self.disposition) is not RegistrationDisposition:
            raise TypeError("disposition must be RegistrationDisposition")
        if self.order is not None and type(self.order) is not LiveOrder:
            raise TypeError("order must be LiveOrder or None")
        if (self.disposition is RegistrationDisposition.REGISTERED) != (self.order is not None):
            raise ValueError("order is required exactly when a command is registered")


class ReceiptRecordDisposition(StrEnum):
    RECORDED = "recorded"
    EXACT_RETRY = "exact_retry"
    VERSION_MISMATCH = "version_mismatch"
    TOKEN_MISMATCH = "token_mismatch"
    ID_CONFLICT = "id_conflict"


@dataclass(frozen=True, slots=True)
class DispatchReceiptRecordResult:
    client_command_id: str = field(repr=False)
    disposition: ReceiptRecordDisposition
    order: LiveOrder | None

    def __post_init__(self) -> None:
        _identifier(self.client_command_id, "client_command_id")
        if type(self.disposition) is not ReceiptRecordDisposition:
            raise TypeError("disposition must be ReceiptRecordDisposition")
        if self.order is not None and type(self.order) is not LiveOrder:
            raise TypeError("order must be LiveOrder or None")
        if (self.disposition is ReceiptRecordDisposition.RECORDED) != (self.order is not None):
            raise ValueError("order is required exactly when a receipt is recorded")


@dataclass(frozen=True, slots=True)
class OutstandingDispatchClaim:
    command: LiveCommand = field(repr=False)
    claim_token: str = field(repr=False)
    claimant_id: str
    expected_order_version: int
    claimed_at: datetime

    def __post_init__(self) -> None:
        if type(self.command) not in LiveCommand.__args__:
            raise TypeError("command must be an exact live command")
        _identifier(self.claim_token, "claim_token")
        _identifier(self.claimant_id, "claimant_id")
        _positive_int(self.expected_order_version, "expected_order_version")
        _utc(self.claimed_at, "claimed_at")


@dataclass(frozen=True, slots=True)
class DurableReconciliationRequirement:
    requirement_id: int
    reason_code: str
    created_at: datetime
    client_order_id: str | None = field(default=None, repr=False)
    observation_id: str | None = None

    def __post_init__(self) -> None:
        _positive_int(self.requirement_id, "requirement_id")
        _identifier(self.reason_code, "reason_code")
        if self.client_order_id is not None:
            _identifier(self.client_order_id, "client_order_id")
        if self.observation_id is not None:
            _identifier(self.observation_id, "observation_id")
        if self.client_order_id is None and self.observation_id is None:
            raise ValueError("a reconciliation requirement must identify durable evidence")
        _utc(self.created_at, "created_at")


@dataclass(frozen=True, slots=True)
class LiveJournalRecoverySnapshot:
    identity: LiveJournalIdentity
    orders: tuple[LiveOrder, ...]
    outstanding_claims: tuple[OutstandingDispatchClaim, ...]
    unresolved_observations: tuple[RawBrokerObservation, ...]
    conflict_observations: tuple[RawBrokerObservation, ...]
    ambiguous_observations: tuple[AmbiguousObservation, ...]
    reconciliation_requirements: tuple[DurableReconciliationRequirement, ...]
    applied_event_ledger: AppliedEventLedger
    journal_sequence: int

    def __post_init__(self) -> None:
        if type(self.identity) is not LiveJournalIdentity:
            raise TypeError("identity must be LiveJournalIdentity")
        for values, expected, name in (
            (self.orders, LiveOrder, "orders"),
            (self.outstanding_claims, OutstandingDispatchClaim, "outstanding_claims"),
            (self.unresolved_observations, RawBrokerObservation, "unresolved_observations"),
            (self.conflict_observations, RawBrokerObservation, "conflict_observations"),
            (self.ambiguous_observations, AmbiguousObservation, "ambiguous_observations"),
            (
                self.reconciliation_requirements,
                DurableReconciliationRequirement,
                "reconciliation_requirements",
            ),
        ):
            if type(values) is not tuple:
                raise TypeError(f"{name} must be a tuple")
            if any(type(item) is not expected for item in values):
                raise TypeError(f"{name} contains an invalid value")
        if type(self.applied_event_ledger) is not AppliedEventLedger:
            raise TypeError("applied_event_ledger must be AppliedEventLedger")
        _positive_int(self.journal_sequence, "journal_sequence")
        order_ids = tuple(order.intent.client_order_id for order in self.orders)
        if len(set(order_ids)) != len(order_ids):
            raise ValueError("recovery orders must have unique client_order_id values")


class LiveJournalError(RuntimeError):
    """Base class with intentionally non-sensitive public messages."""


class LiveJournalIntegrityError(LiveJournalError):
    pass


class LiveJournalConflictError(LiveJournalError):
    pass


class LiveJournalCapacityError(LiveJournalError):
    pass


class LiveJournalClosedError(LiveJournalError):
    pass


@runtime_checkable
class DurableOrderJournalPort(Protocol):
    def register_new_order(
        self,
        command: LiveCommand,
        order: LiveOrder,
        *,
        intent_fingerprint: str,
    ) -> CommandRegistrationResult: ...

    def register_command(
        self,
        command: LiveCommand,
        order: LiveOrder,
        *,
        expected_order_version: int,
    ) -> CommandRegistrationResult: ...

    def claim_dispatch(
        self,
        client_command_id: str,
        payload_fingerprint: str,
        *,
        expected_order_version: int,
        claimant_id: str,
    ) -> DispatchClaim: ...

    def record_dispatch_receipt(
        self,
        receipt: DispatchReceipt,
        *,
        claim_token: str,
        expected_order_version: int,
    ) -> DispatchReceiptRecordResult: ...

    def append_raw_observation(
        self,
        observation: RawBrokerObservation,
    ) -> JournalAppendResult: ...

    def apply_normalized_event(
        self,
        event: NormalizedBrokerOrderEvent | NormalizedBrokerFillEvent,
        *,
        raw_observation_id: str,
        expected_order_version: int | None,
    ) -> EventApplicationResult: ...

    def get_order(self, client_order_id: str) -> LiveOrder | None: ...

    def list_active_orders(self, account_id: str | None = None) -> tuple[LiveOrder, ...]: ...

    def load_recovery_snapshot(self) -> LiveJournalRecoverySnapshot: ...

    def close(self) -> None: ...


__all__ = [
    "CommandRegistrationResult",
    "DispatchReceiptRecordResult",
    "DurableReconciliationRequirement",
    "DurableOrderJournalPort",
    "JournalOpenMode",
    "LiveJournalCapacityError",
    "LiveJournalClosedError",
    "LiveJournalConflictError",
    "LiveJournalError",
    "LiveJournalIdentity",
    "LiveJournalIntegrityError",
    "LiveJournalRecoverySnapshot",
    "OutstandingDispatchClaim",
    "ReceiptRecordDisposition",
    "RegistrationDisposition",
    "intent_fingerprint",
]
