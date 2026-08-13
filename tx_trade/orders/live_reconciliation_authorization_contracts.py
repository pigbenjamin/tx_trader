"""Pure authorization contracts for one durable reconciliation commit."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import re
from typing import Protocol, runtime_checkable

from .live_reconciliation_commit_contracts import (
    DurableReconciliationCommitRequest,
    DurableReconciliationCommitResult,
)

AUTHORIZATION_CONTRACT_VERSION = 1
MAX_AUTHORIZATION_TTL_SECONDS = 300

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


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


def _utc(value: object, name: str) -> None:
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware UTC")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


class ReconciliationAuthorizationAction(StrEnum):
    RECONCILIATION_COMMIT = "reconciliation_commit"


class ReconciliationAuthorizationFailureCode(StrEnum):
    INVALID_AUTHORIZATION = "invalid_authorization"
    AUTHORIZATION_EXPIRED = "authorization_expired"
    AUTHORIZATION_CONFLICT = "authorization_conflict"


_FAILURE_MESSAGES = {
    ReconciliationAuthorizationFailureCode.INVALID_AUTHORIZATION: (
        "reconciliation authorization is invalid"
    ),
    ReconciliationAuthorizationFailureCode.AUTHORIZATION_EXPIRED: (
        "reconciliation authorization has expired"
    ),
    ReconciliationAuthorizationFailureCode.AUTHORIZATION_CONFLICT: (
        "reconciliation authorization conflicts with the request"
    ),
}


class ReconciliationAuthorizationError(RuntimeError):
    """Authorization failure with stable, deliberately sanitized text."""

    def __init__(self, code: ReconciliationAuthorizationFailureCode) -> None:
        if type(code) is not ReconciliationAuthorizationFailureCode:
            raise TypeError("code must be ReconciliationAuthorizationFailureCode")
        self.code = code
        super().__init__(_FAILURE_MESSAGES[code])


@dataclass(frozen=True, slots=True)
class ReconciliationCommitAuthorization:
    """An attestation authorizing exactly one reconciliation commit request."""

    authorization_id: str
    principal_id: str = field(repr=False)
    authority_context_digest: str = field(repr=False)
    action: ReconciliationAuthorizationAction
    journal_id: str = field(repr=False)
    account_id: str = field(repr=False)
    source_inspection_digest: str = field(repr=False)
    operator_plan_digest: str = field(repr=False)
    commit_id: str
    request_digest: str = field(repr=False)
    broker_snapshot_id: str = field(repr=False)
    expected_journal_sequence: int
    authorized_at: datetime
    expires_at: datetime
    reason_code: str

    def __post_init__(self) -> None:
        for name in (
            "authorization_id",
            "principal_id",
            "journal_id",
            "account_id",
            "commit_id",
            "broker_snapshot_id",
            "reason_code",
        ):
            _identifier(getattr(self, name), name)
        for name in (
            "authority_context_digest",
            "source_inspection_digest",
            "operator_plan_digest",
            "request_digest",
        ):
            _digest(getattr(self, name), name)
        if type(self.action) is not ReconciliationAuthorizationAction:
            raise TypeError("action must be ReconciliationAuthorizationAction")
        _nonnegative_int(self.expected_journal_sequence, "expected_journal_sequence")
        _utc(self.authorized_at, "authorized_at")
        _utc(self.expires_at, "expires_at")
        ttl = self.expires_at - self.authorized_at
        if ttl <= timedelta(0):
            raise ValueError("authorization TTL must be positive")
        if ttl > timedelta(seconds=MAX_AUTHORIZATION_TTL_SECONDS):
            raise ValueError("authorization TTL must not exceed maximum")

    @property
    def may_dispatch(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class AuthorizedReconciliationCommitRequest:
    authorization: ReconciliationCommitAuthorization = field(repr=False)
    request: DurableReconciliationCommitRequest = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.authorization) is not ReconciliationCommitAuthorization:
            raise TypeError("authorization must be ReconciliationCommitAuthorization")
        if type(self.request) is not DurableReconciliationCommitRequest:
            raise TypeError("request must be DurableReconciliationCommitRequest")
        authorization = self.authorization
        request = self.request
        if authorization.action is not ReconciliationAuthorizationAction.RECONCILIATION_COMMIT:
            raise ValueError("authorization action must permit reconciliation commit")
        if authorization.account_id != request.account_id:
            raise ValueError("authorization account must match request account")
        if authorization.commit_id != request.commit_id:
            raise ValueError("authorization commit_id must match request commit_id")
        if authorization.expected_journal_sequence != request.expected_journal_sequence:
            raise ValueError("authorization journal sequence must match request")
        if authorization.broker_snapshot_id != request.assessment.broker_snapshot.snapshot_id:
            raise ValueError("authorization broker snapshot must match request")

    @property
    def may_dispatch(self) -> bool:
        return False


@runtime_checkable
class AuthorizedReconciliationCommitPort(Protocol):
    def commit_authorized_reconciliation(
        self, request: AuthorizedReconciliationCommitRequest
    ) -> DurableReconciliationCommitResult: ...


__all__ = [
    "AUTHORIZATION_CONTRACT_VERSION",
    "AuthorizedReconciliationCommitPort",
    "AuthorizedReconciliationCommitRequest",
    "MAX_AUTHORIZATION_TTL_SECONDS",
    "ReconciliationAuthorizationAction",
    "ReconciliationAuthorizationError",
    "ReconciliationAuthorizationFailureCode",
    "ReconciliationCommitAuthorization",
]
