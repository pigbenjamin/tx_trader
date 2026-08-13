"""Canonical integrity bindings for reconciliation commit authorizations.

The digests in this module are integrity bindings, not signatures or proof of
authentication.  Authorization creation is a composition helper for trusted
callers and performs no authentication itself.
"""

from __future__ import annotations

from datetime import datetime

from .live_journal_codec import encode_journal_value, journal_digest
from .live_reconciliation_authorization_contracts import (
    AuthorizedReconciliationCommitRequest,
    ReconciliationAuthorizationAction,
    ReconciliationAuthorizationError,
    ReconciliationAuthorizationFailureCode,
    ReconciliationCommitAuthorization,
)
from .live_reconciliation_commit_contracts import DurableReconciliationCommitRequest

RECONCILIATION_COMMIT_REQUEST_DIGEST_DOMAIN = (
    "tx_trade.live.journal.reconciliation-commit-request.v2"
)
RECONCILIATION_AUTHORIZATION_DIGEST_DOMAIN = "tx_trade.live.journal.reconciliation-authorization.v3"


def reconciliation_commit_request_digest(request: DurableReconciliationCommitRequest) -> str:
    """Return the persisted, byte-compatible digest for one commit request."""

    if type(request) is not DurableReconciliationCommitRequest:
        raise TypeError("request must be DurableReconciliationCommitRequest")
    payload = encode_journal_value(request)
    return journal_digest(RECONCILIATION_COMMIT_REQUEST_DIGEST_DOMAIN, payload)


def reconciliation_authorization_digest(
    authorization: ReconciliationCommitAuthorization,
) -> str:
    """Return a canonical integrity digest for one authorization attestation."""

    if type(authorization) is not ReconciliationCommitAuthorization:
        raise TypeError("authorization must be ReconciliationCommitAuthorization")
    payload = encode_journal_value(authorization)
    return journal_digest(RECONCILIATION_AUTHORIZATION_DIGEST_DOMAIN, payload)


def authorize_reconciliation_commit(
    *,
    request: DurableReconciliationCommitRequest,
    authorization_id: str,
    principal_id: str,
    authority_context_digest: str,
    journal_id: str,
    source_inspection_digest: str,
    operator_plan_digest: str,
    authorized_at: datetime,
    expires_at: datetime,
    reason_code: str,
) -> AuthorizedReconciliationCommitRequest:
    """Compose an exact authorization around a trusted caller's valid request."""

    if type(request) is not DurableReconciliationCommitRequest:
        raise TypeError("request must be DurableReconciliationCommitRequest")
    authorization = ReconciliationCommitAuthorization(
        authorization_id=authorization_id,
        principal_id=principal_id,
        authority_context_digest=authority_context_digest,
        action=ReconciliationAuthorizationAction.RECONCILIATION_COMMIT,
        journal_id=journal_id,
        account_id=request.account_id,
        source_inspection_digest=source_inspection_digest,
        operator_plan_digest=operator_plan_digest,
        commit_id=request.commit_id,
        request_digest=reconciliation_commit_request_digest(request),
        broker_snapshot_id=request.assessment.broker_snapshot.snapshot_id,
        expected_journal_sequence=request.expected_journal_sequence,
        authorized_at=authorized_at,
        expires_at=expires_at,
        reason_code=reason_code,
    )
    return AuthorizedReconciliationCommitRequest(authorization, request)


def validate_authorized_reconciliation_commit(
    value: AuthorizedReconciliationCommitRequest,
) -> AuthorizedReconciliationCommitRequest:
    """Fail closed unless the wrapper and canonical request binding are intact."""

    if type(value) is not AuthorizedReconciliationCommitRequest:
        raise TypeError("value must be AuthorizedReconciliationCommitRequest")
    try:
        AuthorizedReconciliationCommitRequest(value.authorization, value.request)
        expected_digest = reconciliation_commit_request_digest(value.request)
        if value.authorization.request_digest != expected_digest:
            raise ValueError("request digest mismatch")
    except MemoryError:
        raise
    except BaseException:
        raise ReconciliationAuthorizationError(
            ReconciliationAuthorizationFailureCode.INVALID_AUTHORIZATION
        ) from None
    return value


__all__ = [
    "RECONCILIATION_AUTHORIZATION_DIGEST_DOMAIN",
    "RECONCILIATION_COMMIT_REQUEST_DIGEST_DOMAIN",
    "authorize_reconciliation_commit",
    "reconciliation_authorization_digest",
    "reconciliation_commit_request_digest",
    "validate_authorized_reconciliation_commit",
]
