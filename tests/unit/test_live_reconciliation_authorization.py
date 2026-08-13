from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

import tx_trade.orders.live_reconciliation_authorization as authorization_module
from tx_trade.orders.live_journal_codec import (
    LiveJournalCodecError,
    encode_journal_value,
    journal_digest,
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
from tx_trade.orders.live_reconciliation_authorization import (
    RECONCILIATION_AUTHORIZATION_DIGEST_DOMAIN,
    RECONCILIATION_COMMIT_REQUEST_DIGEST_DOMAIN,
    authorize_reconciliation_commit,
    reconciliation_authorization_digest,
    reconciliation_commit_request_digest,
    validate_authorized_reconciliation_commit,
)
from tx_trade.orders.live_reconciliation_authorization_contracts import (
    AuthorizedReconciliationCommitRequest,
    ReconciliationAuthorizationError,
    ReconciliationAuthorizationFailureCode,
    ReconciliationCommitAuthorization,
)
from tx_trade.orders.live_reconciliation_commit_contracts import (
    DurableReconciliationCommitRequest,
)
from tx_trade.orders.live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    LocalReconciliationSnapshot,
    ReconciliationAssessment,
)

NOW = datetime(2026, 8, 13, 1, 2, 3, 456789, tzinfo=timezone.utc)
DIGEST = "sha256:" + "1" * 64


def assessment(
    *, account_id: str = "account-secret", snapshot_id: str = "snapshot-secret", sequence: int = 4
) -> ReconciliationAssessment:
    local = LocalReconciliationSnapshot(account_id, (), (), (), NOW, (), sequence)
    captured_at = NOW + timedelta(seconds=1)

    def evidence(kind: EvidenceQueryKind) -> CompletenessEvidence:
        return CompletenessEvidence(
            kind,
            account_id,
            EvidenceCompleteness.COMPLETE,
            captured_at,
            snapshot_id,
        )

    open_orders = OpenOrdersSnapshot((), evidence(EvidenceQueryKind.OPEN_ORDERS))
    fills = BrokerFillsSnapshot((), evidence(EvidenceQueryKind.FILLS))
    positions = BrokerPositionsSnapshot((), evidence(EvidenceQueryKind.POSITIONS))
    broker = BrokerReconciliationSnapshot(
        snapshot_id, account_id, open_orders, fills, positions, captured_at
    )
    result = ReconciliationResult(
        account_id,
        ReconciliationStatus.COMPLETE,
        (),
        (open_orders.evidence, fills.evidence, positions.evidence),
        captured_at + timedelta(seconds=1),
    )
    return ReconciliationAssessment(local, broker, result)


def request(**changes: object) -> DurableReconciliationCommitRequest:
    values: dict[str, object] = {
        "commit_id": "commit-1",
        "account_id": "account-secret",
        "assessment": assessment(),
        "expected_journal_sequence": 4,
    }
    values.update(changes)
    return DurableReconciliationCommitRequest(**values)  # type: ignore[arg-type]


def authorized(**changes: object) -> AuthorizedReconciliationCommitRequest:
    values: dict[str, object] = {
        "request": request(),
        "authorization_id": "authorization-1",
        "principal_id": "principal-secret",
        "authority_context_digest": DIGEST,
        "journal_id": "journal-secret",
        "source_inspection_digest": DIGEST,
        "operator_plan_digest": DIGEST,
        "authorized_at": NOW,
        "expires_at": NOW + timedelta(seconds=60),
        "reason_code": "operator-approved",
    }
    values.update(changes)
    return authorize_reconciliation_commit(**values)  # type: ignore[arg-type]


def test_exact_public_surface_and_frozen_domains() -> None:
    assert authorization_module.__all__ == [
        "RECONCILIATION_AUTHORIZATION_DIGEST_DOMAIN",
        "RECONCILIATION_COMMIT_REQUEST_DIGEST_DOMAIN",
        "authorize_reconciliation_commit",
        "reconciliation_authorization_digest",
        "reconciliation_commit_request_digest",
        "validate_authorized_reconciliation_commit",
    ]
    assert (
        RECONCILIATION_COMMIT_REQUEST_DIGEST_DOMAIN
        == "tx_trade.live.journal.reconciliation-commit-request.v2"
    )
    assert (
        RECONCILIATION_AUTHORIZATION_DIGEST_DOMAIN
        == "tx_trade.live.journal.reconciliation-authorization.v3"
    )


def test_request_digest_is_existing_codec_and_domain_binding() -> None:
    value = request()
    payload = encode_journal_value(value)

    assert reconciliation_commit_request_digest(value) == journal_digest(
        "tx_trade.live.journal.reconciliation-commit-request.v2", payload
    )
    assert (
        reconciliation_commit_request_digest(value)
        == "sha256:e49fc6366b0ce2fc3b2b5f0f9c0a967a2c7e73acea4cedd16c8b4cae93077506"
    )
    assert reconciliation_commit_request_digest(value) == reconciliation_commit_request_digest(
        value
    )


def test_factory_derives_every_request_binding_and_redacts_secrets() -> None:
    commit_request = request()
    value = authorized(request=commit_request)
    attestation = value.authorization

    assert value.request is commit_request
    assert attestation.account_id == commit_request.account_id
    assert attestation.commit_id == commit_request.commit_id
    assert attestation.request_digest == reconciliation_commit_request_digest(commit_request)
    assert attestation.broker_snapshot_id == commit_request.assessment.broker_snapshot.snapshot_id
    assert attestation.expected_journal_sequence == commit_request.expected_journal_sequence
    for secret in (
        "account-secret",
        "principal-secret",
        "journal-secret",
        "snapshot-secret",
        DIGEST,
    ):
        assert secret not in repr(value)
        assert secret not in repr(attestation)


def test_authorized_wrapper_is_not_a_stored_codec_type() -> None:
    with pytest.raises(LiveJournalCodecError):
        encode_journal_value(authorized())


def test_authorization_digest_is_canonical_deterministic_and_domain_separated() -> None:
    attestation = authorized().authorization
    payload = encode_journal_value(attestation)
    digest = reconciliation_authorization_digest(attestation)

    assert digest == journal_digest(RECONCILIATION_AUTHORIZATION_DIGEST_DOMAIN, payload)
    assert digest == reconciliation_authorization_digest(attestation)
    assert digest != reconciliation_commit_request_digest(request())
    assert digest != reconciliation_authorization_digest(
        replace(attestation, authorization_id="authorization-2")
    )


def test_authorization_digest_is_independent_of_equivalent_utc_timezone_context() -> None:
    named_utc = timezone(timedelta(0), "named-utc")
    first = authorized().authorization
    second = replace(
        first,
        authorized_at=first.authorized_at.astimezone(named_utc),
        expires_at=first.expires_at.astimezone(named_utc),
    )

    assert reconciliation_authorization_digest(first) == reconciliation_authorization_digest(second)


def test_digest_and_factory_require_exact_types() -> None:
    class ForgedRequest(DurableReconciliationCommitRequest):
        pass

    class ForgedAuthorization(ReconciliationCommitAuthorization):
        pass

    forged_request = ForgedRequest("commit-1", "account-secret", assessment(), 4)
    with pytest.raises(TypeError, match="DurableReconciliationCommitRequest"):
        reconciliation_commit_request_digest(forged_request)
    with pytest.raises(TypeError, match="DurableReconciliationCommitRequest"):
        authorized(request=forged_request)
    original = authorized().authorization
    forged_authorization = ForgedAuthorization(
        **{field: getattr(original, field) for field in original.__slots__}
    )
    with pytest.raises(TypeError, match="ReconciliationCommitAuthorization"):
        reconciliation_authorization_digest(forged_authorization)
    with pytest.raises(TypeError, match="AuthorizedReconciliationCommitRequest"):
        validate_authorized_reconciliation_commit(object())  # type: ignore[arg-type]


def test_validation_returns_exact_untampered_value() -> None:
    value = authorized()

    assert validate_authorized_reconciliation_commit(value) is value


def test_validation_sanitizes_digest_mismatch_without_secret_values() -> None:
    value = authorized()
    object.__setattr__(
        value.authorization,
        "request_digest",
        "sha256:" + "2" * 64,
    )

    with pytest.raises(ReconciliationAuthorizationError) as caught:
        validate_authorized_reconciliation_commit(value)

    assert caught.value.code is ReconciliationAuthorizationFailureCode.INVALID_AUTHORIZATION
    assert str(caught.value) == "reconciliation authorization is invalid"
    assert "account-secret" not in str(caught.value)
    assert "principal-secret" not in repr(caught.value)
    assert caught.value.__cause__ is None


def test_validation_sanitizes_altered_and_forged_nested_request() -> None:
    altered = authorized()
    object.__setattr__(altered, "request", request(commit_id="commit-2"))
    with pytest.raises(ReconciliationAuthorizationError):
        validate_authorized_reconciliation_commit(altered)

    forged = authorized()
    object.__setattr__(forged, "request", object())
    with pytest.raises(ReconciliationAuthorizationError):
        validate_authorized_reconciliation_commit(forged)


@pytest.mark.parametrize("failure", (Exception("secret"), KeyboardInterrupt(), SystemExit()))
def test_validation_maps_all_non_memory_failures_to_sanitized_error(
    monkeypatch: pytest.MonkeyPatch, failure: BaseException
) -> None:
    value = authorized()

    def fail(_: DurableReconciliationCommitRequest) -> str:
        raise failure

    monkeypatch.setattr(authorization_module, "reconciliation_commit_request_digest", fail)

    with pytest.raises(ReconciliationAuthorizationError) as caught:
        validate_authorized_reconciliation_commit(value)

    assert caught.value.code is ReconciliationAuthorizationFailureCode.INVALID_AUTHORIZATION
    assert "secret" not in str(caught.value)
    assert caught.value.__cause__ is None


def test_validation_propagates_memory_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    value = authorized()

    def fail(_: DurableReconciliationCommitRequest) -> str:
        raise MemoryError("secret")

    monkeypatch.setattr(authorization_module, "reconciliation_commit_request_digest", fail)

    with pytest.raises(MemoryError, match="secret"):
        validate_authorized_reconciliation_commit(value)
