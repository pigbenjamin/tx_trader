from dataclasses import FrozenInstanceError, fields
from datetime import datetime, timedelta, timezone

import pytest

import tx_trade.orders.live_reconciliation_authorization_contracts as contracts
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
from tx_trade.orders.live_reconciliation_authorization_contracts import (
    AUTHORIZATION_CONTRACT_VERSION,
    MAX_AUTHORIZATION_TTL_SECONDS,
    AuthorizedReconciliationCommitPort,
    AuthorizedReconciliationCommitRequest,
    ReconciliationAuthorizationAction,
    ReconciliationAuthorizationError,
    ReconciliationAuthorizationFailureCode,
    ReconciliationCommitAuthorization,
)
from tx_trade.orders.live_reconciliation_commit_contracts import (
    DurableReconciliationCommitRequest,
    DurableReconciliationCommitResult,
    ReconciliationCommitDisposition,
)
from tx_trade.orders.live_reconciliation_contracts import (
    BrokerReconciliationSnapshot,
    LocalReconciliationSnapshot,
    ReconciliationAssessment,
)

NOW = datetime(2026, 8, 13, tzinfo=timezone.utc)
DIGEST = "sha256:" + "1" * 64


def assessment(
    *, account_id: str = "account-1", snapshot_id: str = "snapshot-1", sequence: int = 4
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


def commit_request(**changes: object) -> DurableReconciliationCommitRequest:
    values: dict[str, object] = {
        "commit_id": "commit-1",
        "account_id": "account-1",
        "assessment": assessment(),
        "expected_journal_sequence": 4,
    }
    values.update(changes)
    return DurableReconciliationCommitRequest(**values)  # type: ignore[arg-type]


def authorization(**changes: object) -> ReconciliationCommitAuthorization:
    values: dict[str, object] = {
        "authorization_id": "authorization-1",
        "principal_id": "principal-1",
        "authority_context_digest": DIGEST,
        "action": ReconciliationAuthorizationAction.RECONCILIATION_COMMIT,
        "journal_id": "journal-1",
        "account_id": "account-1",
        "source_inspection_digest": DIGEST,
        "operator_plan_digest": DIGEST,
        "commit_id": "commit-1",
        "request_digest": DIGEST,
        "broker_snapshot_id": "snapshot-1",
        "expected_journal_sequence": 4,
        "authorized_at": NOW,
        "expires_at": NOW + timedelta(seconds=60),
        "reason_code": "operator-approved",
    }
    values.update(changes)
    return ReconciliationCommitAuthorization(**values)  # type: ignore[arg-type]


def test_exact_public_surface_and_enum_values() -> None:
    assert contracts.__all__ == [
        "AUTHORIZATION_CONTRACT_VERSION",
        "AuthorizedReconciliationCommitPort",
        "AuthorizedReconciliationCommitRequest",
        "MAX_AUTHORIZATION_TTL_SECONDS",
        "ReconciliationAuthorizationAction",
        "ReconciliationAuthorizationError",
        "ReconciliationAuthorizationFailureCode",
        "ReconciliationCommitAuthorization",
    ]
    assert AUTHORIZATION_CONTRACT_VERSION == 1
    assert MAX_AUTHORIZATION_TTL_SECONDS == 300
    assert {item.name: item.value for item in ReconciliationAuthorizationAction} == {
        "RECONCILIATION_COMMIT": "reconciliation_commit"
    }
    assert {item.name: item.value for item in ReconciliationAuthorizationFailureCode} == {
        "INVALID_AUTHORIZATION": "invalid_authorization",
        "AUTHORIZATION_EXPIRED": "authorization_expired",
        "AUTHORIZATION_CONFLICT": "authorization_conflict",
    }


def test_authorization_is_frozen_slotted_redacted_and_never_dispatches() -> None:
    value = authorization()
    rendered = repr(value)

    assert not hasattr(value, "__dict__")
    assert value.may_dispatch is False
    assert not hasattr(value, "commit_allowed")
    assert {item.name for item in fields(value)} == {
        "authorization_id",
        "principal_id",
        "authority_context_digest",
        "action",
        "journal_id",
        "account_id",
        "source_inspection_digest",
        "operator_plan_digest",
        "commit_id",
        "request_digest",
        "broker_snapshot_id",
        "expected_journal_sequence",
        "authorized_at",
        "expires_at",
        "reason_code",
    }
    for secret in (
        "principal-1",
        DIGEST,
        "journal-1",
        "account-1",
        "snapshot-1",
    ):
        assert secret not in rendered
    with pytest.raises(FrozenInstanceError):
        value.commit_id = "commit-2"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field_name", "bad_value", "error"),
    (
        ("authorization_id", "", ValueError),
        ("reason_code", "a" * 129, ValueError),
        ("journal_id", "é", ValueError),
        ("account_id", 1, TypeError),
        ("authority_context_digest", "sha256:" + "A" * 64, ValueError),
        ("source_inspection_digest", "sha256:" + "1" * 63, ValueError),
        ("request_digest", 1, TypeError),
        ("expected_journal_sequence", -1, ValueError),
        ("expected_journal_sequence", True, TypeError),
        ("authorized_at", datetime(2026, 8, 13), ValueError),
        ("expires_at", datetime(2026, 8, 13, tzinfo=timezone(timedelta(hours=8))), ValueError),
        ("action", "reconciliation_commit", TypeError),
    ),
)
def test_authorization_rejects_noncanonical_fields(
    field_name: str, bad_value: object, error: type[Exception]
) -> None:
    with pytest.raises(error):
        authorization(**{field_name: bad_value})


def test_identifier_boundaries_and_exact_utc_datetime_type() -> None:
    assert authorization(authorization_id="a", reason_code="a" * 128).authorization_id == "a"

    class ForgedDatetime(datetime):
        pass

    forged = ForgedDatetime(2026, 8, 13, tzinfo=timezone.utc)
    with pytest.raises(TypeError, match="datetime"):
        authorization(authorized_at=forged)


@pytest.mark.parametrize(("seconds", "valid"), ((0, False), (300, True), (301, False)))
def test_ttl_boundaries(seconds: int, valid: bool) -> None:
    if valid:
        assert authorization(expires_at=NOW + timedelta(seconds=seconds)).expires_at > NOW
    else:
        with pytest.raises(ValueError, match="TTL"):
            authorization(expires_at=NOW + timedelta(seconds=seconds))


def test_authorized_request_is_exact_redacted_frozen_and_statically_bound() -> None:
    value = AuthorizedReconciliationCommitRequest(authorization(), commit_request())

    assert value.may_dispatch is False
    assert not hasattr(value, "commit_allowed")
    assert not hasattr(value, "__dict__")
    assert "account-1" not in repr(value)
    with pytest.raises(FrozenInstanceError):
        value.request = commit_request(commit_id="commit-2")  # type: ignore[misc]


@pytest.mark.parametrize(
    ("auth_changes", "request_changes", "message"),
    (
        ({"account_id": "account-2"}, {}, "account"),
        ({"commit_id": "commit-2"}, {}, "commit_id"),
        ({"expected_journal_sequence": 3}, {}, "journal sequence"),
        ({"broker_snapshot_id": "snapshot-2"}, {}, "broker snapshot"),
    ),
)
def test_authorized_request_rejects_static_binding_conflicts(
    auth_changes: dict[str, object], request_changes: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        AuthorizedReconciliationCommitRequest(
            authorization(**auth_changes), commit_request(**request_changes)
        )


def test_authorized_request_rejects_forged_nested_types_and_action() -> None:
    class ForgedAuthorization(ReconciliationCommitAuthorization):
        pass

    class ForgedRequest(DurableReconciliationCommitRequest):
        pass

    with pytest.raises(TypeError, match="authorization"):
        AuthorizedReconciliationCommitRequest(
            ForgedAuthorization(**authorization_values()), commit_request()
        )
    with pytest.raises(TypeError, match="request"):
        AuthorizedReconciliationCommitRequest(
            authorization(),
            ForgedRequest("commit-1", "account-1", assessment(), 4),
        )
    forged_action = authorization()
    object.__setattr__(forged_action, "action", "reconciliation_commit")
    with pytest.raises(ValueError, match="action"):
        AuthorizedReconciliationCommitRequest(forged_action, commit_request())


def authorization_values() -> dict[str, object]:
    value = authorization()
    return {item.name: getattr(value, item.name) for item in fields(value)}


@pytest.mark.parametrize(
    ("code", "message"),
    (
        (
            ReconciliationAuthorizationFailureCode.INVALID_AUTHORIZATION,
            "reconciliation authorization is invalid",
        ),
        (
            ReconciliationAuthorizationFailureCode.AUTHORIZATION_EXPIRED,
            "reconciliation authorization has expired",
        ),
        (
            ReconciliationAuthorizationFailureCode.AUTHORIZATION_CONFLICT,
            "reconciliation authorization conflicts with the request",
        ),
    ),
)
def test_failure_errors_have_fixed_sanitized_messages(
    code: ReconciliationAuthorizationFailureCode, message: str
) -> None:
    error = ReconciliationAuthorizationError(code)
    assert error.code is code
    assert str(error) == message
    assert "principal-1" not in str(error)


def test_failure_error_rejects_untyped_code() -> None:
    with pytest.raises(TypeError, match="FailureCode"):
        ReconciliationAuthorizationError("invalid_authorization")  # type: ignore[arg-type]


def test_port_exposes_authorized_commit_operation() -> None:
    expected = DurableReconciliationCommitResult(
        "commit-1",
        "account-1",
        ReconciliationCommitDisposition.COMMITTED,
        NOW,
        5,
    )

    class Committer:
        def commit_authorized_reconciliation(
            self, request: AuthorizedReconciliationCommitRequest
        ) -> DurableReconciliationCommitResult:
            return expected

    assert isinstance(Committer(), AuthorizedReconciliationCommitPort)
    assert not isinstance(object(), AuthorizedReconciliationCommitPort)


def test_request_digest_is_attested_but_not_recomputed_here() -> None:
    alternate_digest = "sha256:" + "2" * 64
    value = AuthorizedReconciliationCommitRequest(
        authorization(request_digest=alternate_digest), commit_request()
    )
    assert value.authorization.request_digest == alternate_digest
