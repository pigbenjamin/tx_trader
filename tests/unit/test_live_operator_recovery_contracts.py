from dataclasses import FrozenInstanceError, fields
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tx_trade.orders.live_operator_recovery_contracts import (
    ExplicitOperatorRecoverySelection,
    ExplicitOperatorRecoveryTargetSelection,
    OfflineOperatorRecoveryPlan,
    OperatorRecoveryDisposition,
    OperatorRecoveryReason,
    OperatorRecoveryResolution,
    OperatorRecoveryTargetKind,
    RedactedOperatorRecoveryTarget,
)
from tx_trade.orders.live_contracts import (
    LiveOrder,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
)
from tx_trade.orders.live_reconciliation_commit_contracts import ExpectedOrderVersion
from tx_trade.orders.live_reconciliation_projection_contracts import (
    AuthoritativeOrderProjectionPlan,
    OrderProjectionDisposition,
)

DIGEST = "sha256:" + "1" * 64
NOW = datetime(2026, 8, 4, tzinfo=timezone.utc)


def _claim_target(target_id: str = "redacted-claim-1") -> RedactedOperatorRecoveryTarget:
    return RedactedOperatorRecoveryTarget(
        OperatorRecoveryTargetKind.CLAIM,
        target_id,
        (
            OperatorRecoveryResolution.BROKER_FILL_CONFIRMED,
            OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,
        ),
    )


def _ready_projection(account_id: str = "account-secret") -> AuthoritativeOrderProjectionPlan:
    intent = LiveOrderIntent(
        "strategy-1",
        "order-1",
        account_id,
        "TXF-202608",
        LiveSide.BUY,
        Decimal("1"),
        LiveOrderType.LIMIT,
        Decimal("22000"),
        LiveTimeInForce.DAY,
        False,
        NOW,
    )
    order = LiveOrder(
        intent,
        LiveOrderState.ACCEPTED,
        Decimal("1"),
        Decimal("0"),
        Decimal("1"),
        None,
        Decimal("22000"),
        2,
        NOW,
        accepted_at=NOW,
    )
    return AuthoritativeOrderProjectionPlan(
        account_id,
        "snapshot-1",
        7,
        OrderProjectionDisposition.READY,
        (ExpectedOrderVersion("order-1", 1),),
        (order,),
        ("discrepancy-1",),
    )


def _ready_plan(**changes: object) -> OfflineOperatorRecoveryPlan:
    values: dict[str, object] = {
        "account_id": "account-secret",
        "journal_sequence": 7,
        "inspection_digest": DIGEST,
        "disposition": OperatorRecoveryDisposition.READY_FOR_EXPLICIT_COMMIT,
        "issue_codes": ("claimed-outcome-unknown",),
        "targets": (_claim_target(),),
        "projection_plan": _ready_projection(),
    }
    values.update(changes)
    return OfflineOperatorRecoveryPlan(**values)  # type: ignore[arg-type]


def test_redacted_target_is_exact_sorted_slotted_and_sensitive_repr_safe() -> None:
    target = _claim_target()
    assert not hasattr(target, "__dict__")
    assert "redacted-claim-1" not in repr(target)
    assert {item.name for item in fields(target)} == {
        "kind",
        "target_id",
        "allowed_resolutions",
    }
    with pytest.raises(FrozenInstanceError):
        target.target_id = "different"  # type: ignore[misc]
    with pytest.raises(ValueError, match="sorted"):
        RedactedOperatorRecoveryTarget(
            OperatorRecoveryTargetKind.CLAIM,
            "claim-1",
            (
                OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,
                OperatorRecoveryResolution.BROKER_FILL_CONFIRMED,
            ),
        )
    with pytest.raises(ValueError, match="target kind"):
        RedactedOperatorRecoveryTarget(
            OperatorRecoveryTargetKind.REQUIREMENT,
            "requirement-1",
            (OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,),
        )


@pytest.mark.parametrize(
    "plan",
    (
        OfflineOperatorRecoveryPlan(
            "account-secret",
            7,
            DIGEST,
            OperatorRecoveryDisposition.READY_NO_ACTION,
        ),
        OfflineOperatorRecoveryPlan(
            "account-secret",
            7,
            DIGEST,
            OperatorRecoveryDisposition.NEEDS_BROKER_EVIDENCE,
            reasons=(OperatorRecoveryReason.BROKER_EVIDENCE_REQUIRED,),
        ),
        _ready_plan(),
        OfflineOperatorRecoveryPlan(
            "account-secret",
            7,
            DIGEST,
            OperatorRecoveryDisposition.UNSUPPORTED_REQUIRES_ESCALATION,
            reasons=(OperatorRecoveryReason.UNSUPPORTED_ISSUE,),
        ),
        OfflineOperatorRecoveryPlan(
            "account-secret",
            7,
            DIGEST,
            OperatorRecoveryDisposition.BLOCKED_INTEGRITY_FAILURE,
            reasons=(OperatorRecoveryReason.INTEGRITY_FAILURE,),
        ),
    ),
)
def test_every_plan_disposition_has_strict_commit_and_dispatch_properties(
    plan: OfflineOperatorRecoveryPlan,
) -> None:
    assert plan.commit_allowed == (
        plan.disposition is OperatorRecoveryDisposition.READY_FOR_EXPLICIT_COMMIT
    )
    assert not plan.may_dispatch
    assert "account-secret" not in repr(plan)
    assert DIGEST not in repr(plan)


def test_ready_plan_requires_matching_ready_projection() -> None:
    projection = _ready_projection()
    assert _ready_plan(projection_plan=projection).projection_plan is projection
    with pytest.raises(ValueError, match="READY projection"):
        _ready_plan(projection_plan=None)

    projection = AuthoritativeOrderProjectionPlan(
        "account-secret",
        "snapshot-1",
        7,
        OrderProjectionDisposition.NO_CHANGE,
    )
    with pytest.raises(ValueError, match="READY projection"):
        _ready_plan(projection_plan=projection)
    with pytest.raises(ValueError, match="account and journal cut"):
        _ready_plan(
            projection_plan=AuthoritativeOrderProjectionPlan(
                "other-account",
                "snapshot-1",
                7,
                OrderProjectionDisposition.NO_CHANGE,
            )
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"journal_sequence": True}, "integer"),
        ({"inspection_digest": "sha256:bad"}, "SHA-256"),
        ({"targets": [_claim_target()]}, "tuple"),
        ({"issue_codes": ("issue-2", "issue-1")}, "sorted"),
        ({"issue_codes": ("issue-1", "issue-1")}, "unique"),
        (
            {"targets": (_claim_target("redacted-2"), _claim_target("redacted-1"))},
            "sorted",
        ),
    ),
)
def test_plan_rejects_coercion_duplicates_and_unsorted_values(
    changes: dict[str, object], message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        _ready_plan(**changes)


def test_plan_rejects_string_subclasses() -> None:
    class StringSubclass(str):
        pass

    with pytest.raises(TypeError, match="exact str"):
        _ready_plan(issue_codes=(StringSubclass("issue-1"),))


def test_plan_disposition_invariants_are_strict() -> None:
    with pytest.raises(ValueError, match="must not contain"):
        OfflineOperatorRecoveryPlan(
            "account-secret",
            7,
            DIGEST,
            OperatorRecoveryDisposition.READY_NO_ACTION,
            issue_codes=("issue-1",),
        )
    with pytest.raises(ValueError, match="requires issues"):
        _ready_plan(issue_codes=())
    with pytest.raises(ValueError, match="require reasons"):
        OfflineOperatorRecoveryPlan(
            "account-secret",
            7,
            DIGEST,
            OperatorRecoveryDisposition.NEEDS_BROKER_EVIDENCE,
        )
    with pytest.raises(ValueError, match="integrity reason"):
        OfflineOperatorRecoveryPlan(
            "account-secret",
            7,
            DIGEST,
            OperatorRecoveryDisposition.BLOCKED_INTEGRITY_FAILURE,
            reasons=(OperatorRecoveryReason.UNSUPPORTED_ISSUE,),
        )


def test_explicit_selection_is_minimal_frozen_sorted_and_redacted() -> None:
    selected = (
        ExplicitOperatorRecoveryTargetSelection(
            OperatorRecoveryTargetKind.CLAIM,
            "claim-secret",
            OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,
        ),
        ExplicitOperatorRecoveryTargetSelection(
            OperatorRecoveryTargetKind.REQUIREMENT,
            "requirement-secret",
            OperatorRecoveryResolution.SATISFIED,
        ),
    )
    selection = ExplicitOperatorRecoverySelection(
        "commit-1",
        "account-secret",
        7,
        DIGEST,
        selected,
    )
    assert not hasattr(selection, "__dict__")
    assert "account-secret" not in repr(selection)
    assert "claim-secret" not in repr(selection)
    assert "requirement-secret" not in repr(selection)
    assert DIGEST not in repr(selection)
    assert {item.name for item in fields(selection)} == {
        "commit_id",
        "account_id",
        "journal_sequence",
        "inspection_digest",
        "selected_targets",
    }
    with pytest.raises(FrozenInstanceError):
        selection.commit_id = "different"  # type: ignore[misc]


def test_selection_rejects_invalid_resolution_order_and_container_coercion() -> None:
    with pytest.raises(ValueError, match="requirement"):
        ExplicitOperatorRecoveryTargetSelection(
            OperatorRecoveryTargetKind.REQUIREMENT,
            "requirement-1",
            OperatorRecoveryResolution.BROKER_FILL_CONFIRMED,
        )
    first = ExplicitOperatorRecoveryTargetSelection(
        OperatorRecoveryTargetKind.OBSERVATION,
        "observation-2",
        OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,
    )
    second = ExplicitOperatorRecoveryTargetSelection(
        OperatorRecoveryTargetKind.OBSERVATION,
        "observation-1",
        OperatorRecoveryResolution.BROKER_ORDER_CONFIRMED,
    )
    with pytest.raises(ValueError, match="sorted"):
        ExplicitOperatorRecoverySelection("commit-1", "account-secret", 7, DIGEST, (first, second))
    with pytest.raises(TypeError, match="tuple"):
        ExplicitOperatorRecoverySelection(
            "commit-1",
            "account-secret",
            7,
            DIGEST,
            [second],  # type: ignore[arg-type]
        )


def test_sensitive_values_do_not_appear_in_validation_errors() -> None:
    secret = "account secret should not leak"
    with pytest.raises(ValueError) as raised:
        OfflineOperatorRecoveryPlan(
            secret,
            7,
            DIGEST,
            OperatorRecoveryDisposition.READY_NO_ACTION,
        )
    assert secret not in str(raised.value)


def test_selection_has_no_evidence_timestamp_token_receipt_or_projection_fields() -> None:
    names = {item.name for item in fields(ExplicitOperatorRecoverySelection)}
    assert not names & {
        "assessment",
        "broker_evidence",
        "timestamp",
        "claim_token",
        "receipt",
        "order_projections",
    }
