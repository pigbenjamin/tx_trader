from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tx_trade.orders.live_contracts import (
    LiveOrderIntent,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
)
from tx_trade.orders.live_reconciliation_commit_contracts import ExpectedOrderVersion
from tx_trade.orders.live_reconciliation_projection_contracts import (
    AuthoritativeOrderProjectionPlan,
    OrderProjectionDisposition,
    OrderProjectionReason,
)
from tx_trade.orders.live_state_machine import create_live_order

NOW = datetime(2026, 8, 4, tzinfo=timezone.utc)


def _projected(order_id: str = "order-1", account_id: str = "account-secret"):
    intent = LiveOrderIntent(
        strategy_id="strategy-1",
        client_order_id=order_id,
        account_id=account_id,
        instrument_id="TXF-202608",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=NOW,
    )
    return replace(create_live_order(intent), version=2)


def _ready(**changes: object) -> AuthoritativeOrderProjectionPlan:
    values: dict[str, object] = {
        "account_id": "account-secret",
        "snapshot_id": "snapshot-1",
        "expected_journal_sequence": 4,
        "disposition": OrderProjectionDisposition.READY,
        "expected_order_versions": (ExpectedOrderVersion("order-1", 1),),
        "projected_orders": (_projected(),),
        "consumed_discrepancy_ids": ("discrepancy-1",),
    }
    values.update(changes)
    return AuthoritativeOrderProjectionPlan(**values)  # type: ignore[arg-type]


def test_ready_plan_is_frozen_slotted_redacted_and_commit_only() -> None:
    plan = _ready()
    assert plan.may_commit
    assert not plan.may_dispatch
    assert not hasattr(plan, "__dict__")
    assert "account-secret" not in repr(plan)
    assert "LiveOrder" not in repr(plan)
    with pytest.raises(FrozenInstanceError):
        plan.snapshot_id = "different"  # type: ignore[misc]


@pytest.mark.parametrize(
    "plan",
    (
        AuthoritativeOrderProjectionPlan(
            "account-secret",
            "snapshot-1",
            4,
            OrderProjectionDisposition.NO_CHANGE,
        ),
        AuthoritativeOrderProjectionPlan(
            "account-secret",
            "snapshot-1",
            4,
            OrderProjectionDisposition.NOT_AUTHORITATIVE,
            reasons=(OrderProjectionReason.NOT_AUTHORITATIVE,),
        ),
        AuthoritativeOrderProjectionPlan(
            "account-secret",
            "snapshot-1",
            4,
            OrderProjectionDisposition.UNSUPPORTED,
            unsupported_discrepancy_ids=("discrepancy-unsupported",),
            reasons=(OrderProjectionReason.UNSUPPORTED_DISCREPANCY,),
        ),
    ),
)
def test_non_ready_dispositions_never_commit_or_dispatch(
    plan: AuthoritativeOrderProjectionPlan,
) -> None:
    assert not plan.may_commit
    assert not plan.may_dispatch


def test_reason_codes_cover_authority_evidence_matching_and_projection_failures() -> None:
    assert {item.value for item in OrderProjectionReason} == {
        "assessment_mismatch",
        "not_authoritative",
        "incomplete_evidence",
        "ambiguous_evidence",
        "unsupported_discrepancy",
        "unsupported_local_state",
        "unsupported_command",
        "missing_broker_match",
        "multiple_broker_matches",
        "broker_identity_mismatch",
        "broker_quantity_mismatch",
        "broker_price_mismatch",
        "broker_time_mismatch",
        "fill_evidence_unsupported",
    }


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"expected_journal_sequence": True}, "integer"),
        ({"expected_journal_sequence": -1}, "nonnegative"),
        ({"expected_order_versions": [ExpectedOrderVersion("order-1", 1)]}, "tuple"),
        ({"consumed_discrepancy_ids": ("discrepancy-2", "discrepancy-1")}, "sorted"),
        (
            {"consumed_discrepancy_ids": ("discrepancy-1", "discrepancy-1")},
            "unique",
        ),
        ({"projected_orders": (_projected(account_id="other-account"),)}, "account"),
        (
            {"expected_order_versions": (ExpectedOrderVersion("different-order", 1),)},
            "one-to-one",
        ),
        ({"projected_orders": (replace(_projected(), version=3),)}, "exactly once"),
    ),
)
def test_plan_rejects_noncanonical_or_mismatched_values(
    changes: dict[str, object], message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        _ready(**changes)


def test_plan_rejects_string_subclasses_and_unsorted_order_mappings() -> None:
    class StringSubclass(str):
        pass

    with pytest.raises(TypeError, match="exact str"):
        _ready(consumed_discrepancy_ids=(StringSubclass("discrepancy-1"),))
    with pytest.raises(ValueError, match="sorted"):
        _ready(
            expected_order_versions=(
                ExpectedOrderVersion("order-2", 1),
                ExpectedOrderVersion("order-1", 1),
            ),
            projected_orders=(_projected("order-1"), _projected("order-2")),
        )


def test_disposition_payload_invariants_are_strict() -> None:
    with pytest.raises(ValueError, match="READY"):
        _ready(consumed_discrepancy_ids=())
    with pytest.raises(ValueError, match="NO_CHANGE"):
        AuthoritativeOrderProjectionPlan(
            "account-secret",
            "snapshot-1",
            4,
            OrderProjectionDisposition.NO_CHANGE,
            reasons=(OrderProjectionReason.NOT_AUTHORITATIVE,),
        )
    with pytest.raises(ValueError, match="reasons"):
        AuthoritativeOrderProjectionPlan(
            "account-secret",
            "snapshot-1",
            4,
            OrderProjectionDisposition.UNSUPPORTED,
        )
    with pytest.raises(ValueError, match="must not classify"):
        AuthoritativeOrderProjectionPlan(
            "account-secret",
            "snapshot-1",
            4,
            OrderProjectionDisposition.NOT_AUTHORITATIVE,
            unsupported_discrepancy_ids=("discrepancy-1",),
            reasons=(OrderProjectionReason.NOT_AUTHORITATIVE,),
        )


def test_contract_contains_only_projection_data() -> None:
    names = {item.name for item in fields(AuthoritativeOrderProjectionPlan)}
    assert not names & {
        "claim_token",
        "broker_assessment",
        "receipt",
        "committed_at",
        "raw_payload",
    }
