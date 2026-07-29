from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

from tx_trade.orders.live_contracts import (
    FingerprintDomain,
    LiveOrderIntent,
    LiveOrderState,
    LiveOrderType,
    LiveSide,
    LiveTimeInForce,
    NewOrderCommand,
    PendingCommandBinding,
    payload_fingerprint,
)
from tx_trade.orders.live_journal_contracts import (
    DurableReconciliationRequirement,
    LiveJournalIdentity,
    LiveJournalRecoverySnapshot,
    OutstandingDispatchClaim,
)
from tx_trade.orders.live_journal_recovery import (
    PendingRecoveryKind,
    RecoveryIssueCode,
    RecoveryReadiness,
    verify_recovery_snapshot,
)
from tx_trade.orders.live_ports import AmbiguousObservation, RawBrokerObservation
from tx_trade.orders.live_state_machine import AppliedEvent, AppliedEventLedger, create_live_order

NOW = datetime(2026, 7, 30, tzinfo=timezone.utc)
SHA = "sha256:" + ("a" * 64)


def _intent(order_id: str = "order-1") -> LiveOrderIntent:
    return LiveOrderIntent(
        strategy_id="strategy-1",
        client_order_id=order_id,
        account_id="account-1",
        instrument_id="TXF",
        side=LiveSide.BUY,
        quantity=Decimal("1"),
        order_type=LiveOrderType.LIMIT,
        limit_price=Decimal("22000"),
        time_in_force=LiveTimeInForce.DAY,
        day_trade=False,
        created_at=NOW,
    )


def _pending_order(order_id: str = "order-1", command_id: str = "command-1"):
    intent = _intent(order_id)
    command = NewOrderCommand(command_id, intent, NOW)
    binding = PendingCommandBinding(
        command,
        payload_fingerprint(command, FingerprintDomain.NEW_COMMAND_V1),
    )
    return replace(
        create_live_order(intent),
        state=LiveOrderState.SUBMITTING,
        version=2,
        pending_command=binding,
    )


def _observation(observation_id: str = "observation-1") -> RawBrokerObservation:
    return RawBrokerObservation(observation_id, "reply", 1, 1, NOW, b"opaque")


def _snapshot(
    *,
    orders=(),
    claims=(),
    unresolved=(),
    conflicts=(),
    ambiguous=(),
    requirements=(),
    ledger=AppliedEventLedger(),
) -> LiveJournalRecoverySnapshot:
    return LiveJournalRecoverySnapshot(
        LiveJournalIdentity("journal-1", 1, SHA, NOW),
        tuple(orders),
        tuple(claims),
        tuple(unresolved),
        tuple(conflicts),
        tuple(ambiguous),
        tuple(requirements),
        ledger,
        7,
    )


def test_empty_valid_snapshot_is_ready_and_projection_results_are_deterministic() -> None:
    orders = (create_live_order(_intent("order-b")), create_live_order(_intent("order-a")))

    first = verify_recovery_snapshot(_snapshot(orders=orders))
    second = verify_recovery_snapshot(_snapshot(orders=reversed(orders)))

    assert first.readiness is RecoveryReadiness.READY
    assert not first.may_dispatch
    assert [item.client_order_id for item in first.projections] == ["order-a", "order-b"]
    assert first.projections == second.projections


def test_claim_without_receipt_always_requires_reconciliation_and_never_retry() -> None:
    order = _pending_order()
    assert order.pending_command is not None
    claim = OutstandingDispatchClaim(
        order.pending_command.command,
        "claim-1",
        "dispatcher-1",
        order.version,
        NOW,
    )

    result = verify_recovery_snapshot(_snapshot(orders=(order,), claims=(claim,)))

    assert result.readiness is RecoveryReadiness.RECONCILIATION_REQUIRED
    assert result.pending[0].kind is PendingRecoveryKind.CLAIMED_OUTCOME_UNKNOWN
    assert not result.pending[0].may_redispatch
    assert RecoveryIssueCode.OUTSTANDING_DISPATCH in result.issues


def test_pending_without_outstanding_claim_still_needs_broker_evidence() -> None:
    result = verify_recovery_snapshot(_snapshot(orders=(_pending_order(),)))

    assert result.readiness is RecoveryReadiness.RECONCILIATION_REQUIRED
    assert result.pending[0].kind is PendingRecoveryKind.REGISTERED_AWAITING_BROKER_EVIDENCE
    assert RecoveryIssueCode.PENDING_BROKER_EVIDENCE in result.issues


def test_unresolved_and_ambiguous_observations_prevent_ready() -> None:
    first = _observation("observation-1")
    second = replace(
        _observation("observation-2"),
        adapter_received_sequence=2,
    )
    orders = (
        create_live_order(_intent("order-1")),
        create_live_order(_intent("order-2")),
    )
    result = verify_recovery_snapshot(
        _snapshot(
            orders=orders,
            unresolved=(first,),
            ambiguous=(AmbiguousObservation(second, ("order-1", "order-2")),),
        )
    )

    assert result.readiness is RecoveryReadiness.RECONCILIATION_REQUIRED
    assert RecoveryIssueCode.UNRESOLVED_OBSERVATION in result.issues
    assert RecoveryIssueCode.AMBIGUOUS_OBSERVATION in result.issues


def test_conflict_and_durable_requirement_require_reconciliation_deterministically() -> None:
    first = _observation("observation-b")
    second = replace(_observation("observation-a"), adapter_received_sequence=2)
    orders = (
        create_live_order(_intent("order-b")),
        create_live_order(_intent("order-a")),
    )
    requirements = (
        DurableReconciliationRequirement(9, "event-conflict", NOW, "order-b", "observation-b"),
        DurableReconciliationRequirement(3, "broker-conflict", NOW, "order-a", "observation-a"),
    )

    result = verify_recovery_snapshot(
        _snapshot(orders=orders, conflicts=(first, second), requirements=requirements)
    )

    assert result.readiness is RecoveryReadiness.RECONCILIATION_REQUIRED
    assert not result.may_dispatch
    assert result.conflict_observation_ids == ("observation-a", "observation-b")
    assert result.reconciliation_requirement_ids == (3, 9)
    assert RecoveryIssueCode.CONFLICT_OBSERVATION in result.issues
    assert RecoveryIssueCode.DURABLE_RECONCILIATION_REQUIREMENT in result.issues


def test_cross_order_claim_fails_closed_with_sanitized_result() -> None:
    order = _pending_order()
    foreign = _pending_order("order-2", "command-2")
    assert foreign.pending_command is not None
    claim = OutstandingDispatchClaim(
        foreign.pending_command.command,
        "claim-1",
        "dispatcher-1",
        order.version,
        NOW,
    )

    result = verify_recovery_snapshot(_snapshot(orders=(order,), claims=(claim,)))

    assert result.readiness is RecoveryReadiness.BLOCKED
    assert RecoveryIssueCode.CLAIM_ORDER_MISMATCH in result.issues
    assert "account-1" not in repr(result)
    assert "command-2" not in repr(result)


def test_claim_must_match_exact_pending_command_and_order_version() -> None:
    order = _pending_order()
    assert order.pending_command is not None
    different = NewOrderCommand(
        order.pending_command.client_command_id,
        replace(order.intent, quantity=Decimal("2")),
        NOW,
    )
    claim = OutstandingDispatchClaim(
        different,
        "claim-1",
        "dispatcher-1",
        order.version + 1,
        NOW,
    )

    result = verify_recovery_snapshot(_snapshot(orders=(order,), claims=(claim,)))

    assert result.readiness is RecoveryReadiness.BLOCKED
    assert RecoveryIssueCode.CLAIM_COMMAND_MISMATCH in result.issues
    assert RecoveryIssueCode.CLAIM_VERSION_MISMATCH in result.issues


def test_duplicate_applied_event_identity_and_bad_fingerprint_fail_closed() -> None:
    ledger = AppliedEventLedger(
        (
            AppliedEvent("reply", "event-1", SHA),
            AppliedEvent("reply", "event-1", "not-a-fingerprint"),
        )
    )

    result = verify_recovery_snapshot(_snapshot(ledger=ledger))

    assert result.readiness is RecoveryReadiness.BLOCKED
    assert RecoveryIssueCode.DUPLICATE_EVENT_IDENTITY in result.issues
    assert RecoveryIssueCode.INVALID_EVENT_FINGERPRINT in result.issues


def test_overlapping_resolution_categories_and_unknown_candidates_fail_closed() -> None:
    observation = _observation()
    orders = (
        create_live_order(_intent("order-1")),
        create_live_order(_intent("order-2")),
    )

    result = verify_recovery_snapshot(
        _snapshot(
            orders=orders,
            unresolved=(observation,),
            ambiguous=(AmbiguousObservation(observation, ("order-1", "missing-order")),),
        )
    )

    assert result.readiness is RecoveryReadiness.BLOCKED
    assert RecoveryIssueCode.OBSERVATION_RESOLUTION_CONFLICT in result.issues
    assert RecoveryIssueCode.UNKNOWN_AMBIGUITY_CANDIDATE in result.issues


def test_duplicate_or_cross_owned_reconciliation_requirements_fail_closed() -> None:
    observation = _observation()
    order = create_live_order(_intent())
    requirements = (
        DurableReconciliationRequirement(
            1,
            "event-conflict",
            NOW,
            "missing-order",
            observation.observation_id,
        ),
        DurableReconciliationRequirement(
            1,
            "event-conflict",
            NOW,
            order.intent.client_order_id,
            "missing-observation",
        ),
    )

    result = verify_recovery_snapshot(
        _snapshot(
            orders=(order,),
            conflicts=(observation,),
            requirements=requirements,
        )
    )

    assert result.readiness is RecoveryReadiness.BLOCKED
    assert RecoveryIssueCode.DUPLICATE_REQUIREMENT_ID in result.issues
    assert RecoveryIssueCode.REQUIREMENT_ORDER_MISMATCH in result.issues
    assert RecoveryIssueCode.REQUIREMENT_OBSERVATION_MISMATCH in result.issues


def test_projection_roundtrip_failure_blocks_without_exposing_value() -> None:
    order = create_live_order(_intent())
    object.__setattr__(order, "version", 0)

    result = verify_recovery_snapshot(_snapshot(orders=(order,)))

    assert result.readiness is RecoveryReadiness.BLOCKED
    assert RecoveryIssueCode.PROJECTION_INVALID in result.issues
    assert "account-1" not in repr(result)
