from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime
from decimal import Decimal
from uuid import UUID

import pytest

from tx_trade.orders import (
    CancelIntent,
    MatchDisposition,
    MatchResult,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PaperDecision,
    PaperDecisionBatchResult,
    PaperEvent,
    PaperEventType,
    PaperOrder,
    PaperRejection,
    RejectionCode,
    TimeInForce,
    canonical_json,
)
from tx_trade.orders.contracts import TAIPEI

RUN_ID = UUID("11111111-1111-4111-8111-111111111111")
ORDER_ID = UUID("22222222-2222-4222-8222-222222222222")
SESSION_ID = UUID("55555555-5555-4555-8555-555555555555")
OTHER_SESSION_ID = UUID("99999999-9999-4999-8999-999999999999")
NOW = datetime(2026, 7, 28, 9, 0, tzinfo=TAIPEI)


def order_intent(**changes: object) -> OrderIntent:
    values: dict[str, object] = {
        "strategy_id": "strategy-a",
        "client_order_id": "client-1",
        "account_id": "paper-account",
        "instrument_id": "TXF-202608",
        "side": OrderSide.BUY,
        "quantity": Decimal("2"),
        "order_type": OrderType.LIMIT,
        "limit_price": Decimal("22100.5"),
        "time_in_force": TimeInForce.DAY,
        "day_trade": True,
        "created_at": NOW,
        "source_session_id": SESSION_ID,
        "source_ingest_sequence": 7,
    }
    values.update(changes)
    return OrderIntent(**values)  # type: ignore[arg-type]


def cancel_intent(**changes: object) -> CancelIntent:
    values: dict[str, object] = {
        "strategy_id": "strategy-a",
        "client_order_id": "client-1",
        "paper_order_id": ORDER_ID,
        "requested_at": NOW,
        "source_session_id": SESSION_ID,
        "source_ingest_sequence": 7,
    }
    values.update(changes)
    return CancelIntent(**values)  # type: ignore[arg-type]


def decision(*commands: OrderIntent | CancelIntent) -> PaperDecision:
    return PaperDecision(
        source_session_id=SESSION_ID,
        source_ingest_sequence=7,
        commands=commands,
    )


def match_result(**changes: object) -> MatchResult:
    values: dict[str, object] = {
        "paper_run_id": RUN_ID,
        "disposition": MatchDisposition.PROCESSED,
        "source_session_id": SESSION_ID,
        "source_ingest_sequence": 7,
        "fills": (),
        "events": (),
        "skip_reasons": (),
        "snapshot_version": 1,
    }
    values.update(changes)
    return MatchResult(**values)  # type: ignore[arg-type]


def paper_order(**changes: object) -> PaperOrder:
    intent = order_intent()
    values: dict[str, object] = {
        "paper_run_id": RUN_ID,
        "paper_order_id": ORDER_ID,
        "intent": intent,
        "status": OrderStatus.ACCEPTED,
        "filled_quantity": Decimal("0"),
        "remaining_quantity": intent.quantity,
        "average_fill_price": None,
        "accepted_at": NOW,
        "updated_at": NOW,
    }
    values.update(changes)
    return PaperOrder(**values)  # type: ignore[arg-type]


def rejection(**changes: object) -> PaperRejection:
    values: dict[str, object] = {
        "paper_run_id": RUN_ID,
        "strategy_id": "strategy-a",
        "client_order_id": "client-1",
        "code": RejectionCode.INVALID_INTENT,
        "rejected_at": NOW,
    }
    values.update(changes)
    return PaperRejection(**values)  # type: ignore[arg-type]


def paper_event(
    paper_sequence: int,
    payload: PaperOrder | PaperRejection,
) -> PaperEvent:
    event_type = (
        PaperEventType.ORDER_CANCELLED
        if type(payload) is PaperOrder
        else PaperEventType.ORDER_REJECTED
    )
    return PaperEvent(
        paper_run_id=RUN_ID,
        paper_event_id=UUID(f"44444444-4444-4444-8444-{paper_sequence:012d}"),
        paper_sequence=paper_sequence,
        event_type=event_type,
        payload=payload,
        occurred_at=NOW,
        source_session_id=SESSION_ID,
        source_ingest_sequence=7,
    )


def test_decision_is_frozen_strict_and_computes_its_own_fingerprint() -> None:
    value = decision(order_intent())

    assert value.decision_fingerprint.startswith("sha256:")
    assert len(value.decision_fingerprint) == 71
    assert canonical_json(value).endswith(f'"source_session_id":"{SESSION_ID}"}}')
    with pytest.raises(FrozenInstanceError):
        value.commands = ()  # type: ignore[misc]
    with pytest.raises(TypeError, match="unexpected keyword"):
        PaperDecision(  # type: ignore[call-arg]
            source_session_id=SESSION_ID,
            source_ingest_sequence=7,
            commands=(),
            decision_fingerprint="sha256:" + ("0" * 64),
        )
    with pytest.raises(TypeError, match="commands must be a tuple"):
        PaperDecision(
            source_session_id=SESSION_ID,
            source_ingest_sequence=7,
            commands=[],  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match="commands must contain only"):
        decision("submit")  # type: ignore[arg-type]


def test_decision_requires_exact_nonnegative_source_and_matching_commands() -> None:
    with pytest.raises(TypeError, match="source_session_id must be UUID"):
        PaperDecision(  # type: ignore[arg-type]
            source_session_id=str(SESSION_ID),
            source_ingest_sequence=7,
            commands=(),
        )
    with pytest.raises(TypeError, match="source_ingest_sequence must be an integer"):
        PaperDecision(
            source_session_id=SESSION_ID,
            source_ingest_sequence=True,  # type: ignore[arg-type]
            commands=(),
        )
    with pytest.raises(ValueError, match="source_ingest_sequence must be non-negative"):
        PaperDecision(
            source_session_id=SESSION_ID,
            source_ingest_sequence=-1,
            commands=(),
        )
    with pytest.raises(ValueError, match="source causation must match decision"):
        decision(order_intent(source_ingest_sequence=8))
    with pytest.raises(ValueError, match="source causation must match decision"):
        decision(
            cancel_intent(
                source_session_id=OTHER_SESSION_ID,
                source_ingest_sequence=7,
            )
        )
    with pytest.raises(ValueError, match="source causation must match decision"):
        decision(
            replace(
                order_intent(),
                source_session_id=None,
                source_ingest_sequence=None,
            )
        )


def test_decision_fingerprint_is_stable_ordered_and_domain_separated() -> None:
    submit = order_intent()
    cancel = cancel_intent()
    first = decision(submit, cancel)
    repeat = decision(submit, cancel)
    reversed_commands = decision(cancel, submit)
    changed_source = PaperDecision(
        source_session_id=SESSION_ID,
        source_ingest_sequence=8,
        commands=(
            replace(submit, source_ingest_sequence=8),
            replace(cancel, source_ingest_sequence=8),
        ),
    )
    changed_field = decision(replace(submit, client_order_id="client-2"), cancel)

    assert first.decision_fingerprint == repeat.decision_fingerprint
    assert first.decision_fingerprint != reversed_commands.decision_fingerprint
    assert first.decision_fingerprint != changed_source.decision_fingerprint
    assert first.decision_fingerprint != changed_field.decision_fingerprint


def test_decision_fingerprint_normalizes_semantically_equal_decimals() -> None:
    left = decision(order_intent(quantity=Decimal("2.0"), limit_price=Decimal("22100.50")))
    right = decision(order_intent(quantity=Decimal("2.00"), limit_price=Decimal("22100.500")))

    assert left.decision_fingerprint == right.decision_fingerprint


def test_empty_decision_has_a_stable_nontrivial_fingerprint() -> None:
    first = decision()
    second = decision()

    assert first.decision_fingerprint == second.decision_fingerprint
    assert first.decision_fingerprint != "sha256:" + ("0" * 64)


def test_batch_result_is_frozen_strict_and_run_source_consistent() -> None:
    fingerprint = decision(order_intent()).decision_fingerprint
    result = PaperDecisionBatchResult(
        paper_run_id=RUN_ID,
        source_session_id=SESSION_ID,
        source_ingest_sequence=7,
        decision_fingerprint=fingerprint,
        match_result=match_result(),
        command_results=(paper_order(), rejection()),
        events=(),
    )

    assert result.command_results[0].paper_run_id == RUN_ID
    with pytest.raises(FrozenInstanceError):
        result.command_results = ()  # type: ignore[misc]
    with pytest.raises(TypeError, match="command_results must be a tuple"):
        replace(result, command_results=[])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="command_results must contain only"):
        replace(result, command_results=("accepted",))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="events must be a tuple"):
        replace(result, events=[])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="events must contain only PaperEvent"):
        replace(result, events=("accepted",))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="match_result must be MatchResult"):
        replace(result, match_result="processed")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="decision_fingerprint"):
        replace(result, decision_fingerprint="0" * 64)
    with pytest.raises(ValueError, match="match_result paper_run_id"):
        replace(
            result,
            match_result=match_result(paper_run_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")),
        )
    with pytest.raises(ValueError, match="match_result source causation"):
        replace(result, match_result=match_result(source_ingest_sequence=8))
    with pytest.raises(ValueError, match="command result paper_run_id"):
        replace(
            result,
            command_results=(rejection(paper_run_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")),),
        )


def test_batch_cancel_result_may_contain_an_old_order_source_snapshot() -> None:
    cancel_decision = decision(cancel_intent())
    old_order = paper_order(
        intent=order_intent(
            source_ingest_sequence=3,
            client_order_id="original-submit",
        ),
        status=OrderStatus.CANCELLED,
    )

    result = PaperDecisionBatchResult(
        paper_run_id=RUN_ID,
        source_session_id=SESSION_ID,
        source_ingest_sequence=7,
        decision_fingerprint=cancel_decision.decision_fingerprint,
        match_result=match_result(),
        command_results=(old_order,),
        events=(paper_event(1, old_order),),
    )

    assert result.command_results == (old_order,)
    assert old_order.intent.source_ingest_sequence == 3


def test_batch_events_require_match_prefix_order_run_and_source_consistency() -> None:
    cancelled = paper_order(status=OrderStatus.CANCELLED)
    match_event = paper_event(1, cancelled)
    command_event = paper_event(2, rejection())
    match = match_result(events=(match_event,))
    result = PaperDecisionBatchResult(
        paper_run_id=RUN_ID,
        source_session_id=SESSION_ID,
        source_ingest_sequence=7,
        decision_fingerprint=decision(cancel_intent()).decision_fingerprint,
        match_result=match,
        command_results=(rejection(),),
        events=(match_event, command_event),
    )

    assert result.events == (match_event, command_event)
    with pytest.raises(ValueError, match="start with match_result events"):
        replace(result, events=(command_event, match_event))
    with pytest.raises(ValueError, match="event paper_run_id"):
        other_run_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
        replace(
            result,
            match_result=match_result(),
            events=(
                replace(
                    command_event,
                    paper_run_id=other_run_id,
                    payload=rejection(paper_run_id=other_run_id),
                ),
            ),
        )
    with pytest.raises(ValueError, match="event source causation"):
        replace(
            result,
            match_result=match_result(),
            events=(replace(command_event, source_ingest_sequence=8),),
        )
    with pytest.raises(ValueError, match="strictly increasing"):
        replace(
            result,
            match_result=match_result(),
            events=(command_event, match_event),
        )


def test_duplicate_batch_result_cannot_claim_command_side_effects() -> None:
    fingerprint = decision().decision_fingerprint
    duplicate = PaperDecisionBatchResult(
        paper_run_id=RUN_ID,
        source_session_id=SESSION_ID,
        source_ingest_sequence=7,
        decision_fingerprint=fingerprint,
        match_result=match_result(disposition=MatchDisposition.DUPLICATE),
        command_results=(),
        events=(),
    )

    with pytest.raises(ValueError, match="duplicate batch results"):
        replace(duplicate, command_results=(rejection(),))
    with pytest.raises(ValueError, match="duplicate batch results"):
        replace(duplicate, events=(paper_event(1, rejection()),))
