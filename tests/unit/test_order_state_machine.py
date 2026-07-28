from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import product
from uuid import UUID

import pytest

from tx_trade.orders import (
    InvalidOrderTransition,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PaperOrder,
    TimeInForce,
    can_transition,
    validate_order_transition,
)
from tx_trade.orders.contracts import TAIPEI

RUN_ID = UUID("11111111-1111-4111-8111-111111111111")
ORDER_ID = UUID("22222222-2222-4222-8222-222222222222")
NOW = datetime(2026, 7, 28, 9, 0, tzinfo=TAIPEI)

EXPECTED = {
    (OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED),
    (OrderStatus.ACCEPTED, OrderStatus.FILLED),
    (OrderStatus.ACCEPTED, OrderStatus.CANCELLED),
    (OrderStatus.PARTIALLY_FILLED, OrderStatus.PARTIALLY_FILLED),
    (OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED),
    (OrderStatus.PARTIALLY_FILLED, OrderStatus.CANCELLED),
}


def snapshot(
    status: OrderStatus,
    *,
    filled: Decimal | None = None,
    quantity: Decimal = Decimal("4"),
    updated_at: datetime = NOW,
) -> PaperOrder:
    default_filled = {
        OrderStatus.ACCEPTED: Decimal("0"),
        OrderStatus.PARTIALLY_FILLED: Decimal("1"),
        OrderStatus.FILLED: quantity,
        OrderStatus.CANCELLED: Decimal("0"),
        OrderStatus.REJECTED: Decimal("0"),
    }[status]
    actual_filled = default_filled if filled is None else filled
    intent = OrderIntent(
        strategy_id="strategy-a",
        client_order_id="client-1",
        account_id="paper-account",
        instrument_id="TXF-202608",
        side=OrderSide.BUY,
        quantity=quantity,
        order_type=OrderType.MARKET,
        limit_price=None,
        time_in_force=TimeInForce.DAY,
        day_trade=True,
        created_at=NOW,
    )
    return PaperOrder(
        paper_run_id=RUN_ID,
        paper_order_id=ORDER_ID,
        intent=intent,
        status=status,
        filled_quantity=actual_filled,
        remaining_quantity=quantity - actual_filled,
        average_fill_price=None if actual_filled == 0 else Decimal("22100"),
        accepted_at=None if status is OrderStatus.REJECTED else NOW,
        updated_at=updated_at,
    )


@pytest.mark.parametrize(("previous", "current"), list(product(OrderStatus, repeat=2)))
def test_complete_transition_cartesian_matrix(previous: OrderStatus, current: OrderStatus) -> None:
    assert can_transition(previous, current) is ((previous, current) in EXPECTED)


@pytest.mark.parametrize(
    "terminal", [OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED]
)
@pytest.mark.parametrize("current", list(OrderStatus))
def test_terminal_states_never_transition(terminal: OrderStatus, current: OrderStatus) -> None:
    assert not can_transition(terminal, current)


def test_transition_validation_accepts_progressive_partial_fill_and_full_fill() -> None:
    accepted = snapshot(OrderStatus.ACCEPTED)
    partial = snapshot(
        OrderStatus.PARTIALLY_FILLED,
        filled=Decimal("1"),
        updated_at=NOW + timedelta(seconds=1),
    )
    more_partial = snapshot(
        OrderStatus.PARTIALLY_FILLED,
        filled=Decimal("3"),
        updated_at=NOW + timedelta(seconds=2),
    )
    filled = snapshot(OrderStatus.FILLED, updated_at=NOW + timedelta(seconds=3))

    validate_order_transition(accepted, partial)
    validate_order_transition(partial, more_partial)
    validate_order_transition(more_partial, filled)


def test_transition_validation_accepts_cancel_without_new_fill() -> None:
    partial = snapshot(OrderStatus.PARTIALLY_FILLED, filled=Decimal("1"))
    cancelled = snapshot(
        OrderStatus.CANCELLED,
        filled=Decimal("1"),
        updated_at=NOW + timedelta(seconds=1),
    )

    validate_order_transition(partial, cancelled)


def test_transition_validation_rejects_identity_and_intent_changes() -> None:
    accepted = snapshot(OrderStatus.ACCEPTED)
    partial = snapshot(OrderStatus.PARTIALLY_FILLED)

    with pytest.raises(InvalidOrderTransition, match="paper_run_id"):
        validate_order_transition(accepted, replace(partial, paper_run_id=UUID(int=9)))
    with pytest.raises(InvalidOrderTransition, match="paper_order_id"):
        validate_order_transition(accepted, replace(partial, paper_order_id=UUID(int=8)))
    with pytest.raises(InvalidOrderTransition, match="intent"):
        validate_order_transition(
            accepted,
            replace(partial, intent=replace(partial.intent, client_order_id="different")),
        )


def test_transition_validation_rejects_backwards_time_or_fill_progress() -> None:
    partial = snapshot(
        OrderStatus.PARTIALLY_FILLED,
        filled=Decimal("2"),
        updated_at=NOW + timedelta(seconds=2),
    )
    later_status_less_fill = snapshot(
        OrderStatus.PARTIALLY_FILLED,
        filled=Decimal("1"),
        updated_at=NOW + timedelta(seconds=3),
    )
    earlier_time = snapshot(
        OrderStatus.PARTIALLY_FILLED,
        filled=Decimal("3"),
        updated_at=NOW + timedelta(seconds=1),
    )

    with pytest.raises(InvalidOrderTransition, match="filled_quantity must not decrease"):
        validate_order_transition(partial, later_status_less_fill)
    with pytest.raises(InvalidOrderTransition, match="updated_at"):
        validate_order_transition(partial, earlier_time)


def test_repeated_partial_status_requires_additional_fill() -> None:
    partial = snapshot(OrderStatus.PARTIALLY_FILLED)

    with pytest.raises(InvalidOrderTransition, match="must add filled quantity"):
        validate_order_transition(partial, partial)


def test_cancel_transition_cannot_simultaneously_add_fill() -> None:
    partial = snapshot(OrderStatus.PARTIALLY_FILLED, filled=Decimal("1"))
    cancelled = snapshot(
        OrderStatus.CANCELLED,
        filled=Decimal("2"),
        updated_at=NOW + timedelta(seconds=1),
    )

    with pytest.raises(InvalidOrderTransition, match="cancellation must not add"):
        validate_order_transition(partial, cancelled)


def test_partial_to_cancel_cannot_change_acceptance_or_average_without_fill() -> None:
    partial = snapshot(OrderStatus.PARTIALLY_FILLED, filled=Decimal("1"))
    cancelled = snapshot(
        OrderStatus.CANCELLED,
        filled=Decimal("1"),
        updated_at=NOW + timedelta(seconds=1),
    )

    with pytest.raises(InvalidOrderTransition, match="accepted_at must not change"):
        validate_order_transition(
            partial,
            replace(cancelled, accepted_at=NOW + timedelta(milliseconds=1)),
        )
    with pytest.raises(InvalidOrderTransition, match="average_fill_price must not change"):
        validate_order_transition(
            partial,
            replace(cancelled, average_fill_price=Decimal("22101")),
        )


def test_state_machine_rejects_non_contract_arguments() -> None:
    with pytest.raises(TypeError, match="previous must be OrderStatus"):
        can_transition("accepted", OrderStatus.FILLED)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="previous must be PaperOrder"):
        validate_order_transition(object(), snapshot(OrderStatus.FILLED))  # type: ignore[arg-type]
