"""Pure validation for deterministic paper-order state transitions."""

from __future__ import annotations

from .contracts import OrderStatus, PaperOrder

_ALLOWED_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.ACCEPTED: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
        }
    ),
    OrderStatus.PARTIALLY_FILLED: frozenset(
        {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
        }
    ),
    OrderStatus.FILLED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
}


class InvalidOrderTransition(ValueError):
    """Raised when two valid snapshots cannot form a legal transition."""


def can_transition(previous: OrderStatus, current: OrderStatus) -> bool:
    if type(previous) is not OrderStatus:
        raise TypeError("previous must be OrderStatus")
    if type(current) is not OrderStatus:
        raise TypeError("current must be OrderStatus")
    return current in _ALLOWED_TRANSITIONS[previous]


def validate_order_transition(previous: PaperOrder, current: PaperOrder) -> None:
    """Validate identity, monotonic progress, and the complete status graph."""

    if type(previous) is not PaperOrder:
        raise TypeError("previous must be PaperOrder")
    if type(current) is not PaperOrder:
        raise TypeError("current must be PaperOrder")
    if previous.paper_run_id != current.paper_run_id:
        raise InvalidOrderTransition("paper_run_id must not change")
    if previous.paper_order_id != current.paper_order_id:
        raise InvalidOrderTransition("paper_order_id must not change")
    if previous.intent != current.intent:
        raise InvalidOrderTransition("intent must not change")
    if previous.provenance is not current.provenance:
        raise InvalidOrderTransition("provenance must not change")
    if previous.accepted_at != current.accepted_at:
        raise InvalidOrderTransition("accepted_at must not change")
    if not can_transition(previous.status, current.status):
        raise InvalidOrderTransition(
            f"transition from {previous.status.value} to {current.status.value} is not allowed"
        )
    if current.updated_at < previous.updated_at:
        raise InvalidOrderTransition("updated_at must not move backwards")
    if current.filled_quantity < previous.filled_quantity:
        raise InvalidOrderTransition("filled_quantity must not decrease")
    if current.remaining_quantity > previous.remaining_quantity:
        raise InvalidOrderTransition("remaining_quantity must not increase")
    if (
        current.filled_quantity == previous.filled_quantity
        and current.average_fill_price != previous.average_fill_price
    ):
        raise InvalidOrderTransition(
            "average_fill_price must not change without additional filled quantity"
        )
    if (
        current.status is OrderStatus.PARTIALLY_FILLED
        and current.filled_quantity <= previous.filled_quantity
    ):
        raise InvalidOrderTransition("partial-fill transitions must add filled quantity")
    if (
        current.status is OrderStatus.CANCELLED
        and current.filled_quantity != previous.filled_quantity
    ):
        raise InvalidOrderTransition("cancellation must not add filled quantity")
