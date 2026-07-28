from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime
from decimal import Decimal
from uuid import UUID

import pytest

from tx_trade.orders import (
    ExecutionProvenance,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PaperEvent,
    PaperEventType,
    PaperFill,
    PaperOrder,
    PaperPosition,
    PaperRejection,
    RejectionCode,
    TimeInForce,
    canonical_json,
    to_canonical_primitive,
)
from tx_trade.orders.contracts import TAIPEI

RUN_ID = UUID("11111111-1111-4111-8111-111111111111")
ORDER_ID = UUID("22222222-2222-4222-8222-222222222222")
FILL_ID = UUID("33333333-3333-4333-8333-333333333333")
EVENT_ID = UUID("44444444-4444-4444-8444-444444444444")
SESSION_ID = UUID("55555555-5555-4555-8555-555555555555")
NOW = datetime(2026, 7, 28, 9, 0, tzinfo=TAIPEI)


def intent(**changes: object) -> OrderIntent:
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
    }
    values.update(changes)
    return OrderIntent(**values)  # type: ignore[arg-type]


def order(status: OrderStatus = OrderStatus.ACCEPTED, **changes: object) -> PaperOrder:
    quantities = {
        OrderStatus.ACCEPTED: (Decimal("0"), Decimal("2"), None),
        OrderStatus.PARTIALLY_FILLED: (Decimal("1"), Decimal("1"), Decimal("22100.5")),
        OrderStatus.FILLED: (Decimal("2"), Decimal("0"), Decimal("22100.5")),
        OrderStatus.CANCELLED: (Decimal("0"), Decimal("2"), None),
        OrderStatus.REJECTED: (Decimal("0"), Decimal("2"), None),
    }
    filled, remaining, average = quantities[status]
    values: dict[str, object] = {
        "paper_run_id": RUN_ID,
        "paper_order_id": ORDER_ID,
        "intent": intent(),
        "status": status,
        "filled_quantity": filled,
        "remaining_quantity": remaining,
        "average_fill_price": average,
        "accepted_at": None if status is OrderStatus.REJECTED else NOW,
        "updated_at": NOW,
    }
    values.update(changes)
    return PaperOrder(**values)  # type: ignore[arg-type]


def fill() -> PaperFill:
    return PaperFill(
        paper_run_id=RUN_ID,
        paper_fill_id=FILL_ID,
        paper_order_id=ORDER_ID,
        strategy_id="strategy-a",
        account_id="paper-account",
        instrument_id="TXF-202608",
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        execution_price=Decimal("22100.5"),
        fee=Decimal("15"),
        source_session_id=SESSION_ID,
        source_ingest_sequence=7,
        occurred_at=NOW,
    )


def test_intent_is_immutable_and_accepts_market_and_limit_contracts() -> None:
    limit = intent()
    market = intent(order_type=OrderType.MARKET, limit_price=None)

    assert limit.limit_price == Decimal("22100.5")
    assert market.order_type is OrderType.MARKET
    with pytest.raises(FrozenInstanceError):
        limit.quantity = Decimal("3")  # type: ignore[misc]


@pytest.mark.parametrize("value", [1, 1.0, True, "1", Decimal("NaN"), Decimal("Infinity")])
def test_intent_rejects_non_decimal_or_non_finite_quantity(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        intent(quantity=value)


@pytest.mark.parametrize("value", [Decimal("0"), Decimal("-1")])
def test_intent_rejects_non_positive_quantity(value: Decimal) -> None:
    with pytest.raises(ValueError, match="quantity must be greater than zero"):
        intent(quantity=value)


def test_intent_enforces_market_and_limit_price_rules() -> None:
    with pytest.raises(ValueError, match="market orders"):
        intent(order_type=OrderType.MARKET)
    with pytest.raises(TypeError, match="limit_price must be Decimal"):
        intent(limit_price=None)
    with pytest.raises(TypeError, match="limit_price must be Decimal"):
        intent(limit_price=22100.5)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("side", "buy"),
        ("order_type", "limit"),
        ("time_in_force", "day"),
        ("day_trade", 1),
        ("created_at", datetime(2026, 7, 28, 9, 0)),
    ],
)
def test_intent_rejects_implicit_coercion(field: str, value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        intent(**{field: value})


@pytest.mark.parametrize("status", list(OrderStatus))
def test_paper_order_has_conserved_quantity_for_every_status(status: OrderStatus) -> None:
    snapshot = order(status)

    assert snapshot.filled_quantity + snapshot.remaining_quantity == snapshot.intent.quantity
    assert snapshot.provenance is ExecutionProvenance.PAPER
    assert snapshot.status.is_terminal is (
        status in {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}
    )


def test_paper_order_rejects_inconsistent_quantity_status_and_average() -> None:
    with pytest.raises(ValueError, match="must equal intent quantity"):
        order(remaining_quantity=Decimal("1"))
    with pytest.raises(ValueError, match="average_fill_price"):
        order(average_fill_price=Decimal("1"))
    with pytest.raises(ValueError, match="partial filled_quantity"):
        order(
            OrderStatus.PARTIALLY_FILLED,
            filled_quantity=Decimal("2"),
            remaining_quantity=Decimal("0"),
        )
    with pytest.raises(ValueError, match="remaining quantity"):
        order(
            OrderStatus.CANCELLED,
            filled_quantity=Decimal("2"),
            remaining_quantity=Decimal("0"),
            average_fill_price=Decimal("1"),
        )


def test_paper_results_require_strict_paper_provenance() -> None:
    with pytest.raises(TypeError, match="provenance"):
        replace(order(), provenance="paper")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="provenance"):
        replace(fill(), provenance="broker")  # type: ignore[arg-type]


def test_fill_requires_finite_values_and_source_pair() -> None:
    recorded = fill()
    assert recorded.source_ingest_sequence == 7

    with pytest.raises(TypeError, match="execution_price must be Decimal"):
        replace(recorded, execution_price=1.0)
    with pytest.raises(ValueError, match="fee must be non-negative"):
        replace(recorded, fee=Decimal("-1"))
    with pytest.raises(ValueError, match="provided together"):
        replace(recorded, source_session_id=None)  # type: ignore[arg-type]


def test_position_enforces_flat_and_open_average_invariant() -> None:
    flat = PaperPosition(
        paper_run_id=RUN_ID,
        strategy_id="strategy-a",
        account_id="paper-account",
        instrument_id="TXF-202608",
        net_quantity=Decimal("0"),
        average_open_price=None,
        cumulative_fees=Decimal("15"),
        version=1,
        updated_at=NOW,
    )
    short = replace(
        flat,
        net_quantity=Decimal("-2"),
        average_open_price=Decimal("22000"),
        version=2,
    )

    assert short.net_quantity == Decimal("-2")
    with pytest.raises(ValueError, match="average_open_price"):
        replace(flat, average_open_price=Decimal("1"))
    with pytest.raises(TypeError, match="version must be an integer"):
        replace(flat, version=True)


def test_rejection_exposes_only_stable_public_message() -> None:
    rejection = PaperRejection(
        paper_run_id=RUN_ID,
        strategy_id="strategy-a",
        client_order_id="client-1",
        code=RejectionCode.IDEMPOTENCY_CONFLICT,
        rejected_at=NOW,
    )

    assert rejection.message == "paper order idempotency conflict"
    assert "credential-canary" not in rejection.message
    assert len({code.public_message for code in RejectionCode}) == len(RejectionCode)


@pytest.mark.parametrize(
    ("event_type", "status"),
    [
        (PaperEventType.ORDER_ACCEPTED, OrderStatus.ACCEPTED),
        (PaperEventType.ORDER_PARTIALLY_FILLED, OrderStatus.PARTIALLY_FILLED),
        (PaperEventType.ORDER_FILLED, OrderStatus.FILLED),
        (PaperEventType.ORDER_CANCELLED, OrderStatus.CANCELLED),
    ],
)
def test_order_event_type_must_match_order_status(
    event_type: PaperEventType, status: OrderStatus
) -> None:
    source = (
        {
            "source_session_id": SESSION_ID,
            "source_ingest_sequence": 7,
        }
        if status in {OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}
        else {}
    )
    event = PaperEvent(
        paper_run_id=RUN_ID,
        paper_event_id=EVENT_ID,
        paper_sequence=1,
        event_type=event_type,
        payload=order(status),
        occurred_at=NOW,
        **source,  # type: ignore[arg-type]
    )

    assert event.payload.status is status  # type: ignore[union-attr]
    with pytest.raises(ValueError, match="must match"):
        replace(event, payload=order(OrderStatus.REJECTED))


def test_fill_event_requires_matching_source_causation() -> None:
    event = PaperEvent(
        paper_run_id=RUN_ID,
        paper_event_id=EVENT_ID,
        paper_sequence=2,
        event_type=PaperEventType.FILL_RECORDED,
        payload=fill(),
        occurred_at=NOW,
        source_session_id=SESSION_ID,
        source_ingest_sequence=7,
    )

    assert event.provenance is ExecutionProvenance.PAPER
    with pytest.raises(ValueError, match="must match event"):
        replace(event, source_ingest_sequence=8)
    with pytest.raises(ValueError, match="provided together"):
        replace(event, source_session_id=None)
    with pytest.raises(ValueError, match="payload time"):
        replace(event, occurred_at=NOW.replace(minute=1))


def test_position_change_event_requires_source_causation() -> None:
    position = PaperPosition(
        paper_run_id=RUN_ID,
        strategy_id="strategy-a",
        account_id="paper-account",
        instrument_id="TXF-202608",
        net_quantity=Decimal("1"),
        average_open_price=Decimal("22100.5"),
        cumulative_fees=Decimal("15"),
        version=1,
        updated_at=NOW,
    )
    with pytest.raises(ValueError, match="require market-data source"):
        PaperEvent(
            paper_run_id=RUN_ID,
            paper_event_id=EVENT_ID,
            paper_sequence=3,
            event_type=PaperEventType.POSITION_CHANGED,
            payload=position,
            occurred_at=NOW,
        )


def test_canonical_json_is_compact_sorted_and_byte_stable() -> None:
    event = PaperEvent(
        paper_run_id=RUN_ID,
        paper_event_id=EVENT_ID,
        paper_sequence=2,
        event_type=PaperEventType.FILL_RECORDED,
        payload=fill(),
        occurred_at=NOW,
        source_session_id=SESSION_ID,
        source_ingest_sequence=7,
    )

    first = canonical_json(event)
    second = canonical_json(event)

    assert first == second
    assert " " not in first
    assert first.startswith('{"event_type":"fill_recorded"')
    assert '"execution_price":"22100.5"' in first


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        (Decimal("4.0"), Decimal("4.00"), '"quantity":"4"'),
        (Decimal("1E+3"), Decimal("1000.00"), '"quantity":"1e3"'),
        (Decimal("1.2300E-3"), Decimal("0.0012300"), '"quantity":"0.00123"'),
    ],
)
def test_canonical_json_normalizes_decimal_representation_without_float(
    left: Decimal, right: Decimal, expected: str
) -> None:
    first = canonical_json(intent(quantity=left))
    second = canonical_json(intent(quantity=right))

    assert first == second
    assert expected in first


def test_canonical_json_normalizes_positive_and_negative_zero() -> None:
    position = PaperPosition(
        paper_run_id=RUN_ID,
        strategy_id="strategy-a",
        account_id="paper-account",
        instrument_id="TXF-202608",
        net_quantity=Decimal("0"),
        average_open_price=None,
        cumulative_fees=Decimal("0"),
        version=1,
        updated_at=NOW,
    )

    first = canonical_json(position)
    second = canonical_json(
        replace(
            position,
            net_quantity=Decimal("-0"),
            cumulative_fees=Decimal("-0"),
        )
    )

    assert first == second
    assert '"net_quantity":"0"' in first
    assert '"cumulative_fees":"0"' in first


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("1e1000000"), "1e1000000"),
        (Decimal("1e-1000000"), "1e-1000000"),
    ],
)
def test_canonical_decimal_does_not_expand_extreme_exponents(value: Decimal, expected: str) -> None:
    serialized = canonical_json(intent(quantity=value))

    assert f'"quantity":"{expected}"' in serialized
    assert len(serialized) < 500


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")])
def test_canonical_decimal_rejects_non_finite_values(value: Decimal) -> None:
    with pytest.raises(ValueError, match="must be finite"):
        to_canonical_primitive(value)
