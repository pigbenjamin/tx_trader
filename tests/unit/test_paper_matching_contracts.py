from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime
from decimal import Decimal
from uuid import UUID

import pytest

from tx_trade.orders import (
    InstrumentMetadataSnapshot,
    MatchDisposition,
    MatchResult,
    MatchSkipReason,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PaperBrokerLimits,
    PaperBrokerSnapshot,
    PaperEvent,
    PaperEventType,
    PaperFill,
    PaperOrder,
    TimeInForce,
    canonical_json,
)
from tx_trade.orders.contracts import TAIPEI

RUN_ID = UUID("11111111-1111-4111-8111-111111111111")
OTHER_RUN_ID = UUID("99999999-9999-4999-8999-999999999999")
ORDER_ID = UUID("22222222-2222-4222-8222-222222222222")
FILL_ID = UUID("33333333-3333-4333-8333-333333333333")
EVENT_ID = UUID("44444444-4444-4444-8444-444444444444")
SESSION_ID = UUID("55555555-5555-4555-8555-555555555555")
NOW = datetime(2026, 7, 28, 9, 0, tzinfo=TAIPEI)


def make_order(*, paper_run_id: UUID = RUN_ID) -> PaperOrder:
    intent = OrderIntent(
        strategy_id="strategy-a",
        client_order_id="client-1",
        account_id="paper-account",
        instrument_id="TXF-202608",
        side=OrderSide.BUY,
        quantity=Decimal("2"),
        order_type=OrderType.LIMIT,
        limit_price=Decimal("22100.5"),
        time_in_force=TimeInForce.DAY,
        day_trade=True,
        created_at=NOW,
    )
    return PaperOrder(
        paper_run_id=paper_run_id,
        paper_order_id=ORDER_ID,
        intent=intent,
        status=OrderStatus.ACCEPTED,
        filled_quantity=Decimal("0"),
        remaining_quantity=Decimal("2"),
        average_fill_price=None,
        accepted_at=NOW,
        updated_at=NOW,
    )


def make_fill(
    *,
    paper_run_id: UUID = RUN_ID,
    source_ingest_sequence: int = 7,
) -> PaperFill:
    return PaperFill(
        paper_run_id=paper_run_id,
        paper_fill_id=FILL_ID,
        paper_order_id=ORDER_ID,
        strategy_id="strategy-a",
        account_id="paper-account",
        instrument_id="TXF-202608",
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        execution_price=Decimal("22100.5"),
        fee=Decimal("0"),
        source_session_id=SESSION_ID,
        source_ingest_sequence=source_ingest_sequence,
        occurred_at=NOW,
    )


def make_event(*, paper_run_id: UUID = RUN_ID) -> PaperEvent:
    order = make_order(paper_run_id=paper_run_id)
    return PaperEvent(
        paper_run_id=paper_run_id,
        paper_event_id=EVENT_ID,
        paper_sequence=1,
        event_type=PaperEventType.ORDER_ACCEPTED,
        payload=order,
        occurred_at=NOW,
    )


def make_fill_event(*, source_ingest_sequence: int = 7) -> PaperEvent:
    fill = make_fill(source_ingest_sequence=source_ingest_sequence)
    return PaperEvent(
        paper_run_id=RUN_ID,
        paper_event_id=EVENT_ID,
        paper_sequence=2,
        event_type=PaperEventType.FILL_RECORDED,
        payload=fill,
        occurred_at=NOW,
        source_session_id=SESSION_ID,
        source_ingest_sequence=source_ingest_sequence,
    )


def test_limits_are_frozen_strict_positive_and_internally_bounded() -> None:
    limits = PaperBrokerLimits(
        max_orders=100,
        max_open_orders=50,
        max_fills=200,
        max_events=500,
        max_market_data_records=1_000,
        max_instrument_versions=100,
    )

    with pytest.raises(FrozenInstanceError):
        limits.max_orders = 101  # type: ignore[misc]
    with pytest.raises(ValueError, match="must not exceed"):
        replace(limits, max_open_orders=101)


@pytest.mark.parametrize(
    "field",
    [
        "max_orders",
        "max_open_orders",
        "max_fills",
        "max_events",
        "max_market_data_records",
        "max_instrument_versions",
    ],
)
def test_every_limit_rejects_bool_zero_and_negative(field: str) -> None:
    limits = PaperBrokerLimits(
        max_orders=100,
        max_open_orders=50,
        max_fills=200,
        max_events=500,
        max_market_data_records=1_000,
        max_instrument_versions=100,
    )

    with pytest.raises(TypeError, match=rf"{field} must be an integer"):
        replace(limits, **{field: True})
    for value in (0, -1):
        with pytest.raises(ValueError, match=rf"{field} must be at least 1"):
            replace(limits, **{field: value})


def test_match_result_requires_immutable_typed_collections_and_consistent_causation() -> None:
    result = MatchResult(
        paper_run_id=RUN_ID,
        disposition=MatchDisposition.PROCESSED,
        source_session_id=SESSION_ID,
        source_ingest_sequence=7,
        fills=(make_fill(),),
        events=(),
        skip_reasons=(MatchSkipReason.LIMIT_NOT_CROSSED,),
        snapshot_version=2,
    )

    assert result.fills[0].fee == 0
    assert '"disposition":"processed"' in canonical_json(result)
    with pytest.raises(TypeError, match="fills must be a tuple"):
        replace(result, fills=[])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="skip_reasons must contain only"):
        replace(result, skip_reasons=("limit_not_crossed",))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="source causation must match"):
        replace(result, source_ingest_sequence=8)
    with pytest.raises(ValueError, match="event source causation must match"):
        replace(result, events=(make_fill_event(source_ingest_sequence=8),))
    with pytest.raises(ValueError, match="paper_run_id must match"):
        replace(result, fills=(make_fill(paper_run_id=OTHER_RUN_ID),))


def test_duplicate_match_result_cannot_claim_new_side_effects() -> None:
    duplicate = MatchResult(
        paper_run_id=RUN_ID,
        disposition=MatchDisposition.DUPLICATE,
        source_session_id=SESSION_ID,
        source_ingest_sequence=7,
        fills=(),
        events=(),
        skip_reasons=(),
        snapshot_version=2,
    )

    assert duplicate.disposition is MatchDisposition.DUPLICATE
    with pytest.raises(ValueError, match="must not contain"):
        replace(duplicate, events=(make_event(),))


def test_snapshot_is_immutable_ordered_and_run_consistent() -> None:
    metadata = InstrumentMetadataSnapshot(
        instrument_id="TXF-202608",
        metadata_version=3,
        price_scale=Decimal("0.01"),
        quantity_scale=Decimal("1"),
    )
    event = make_event()
    snapshot = PaperBrokerSnapshot(
        paper_run_id=RUN_ID,
        bound_source_session_id=SESSION_ID,
        last_committed_ingest_sequence=7,
        next_paper_sequence=2,
        snapshot_version=3,
        orders=(make_order(),),
        fills=(make_fill(),),
        events=(event,),
        instruments=(metadata,),
    )

    assert snapshot.events == (event,)
    assert canonical_json(snapshot) == canonical_json(snapshot)
    with pytest.raises(FrozenInstanceError):
        snapshot.events = ()  # type: ignore[misc]
    with pytest.raises(TypeError, match="orders must be a tuple"):
        replace(snapshot, orders=[])  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="provided together"):
        replace(snapshot, last_committed_ingest_sequence=None)
    with pytest.raises(ValueError, match="must follow"):
        replace(snapshot, next_paper_sequence=3)
    with pytest.raises(ValueError, match="paper_run_id must match"):
        replace(snapshot, orders=(make_order(paper_run_id=OTHER_RUN_ID),))


def test_empty_snapshot_starts_at_sequence_one_and_version_zero() -> None:
    snapshot = PaperBrokerSnapshot(
        paper_run_id=RUN_ID,
        bound_source_session_id=None,
        last_committed_ingest_sequence=None,
        next_paper_sequence=1,
        snapshot_version=0,
        orders=(),
        fills=(),
        events=(),
        instruments=(),
    )

    assert snapshot.snapshot_version == 0
    with pytest.raises(TypeError, match="snapshot_version must be an integer"):
        replace(snapshot, snapshot_version=True)
    with pytest.raises(ValueError, match="snapshot_version must be non-negative"):
        replace(snapshot, snapshot_version=-1)
