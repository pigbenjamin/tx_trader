from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from uuid import UUID

from tx_trade.market_data.fixtures import (
    InMemoryReplaySource,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import SCHEMA_VERSION, EventType, Quote
from tx_trade.orders.contracts import (
    MatchDisposition,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PaperBrokerLimits,
    TimeInForce,
    canonical_json,
)
from tx_trade.orders.paper_broker import PaperBroker
from tx_trade.replay.contracts import (
    ReplayFailureCode,
    ReplayMode,
    ReplayOptions,
    ReplaySessionDescriptor,
    ReplayState,
)
from tx_trade.replay.runtime import ReplayRuntime

RUN_ID = UUID("22222222-2222-2222-2222-222222222222")
LIMITS = PaperBrokerLimits(
    max_orders=10,
    max_open_orders=10,
    max_fills=20,
    max_events=50,
    max_market_data_records=20,
    max_instrument_versions=10,
)


def _broker() -> PaperBroker:
    events = make_offline_fixture_envelopes()
    return PaperBroker(
        paper_run_id=RUN_ID,
        expected_source_session_id=events[0].session_id,
        limits=LIMITS,
    )


def _intent(client_order_id: str = "replay-order") -> OrderIntent:
    events = make_offline_fixture_envelopes()
    quote = events[3].payload
    assert isinstance(quote, Quote)
    return OrderIntent(
        strategy_id="integration-strategy",
        client_order_id=client_order_id,
        account_id="paper",
        instrument_id=quote.instrument_id,
        side=OrderSide.BUY,
        quantity=Decimal("2"),
        order_type=OrderType.MARKET,
        limit_price=None,
        time_in_force=TimeInForce.DAY,
        day_trade=False,
        created_at=quote.received_at,
    )


def _descriptor(events: tuple) -> ReplaySessionDescriptor:
    return ReplaySessionDescriptor(
        session_id=events[0].session_id,
        status="complete",
        schema_version=SCHEMA_VERSION,
        event_count=len(events),
        first_ingest_sequence=events[0].ingest_sequence,
        last_ingest_sequence=events[-1].ingest_sequence,
    )


def _runtime(
    events: tuple,
    sink: object,
    *,
    after_ingest_sequence: int | None = None,
) -> ReplayRuntime:
    return ReplayRuntime(
        source=InMemoryReplaySource(events),
        descriptor=_descriptor(events),
        sink=sink,  # type: ignore[arg-type]
        options=ReplayOptions(
            mode=ReplayMode.FASTEST,
            after_ingest_sequence=after_ingest_sequence,
        ),
    )


def _canonical_journal(broker: PaperBroker) -> bytes:
    events = broker.snapshot().events
    return ("[" + ",".join(canonical_json(event) for event in events) + "]").encode()


def test_complete_replay_is_byte_deterministic_with_direct_broker_sink() -> None:
    events = make_offline_fixture_envelopes()
    journals: list[bytes] = []

    for _ in range(2):
        broker = _broker()
        broker.submit(_intent())
        result = _runtime(events, broker).run()

        assert result.state is ReplayState.COMPLETED
        assert result.cursor == events[-1].ingest_sequence
        assert len(broker.snapshot().fills) == 1
        journals.append(_canonical_journal(broker))

    assert journals[0] == journals[1]


def test_sequence_gaps_and_non_quotes_preserve_matching_causality() -> None:
    original = make_offline_fixture_envelopes()
    sequences = (10, 20, 40, 70, 90, 120)
    events = tuple(
        replace(
            envelope,
            ingest_sequence=sequence,
            dedupe_key=f"integration-gap:{sequence}",
        )
        for envelope, sequence in zip(original, sequences, strict=True)
    )
    broker = _broker()
    order = broker.submit(_intent("gapped"))

    class ObservingSink:
        def __init__(self) -> None:
            self.fill_counts: list[tuple[EventType, int]] = []

        def publish(self, envelope) -> None:
            broker.publish(envelope)
            self.fill_counts.append((envelope.event_type, len(broker.snapshot().fills)))

    sink = ObservingSink()
    result = _runtime(events, sink).run()

    assert result.state is ReplayState.COMPLETED
    assert result.cursor == 120
    assert sink.fill_counts[:3] == [
        (EventType.CONNECTION_STATUS, 0),
        (EventType.SERVER_TIME, 0),
        (EventType.INSTRUMENT, 0),
    ]
    assert sink.fill_counts[3:] == [
        (EventType.QUOTE, 1),
        (EventType.TICK, 1),
        (EventType.ADAPTER_DIAGNOSTIC, 1),
    ]
    assert broker.snapshot().last_committed_ingest_sequence == 120
    assert broker.get_order(order.paper_order_id).status is OrderStatus.FILLED


def test_exact_duplicate_direct_delivery_does_not_duplicate_fill() -> None:
    events = make_offline_fixture_envelopes()
    broker = _broker()
    broker.submit(_intent("duplicate"))
    result = _runtime(events, broker).run()
    assert result.state is ReplayState.COMPLETED

    before = broker.snapshot()
    duplicate = broker.process_market_data(events[3])

    assert duplicate.disposition is MatchDisposition.DUPLICATE
    assert duplicate.fills == ()
    assert broker.snapshot() == before


def test_pre_delivery_sink_failure_keeps_cursor_and_broker_state_for_retry() -> None:
    events = make_offline_fixture_envelopes()
    broker = _broker()
    broker.submit(_intent("retry"))
    failed_envelope = events[3]

    class FailBeforeQuoteSink:
        def publish(self, envelope) -> None:
            if envelope is failed_envelope:
                raise RuntimeError("injected before broker delivery")
            broker.publish(envelope)

    failed = _runtime(events, FailBeforeQuoteSink()).run()
    after_failure = broker.snapshot()

    assert failed.state is ReplayState.FAILED
    assert failed.failure_code is ReplayFailureCode.SINK_FAILED
    assert failed.cursor == events[2].ingest_sequence
    assert after_failure.last_committed_ingest_sequence == events[2].ingest_sequence
    assert after_failure.fills == ()

    retried = _runtime(
        events,
        broker,
        after_ingest_sequence=failed.cursor,
    ).run()

    assert retried.state is ReplayState.COMPLETED
    assert retried.cursor == events[-1].ingest_sequence
    assert len(broker.snapshot().fills) == 1
