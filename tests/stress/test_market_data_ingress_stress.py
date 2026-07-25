from collections import Counter
from dataclasses import replace
from datetime import datetime
import gc
from threading import Event, Lock, Thread
import tracemalloc
from uuid import UUID

from tx_trade.market_data.fixtures import make_offline_fixture_envelopes
from tx_trade.market_data.ingress import BoundedIngress, BoundedIngressProcessor
from tx_trade.market_data.models import (
    CapturedKind,
    CapturedMarketDataEvent,
    CapturedTickNotification,
    SourceMode,
    TAIPEI,
)
from tx_trade.market_data.pipeline import CapturedEventPipeline
from tx_trade.market_data.sequencer import IngestSequencer
from tx_trade.monitoring.health import (
    ControlledShutdown,
    PipelineHealth,
    SessionImpactTracker,
)
from tx_trade.monitoring.metrics import IngressLane, IngressMetrics

NOW = datetime(2026, 7, 26, 9, 30, tzinfo=TAIPEI)
SESSION = UUID("12345678-1234-5678-1234-567812345678")


class Clock:
    def now(self):
        return NOW


def tick(sequence=0):
    payload = CapturedTickNotification(
        0, 1, 0, 20260726, 93000, 0, 1, 2, 1, 1, 0, False, sequence, NOW
    )
    return CapturedMarketDataEvent(
        CapturedKind.TICK_NOTIFICATION,
        payload,
        None,
        "stress",
        SourceMode.OFFLINE,
        SESSION,
        1,
        sequence,
        None,
        NOW,
        NOW,
        None,
        None,
        None,
    )


def make_ingress(capacity=512, dedupe=128):
    metrics = IngressMetrics()
    health = PipelineHealth(Clock(), max_reasons=4)
    impact = SessionImpactTracker(1, max_reasons=4)
    ingress = BoundedIngress(
        control_capacity=8,
        diagnostic_capacity=8,
        quote_capacity=8,
        tick_capacity=capacity,
        dedupe_capacity=dedupe,
        health=health,
        metrics=metrics,
        session_impact=impact,
        shutdown=ControlledShutdown(),
    )
    return ingress, metrics, health, impact


def test_one_million_distinct_publishes_have_bounded_retained_memory():
    capacity = 512
    dedupe_capacity = 128
    ingress, metrics, health, impact = make_ingress(capacity, dedupe_capacity)
    template = tick()
    for sequence in range(capacity):
        event = replace(
            template,
            sequence=sequence,
            payload=replace(template.payload, callback_sequence=sequence),
            dedupe_candidate=f"warmup-{sequence}",
        )
        ingress.try_publish(event)

    total = 1_000_000
    measured = 100_000
    trace_warmup = 1_024
    untraced = total - measured - trace_warmup
    for sequence in range(capacity, capacity + untraced):
        event = replace(
            template,
            sequence=sequence,
            payload=replace(template.payload, callback_sequence=sequence),
            dedupe_candidate=f"distinct-{sequence}",
        )
        ingress.try_publish(event)

    tracemalloc.start()
    measured_start = capacity + untraced
    for sequence in range(measured_start, measured_start + trace_warmup):
        event = replace(
            template,
            sequence=sequence,
            payload=replace(template.payload, callback_sequence=sequence),
            dedupe_candidate=f"distinct-{sequence}",
        )
        ingress.try_publish(event)
    gc.collect()
    baseline_current, _ = tracemalloc.get_traced_memory()
    tracemalloc.reset_peak()
    sample_start = measured_start + trace_warmup
    for sequence in range(sample_start, sample_start + measured):
        event = replace(
            template,
            sequence=sequence,
            payload=replace(template.payload, callback_sequence=sequence),
            dedupe_candidate=f"distinct-{sequence}",
        )
        ingress.try_publish(event)
    del event
    gc.collect()
    retained_current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    internal = ingress.snapshot()
    metric = metrics.snapshot()
    session = impact.snapshot(SESSION)
    assert internal.tick_depth == capacity
    assert internal.dedupe_depth == dedupe_capacity
    assert impact.tracked_session_count == 1
    assert len(health.snapshot().reasons) <= 4
    assert len(session.reasons) <= 4
    expected_received = capacity + total
    assert metric.received[IngressLane.TICK] == expected_received
    assert expected_received == sum(
        values[IngressLane.TICK]
        for values in (
            metric.accepted,
            metric.coalesced,
            metric.dropped,
            metric.duplicates,
        )
    )
    assert metric.dropped_tick_count == session.dropped_tick_count
    assert metric.dropped_tick_count == total
    assert impact.effective_terminal_status(SESSION, "complete") == "incomplete"
    retained_delta = retained_current - baseline_current
    assert retained_delta < 4 * 1024 * 1024
    assert peak >= retained_current
    print(
        f"retained_delta_bytes={retained_delta} "
        f"traced_peak_bytes={peak} traced_events={measured}"
    )


class SequentialMapper:
    def __init__(self, template):
        self._template = template

    def validate(self, event):
        assert event.captured_kind is CapturedKind.TICK_NOTIFICATION

    def build_envelope(self, event, ingest_sequence):
        return replace(
            self._template,
            source=event.source,
            source_mode=event.source_mode,
            session_id=event.session_id,
            ingest_sequence=ingest_sequence,
            connection_generation=event.connection_generation,
            sequence=event.sequence,
            broker_sequence=event.broker_sequence,
            dedupe_key=f"stress:{event.sequence}",
            event_at=event.event_at,
            received_at=event.received_at,
            trading_day=event.trading_day,
            metadata_version=event.metadata_version,
            raw_payload=event.raw_payload,
        )


class OnlineSequenceSink:
    def __init__(self):
        self.count = 0
        self.last = None

    def publish(self, envelope):
        assert envelope.ingest_sequence == self.count
        assert self.last is None or envelope.ingest_sequence == self.last + 1
        self.last = envelope.ingest_sequence
        self.count += 1


def test_100k_processor_and_sequencer_long_run_without_retaining_envelopes():
    template = make_offline_fixture_envelopes()[4]
    metrics = IngressMetrics()
    health = PipelineHealth(Clock(), max_reasons=4)
    impact = SessionImpactTracker(1, max_reasons=4)
    shutdown = ControlledShutdown()
    ingress = BoundedIngress(
        control_capacity=1,
        diagnostic_capacity=1,
        quote_capacity=1,
        tick_capacity=1,
        dedupe_capacity=32,
        health=health,
        metrics=metrics,
        session_impact=impact,
        shutdown=shutdown,
    )
    sequencer = IngestSequencer()
    sink = OnlineSequenceSink()
    processor = BoundedIngressProcessor(
        ingress,
        CapturedEventPipeline(SequentialMapper(template), sink, sequencer),
        health,
        metrics,
        impact,
        shutdown,
    )
    payload_template = CapturedTickNotification(
        template.payload.market_no_raw,
        template.payload.stock_idx_raw,
        template.payload.source_pointer_raw,
        template.payload.date_raw,
        template.payload.time_hms_raw,
        template.payload.time_subsecond_raw,
        template.payload.bid_raw,
        template.payload.ask_raw,
        template.payload.close_raw,
        template.payload.quantity_raw,
        template.payload.simulate_raw,
        template.payload.is_long_callback,
        0,
        template.received_at,
    )
    event_template = CapturedMarketDataEvent(
        CapturedKind.TICK_NOTIFICATION,
        payload_template,
        None,
        template.source,
        template.source_mode,
        template.session_id,
        template.connection_generation,
        0,
        None,
        template.received_at,
        template.event_at,
        template.trading_day,
        template.metadata_version,
        "long-run-0",
    )
    total = 100_000
    for sequence in range(total):
        event = replace(
            event_template,
            sequence=sequence,
            payload=replace(payload_template, callback_sequence=sequence),
            dedupe_candidate=f"long-run-{sequence}",
        )
        assert ingress.try_publish(event).value == "accepted"
        assert processor.process_one()

    assert sink.count == total
    assert sink.last == total - 1
    assert sequencer.peek_last(template.session_id) == total - 1
    assert ingress.depth() == 0
    assert ingress.snapshot().dedupe_depth == 32
    assert impact.tracked_session_count == 0
    snapshot = metrics.snapshot()
    assert snapshot.received[IngressLane.TICK] == total
    assert snapshot.accepted[IngressLane.TICK] == total


def test_concurrent_producers_and_consumer_use_atomic_aggregates_only():
    ingress, metrics, _, _ = make_ingress(capacity=256)
    producer_count = 4
    per_producer = 10_000
    finished = Event()
    aggregate_lock = Lock()
    outcomes = Counter()
    accepted_sum = 0
    accepted_xor = 0
    consumed_count = 0
    consumed_sum = 0
    consumed_xor = 0

    def producer(index):
        nonlocal accepted_sum, accepted_xor
        local = Counter()
        local_sum = 0
        local_xor = 0
        start = index * per_producer
        template = tick(start)
        for sequence in range(start, start + per_producer):
            event = replace(
                template,
                sequence=sequence,
                payload=replace(template.payload, callback_sequence=sequence),
            )
            decision = ingress.try_publish(event)
            local[decision.value] += 1
            if decision.value == "accepted":
                local_sum += sequence
                local_xor ^= sequence
        with aggregate_lock:
            outcomes.update(local)
            accepted_sum += local_sum
            accepted_xor ^= local_xor

    def consume():
        nonlocal consumed_count, consumed_sum, consumed_xor
        while not finished.is_set() or ingress.depth() > 0:
            event = ingress.try_pop()
            if event is None:
                continue
            consumed_count += 1
            consumed_sum += event.sequence
            consumed_xor ^= event.sequence

    consumer = Thread(target=consume)
    consumer.start()
    producers = [Thread(target=producer, args=(index,)) for index in range(4)]
    for producer_thread in producers:
        producer_thread.start()
    for producer_thread in producers:
        producer_thread.join()
    finished.set()
    consumer.join()

    total = producer_count * per_producer
    assert sum(outcomes.values()) == total
    assert consumed_count == outcomes["accepted"]
    assert consumed_sum == accepted_sum
    assert consumed_xor == accepted_xor
    assert ingress.depth() == 0
    snapshot = metrics.snapshot()
    assert snapshot.received[IngressLane.TICK] == total
    assert snapshot.received[IngressLane.TICK] == sum(
        mapping[IngressLane.TICK]
        for mapping in (
            snapshot.accepted,
            snapshot.coalesced,
            snapshot.dropped,
            snapshot.duplicates,
        )
    )
