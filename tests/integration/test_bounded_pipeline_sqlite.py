from dataclasses import replace
from threading import Event

import pytest

from tx_trade.market_data.fixtures import make_offline_fixture_envelopes
from tx_trade.market_data.ingress import (
    BoundedIngress,
    BoundedIngressProcessor,
    PipelineStorageFailureNotifier,
)
from tx_trade.market_data.models import (
    CapturedKind,
    CapturedMarketDataEvent,
    CapturedQuoteSnapshot,
    EventType,
)
from tx_trade.market_data.pipeline import CapturedEventPipeline
from tx_trade.market_data.ports import IngressDecision, RecordingSession
from tx_trade.market_data.sequencer import IngestSequencer
from tx_trade.monitoring.health import (
    ControlledShutdown,
    HealthState,
    PipelineHealth,
    SessionImpactTracker,
)
from tx_trade.monitoring.metrics import IngressMetrics
from tx_trade.storage import (
    SQLiteMarketDataRepository,
    SQLiteMarketDataWriter,
    StorageError,
)


class Clock:
    def __init__(self, now):
        self._now = now

    def now(self):
        return self._now


class QuoteMapper:
    def __init__(self, template):
        self._template = template

    def validate(self, event):
        assert event.captured_kind is CapturedKind.QUOTE_SNAPSHOT

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
            dedupe_key=f"mapped:{event.sequence}",
            event_at=event.event_at,
            received_at=event.received_at,
            trading_day=event.trading_day,
            metadata_version=event.metadata_version,
            raw_payload=event.raw_payload,
        )


def captured_quote(template, sequence, symbol=7, dedupe=None):
    payload = CapturedQuoteSnapshot(
        0,
        symbol,
        2_000_000,
        2_000_200,
        2_000_100,
        1,
        1,
        1,
        False,
        sequence,
        template.received_at,
    )
    return CapturedMarketDataEvent(
        CapturedKind.QUOTE_SNAPSHOT,
        payload,
        {"sequence": sequence},
        template.source,
        template.source_mode,
        template.session_id,
        template.connection_generation,
        sequence,
        None,
        template.received_at,
        template.event_at,
        template.trading_day,
        template.metadata_version,
        dedupe,
    )


def begin(repository, template):
    repository.begin_session(
        RecordingSession(
            template.session_id,
            template.schema_version,
            template.source,
            template.source_mode,
            template.received_at,
            template.trading_day,
            "bounded-integration",
        )
    )


def components(template):
    health = PipelineHealth(Clock(template.received_at))
    metrics = IngressMetrics()
    impact = SessionImpactTracker(2)
    shutdown = ControlledShutdown()
    ingress = BoundedIngress(
        control_capacity=1,
        diagnostic_capacity=1,
        quote_capacity=1,
        tick_capacity=1,
        dedupe_capacity=8,
        health=health,
        metrics=metrics,
        session_impact=impact,
        shutdown=shutdown,
    )
    return ingress, health, metrics, impact, shutdown


def test_accepted_drain_then_actual_sqlite_writer_persists_only_coalesced(
    tmp_path,
):
    template = next(
        item
        for item in make_offline_fixture_envelopes()
        if item.event_type is EventType.QUOTE
    )
    repository = SQLiteMarketDataRepository(tmp_path / "bounded.db")
    begin(repository, template)
    writer = SQLiteMarketDataWriter(
        repository,
        capacity=4,
        batch_size=2,
        flush_interval_seconds=10,
    )
    writer.start()
    ingress, health, metrics, impact, shutdown = components(template)
    sequencer = IngestSequencer()
    processor = BoundedIngressProcessor(
        ingress,
        CapturedEventPipeline(QuoteMapper(template), writer, sequencer),
        health,
        metrics,
        impact,
        shutdown,
    )
    assert (
        ingress.try_publish(captured_quote(template, 3, dedupe="first"))
        is IngressDecision.ACCEPTED
    )
    assert (
        ingress.try_publish(captured_quote(template, 4, dedupe="newer"))
        is IngressDecision.COALESCED
    )
    assert (
        ingress.try_publish(captured_quote(template, 2, dedupe="stale"))
        is IngressDecision.DUPLICATE
    )
    assert (
        ingress.try_publish(captured_quote(template, 5, symbol=8, dedupe="full"))
        is IngressDecision.DROPPED
    )
    assert sequencer.peek_last(template.session_id) is None
    assert tuple(repository.iter_events(template.session_id)) == ()
    assert processor.drain(10) == 1
    assert sequencer.peek_last(template.session_id) == 0
    writer.flush(timeout=2)
    actual = tuple(repository.iter_events(template.session_id))
    assert len(actual) == 1
    assert actual[0].sequence == 4
    assert actual[0].ingest_sequence == 0
    writer.stop(timeout=2)


def test_actual_background_writer_fatal_notifies_pipeline_and_db_incomplete(
    tmp_path,
):
    class FailingRepository(SQLiteMarketDataRepository):
        failed = Event()

        def append_batch(self, events):
            self.failed.set()
            raise StorageError("deterministic append failure")

    template = make_offline_fixture_envelopes()[0]
    repository = FailingRepository(tmp_path / "fatal.db")
    begin(repository, template)
    _, health, metrics, impact, shutdown = components(template)
    notifier = PipelineStorageFailureNotifier(
        template.session_id, health, metrics, impact, shutdown
    )
    writer = SQLiteMarketDataWriter(
        repository,
        capacity=2,
        batch_size=1,
        flush_interval_seconds=10,
        notifier=notifier,
    )
    writer.start()
    writer.publish(template)
    assert repository.failed.wait(1)
    assert writer._thread is not None
    writer._thread.join(1)
    assert health.snapshot().state is HealthState.FAILED
    assert impact.effective_terminal_status(
        template.session_id, "complete"
    ) == "incomplete"
    assert shutdown.snapshot().request_count == 1
    assert metrics.snapshot().storage_failures == 1
    repository.end_session(
        template.session_id,
        template.received_at,
        impact.effective_terminal_status(template.session_id, "complete"),
    )
    assert repository.get_session(template.session_id).status == "incomplete"
    with pytest.raises(StorageError, match="deterministic append failure"):
        writer.stop(timeout=1)
