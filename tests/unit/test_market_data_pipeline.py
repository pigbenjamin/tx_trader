from dataclasses import replace
from datetime import date, datetime, timedelta
from uuid import UUID

import pytest

from tx_trade.market_data.fixtures import make_offline_fixture_envelopes
from tx_trade.market_data.models import (
    CapturedConnectionNotification,
    CapturedKind,
    CapturedMarketDataEvent,
    SourceMode,
    TAIPEI,
)
from tx_trade.market_data.pipeline import CapturedEventPipeline
from tx_trade.market_data.sequencer import IngestSequencer

NOW = datetime(2026, 7, 26, 9, 30, tzinfo=TAIPEI)
SESSION = UUID("12345678-1234-5678-1234-567812345678")


def make_captured(sequence=3):
    payload = CapturedConnectionNotification(3003, 0, sequence, NOW)
    return CapturedMarketDataEvent(
        captured_kind=CapturedKind.CONNECTION_NOTIFICATION,
        payload=payload,
        raw_payload={"kind": 3003},
        source="fixture",
        source_mode=SourceMode.OFFLINE,
        session_id=SESSION,
        connection_generation=7,
        sequence=sequence,
        broker_sequence=None,
        received_at=NOW,
        event_at=NOW,
        trading_day=None,
        metadata_version=None,
        dedupe_candidate="fixed",
    )


class Mapper:
    def __init__(self):
        self.validation_error = None
        self.build_error = None
        self.tamper = None

    def validate(self, event):
        if self.validation_error:
            raise self.validation_error

    def build_envelope(self, event, ingest_sequence):
        if self.build_error:
            raise self.build_error
        fixture = make_offline_fixture_envelopes()[0]
        payload = replace(
            fixture.payload,
            changed_at=event.event_at,
            connection_generation=event.connection_generation,
        )
        envelope = replace(
            fixture,
            payload=payload,
            source=event.source,
            source_mode=event.source_mode,
            session_id=event.session_id,
            ingest_sequence=ingest_sequence,
            sequence=event.sequence,
            connection_generation=event.connection_generation,
            broker_sequence=event.broker_sequence,
            received_at=event.received_at,
            event_at=event.event_at,
            trading_day=event.trading_day,
            metadata_version=event.metadata_version,
            raw_payload=event.raw_payload,
        )
        return self.tamper(envelope) if self.tamper else envelope


class Sink:
    def __init__(self):
        self.events = []
        self.error = None

    def publish(self, envelope):
        if self.error:
            raise self.error
        self.events.append(envelope)


def test_validate_failure_does_not_allocate_or_publish():
    mapper, sink, sequencer = Mapper(), Sink(), IngestSequencer()
    mapper.validation_error = ValueError("invalid capture")
    pipeline = CapturedEventPipeline(mapper, sink, sequencer)
    with pytest.raises(ValueError, match="invalid capture"):
        pipeline.accept(make_captured())
    assert sequencer.peek_last(SESSION) is None
    assert sink.events == []


def test_success_allocates_then_builds_and_publishes():
    mapper, sink, sequencer = Mapper(), Sink(), IngestSequencer()
    pipeline = CapturedEventPipeline(mapper, sink, sequencer)
    first = pipeline.accept(make_captured(3))
    second = pipeline.accept(make_captured(4))
    assert [first.ingest_sequence, second.ingest_sequence] == [0, 1]
    assert sink.events == [first, second]


@pytest.mark.parametrize("failure_stage", ["build", "publish"])
def test_build_or_publish_failure_may_leave_sequence_gap(failure_stage):
    mapper, sink, sequencer = Mapper(), Sink(), IngestSequencer()
    error = RuntimeError(f"{failure_stage} failed")
    if failure_stage == "build":
        mapper.build_error = error
    else:
        sink.error = error
    pipeline = CapturedEventPipeline(mapper, sink, sequencer)
    with pytest.raises(RuntimeError, match="failed"):
        pipeline.accept(make_captured())
    assert sequencer.peek_last(SESSION) == 0
    assert sink.events == []


def test_pipeline_rejects_non_capture_before_mapper():
    with pytest.raises(TypeError):
        CapturedEventPipeline(Mapper(), Sink()).accept(object())


def _changed_generation(envelope):
    generation = envelope.connection_generation + 1
    return replace(
        envelope,
        payload=replace(envelope.payload, connection_generation=generation),
        connection_generation=generation,
    )


def _changed_event_at(envelope):
    changed_at = envelope.event_at + timedelta(seconds=1)
    return replace(
        envelope,
        payload=replace(envelope.payload, changed_at=changed_at),
        event_at=changed_at,
    )


@pytest.mark.parametrize(
    ("field", "tamper"),
    [
        ("source", lambda envelope: replace(envelope, source="changed")),
        (
            "source_mode",
            lambda envelope: replace(envelope, source_mode=SourceMode.LIVE),
        ),
        ("session_id", lambda envelope: replace(envelope, session_id=UUID(int=0))),
        ("connection_generation", _changed_generation),
        ("sequence", lambda envelope: replace(envelope, sequence=999)),
        ("broker_sequence", lambda envelope: replace(envelope, broker_sequence=99)),
        (
            "received_at",
            lambda envelope: replace(
                envelope, received_at=envelope.received_at + timedelta(seconds=1)
            ),
        ),
        ("event_at", _changed_event_at),
        (
            "trading_day",
            lambda envelope: replace(envelope, trading_day=date(2026, 7, 27)),
        ),
        ("metadata_version", lambda envelope: replace(envelope, metadata_version=2)),
        ("raw_payload", lambda envelope: replace(envelope, raw_payload={"changed": 1})),
        (
            "ingest_sequence",
            lambda envelope: replace(
                envelope, ingest_sequence=envelope.ingest_sequence + 1
            ),
        ),
    ],
)
def test_pipeline_rejects_mapper_metadata_tampering_without_publish(field, tamper):
    mapper, sink, sequencer = Mapper(), Sink(), IngestSequencer()
    mapper.tamper = tamper
    pipeline = CapturedEventPipeline(mapper, sink, sequencer)
    with pytest.raises(ValueError, match=field):
        pipeline.accept(make_captured())
    assert sequencer.peek_last(SESSION) == 0
    assert sink.events == []
