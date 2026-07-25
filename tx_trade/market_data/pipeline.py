"""Synchronous captured-event pipeline contract."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .models import CapturedMarketDataEvent, MarketDataEnvelope
from .ports import MarketDataSink
from .sequencer import IngestSequencer


@runtime_checkable
class CapturedEventMapper(Protocol):
    """Validate and map a captured event without embedding broker semantics here."""

    def validate(self, event: CapturedMarketDataEvent) -> None: ...

    def build_envelope(
        self, event: CapturedMarketDataEvent, ingest_sequence: int
    ) -> MarketDataEnvelope: ...


class CapturedEventPipeline:
    """Validate, sequence, map, and synchronously publish one captured event."""

    def __init__(
        self,
        mapper: CapturedEventMapper,
        sink: MarketDataSink,
        sequencer: IngestSequencer | None = None,
    ) -> None:
        self._mapper = mapper
        self._sink = sink
        self._sequencer = sequencer if sequencer is not None else IngestSequencer()

    def accept(self, event: CapturedMarketDataEvent) -> MarketDataEnvelope:
        if type(event) is not CapturedMarketDataEvent:
            raise TypeError("event must be CapturedMarketDataEvent")

        self._mapper.validate(event)
        ingest_sequence = self._sequencer.next(event.session_id)
        envelope = self._mapper.build_envelope(event, ingest_sequence)
        if type(envelope) is not MarketDataEnvelope:
            raise TypeError("mapper must return MarketDataEnvelope")
        if envelope.ingest_sequence != ingest_sequence:
            raise ValueError("mapper changed ingest_sequence")
        duplicated_metadata = (
            "source",
            "source_mode",
            "session_id",
            "connection_generation",
            "sequence",
            "broker_sequence",
            "received_at",
            "event_at",
            "trading_day",
            "metadata_version",
            "raw_payload",
        )
        for name in duplicated_metadata:
            if getattr(envelope, name) != getattr(event, name):
                raise ValueError(f"mapper changed {name}")
        self._sink.publish(envelope)
        return envelope
