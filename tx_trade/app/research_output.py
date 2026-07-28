"""Deterministic, buffered JSONL materialization for research paper runs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from typing import Any
from uuid import UUID

from tx_trade.market_data.models import (
    MarketDataEnvelope,
    serialize_envelope,
    to_primitive,
)
from tx_trade.orders.contracts import (
    PaperBrokerSnapshot,
    PaperEvent,
    to_canonical_primitive,
)
from tx_trade.strategy.coordinator import StrategyDecisionRecord

OUTPUT_SCHEMA_VERSION = 1
_FAILURE_MESSAGE = "research output materialization failed"


class ResearchOutputError(RuntimeError):
    """A stable public failure which never contains input or serialization details."""


@dataclass(frozen=True, slots=True)
class ResearchOutputCorrelation:
    replay_session_id: UUID
    paper_run_id: UUID
    execution_config_fingerprint: str
    terminal_cursor: int

    def __post_init__(self) -> None:
        if type(self.replay_session_id) is not UUID:
            raise TypeError("replay_session_id must be UUID")
        if type(self.paper_run_id) is not UUID:
            raise TypeError("paper_run_id must be UUID")
        if (
            type(self.execution_config_fingerprint) is not str
            or not self.execution_config_fingerprint.startswith("sha256:")
            or len(self.execution_config_fingerprint) != 71
        ):
            raise ValueError("execution_config_fingerprint must be a sha256 fingerprint")
        if type(self.terminal_cursor) is not int or self.terminal_cursor < 0:
            raise ValueError("terminal_cursor must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class ResearchOutputLimits:
    max_market_records: int
    max_paper_events: int
    max_decision_records: int
    max_output_bytes: int

    def __post_init__(self) -> None:
        for name in (
            "max_market_records",
            "max_paper_events",
            "max_decision_records",
            "max_output_bytes",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"{name} must be a positive integer")


def materialize_research_jsonl(
    *,
    market_envelopes: tuple[MarketDataEnvelope, ...],
    decision_records: tuple[StrategyDecisionRecord, ...],
    broker_snapshot: PaperBrokerSnapshot,
    correlation: ResearchOutputCorrelation,
    limits: ResearchOutputLimits,
) -> bytes:
    """Validate a completed run and materialize its complete UTF-8 JSONL output."""

    try:
        return _materialize(
            market_envelopes,
            decision_records,
            broker_snapshot,
            correlation,
            limits,
        )
    except ResearchOutputError:
        raise
    except Exception:
        raise ResearchOutputError(_FAILURE_MESSAGE) from None


def _materialize(
    envelopes: tuple[MarketDataEnvelope, ...],
    decisions: tuple[StrategyDecisionRecord, ...],
    snapshot: PaperBrokerSnapshot,
    correlation: ResearchOutputCorrelation,
    limits: ResearchOutputLimits,
) -> bytes:
    if type(envelopes) is not tuple or any(
        type(item) is not MarketDataEnvelope for item in envelopes
    ):
        raise TypeError("market_envelopes must be a tuple of MarketDataEnvelope")
    if type(decisions) is not tuple or any(
        type(item) is not StrategyDecisionRecord for item in decisions
    ):
        raise TypeError("decision_records must be a tuple of StrategyDecisionRecord")
    if type(snapshot) is not PaperBrokerSnapshot:
        raise TypeError("broker_snapshot must be PaperBrokerSnapshot")
    if type(correlation) is not ResearchOutputCorrelation:
        raise TypeError("correlation must be ResearchOutputCorrelation")
    if type(limits) is not ResearchOutputLimits:
        raise TypeError("limits must be ResearchOutputLimits")
    if not envelopes:
        raise ValueError("a completed research run must contain market records")
    if len(envelopes) > limits.max_market_records:
        raise ValueError("market record capacity exceeded")
    if len(decisions) > limits.max_decision_records:
        raise ValueError("decision record capacity exceeded")
    if len(snapshot.events) > limits.max_paper_events:
        raise ValueError("paper event capacity exceeded")

    _validate_correlation(envelopes, decisions, snapshot, correlation)
    decision_by_source = {
        (record.source_session_id, record.source_ingest_sequence): record for record in decisions
    }
    lines: list[bytes] = []
    output_bytes = 0
    for envelope in envelopes:
        output_bytes = _append_encoded_line(
            lines,
            encode_market_record(envelope),
            limits.max_output_bytes,
            output_bytes,
        )
    for event in snapshot.events:
        record = (
            None
            if event.source_session_id is None or event.source_ingest_sequence is None
            else decision_by_source.get((event.source_session_id, event.source_ingest_sequence))
        )
        output_bytes = _append_encoded_line(
            lines,
            encode_paper_record(event, record),
            limits.max_output_bytes,
            output_bytes,
        )
    _append_encoded_line(
        lines,
        encode_summary_record(
            market_record_count=len(envelopes),
            decision_record_count=len(decisions),
            broker_snapshot=snapshot,
            correlation=correlation,
        ),
        limits.max_output_bytes,
        output_bytes,
    )
    return b"".join(lines)


def encode_market_record(envelope: MarketDataEnvelope) -> bytes:
    """Encode one schema-v1 market record as canonical newline-terminated JSON."""

    if type(envelope) is not MarketDataEnvelope:
        raise TypeError("envelope must be MarketDataEnvelope")
    return _encode_line(
        {
            "envelope": to_primitive(envelope),
            "record_type": "market",
            "schema_version": OUTPUT_SCHEMA_VERSION,
        }
    )


def encode_paper_record(
    event: PaperEvent,
    decision_record: StrategyDecisionRecord | None,
) -> bytes:
    """Encode one schema-v1 paper record as canonical newline-terminated JSON."""

    if type(event) is not PaperEvent:
        raise TypeError("event must be PaperEvent")
    if decision_record is not None and type(decision_record) is not StrategyDecisionRecord:
        raise TypeError("decision_record must be StrategyDecisionRecord or None")
    return _encode_line(
        {
            "decision_fingerprint": (
                None if decision_record is None else decision_record.decision.decision_fingerprint
            ),
            "envelope_digest": (
                None if decision_record is None else decision_record.envelope_digest
            ),
            "event": to_canonical_primitive(event),
            "record_type": "paper",
            "schema_version": OUTPUT_SCHEMA_VERSION,
        }
    )


def encode_summary_record(
    *,
    market_record_count: int,
    decision_record_count: int,
    broker_snapshot: PaperBrokerSnapshot,
    correlation: ResearchOutputCorrelation,
) -> bytes:
    """Encode the unique schema-v1 terminal summary record."""

    if type(market_record_count) is not int or market_record_count < 1:
        raise ValueError("market_record_count must be a positive integer")
    if type(decision_record_count) is not int or decision_record_count < 0:
        raise ValueError("decision_record_count must be a non-negative integer")
    if type(broker_snapshot) is not PaperBrokerSnapshot:
        raise TypeError("broker_snapshot must be PaperBrokerSnapshot")
    if type(correlation) is not ResearchOutputCorrelation:
        raise TypeError("correlation must be ResearchOutputCorrelation")
    return _encode_line(
        {
            "broker_snapshot_version": broker_snapshot.snapshot_version,
            "counts": {
                "decisions": decision_record_count,
                "fills": len(broker_snapshot.fills),
                "market_records": market_record_count,
                "orders": len(broker_snapshot.orders),
                "paper_events": len(broker_snapshot.events),
                "positions": len(broker_snapshot.positions),
            },
            "execution_config_fingerprint": correlation.execution_config_fingerprint,
            "paper_run_id": str(correlation.paper_run_id),
            "record_type": "summary",
            "replay_session_id": str(correlation.replay_session_id),
            "schema_version": OUTPUT_SCHEMA_VERSION,
            "terminal_broker_sequence": broker_snapshot.next_paper_sequence - 1,
            "terminal_cursor": correlation.terminal_cursor,
        }
    )


def _validate_correlation(
    envelopes: tuple[MarketDataEnvelope, ...],
    decisions: tuple[StrategyDecisionRecord, ...],
    snapshot: PaperBrokerSnapshot,
    correlation: ResearchOutputCorrelation,
) -> None:
    sequences = tuple(envelope.ingest_sequence for envelope in envelopes)
    if any(envelope.session_id != correlation.replay_session_id for envelope in envelopes):
        raise ValueError("market session correlation mismatch")
    if any(left >= right for left, right in zip(sequences, sequences[1:], strict=False)):
        raise ValueError("market records must be strictly ordered")
    if sequences[-1] != correlation.terminal_cursor:
        raise ValueError("terminal cursor correlation mismatch")
    if snapshot.paper_run_id != correlation.paper_run_id:
        raise ValueError("paper run correlation mismatch")
    if snapshot.execution_config_fingerprint != correlation.execution_config_fingerprint:
        raise ValueError("execution config correlation mismatch")
    if (
        snapshot.bound_source_session_id != correlation.replay_session_id
        or snapshot.last_committed_ingest_sequence != correlation.terminal_cursor
    ):
        raise ValueError("broker terminal correlation mismatch")

    envelope_by_sequence = {envelope.ingest_sequence: envelope for envelope in envelopes}
    decision_sequences: set[int] = set()
    for record in decisions:
        if record.source_session_id != correlation.replay_session_id:
            raise ValueError("decision session correlation mismatch")
        envelope = envelope_by_sequence.get(record.source_ingest_sequence)
        if envelope is None:
            raise ValueError("decision source is absent from market records")
        digest = sha256(serialize_envelope(envelope).encode("utf-8")).hexdigest()
        if record.envelope_digest != digest:
            raise ValueError("decision envelope correlation mismatch")
        if record.source_ingest_sequence in decision_sequences:
            raise ValueError("duplicate decision source")
        decision_sequences.add(record.source_ingest_sequence)
        if record.batch_result is None:
            raise ValueError("paper decision did not complete")
        if record.batch_result.paper_run_id != correlation.paper_run_id:
            raise ValueError("decision paper run correlation mismatch")
    if decision_sequences != set(sequences):
        raise ValueError("each market record must have one completed decision")

    for event in snapshot.events:
        _validate_event(event, correlation, envelope_by_sequence)


def _validate_event(
    event: PaperEvent,
    correlation: ResearchOutputCorrelation,
    envelope_by_sequence: dict[int, MarketDataEnvelope],
) -> None:
    if event.paper_run_id != correlation.paper_run_id:
        raise ValueError("event paper run correlation mismatch")
    if (
        event.source_session_id != correlation.replay_session_id
        or event.source_ingest_sequence not in envelope_by_sequence
    ):
        raise ValueError("event market source correlation mismatch")


def _append_line(
    lines: list[bytes],
    value: dict[str, Any],
    max_bytes: int,
    current_bytes: int,
) -> int:
    encoded = _encode_line(value)
    return _append_encoded_line(lines, encoded, max_bytes, current_bytes)


def _encode_line(value: dict[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        + b"\n"
    )


def _append_encoded_line(
    lines: list[bytes], encoded: bytes, max_bytes: int, current_bytes: int
) -> int:
    next_bytes = current_bytes + len(encoded)
    if next_bytes > max_bytes:
        raise ValueError("output capacity exceeded")
    lines.append(encoded)
    return next_bytes
