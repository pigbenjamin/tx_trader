"""Strict, storage-agnostic contracts for durable research-paper runs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from hashlib import sha256
from uuid import UUID

MAX_CHECKPOINT_BYTES = 64 * 1024 * 1024
MAX_OUTBOX_PAYLOAD_BYTES = 64 * 1024 * 1024
MAX_BATCH_OUTBOX_RECORDS = 1_000_001
MAX_STRATEGIES = 1_024
MAX_VERSION = 2**63 - 1


class ResearchRunStatus(StrEnum):
    ACTIVE = "active"
    COMPLETE = "complete"
    FAILED = "failed"


class CheckpointKind(StrEnum):
    BROKER = "broker"
    COORDINATOR = "coordinator"


class ResearchOutboxRecordType(StrEnum):
    MARKET = "market"
    PAPER = "paper"
    SUMMARY = "summary"


class DurableBatchDisposition(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"


class ResearchPersistenceErrorCode(StrEnum):
    NOT_FOUND = "not_found"
    ALREADY_EXISTS = "already_exists"
    CONFLICT = "conflict"
    CORRUPT = "corrupt"
    SCHEMA_MISMATCH = "schema_mismatch"
    CAPACITY = "capacity"
    CLOSED = "closed"
    IO_FAILURE = "io_failure"


_ERROR_MESSAGES = {
    ResearchPersistenceErrorCode.NOT_FOUND: "research run was not found",
    ResearchPersistenceErrorCode.ALREADY_EXISTS: "research run already exists",
    ResearchPersistenceErrorCode.CONFLICT: "research run state conflict",
    ResearchPersistenceErrorCode.CORRUPT: "research state is corrupt",
    ResearchPersistenceErrorCode.SCHEMA_MISMATCH: "research state schema is unsupported",
    ResearchPersistenceErrorCode.CAPACITY: "research state capacity was exceeded",
    ResearchPersistenceErrorCode.CLOSED: "research state repository is closed",
    ResearchPersistenceErrorCode.IO_FAILURE: "research state storage failed",
}


class ResearchPersistenceError(RuntimeError):
    """A stable persistence failure that never exposes payloads or backend details."""

    def __init__(self, code: ResearchPersistenceErrorCode) -> None:
        if type(code) is not ResearchPersistenceErrorCode:
            raise TypeError("code must be ResearchPersistenceErrorCode")
        self.code = code
        super().__init__(_ERROR_MESSAGES[code])


def _uuid(value: object, name: str) -> None:
    if type(value) is not UUID:
        raise TypeError(f"{name} must be UUID")


def _int(
    value: object,
    name: str,
    *,
    minimum: int = 0,
    maximum: int = MAX_VERSION,
) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} is outside the supported range")


def _identifier(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if not value or value.strip() != value or len(value) > 128:
        raise ValueError(f"{name} must be a non-empty identifier of at most 128 characters")


def _fingerprint(value: object, name: str) -> None:
    if type(value) is not str:
        raise TypeError(f"{name} must be a string")
    if (
        len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{name} must be a canonical sha256 fingerprint")


def _taipei_datetime(value: object, name: str, *, optional: bool = False) -> None:
    if value is None and optional:
        return
    if type(value) is not datetime:
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    if getattr(value.tzinfo, "key", None) != "Asia/Taipei":
        raise ValueError(f"{name} must use Asia/Taipei timezone")


def _digest_bytes(domain: bytes, payload: bytes) -> str:
    return f"sha256:{sha256(domain + payload).hexdigest()}"


@dataclass(frozen=True, slots=True)
class StrategyFingerprint:
    strategy_id: str
    fingerprint: str

    def __post_init__(self) -> None:
        _identifier(self.strategy_id, "strategy_id")
        _fingerprint(self.fingerprint, "fingerprint")


@dataclass(frozen=True, slots=True)
class ResearchRunIdentity:
    paper_run_id: UUID
    source_session_id: UUID
    source_schema_version: int
    source_event_count: int
    source_first_sequence: int
    source_last_sequence: int
    source_content_fingerprint: str
    research_config_fingerprint: str
    execution_config_fingerprint: str
    strategy_fingerprints: tuple[StrategyFingerprint, ...]
    output_schema_version: int
    broker_algorithm_version: str
    identity_fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        _uuid(self.paper_run_id, "paper_run_id")
        _uuid(self.source_session_id, "source_session_id")
        _int(self.source_schema_version, "source_schema_version", minimum=1)
        _int(self.source_event_count, "source_event_count", minimum=1)
        _int(self.source_first_sequence, "source_first_sequence")
        _int(self.source_last_sequence, "source_last_sequence")
        if self.source_first_sequence > self.source_last_sequence:
            raise ValueError("source_first_sequence must not exceed source_last_sequence")
        if self.source_event_count > self.source_last_sequence - self.source_first_sequence + 1:
            raise ValueError("source_event_count cannot exceed the source sequence span")
        for name in (
            "source_content_fingerprint",
            "research_config_fingerprint",
            "execution_config_fingerprint",
        ):
            _fingerprint(getattr(self, name), name)
        if type(self.strategy_fingerprints) is not tuple:
            raise TypeError("strategy_fingerprints must be a tuple")
        if not self.strategy_fingerprints or len(self.strategy_fingerprints) > MAX_STRATEGIES:
            raise ValueError("strategy_fingerprints count is outside the supported range")
        if any(type(item) is not StrategyFingerprint for item in self.strategy_fingerprints):
            raise TypeError("strategy_fingerprints must contain StrategyFingerprint")
        strategy_ids = tuple(item.strategy_id for item in self.strategy_fingerprints)
        if strategy_ids != tuple(sorted(strategy_ids)) or len(set(strategy_ids)) != len(
            strategy_ids
        ):
            raise ValueError("strategy_fingerprints must be sorted and unique by strategy_id")
        _int(self.output_schema_version, "output_schema_version", minimum=1)
        _identifier(self.broker_algorithm_version, "broker_algorithm_version")
        material = {
            "broker_algorithm_version": self.broker_algorithm_version,
            "execution_config_fingerprint": self.execution_config_fingerprint,
            "output_schema_version": self.output_schema_version,
            "paper_run_id": str(self.paper_run_id),
            "research_config_fingerprint": self.research_config_fingerprint,
            "source_content_fingerprint": self.source_content_fingerprint,
            "source_event_count": self.source_event_count,
            "source_first_sequence": self.source_first_sequence,
            "source_last_sequence": self.source_last_sequence,
            "source_schema_version": self.source_schema_version,
            "source_session_id": str(self.source_session_id),
            "strategy_fingerprints": [
                {"fingerprint": item.fingerprint, "strategy_id": item.strategy_id}
                for item in self.strategy_fingerprints
            ],
        }
        encoded = json.dumps(
            material, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        object.__setattr__(
            self,
            "identity_fingerprint",
            _digest_bytes(b"tx_trade.research.run_identity.v1:", encoded),
        )


@dataclass(frozen=True, slots=True)
class VersionedCheckpoint:
    kind: CheckpointKind
    schema_version: int
    payload: bytes
    payload_sha256: str

    def __post_init__(self) -> None:
        if type(self.kind) is not CheckpointKind:
            raise TypeError("kind must be CheckpointKind")
        _int(self.schema_version, "schema_version", minimum=1)
        if type(self.payload) is not bytes:
            raise TypeError("payload must be bytes")
        if not self.payload or len(self.payload) > MAX_CHECKPOINT_BYTES:
            raise ValueError("payload size is outside the supported checkpoint range")
        _fingerprint(self.payload_sha256, "payload_sha256")
        expected = _digest_bytes(b"tx_trade.research.checkpoint.v1:", self.payload)
        if self.payload_sha256 != expected:
            raise ValueError("payload_sha256 does not match payload")

    @classmethod
    def create(
        cls, *, kind: CheckpointKind, schema_version: int, payload: bytes
    ) -> VersionedCheckpoint:
        if type(payload) is not bytes:
            raise TypeError("payload must be bytes")
        return cls(
            kind=kind,
            schema_version=schema_version,
            payload=payload,
            payload_sha256=_digest_bytes(b"tx_trade.research.checkpoint.v1:", payload),
        )


@dataclass(frozen=True, slots=True)
class ResearchOutboxRecord:
    paper_run_id: UUID
    output_sequence: int
    record_type: ResearchOutboxRecordType
    source_ingest_sequence: int | None
    paper_sequence: int | None
    payload: bytes
    payload_sha256: str

    def __post_init__(self) -> None:
        _uuid(self.paper_run_id, "paper_run_id")
        _int(self.output_sequence, "output_sequence")
        if type(self.record_type) is not ResearchOutboxRecordType:
            raise TypeError("record_type must be ResearchOutboxRecordType")
        if self.source_ingest_sequence is not None:
            _int(self.source_ingest_sequence, "source_ingest_sequence")
        if self.paper_sequence is not None:
            _int(self.paper_sequence, "paper_sequence")
        if self.record_type is ResearchOutboxRecordType.MARKET:
            if self.source_ingest_sequence is None or self.paper_sequence is not None:
                raise ValueError("market records require only source_ingest_sequence")
        elif self.record_type is ResearchOutboxRecordType.PAPER:
            if self.source_ingest_sequence is None or self.paper_sequence is None:
                raise ValueError("paper records require source and paper sequence")
        elif self.source_ingest_sequence is not None or self.paper_sequence is not None:
            raise ValueError("summary records must not have source or paper sequence")
        if type(self.payload) is not bytes:
            raise TypeError("payload must be bytes")
        if (
            not self.payload
            or len(self.payload) > MAX_OUTBOX_PAYLOAD_BYTES
            or not self.payload.endswith(b"\n")
            or self.payload.count(b"\n") != 1
        ):
            raise ValueError("payload must be one bounded newline-terminated record")
        _fingerprint(self.payload_sha256, "payload_sha256")
        if self.payload_sha256 != _digest_bytes(b"tx_trade.research.outbox.v1:", self.payload):
            raise ValueError("payload_sha256 does not match payload")

    @classmethod
    def create(
        cls,
        *,
        paper_run_id: UUID,
        output_sequence: int,
        record_type: ResearchOutboxRecordType,
        source_ingest_sequence: int | None,
        paper_sequence: int | None,
        payload: bytes,
    ) -> ResearchOutboxRecord:
        if type(payload) is not bytes:
            raise TypeError("payload must be bytes")
        return cls(
            paper_run_id=paper_run_id,
            output_sequence=output_sequence,
            record_type=record_type,
            source_ingest_sequence=source_ingest_sequence,
            paper_sequence=paper_sequence,
            payload=payload,
            payload_sha256=_digest_bytes(b"tx_trade.research.outbox.v1:", payload),
        )


@dataclass(frozen=True, slots=True)
class ResearchRunState:
    identity: ResearchRunIdentity
    status: ResearchRunStatus
    state_version: int
    committed_cursor: int | None
    committed_batch_count: int
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None

    def __post_init__(self) -> None:
        if type(self.identity) is not ResearchRunIdentity:
            raise TypeError("identity must be ResearchRunIdentity")
        if type(self.status) is not ResearchRunStatus:
            raise TypeError("status must be ResearchRunStatus")
        _int(self.state_version, "state_version")
        if self.committed_cursor is not None:
            _int(self.committed_cursor, "committed_cursor")
            if not (
                self.identity.source_first_sequence
                <= self.committed_cursor
                <= self.identity.source_last_sequence
            ):
                raise ValueError("committed_cursor is outside the source range")
        _int(self.committed_batch_count, "committed_batch_count")
        if (self.committed_cursor is None) != (self.committed_batch_count == 0):
            raise ValueError("cursor and committed batch count must advance together")
        _taipei_datetime(self.created_at, "created_at")
        _taipei_datetime(self.updated_at, "updated_at")
        _taipei_datetime(self.completed_at, "completed_at", optional=True)
        if self.updated_at < self.created_at:
            raise ValueError("updated_at must not precede created_at")
        if self.status is ResearchRunStatus.COMPLETE:
            if (
                self.completed_at is None
                or self.committed_cursor != self.identity.source_last_sequence
                or self.committed_batch_count != self.identity.source_event_count
            ):
                raise ValueError("complete run state must be terminal")
        elif self.completed_at is not None:
            raise ValueError("only complete run state may have completed_at")
        if self.completed_at is not None and self.completed_at < self.updated_at:
            raise ValueError("completed_at must not precede updated_at")


@dataclass(frozen=True, slots=True)
class ResearchHydrationState:
    run_state: ResearchRunState
    broker_checkpoint: VersionedCheckpoint
    coordinator_checkpoint: VersionedCheckpoint

    def __post_init__(self) -> None:
        if type(self.run_state) is not ResearchRunState:
            raise TypeError("run_state must be ResearchRunState")
        if (
            type(self.broker_checkpoint) is not VersionedCheckpoint
            or self.broker_checkpoint.kind is not CheckpointKind.BROKER
        ):
            raise ValueError("broker_checkpoint must be a broker checkpoint")
        if (
            type(self.coordinator_checkpoint) is not VersionedCheckpoint
            or self.coordinator_checkpoint.kind is not CheckpointKind.COORDINATOR
        ):
            raise ValueError("coordinator_checkpoint must be a coordinator checkpoint")


@dataclass(frozen=True, slots=True)
class ResearchDurableBatch:
    paper_run_id: UUID
    expected_state_version: int
    expected_previous_cursor: int | None
    source_session_id: UUID
    source_ingest_sequence: int
    envelope_fingerprint: str
    decision_fingerprint: str
    broker_checkpoint: VersionedCheckpoint
    coordinator_checkpoint: VersionedCheckpoint
    outbox_records: tuple[ResearchOutboxRecord, ...]
    batch_fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        _uuid(self.paper_run_id, "paper_run_id")
        _int(self.expected_state_version, "expected_state_version")
        if self.expected_previous_cursor is not None:
            _int(self.expected_previous_cursor, "expected_previous_cursor")
        _uuid(self.source_session_id, "source_session_id")
        _int(self.source_ingest_sequence, "source_ingest_sequence")
        _fingerprint(self.envelope_fingerprint, "envelope_fingerprint")
        _fingerprint(self.decision_fingerprint, "decision_fingerprint")
        if (
            type(self.broker_checkpoint) is not VersionedCheckpoint
            or self.broker_checkpoint.kind is not CheckpointKind.BROKER
        ):
            raise ValueError("broker_checkpoint must be a broker checkpoint")
        if (
            type(self.coordinator_checkpoint) is not VersionedCheckpoint
            or self.coordinator_checkpoint.kind is not CheckpointKind.COORDINATOR
        ):
            raise ValueError("coordinator_checkpoint must be a coordinator checkpoint")
        if type(self.outbox_records) is not tuple:
            raise TypeError("outbox_records must be a tuple")
        if not self.outbox_records or len(self.outbox_records) > MAX_BATCH_OUTBOX_RECORDS:
            raise ValueError("outbox_records count is outside the supported range")
        if any(type(record) is not ResearchOutboxRecord for record in self.outbox_records):
            raise TypeError("outbox_records must contain ResearchOutboxRecord")
        if any(record.paper_run_id != self.paper_run_id for record in self.outbox_records):
            raise ValueError("outbox paper_run_id correlation mismatch")
        if any(
            record.record_type is ResearchOutboxRecordType.SUMMARY for record in self.outbox_records
        ):
            raise ValueError("durable batches must not contain summary records")
        market = tuple(
            record
            for record in self.outbox_records
            if record.record_type is ResearchOutboxRecordType.MARKET
        )
        if len(market) != 1 or market[0].source_ingest_sequence != self.source_ingest_sequence:
            raise ValueError("durable batches require exactly one correlated market record")
        if any(
            record.source_ingest_sequence != self.source_ingest_sequence
            for record in self.outbox_records
        ):
            raise ValueError("outbox source correlation mismatch")
        output_sequences = tuple(record.output_sequence for record in self.outbox_records)
        if output_sequences != tuple(sorted(output_sequences)) or len(set(output_sequences)) != len(
            output_sequences
        ):
            raise ValueError("outbox records must have unique increasing output sequences")
        types = tuple(record.record_type for record in self.outbox_records)
        if types[0] is not ResearchOutboxRecordType.MARKET or any(
            left is ResearchOutboxRecordType.PAPER and right is ResearchOutboxRecordType.MARKET
            for left, right in zip(types, types[1:], strict=False)
        ):
            raise ValueError("market record must precede paper records")
        paper_sequences = tuple(
            record.paper_sequence
            for record in self.outbox_records
            if record.record_type is ResearchOutboxRecordType.PAPER
            and record.paper_sequence is not None
        )
        if paper_sequences != tuple(sorted(paper_sequences)) or len(set(paper_sequences)) != len(
            paper_sequences
        ):
            raise ValueError("paper records must have unique increasing paper sequences")
        material = {
            "broker_checkpoint": {
                "payload_sha256": self.broker_checkpoint.payload_sha256,
                "schema_version": self.broker_checkpoint.schema_version,
            },
            "coordinator_checkpoint": {
                "payload_sha256": self.coordinator_checkpoint.payload_sha256,
                "schema_version": self.coordinator_checkpoint.schema_version,
            },
            "decision_fingerprint": self.decision_fingerprint,
            "envelope_fingerprint": self.envelope_fingerprint,
            "expected_previous_cursor": self.expected_previous_cursor,
            "expected_state_version": self.expected_state_version,
            "outbox": [
                {
                    "output_sequence": record.output_sequence,
                    "paper_sequence": record.paper_sequence,
                    "payload_sha256": record.payload_sha256,
                    "record_type": record.record_type.value,
                    "source_ingest_sequence": record.source_ingest_sequence,
                }
                for record in self.outbox_records
            ],
            "paper_run_id": str(self.paper_run_id),
            "source_ingest_sequence": self.source_ingest_sequence,
            "source_session_id": str(self.source_session_id),
        }
        encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
        object.__setattr__(
            self,
            "batch_fingerprint",
            _digest_bytes(b"tx_trade.research.durable_batch.v1:", encoded),
        )


@dataclass(frozen=True, slots=True)
class ResearchDurableBatchResult:
    disposition: DurableBatchDisposition
    run_state: ResearchRunState

    def __post_init__(self) -> None:
        if type(self.disposition) is not DurableBatchDisposition:
            raise TypeError("disposition must be DurableBatchDisposition")
        if type(self.run_state) is not ResearchRunState:
            raise TypeError("run_state must be ResearchRunState")


@dataclass(frozen=True, slots=True)
class CompleteResearchRun:
    paper_run_id: UUID
    expected_state_version: int
    expected_previous_cursor: int
    summary_record: ResearchOutboxRecord
    completed_at: datetime

    def __post_init__(self) -> None:
        _uuid(self.paper_run_id, "paper_run_id")
        _int(self.expected_state_version, "expected_state_version")
        _int(self.expected_previous_cursor, "expected_previous_cursor")
        if (
            type(self.summary_record) is not ResearchOutboxRecord
            or self.summary_record.record_type is not ResearchOutboxRecordType.SUMMARY
        ):
            raise ValueError("summary_record must be a summary outbox record")
        if self.summary_record.paper_run_id != self.paper_run_id:
            raise ValueError("summary paper_run_id correlation mismatch")
        _taipei_datetime(self.completed_at, "completed_at")
