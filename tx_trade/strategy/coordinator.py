"""Deterministic coordinator between replay, strategies, and the paper broker."""

from __future__ import annotations

import json
from dataclasses import dataclass, fields, replace
from datetime import datetime
from decimal import Decimal
from enum import Enum
from hashlib import sha256
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

from tx_trade.market_data.models import MarketDataEnvelope, serialize_envelope
from tx_trade.orders.contracts import (
    CancelIntent,
    ExecutionProvenance,
    MatchDisposition,
    MatchResult,
    MatchSkipReason,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PaperDecision,
    PaperDecisionBatchResult,
    PaperEvent,
    PaperEventType,
    PaperFill,
    PaperOrder,
    PaperPosition,
    PaperRejection,
    RejectionCode,
    TimeInForce,
)
from tx_trade.research.contracts import CheckpointKind, VersionedCheckpoint

from .contracts import (
    StrategyContext,
    StrategyDecision,
    StrategyExecutionMode,
    StrategyRegistration,
)
from .ports import TransactionalPaperBrokerSnapshotPort


class StrategyCoordinatorError(RuntimeError):
    """A stable, non-sensitive strategy coordination failure."""


class StrategyCheckpointError(ValueError):
    """A stable, non-sensitive coordinator checkpoint failure."""


@dataclass(frozen=True, slots=True)
class StrategyDecisionRecord:
    source_session_id: UUID
    source_ingest_sequence: int
    envelope_digest: str
    decision: PaperDecision
    batch_result: PaperDecisionBatchResult | None

    def __post_init__(self) -> None:
        if type(self.source_session_id) is not UUID:
            raise TypeError("source_session_id must be UUID")
        if type(self.source_ingest_sequence) is not int:
            raise TypeError("source_ingest_sequence must be an integer")
        if self.source_ingest_sequence < 0:
            raise ValueError("source_ingest_sequence must be non-negative")
        if (
            type(self.envelope_digest) is not str
            or len(self.envelope_digest) != 64
            or any(character not in "0123456789abcdef" for character in self.envelope_digest)
        ):
            raise ValueError("envelope_digest must be a lowercase sha256 digest")
        if type(self.decision) is not PaperDecision:
            raise TypeError("decision must be PaperDecision")
        if (
            self.batch_result is not None
            and type(self.batch_result) is not PaperDecisionBatchResult
        ):
            raise TypeError("batch_result must be PaperDecisionBatchResult or None")
        if (
            self.decision.source_session_id != self.source_session_id
            or self.decision.source_ingest_sequence != self.source_ingest_sequence
        ):
            raise ValueError("decision source causation must match record")
        if self.batch_result is not None and (
            self.batch_result.source_session_id != self.source_session_id
            or self.batch_result.source_ingest_sequence != self.source_ingest_sequence
            or self.batch_result.decision_fingerprint != self.decision.decision_fingerprint
        ):
            raise ValueError("batch result must match decision record")


class PaperReplayCoordinator:
    def __init__(
        self,
        *,
        broker: TransactionalPaperBrokerSnapshotPort,
        registrations: tuple[StrategyRegistration, ...],
        mode: StrategyExecutionMode,
        max_decision_records: int,
    ) -> None:
        if not isinstance(broker, TransactionalPaperBrokerSnapshotPort):
            raise TypeError("broker must implement TransactionalPaperBrokerSnapshotPort")
        if type(registrations) is not tuple:
            raise TypeError("registrations must be a tuple")
        if any(type(registration) is not StrategyRegistration for registration in registrations):
            raise TypeError("registrations must contain only StrategyRegistration")
        if type(mode) is not StrategyExecutionMode:
            raise TypeError("mode must be StrategyExecutionMode")
        if type(max_decision_records) is not int:
            raise TypeError("max_decision_records must be an integer")
        if max_decision_records < 1:
            raise ValueError("max_decision_records must be at least 1")
        strategy_ids = tuple(registration.strategy_id for registration in registrations)
        if len(set(strategy_ids)) != len(strategy_ids):
            raise ValueError("registrations must have unique strategy_id values")
        self._broker = broker
        self._registrations = tuple(
            sorted(registrations, key=lambda registration: registration.strategy_id)
        )
        self._mode = mode
        self._max_decision_records = max_decision_records
        self._records: dict[tuple[UUID, int, str], StrategyDecisionRecord] = {}
        self._source_digests: dict[tuple[UUID, int], str] = {}

    @property
    def decision_count(self) -> int:
        return len(self._records)

    def decision_records(self) -> tuple[StrategyDecisionRecord, ...]:
        return tuple(
            sorted(
                self._records.values(),
                key=lambda record: (
                    str(record.source_session_id),
                    record.source_ingest_sequence,
                    record.envelope_digest,
                ),
            )
        )

    def export_checkpoint(self) -> VersionedCheckpoint:
        payload = {
            "max_decision_records": self._max_decision_records,
            "mode": _encode(self._mode),
            "records": _encode(self.decision_records()),
            "registrations": _encode(
                tuple(
                    (registration.strategy_id, registration.fingerprint)
                    for registration in self._registrations
                )
            ),
            "source_digests": _encode(
                tuple(
                    (session_id, sequence, digest)
                    for (session_id, sequence), digest in sorted(
                        self._source_digests.items(),
                        key=lambda item: (str(item[0][0]), item[0][1]),
                    )
                )
            ),
        }
        encoded = json.dumps(
            payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        return VersionedCheckpoint.create(
            kind=CheckpointKind.COORDINATOR,
            schema_version=1,
            payload=encoded,
        )

    @classmethod
    def restore_checkpoint(
        cls,
        checkpoint: VersionedCheckpoint,
        *,
        broker: TransactionalPaperBrokerSnapshotPort,
        registrations: tuple[StrategyRegistration, ...],
    ) -> PaperReplayCoordinator:
        try:
            if type(checkpoint) is not VersionedCheckpoint:
                raise TypeError
            if checkpoint.kind is not CheckpointKind.COORDINATOR or checkpoint.schema_version != 1:
                raise ValueError
            document = json.loads(
                checkpoint.payload.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_json_keys,
            )
            _exact_keys(
                document,
                {
                    "max_decision_records",
                    "mode",
                    "records",
                    "registrations",
                    "source_digests",
                },
            )
            mode = _decode(document["mode"])
            records = _decode(document["records"])
            identities = _decode(document["registrations"])
            source_digests = _decode(document["source_digests"])
            if (
                type(mode) is not StrategyExecutionMode
                or type(records) is not tuple
                or type(identities) is not tuple
                or type(source_digests) is not tuple
            ):
                raise TypeError
            expected_identities = tuple(
                (registration.strategy_id, registration.fingerprint)
                for registration in sorted(
                    registrations, key=lambda registration: registration.strategy_id
                )
            )
            if identities != expected_identities:
                raise ValueError
            coordinator = cls(
                broker=broker,
                registrations=registrations,
                mode=mode,
                max_decision_records=document["max_decision_records"],
            )
            coordinator._restore_state(records, source_digests)
            if coordinator.export_checkpoint().payload != checkpoint.payload:
                raise ValueError
            return coordinator
        except StrategyCheckpointError:
            raise
        except (
            TypeError,
            ValueError,
            KeyError,
            UnicodeError,
            json.JSONDecodeError,
            RecursionError,
        ):
            raise StrategyCheckpointError("strategy coordinator checkpoint is invalid") from None

    def _restore_state(
        self,
        records: tuple[object, ...],
        source_digests: tuple[object, ...],
    ) -> None:
        if len(records) > self._max_decision_records or len(records) != len(source_digests):
            raise ValueError
        registered_strategy_ids = {registration.strategy_id for registration in self._registrations}
        restored_records: dict[tuple[UUID, int, str], StrategyDecisionRecord] = {}
        prior_record_key: tuple[str, int, str] | None = None
        for record in records:
            if type(record) is not StrategyDecisionRecord:
                raise TypeError
            sort_key = (
                str(record.source_session_id),
                record.source_ingest_sequence,
                record.envelope_digest,
            )
            if prior_record_key is not None and sort_key <= prior_record_key:
                raise ValueError
            if (
                self._mode is StrategyExecutionMode.OBSERVE_ONLY and record.batch_result is not None
            ) or any(
                command.strategy_id not in registered_strategy_ids
                for command in record.decision.commands
            ):
                raise ValueError
            prior_record_key = sort_key
            key = (
                record.source_session_id,
                record.source_ingest_sequence,
                record.envelope_digest,
            )
            restored_records[key] = record
        restored_digests: dict[tuple[UUID, int], str] = {}
        prior_source_key: tuple[str, int] | None = None
        for item in source_digests:
            if (
                type(item) is not tuple
                or len(item) != 3
                or type(item[0]) is not UUID
                or type(item[1]) is not int
                or type(item[2]) is not str
            ):
                raise TypeError
            session_id, sequence, digest = item
            source_sort_key = (str(session_id), sequence)
            if prior_source_key is not None and source_sort_key <= prior_source_key:
                raise ValueError
            prior_source_key = source_sort_key
            matching = restored_records.get((session_id, sequence, digest))
            if matching is None:
                raise ValueError
            restored_digests[(session_id, sequence)] = digest
        self._records = restored_records
        self._source_digests = restored_digests

    def publish(self, envelope: MarketDataEnvelope) -> None:
        if type(envelope) is not MarketDataEnvelope:
            raise TypeError("envelope must be MarketDataEnvelope")
        envelope_digest = sha256(serialize_envelope(envelope).encode("utf-8")).hexdigest()
        source_key = (envelope.session_id, envelope.ingest_sequence)
        prior_digest = self._source_digests.get(source_key)
        if prior_digest is not None and prior_digest != envelope_digest:
            raise StrategyCoordinatorError("market-data retry conflict")
        cache_key = (*source_key, envelope_digest)
        cached = self._records.get(cache_key)
        if cached is not None:
            self._process_cached(envelope, cache_key, cached)
            return
        if len(self._records) >= self._max_decision_records:
            raise StrategyCoordinatorError("strategy decision capacity exceeded")

        snapshot = self._broker.snapshot()
        decision_at = envelope.event_at or envelope.received_at
        commands: list[OrderIntent | CancelIntent] = []
        for registration in self._registrations:
            context = StrategyContext(
                strategy_id=registration.strategy_id,
                source_session_id=envelope.session_id,
                source_ingest_sequence=envelope.ingest_sequence,
                decision_at=decision_at,
                orders=tuple(
                    sorted(
                        (
                            order
                            for order in snapshot.orders
                            if order.intent.strategy_id == registration.strategy_id
                        ),
                        key=lambda order: (
                            order.intent.account_id,
                            order.intent.instrument_id,
                            order.intent.client_order_id,
                            str(order.paper_order_id),
                        ),
                    )
                ),
                positions=tuple(
                    sorted(
                        (
                            position
                            for position in snapshot.positions
                            if position.strategy_id == registration.strategy_id
                        ),
                        key=lambda position: (
                            position.account_id,
                            position.instrument_id,
                            str(position.paper_position_id),
                        ),
                    )
                ),
                broker_snapshot_version=snapshot.snapshot_version,
            )
            try:
                strategy_decision = registration.strategy.decide(envelope, context)
            except Exception:
                raise StrategyCoordinatorError("strategy evaluation failed") from None
            self._validate_strategy_decision(
                strategy_decision,
                registration.strategy_id,
                envelope,
                decision_at,
            )
            commands.extend(strategy_decision.commands)
        self._validate_unique_client_ids(commands)
        try:
            combined = PaperDecision(
                source_session_id=envelope.session_id,
                source_ingest_sequence=envelope.ingest_sequence,
                commands=tuple(commands),
            )
        except Exception:
            raise StrategyCoordinatorError("strategy decision aggregation failed") from None

        record = StrategyDecisionRecord(
            source_session_id=envelope.session_id,
            source_ingest_sequence=envelope.ingest_sequence,
            envelope_digest=envelope_digest,
            decision=combined,
            batch_result=None,
        )
        self._source_digests[source_key] = envelope_digest
        self._records[cache_key] = record
        self._process_cached(envelope, cache_key, record)

    def _process_cached(
        self,
        envelope: MarketDataEnvelope,
        cache_key: tuple[UUID, int, str],
        record: StrategyDecisionRecord,
    ) -> None:
        if self._mode is StrategyExecutionMode.OBSERVE_ONLY:
            return
        result = self._broker.process_decision_batch(envelope, record.decision)
        if record.batch_result is None:
            self._records[cache_key] = replace(record, batch_result=result)

    @staticmethod
    def _validate_strategy_decision(
        decision: object,
        strategy_id: str,
        envelope: MarketDataEnvelope,
        decision_at: object,
    ) -> None:
        if type(decision) is not StrategyDecision:
            raise StrategyCoordinatorError("strategy returned an invalid decision")
        for command in decision.commands:
            command_at = (
                command.created_at if isinstance(command, OrderIntent) else command.requested_at
            )
            if command.strategy_id != strategy_id:
                raise StrategyCoordinatorError("strategy command authority mismatch")
            if (
                command.source_session_id != envelope.session_id
                or command.source_ingest_sequence != envelope.ingest_sequence
            ):
                raise StrategyCoordinatorError("strategy command source mismatch")
            if command_at != decision_at:
                raise StrategyCoordinatorError("strategy command time mismatch")

    @staticmethod
    def _validate_unique_client_ids(commands: list[OrderIntent | CancelIntent]) -> None:
        keys = tuple((command.strategy_id, command.client_order_id) for command in commands)
        if len(keys) != len(set(keys)):
            raise StrategyCoordinatorError("strategy command client id conflict")


_CHECKPOINT_TYPES: dict[str, Any] = {
    item.__name__: item
    for item in (
        StrategyDecisionRecord,
        OrderIntent,
        CancelIntent,
        PaperDecision,
        PaperOrder,
        PaperFill,
        PaperRejection,
        PaperPosition,
        PaperEvent,
        MatchResult,
        PaperDecisionBatchResult,
    )
}
_CHECKPOINT_ENUMS: dict[str, Any] = {
    item.__name__: item
    for item in (
        StrategyExecutionMode,
        OrderSide,
        OrderStatus,
        OrderType,
        TimeInForce,
        ExecutionProvenance,
        RejectionCode,
        PaperEventType,
        MatchDisposition,
        MatchSkipReason,
    )
}


def _encode(value: object) -> object:
    if value is None or type(value) in {str, int, bool}:
        return value
    if type(value) is UUID:
        return {"$uuid": str(value)}
    if type(value) is Decimal:
        return {"$decimal": str(value)}
    if type(value) is datetime:
        return {"$datetime": value.isoformat(timespec="microseconds")}
    if isinstance(value, Enum):
        return {"$enum": type(value).__name__, "value": value.value}
    if type(value) is tuple:
        return {"$tuple": [_encode(item) for item in value]}
    value_type = type(value)
    if value_type.__name__ in _CHECKPOINT_TYPES:
        return {
            "$type": value_type.__name__,
            **{
                item.name: _encode(getattr(value, item.name))
                for item in fields(value)  # type: ignore[arg-type]
                if item.init
            },
        }
    raise TypeError


def _decode(value: object) -> object:
    if value is None or type(value) in {str, int, bool}:
        return value
    if type(value) is not dict:
        raise TypeError
    if "$uuid" in value:
        _exact_keys(value, {"$uuid"})
        if type(value["$uuid"]) is not str:
            raise TypeError
        return UUID(value["$uuid"])
    if "$decimal" in value:
        _exact_keys(value, {"$decimal"})
        if type(value["$decimal"]) is not str:
            raise TypeError
        return Decimal(value["$decimal"])
    if "$datetime" in value:
        _exact_keys(value, {"$datetime"})
        if type(value["$datetime"]) is not str:
            raise TypeError
        parsed = datetime.fromisoformat(value["$datetime"])
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        return parsed.astimezone(ZoneInfo("Asia/Taipei"))
    if "$enum" in value:
        _exact_keys(value, {"$enum", "value"})
        enum_type = _CHECKPOINT_ENUMS.get(value["$enum"])
        if enum_type is None or type(value["value"]) is not str:
            raise TypeError
        return enum_type(value["value"])
    if "$tuple" in value:
        _exact_keys(value, {"$tuple"})
        if type(value["$tuple"]) is not list:
            raise TypeError
        return tuple(_decode(item) for item in value["$tuple"])
    type_name = value.get("$type")
    if type(type_name) is not str:
        raise TypeError
    value_type = _CHECKPOINT_TYPES.get(type_name)
    if value_type is None:
        raise TypeError
    expected = {"$type", *(item.name for item in fields(value_type) if item.init)}
    _exact_keys(value, expected)
    arguments = {item.name: _decode(value[item.name]) for item in fields(value_type) if item.init}
    return value_type(**arguments)


def _exact_keys(value: object, expected: set[str]) -> None:
    if type(value) is not dict or set(value) != expected:
        raise ValueError


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result
