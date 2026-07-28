"""Deterministic coordinator between replay, strategies, and the paper broker."""

from __future__ import annotations

from dataclasses import dataclass, replace
from hashlib import sha256
from uuid import UUID

from tx_trade.market_data.models import MarketDataEnvelope, serialize_envelope
from tx_trade.orders.contracts import (
    CancelIntent,
    OrderIntent,
    PaperDecision,
    PaperDecisionBatchResult,
)

from .contracts import (
    StrategyContext,
    StrategyDecision,
    StrategyExecutionMode,
    StrategyRegistration,
)
from .ports import TransactionalPaperBrokerSnapshotPort


class StrategyCoordinatorError(RuntimeError):
    """A stable, non-sensitive strategy coordination failure."""


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
