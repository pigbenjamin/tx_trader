"""Synchronous transactional in-memory broker for deterministic paper matching."""

from __future__ import annotations

import json
from dataclasses import dataclass, fields, replace
from datetime import datetime
from decimal import Decimal, localcontext
from enum import Enum
from hashlib import sha256
from threading import RLock
from types import MappingProxyType
from typing import Any, Mapping, TypeAlias, TypeVar
from uuid import UUID, uuid5
from zoneinfo import ZoneInfo

from tx_trade.market_data.models import (
    EventType,
    Instrument,
    MarketDataEnvelope,
    Quote,
    serialize_envelope,
)
from tx_trade.research.contracts import CheckpointKind, VersionedCheckpoint

from .contracts import (
    CancelIntent,
    DEFAULT_EXECUTION_CONFIG,
    InstrumentMetadataSnapshot,
    ExecutionProvenance,
    FeePolicyKind,
    FeeRoundingMode,
    MatchDisposition,
    MatchResult,
    MatchSkipReason,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PaperBrokerLimits,
    PaperBrokerSnapshot,
    PaperDecision,
    PaperDecisionBatchResult,
    PaperExecutionConfig,
    PaperEvent,
    PaperEventType,
    PaperFeeRule,
    PaperFeeSchedule,
    PaperFill,
    PaperOrder,
    PaperPosition,
    PaperRejection,
    RejectionCode,
    SlippageConfig,
    SlippageMode,
    TimeInForce,
    canonical_json,
)
from .execution_policies import (
    ExecutionPolicyError,
    ExecutionPolicyErrorCode,
    assess_fee,
    assess_limit,
    assess_slippage,
)
from .matching import (
    MATCHING_CONTEXT,
    QuoteTop,
    execution_price,
    inspect_quote_top,
    weighted_average,
)
from .ports import OrderCommandResult
from .position_ledger import (
    PositionLedgerError,
    PositionLedgerErrorCode,
    apply_fill_to_position,
    paper_position_id,
)
from .state_machine import validate_order_transition

SubmitKey: TypeAlias = tuple[str, str]
CancelKey: TypeAlias = tuple[UUID, str, str]
PositionKey: TypeAlias = tuple[str, str, str]
_Key = TypeVar("_Key")
_Value = TypeVar("_Value")


class PaperBrokerInputError(ValueError):
    """Stable fail-closed error for conflicting market-data input."""


class PaperBrokerCapacityError(RuntimeError):
    """Raised when an envelope's complete staged batch cannot fit."""


class PaperBrokerCheckpointError(ValueError):
    """A checkpoint was not a valid, self-consistent paper-broker state."""


@dataclass(frozen=True, slots=True)
class _OutcomeRecord:
    primary_digest: str
    primary_outcome: OrderCommandResult
    alternate_digest: str | None = None
    alternate_outcome: OrderCommandResult | None = None

    def find(self, digest: str) -> OrderCommandResult | None:
        if digest == self.primary_digest:
            return self.primary_outcome
        if digest == self.alternate_digest:
            return self.alternate_outcome
        return None

    @property
    def has_alternate(self) -> bool:
        return self.alternate_digest is not None


@dataclass(frozen=True, slots=True)
class _BrokerState:
    effective_session: UUID | None
    bound_session: UUID | None
    bound_source: str | None
    last_sequence: int | None
    next_paper_sequence: int
    snapshot_version: int
    execution_config_fingerprint: str
    orders: Mapping[UUID, PaperOrder]
    positions: Mapping[PositionKey, PaperPosition]
    submit_outcomes: Mapping[SubmitKey, _OutcomeRecord]
    cancel_outcomes: Mapping[CancelKey, _OutcomeRecord]
    acceptance_ordinal: Mapping[UUID, int]
    eligibility_sequence: Mapping[UUID, int]
    next_acceptance_ordinal: int
    fills: tuple[PaperFill, ...]
    events: tuple[PaperEvent, ...]
    instruments: Mapping[tuple[str, int], InstrumentMetadataSnapshot]
    envelope_fingerprints: Mapping[int, str]
    decision_fingerprints: Mapping[int, str]
    dedupe_sequences: Mapping[str, int]


def _mapping(
    values: Mapping[_Key, _Value] | None = None,
) -> Mapping[_Key, _Value]:
    return MappingProxyType({} if values is None else dict(values))


class PaperBroker:
    """A deterministic, run-local broker with an authoritative event journal."""

    def __init__(
        self,
        *,
        paper_run_id: UUID,
        limits: PaperBrokerLimits,
        execution_config: PaperExecutionConfig | None = None,
        expected_source_session_id: UUID | None = None,
    ) -> None:
        if type(paper_run_id) is not UUID:
            raise TypeError("paper_run_id must be UUID")
        if type(limits) is not PaperBrokerLimits:
            raise TypeError("limits must be PaperBrokerLimits")
        if execution_config is not None and type(execution_config) is not PaperExecutionConfig:
            raise TypeError("execution_config must be PaperExecutionConfig or None")
        if expected_source_session_id is not None and type(expected_source_session_id) is not UUID:
            raise TypeError("expected_source_session_id must be UUID or None")
        self._paper_run_id = paper_run_id
        self._limits = limits
        self._execution_config = (
            DEFAULT_EXECUTION_CONFIG if execution_config is None else execution_config
        )
        self._lock = RLock()
        self._state = _BrokerState(
            effective_session=expected_source_session_id,
            bound_session=None,
            bound_source=None,
            last_sequence=None,
            next_paper_sequence=1,
            snapshot_version=0,
            execution_config_fingerprint=self._execution_config.fingerprint,
            orders=_mapping(),
            positions=_mapping(),
            submit_outcomes=_mapping(),
            cancel_outcomes=_mapping(),
            acceptance_ordinal=_mapping(),
            eligibility_sequence=_mapping(),
            next_acceptance_ordinal=1,
            fills=(),
            events=(),
            instruments=_mapping(),
            envelope_fingerprints=_mapping(),
            decision_fingerprints=_mapping(),
            dedupe_sequences=_mapping(),
        )

    def submit(self, intent: OrderIntent) -> OrderCommandResult:
        if type(intent) is not OrderIntent:
            raise TypeError("intent must be OrderIntent")
        digest = _command_digest(intent)
        key = (intent.strategy_id, intent.client_order_id)
        with self._lock:
            state = self._state
            record = state.submit_outcomes.get(key)
            if record is not None:
                cached = record.find(digest)
                if cached is not None:
                    return self._resolve_cached_outcome(state, cached)
                conflict = self._rejection_value(
                    intent.strategy_id,
                    intent.client_order_id,
                    RejectionCode.IDEMPOTENCY_CONFLICT,
                    intent.created_at,
                    self._order_id(*key),
                )
                if record.has_alternate:
                    return conflict
                staged = self._stage_rejection(
                    state,
                    conflict,
                    submit_update=(
                        key,
                        replace(
                            record,
                            alternate_digest=digest,
                            alternate_outcome=conflict,
                        ),
                    ),
                )
                self._commit(staged)
                return conflict

            effective_session = state.effective_session
            if intent.source_session_id is not None:
                if effective_session is None:
                    effective_session = intent.source_session_id
                elif intent.source_session_id != effective_session:
                    rejection = self._rejection_value(
                        intent.strategy_id,
                        intent.client_order_id,
                        RejectionCode.INVALID_INTENT,
                        intent.created_at,
                        self._order_id(*key),
                    )
                    return self._commit_new_submit_rejection(
                        state,
                        key,
                        digest,
                        rejection,
                    )

            if len(state.submit_outcomes) >= self._limits.max_orders:
                return self._rejection_value(
                    intent.strategy_id,
                    intent.client_order_id,
                    RejectionCode.CAPACITY_EXCEEDED,
                    intent.created_at,
                    self._order_id(*key),
                )
            if (
                len(state.orders) >= self._limits.max_orders
                or self._open_order_count(state) >= self._limits.max_open_orders
                or len(state.events) >= self._limits.max_events
            ):
                rejection = self._rejection_value(
                    intent.strategy_id,
                    intent.client_order_id,
                    RejectionCode.CAPACITY_EXCEEDED,
                    intent.created_at,
                    self._order_id(*key),
                )
                return self._commit_new_submit_rejection(
                    replace(state, effective_session=effective_session),
                    key,
                    digest,
                    rejection,
                )

            order_id = self._order_id(*key)
            with localcontext(MATCHING_CONTEXT):
                order = PaperOrder(
                    paper_run_id=self._paper_run_id,
                    paper_order_id=order_id,
                    intent=intent,
                    status=OrderStatus.ACCEPTED,
                    filled_quantity=Decimal(0),
                    remaining_quantity=intent.quantity,
                    average_fill_price=None,
                    accepted_at=intent.created_at,
                    updated_at=intent.created_at,
                )
            event = self._make_event(
                state.next_paper_sequence,
                PaperEventType.ORDER_ACCEPTED,
                order,
                intent.created_at,
                source_session_id=intent.source_session_id,
                source_ingest_sequence=intent.source_ingest_sequence,
            )
            orders = dict(state.orders)
            orders[order_id] = order
            outcomes = dict(state.submit_outcomes)
            outcomes[key] = _OutcomeRecord(digest, order)
            ordinals = dict(state.acceptance_ordinal)
            ordinals[order_id] = state.next_acceptance_ordinal
            eligibility = dict(state.eligibility_sequence)
            eligibility[order_id] = (
                intent.source_ingest_sequence
                if intent.source_ingest_sequence is not None
                else (-1 if state.last_sequence is None else state.last_sequence)
            )
            staged = replace(
                state,
                effective_session=effective_session,
                orders=_mapping(orders),
                submit_outcomes=_mapping(outcomes),
                acceptance_ordinal=_mapping(ordinals),
                eligibility_sequence=_mapping(eligibility),
                next_acceptance_ordinal=state.next_acceptance_ordinal + 1,
                events=state.events + (event,),
                next_paper_sequence=state.next_paper_sequence + 1,
                snapshot_version=state.snapshot_version + 1,
            )
            self._commit(staged)
            return order

    def cancel(self, request: CancelIntent) -> OrderCommandResult:
        if type(request) is not CancelIntent:
            raise TypeError("request must be CancelIntent")
        digest = _command_digest(request)
        key = (
            request.paper_order_id,
            request.strategy_id,
            request.client_order_id,
        )
        with self._lock:
            state = self._state
            if request.source_session_id is not None:
                if (
                    state.effective_session is not None
                    and request.source_session_id != state.effective_session
                ) or (
                    state.bound_session is not None
                    and request.source_session_id != state.bound_session
                ):
                    raise PaperBrokerInputError("paper cancel source session mismatch")
                if state.effective_session is None:
                    state = replace(
                        state,
                        effective_session=request.source_session_id,
                    )
            record = state.cancel_outcomes.get(key)
            if record is not None:
                cached = record.find(digest)
                if cached is not None:
                    return self._resolve_cached_outcome(state, cached)

            outcome, event_type = self._evaluate_cancel(state, request)
            if isinstance(outcome, PaperOrder) and event_type is None:
                # Once cancelled, every identity-matching timestamp variant is
                # the same business query and needs neither cache nor journal.
                return outcome

            can_cache = (
                record is None and len(state.cancel_outcomes) < self._limits.max_orders
            ) or (record is not None and not record.has_alternate)
            staged = state
            if can_cache:
                outcomes = dict(state.cancel_outcomes)
                if record is None:
                    outcomes[key] = _OutcomeRecord(digest, outcome)
                else:
                    outcomes[key] = replace(
                        record,
                        alternate_digest=digest,
                        alternate_outcome=outcome,
                    )
                staged = replace(state, cancel_outcomes=_mapping(outcomes))

            if event_type is not None:
                assert isinstance(outcome, PaperOrder)
                event = self._make_event(
                    state.next_paper_sequence,
                    event_type,
                    outcome,
                    request.requested_at,
                    source_session_id=request.source_session_id,
                    source_ingest_sequence=request.source_ingest_sequence,
                )
                orders = dict(state.orders)
                orders[outcome.paper_order_id] = outcome
                staged = replace(
                    staged,
                    orders=_mapping(orders),
                    events=state.events + (event,),
                    next_paper_sequence=state.next_paper_sequence + 1,
                    snapshot_version=state.snapshot_version + 1,
                )
                self._commit(staged)
                return outcome

            assert isinstance(outcome, PaperRejection)
            if not can_cache:
                # A bounded cache must not rewrite the actual rejection code.
                # Uncached rejections deliberately have no journal side effect.
                return outcome
            staged = self._append_rejection_event(staged, outcome)
            if staged.snapshot_version == state.snapshot_version:
                staged = replace(staged, snapshot_version=state.snapshot_version + 1)
            self._commit(staged)
            return outcome

    def get_order(self, paper_order_id: UUID) -> PaperOrder | None:
        if type(paper_order_id) is not UUID:
            raise TypeError("paper_order_id must be UUID")
        with self._lock:
            return self._state.orders.get(paper_order_id)

    def list_orders(self) -> tuple[PaperOrder, ...]:
        with self._lock:
            return self._ordered_orders(self._state)

    def list_positions(self) -> tuple[PaperPosition, ...]:
        with self._lock:
            return self._ordered_positions(self._state)

    def get_position(
        self,
        strategy_id: str,
        account_id: str,
        instrument_id: str,
    ) -> PaperPosition | None:
        for name, value in (
            ("strategy_id", strategy_id),
            ("account_id", account_id),
            ("instrument_id", instrument_id),
        ):
            if type(value) is not str:
                raise TypeError(f"{name} must be a string")
            if not value.strip():
                raise ValueError(f"{name} must not be empty")
        with self._lock:
            return self._state.positions.get((strategy_id, account_id, instrument_id))

    def snapshot(self) -> PaperBrokerSnapshot:
        with self._lock:
            state = self._state
            return PaperBrokerSnapshot(
                paper_run_id=self._paper_run_id,
                bound_source_session_id=state.bound_session,
                last_committed_ingest_sequence=state.last_sequence,
                next_paper_sequence=state.next_paper_sequence,
                snapshot_version=state.snapshot_version,
                execution_config_fingerprint=state.execution_config_fingerprint,
                orders=self._ordered_orders(state),
                fills=state.fills,
                events=state.events,
                instruments=tuple(state.instruments[key] for key in sorted(state.instruments)),
                positions=self._ordered_positions(state),
            )

    def export_checkpoint(self) -> VersionedCheckpoint:
        """Export every authoritative and retry-fence field as canonical JSON."""

        with self._lock:
            payload = {
                "execution_config": _checkpoint_encode(self._execution_config),
                "limits": _checkpoint_encode(self._limits),
                "paper_run_id": _checkpoint_encode(self._paper_run_id),
                "state": _checkpoint_encode(self._state),
            }
            encoded = json.dumps(
                payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True
            ).encode("utf-8")
        return VersionedCheckpoint.create(
            kind=CheckpointKind.BROKER,
            schema_version=1,
            payload=encoded,
        )

    @classmethod
    def restore_checkpoint(cls, checkpoint: VersionedCheckpoint) -> PaperBroker:
        """Restore a broker without consulting the environment, clock, or filesystem."""

        try:
            if type(checkpoint) is not VersionedCheckpoint:
                raise TypeError
            if checkpoint.kind is not CheckpointKind.BROKER or checkpoint.schema_version != 1:
                raise ValueError
            document = json.loads(
                checkpoint.payload.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_json_keys,
            )
            _validate_json_complexity(document)
            _exact_keys(
                document,
                {"execution_config", "limits", "paper_run_id", "state"},
            )
            paper_run_id = _checkpoint_decode(document["paper_run_id"])
            limits = _checkpoint_decode(document["limits"])
            execution_config = _checkpoint_decode(document["execution_config"])
            state = _checkpoint_decode(document["state"])
            if (
                type(paper_run_id) is not UUID
                or type(limits) is not PaperBrokerLimits
                or type(execution_config) is not PaperExecutionConfig
                or type(state) is not _BrokerState
            ):
                raise ValueError
            restored = cls(
                paper_run_id=paper_run_id,
                limits=limits,
                execution_config=execution_config,
                expected_source_session_id=state.effective_session,
            )
            restored._validate_checkpoint_state(state)
            restored._state = state
            if restored.export_checkpoint().payload != checkpoint.payload:
                raise ValueError
            return restored
        except PaperBrokerCheckpointError:
            raise
        except (
            TypeError,
            ValueError,
            KeyError,
            UnicodeError,
            json.JSONDecodeError,
            RecursionError,
        ):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid") from None

    def _validate_checkpoint_state(self, state: _BrokerState) -> None:
        if (
            type(state.effective_session) not in {UUID, type(None)}
            or type(state.bound_session) not in {UUID, type(None)}
            or type(state.bound_source) not in {str, type(None)}
            or type(state.last_sequence) not in {int, type(None)}
            or type(state.next_paper_sequence) is not int
            or type(state.snapshot_version) is not int
            or type(state.next_acceptance_ordinal) is not int
            or state.next_paper_sequence < 1
            or state.snapshot_version < 0
            or state.next_acceptance_ordinal < 1
        ):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if (
            any(
                type(key) is not UUID or type(value) is not PaperOrder
                for key, value in state.orders.items()
            )
            or any(
                type(key) is not tuple
                or len(key) != 3
                or any(type(part) is not str for part in key)
                or type(value) is not PaperPosition
                for key, value in state.positions.items()
            )
            or any(type(value) is not PaperFill for value in state.fills)
            or any(type(value) is not PaperEvent for value in state.events)
            or any(
                type(key) is not tuple
                or len(key) != 2
                or type(key[0]) is not str
                or type(key[1]) is not int
                or type(value) is not InstrumentMetadataSnapshot
                or key != (value.instrument_id, value.metadata_version)
                for key, value in state.instruments.items()
            )
        ):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if state.execution_config_fingerprint != self._execution_config.fingerprint:
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if state.effective_session is not None and state.bound_session not in {
            None,
            state.effective_session,
        }:
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if (state.bound_session is None) != (state.bound_source is None):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if (state.bound_session is None) != (state.last_sequence is None):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if state.next_paper_sequence != len(state.events) + 1:
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if tuple(event.paper_sequence for event in state.events) != tuple(
            range(1, state.next_paper_sequence)
        ):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if any(
            event.paper_event_id
            != self._uuid("event", str(event.paper_sequence), event.event_type.value)
            for event in state.events
        ):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if set(state.acceptance_ordinal) != set(state.orders):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        ordinals = tuple(sorted(state.acceptance_ordinal.values()))
        if ordinals != tuple(range(1, state.next_acceptance_ordinal)):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if set(state.eligibility_sequence) != set(state.orders):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if any(
            type(value) is not int or value < -1 for value in state.eligibility_sequence.values()
        ):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        for order_id, order in state.orders.items():
            if (
                order.paper_run_id != self._paper_run_id
                or order_id != order.paper_order_id
                or order_id
                != self._order_id(order.intent.strategy_id, order.intent.client_order_id)
            ):
                raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
            submit_record = state.submit_outcomes.get(
                (order.intent.strategy_id, order.intent.client_order_id)
            )
            if (
                submit_record is None
                or type(submit_record.primary_outcome) is not PaperOrder
                or submit_record.primary_outcome.paper_order_id != order_id
                or submit_record.primary_outcome.status is not OrderStatus.ACCEPTED
                or submit_record.primary_digest
                != _command_digest(submit_record.primary_outcome.intent)
            ):
                raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if any(fill.paper_run_id != self._paper_run_id for fill in state.fills):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        fill_ordinals: dict[tuple[UUID, UUID, int], int] = {}
        for fill in state.fills:
            fill_key = (
                fill.paper_order_id,
                fill.source_session_id,
                fill.source_ingest_sequence,
            )
            fill_ordinals[fill_key] = fill_ordinals.get(fill_key, 0) + 1
            if fill.paper_fill_id != self._uuid(
                "fill",
                str(fill.paper_order_id),
                str(fill.source_session_id),
                str(fill.source_ingest_sequence),
                str(fill_ordinals[fill_key]),
                state.execution_config_fingerprint,
            ):
                raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
            if (
                fill.paper_order_id not in state.orders
                or fill.execution_config_fingerprint != state.execution_config_fingerprint
            ):
                raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        for position_key, position in state.positions.items():
            if (
                position.paper_run_id != self._paper_run_id
                or position_key
                != (position.strategy_id, position.account_id, position.instrument_id)
                or position.paper_position_id
                != paper_position_id(self._paper_run_id, *position_key)
            ):
                raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        fill_events = tuple(
            event for event in state.events if event.event_type is PaperEventType.FILL_RECORDED
        )
        if (
            len(fill_events) != len(state.fills)
            or {
                event.payload.paper_fill_id
                for event in fill_events
                if isinstance(event.payload, PaperFill)
            }
            != {fill.paper_fill_id for fill in state.fills}
            or any(
                type(event.payload) is not PaperFill or event.payload not in state.fills
                for event in fill_events
            )
        ):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        for order_id, order in state.orders.items():
            order_events = tuple(
                event
                for event in state.events
                if type(event.payload) is PaperOrder and event.payload.paper_order_id == order_id
            )
            order_fills = tuple(fill for fill in state.fills if fill.paper_order_id == order_id)
            if (
                not order_events
                or order_events[0].event_type is not PaperEventType.ORDER_ACCEPTED
                or order_events[-1].payload != order
                or sum((fill.quantity for fill in order_fills), Decimal("0"))
                != order.filled_quantity
            ):
                raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        for position_key, position in state.positions.items():
            position_events = tuple(
                event
                for event in state.events
                if type(event.payload) is PaperPosition
                and (
                    event.payload.strategy_id,
                    event.payload.account_id,
                    event.payload.instrument_id,
                )
                == position_key
            )
            if not position_events or position_events[-1].payload != position:
                raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if any(
            (type(event.payload) is PaperOrder and event.payload.paper_order_id not in state.orders)
            or (
                type(event.payload) is PaperFill
                and event.payload.paper_order_id not in state.orders
            )
            for event in state.events
        ):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if len(state.orders) > self._limits.max_orders:
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if self._open_order_count(state) > self._limits.max_open_orders:
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if len(state.fills) > self._limits.max_fills or len(state.events) > self._limits.max_events:
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if len(state.positions) > self._limits.max_positions:
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if len(state.instruments) > self._limits.max_instrument_versions:
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if (
            len(state.submit_outcomes) > self._limits.max_orders
            or len(state.cancel_outcomes) > self._limits.max_orders
            or len(state.acceptance_ordinal) > self._limits.max_orders
            or len(state.eligibility_sequence) > self._limits.max_orders
        ):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        fence_keys = set(state.envelope_fingerprints)
        if (
            len(fence_keys) > self._limits.max_market_data_records
            or any(type(key) is not int or key < 0 for key in fence_keys)
            or any(
                type(value) is not str or len(value) != 64
                for value in state.envelope_fingerprints.values()
            )
            or any(
                type(value) is not str or not value.startswith("sha256:")
                for value in state.decision_fingerprints.values()
            )
            or any(
                type(key) is not str or len(key) != 64 or type(value) is not int
                for key, value in state.dedupe_sequences.items()
            )
        ):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if fence_keys != set(state.decision_fingerprints):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if any(sequence not in fence_keys for sequence in state.dedupe_sequences.values()):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        if state.last_sequence is not None and (
            not fence_keys or max(fence_keys) != state.last_sequence
        ):
            raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
        for key, record in state.submit_outcomes.items():
            if any(
                key != _outcome_strategy_client(outcome)
                or outcome.paper_run_id != self._paper_run_id
                for outcome in (record.primary_outcome, record.alternate_outcome)
                if outcome is not None
            ):
                raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
            _validate_outcome_record(record)
        for cancel_key, record in state.cancel_outcomes.items():
            outcome = record.primary_outcome
            if any(
                cancel_key
                != (
                    getattr(candidate, "paper_order_id", None),
                    *_outcome_strategy_client(candidate),
                )
                or candidate.paper_run_id != self._paper_run_id
                for candidate in (record.primary_outcome, record.alternate_outcome)
                if candidate is not None
            ) or (
                cancel_key[0] not in state.orders
                and not (
                    isinstance(outcome, PaperRejection)
                    and outcome.code is RejectionCode.UNKNOWN_ORDER
                )
            ):
                raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
            _validate_outcome_record(record)

    def publish(self, envelope: MarketDataEnvelope) -> None:
        self.process_market_data(envelope)

    def process_market_data(self, envelope: MarketDataEnvelope) -> MatchResult:
        if type(envelope) is not MarketDataEnvelope:
            raise TypeError("envelope must be MarketDataEnvelope")
        decision = PaperDecision(
            source_session_id=envelope.session_id,
            source_ingest_sequence=envelope.ingest_sequence,
            commands=(),
        )
        return self.process_decision_batch(envelope, decision).match_result

    def process_decision_batch(
        self,
        envelope: MarketDataEnvelope,
        decision: PaperDecision,
    ) -> PaperDecisionBatchResult:
        if type(envelope) is not MarketDataEnvelope:
            raise TypeError("envelope must be MarketDataEnvelope")
        if type(decision) is not PaperDecision:
            raise TypeError("decision must be PaperDecision")
        if (
            decision.source_session_id != envelope.session_id
            or decision.source_ingest_sequence != envelope.ingest_sequence
        ):
            raise PaperBrokerInputError("paper decision source causation mismatch")
        fingerprint = sha256(serialize_envelope(envelope).encode("utf-8")).hexdigest()
        dedupe_digest = sha256(envelope.dedupe_key.encode("utf-8")).hexdigest()
        with self._lock:
            state = self._state
            duplicate = self._validate_envelope_fence(
                state,
                envelope,
                fingerprint,
                dedupe_digest,
            )
            if duplicate:
                prior_decision = state.decision_fingerprints.get(envelope.ingest_sequence)
                if prior_decision != decision.decision_fingerprint:
                    raise PaperBrokerInputError("paper decision content conflict")
                match_result = MatchResult(
                    paper_run_id=self._paper_run_id,
                    disposition=MatchDisposition.DUPLICATE,
                    source_session_id=envelope.session_id,
                    source_ingest_sequence=envelope.ingest_sequence,
                    fills=(),
                    events=(),
                    skip_reasons=(),
                    snapshot_version=state.snapshot_version,
                )
                return PaperDecisionBatchResult(
                    paper_run_id=self._paper_run_id,
                    source_session_id=envelope.session_id,
                    source_ingest_sequence=envelope.ingest_sequence,
                    decision_fingerprint=decision.decision_fingerprint,
                    match_result=match_result,
                    command_results=(),
                    events=(),
                )
            if len(state.envelope_fingerprints) >= self._limits.max_market_data_records:
                raise PaperBrokerCapacityError("market-data record capacity was exceeded")

            orders = dict(state.orders)
            positions = dict(state.positions)
            instruments = dict(state.instruments)
            fills = list(state.fills)
            events = list(state.events)
            next_sequence = state.next_paper_sequence
            result_fills: list[PaperFill] = []
            result_positions: list[PaperPosition] = []
            result_events: list[PaperEvent] = []
            reasons: list[MatchSkipReason] = []

            if envelope.event_type is EventType.INSTRUMENT:
                payload = envelope.payload
                assert isinstance(payload, Instrument)
                metadata = InstrumentMetadataSnapshot(
                    instrument_id=payload.instrument_id,
                    metadata_version=payload.metadata_version,
                    price_scale=payload.price_scale,
                    quantity_scale=payload.quantity_scale,
                    currency=payload.currency,
                )
                metadata_key = (payload.instrument_id, payload.metadata_version)
                existing = instruments.get(metadata_key)
                if existing is not None and existing != metadata:
                    raise PaperBrokerInputError("instrument metadata version conflict")
                if existing is None and len(instruments) >= self._limits.max_instrument_versions:
                    raise PaperBrokerCapacityError(
                        "instrument metadata version capacity was exceeded"
                    )
                instruments[metadata_key] = metadata
                reasons.append(MatchSkipReason.EVENT_NOT_QUOTE)
            elif envelope.event_type is EventType.QUOTE:
                payload = envelope.payload
                assert isinstance(payload, Quote)
                quote_metadata = (
                    None
                    if envelope.metadata_version is None
                    else instruments.get((payload.instrument_id, envelope.metadata_version))
                )
                assessment = inspect_quote_top(payload, quote_metadata)
                if isinstance(assessment, tuple):
                    reasons.extend(assessment)
                else:
                    reasons.extend(assessment.skip_reasons)
                    next_sequence = self._stage_matches(
                        state=state,
                        envelope=envelope,
                        top=assessment,
                        orders=orders,
                        positions=positions,
                        fills=fills,
                        events=events,
                        result_fills=result_fills,
                        result_positions=result_positions,
                        result_events=result_events,
                        reasons=reasons,
                        next_sequence=next_sequence,
                    )
            else:
                reasons.append(MatchSkipReason.EVENT_NOT_QUOTE)

            if (
                len(fills) > self._limits.max_fills
                or len(events) > self._limits.max_events
                or len(positions) > self._limits.max_positions
            ):
                raise PaperBrokerCapacityError("market-data batch exceeds paper broker capacity")
            staged = replace(
                state,
                effective_session=envelope.session_id,
                bound_session=envelope.session_id,
                bound_source=envelope.source,
                last_sequence=envelope.ingest_sequence,
                next_paper_sequence=next_sequence,
                orders=_mapping(orders),
                positions=_mapping(positions),
                fills=tuple(fills),
                events=tuple(events),
                instruments=_mapping(instruments),
            )
            command_event_start = len(staged.events)
            command_results: list[OrderCommandResult] = []
            for command in decision.commands:
                staged, outcome = self._stage_batch_command(staged, command, envelope)
                command_results.append(outcome)

            if (
                len(staged.fills) > self._limits.max_fills
                or len(staged.events) > self._limits.max_events
                or len(staged.positions) > self._limits.max_positions
            ):
                raise PaperBrokerCapacityError("paper decision batch exceeds paper broker capacity")
            fingerprints = dict(state.envelope_fingerprints)
            fingerprints[envelope.ingest_sequence] = fingerprint
            decision_fingerprints = dict(state.decision_fingerprints)
            decision_fingerprints[envelope.ingest_sequence] = decision.decision_fingerprint
            dedupe_sequences = dict(state.dedupe_sequences)
            dedupe_sequences[dedupe_digest] = envelope.ingest_sequence
            staged = replace(
                staged,
                snapshot_version=state.snapshot_version + 1,
                envelope_fingerprints=_mapping(fingerprints),
                decision_fingerprints=_mapping(decision_fingerprints),
                dedupe_sequences=_mapping(dedupe_sequences),
            )
            match_result = MatchResult(
                paper_run_id=self._paper_run_id,
                disposition=MatchDisposition.PROCESSED,
                source_session_id=envelope.session_id,
                source_ingest_sequence=envelope.ingest_sequence,
                fills=tuple(result_fills),
                events=tuple(result_events),
                skip_reasons=_unique_reasons(reasons),
                snapshot_version=staged.snapshot_version,
                positions=tuple(result_positions),
            )
            result = PaperDecisionBatchResult(
                paper_run_id=self._paper_run_id,
                source_session_id=envelope.session_id,
                source_ingest_sequence=envelope.ingest_sequence,
                decision_fingerprint=decision.decision_fingerprint,
                match_result=match_result,
                command_results=tuple(command_results),
                events=tuple(result_events) + staged.events[command_event_start:],
            )
            self._commit(staged)
            return result

    def _stage_batch_command(
        self,
        state: _BrokerState,
        command: OrderIntent | CancelIntent,
        envelope: MarketDataEnvelope,
    ) -> tuple[_BrokerState, OrderCommandResult]:
        if isinstance(command, OrderIntent):
            return self._stage_batch_submit(state, command, envelope)
        return self._stage_batch_cancel(state, command, envelope)

    def _stage_batch_submit(
        self,
        state: _BrokerState,
        intent: OrderIntent,
        envelope: MarketDataEnvelope,
    ) -> tuple[_BrokerState, OrderCommandResult]:
        digest = _command_digest(intent)
        key = (intent.strategy_id, intent.client_order_id)
        record = state.submit_outcomes.get(key)
        if record is not None:
            cached = record.find(digest)
            if cached is not None:
                return state, self._resolve_cached_outcome(state, cached)
            conflict = self._rejection_value(
                intent.strategy_id,
                intent.client_order_id,
                RejectionCode.IDEMPOTENCY_CONFLICT,
                intent.created_at,
                self._order_id(*key),
            )
            if record.has_alternate:
                return state, conflict
            outcomes = dict(state.submit_outcomes)
            outcomes[key] = replace(
                record,
                alternate_digest=digest,
                alternate_outcome=conflict,
            )
            staged = replace(state, submit_outcomes=_mapping(outcomes))
            return self._stage_batch_rejection(staged, conflict, envelope), conflict

        if len(state.submit_outcomes) >= self._limits.max_orders:
            rejection = self._rejection_value(
                intent.strategy_id,
                intent.client_order_id,
                RejectionCode.CAPACITY_EXCEEDED,
                intent.created_at,
                self._order_id(*key),
            )
            return state, rejection

        if (
            len(state.orders) >= self._limits.max_orders
            or self._open_order_count(state) >= self._limits.max_open_orders
            or len(state.events) >= self._limits.max_events
        ):
            rejection = self._rejection_value(
                intent.strategy_id,
                intent.client_order_id,
                RejectionCode.CAPACITY_EXCEEDED,
                intent.created_at,
                self._order_id(*key),
            )
            outcomes = dict(state.submit_outcomes)
            outcomes[key] = _OutcomeRecord(digest, rejection)
            staged = replace(state, submit_outcomes=_mapping(outcomes))
            return self._stage_batch_rejection(staged, rejection, envelope), rejection

        order_id = self._order_id(*key)
        with localcontext(MATCHING_CONTEXT):
            order = PaperOrder(
                paper_run_id=self._paper_run_id,
                paper_order_id=order_id,
                intent=intent,
                status=OrderStatus.ACCEPTED,
                filled_quantity=Decimal(0),
                remaining_quantity=intent.quantity,
                average_fill_price=None,
                accepted_at=intent.created_at,
                updated_at=intent.created_at,
            )
        event = self._make_event(
            state.next_paper_sequence,
            PaperEventType.ORDER_ACCEPTED,
            order,
            intent.created_at,
            envelope,
        )
        orders = dict(state.orders)
        orders[order_id] = order
        outcomes = dict(state.submit_outcomes)
        outcomes[key] = _OutcomeRecord(digest, order)
        ordinals = dict(state.acceptance_ordinal)
        ordinals[order_id] = state.next_acceptance_ordinal
        eligibility = dict(state.eligibility_sequence)
        eligibility[order_id] = envelope.ingest_sequence
        return (
            replace(
                state,
                orders=_mapping(orders),
                submit_outcomes=_mapping(outcomes),
                acceptance_ordinal=_mapping(ordinals),
                eligibility_sequence=_mapping(eligibility),
                next_acceptance_ordinal=state.next_acceptance_ordinal + 1,
                events=state.events + (event,),
                next_paper_sequence=state.next_paper_sequence + 1,
            ),
            order,
        )

    def _stage_batch_cancel(
        self,
        state: _BrokerState,
        request: CancelIntent,
        envelope: MarketDataEnvelope,
    ) -> tuple[_BrokerState, OrderCommandResult]:
        digest = _command_digest(request)
        key = (
            request.paper_order_id,
            request.strategy_id,
            request.client_order_id,
        )
        record = state.cancel_outcomes.get(key)
        if record is not None:
            cached = record.find(digest)
            if cached is not None:
                return state, self._resolve_cached_outcome(state, cached)

        outcome, event_type = self._evaluate_cancel(state, request)
        if isinstance(outcome, PaperOrder) and event_type is None:
            return state, outcome

        can_cache = (record is None and len(state.cancel_outcomes) < self._limits.max_orders) or (
            record is not None and not record.has_alternate
        )
        staged = state
        if can_cache:
            outcomes = dict(state.cancel_outcomes)
            if record is None:
                outcomes[key] = _OutcomeRecord(digest, outcome)
            else:
                outcomes[key] = replace(
                    record,
                    alternate_digest=digest,
                    alternate_outcome=outcome,
                )
            staged = replace(state, cancel_outcomes=_mapping(outcomes))

        if event_type is not None:
            assert isinstance(outcome, PaperOrder)
            event = self._make_event(
                staged.next_paper_sequence,
                event_type,
                outcome,
                request.requested_at,
                envelope,
            )
            orders = dict(staged.orders)
            orders[outcome.paper_order_id] = outcome
            return (
                replace(
                    staged,
                    orders=_mapping(orders),
                    events=staged.events + (event,),
                    next_paper_sequence=staged.next_paper_sequence + 1,
                ),
                outcome,
            )

        assert isinstance(outcome, PaperRejection)
        if not can_cache:
            return staged, outcome
        return self._stage_batch_rejection(staged, outcome, envelope), outcome

    def _stage_batch_rejection(
        self,
        state: _BrokerState,
        rejection: PaperRejection,
        envelope: MarketDataEnvelope,
    ) -> _BrokerState:
        if len(state.events) >= self._limits.max_events:
            return state
        event = self._make_event(
            state.next_paper_sequence,
            PaperEventType.ORDER_REJECTED,
            rejection,
            rejection.rejected_at,
            envelope,
        )
        return replace(
            state,
            events=state.events + (event,),
            next_paper_sequence=state.next_paper_sequence + 1,
        )

    def _stage_matches(
        self,
        *,
        state: _BrokerState,
        envelope: MarketDataEnvelope,
        top: QuoteTop,
        orders: dict[UUID, PaperOrder],
        positions: dict[PositionKey, PaperPosition],
        fills: list[PaperFill],
        events: list[PaperEvent],
        result_fills: list[PaperFill],
        result_positions: list[PaperPosition],
        result_events: list[PaperEvent],
        reasons: list[MatchSkipReason],
        next_sequence: int,
    ) -> int:
        payload = envelope.payload
        assert isinstance(payload, Quote)
        capacities = {
            OrderSide.BUY: top.ask_capacity,
            OrderSide.SELL: top.bid_capacity,
        }
        causal_time = envelope.event_at or envelope.received_at
        ordered_ids = [
            order_id
            for order_id, _ in sorted(
                state.acceptance_ordinal.items(),
                key=lambda item: item[1],
            )
        ]
        fill_ordinal = 0
        for order_id in ordered_ids:
            order = orders[order_id]
            if order.status.is_terminal or order.intent.instrument_id != payload.instrument_id:
                continue
            if (
                envelope.ingest_sequence <= state.eligibility_sequence[order_id]
                or (
                    order.intent.source_session_id is not None
                    and order.intent.source_session_id != envelope.session_id
                )
                or causal_time < order.updated_at
            ):
                reasons.append(MatchSkipReason.ORDER_NOT_ELIGIBLE)
                continue
            reference_price = execution_price(order, top)
            if reference_price is None:
                reasons.append(MatchSkipReason.LIMIT_NOT_CROSSED)
                continue
            available = capacities[order.intent.side]
            if available is None:
                reasons.append(MatchSkipReason.QUANTITY_UNAVAILABLE)
                continue
            if available <= 0:
                reasons.append(MatchSkipReason.NO_LIQUIDITY)
                continue
            try:
                slippage = assess_slippage(
                    reference_price,
                    order.intent.side,
                    self._execution_config.slippage,
                )
                limit = assess_limit(
                    slippage.execution_price,
                    order.intent.side,
                    order.intent.limit_price,
                )
            except ExecutionPolicyError as exc:
                skip_reason = _policy_skip_reason(exc)
                if skip_reason is None:
                    raise
                reasons.append(skip_reason)
                continue
            if not limit.executable:
                assert limit.skip_reason is not None
                reasons.append(limit.skip_reason)
                continue
            with localcontext(MATCHING_CONTEXT):
                fill_quantity = min(order.remaining_quantity, available)
                filled_quantity = order.filled_quantity + fill_quantity
                try:
                    fee = assess_fee(
                        self._execution_config.fee_schedule,
                        instrument_id=order.intent.instrument_id,
                        metadata_currency=top.currency,
                        cumulative_quantity_before=order.filled_quantity,
                        cumulative_quantity_after=filled_quantity,
                    )
                except ExecutionPolicyError as exc:
                    skip_reason = _policy_skip_reason(exc)
                    if skip_reason is None:
                        raise
                    reasons.append(skip_reason)
                    continue
                candidate_fill_ordinal = fill_ordinal + 1
                fill = PaperFill(
                    paper_run_id=self._paper_run_id,
                    paper_fill_id=self._uuid(
                        "fill",
                        str(order_id),
                        str(envelope.session_id),
                        str(envelope.ingest_sequence),
                        str(candidate_fill_ordinal),
                        state.execution_config_fingerprint,
                    ),
                    paper_order_id=order_id,
                    strategy_id=order.intent.strategy_id,
                    account_id=order.intent.account_id,
                    instrument_id=order.intent.instrument_id,
                    side=order.intent.side,
                    quantity=fill_quantity,
                    execution_price=slippage.execution_price,
                    fee=fee.fee,
                    source_session_id=envelope.session_id,
                    source_ingest_sequence=envelope.ingest_sequence,
                    occurred_at=causal_time,
                    reference_price=slippage.reference_price,
                    slippage_amount=slippage.slippage_amount,
                    fee_currency=fee.currency,
                    execution_config_fingerprint=state.execution_config_fingerprint,
                )
                status = (
                    OrderStatus.FILLED
                    if filled_quantity == order.intent.quantity
                    else OrderStatus.PARTIALLY_FILLED
                )
                updated = replace(
                    order,
                    status=status,
                    filled_quantity=filled_quantity,
                    remaining_quantity=order.intent.quantity - filled_quantity,
                    average_fill_price=weighted_average(
                        order.filled_quantity,
                        order.average_fill_price,
                        fill_quantity,
                        slippage.execution_price,
                    ),
                    updated_at=causal_time,
                )
                validate_order_transition(order, updated)
                position_key = (
                    order.intent.strategy_id,
                    order.intent.account_id,
                    order.intent.instrument_id,
                )
                try:
                    position = apply_fill_to_position(positions.get(position_key), fill)
                except PositionLedgerError as exc:
                    if exc.code is not PositionLedgerErrorCode.TEMPORAL_ORDER:
                        raise
                    reasons.append(MatchSkipReason.ORDER_NOT_ELIGIBLE)
                    continue
                fill_ordinal = candidate_fill_ordinal
                remaining_capacity = available - fill_quantity
            fill_event = self._make_event(
                next_sequence,
                PaperEventType.FILL_RECORDED,
                fill,
                causal_time,
                envelope,
            )
            order_event = self._make_event(
                next_sequence + 1,
                (
                    PaperEventType.ORDER_FILLED
                    if status is OrderStatus.FILLED
                    else PaperEventType.ORDER_PARTIALLY_FILLED
                ),
                updated,
                causal_time,
                envelope,
            )
            position_event = self._make_event(
                next_sequence + 2,
                PaperEventType.POSITION_CHANGED,
                position,
                causal_time,
                envelope,
            )
            next_sequence += 3
            orders[order_id] = updated
            positions[position_key] = position
            fills.append(fill)
            result_fills.append(fill)
            result_positions.append(position)
            events.extend((fill_event, order_event, position_event))
            result_events.extend((fill_event, order_event, position_event))
            capacities[order.intent.side] = remaining_capacity
        return next_sequence

    def _evaluate_cancel(
        self,
        state: _BrokerState,
        request: CancelIntent,
    ) -> tuple[OrderCommandResult, PaperEventType | None]:
        order = state.orders.get(request.paper_order_id)
        if (
            order is None
            or order.intent.strategy_id != request.strategy_id
            or order.intent.client_order_id != request.client_order_id
        ):
            return (
                self._rejection_value(
                    request.strategy_id,
                    request.client_order_id,
                    RejectionCode.UNKNOWN_ORDER,
                    request.requested_at,
                    request.paper_order_id,
                ),
                None,
            )
        if request.requested_at < order.updated_at:
            return (
                self._rejection_value(
                    request.strategy_id,
                    request.client_order_id,
                    RejectionCode.INVALID_INTENT,
                    request.requested_at,
                    request.paper_order_id,
                ),
                None,
            )
        if order.status is OrderStatus.CANCELLED:
            return order, None
        if order.status.is_terminal:
            return (
                self._rejection_value(
                    request.strategy_id,
                    request.client_order_id,
                    RejectionCode.ORDER_TERMINAL,
                    request.requested_at,
                    request.paper_order_id,
                ),
                None,
            )
        if len(state.events) >= self._limits.max_events:
            return (
                self._rejection_value(
                    request.strategy_id,
                    request.client_order_id,
                    RejectionCode.CAPACITY_EXCEEDED,
                    request.requested_at,
                    request.paper_order_id,
                ),
                None,
            )
        with localcontext(MATCHING_CONTEXT):
            cancelled = replace(
                order,
                status=OrderStatus.CANCELLED,
                updated_at=request.requested_at,
            )
            validate_order_transition(order, cancelled)
        return cancelled, PaperEventType.ORDER_CANCELLED

    def _validate_envelope_fence(
        self,
        state: _BrokerState,
        envelope: MarketDataEnvelope,
        fingerprint: str,
        dedupe_digest: str,
    ) -> bool:
        if state.effective_session is not None and envelope.session_id != state.effective_session:
            raise PaperBrokerInputError("market-data source session mismatch")
        if state.bound_source is not None and envelope.source != state.bound_source:
            raise PaperBrokerInputError("market-data source mismatch")
        previous_fingerprint = state.envelope_fingerprints.get(envelope.ingest_sequence)
        if previous_fingerprint is not None:
            if previous_fingerprint == fingerprint:
                return True
            raise PaperBrokerInputError("market-data sequence content conflict")
        if state.last_sequence is not None and envelope.ingest_sequence < state.last_sequence:
            raise PaperBrokerInputError("market-data sequence is out of order")
        if dedupe_digest in state.dedupe_sequences:
            raise PaperBrokerInputError("market-data dedupe key was reused")
        return False

    def _commit_new_submit_rejection(
        self,
        state: _BrokerState,
        key: SubmitKey,
        digest: str,
        rejection: PaperRejection,
    ) -> PaperRejection:
        if len(state.submit_outcomes) >= self._limits.max_orders:
            return rejection
        outcomes = dict(state.submit_outcomes)
        outcomes[key] = _OutcomeRecord(digest, rejection)
        staged = self._stage_rejection(
            state,
            rejection,
            submit_outcomes=outcomes,
        )
        self._commit(staged)
        return rejection

    def _stage_rejection(
        self,
        state: _BrokerState,
        rejection: PaperRejection,
        *,
        submit_update: tuple[SubmitKey, _OutcomeRecord] | None = None,
        submit_outcomes: Mapping[SubmitKey, _OutcomeRecord] | None = None,
    ) -> _BrokerState:
        outcomes = dict(state.submit_outcomes if submit_outcomes is None else submit_outcomes)
        if submit_update is not None:
            outcomes[submit_update[0]] = submit_update[1]
        staged = replace(state, submit_outcomes=_mapping(outcomes))
        staged = self._append_rejection_event(staged, rejection)
        if staged.snapshot_version == state.snapshot_version:
            staged = replace(staged, snapshot_version=state.snapshot_version + 1)
        return staged

    def _append_rejection_event(
        self,
        state: _BrokerState,
        rejection: PaperRejection,
    ) -> _BrokerState:
        if len(state.events) >= self._limits.max_events:
            return state
        event = self._make_event(
            state.next_paper_sequence,
            PaperEventType.ORDER_REJECTED,
            rejection,
            rejection.rejected_at,
        )
        return replace(
            state,
            events=state.events + (event,),
            next_paper_sequence=state.next_paper_sequence + 1,
            snapshot_version=state.snapshot_version + 1,
        )

    def _rejection_value(
        self,
        strategy_id: str,
        client_order_id: str,
        code: RejectionCode,
        rejected_at: datetime,
        paper_order_id: UUID | None,
    ) -> PaperRejection:
        return PaperRejection(
            paper_run_id=self._paper_run_id,
            strategy_id=strategy_id,
            client_order_id=client_order_id,
            code=code,
            rejected_at=rejected_at,
            paper_order_id=paper_order_id,
        )

    def _make_event(
        self,
        sequence: int,
        event_type: PaperEventType,
        payload: PaperOrder | PaperFill | PaperPosition | PaperRejection,
        occurred_at: datetime,
        envelope: MarketDataEnvelope | None = None,
        *,
        source_session_id: UUID | None = None,
        source_ingest_sequence: int | None = None,
    ) -> PaperEvent:
        if envelope is not None:
            source_session_id = envelope.session_id
            source_ingest_sequence = envelope.ingest_sequence
        return PaperEvent(
            paper_run_id=self._paper_run_id,
            paper_event_id=self._uuid("event", str(sequence), event_type.value),
            paper_sequence=sequence,
            event_type=event_type,
            payload=payload,
            occurred_at=occurred_at,
            source_session_id=source_session_id,
            source_ingest_sequence=source_ingest_sequence,
        )

    def _commit(self, staged_state: _BrokerState) -> None:
        """The sole production commit boundary for authoritative broker state."""

        self._state = staged_state

    @staticmethod
    def _resolve_cached_outcome(
        state: _BrokerState,
        outcome: OrderCommandResult,
    ) -> OrderCommandResult:
        if isinstance(outcome, PaperOrder):
            return state.orders.get(outcome.paper_order_id, outcome)
        return outcome

    def _uuid(self, *parts: str) -> UUID:
        return uuid5(self._paper_run_id, "\x1f".join(parts))

    def _order_id(self, strategy_id: str, client_order_id: str) -> UUID:
        return self._uuid("order", strategy_id, client_order_id)

    @staticmethod
    def _open_order_count(state: _BrokerState) -> int:
        return sum(not order.status.is_terminal for order in state.orders.values())

    @staticmethod
    def _ordered_orders(state: _BrokerState) -> tuple[PaperOrder, ...]:
        return tuple(
            state.orders[order_id]
            for order_id, _ in sorted(
                state.acceptance_ordinal.items(),
                key=lambda item: item[1],
            )
        )

    @staticmethod
    def _ordered_positions(state: _BrokerState) -> tuple[PaperPosition, ...]:
        return tuple(state.positions[key] for key in sorted(state.positions))


def _command_digest(command: OrderIntent | CancelIntent) -> str:
    return sha256(canonical_json(command).encode("utf-8")).hexdigest()


def _unique_reasons(
    reasons: list[MatchSkipReason],
) -> tuple[MatchSkipReason, ...]:
    return tuple(dict.fromkeys(reasons))


def _policy_skip_reason(error: ExecutionPolicyError) -> MatchSkipReason | None:
    if error.code is ExecutionPolicyErrorCode.FEE_CURRENCY_MISMATCH:
        return MatchSkipReason.METADATA_MISMATCH
    if error.code in {
        ExecutionPolicyErrorCode.FEE_RULE_MISSING,
        ExecutionPolicyErrorCode.FEE_CURRENCY_MISSING,
    }:
        return MatchSkipReason.METADATA_UNAVAILABLE
    if error.code is ExecutionPolicyErrorCode.SLIPPAGE_UNREPRESENTABLE:
        return MatchSkipReason.PRICE_UNAVAILABLE
    return None


_CHECKPOINT_TYPES: dict[str, Any] = {
    item.__name__: item
    for item in (
        _BrokerState,
        _OutcomeRecord,
        PaperBrokerLimits,
        PaperExecutionConfig,
        SlippageConfig,
        PaperFeeSchedule,
        PaperFeeRule,
        OrderIntent,
        PaperOrder,
        PaperFill,
        PaperRejection,
        PaperPosition,
        PaperEvent,
        InstrumentMetadataSnapshot,
    )
}
_CHECKPOINT_ENUMS: dict[str, Any] = {
    item.__name__: item
    for item in (
        OrderSide,
        OrderStatus,
        OrderType,
        TimeInForce,
        ExecutionProvenance,
        RejectionCode,
        PaperEventType,
        SlippageMode,
        FeePolicyKind,
        FeeRoundingMode,
    )
}


def _checkpoint_encode(value: object) -> object:
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
        return {"$tuple": [_checkpoint_encode(item) for item in value]}
    if isinstance(value, Mapping):
        encoded_items = [
            [_checkpoint_encode(key), _checkpoint_encode(item)] for key, item in value.items()
        ]
        encoded_items.sort(
            key=lambda item: json.dumps(
                item[0], ensure_ascii=True, separators=(",", ":"), sort_keys=True
            )
        )
        return {"$map": encoded_items}
    value_type = type(value)
    if value_type.__name__ in _CHECKPOINT_TYPES:
        return {
            "$type": value_type.__name__,
            **{
                item.name: _checkpoint_encode(getattr(value, item.name))
                for item in fields(value)  # type: ignore[arg-type]
                if item.init
            },
        }
    raise TypeError("unsupported paper broker checkpoint value")


def _checkpoint_decode(value: object) -> object:
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
        return tuple(_checkpoint_decode(item) for item in value["$tuple"])
    if "$map" in value:
        _exact_keys(value, {"$map"})
        if type(value["$map"]) is not list:
            raise TypeError
        result: dict[object, object] = {}
        for pair in value["$map"]:
            if type(pair) is not list or len(pair) != 2:
                raise TypeError
            key = _checkpoint_decode(pair[0])
            if key in result:
                raise ValueError
            result[key] = _checkpoint_decode(pair[1])
        return _mapping(result)
    type_name = value.get("$type")
    if type(type_name) is not str:
        raise TypeError
    value_type = _CHECKPOINT_TYPES.get(type_name)
    if value_type is None:
        raise TypeError
    expected = {"$type", *(item.name for item in fields(value_type) if item.init)}
    _exact_keys(value, expected)
    arguments = {
        item.name: _checkpoint_decode(value[item.name]) for item in fields(value_type) if item.init
    }
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


def _validate_json_complexity(value: object) -> None:
    stack = [(value, 0)]
    node_count = 0
    while stack:
        current, depth = stack.pop()
        node_count += 1
        if depth > 64 or node_count > 1_000_000:
            raise ValueError
        if type(current) is dict:
            stack.extend((item, depth + 1) for item in current.values())
        elif type(current) is list:
            stack.extend((item, depth + 1) for item in current)


def _validate_outcome_record(record: _OutcomeRecord) -> None:
    if (
        type(record.primary_digest) is not str
        or len(record.primary_digest) != 64
        or any(character not in "0123456789abcdef" for character in record.primary_digest)
        or type(record.primary_outcome) not in {PaperOrder, PaperRejection}
    ):
        raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
    if (record.alternate_digest is None) != (record.alternate_outcome is None):
        raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
    if record.alternate_digest == record.primary_digest:
        raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")
    if record.alternate_digest is not None and (
        type(record.alternate_digest) is not str
        or len(record.alternate_digest) != 64
        or any(character not in "0123456789abcdef" for character in record.alternate_digest)
        or type(record.alternate_outcome) not in {PaperOrder, PaperRejection}
    ):
        raise PaperBrokerCheckpointError("paper broker checkpoint is invalid")


def _outcome_strategy_client(outcome: OrderCommandResult) -> tuple[str, str]:
    if isinstance(outcome, PaperOrder):
        return outcome.intent.strategy_id, outcome.intent.client_order_id
    return outcome.strategy_id, outcome.client_order_id
