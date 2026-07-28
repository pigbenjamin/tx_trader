"""Synchronous transactional in-memory broker for deterministic paper matching."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, localcontext
from hashlib import sha256
from threading import RLock
from types import MappingProxyType
from typing import Mapping, TypeAlias, TypeVar
from uuid import UUID, uuid5

from tx_trade.market_data.models import (
    EventType,
    Instrument,
    MarketDataEnvelope,
    Quote,
    serialize_envelope,
)

from .contracts import (
    CancelIntent,
    InstrumentMetadataSnapshot,
    MatchDisposition,
    MatchResult,
    MatchSkipReason,
    OrderIntent,
    OrderSide,
    OrderStatus,
    PaperBrokerLimits,
    PaperBrokerSnapshot,
    PaperEvent,
    PaperEventType,
    PaperFill,
    PaperOrder,
    PaperPosition,
    PaperRejection,
    RejectionCode,
    canonical_json,
)
from .matching import (
    MATCHING_CONTEXT,
    QuoteTop,
    execution_price,
    inspect_quote_top,
    weighted_average,
)
from .ports import OrderCommandResult
from .state_machine import validate_order_transition

SubmitKey: TypeAlias = tuple[str, str]
CancelKey: TypeAlias = tuple[UUID, str, str]
_Key = TypeVar("_Key")
_Value = TypeVar("_Value")


class PaperBrokerInputError(ValueError):
    """Stable fail-closed error for conflicting market-data input."""


class PaperBrokerCapacityError(RuntimeError):
    """Raised when an envelope's complete staged batch cannot fit."""


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
    orders: Mapping[UUID, PaperOrder]
    submit_outcomes: Mapping[SubmitKey, _OutcomeRecord]
    cancel_outcomes: Mapping[CancelKey, _OutcomeRecord]
    acceptance_ordinal: Mapping[UUID, int]
    eligibility_sequence: Mapping[UUID, int]
    next_acceptance_ordinal: int
    fills: tuple[PaperFill, ...]
    events: tuple[PaperEvent, ...]
    instruments: Mapping[tuple[str, int], InstrumentMetadataSnapshot]
    envelope_fingerprints: Mapping[int, str]
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
        expected_source_session_id: UUID | None = None,
    ) -> None:
        if type(paper_run_id) is not UUID:
            raise TypeError("paper_run_id must be UUID")
        if type(limits) is not PaperBrokerLimits:
            raise TypeError("limits must be PaperBrokerLimits")
        if expected_source_session_id is not None and type(expected_source_session_id) is not UUID:
            raise TypeError("expected_source_session_id must be UUID or None")
        self._paper_run_id = paper_run_id
        self._limits = limits
        self._lock = RLock()
        self._state = _BrokerState(
            effective_session=expected_source_session_id,
            bound_session=None,
            bound_source=None,
            last_sequence=None,
            next_paper_sequence=1,
            snapshot_version=0,
            orders=_mapping(),
            submit_outcomes=_mapping(),
            cancel_outcomes=_mapping(),
            acceptance_ordinal=_mapping(),
            eligibility_sequence=_mapping(),
            next_acceptance_ordinal=1,
            fills=(),
            events=(),
            instruments=_mapping(),
            envelope_fingerprints=_mapping(),
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
        return ()

    def snapshot(self) -> PaperBrokerSnapshot:
        with self._lock:
            state = self._state
            return PaperBrokerSnapshot(
                paper_run_id=self._paper_run_id,
                bound_source_session_id=state.bound_session,
                last_committed_ingest_sequence=state.last_sequence,
                next_paper_sequence=state.next_paper_sequence,
                snapshot_version=state.snapshot_version,
                orders=self._ordered_orders(state),
                fills=state.fills,
                events=state.events,
                instruments=tuple(state.instruments[key] for key in sorted(state.instruments)),
            )

    def publish(self, envelope: MarketDataEnvelope) -> None:
        self.process_market_data(envelope)

    def process_market_data(self, envelope: MarketDataEnvelope) -> MatchResult:
        if type(envelope) is not MarketDataEnvelope:
            raise TypeError("envelope must be MarketDataEnvelope")
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
                return MatchResult(
                    paper_run_id=self._paper_run_id,
                    disposition=MatchDisposition.DUPLICATE,
                    source_session_id=envelope.session_id,
                    source_ingest_sequence=envelope.ingest_sequence,
                    fills=(),
                    events=(),
                    skip_reasons=(),
                    snapshot_version=state.snapshot_version,
                )
            if len(state.envelope_fingerprints) >= self._limits.max_market_data_records:
                raise PaperBrokerCapacityError("market-data record capacity was exceeded")

            orders = dict(state.orders)
            instruments = dict(state.instruments)
            fills = list(state.fills)
            events = list(state.events)
            next_sequence = state.next_paper_sequence
            result_fills: list[PaperFill] = []
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
                        fills=fills,
                        events=events,
                        result_fills=result_fills,
                        result_events=result_events,
                        reasons=reasons,
                        next_sequence=next_sequence,
                    )
            else:
                reasons.append(MatchSkipReason.EVENT_NOT_QUOTE)

            if len(fills) > self._limits.max_fills or len(events) > self._limits.max_events:
                raise PaperBrokerCapacityError("market-data batch exceeds paper broker capacity")
            fingerprints = dict(state.envelope_fingerprints)
            fingerprints[envelope.ingest_sequence] = fingerprint
            dedupe_sequences = dict(state.dedupe_sequences)
            dedupe_sequences[dedupe_digest] = envelope.ingest_sequence
            staged = replace(
                state,
                effective_session=envelope.session_id,
                bound_session=envelope.session_id,
                bound_source=envelope.source,
                last_sequence=envelope.ingest_sequence,
                next_paper_sequence=next_sequence,
                snapshot_version=state.snapshot_version + 1,
                orders=_mapping(orders),
                fills=tuple(fills),
                events=tuple(events),
                instruments=_mapping(instruments),
                envelope_fingerprints=_mapping(fingerprints),
                dedupe_sequences=_mapping(dedupe_sequences),
            )
            result = MatchResult(
                paper_run_id=self._paper_run_id,
                disposition=MatchDisposition.PROCESSED,
                source_session_id=envelope.session_id,
                source_ingest_sequence=envelope.ingest_sequence,
                fills=tuple(result_fills),
                events=tuple(result_events),
                skip_reasons=_unique_reasons(reasons),
                snapshot_version=staged.snapshot_version,
            )
            self._commit(staged)
            return result

    def _stage_matches(
        self,
        *,
        state: _BrokerState,
        envelope: MarketDataEnvelope,
        top: QuoteTop,
        orders: dict[UUID, PaperOrder],
        fills: list[PaperFill],
        events: list[PaperEvent],
        result_fills: list[PaperFill],
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
            price = execution_price(order, top)
            if price is None:
                reasons.append(MatchSkipReason.LIMIT_NOT_CROSSED)
                continue
            available = capacities[order.intent.side]
            if available is None:
                reasons.append(MatchSkipReason.QUANTITY_UNAVAILABLE)
                continue
            if available <= 0:
                reasons.append(MatchSkipReason.NO_LIQUIDITY)
                continue
            with localcontext(MATCHING_CONTEXT):
                fill_quantity = min(order.remaining_quantity, available)
                fill_ordinal += 1
                fill = PaperFill(
                    paper_run_id=self._paper_run_id,
                    paper_fill_id=self._uuid(
                        "fill",
                        str(order_id),
                        str(envelope.session_id),
                        str(envelope.ingest_sequence),
                        str(fill_ordinal),
                    ),
                    paper_order_id=order_id,
                    strategy_id=order.intent.strategy_id,
                    account_id=order.intent.account_id,
                    instrument_id=order.intent.instrument_id,
                    side=order.intent.side,
                    quantity=fill_quantity,
                    execution_price=price,
                    fee=Decimal(0),
                    source_session_id=envelope.session_id,
                    source_ingest_sequence=envelope.ingest_sequence,
                    occurred_at=causal_time,
                )
                filled_quantity = order.filled_quantity + fill_quantity
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
                        price,
                    ),
                    updated_at=causal_time,
                )
                validate_order_transition(order, updated)
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
            next_sequence += 2
            orders[order_id] = updated
            fills.append(fill)
            result_fills.append(fill)
            events.extend((fill_event, order_event))
            result_events.extend((fill_event, order_event))
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
        payload: PaperOrder | PaperFill | PaperRejection,
        occurred_at: datetime,
        envelope: MarketDataEnvelope | None = None,
    ) -> PaperEvent:
        return PaperEvent(
            paper_run_id=self._paper_run_id,
            paper_event_id=self._uuid("event", str(sequence), event_type.value),
            paper_sequence=sequence,
            event_type=event_type,
            payload=payload,
            occurred_at=occurred_at,
            source_session_id=None if envelope is None else envelope.session_id,
            source_ingest_sequence=(None if envelope is None else envelope.ingest_sequence),
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


def _command_digest(command: OrderIntent | CancelIntent) -> str:
    return sha256(canonical_json(command).encode("utf-8")).hexdigest()


def _unique_reasons(
    reasons: list[MatchSkipReason],
) -> tuple[MatchSkipReason, ...]:
    return tuple(dict.fromkeys(reasons))
