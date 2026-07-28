from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from uuid import UUID

import pytest

from tx_trade.market_data.fixtures import (
    InMemoryReplaySource,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import SCHEMA_VERSION, EventType, Instrument, Quote
from tx_trade.orders.contracts import (
    FeePolicyKind,
    FeeRoundingMode,
    MatchDisposition,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PaperBrokerLimits,
    PaperDecision,
    PaperEventType,
    PaperExecutionConfig,
    PaperFeeRule,
    PaperFeeSchedule,
    SlippageConfig,
    SlippageMode,
    TimeInForce,
    canonical_json,
)
from tx_trade.orders.execution_policies import (
    ExecutionPolicyError,
    ExecutionPolicyErrorCode,
    assess_fee,
)
from tx_trade.orders.paper_broker import PaperBroker
from tx_trade.orders.paper_broker import PaperBrokerInputError
from tx_trade.replay.contracts import (
    ReplayFailureCode,
    ReplayMode,
    ReplayOptions,
    ReplaySessionDescriptor,
    ReplayState,
)
from tx_trade.replay.runtime import ReplayRuntime
from tx_trade.strategy import (
    PaperReplayCoordinator,
    StrategyContext,
    StrategyDecision,
    StrategyExecutionMode,
    StrategyRegistration,
)

RUN_ID = UUID("22222222-2222-2222-2222-222222222222")
LIMITS = PaperBrokerLimits(
    max_orders=10,
    max_open_orders=10,
    max_fills=20,
    max_events=50,
    max_market_data_records=20,
    max_instrument_versions=10,
)


def _broker(
    *,
    limits: PaperBrokerLimits = LIMITS,
    execution_config: PaperExecutionConfig | None = None,
) -> PaperBroker:
    events = make_offline_fixture_envelopes()
    return PaperBroker(
        paper_run_id=RUN_ID,
        expected_source_session_id=events[0].session_id,
        limits=limits,
        execution_config=execution_config,
    )


def _intent(client_order_id: str = "replay-order") -> OrderIntent:
    events = make_offline_fixture_envelopes()
    quote = events[3].payload
    assert isinstance(quote, Quote)
    return OrderIntent(
        strategy_id="integration-strategy",
        client_order_id=client_order_id,
        account_id="paper",
        instrument_id=quote.instrument_id,
        side=OrderSide.BUY,
        quantity=Decimal("2"),
        order_type=OrderType.MARKET,
        limit_price=None,
        time_in_force=TimeInForce.DAY,
        day_trade=False,
        created_at=quote.received_at,
    )


def _descriptor(events: tuple) -> ReplaySessionDescriptor:
    return ReplaySessionDescriptor(
        session_id=events[0].session_id,
        status="complete",
        schema_version=SCHEMA_VERSION,
        event_count=len(events),
        first_ingest_sequence=events[0].ingest_sequence,
        last_ingest_sequence=events[-1].ingest_sequence,
    )


def _runtime(
    events: tuple,
    sink: object,
    *,
    after_ingest_sequence: int | None = None,
) -> ReplayRuntime:
    return ReplayRuntime(
        source=InMemoryReplaySource(events),
        descriptor=_descriptor(events),
        sink=sink,  # type: ignore[arg-type]
        options=ReplayOptions(
            mode=ReplayMode.FASTEST,
            after_ingest_sequence=after_ingest_sequence,
        ),
    )


def _canonical_journal(broker: PaperBroker) -> bytes:
    events = broker.snapshot().events
    return ("[" + ",".join(canonical_json(event) for event in events) + "]").encode()


def _execution_config() -> PaperExecutionConfig:
    instrument = make_offline_fixture_envelopes()[2].payload
    assert isinstance(instrument, Instrument)
    return PaperExecutionConfig(
        slippage=SlippageConfig(
            mode=SlippageMode.BASIS_POINTS,
            value=Decimal("10"),
        ),
        fee_schedule=PaperFeeSchedule(
            kind=FeePolicyKind.PER_UNIT,
            rules=(
                PaperFeeRule(
                    instrument_id=instrument.instrument_id,
                    currency="TWD",
                    amount_per_unit=Decimal("0.6"),
                    quantum=Decimal("0.01"),
                    rounding_mode=FeeRoundingMode.ROUND_HALF_UP,
                    policy_id="phase2b3-integration",
                    policy_version="1",
                ),
            ),
        ),
    )


def test_complete_replay_is_byte_deterministic_with_direct_broker_sink() -> None:
    events = make_offline_fixture_envelopes()
    journals: list[bytes] = []

    for _ in range(2):
        broker = _broker()
        broker.submit(_intent())
        result = _runtime(events, broker).run()

        assert result.state is ReplayState.COMPLETED
        assert result.cursor == events[-1].ingest_sequence
        assert len(broker.snapshot().fills) == 1
        journals.append(_canonical_journal(broker))

    assert journals[0] == journals[1]


def test_nonzero_execution_replay_is_byte_deterministic_and_auditable() -> None:
    events = make_offline_fixture_envelopes()
    config = _execution_config()
    journals: list[bytes] = []
    snapshots: list[bytes] = []

    for _ in range(2):
        broker = _broker(execution_config=config)
        broker.submit(_intent("priced-replay"))
        result = _runtime(events, broker).run()
        snapshot = broker.snapshot()

        assert result.state is ReplayState.COMPLETED
        assert result.cursor == events[-1].ingest_sequence
        assert snapshot.execution_config_fingerprint == config.fingerprint
        assert len(snapshot.fills) == len(snapshot.positions) == 1
        fill = snapshot.fills[0]
        position = snapshot.positions[0]
        assert fill.reference_price == Decimal("20002.00")
        assert fill.execution_price == Decimal("20022.002")
        assert fill.slippage_amount == Decimal("20.002")
        assert fill.execution_config_fingerprint == config.fingerprint
        assert fill.fee == Decimal("1.20")
        assert fill.fee_currency == "TWD"
        assert position.net_quantity == Decimal("2")
        assert position.average_open_price == fill.execution_price
        assert position.cumulative_fees == fill.fee
        assert position.fee_currency == fill.fee_currency
        assert position.version == 1
        assert [event.event_type for event in snapshot.events[-3:]] == [
            PaperEventType.FILL_RECORDED,
            PaperEventType.ORDER_FILLED,
            PaperEventType.POSITION_CHANGED,
        ]
        assert snapshot.events[-1].payload == position
        journals.append(_canonical_journal(broker))
        snapshots.append(canonical_json(snapshot).encode())

    assert journals[0] == journals[1]
    assert snapshots[0] == snapshots[1]


def test_sequence_gaps_and_non_quotes_preserve_matching_causality() -> None:
    original = make_offline_fixture_envelopes()
    sequences = (10, 20, 40, 70, 90, 120)
    events = tuple(
        replace(
            envelope,
            ingest_sequence=sequence,
            dedupe_key=f"integration-gap:{sequence}",
        )
        for envelope, sequence in zip(original, sequences, strict=True)
    )
    broker = _broker()
    order = broker.submit(_intent("gapped"))

    class ObservingSink:
        def __init__(self) -> None:
            self.fill_counts: list[tuple[EventType, int]] = []

        def publish(self, envelope) -> None:
            broker.publish(envelope)
            self.fill_counts.append((envelope.event_type, len(broker.snapshot().fills)))

    sink = ObservingSink()
    result = _runtime(events, sink).run()

    assert result.state is ReplayState.COMPLETED
    assert result.cursor == 120
    assert sink.fill_counts[:3] == [
        (EventType.CONNECTION_STATUS, 0),
        (EventType.SERVER_TIME, 0),
        (EventType.INSTRUMENT, 0),
    ]
    assert sink.fill_counts[3:] == [
        (EventType.QUOTE, 1),
        (EventType.TICK, 1),
        (EventType.ADAPTER_DIAGNOSTIC, 1),
    ]
    assert broker.snapshot().last_committed_ingest_sequence == 120
    assert broker.get_order(order.paper_order_id).status is OrderStatus.FILLED


def test_exact_duplicate_direct_delivery_does_not_duplicate_fill() -> None:
    events = make_offline_fixture_envelopes()
    broker = _broker()
    broker.submit(_intent("duplicate"))
    result = _runtime(events, broker).run()
    assert result.state is ReplayState.COMPLETED

    before = broker.snapshot()
    duplicate = broker.process_market_data(events[3])

    assert duplicate.disposition is MatchDisposition.DUPLICATE
    assert duplicate.fills == ()
    assert broker.snapshot() == before


def test_post_commit_sink_failure_retries_quote_as_duplicate_without_double_booking() -> None:
    events = make_offline_fixture_envelopes()
    broker = _broker(execution_config=_execution_config())
    broker.submit(_intent("post-commit-retry"))
    failed_envelope = events[3]

    class FailAfterQuoteCommitSink:
        def publish(self, envelope) -> None:
            broker.process_market_data(envelope)
            if envelope is failed_envelope:
                raise RuntimeError("injected after broker commit")

    failed = _runtime(events, FailAfterQuoteCommitSink()).run()
    after_failure = broker.snapshot()

    assert failed.state is ReplayState.FAILED
    assert failed.failure_code is ReplayFailureCode.SINK_FAILED
    assert failed.cursor == events[2].ingest_sequence
    assert after_failure.last_committed_ingest_sequence == failed_envelope.ingest_sequence
    assert len(after_failure.fills) == len(after_failure.positions) == 1
    fill_count = len(after_failure.fills)
    event_count = len(after_failure.events)
    position = after_failure.positions[0]

    dispositions: list[MatchDisposition] = []

    class ObservingRetrySink:
        def publish(self, envelope) -> None:
            dispositions.append(broker.process_market_data(envelope).disposition)

    retried = _runtime(
        events,
        ObservingRetrySink(),
        after_ingest_sequence=failed.cursor,
    ).run()
    final = broker.snapshot()

    assert retried.state is ReplayState.COMPLETED
    assert retried.cursor == events[-1].ingest_sequence
    assert dispositions[0] is MatchDisposition.DUPLICATE
    assert len(final.fills) == fill_count
    assert len(final.events) == event_count
    assert len(final.positions) == 1
    assert final.positions[0] == position


def test_policy_failure_rolls_back_quote_and_keeps_replay_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = make_offline_fixture_envelopes()
    broker = _broker(execution_config=_execution_config())
    broker.submit(_intent("policy-rollback"))
    before_quote: list[bytes] = []

    def fail_fee(*args: object, **kwargs: object) -> object:
        raise ArithmeticError("injected fee arithmetic failure")

    monkeypatch.setattr("tx_trade.orders.paper_broker.assess_fee", fail_fee)

    class SnapshotBeforeDeliverySink:
        def publish(self, envelope) -> None:
            if envelope is events[3]:
                before_quote.append(canonical_json(broker.snapshot()).encode())
            broker.process_market_data(envelope)

    failed = _runtime(events, SnapshotBeforeDeliverySink()).run()

    assert failed.state is ReplayState.FAILED
    assert failed.failure_code is ReplayFailureCode.SINK_FAILED
    assert failed.cursor == events[2].ingest_sequence
    assert canonical_json(broker.snapshot()).encode() == before_quote[0]
    assert broker.snapshot().fills == broker.snapshot().positions == ()


def test_second_fifo_fee_arithmetic_failure_rolls_back_staged_fill_and_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = make_offline_fixture_envelopes()
    config = _execution_config()
    broker = _broker(execution_config=config)
    intents = (_intent("fifo-first"), _intent("fifo-second"))
    for intent in intents:
        broker.submit(intent)
    before_quote: list[bytes] = []
    fee_calls = 0

    def fail_second_fee(*args: object, **kwargs: object) -> object:
        nonlocal fee_calls
        fee_calls += 1
        if fee_calls == 2:
            raise ExecutionPolicyError(ExecutionPolicyErrorCode.ARITHMETIC_FAILURE)
        return assess_fee(*args, **kwargs)

    monkeypatch.setattr(
        "tx_trade.orders.paper_broker.assess_fee",
        fail_second_fee,
    )

    class SnapshotBeforeDeliverySink:
        def publish(self, envelope) -> None:
            if envelope is events[3]:
                before_quote.append(canonical_json(broker.snapshot()).encode())
            broker.process_market_data(envelope)

    failed = _runtime(events, SnapshotBeforeDeliverySink()).run()

    assert failed.state is ReplayState.FAILED
    assert failed.failure_code is ReplayFailureCode.SINK_FAILED
    assert failed.cursor == events[2].ingest_sequence
    assert fee_calls == 2
    assert canonical_json(broker.snapshot()).encode() == before_quote[0]
    assert broker.snapshot().fills == broker.snapshot().positions == ()

    monkeypatch.setattr("tx_trade.orders.paper_broker.assess_fee", assess_fee)
    retried = _runtime(
        events,
        broker,
        after_ingest_sequence=failed.cursor,
    ).run()

    clean = _broker(execution_config=config)
    for intent in intents:
        clean.submit(intent)
    clean_result = _runtime(events, clean).run()

    assert retried.state is ReplayState.COMPLETED
    assert retried.cursor == events[-1].ingest_sequence
    assert clean_result.state is ReplayState.COMPLETED
    assert len(broker.snapshot().fills) == 2
    assert broker.snapshot().positions[0].version == 2
    assert canonical_json(broker.snapshot()) == canonical_json(clean.snapshot())


def test_position_capacity_failure_rolls_back_whole_quote_batch_and_cursor() -> None:
    events = make_offline_fixture_envelopes()
    limits = replace(LIMITS, max_positions=1)
    broker = _broker(limits=limits)
    broker.submit(_intent("position-one"))
    second = replace(
        _intent("position-two"),
        strategy_id="second-integration-strategy",
    )
    broker.submit(second)
    before_quote: list[bytes] = []

    class SnapshotBeforeDeliverySink:
        def publish(self, envelope) -> None:
            if envelope is events[3]:
                before_quote.append(canonical_json(broker.snapshot()).encode())
            broker.process_market_data(envelope)

    failed = _runtime(events, SnapshotBeforeDeliverySink()).run()

    assert failed.state is ReplayState.FAILED
    assert failed.failure_code is ReplayFailureCode.SINK_FAILED
    assert failed.cursor == events[2].ingest_sequence
    assert canonical_json(broker.snapshot()).encode() == before_quote[0]
    assert broker.snapshot().fills == broker.snapshot().positions == ()


def test_pre_delivery_sink_failure_keeps_cursor_and_broker_state_for_retry() -> None:
    events = make_offline_fixture_envelopes()
    broker = _broker()
    broker.submit(_intent("retry"))
    failed_envelope = events[3]

    class FailBeforeQuoteSink:
        def publish(self, envelope) -> None:
            if envelope is failed_envelope:
                raise RuntimeError("injected before broker delivery")
            broker.publish(envelope)

    failed = _runtime(events, FailBeforeQuoteSink()).run()
    after_failure = broker.snapshot()

    assert failed.state is ReplayState.FAILED
    assert failed.failure_code is ReplayFailureCode.SINK_FAILED
    assert failed.cursor == events[2].ingest_sequence
    assert after_failure.last_committed_ingest_sequence == events[2].ingest_sequence
    assert after_failure.fills == ()

    retried = _runtime(
        events,
        broker,
        after_ingest_sequence=failed.cursor,
    ).run()

    assert retried.state is ReplayState.COMPLETED
    assert retried.cursor == events[-1].ingest_sequence
    assert len(broker.snapshot().fills) == 1


class _CountingOrderStrategy:
    def __init__(self) -> None:
        self.calls = 0

    def decide(
        self,
        envelope,
        context: StrategyContext,
    ) -> StrategyDecision:
        self.calls += 1
        intent = replace(
            _intent("coordinated-retry"),
            created_at=context.decision_at,
            source_session_id=envelope.session_id,
            source_ingest_sequence=envelope.ingest_sequence,
        )
        return StrategyDecision(commands=(intent,))


class _FailingStrategy:
    def decide(self, envelope, context: StrategyContext) -> StrategyDecision:
        raise RuntimeError("injected strategy failure")


def test_strategy_failure_keeps_replay_cursor_and_broker_unadvanced() -> None:
    events = make_offline_fixture_envelopes()
    broker = _broker()
    coordinator = PaperReplayCoordinator(
        broker=broker,
        registrations=(StrategyRegistration("integration-strategy", _FailingStrategy()),),
        mode=StrategyExecutionMode.PAPER,
        max_decision_records=len(events),
    )

    failed = _runtime(events, coordinator).run()

    assert failed.state is ReplayState.FAILED
    assert failed.failure_code is ReplayFailureCode.SINK_FAILED
    assert failed.cursor is None
    assert broker.snapshot().last_committed_ingest_sequence is None
    assert coordinator.decision_count == 0


def test_post_commit_wrapper_failure_retries_cached_decision_without_double_booking() -> None:
    events = make_offline_fixture_envelopes()
    broker = _broker()
    strategy = _CountingOrderStrategy()

    class FailOnceAfterCommitBroker:
        def __init__(self) -> None:
            self.failed = False

        def submit(self, intent):
            return broker.submit(intent)

        def cancel(self, request):
            return broker.cancel(request)

        def get_order(self, paper_order_id):
            return broker.get_order(paper_order_id)

        def list_orders(self):
            return broker.list_orders()

        def list_positions(self):
            return broker.list_positions()

        def get_position(self, strategy_id, account_id, instrument_id):
            return broker.get_position(strategy_id, account_id, instrument_id)

        def snapshot(self):
            return broker.snapshot()

        def process_decision_batch(self, envelope, decision):
            result = broker.process_decision_batch(envelope, decision)
            if not self.failed:
                self.failed = True
                raise RuntimeError("injected wrapper failure after commit")
            return result

    coordinator = PaperReplayCoordinator(
        broker=FailOnceAfterCommitBroker(),
        registrations=(StrategyRegistration("integration-strategy", strategy),),
        mode=StrategyExecutionMode.PAPER,
        max_decision_records=len(events),
    )
    failed = _runtime(events, coordinator).run()
    after_commit = canonical_json(broker.snapshot())

    assert failed.state is ReplayState.FAILED
    assert failed.cursor is None
    assert broker.snapshot().last_committed_ingest_sequence == events[0].ingest_sequence
    assert strategy.calls == 1

    retried = _runtime(events, coordinator, after_ingest_sequence=failed.cursor).run()

    assert retried.state is ReplayState.COMPLETED
    assert strategy.calls == len(events)
    assert coordinator.decision_count == len(events)
    assert canonical_json(broker.snapshot()) != after_commit
    assert (
        len(
            [
                order
                for order in broker.snapshot().orders
                if order.intent.client_order_id == "coordinated-retry"
            ]
        )
        == 1
    )


def test_decision_conflict_after_commit_is_atomic_and_journal_remains_authoritative() -> None:
    envelope = make_offline_fixture_envelopes()[0]
    broker = _broker()
    intent = replace(
        _intent("authoritative-journal"),
        created_at=envelope.received_at,
        source_session_id=envelope.session_id,
        source_ingest_sequence=envelope.ingest_sequence,
    )
    committed_decision = PaperDecision(
        source_session_id=envelope.session_id,
        source_ingest_sequence=envelope.ingest_sequence,
        commands=(intent,),
    )
    committed = broker.process_decision_batch(envelope, committed_decision)
    after_commit = canonical_json(broker.snapshot()).encode()

    duplicate = broker.process_decision_batch(envelope, committed_decision)

    assert committed.events
    assert duplicate.match_result.disposition is MatchDisposition.DUPLICATE
    assert duplicate.events == ()
    assert canonical_json(broker.snapshot()).encode() == after_commit
    assert broker.snapshot().events[-len(committed.events) :] == committed.events

    conflicting_decision = PaperDecision(
        source_session_id=envelope.session_id,
        source_ingest_sequence=envelope.ingest_sequence,
        commands=(),
    )
    with pytest.raises(PaperBrokerInputError, match="decision content conflict"):
        broker.process_decision_batch(envelope, conflicting_decision)

    assert canonical_json(broker.snapshot()).encode() == after_commit
