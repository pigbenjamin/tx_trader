from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from uuid import UUID

import pytest

from tx_trade.market_data.fixtures import make_offline_fixture_envelopes
from tx_trade.orders import (
    OrderIntent,
    OrderSide,
    OrderType,
    PaperBrokerLimits,
    TimeInForce,
)
from tx_trade.orders.paper_broker import PaperBroker
from tx_trade.strategy import (
    InstrumentTriggeredOrderStrategy,
    OrderTemplate,
    PaperReplayCoordinator,
    StrategyCoordinatorError,
    StrategyDecision,
    StrategyExecutionMode,
    StrategyRegistration,
)

RUN_ID = UUID("dfe26f6d-c347-4976-82eb-b5d38a8d4111")


def _broker() -> PaperBroker:
    return PaperBroker(
        paper_run_id=RUN_ID,
        limits=PaperBrokerLimits(
            max_orders=20,
            max_open_orders=20,
            max_fills=20,
            max_events=100,
            max_market_data_records=20,
            max_instrument_versions=20,
            max_positions=20,
        ),
    )


class RecordingStrategy:
    def __init__(self, strategy_id: str, calls: list[str]) -> None:
        self.strategy_id = strategy_id
        self.calls = calls

    def decide(self, envelope: object, context: object) -> StrategyDecision:
        self.calls.append(self.strategy_id)
        return StrategyDecision(commands=())


class FailingStrategy:
    def decide(self, envelope: object, context: object) -> StrategyDecision:
        raise StrategyCoordinatorError("sensitive details")


class OneOrderStrategy:
    def __init__(self, strategy_id: str, client_order_id: str) -> None:
        self.strategy_id = strategy_id
        self.client_order_id = client_order_id
        self.calls = 0

    def decide(self, envelope: object, context: object) -> StrategyDecision:
        self.calls += 1
        return StrategyDecision(
            commands=(
                OrderIntent(
                    strategy_id=self.strategy_id,
                    client_order_id=self.client_order_id,
                    account_id="paper-account",
                    instrument_id="TAIFEX:0:TX00",
                    side=OrderSide.BUY,
                    quantity=Decimal("1"),
                    order_type=OrderType.MARKET,
                    limit_price=None,
                    time_in_force=TimeInForce.DAY,
                    day_trade=False,
                    created_at=context.decision_at,  # type: ignore[attr-defined]
                    source_session_id=context.source_session_id,  # type: ignore[attr-defined]
                    source_ingest_sequence=context.source_ingest_sequence,  # type: ignore[attr-defined]
                ),
            )
        )


def test_coordinator_sorts_strategies_and_preserves_command_fifo() -> None:
    calls: list[str] = []
    broker = _broker()
    coordinator = PaperReplayCoordinator(
        broker=broker,
        registrations=(
            StrategyRegistration("zeta", RecordingStrategy("zeta", calls)),
            StrategyRegistration("alpha", RecordingStrategy("alpha", calls)),
        ),
        mode=StrategyExecutionMode.PAPER,
        max_decision_records=2,
    )

    coordinator.publish(make_offline_fixture_envelopes()[0])

    assert calls == ["alpha", "zeta"]
    assert coordinator.decision_count == 1
    assert coordinator.decision_records()[0].batch_result is not None


def test_retry_uses_cached_decision_and_reaches_broker_duplicate_fence() -> None:
    envelope = make_offline_fixture_envelopes()[0]
    strategy = OneOrderStrategy("alpha", "entry")
    broker = _broker()
    coordinator = PaperReplayCoordinator(
        broker=broker,
        registrations=(StrategyRegistration("alpha", strategy),),
        mode=StrategyExecutionMode.PAPER,
        max_decision_records=1,
    )

    coordinator.publish(envelope)
    version = broker.snapshot().snapshot_version
    coordinator.publish(envelope)

    assert strategy.calls == 1
    assert broker.snapshot().snapshot_version == version
    assert coordinator.decision_count == 1


def test_strategy_failure_does_not_call_broker_or_cache_decision() -> None:
    envelope = make_offline_fixture_envelopes()[0]
    broker = _broker()
    coordinator = PaperReplayCoordinator(
        broker=broker,
        registrations=(StrategyRegistration("alpha", FailingStrategy()),),
        mode=StrategyExecutionMode.PAPER,
        max_decision_records=1,
    )

    with pytest.raises(StrategyCoordinatorError, match="strategy evaluation failed") as raised:
        coordinator.publish(envelope)

    assert raised.value.__cause__ is None
    assert broker.snapshot().snapshot_version == 0
    assert coordinator.decision_count == 0


def test_observe_only_evaluates_and_caches_without_mutating_broker() -> None:
    envelope = make_offline_fixture_envelopes()[0]
    strategy = OneOrderStrategy("alpha", "entry")
    broker = _broker()
    coordinator = PaperReplayCoordinator(
        broker=broker,
        registrations=(StrategyRegistration("alpha", strategy),),
        mode=StrategyExecutionMode.OBSERVE_ONLY,
        max_decision_records=1,
    )

    coordinator.publish(envelope)
    coordinator.publish(envelope)

    assert strategy.calls == 1
    assert broker.snapshot().snapshot_version == 0
    assert coordinator.decision_records()[0].batch_result is None


def test_capacity_preflight_and_digest_conflict_are_fail_closed() -> None:
    envelopes = make_offline_fixture_envelopes()
    strategy = OneOrderStrategy("alpha", "entry")
    coordinator = PaperReplayCoordinator(
        broker=_broker(),
        registrations=(StrategyRegistration("alpha", strategy),),
        mode=StrategyExecutionMode.OBSERVE_ONLY,
        max_decision_records=1,
    )
    coordinator.publish(envelopes[0])

    with pytest.raises(StrategyCoordinatorError, match="capacity"):
        coordinator.publish(envelopes[1])
    assert strategy.calls == 1

    conflicting = replace(envelopes[0], dedupe_key=envelopes[0].dedupe_key + "-changed")
    with pytest.raises(StrategyCoordinatorError, match="retry conflict"):
        coordinator.publish(conflicting)
    assert strategy.calls == 1


def test_instrument_triggered_order_is_not_eligible_until_next_envelope() -> None:
    envelopes = make_offline_fixture_envelopes()
    broker = _broker()
    strategy = InstrumentTriggeredOrderStrategy(
        OrderTemplate(
            strategy_id="alpha",
            client_order_id="entry",
            account_id="paper-account",
            instrument_id="TAIFEX:0:TX00",
            side=OrderSide.BUY,
            quantity=Decimal("1"),
            order_type=OrderType.MARKET,
            limit_price=None,
            time_in_force=TimeInForce.DAY,
            day_trade=False,
        )
    )
    coordinator = PaperReplayCoordinator(
        broker=broker,
        registrations=(StrategyRegistration("alpha", strategy),),
        mode=StrategyExecutionMode.PAPER,
        max_decision_records=4,
    )

    for envelope in envelopes[:3]:
        coordinator.publish(envelope)
    before_quote = broker.snapshot()
    assert len(before_quote.orders) == 1
    assert before_quote.fills == ()

    coordinator.publish(envelopes[3])
    assert len(broker.snapshot().fills) == 1
