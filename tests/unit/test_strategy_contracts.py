from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from tx_trade.market_data.fixtures import OFFLINE_FIXTURE_TIME, make_offline_fixture_envelopes
from tx_trade.strategy import (
    NoOpStrategy,
    StrategyContext,
    StrategyDecision,
    StrategyExecutionMode,
    StrategyRegistration,
)


def test_strategy_contracts_are_strict_immutable_values() -> None:
    envelope = make_offline_fixture_envelopes()[0]
    context = StrategyContext(
        strategy_id="alpha",
        source_session_id=envelope.session_id,
        source_ingest_sequence=envelope.ingest_sequence,
        decision_at=OFFLINE_FIXTURE_TIME,
        orders=(),
        positions=(),
        broker_snapshot_version=0,
    )
    decision = StrategyDecision(commands=())
    registration = StrategyRegistration(strategy_id="alpha", strategy=NoOpStrategy())

    assert StrategyExecutionMode.PAPER.value == "paper"
    assert context.orders == ()
    assert decision.commands == ()
    assert registration.strategy_id == "alpha"
    with pytest.raises(FrozenInstanceError):
        context.strategy_id = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("factory", "error"),
    [
        (lambda: StrategyDecision(commands=[]), TypeError),
        (lambda: StrategyRegistration(strategy_id="", strategy=NoOpStrategy()), ValueError),
        (lambda: StrategyRegistration(strategy_id="alpha", strategy=object()), TypeError),
    ],
)
def test_strategy_contracts_reject_invalid_shapes(factory: object, error: type[Exception]) -> None:
    with pytest.raises(error):
        factory()  # type: ignore[operator]
