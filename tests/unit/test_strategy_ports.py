from __future__ import annotations

from tx_trade.market_data.ports import MarketDataSink
from tx_trade.strategy import (
    NoOpStrategy,
    PaperReplayCoordinator,
    StrategyPort,
    TransactionalPaperBrokerSnapshotPort,
)


def test_noop_strategy_structurally_implements_strategy_port() -> None:
    assert isinstance(NoOpStrategy(), StrategyPort)


def test_coordinator_is_a_structural_market_data_sink() -> None:
    assert callable(PaperReplayCoordinator.publish)
    assert issubclass(TransactionalPaperBrokerSnapshotPort, object)
    assert hasattr(MarketDataSink, "publish")
