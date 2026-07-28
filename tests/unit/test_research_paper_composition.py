from __future__ import annotations

import pytest

from tx_trade.app.research_paper import (
    ResearchPaperApplicationError,
    ResearchPaperResult,
    _BufferedCoordinatorSink,
    run_research_paper,
)
from tx_trade.market_data.fixtures import make_offline_fixture_envelopes
from tx_trade.market_data.models import serialize_envelope


def test_terminal_result_is_frozen_and_slotted() -> None:
    assert "__dict__" not in ResearchPaperResult.__slots__
    assert ResearchPaperResult.__dataclass_params__.frozen


def test_library_api_rejects_non_settings_before_repository_use() -> None:
    called = False

    def repository_factory(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError

    with pytest.raises(TypeError, match="ResearchPaperSettings"):
        run_research_paper(object(), repository_factory=repository_factory)  # type: ignore[arg-type]

    assert not called


def test_missing_database_is_fixed_failure_and_not_created(tmp_path) -> None:
    from tx_trade.app.research_paper_config import parse_research_paper_settings

    missing = tmp_path / "sensitive" / "recording.db"
    values = {
        "TX_TRADE_RUNTIME_PRESET": "research_paper",
        "TX_TRADE_RESEARCH_PAPER_DB_PATH": str(missing),
        "TX_TRADE_RESEARCH_PAPER_SESSION_ID": "11111111-1111-1111-1111-111111111111",
        "TX_TRADE_RESEARCH_PAPER_REPLAY_MODE": "fastest",
        "TX_TRADE_RESEARCH_PAPER_REPLAY_SPEED": "1",
        "TX_TRADE_RESEARCH_PAPER_RUN_ID": "22222222-2222-2222-2222-222222222222",
        "TX_TRADE_RESEARCH_PAPER_MAX_ORDERS": "1",
        "TX_TRADE_RESEARCH_PAPER_MAX_OPEN_ORDERS": "1",
        "TX_TRADE_RESEARCH_PAPER_MAX_FILLS": "1",
        "TX_TRADE_RESEARCH_PAPER_MAX_EVENTS": "1",
        "TX_TRADE_RESEARCH_PAPER_MAX_MARKET_DATA_RECORDS": "1",
        "TX_TRADE_RESEARCH_PAPER_MAX_INSTRUMENT_VERSIONS": "1",
        "TX_TRADE_RESEARCH_PAPER_MAX_POSITIONS": "1",
        "TX_TRADE_RESEARCH_PAPER_MAX_DECISION_RECORDS": "1",
        "TX_TRADE_RESEARCH_PAPER_SLIPPAGE_MODE": "none",
        "TX_TRADE_RESEARCH_PAPER_SLIPPAGE_VALUE": "0",
        "TX_TRADE_RESEARCH_PAPER_FEE_POLICY": "zero",
        "TX_TRADE_RESEARCH_PAPER_STRATEGY_ID": "alpha",
        "TX_TRADE_RESEARCH_PAPER_CLIENT_ORDER_ID": "entry",
        "TX_TRADE_RESEARCH_PAPER_ACCOUNT_ID": "paper",
        "TX_TRADE_RESEARCH_PAPER_INSTRUMENT_ID": "TX",
        "TX_TRADE_RESEARCH_PAPER_ORDER_SIDE": "buy",
        "TX_TRADE_RESEARCH_PAPER_ORDER_QUANTITY": "1",
        "TX_TRADE_RESEARCH_PAPER_ORDER_TYPE": "market",
        "TX_TRADE_RESEARCH_PAPER_TIME_IN_FORCE": "day",
        "TX_TRADE_RESEARCH_PAPER_DAY_TRADE": "0",
    }

    with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
        run_research_paper(parse_research_paper_settings(values))

    assert not missing.exists()


def test_sink_checks_serialized_byte_budget_before_coordinator_commit() -> None:
    envelope = make_offline_fixture_envelopes()[0]

    class Coordinator:
        called = False

        def publish(self, envelope) -> None:
            self.called = True

    coordinator = Coordinator()
    budget = len(serialize_envelope(envelope).encode("utf-8"))
    sink = _BufferedCoordinatorSink(  # type: ignore[arg-type]
        coordinator,
        max_market_records=1,
        max_output_bytes=budget,
    )

    with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
        sink.publish(envelope)

    assert not coordinator.called
    assert sink.snapshot() == ()


def test_sink_enforces_cumulative_serialized_budget_without_second_commit() -> None:
    envelope = make_offline_fixture_envelopes()[0]

    class Coordinator:
        calls = 0

        def publish(self, envelope) -> None:
            self.calls += 1

    coordinator = Coordinator()
    one_record = len(serialize_envelope(envelope).encode("utf-8")) + 1
    sink = _BufferedCoordinatorSink(  # type: ignore[arg-type]
        coordinator,
        max_market_records=2,
        max_output_bytes=one_record,
    )

    sink.publish(envelope)
    with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
        sink.publish(envelope)

    assert coordinator.calls == 1
    assert sink.snapshot() == (envelope,)
