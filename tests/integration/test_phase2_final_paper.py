from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_TIME,
    OFFLINE_FIXTURE_TRADING_DAY,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import SCHEMA_VERSION, SourceMode
from tx_trade.market_data.ports import RecordingSession
from tx_trade.orders import (
    OrderSide,
    OrderType,
    PaperBrokerLimits,
    TimeInForce,
)
from tx_trade.orders.paper_broker import PaperBroker
from tx_trade.storage import SQLiteMarketDataRepository
from tx_trade.strategy import (
    InstrumentTriggeredOrderStrategy,
    OrderTemplate,
    PaperReplayCoordinator,
    StrategyExecutionMode,
    StrategyRegistration,
)

SESSION_ID = UUID("11111111-1111-1111-1111-111111111111")
RUN_ID = UUID("22222222-2222-2222-2222-222222222222")
INSTRUMENT_ID = "TAIFEX:0:TX00"
_CHILD_OS_ENVIRONMENT_KEYS = (
    "COMSPEC",
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "WINDIR",
)


def _record_source(database_path: Path) -> tuple:
    repository = SQLiteMarketDataRepository(database_path)
    events = tuple(
        replace(envelope, session_id=SESSION_ID) for envelope in make_offline_fixture_envelopes()
    )
    repository.begin_session(
        RecordingSession(
            session_id=SESSION_ID,
            schema_version=SCHEMA_VERSION,
            source=events[0].source,
            source_mode=SourceMode.OFFLINE,
            started_at=OFFLINE_FIXTURE_TIME,
            trading_day=OFFLINE_FIXTURE_TRADING_DAY,
            config_fingerprint="phase2-final-paper",
        )
    )
    repository.append_batch(events)
    repository.end_session(
        SESSION_ID,
        OFFLINE_FIXTURE_TIME + timedelta(minutes=1),
        "complete",
    )
    repository.close()
    return events


def _paper_environment(database_path: Path) -> dict[str, str]:
    environment = {key: os.environ[key] for key in _CHILD_OS_ENVIRONMENT_KEYS if key in os.environ}
    environment.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUTF8": "1",
            "TX_TRADE_RUNTIME_PRESET": "research_paper",
            "TX_TRADE_RESEARCH_PAPER_DB_PATH": str(database_path),
            "TX_TRADE_RESEARCH_PAPER_SESSION_ID": str(SESSION_ID),
            "TX_TRADE_RESEARCH_PAPER_REPLAY_MODE": "fastest",
            "TX_TRADE_RESEARCH_PAPER_REPLAY_SPEED": "1",
            "TX_TRADE_RESEARCH_PAPER_RUN_ID": str(RUN_ID),
            "TX_TRADE_RESEARCH_PAPER_MAX_ORDERS": "10",
            "TX_TRADE_RESEARCH_PAPER_MAX_OPEN_ORDERS": "10",
            "TX_TRADE_RESEARCH_PAPER_MAX_FILLS": "10",
            "TX_TRADE_RESEARCH_PAPER_MAX_EVENTS": "30",
            "TX_TRADE_RESEARCH_PAPER_MAX_MARKET_DATA_RECORDS": "20",
            "TX_TRADE_RESEARCH_PAPER_MAX_INSTRUMENT_VERSIONS": "10",
            "TX_TRADE_RESEARCH_PAPER_MAX_POSITIONS": "10",
            "TX_TRADE_RESEARCH_PAPER_MAX_DECISION_RECORDS": "20",
            "TX_TRADE_RESEARCH_PAPER_SLIPPAGE_MODE": "none",
            "TX_TRADE_RESEARCH_PAPER_SLIPPAGE_VALUE": "0",
            "TX_TRADE_RESEARCH_PAPER_FEE_POLICY": "per_unit",
            "TX_TRADE_RESEARCH_PAPER_FEE_INSTRUMENT_ID": INSTRUMENT_ID,
            "TX_TRADE_RESEARCH_PAPER_FEE_CURRENCY": "TWD",
            "TX_TRADE_RESEARCH_PAPER_FEE_AMOUNT_PER_UNIT": "0.6",
            "TX_TRADE_RESEARCH_PAPER_FEE_QUANTUM": "0.01",
            "TX_TRADE_RESEARCH_PAPER_FEE_ROUNDING_MODE": "round_half_up",
            "TX_TRADE_RESEARCH_PAPER_FEE_POLICY_ID": "phase2-final",
            "TX_TRADE_RESEARCH_PAPER_FEE_POLICY_VERSION": "1",
            "TX_TRADE_RESEARCH_PAPER_STRATEGY_ID": "alpha",
            "TX_TRADE_RESEARCH_PAPER_CLIENT_ORDER_ID": "entry-1",
            "TX_TRADE_RESEARCH_PAPER_ACCOUNT_ID": "paper",
            "TX_TRADE_RESEARCH_PAPER_INSTRUMENT_ID": INSTRUMENT_ID,
            "TX_TRADE_RESEARCH_PAPER_ORDER_SIDE": "buy",
            "TX_TRADE_RESEARCH_PAPER_ORDER_QUANTITY": "2",
            "TX_TRADE_RESEARCH_PAPER_ORDER_TYPE": "market",
            "TX_TRADE_RESEARCH_PAPER_TIME_IN_FORCE": "day",
            "TX_TRADE_RESEARCH_PAPER_DAY_TRADE": "0",
        }
    )
    return environment


def _run_cli(database_path: Path) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [sys.executable, "-m", "tx_trade.app.research_paper"],
        cwd=Path(__file__).resolve().parents[2],
        env=_paper_environment(database_path),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=30,
    )


def _broker() -> PaperBroker:
    return PaperBroker(
        paper_run_id=RUN_ID,
        limits=PaperBrokerLimits(
            max_orders=10,
            max_open_orders=10,
            max_fills=10,
            max_events=30,
            max_market_data_records=20,
            max_instrument_versions=10,
            max_positions=10,
        ),
    )


def _coordinator(broker: PaperBroker, mode: StrategyExecutionMode) -> PaperReplayCoordinator:
    strategy = InstrumentTriggeredOrderStrategy(
        OrderTemplate(
            strategy_id="alpha",
            client_order_id="entry-1",
            account_id="paper",
            instrument_id=INSTRUMENT_ID,
            side=OrderSide.BUY,
            quantity=Decimal("2"),
            order_type=OrderType.MARKET,
            limit_price=None,
            time_in_force=TimeInForce.DAY,
            day_trade=False,
        )
    )
    return PaperReplayCoordinator(
        broker=broker,
        registrations=(StrategyRegistration("alpha", strategy),),
        mode=mode,
        max_decision_records=20,
    )


def test_observe_only_evaluates_without_any_paper_execution_effect() -> None:
    events = make_offline_fixture_envelopes()
    observed_broker = _broker()
    paper_broker = _broker()
    observed = _coordinator(observed_broker, StrategyExecutionMode.OBSERVE_ONLY)
    paper = _coordinator(paper_broker, StrategyExecutionMode.PAPER)

    for event in events:
        observed.publish(event)
        paper.publish(event)

    observed_snapshot = observed_broker.snapshot()
    paper_snapshot = paper_broker.snapshot()
    assert observed.decision_count == paper.decision_count == len(events)
    assert all(record.batch_result is None for record in observed.decision_records())
    assert observed_snapshot.snapshot_version == 0
    assert observed_snapshot.orders == observed_snapshot.fills == observed_snapshot.positions == ()
    assert observed_snapshot.events == ()
    assert len(paper_snapshot.orders) == len(paper_snapshot.fills) == 1
    assert len(paper_snapshot.positions) == 1


def test_real_module_cli_is_byte_deterministic_read_only_and_conserves_paper_state(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source.sqlite3"
    events = _record_source(source)
    source_before = hashlib.sha256(source.read_bytes()).digest()
    sidecars = (Path(f"{source}-wal"), Path(f"{source}-shm"))
    sidecars_before = {path.name: path.read_bytes() if path.exists() else None for path in sidecars}

    first = _run_cli(source)
    second = _run_cli(source)

    assert first.returncode == second.returncode == 0
    assert (
        first.stderr == second.stderr == (f"Research paper replay completed.{os.linesep}".encode())
    )
    assert first.stdout == second.stdout
    assert first.stdout.endswith(b"\n")
    assert hashlib.sha256(source.read_bytes()).digest() == source_before
    assert {
        path.name: path.read_bytes() if path.exists() else None for path in sidecars
    } == sidecars_before

    records = [json.loads(line) for line in first.stdout.splitlines()]
    market = [record for record in records if record["record_type"] == "market"]
    paper = [record for record in records if record["record_type"] == "paper"]
    summary = records[-1]
    assert len(market) == len(events)
    assert summary["record_type"] == "summary"
    assert summary["replay_session_id"] == str(SESSION_ID)
    assert summary["paper_run_id"] == str(RUN_ID)
    assert summary["terminal_cursor"] == events[-1].ingest_sequence
    assert summary["counts"] == {
        "decisions": len(events),
        "fills": 1,
        "market_records": len(events),
        "orders": 1,
        "paper_events": 4,
        "positions": 1,
    }

    event_types = [record["event"]["event_type"] for record in paper]
    assert event_types == [
        "order_accepted",
        "fill_recorded",
        "order_filled",
        "position_changed",
    ]
    accepted, fill, filled, position = (record["event"]["payload"] for record in paper)
    assert accepted["provenance"] == fill["provenance"] == "paper"
    assert filled["provenance"] == position["provenance"] == "paper"
    assert filled["paper_order_id"] == accepted["paper_order_id"] == fill["paper_order_id"]
    assert fill["paper_run_id"] == position["paper_run_id"] == str(RUN_ID)
    assert filled["filled_quantity"] == accepted["intent"]["quantity"]
    assert filled["remaining_quantity"] == "0"
    assert position["net_quantity"] == fill["quantity"]
    assert position["average_open_price"] == fill["execution_price"]
    assert fill["fee"] == position["cumulative_fees"] == "1.2"
    assert fill["fee_currency"] == position["fee_currency"] == "TWD"
    assert fill["execution_config_fingerprint"] == summary["execution_config_fingerprint"]


def test_real_module_cli_failure_is_empty_sanitized_and_nonzero(tmp_path: Path) -> None:
    missing = tmp_path / "sensitive-source-name.sqlite3"

    result = _run_cli(missing)

    assert result.returncode == 2
    assert result.stdout == b""
    assert result.stderr == f"Research paper replay failed safely.{os.linesep}".encode()
    assert str(missing).encode() not in result.stderr
    assert not missing.exists()
    assert not Path(f"{missing}-wal").exists()
    assert not Path(f"{missing}-shm").exists()


def test_real_module_cli_does_not_inherit_parent_research_configuration(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "hermetic-source.sqlite3"
    _record_source(source)
    monkeypatch.setenv(
        "TX_TRADE_RESEARCH_PAPER_REPLAY_AFTER_INGEST_SEQUENCE",
        "999999",
    )
    monkeypatch.setenv("PYTHONPROFILEIMPORTTIME", "1")
    monkeypatch.setenv("PYTHONWARNINGS", "error")
    monkeypatch.setenv("PYTHONPATH", str(tmp_path / "sitecustomize-poison"))
    monkeypatch.setenv("PYTHONHASHSEED", "parent-poison")

    result = _run_cli(source)

    assert result.returncode == 0
    assert result.stderr == f"Research paper replay completed.{os.linesep}".encode()
    summary = json.loads(result.stdout.splitlines()[-1])
    assert summary["record_type"] == "summary"
    assert summary["terminal_cursor"] == make_offline_fixture_envelopes()[-1].ingest_sequence
