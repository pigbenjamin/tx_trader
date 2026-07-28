from __future__ import annotations

import hashlib
import io
import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest

from tx_trade.app.research_paper import (
    ResearchPaperApplicationError,
    main,
    run_research_paper_app,
)
from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_TIME,
    OFFLINE_FIXTURE_TRADING_DAY,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import SCHEMA_VERSION, SourceMode
from tx_trade.market_data.ports import RecordingSession
from tx_trade.replay import ReplayState
from tx_trade.storage import SQLiteMarketDataRepository

SESSION_ID = UUID("11111111-1111-1111-1111-111111111111")
RUN_ID = UUID("22222222-2222-2222-2222-222222222222")


def _record(database_path: Path) -> tuple:
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
            config_fingerprint="research-paper-app-test",
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


def _environment(database_path: Path) -> dict[str, str]:
    return {
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
        "TX_TRADE_RESEARCH_PAPER_FEE_POLICY": "zero",
        "TX_TRADE_RESEARCH_PAPER_STRATEGY_ID": "alpha",
        "TX_TRADE_RESEARCH_PAPER_CLIENT_ORDER_ID": "entry-1",
        "TX_TRADE_RESEARCH_PAPER_ACCOUNT_ID": "paper",
        "TX_TRADE_RESEARCH_PAPER_INSTRUMENT_ID": "TAIFEX:0:TX00",
        "TX_TRADE_RESEARCH_PAPER_ORDER_SIDE": "buy",
        "TX_TRADE_RESEARCH_PAPER_ORDER_QUANTITY": "2",
        "TX_TRADE_RESEARCH_PAPER_ORDER_TYPE": "market",
        "TX_TRADE_RESEARCH_PAPER_TIME_IN_FORCE": "day",
        "TX_TRADE_RESEARCH_PAPER_DAY_TRADE": "0",
    }


def test_complete_research_run_is_correlated_and_byte_deterministic(tmp_path) -> None:
    database_path = tmp_path / "research.db"
    events = _record(database_path)
    environment = _environment(database_path)

    first = run_research_paper_app(environment)
    second = run_research_paper_app(environment)

    assert first.output == second.output
    assert first.replay_snapshot.state is ReplayState.COMPLETED
    assert first.market_envelopes == events
    assert len(first.decision_records) == len(events)
    assert first.broker_snapshot.last_committed_ingest_sequence == events[-1].ingest_sequence
    records = [json.loads(line) for line in first.output.splitlines()]
    assert [record["record_type"] for record in records[: len(events)]] == ["market"] * len(events)
    assert records[-1]["record_type"] == "summary"
    assert records[-1]["counts"]["fills"] == 1


def test_research_run_is_read_only_and_creates_no_sidecars(tmp_path) -> None:
    database_path = tmp_path / "readonly.db"
    _record(database_path)
    before = hashlib.sha256(database_path.read_bytes()).digest()

    run_research_paper_app(_environment(database_path))

    assert hashlib.sha256(database_path.read_bytes()).digest() == before
    assert not Path(f"{database_path}-wal").exists()
    assert not Path(f"{database_path}-shm").exists()


def test_main_writes_buffer_only_after_success_and_sanitizes_failure(
    tmp_path,
    capsys,
) -> None:
    database_path = tmp_path / "cli.db"
    _record(database_path)
    output = io.BytesIO()

    assert main([], environment=_environment(database_path), output=output) == 0
    captured = capsys.readouterr()
    assert output.getvalue().endswith(b"\n")
    assert captured.out == ""
    assert captured.err == "Research paper replay completed.\n"

    missing = tmp_path / "secret-missing.db"
    failed_output = io.BytesIO()
    assert main([], environment=_environment(missing), output=failed_output) == 2
    captured = capsys.readouterr()
    assert failed_output.getvalue() == b""
    assert captured.out == ""
    assert captured.err == "Research paper replay failed safely.\n"
    assert str(missing) not in captured.err


def test_main_rejects_arguments_without_opening_or_output(tmp_path, capsys) -> None:
    output = io.BytesIO()

    assert main(["unexpected"], environment={}, output=output) == 2

    captured = capsys.readouterr()
    assert output.getvalue() == b""
    assert captured.out == ""
    assert captured.err == "Research paper replay failed safely.\n"


def test_composition_passes_one_canonical_path_and_closes_repository(tmp_path) -> None:
    database_path = tmp_path / "canonical.db"
    _record(database_path)
    alias = tmp_path / "child" / ".." / database_path.name
    calls = []

    def factory(path, *, recover_incomplete_sessions, read_only):
        calls.append(path)
        return SQLiteMarketDataRepository(
            path,
            recover_incomplete_sessions=recover_incomplete_sessions,
            read_only=read_only,
        )

    result = run_research_paper_app(_environment(alias), repository_factory=factory)

    assert result.replay_snapshot.state is ReplayState.COMPLETED
    assert calls == [database_path.resolve(strict=True)]


def test_sidecar_created_during_open_is_rejected_and_repository_closed(
    tmp_path,
) -> None:
    database_path = tmp_path / "toctou.db"
    _record(database_path)
    opened = []
    sidecar = Path(f"{database_path}-wal")

    class Repository:
        closed = False

        def close(self) -> None:
            self.closed = True

    def factory(path, *, recover_incomplete_sessions, read_only):
        repository = Repository()
        opened.append(repository)
        sidecar.touch()
        return repository

    try:
        with pytest.raises(ResearchPaperApplicationError, match="failed safely"):
            run_research_paper_app(
                _environment(database_path),
                repository_factory=factory,
            )
        assert opened[0].closed
    finally:
        sidecar.unlink(missing_ok=True)


def test_descriptor_record_limits_fail_before_replay_and_without_output(
    tmp_path,
) -> None:
    database_path = tmp_path / "bounded.db"
    _record(database_path)
    environment = _environment(database_path)
    environment["TX_TRADE_RESEARCH_PAPER_MAX_MARKET_DATA_RECORDS"] = "5"
    output = io.BytesIO()

    assert main([], environment=environment, output=output) == 2
    assert output.getvalue() == b""
