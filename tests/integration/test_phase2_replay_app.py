from __future__ import annotations

import json
import hashlib
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from tx_trade.app.phase2 import (
    JsonLinesSink,
    Phase2ApplicationError,
    main,
    run_phase2_replay,
)
from tx_trade.app.phase2_config import Phase2ReplaySettings
from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_TIME,
    OFFLINE_FIXTURE_TRADING_DAY,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import SCHEMA_VERSION, SourceMode, serialize_envelope
from tx_trade.market_data.ports import RecordingSession
from tx_trade.replay import ReplayMode, ReplayOptions, ReplayState
from tx_trade.storage import SQLiteMarketDataRepository


class _CollectingSink:
    def __init__(self) -> None:
        self.envelopes = []

    def publish(self, envelope) -> None:
        self.envelopes.append(envelope)


def _record_complete_session(database_path) -> tuple[UUID, tuple]:
    repository = SQLiteMarketDataRepository(database_path)
    session_id = uuid4()
    events = tuple(
        replace(envelope, session_id=session_id) for envelope in make_offline_fixture_envelopes()
    )
    repository.begin_session(
        RecordingSession(
            session_id=session_id,
            schema_version=SCHEMA_VERSION,
            source=events[0].source,
            source_mode=SourceMode.OFFLINE,
            started_at=OFFLINE_FIXTURE_TIME,
            trading_day=OFFLINE_FIXTURE_TRADING_DAY,
            config_fingerprint="phase2-app-test",
        )
    )
    repository.append_batch(events)
    repository.end_session(
        session_id,
        OFFLINE_FIXTURE_TIME + timedelta(minutes=1),
        "complete",
    )
    repository.close()
    return session_id, events


def _settings(database_path, session_id) -> Phase2ReplaySettings:
    return Phase2ReplaySettings(
        runtime_preset="phase2_replay",
        execution_mode="disabled",
        database_path=database_path,
        session_id=session_id,
        options=ReplayOptions(mode=ReplayMode.FASTEST),
    )


def test_same_complete_session_replays_deterministically_via_composition(tmp_path) -> None:
    database_path = tmp_path / "complete.db"
    session_id, expected = _record_complete_session(database_path)
    runs = []

    for _ in range(2):
        sink = _CollectingSink()
        snapshot = run_phase2_replay(_settings(database_path, session_id), sink)

        assert snapshot.state is ReplayState.COMPLETED
        assert snapshot.emitted_count == len(expected)
        runs.append(tuple(sink.envelopes))

    assert runs == [expected, expected]


def test_replay_keeps_source_database_bytes_unchanged_and_creates_no_sidecars(
    tmp_path,
) -> None:
    database_path = tmp_path / "readonly-source.db"
    session_id, expected = _record_complete_session(database_path)
    before = hashlib.sha256(database_path.read_bytes()).digest()
    sidecars = [Path(f"{database_path}-wal"), Path(f"{database_path}-shm")]
    sidecars_before = {path.name: path.read_bytes() if path.exists() else None for path in sidecars}
    database_path.chmod(0o444)
    sink = _CollectingSink()

    try:
        snapshot = run_phase2_replay(_settings(database_path, session_id), sink)
    finally:
        database_path.chmod(0o666)

    after = hashlib.sha256(database_path.read_bytes()).digest()
    assert snapshot.state is ReplayState.COMPLETED
    assert tuple(sink.envelopes) == expected
    assert after == before
    assert {
        path.name: path.read_bytes() if path.exists() else None for path in sidecars
    } == sidecars_before


def test_existing_empty_database_fails_without_modifying_it(tmp_path) -> None:
    database_path = tmp_path / "empty-existing.db"
    database_path.touch()
    before = database_path.read_bytes()

    with pytest.raises(Phase2ApplicationError, match="failed safely"):
        run_phase2_replay(_settings(database_path, uuid4()), _CollectingSink())

    assert database_path.read_bytes() == before
    assert not Path(f"{database_path}-wal").exists()
    assert not Path(f"{database_path}-shm").exists()


def test_active_wal_database_is_rejected_before_replay(tmp_path) -> None:
    database_path = tmp_path / "active-writer.db"
    repository = SQLiteMarketDataRepository(database_path)
    session_id = uuid4()
    events = tuple(
        replace(envelope, session_id=session_id) for envelope in make_offline_fixture_envelopes()
    )
    repository.begin_session(
        RecordingSession(
            session_id=session_id,
            schema_version=SCHEMA_VERSION,
            source=events[0].source,
            source_mode=SourceMode.OFFLINE,
            started_at=OFFLINE_FIXTURE_TIME,
            trading_day=OFFLINE_FIXTURE_TRADING_DAY,
            config_fingerprint="active-writer",
        )
    )
    repository.append_batch(events)
    assert Path(f"{database_path}-wal").exists()

    try:
        with pytest.raises(Phase2ApplicationError, match="failed safely"):
            run_phase2_replay(_settings(database_path, session_id), _CollectingSink())
    finally:
        repository.close()


def test_main_outputs_only_canonical_json_lines_and_fixed_summary(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_path = tmp_path / "cli.db"
    session_id, expected = _record_complete_session(database_path)
    monkeypatch.setenv("TX_TRADE_REPLAY_DB_PATH", str(database_path))
    monkeypatch.setenv("TX_TRADE_REPLAY_SESSION_ID", str(session_id))
    monkeypatch.setenv("TX_TRADE_ACCOUNT", "account-secret-canary")
    monkeypatch.setenv("TX_TRADE_PASSWORD", "password-secret-canary")
    monkeypatch.setenv("TX_TRADE_SKCOM_DLL_PATH", "dll-secret-canary")

    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out.splitlines() == [serialize_envelope(event) for event in expected]
    assert all(isinstance(json.loads(line), dict) for line in captured.out.splitlines())
    assert captured.err == "Phase 2 replay completed.\n"
    rendered = captured.out + captured.err
    assert "account-secret-canary" not in rendered
    assert "password-secret-canary" not in rendered
    assert "dll-secret-canary" not in rendered
    assert str(database_path) not in captured.err
    assert str(session_id) not in captured.err


def test_main_missing_database_is_nonzero_sanitized_and_does_not_create_it(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_path = tmp_path / "missing" / "secret-name.db"
    session_id = uuid4()
    monkeypatch.setenv("TX_TRADE_REPLAY_DB_PATH", str(database_path))
    monkeypatch.setenv("TX_TRADE_REPLAY_SESSION_ID", str(session_id))

    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err == "Phase 2 replay failed safely.\n"
    assert not database_path.exists()
    assert str(database_path) not in captured.err
    assert str(session_id) not in captured.err


def test_main_incomplete_session_is_nonzero_and_sanitized(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    database_path = tmp_path / "incomplete.db"
    repository = SQLiteMarketDataRepository(database_path)
    session_id = uuid4()
    events = tuple(
        replace(envelope, session_id=session_id) for envelope in make_offline_fixture_envelopes()
    )
    repository.begin_session(
        RecordingSession(
            session_id=session_id,
            schema_version=SCHEMA_VERSION,
            source=events[0].source,
            source_mode=SourceMode.OFFLINE,
            started_at=OFFLINE_FIXTURE_TIME,
            trading_day=OFFLINE_FIXTURE_TRADING_DAY,
            config_fingerprint="phase2-app-test",
        )
    )
    repository.append_batch(events)
    repository.close()
    monkeypatch.setenv("TX_TRADE_REPLAY_DB_PATH", str(database_path))
    monkeypatch.setenv("TX_TRADE_REPLAY_SESSION_ID", str(session_id))

    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err == "Phase 2 replay failed safely.\n"


@pytest.mark.parametrize("failure", ["missing_session", "corrupt"])
def test_main_rejects_invalid_session_before_output_and_closes_database(
    tmp_path,
    monkeypatch,
    capsys,
    failure,
) -> None:
    database_path = tmp_path / f"{failure}.db"
    if failure == "corrupt":
        session_id, events = _record_complete_session(database_path)
        repository = SQLiteMarketDataRepository(database_path)
        repository.connection.execute(
            """UPDATE event_log SET payload_json=?
            WHERE session_id=? AND ingest_sequence=?""",
            ("storage-secret-canary", str(session_id), events[0].ingest_sequence),
        )
        repository.close()
    else:
        repository = SQLiteMarketDataRepository(database_path)
        repository.close()
        session_id = uuid4()
    monkeypatch.setenv("TX_TRADE_REPLAY_DB_PATH", str(database_path))
    monkeypatch.setenv("TX_TRADE_REPLAY_SESSION_ID", str(session_id))

    exit_code = main([])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err == "Phase 2 replay failed safely.\n"
    assert "storage-secret-canary" not in captured.err
    database_path.rename(tmp_path / f"{failure}-closed.db")


def test_main_rejects_arguments_without_reading_environment(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "tx_trade.app.phase2.os",
        SimpleNamespace(environment="must-not-be-read"),
    )

    exit_code = main(["--unsafe"])

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert captured.err == "Phase 2 replay failed safely.\n"


def test_json_lines_sink_flushes_each_envelope(tmp_path) -> None:
    database_path = tmp_path / "flush.db"
    session_id, expected = _record_complete_session(database_path)

    class Stream:
        def __init__(self):
            self.parts = []
            self.flushes = 0

        def write(self, text):
            self.parts.append(text)

        def flush(self):
            self.flushes += 1

    stream = Stream()
    snapshot = run_phase2_replay(
        _settings(database_path, session_id),
        JsonLinesSink(stream),  # type: ignore[arg-type]
    )

    assert snapshot.state is ReplayState.COMPLETED
    assert stream.flushes == len(expected)
    assert "".join(stream.parts).splitlines() == [serialize_envelope(event) for event in expected]
