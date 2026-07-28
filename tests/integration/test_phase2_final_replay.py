from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from threading import Event
from uuid import UUID, uuid4

from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_TIME,
    OFFLINE_FIXTURE_TRADING_DAY,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import (
    SCHEMA_VERSION,
    MarketDataEnvelope,
    SourceMode,
    serialize_envelope,
)
from tx_trade.market_data.ports import RecordingSession
from tx_trade.replay import ReplayMode, ReplayOptions, ReplayRuntime, ReplayState
from tx_trade.replay.sqlite_source import prepare_sqlite_replay_source
from tx_trade.storage import SQLiteMarketDataRepository

_PROJECT_ROOT = Path(__file__).parents[2]
_CHILD_OS_ENVIRONMENT_KEYS = {
    "COMSPEC",
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "WINDIR",
}


def _record_complete_session(
    database_path: Path,
) -> tuple[UUID, tuple[MarketDataEnvelope, ...]]:
    repository = SQLiteMarketDataRepository(database_path)
    session_id = uuid4()
    events = tuple(
        replace(event, session_id=session_id) for event in make_offline_fixture_envelopes()
    )
    repository.begin_session(
        RecordingSession(
            session_id=session_id,
            schema_version=SCHEMA_VERSION,
            source=events[0].source,
            source_mode=SourceMode.OFFLINE,
            started_at=OFFLINE_FIXTURE_TIME,
            trading_day=OFFLINE_FIXTURE_TRADING_DAY,
            config_fingerprint="phase2-final-acceptance",
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


def _source_artifacts(database_path: Path) -> dict[str, bytes | None]:
    paths = (
        database_path,
        Path(f"{database_path}-wal"),
        Path(f"{database_path}-shm"),
    )
    return {
        path.name: hashlib.sha256(path.read_bytes()).digest() if path.exists() else None
        for path in paths
    }


def _runtime(
    database_path: Path,
    session_id: UUID,
    sink: object,
) -> tuple[SQLiteMarketDataRepository, ReplayRuntime]:
    repository = SQLiteMarketDataRepository(
        database_path,
        recover_incomplete_sessions=False,
        read_only=True,
    )
    source, descriptor = prepare_sqlite_replay_source(repository, session_id)
    runtime = ReplayRuntime(
        source=source,
        descriptor=descriptor,
        sink=sink,  # type: ignore[arg-type]
        options=ReplayOptions(mode=ReplayMode.FASTEST),
    )
    return repository, runtime


def _child_environment() -> dict[str, str]:
    return {
        key: value for key, value in os.environ.items() if key.upper() in _CHILD_OS_ENVIRONMENT_KEYS
    }


def test_module_process_is_byte_deterministic_and_read_only(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "phase2-final.db"
    session_id, expected = _record_complete_session(database_path)
    before = _source_artifacts(database_path)
    monkeypatch.setenv("TX_TRADE_REPLAY_AFTER_INGEST_SEQUENCE", "999999")
    monkeypatch.setenv("TX_TRADE_REPLAY_MODE", "paced")
    monkeypatch.setenv("TX_TRADE_REPLAY_SPEED", "0.000001")
    monkeypatch.setenv("TX_TRADE_UNRELATED_PARENT_POISON", "must-not-be-inherited")
    environment = _child_environment()
    assert "TX_TRADE_REPLAY_AFTER_INGEST_SEQUENCE" not in environment
    assert not any(key.startswith("TX_TRADE_") for key in environment)
    environment.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUTF8": "1",
            "TX_TRADE_RUNTIME_PRESET": "phase2_replay",
            "TX_TRADE_REPLAY_DB_PATH": str(database_path),
            "TX_TRADE_REPLAY_SESSION_ID": str(session_id),
            "TX_TRADE_ACCOUNT": "live-account-must-not-be-used",
            "TX_TRADE_PASSWORD": "live-password-must-not-be-used",
            "TX_TRADE_ORDER_ACCOUNT": "order-account-must-not-be-used",
            "TX_TRADE_SKCOM_DLL_PATH": "com-dll-must-not-be-loaded",
        }
    )

    runs = [
        subprocess.run(
            [sys.executable, "-B", "-m", "tx_trade.app.phase2"],
            cwd=_PROJECT_ROOT,
            env=environment,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            timeout=10,
        )
        for _ in range(2)
    ]

    assert [run.returncode for run in runs] == [0, 0]
    assert runs[0].stdout == runs[1].stdout
    assert runs[0].stdout.splitlines() == [
        serialize_envelope(event).encode("utf-8") for event in expected
    ]
    assert [run.stderr for run in runs] == [
        f"Phase 2 replay completed.{os.linesep}".encode(),
        f"Phase 2 replay completed.{os.linesep}".encode(),
    ]
    assert _source_artifacts(database_path) == before
    combined_output = b"".join(run.stdout + run.stderr for run in runs)
    assert b"live-account-must-not-be-used" not in combined_output
    assert b"live-password-must-not-be-used" not in combined_output
    assert b"order-account-must-not-be-used" not in combined_output
    assert b"com-dll-must-not-be-loaded" not in combined_output


def test_sqlite_replay_pause_resume_has_no_duplicate_or_loss(tmp_path: Path) -> None:
    database_path = tmp_path / "pause-resume.db"
    session_id, expected = _record_complete_session(database_path)
    before = _source_artifacts(database_path)
    pause_requested = Event()
    published: list[MarketDataEnvelope] = []
    runtime: ReplayRuntime

    class PauseAfterFirst:
        def publish(self, envelope: MarketDataEnvelope) -> None:
            published.append(envelope)
            if len(published) == 1:
                runtime.pause()
                pause_requested.set()

    repository, runtime = _runtime(database_path, session_id, PauseAfterFirst())
    try:
        runtime.start()
        assert pause_requested.wait(2)
        runtime.pause()

        paused = runtime.snapshot()
        assert paused.state is ReplayState.PAUSED
        assert published == [expected[0]]
        assert paused.cursor == expected[0].ingest_sequence
        assert paused.emitted_count == 1

        runtime.resume()
        completed = runtime.wait(2)
    finally:
        repository.close()

    assert completed.state is ReplayState.COMPLETED
    assert completed.emitted_count == len(expected)
    assert published == list(expected)
    assert _source_artifacts(database_path) == before


def test_sqlite_replay_stop_prevents_any_further_publish(tmp_path: Path) -> None:
    database_path = tmp_path / "stop.db"
    session_id, expected = _record_complete_session(database_path)
    before = _source_artifacts(database_path)
    stop_requested = Event()
    published: list[MarketDataEnvelope] = []
    runtime: ReplayRuntime

    class StopAfterFirst:
        def publish(self, envelope: MarketDataEnvelope) -> None:
            published.append(envelope)
            if len(published) == 1:
                runtime.stop()
                stop_requested.set()

    repository, runtime = _runtime(database_path, session_id, StopAfterFirst())
    try:
        runtime.start()
        assert stop_requested.wait(2)
        runtime.stop(2)
        stopped = runtime.snapshot()
    finally:
        repository.close()

    assert stopped.state is ReplayState.STOPPED
    assert stopped.emitted_count == 1
    assert stopped.cursor == expected[0].ingest_sequence
    assert published == [expected[0]]
    assert _source_artifacts(database_path) == before
