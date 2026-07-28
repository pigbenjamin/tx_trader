from __future__ import annotations

import hashlib
import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from uuid import UUID

from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_TIME,
    OFFLINE_FIXTURE_TRADING_DAY,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import SCHEMA_VERSION, ConnectionStatus, SourceMode
from tx_trade.market_data.ports import RecordingSession
from tx_trade.research import ResearchOutboxRecordType, ResearchRunStatus
from tx_trade.research.sqlite_repository import SQLiteResearchStateRepository
from tx_trade.storage import SQLiteMarketDataRepository

SESSION_ID = UUID("11111111-1111-1111-1111-111111111111")
RUN_ID = UUID("22222222-2222-2222-2222-222222222222")
_STATE_LIMIT = 16 * 1024 * 1024
_CHILD_OS_ENV_ALLOWLIST = frozenset({"COMSPEC", "SYSTEMDRIVE", "SYSTEMROOT", "WINDIR"})


def _record_paced_source(path: Path) -> tuple[int, ...]:
    fixture = make_offline_fixture_envelopes()
    first = fixture[0]
    assert type(first.payload) is ConnectionStatus
    earlier = OFFLINE_FIXTURE_TIME - timedelta(seconds=30)
    events = (
        replace(
            first,
            payload=replace(first.payload, changed_at=earlier),
            session_id=SESSION_ID,
            event_at=earlier,
            received_at=earlier,
        ),
        *(replace(event, session_id=SESSION_ID) for event in fixture[1:]),
    )
    repository = SQLiteMarketDataRepository(path)
    try:
        repository.begin_session(
            RecordingSession(
                session_id=SESSION_ID,
                schema_version=SCHEMA_VERSION,
                source=events[0].source,
                source_mode=SourceMode.OFFLINE,
                started_at=earlier,
                trading_day=OFFLINE_FIXTURE_TRADING_DAY,
                config_fingerprint="phase2-final-recovery",
            )
        )
        repository.append_batch(events)
        repository.end_session(
            SESSION_ID,
            OFFLINE_FIXTURE_TIME + timedelta(minutes=1),
            "complete",
        )
    finally:
        repository.close()
    return tuple(event.ingest_sequence for event in events)


def _environment(
    source: Path,
    state: Path,
    *,
    restart_mode: str,
    replay_mode: str,
) -> dict[str, str]:
    return {
        "TX_TRADE_RUNTIME_PRESET": "research_paper",
        "TX_TRADE_RESEARCH_PAPER_DB_PATH": str(source),
        "TX_TRADE_RESEARCH_PAPER_SESSION_ID": str(SESSION_ID),
        "TX_TRADE_RESEARCH_PAPER_REPLAY_MODE": replay_mode,
        "TX_TRADE_RESEARCH_PAPER_REPLAY_SPEED": "1",
        "TX_TRADE_RESEARCH_PAPER_RUN_ID": str(RUN_ID),
        "TX_TRADE_RESEARCH_PAPER_RESTART_MODE": restart_mode,
        "TX_TRADE_RESEARCH_PAPER_STATE_DB_PATH": str(state),
        "TX_TRADE_RESEARCH_PAPER_MAX_STATE_MAIN_DB_BYTES": str(_STATE_LIMIT),
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


def _child_environment(values: dict[str, str], tmp_path: Path) -> dict[str, str]:
    environment = {
        key: value for key, value in os.environ.items() if key.upper() in _CHILD_OS_ENV_ALLOWLIST
    }
    environment.update(values)
    environment.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
            "PYTHONUTF8": "1",
            "TEMP": str(tmp_path),
            "TMP": str(tmp_path),
            "TMPDIR": str(tmp_path),
        }
    )
    return environment


def _start_child(tmp_path: Path, values: dict[str, str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-B", "-m", "tx_trade.app.research_paper"],
        cwd=tmp_path,
        env=_child_environment(values, tmp_path),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _run_child(tmp_path: Path, values: dict[str, str]) -> bytes:
    process = _start_child(tmp_path, values)
    try:
        stdout, stderr = process.communicate(timeout=15)
    except BaseException:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)
        raise
    assert process.returncode == 0
    assert stderr == f"Research paper replay completed.{os.linesep}".encode()
    assert stdout.endswith(b"\n")
    return stdout


def _wait_for_first_commit(state: Path) -> tuple[int, int]:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            if not state.is_file() or state.stat().st_size == 0:
                time.sleep(0.01)
                continue
        except OSError:
            time.sleep(0.01)
            continue
        try:
            with sqlite3.connect(state, timeout=0.1) as connection:
                row = connection.execute(
                    """SELECT committed_cursor, committed_batch_count
                    FROM research_runs WHERE paper_run_id=?""",
                    (str(RUN_ID),),
                ).fetchone()
            if row is not None and row[1] == 1:
                return int(row[0]), int(row[1])
        except sqlite3.Error:
            pass
        time.sleep(0.01)
    raise AssertionError("child did not reach the deterministic first-commit boundary")


def _raw_ledger(path: Path) -> tuple[tuple[object, ...], tuple[tuple[object, ...], ...]]:
    with sqlite3.connect(path) as connection:
        run = connection.execute(
            """SELECT status, state_version, committed_cursor, committed_batch_count,
            broker_checkpoint, broker_checkpoint_sha256,
            coordinator_checkpoint, coordinator_checkpoint_sha256
            FROM research_runs WHERE paper_run_id=?""",
            (str(RUN_ID),),
        ).fetchone()
        batches = connection.execute(
            """SELECT source_ingest_sequence, envelope_fingerprint,
            decision_fingerprint, batch_fingerprint, applied_state_version
            FROM research_batches WHERE paper_run_id=?
            ORDER BY applied_state_version""",
            (str(RUN_ID),),
        ).fetchall()
    assert run is not None
    return tuple(run), tuple(tuple(row) for row in batches)


def _raw_outbox(path: Path) -> tuple[tuple[object, ...], ...]:
    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            """SELECT paper_run_id, output_sequence, record_type,
            source_ingest_sequence, paper_sequence, payload, payload_sha256,
            payload_bytes, created_state_version
            FROM research_outbox WHERE paper_run_id=?
            ORDER BY output_sequence""",
            (str(RUN_ID),),
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _assert_raw_outbox(
    rows: tuple[tuple[object, ...], ...],
    *,
    source_sequences: tuple[int, ...],
) -> bytes:
    assert rows
    assert all(row[0] == str(RUN_ID) for row in rows)
    assert tuple(row[1] for row in rows) == tuple(range(len(rows)))

    market = tuple(row for row in rows if row[2] == "market")
    paper = tuple(row for row in rows if row[2] == "paper")
    summary = tuple(row for row in rows if row[2] == "summary")
    assert tuple(row[2] for row in rows) == (
        *("market" for _ in market),
        *("paper" for _ in paper),
        "summary",
    )
    assert tuple(row[3] for row in market) == source_sequences
    assert all(row[4] is None for row in market)
    assert tuple(row[4] for row in paper) == tuple(range(1, len(paper) + 1))
    assert all(row[3] in source_sequences for row in paper)
    assert len(summary) == 1
    assert summary[0][3] is None and summary[0][4] is None

    source_version = {
        sequence: version for version, sequence in enumerate(source_sequences, start=1)
    }
    assert tuple(row[8] for row in market) == tuple(range(1, len(source_sequences) + 1))
    assert all(row[8] == source_version[row[3]] for row in paper)
    assert summary[0][8] == len(source_sequences) + 1

    payloads = tuple(row[5] for row in rows)
    assert all(type(payload) is bytes for payload in payloads)
    assert all(
        len(payload) == row[7]
        and payload.endswith(b"\n")
        and payload.count(b"\n") == 1
        and row[6]
        == "sha256:" + hashlib.sha256(b"tx_trade.research.outbox.v1:" + payload).hexdigest()
        for row, payload in zip(rows, payloads, strict=True)
    )
    return b"".join(payloads)


def test_abrupt_post_commit_death_resumes_to_byte_exact_oracle(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source.sqlite3"
    clean_state = tmp_path / "clean-state.sqlite3"
    recovered_state = tmp_path / "recovered-state.sqlite3"
    poison = {
        "PYTHONHASHSEED": "random",
        "PYTHONHOME": str(tmp_path / "poison-python-home"),
        "PYTHONINSPECT": "1",
        "PYTHONDONTWRITEBYTECODE": "0",
        "PYTHONPATH": str(tmp_path / "poison-python-path"),
        "PYTHONPROFILEIMPORTTIME": "1",
        "PYTHONSTARTUP": str(tmp_path / "poison-startup.py"),
        "PYTHONUTF8": "0",
        "PYTHONWARNINGS": "error",
        "TX_TRADE_UNKNOWN_POISON": "must-not-reach-child",
    }
    for key, value in poison.items():
        monkeypatch.setenv(key, value)
    probed_environment = _child_environment(
        _environment(source, clean_state, restart_mode="create", replay_mode="fastest"),
        tmp_path,
    )
    explicitly_overridden = {"PYTHONDONTWRITEBYTECODE", "PYTHONPATH", "PYTHONUTF8"}
    assert (poison.keys() - explicitly_overridden).isdisjoint(probed_environment)
    assert probed_environment["PYTHONPATH"] != poison["PYTHONPATH"]
    assert probed_environment["PYTHONPATH"] == str(Path(__file__).resolve().parents[2])
    assert probed_environment["PYTHONDONTWRITEBYTECODE"] == "1"
    assert probed_environment["PYTHONUTF8"] == "1"

    source_sequences = _record_paced_source(source)
    source_before = hashlib.sha256(source.read_bytes()).digest()
    source_sidecars = (Path(f"{source}-wal"), Path(f"{source}-shm"))
    assert all(not path.exists() for path in source_sidecars)

    clean_stdout = _run_child(
        tmp_path,
        _environment(source, clean_state, restart_mode="create", replay_mode="fastest"),
    )

    interrupted = _start_child(
        tmp_path,
        _environment(source, recovered_state, restart_mode="create", replay_mode="paced"),
    )
    try:
        cursor_at_death, batch_count_at_death = _wait_for_first_commit(recovered_state)
        assert cursor_at_death == source_sequences[0]
        assert batch_count_at_death == 1
        assert interrupted.poll() is None
        # The next event is 30 seconds away in paced replay. TerminateProcess/SIGKILL
        # makes this a real abrupt death after the independently observed commit.
        interrupted.kill()
        crashed_stdout, _ = interrupted.communicate(timeout=5)
    finally:
        if interrupted.poll() is None:
            interrupted.kill()
            interrupted.communicate(timeout=5)
    assert interrupted.returncode != 0
    assert crashed_stdout == b""

    recovered_stdout = _run_child(
        tmp_path,
        _environment(source, recovered_state, restart_mode="resume", replay_mode="paced"),
    )
    assert recovered_stdout == clean_stdout

    clean_raw_outbox = _raw_outbox(clean_state)
    recovered_raw_outbox = _raw_outbox(recovered_state)
    assert recovered_raw_outbox == clean_raw_outbox
    assert _assert_raw_outbox(clean_raw_outbox, source_sequences=source_sequences) == clean_stdout
    assert (
        _assert_raw_outbox(recovered_raw_outbox, source_sequences=source_sequences)
        == recovered_stdout
    )

    # The production decoder is a supplemental cross-check; the byte oracle above
    # is read independently from every persisted outbox column using sqlite3.
    with SQLiteResearchStateRepository(
        clean_state, max_main_database_bytes=_STATE_LIMIT
    ) as clean_repository:
        clean_hydration = clean_repository.load_run(RUN_ID)
        clean_outbox = clean_repository.read_outbox(RUN_ID)
    with SQLiteResearchStateRepository(
        recovered_state, max_main_database_bytes=_STATE_LIMIT
    ) as recovered_repository:
        recovered_hydration = recovered_repository.load_run(RUN_ID)
        recovered_outbox = recovered_repository.read_outbox(RUN_ID)

    assert recovered_hydration == clean_hydration
    assert recovered_hydration.run_state.status is ResearchRunStatus.COMPLETE
    assert recovered_hydration.run_state.committed_cursor == source_sequences[-1]
    assert recovered_hydration.run_state.committed_batch_count == len(source_sequences)
    assert recovered_hydration.run_state.state_version == len(source_sequences) + 1
    assert recovered_outbox == clean_outbox
    assert tuple(record.output_sequence for record in recovered_outbox) == tuple(
        range(len(recovered_outbox))
    )
    assert (
        tuple(
            record.source_ingest_sequence
            for record in recovered_outbox
            if record.record_type is ResearchOutboxRecordType.MARKET
        )
        == source_sequences
    )
    paper_sequences = tuple(
        record.paper_sequence
        for record in recovered_outbox
        if record.record_type is ResearchOutboxRecordType.PAPER
    )
    assert paper_sequences == tuple(range(1, len(paper_sequences) + 1))
    assert b"".join(record.payload for record in recovered_outbox) == recovered_stdout

    clean_run, clean_batches = _raw_ledger(clean_state)
    recovered_run, recovered_batches = _raw_ledger(recovered_state)
    assert recovered_run == clean_run
    assert recovered_batches == clean_batches
    assert tuple(batch[0] for batch in recovered_batches) == source_sequences
    assert tuple(batch[4] for batch in recovered_batches) == tuple(
        range(1, len(source_sequences) + 1)
    )

    assert hashlib.sha256(source.read_bytes()).digest() == source_before
    assert all(not path.exists() for path in source_sidecars)
    assert {source, clean_state, recovered_state} <= set(tmp_path.iterdir())
    assert {path.name for path in tmp_path.iterdir()} <= {
        source.name,
        clean_state.name,
        f"{clean_state.name}-wal",
        f"{clean_state.name}-shm",
        recovered_state.name,
        f"{recovered_state.name}-wal",
        f"{recovered_state.name}-shm",
    }
