from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from threading import Event
from time import monotonic
from types import SimpleNamespace
from typing import Callable
from uuid import UUID
from uuid import uuid4

import pytest

from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_TIME,
    OFFLINE_FIXTURE_TRADING_DAY,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import (
    SCHEMA_VERSION,
    MarketDataEnvelope,
    SourceMode,
)
from tx_trade.market_data.ports import RecordingSession
from tx_trade.replay.contracts import ReplayError, ReplayFailureCode
from tx_trade.replay import ReplayMode, ReplayOptions, ReplayRuntime, ReplayState
from tx_trade.replay.sqlite_source import prepare_sqlite_replay_source
from tx_trade.storage import SQLiteMarketDataRepository

_SECRET = "must-not-leak-from-storage"


class _CollectingSink:
    def __init__(self, expected_count: int) -> None:
        self.envelopes: list[MarketDataEnvelope] = []
        self.completed = Event()
        self._expected_count = expected_count

    def publish(self, envelope: MarketDataEnvelope) -> None:
        self.envelopes.append(envelope)
        if len(self.envelopes) == self._expected_count:
            self.completed.set()


def _begin_session(
    repository: SQLiteMarketDataRepository,
    *,
    session_id: UUID | None = None,
) -> tuple[UUID, tuple[MarketDataEnvelope, ...]]:
    fixture = make_offline_fixture_envelopes()
    target_session_id = session_id or uuid4()
    events = tuple(replace(event, session_id=target_session_id) for event in fixture)
    repository.begin_session(
        RecordingSession(
            session_id=target_session_id,
            schema_version=SCHEMA_VERSION,
            source=events[0].source,
            source_mode=SourceMode.OFFLINE,
            started_at=OFFLINE_FIXTURE_TIME,
            trading_day=OFFLINE_FIXTURE_TRADING_DAY,
            config_fingerprint="phase2-test",
        )
    )
    return target_session_id, events


def _assert_replay_error(
    expected_code: ReplayFailureCode,
    action: Callable[[], object],
) -> None:
    with pytest.raises(ReplayError) as caught:
        action()
    assert caught.value.code is expected_code
    assert (
        str(caught.value)
        == {
            ReplayFailureCode.SESSION_NOT_FOUND: "replay session was not found",
            ReplayFailureCode.SESSION_NOT_COMPLETE: "replay session is not complete",
            ReplayFailureCode.SCHEMA_MISMATCH: "replay session schema is unsupported",
            ReplayFailureCode.EMPTY_SESSION: "replay session contains no events",
            ReplayFailureCode.INTEGRITY_FAILED: "replay session failed integrity validation",
            ReplayFailureCode.SOURCE_FAILED: "replay source failed",
        }[expected_code]
    )
    assert _SECRET not in str(caught.value)
    assert _SECRET not in repr(caught.value)


def test_prepares_complete_current_nonempty_valid_session_and_preserves_gaps(
    tmp_path,
) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / "valid.db")
    session_id, fixture = _begin_session(repository)
    sequences = (2, 5, 9, 12, 20, 21)
    events = tuple(
        replace(event, ingest_sequence=sequence)
        for event, sequence in zip(fixture, sequences, strict=True)
    )
    repository.append_batch(events)
    repository.end_session(
        session_id,
        OFFLINE_FIXTURE_TIME + timedelta(minutes=1),
        "complete",
    )

    source, descriptor = prepare_sqlite_replay_source(repository, session_id)

    assert descriptor.session_id == session_id
    assert descriptor.status == "complete"
    assert descriptor.schema_version == SCHEMA_VERSION
    assert descriptor.event_count == len(events)
    assert descriptor.first_ingest_sequence == sequences[0]
    assert descriptor.last_ingest_sequence == sequences[-1]
    assert [event.ingest_sequence for event in source.iter_events()] == list(sequences)
    assert [
        event.ingest_sequence for event in source.iter_events(after_ingest_sequence=sequences[1])
    ] == list(sequences[2:])


def test_complete_sqlite_session_replays_deterministically_end_to_end(tmp_path) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / "deterministic.db")
    session_id, events = _begin_session(repository)
    repository.append_batch(events)
    repository.end_session(session_id, OFFLINE_FIXTURE_TIME, "complete")
    runs: list[tuple[MarketDataEnvelope, ...]] = []

    for _ in range(2):
        source, descriptor = prepare_sqlite_replay_source(repository, session_id)
        sink = _CollectingSink(len(events))
        runtime = ReplayRuntime(
            source=source,
            descriptor=descriptor,
            sink=sink,
            options=ReplayOptions(mode=ReplayMode.FASTEST),
        )

        runtime.start()
        assert sink.completed.wait(1)
        deadline = monotonic() + 1
        while not runtime.snapshot().state.is_terminal and monotonic() < deadline:
            sink.completed.wait(0.001)
        assert runtime.snapshot().state is ReplayState.COMPLETED
        runtime.stop(1)

        snapshot = runtime.snapshot()
        assert snapshot.state is ReplayState.COMPLETED
        assert snapshot.cursor == events[-1].ingest_sequence
        assert snapshot.emitted_count == len(events)
        runs.append(tuple(sink.envelopes))

    assert runs == [events, events]


def test_missing_session_is_rejected(tmp_path) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / "missing.db")

    _assert_replay_error(
        ReplayFailureCode.SESSION_NOT_FOUND,
        lambda: prepare_sqlite_replay_source(repository, uuid4()),
    )


@pytest.mark.parametrize("status", ["recording", "incomplete", "failed", "degraded"])
def test_noncomplete_session_is_rejected_before_integrity_check(
    tmp_path,
    status: str,
) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / f"{status}.db")
    session_id, events = _begin_session(repository)
    repository.append_batch(events)
    if status != "recording":
        repository.end_session(session_id, OFFLINE_FIXTURE_TIME, status)
    repository.connection.execute(
        "UPDATE event_log SET payload_json=? WHERE session_id=?",
        (_SECRET, str(session_id)),
    )

    _assert_replay_error(
        ReplayFailureCode.SESSION_NOT_COMPLETE,
        lambda: prepare_sqlite_replay_source(repository, session_id),
    )


def test_session_schema_mismatch_is_rejected_before_readback(tmp_path) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / "schema.db")
    session_id, events = _begin_session(repository)
    repository.append_batch(events)
    repository.end_session(session_id, OFFLINE_FIXTURE_TIME, "complete")
    repository.connection.execute(
        "UPDATE recording_sessions SET schema_version=? WHERE session_id=?",
        (SCHEMA_VERSION + 1, str(session_id)),
    )
    repository.connection.execute(
        "UPDATE event_log SET payload_json=? WHERE session_id=?",
        (_SECRET, str(session_id)),
    )

    _assert_replay_error(
        ReplayFailureCode.SCHEMA_MISMATCH,
        lambda: prepare_sqlite_replay_source(repository, session_id),
    )


def test_empty_complete_session_is_rejected(tmp_path) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / "empty.db")
    session_id, _ = _begin_session(repository)
    repository.end_session(session_id, OFFLINE_FIXTURE_TIME, "complete")

    _assert_replay_error(
        ReplayFailureCode.EMPTY_SESSION,
        lambda: prepare_sqlite_replay_source(repository, session_id),
    )


@pytest.mark.parametrize("corruption", ["payload", "checkpoint"])
def test_corrupt_or_checkpoint_mismatched_session_is_rejected(
    tmp_path,
    corruption: str,
) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / f"{corruption}.db")
    session_id, events = _begin_session(repository)
    repository.append_batch(events)
    repository.end_session(session_id, OFFLINE_FIXTURE_TIME, "complete")
    if corruption == "payload":
        repository.connection.execute(
            """UPDATE event_log SET payload_json=?
            WHERE session_id=? AND ingest_sequence=?""",
            (_SECRET, str(session_id), events[0].ingest_sequence),
        )
    else:
        repository.connection.execute(
            "UPDATE recording_sessions SET last_ingest_sequence=? WHERE session_id=?",
            (999, str(session_id)),
        )

    _assert_replay_error(
        ReplayFailureCode.INTEGRITY_FAILED,
        lambda: prepare_sqlite_replay_source(repository, session_id),
    )


class _FailingRepository:
    def get_session(self, session_id):
        raise RuntimeError(_SECRET)


def test_unknown_repository_error_is_sanitized() -> None:
    _assert_replay_error(
        ReplayFailureCode.SOURCE_FAILED,
        lambda: prepare_sqlite_replay_source(_FailingRepository(), uuid4()),  # type: ignore[arg-type]
    )


class _FailingOpenRepository:
    def __init__(self, session_id: UUID) -> None:
        self._session_id = session_id
        self._calls = 0

    def get_session(self, session_id):
        self._calls += 1
        if self._calls > 1:
            raise RuntimeError(_SECRET)
        return SimpleNamespace(
            session_id=self._session_id,
            status="complete",
            schema_version=SCHEMA_VERSION,
        )


def test_unknown_source_open_error_is_sanitized() -> None:
    session_id = uuid4()
    repository = _FailingOpenRepository(session_id)

    _assert_replay_error(
        ReplayFailureCode.SOURCE_FAILED,
        lambda: prepare_sqlite_replay_source(repository, session_id),  # type: ignore[arg-type]
    )


def test_integrity_exception_is_sanitized(tmp_path, monkeypatch) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / "decode.db")
    session_id, events = _begin_session(repository)
    repository.append_batch(events)
    repository.end_session(session_id, OFFLINE_FIXTURE_TIME, "complete")

    def fail_integrity(_source):
        raise RuntimeError(_SECRET)

    monkeypatch.setattr(
        "tx_trade.replay.sqlite_source.SQLiteReplaySource.verify_integrity",
        fail_integrity,
    )

    _assert_replay_error(
        ReplayFailureCode.INTEGRITY_FAILED,
        lambda: prepare_sqlite_replay_source(repository, session_id),
    )
