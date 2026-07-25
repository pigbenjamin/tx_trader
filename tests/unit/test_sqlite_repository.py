from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import timedelta

import pytest

from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_TIME,
    OFFLINE_FIXTURE_TRADING_DAY,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import EventType, SCHEMA_VERSION, SourceMode
from tx_trade.market_data.ports import RecordingSession
from tx_trade.storage import (
    DuplicateSequenceError,
    IntegrityError,
    SchemaMismatchError,
    SQLiteMarketDataRepository,
    StorageError,
)


def _begin(repository: SQLiteMarketDataRepository) -> tuple:
    events = make_offline_fixture_envelopes()
    repository.begin_session(
        RecordingSession(
            events[0].session_id,
            SCHEMA_VERSION,
            events[0].source,
            SourceMode.OFFLINE,
            OFFLINE_FIXTURE_TIME,
            OFFLINE_FIXTURE_TRADING_DAY,
            "fixture",
        )
    )
    return events


def test_schema_wal_projection_and_cursor(tmp_path) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / "events.db")
    events = _begin(repository)
    repository.append_batch(events)
    assert repository.connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    tables = {
        row[0]
        for row in repository.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    assert {
        "schema_meta", "recording_sessions", "event_log", "instruments",
        "quotes", "ticks", "connection_events",
    } <= tables
    assert repository.connection.execute("SELECT COUNT(*) FROM quotes").fetchone()[0] == 1
    assert repository.connection.execute("SELECT COUNT(*) FROM ticks").fetchone()[0] == 1
    assert repository.connection.execute("SELECT COUNT(*) FROM connection_events").fetchone()[0] == 1
    assert [e.ingest_sequence for e in repository.iter_events(
        events[0].session_id, after_ingest_sequence=2,
        event_types={EventType.QUOTE, EventType.TICK},
    )] == [3, 4]


def test_duplicate_dedupe_and_sequence_collision(tmp_path) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / "events.db")
    events = _begin(repository)
    repository.append_batch(events[:1])
    repository.append_batch(events[:1])
    assert repository.stats().duplicate_events == 1
    assert repository.connection.execute("SELECT COUNT(*) FROM event_log").fetchone()[0] == 1
    collision = replace(events[1], ingest_sequence=0)
    with pytest.raises(DuplicateSequenceError):
        repository.append_batch([collision])


def test_duplicate_inside_one_batch_is_skipped(tmp_path) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / "events.db")
    events = _begin(repository)
    duplicate = replace(events[0], ingest_sequence=1, sequence=1)
    repository.append_batch([events[0], duplicate])
    assert repository.connection.execute(
        "SELECT COUNT(*) FROM event_log"
    ).fetchone()[0] == 1
    assert repository.stats().duplicate_events == 1


def test_schema_missing_and_mismatch_are_rejected(tmp_path) -> None:
    path = tmp_path / "bad.db"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE unrelated(value INTEGER)")
    connection.close()
    with pytest.raises(SchemaMismatchError):
        SQLiteMarketDataRepository(path)

    path2 = tmp_path / "old.db"
    connection = sqlite3.connect(path2)
    connection.execute("CREATE TABLE schema_meta(version INTEGER, applied_at TEXT)")
    connection.execute("INSERT INTO schema_meta VALUES (99, 'x')")
    connection.commit()
    connection.close()
    with pytest.raises(SchemaMismatchError):
        SQLiteMarketDataRepository(path2)


def test_reopen_recovers_recording_session_as_incomplete(tmp_path) -> None:
    path = tmp_path / "events.db"
    repository = SQLiteMarketDataRepository(path)
    events = _begin(repository)
    repository.close()
    reopened = SQLiteMarketDataRepository(path, recover_incomplete_sessions=True)
    assert reopened.get_session(events[0].session_id).status == "incomplete"


def test_second_repository_does_not_mutate_active_session_by_default(tmp_path) -> None:
    path = tmp_path / "events.db"
    first = SQLiteMarketDataRepository(path)
    events = _begin(first)
    second = SQLiteMarketDataRepository(path)
    assert first.get_session(events[0].session_id).status == "recording"
    assert second.get_session(events[0].session_id).status == "recording"


def test_projection_failure_preserves_authoritative_events(tmp_path) -> None:
    class BrokenProjection(SQLiteMarketDataRepository):
        def _insert_projection(self, connection, event_id, envelope):
            if envelope.event_type is EventType.QUOTE:
                raise sqlite3.OperationalError("projection mismatch")
            return super()._insert_projection(connection, event_id, envelope)

    repository = BrokenProjection(tmp_path / "events.db")
    events = _begin(repository)
    repository.append_batch(events)
    assert repository.connection.execute("SELECT COUNT(*) FROM event_log").fetchone()[0] == 6
    assert repository.connection.execute("SELECT COUNT(*) FROM quotes").fetchone()[0] == 0
    assert repository.get_session(events[0].session_id).status == "incomplete"
    assert repository.stats().projection_failures == 1

    later = replace(
        events[3],
        ingest_sequence=6,
        sequence=6,
        dedupe_key=f"{events[3].dedupe_key}:later",
    )
    repository.append_batch([later])
    assert repository.connection.execute(
        "SELECT COUNT(*) FROM event_log"
    ).fetchone()[0] == 7
    assert repository.connection.execute(
        "SELECT COUNT(*) FROM quotes"
    ).fetchone()[0] == 0
    assert repository.stats().persisted_events == 7
    repository.end_session(
        events[0].session_id,
        OFFLINE_FIXTURE_TIME + timedelta(minutes=1),
        "complete",
    )
    row = repository.connection.execute(
        "SELECT status,ended_at FROM recording_sessions WHERE session_id=?",
        (str(events[0].session_id),),
    ).fetchone()
    assert row["status"] == "incomplete"
    assert row["ended_at"] is not None
    with pytest.raises(StorageError):
        repository.append_batch([
            replace(
                events[3],
                ingest_sequence=7,
                sequence=7,
                dedupe_key=f"{events[3].dedupe_key}:after-finalize",
            )
        ])


def test_non_sqlite_projection_bug_is_fatal_and_rolls_back(tmp_path) -> None:
    class ProgrammingBug(SQLiteMarketDataRepository):
        def _insert_projection(self, connection, event_id, envelope):
            raise TypeError("programming bug")

    repository = ProgrammingBug(tmp_path / "events.db")
    events = _begin(repository)
    with pytest.raises(Exception) as caught:
        repository.append_batch(events[:1])
    assert caught.type.__name__ == "StorageError"
    assert repository.connection.execute(
        "SELECT COUNT(*) FROM event_log"
    ).fetchone()[0] == 0
    assert repository.stats().projection_failures == 0


def test_begin_immediate_lock_failure_preserves_original_cause(tmp_path) -> None:
    path = tmp_path / "events.db"
    repository = SQLiteMarketDataRepository(path, busy_timeout_ms=1)
    events = _begin(repository)
    blocker = sqlite3.connect(path, isolation_level=None)
    blocker.execute("BEGIN IMMEDIATE")
    try:
        with pytest.raises(Exception) as caught:
            repository.append_batch(events[:1])
        assert caught.type.__name__ == "StorageError"
        assert isinstance(caught.value.__cause__, sqlite3.OperationalError)
    finally:
        blocker.execute("ROLLBACK")
        blocker.close()


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("source", "tampered-source"),
        ("source_mode", "replay"),
        ("connection_generation", 9),
        ("sequence", 999),
        ("broker_sequence", 3),
        ("dedupe_key", "tampered-dedupe"),
        ("event_at", (OFFLINE_FIXTURE_TIME + timedelta(seconds=1)).isoformat()),
        ("trading_day", "2026-07-27"),
        ("received_at", (OFFLINE_FIXTURE_TIME + timedelta(seconds=1)).isoformat()),
        ("metadata_version", 7),
        ("raw_json", '{"$tx-storage-type":"map","items":[]}'),
    ],
)
def test_authoritative_metadata_tamper_is_detected(tmp_path, column, value) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / "events.db")
    events = _begin(repository)
    repository.append_batch(events)
    repository.connection.execute(
        f"UPDATE event_log SET {column}=? WHERE ingest_sequence=3", (value,)
    )
    with pytest.raises(IntegrityError):
        tuple(repository.iter_events(events[0].session_id))
    from tx_trade.storage import SQLiteReplaySource
    replay = SQLiteReplaySource(repository)
    replay.open(events[0].session_id)
    assert not replay.verify_integrity().is_valid


def test_checkpoint_tamper_is_reported(tmp_path) -> None:
    from tx_trade.storage import SQLiteReplaySource
    repository = SQLiteMarketDataRepository(tmp_path / "events.db")
    events = _begin(repository)
    repository.append_batch(events)
    repository.connection.execute(
        "UPDATE recording_sessions SET last_ingest_sequence=4 WHERE session_id=?",
        (str(events[0].session_id),),
    )
    replay = SQLiteReplaySource(repository)
    replay.open(events[0].session_id)
    assert not replay.verify_integrity().is_valid


def test_current_version_but_incomplete_schema_is_rejected(tmp_path) -> None:
    path = tmp_path / "partial.db"
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE schema_meta(version INTEGER PRIMARY KEY, applied_at TEXT)"
    )
    connection.execute("INSERT INTO schema_meta VALUES (1, 'x')")
    connection.commit()
    connection.close()
    with pytest.raises(SchemaMismatchError):
        SQLiteMarketDataRepository(path)


def test_missing_required_index_is_rejected(tmp_path) -> None:
    path = tmp_path / "missing-index.db"
    repository = SQLiteMarketDataRepository(path)
    repository.connection.execute("DROP INDEX idx_event_log_readback")
    repository.close()
    with pytest.raises(SchemaMismatchError):
        SQLiteMarketDataRepository(path)


def test_same_named_index_with_wrong_column_order_is_rejected(tmp_path) -> None:
    path = tmp_path / "wrong-index.db"
    repository = SQLiteMarketDataRepository(path)
    repository.connection.execute("DROP INDEX idx_event_log_readback")
    repository.connection.execute(
        "CREATE INDEX idx_event_log_readback "
        "ON event_log(ingest_sequence, session_id)"
    )
    repository.close()
    with pytest.raises(SchemaMismatchError):
        SQLiteMarketDataRepository(path)


def test_same_named_lookup_index_cannot_be_unique(tmp_path) -> None:
    path = tmp_path / "unique-lookup-index.db"
    repository = SQLiteMarketDataRepository(path)
    repository.connection.execute("DROP INDEX idx_event_log_readback")
    repository.connection.execute(
        "CREATE UNIQUE INDEX idx_event_log_readback "
        "ON event_log(session_id, ingest_sequence)"
    )
    repository.close()
    with pytest.raises(SchemaMismatchError):
        SQLiteMarketDataRepository(path)


def test_missing_required_column_is_rejected(tmp_path) -> None:
    path = tmp_path / "missing-column.db"
    repository = SQLiteMarketDataRepository(path)
    repository.connection.execute(
        "ALTER TABLE event_log DROP COLUMN record_sha256"
    )
    repository.close()
    with pytest.raises(SchemaMismatchError):
        SQLiteMarketDataRepository(path)


def test_missing_event_log_unique_constraints_is_rejected(tmp_path) -> None:
    path = tmp_path / "missing-unique.db"
    repository = SQLiteMarketDataRepository(path)
    connection = repository.connection
    create_sql = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='event_log'"
    ).fetchone()["sql"]
    malformed = create_sql.replace(
        "CREATE TABLE event_log", "CREATE TABLE event_log_new"
    ).replace(
        ",\n    UNIQUE (session_id, ingest_sequence),"
        "\n    UNIQUE (session_id, dedupe_key)",
        "",
    )
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute(malformed)
    connection.execute("DROP TABLE event_log")
    connection.execute("ALTER TABLE event_log_new RENAME TO event_log")
    connection.execute(
        "CREATE INDEX idx_event_log_readback "
        "ON event_log(session_id, ingest_sequence)"
    )
    connection.execute(
        "CREATE INDEX idx_event_log_type_day "
        "ON event_log(event_type, trading_day)"
    )
    repository.close()
    with pytest.raises(SchemaMismatchError):
        SQLiteMarketDataRepository(path)


def test_source_mode_check_with_extra_literal_is_rejected(tmp_path) -> None:
    path = tmp_path / "extra-source-mode.db"
    repository = SQLiteMarketDataRepository(path)
    connection = repository.connection
    create_sql = connection.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type='table' AND name='recording_sessions'"
    ).fetchone()["sql"]
    malformed = create_sql.replace(
        "CREATE TABLE recording_sessions",
        "CREATE TABLE recording_sessions_new",
    ).replace(
        "('offline', 'replay', 'live')",
        "('offline', 'replay', 'live', 'paper')",
    )
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute(malformed)
    connection.execute("DROP TABLE recording_sessions")
    connection.execute(
        "ALTER TABLE recording_sessions_new RENAME TO recording_sessions"
    )
    connection.execute(
        "CREATE INDEX idx_sessions_trading_day_started "
        "ON recording_sessions(trading_day, started_at)"
    )
    repository.close()
    with pytest.raises(SchemaMismatchError):
        SQLiteMarketDataRepository(path)


def test_iter_events_is_lazy_and_pages_large_recording(tmp_path) -> None:
    repository = SQLiteMarketDataRepository(tmp_path / "large.db")
    fixture = _begin(repository)
    base = fixture[1]
    events = [
        replace(
            base,
            ingest_sequence=index,
            sequence=index,
            dedupe_key=f"{base.dedupe_key}:{index}",
        )
        for index in range(1000)
    ]
    repository.append_batch(events)
    iterator = repository.iter_events(fixture[0].session_id)
    assert iter(iterator) is iterator
    assert next(iterator).ingest_sequence == 0
    assert sum(1 for _ in iterator) == 999
