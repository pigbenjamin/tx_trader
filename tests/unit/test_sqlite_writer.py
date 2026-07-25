from __future__ import annotations

import threading

import pytest

from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_TIME,
    OFFLINE_FIXTURE_TRADING_DAY,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import SCHEMA_VERSION, SourceMode
from tx_trade.market_data.ports import RecordingSession
from tx_trade.storage import (
    SQLiteMarketDataRepository,
    SQLiteMarketDataWriter,
    StorageBackpressureError,
    StorageError,
    WriterState,
)


def _repository(tmp_path):
    repository = SQLiteMarketDataRepository(tmp_path / "events.db")
    events = make_offline_fixture_envelopes()
    repository.begin_session(RecordingSession(
        events[0].session_id, SCHEMA_VERSION, events[0].source,
        SourceMode.OFFLINE, OFFLINE_FIXTURE_TIME,
        OFFLINE_FIXTURE_TRADING_DAY, "fixture",
    ))
    return repository, events


def test_writer_batches_flushes_and_stops(tmp_path) -> None:
    repository, events = _repository(tmp_path)
    writer = SQLiteMarketDataWriter(
        repository, capacity=8, batch_size=2, flush_interval_seconds=10
    )
    writer.start()
    for event in events:
        writer.publish(event)
    writer.flush(timeout=2)
    stats = writer.stats()
    assert stats.accepted_events == 6
    assert stats.persisted_events == 6
    assert stats.flush_count == 3
    writer.stop(timeout=2)
    assert not writer.stats().is_running
    writer.stop(timeout=2)


def test_writer_interval_flush(tmp_path) -> None:
    repository, events = _repository(tmp_path)
    writer = SQLiteMarketDataWriter(
        repository, capacity=2, batch_size=2, flush_interval_seconds=0.02
    )
    writer.start()
    writer.publish(events[0])
    assert writer._thread is not None
    writer._thread.join(0.08)
    writer.flush(timeout=2)
    assert writer.stats().persisted_events == 1
    writer.stop(timeout=2)


def test_queue_full_is_explicit(tmp_path) -> None:
    class BlockingRepository(SQLiteMarketDataRepository):
        entered = threading.Event()
        release = threading.Event()
        def append_batch(self, events):
            self.entered.set()
            self.release.wait(2)
            super().append_batch(events)

    repository = BlockingRepository(tmp_path / "events.db")
    events = _begin_for(repository)
    writer = SQLiteMarketDataWriter(
        repository, capacity=1, batch_size=1, flush_interval_seconds=10
    )
    writer.start()
    writer.publish(events[0])
    assert repository.entered.wait(1)
    writer.publish(events[1])
    with pytest.raises(StorageBackpressureError):
        writer.publish(events[2])
    repository.release.set()
    writer.stop(timeout=2)


def _begin_for(repository):
    events = make_offline_fixture_envelopes()
    repository.begin_session(RecordingSession(
        events[0].session_id, SCHEMA_VERSION, events[0].source,
        SourceMode.OFFLINE, OFFLINE_FIXTURE_TIME,
        OFFLINE_FIXTURE_TRADING_DAY, "fixture",
    ))
    return events


def test_background_error_is_rethrown(tmp_path) -> None:
    class FailingRepository(SQLiteMarketDataRepository):
        def append_batch(self, events):
            raise StorageError("boom")

    repository = FailingRepository(tmp_path / "events.db")
    events = _begin_for(repository)
    writer = SQLiteMarketDataWriter(
        repository, capacity=2, batch_size=1, flush_interval_seconds=10
    )
    writer.start()
    writer.publish(events[0])
    assert writer._thread is not None
    writer._thread.join(1)
    with pytest.raises(StorageError, match="boom"):
        writer.flush(timeout=1)
    with pytest.raises(StorageError, match="boom"):
        writer.publish(events[1])
    with pytest.raises(StorageError, match="boom"):
        writer.stop(timeout=1)


def test_publish_and_flush_are_rejected_after_atomic_stop_cutoff(tmp_path) -> None:
    class BlockingRepository(SQLiteMarketDataRepository):
        entered = threading.Event()
        release = threading.Event()
        def append_batch(self, events):
            self.entered.set()
            assert self.release.wait(2)
            super().append_batch(events)

    repository = BlockingRepository(tmp_path / "events.db")
    events = _begin_for(repository)
    writer = SQLiteMarketDataWriter(
        repository, capacity=4, batch_size=1, flush_interval_seconds=10
    )
    writer.start()
    writer.publish(events[0])
    assert repository.entered.wait(1)
    writer.publish(events[1])
    stopped: list[BaseException] = []
    thread = threading.Thread(
        target=lambda: _capture(lambda: writer.stop(timeout=2), stopped)
    )
    thread.start()
    for _ in range(1000):
        if writer.state is WriterState.STOPPING:
            break
        threading.Event().wait(0.001)
    assert writer.state is WriterState.STOPPING
    with pytest.raises(StorageError):
        writer.publish(events[2])
    with pytest.raises(StorageError):
        writer.flush(timeout=1)
    repository.release.set()
    thread.join(2)
    assert not stopped
    stats = writer.stats()
    assert stats.accepted_events == stats.persisted_events + stats.duplicate_events
    assert stats.queue_depth == 0
    assert writer.state is WriterState.STOPPED


def test_flush_barrier_ordered_before_stop_token(tmp_path) -> None:
    class BlockingRepository(SQLiteMarketDataRepository):
        entered = threading.Event()
        release = threading.Event()
        def append_batch(self, events):
            self.entered.set()
            assert self.release.wait(2)
            super().append_batch(events)

    repository = BlockingRepository(tmp_path / "events.db")
    events = _begin_for(repository)
    writer = SQLiteMarketDataWriter(
        repository, capacity=4, batch_size=1, flush_interval_seconds=10
    )
    writer.start()
    writer.publish(events[0])
    assert repository.entered.wait(1)
    flush_errors: list[BaseException] = []
    flush_thread = threading.Thread(
        target=lambda: _capture(lambda: writer.flush(timeout=2), flush_errors)
    )
    flush_thread.start()
    while writer.stats().queue_depth < 1:
        threading.Event().wait(0.001)
    stop_errors: list[BaseException] = []
    stop_thread = threading.Thread(
        target=lambda: _capture(lambda: writer.stop(timeout=2), stop_errors)
    )
    stop_thread.start()
    repository.release.set()
    flush_thread.join(2)
    stop_thread.join(2)
    assert not flush_errors
    assert not stop_errors
    assert writer.state is WriterState.STOPPED


def _capture(call, errors):
    try:
        call()
    except BaseException as exc:
        errors.append(exc)
