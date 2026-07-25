"""Bounded dedicated-thread batch writer with an explicit lifecycle."""

from __future__ import annotations

import math
import queue
import threading
import time
from dataclasses import dataclass
from enum import StrEnum

from tx_trade.market_data.models import MarketDataEnvelope

from .sqlite_repository import SQLiteMarketDataRepository, StorageError


class StorageBackpressureError(StorageError):
    pass


class WriterState(StrEnum):
    NEW = "new"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class WriterStats:
    accepted_events: int
    persisted_events: int
    duplicate_events: int
    flush_count: int
    queue_depth: int
    capacity: int
    is_running: bool


@dataclass(slots=True)
class _Barrier:
    done: threading.Event
    error: StorageError | None = None


class SQLiteMarketDataWriter:
    def __init__(
        self,
        repository: SQLiteMarketDataRepository,
        *,
        capacity: int,
        batch_size: int,
        flush_interval_seconds: float,
    ) -> None:
        if type(capacity) is not int or capacity < 1:
            raise ValueError("capacity must be positive")
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("batch_size must be positive")
        if (
            isinstance(flush_interval_seconds, bool)
            or not isinstance(flush_interval_seconds, (int, float))
            or not math.isfinite(float(flush_interval_seconds))
            or flush_interval_seconds <= 0
        ):
            raise ValueError("flush_interval_seconds must be finite and positive")
        self._repository = repository
        self._queue: queue.Queue[object] = queue.Queue(maxsize=capacity)
        self._capacity = capacity
        self._batch_size = batch_size
        self._interval = float(flush_interval_seconds)
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._state = WriterState.NEW
        self._fatal: StorageError | None = None
        self._accepted = 0
        self._persisted = 0
        self._duplicates = 0
        self._flushes = 0
        self._stop_token = object()

    @property
    def state(self) -> WriterState:
        with self._lock:
            return self._state

    def start(self) -> None:
        with self._lock:
            if self._state is WriterState.RUNNING:
                return
            if self._state is WriterState.FAILED:
                assert self._fatal is not None
                raise self._fatal
            if self._state is not WriterState.NEW:
                raise StorageError("stopped storage writer cannot be restarted")
            self._state = WriterState.RUNNING
            self._thread = threading.Thread(
                target=self._run,
                name="sqlite-market-data-writer",
                daemon=True,
            )
            self._thread.start()

    def _require_running(self) -> None:
        if self._state is WriterState.FAILED:
            assert self._fatal is not None
            raise self._fatal
        if self._state is not WriterState.RUNNING:
            raise StorageError(
                f"storage writer is not running ({self._state.value})"
            )

    def publish(self, envelope: MarketDataEnvelope) -> None:
        if type(envelope) is not MarketDataEnvelope:
            raise TypeError("envelope must be MarketDataEnvelope")
        with self._lock:
            self._require_running()
            try:
                self._queue.put_nowait(envelope)
            except queue.Full as exc:
                raise StorageBackpressureError("storage writer queue is full") from exc
            self._accepted += 1

    def flush(self, timeout: float | None = None) -> None:
        barrier = _Barrier(threading.Event())
        with self._lock:
            self._require_running()
            try:
                self._queue.put_nowait(barrier)
            except queue.Full as exc:
                raise StorageBackpressureError(
                    "could not enqueue flush barrier"
                ) from exc
        if not barrier.done.wait(timeout):
            raise TimeoutError("storage writer flush timed out")
        if barrier.error is not None:
            raise barrier.error

    def stop(self, timeout: float | None = None) -> None:
        with self._lock:
            if self._state is WriterState.FAILED:
                assert self._fatal is not None
                raise self._fatal
            if self._state is WriterState.NEW:
                self._state = WriterState.STOPPED
                return
            if self._state is WriterState.STOPPED:
                return
            thread = self._thread
            enqueue_stop = False
            if self._state is WriterState.RUNNING:
                self._state = WriterState.STOPPING
                enqueue_stop = True
            assert self._state is WriterState.STOPPING
        if enqueue_stop:
            try:
                self._queue.put(self._stop_token, timeout=timeout)
            except queue.Full as exc:
                with self._lock:
                    if self._state is WriterState.FAILED:
                        assert self._fatal is not None
                        raise self._fatal
                    self._state = WriterState.RUNNING
                raise StorageBackpressureError(
                    "could not enqueue stop request"
                ) from exc
        assert thread is not None
        thread.join(timeout)
        if thread.is_alive():
            raise TimeoutError("storage writer stop timed out")
        with self._lock:
            if self._state is WriterState.FAILED:
                assert self._fatal is not None
                raise self._fatal
            if self._state is not WriterState.STOPPED:
                raise StorageError("storage writer stopped in an invalid state")

    def stats(self) -> WriterStats:
        with self._lock:
            return WriterStats(
                self._accepted,
                self._persisted,
                self._duplicates,
                self._flushes,
                self._queue.qsize(),
                self._capacity,
                self._state is WriterState.RUNNING,
            )

    def _flush_batch(self, batch: list[MarketDataEnvelope]) -> None:
        if not batch:
            return
        before = self._repository.stats()
        self._repository.append_batch(batch)
        after = self._repository.stats()
        with self._lock:
            self._persisted += after.persisted_events - before.persisted_events
            self._duplicates += after.duplicate_events - before.duplicate_events
            self._flushes += 1
        batch.clear()

    def _run(self) -> None:
        batch: list[MarketDataEnvelope] = []
        deadline = time.monotonic() + self._interval
        try:
            while True:
                timeout = max(0.0, deadline - time.monotonic())
                try:
                    item = self._queue.get(timeout=timeout)
                except queue.Empty:
                    self._flush_batch(batch)
                    deadline = time.monotonic() + self._interval
                    continue
                try:
                    if item is self._stop_token:
                        self._flush_batch(batch)
                        with self._lock:
                            self._state = WriterState.STOPPED
                        return
                    if isinstance(item, _Barrier):
                        try:
                            self._flush_batch(batch)
                        except BaseException as exc:
                            item.error = self._as_storage_error(exc)
                            raise
                        finally:
                            item.done.set()
                        deadline = time.monotonic() + self._interval
                        continue
                    assert isinstance(item, MarketDataEnvelope)
                    batch.append(item)
                    if len(batch) >= self._batch_size:
                        self._flush_batch(batch)
                        deadline = time.monotonic() + self._interval
                finally:
                    self._queue.task_done()
        except BaseException as exc:
            fatal = self._as_storage_error(exc)
            with self._lock:
                self._fatal = fatal
                self._state = WriterState.FAILED
            while True:
                try:
                    pending = self._queue.get_nowait()
                except queue.Empty:
                    break
                if isinstance(pending, _Barrier):
                    pending.error = fatal
                    pending.done.set()
                self._queue.task_done()

    @staticmethod
    def _as_storage_error(exc: BaseException) -> StorageError:
        if isinstance(exc, StorageError):
            return exc
        error = StorageError("background storage writer failed")
        error.__cause__ = exc
        return error
