"""Single-use, deterministic replay runtime."""

from __future__ import annotations

import math
from collections.abc import Iterator
from threading import Condition, Event, RLock, Thread, current_thread

from tx_trade.market_data.models import SCHEMA_VERSION, MarketDataEnvelope
from tx_trade.market_data.ports import MarketDataSink, ReplaySource

from .clock import ReplayTimer, SystemReplayTimer
from .contracts import (
    ReplayError,
    ReplayFailureCode,
    ReplayMode,
    ReplayOptions,
    ReplaySessionDescriptor,
    ReplaySnapshot,
    ReplayState,
)


class ReplayRuntime:
    """Publish one validated session once, with acknowledged playback controls."""

    def __init__(
        self,
        *,
        source: ReplaySource,
        descriptor: ReplaySessionDescriptor,
        sink: MarketDataSink,
        options: ReplayOptions,
        timer: ReplayTimer | None = None,
    ) -> None:
        if type(descriptor) is not ReplaySessionDescriptor:
            raise TypeError("descriptor must be ReplaySessionDescriptor")
        if type(options) is not ReplayOptions:
            raise TypeError("options must be ReplayOptions")
        self._validate_session(descriptor, options)

        self._source = source
        self._descriptor = descriptor
        self._sink = sink
        self._options = options
        self._timer = timer if timer is not None else SystemReplayTimer()

        self._condition = Condition(RLock())
        self._wake_event = Event()
        self._state = ReplayState.READY
        self._cursor = options.after_ingest_sequence
        self._emitted_count = 0
        self._expected_emitted_count = 0
        self._failure_code: ReplayFailureCode | None = None
        self._pause_requested = False
        self._stop_requested = False
        self._worker: Thread | None = None

    @staticmethod
    def _validate_session(descriptor: ReplaySessionDescriptor, options: ReplayOptions) -> None:
        if descriptor.status != "complete":
            raise ReplayError(ReplayFailureCode.SESSION_NOT_COMPLETE)
        if descriptor.schema_version != SCHEMA_VERSION:
            raise ReplayError(ReplayFailureCode.SCHEMA_MISMATCH)
        cursor = options.after_ingest_sequence
        if cursor is not None and cursor > descriptor.last_ingest_sequence:
            raise ReplayError(ReplayFailureCode.CURSOR_OUT_OF_RANGE)

    def start(self) -> None:
        """Start the background publisher; a runtime can be started only once."""

        with self._condition:
            if self._state is not ReplayState.READY:
                raise RuntimeError("replay runtime is single-use")
            self._state = ReplayState.RUNNING
            self._worker = Thread(
                target=self._run,
                name=f"replay-{self._descriptor.session_id}",
                daemon=True,
            )
            self._worker.start()

    def run(self) -> ReplaySnapshot:
        """Run synchronously on the calling thread until a terminal state."""

        with self._condition:
            if self._state is not ReplayState.READY:
                raise RuntimeError("replay runtime is single-use")
            self._state = ReplayState.RUNNING
            self._worker = current_thread()
        try:
            self._run()
        except BaseException:
            with self._condition:
                if not self._state.is_terminal:
                    self._stop_requested = True
                    self._state = ReplayState.STOPPED
                    self._condition.notify_all()
            raise
        finally:
            with self._condition:
                self._worker = None
                self._condition.notify_all()
        return self.snapshot()

    def pause(self) -> None:
        """Request a pause and, off-worker, wait until it is acknowledged.

        A call made reentrantly by ``sink.publish`` only records the request;
        the replay worker acknowledges it after the callback returns.
        """

        with self._condition:
            if self._state is ReplayState.PAUSED:
                return
            if self._state is not ReplayState.RUNNING:
                raise RuntimeError("replay runtime is not running")
            self._pause_requested = True
            self._wake_event.set()
            if self._is_worker_thread():
                return
            self._condition.wait_for(lambda: self._state is not ReplayState.RUNNING)

    def resume(self) -> None:
        """Resume an acknowledged pause."""

        with self._condition:
            if self._state is ReplayState.RUNNING and not self._pause_requested:
                return
            if self._state is not ReplayState.PAUSED:
                raise RuntimeError("replay runtime is not paused")
            self._pause_requested = False
            self._state = ReplayState.RUNNING
            self._wake_event.set()
            self._condition.notify_all()

    def stop(self, timeout_seconds: float | None = None) -> None:
        """Stop playback idempotently and, off-worker, wait for termination.

        A call made reentrantly by ``sink.publish`` only records the request
        because a worker cannot join itself.  An external caller may call
        ``stop`` again to wait for the requested termination.
        """

        timeout = self._validate_timeout(timeout_seconds)
        worker: Thread | None
        with self._condition:
            if self._state.is_terminal:
                worker = self._worker
                if worker is None or self._is_worker_thread():
                    return
            elif self._state is ReplayState.READY:
                self._stop_requested = True
                self._state = ReplayState.STOPPED
                self._condition.notify_all()
                return
            else:
                self._stop_requested = True
                self._pause_requested = False
                self._wake_event.set()
                self._condition.notify_all()
                worker = self._worker
                if self._is_worker_thread():
                    return

        if worker is not None:
            worker.join(timeout)
            if worker.is_alive():
                raise TimeoutError("replay worker did not stop before timeout")

    def snapshot(self) -> ReplaySnapshot:
        with self._condition:
            return ReplaySnapshot(
                state=self._state,
                session_id=self._descriptor.session_id,
                cursor=self._cursor,
                emitted_count=self._emitted_count,
                failure_code=self._failure_code,
            )

    def wait(self, timeout_seconds: float | None = None) -> ReplaySnapshot:
        """Wait for a terminal state without requesting pause or stop."""

        timeout = self._validate_timeout(timeout_seconds)
        worker: Thread | None
        with self._condition:
            if self._is_worker_thread():
                raise RuntimeError("replay worker cannot wait for itself")
            if self._state is ReplayState.READY:
                raise RuntimeError("replay runtime has not been started")
            completed = self._condition.wait_for(lambda: self._state.is_terminal, timeout)
            if not completed:
                raise TimeoutError("replay runtime did not finish before timeout")
            worker = self._worker
        if worker is not None:
            worker.join()
        return self.snapshot()

    @staticmethod
    def _validate_timeout(timeout_seconds: float | None) -> float | None:
        if timeout_seconds is None:
            return None
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)):
            raise TypeError("timeout_seconds must be a real number or None")
        timeout = float(timeout_seconds)
        if not math.isfinite(timeout) or timeout < 0:
            raise ValueError("timeout_seconds must be finite and non-negative")
        return timeout

    def _is_worker_thread(self) -> bool:
        return self._worker is current_thread()

    def _run(self) -> None:
        try:
            iterator = self._open_source()
            if iterator is None:
                return
            self._publish_all(iterator)
        except Exception:
            self._fail(ReplayFailureCode.INTERNAL_FAILED)

    def _open_source(self) -> Iterator[MarketDataEnvelope] | None:
        try:
            self._source.open(self._descriptor.session_id)
        except Exception:
            self._fail(ReplayFailureCode.SOURCE_FAILED)
            return None
        try:
            report = self._source.verify_integrity()
        except Exception:
            self._fail(ReplayFailureCode.INTEGRITY_FAILED)
            return None
        if (
            not report.is_valid
            or report.session_id != self._descriptor.session_id
            or report.event_count != self._descriptor.event_count
            or report.first_ingest_sequence != self._descriptor.first_ingest_sequence
            or report.last_ingest_sequence != self._descriptor.last_ingest_sequence
        ):
            self._fail(ReplayFailureCode.INTEGRITY_FAILED)
            return None
        if not self._prepare_expected_count():
            return None
        try:
            return iter(
                self._source.iter_events(after_ingest_sequence=self._options.after_ingest_sequence)
            )
        except Exception:
            self._fail(ReplayFailureCode.SOURCE_FAILED)
            return None

    def _prepare_expected_count(self) -> bool:
        """Validate a full ordered plan and count events after the cursor."""

        try:
            iterator = iter(self._source.iter_events(after_ingest_sequence=None))
            count = 0
            expected = 0
            first: int | None = None
            last: int | None = None
            previous: int | None = None
            cursor = self._options.after_ingest_sequence
            for envelope in iterator:
                if (
                    type(envelope) is not MarketDataEnvelope
                    or envelope.session_id != self._descriptor.session_id
                    or (previous is not None and envelope.ingest_sequence <= previous)
                ):
                    self._fail(ReplayFailureCode.INTEGRITY_FAILED)
                    return False
                if first is None:
                    first = envelope.ingest_sequence
                last = envelope.ingest_sequence
                previous = envelope.ingest_sequence
                count += 1
                if cursor is None or envelope.ingest_sequence > cursor:
                    expected += 1
        except Exception:
            self._fail(ReplayFailureCode.SOURCE_FAILED)
            return False
        if (
            count != self._descriptor.event_count
            or first != self._descriptor.first_ingest_sequence
            or last != self._descriptor.last_ingest_sequence
        ):
            self._fail(ReplayFailureCode.INTEGRITY_FAILED)
            return False
        self._expected_emitted_count = expected
        return True

    def _publish_all(self, iterator: Iterator[MarketDataEnvelope]) -> None:
        anchor = None
        previous_sequence = self._options.after_ingest_sequence
        while self._acknowledge_controls():
            try:
                envelope = next(iterator)
            except StopIteration:
                self._finish_or_fail()
                return
            except Exception:
                self._fail(ReplayFailureCode.SOURCE_FAILED)
                return

            if (
                type(envelope) is not MarketDataEnvelope
                or envelope.session_id != self._descriptor.session_id
                or envelope.ingest_sequence < self._descriptor.first_ingest_sequence
                or envelope.ingest_sequence > self._descriptor.last_ingest_sequence
                or (previous_sequence is not None and envelope.ingest_sequence <= previous_sequence)
            ):
                self._fail(ReplayFailureCode.SOURCE_FAILED)
                return

            if self._options.mode is ReplayMode.PACED and anchor is not None:
                event_at = envelope.event_at
                if event_at is not None:
                    delay = max(0.0, (event_at - anchor).total_seconds())
                    delay /= self._options.speed
                    if delay > 0 and not self._wait(delay):
                        return

            if not self._acknowledge_controls():
                return
            try:
                self._sink.publish(envelope)
            except Exception:
                self._fail(ReplayFailureCode.SINK_FAILED)
                return

            with self._condition:
                self._cursor = envelope.ingest_sequence
                self._emitted_count += 1
            previous_sequence = envelope.ingest_sequence
            if envelope.event_at is not None:
                anchor = envelope.event_at

    def _wait(self, delay_seconds: float) -> bool:
        remaining = delay_seconds
        while remaining > 0:
            if not self._acknowledge_controls():
                return False
            with self._condition:
                self._wake_event.clear()
                if self._stop_requested or self._pause_requested:
                    continue
            try:
                started_at = self._timer.monotonic()
                interrupted = self._timer.wait(remaining, self._wake_event)
                ended_at = self._timer.monotonic()
            except Exception:
                self._fail(ReplayFailureCode.TIMER_FAILED)
                return False
            if not interrupted:
                return True
            remaining = max(0.0, remaining - max(0.0, ended_at - started_at))
        return True

    def _acknowledge_controls(self) -> bool:
        with self._condition:
            if self._stop_requested:
                self._state = ReplayState.STOPPED
                self._condition.notify_all()
                return False
            if self._pause_requested:
                self._state = ReplayState.PAUSED
                self._condition.notify_all()
                self._condition.wait_for(lambda: not self._pause_requested or self._stop_requested)
                if self._stop_requested:
                    self._state = ReplayState.STOPPED
                    self._condition.notify_all()
                    return False
                self._state = ReplayState.RUNNING
                self._condition.notify_all()
            return True

    def _finish_or_fail(self) -> None:
        with self._condition:
            if self._stop_requested:
                self._state = ReplayState.STOPPED
            elif (
                self._cursor != self._descriptor.last_ingest_sequence
                or self._emitted_count != self._expected_emitted_count
            ):
                self._state = ReplayState.FAILED
                self._failure_code = ReplayFailureCode.INTEGRITY_FAILED
            else:
                self._state = ReplayState.COMPLETED
            self._condition.notify_all()

    def _fail(self, code: ReplayFailureCode) -> None:
        with self._condition:
            if not self._state.is_terminal:
                self._state = ReplayState.FAILED
                self._failure_code = code
                self._condition.notify_all()
