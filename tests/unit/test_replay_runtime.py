from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from threading import Event
from time import monotonic, sleep
from uuid import UUID

import pytest

from tx_trade.market_data.fixtures import (
    InMemoryReplaySource,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import SCHEMA_VERSION, Instrument, MarketDataEnvelope
from tx_trade.replay.contracts import (
    ReplayError,
    ReplayFailureCode,
    ReplayMode,
    ReplayOptions,
    ReplaySessionDescriptor,
    ReplayState,
)
from tx_trade.replay.runtime import ReplayRuntime


class CollectingSink:
    def __init__(self) -> None:
        self.envelopes: list[MarketDataEnvelope] = []

    def publish(self, envelope: MarketDataEnvelope) -> None:
        self.envelopes.append(envelope)


class RecordingTimer:
    def __init__(self) -> None:
        self.delays: list[float] = []

    def monotonic(self) -> float:
        return 0.0

    def wait(self, delay_seconds: float, wake_event: Event) -> bool:
        self.delays.append(delay_seconds)
        return False


def _events(
    offsets: tuple[float | None, ...] = (0.0, 2.0, 1.0, None, 5.0),
) -> tuple[MarketDataEnvelope, ...]:
    base = make_offline_fixture_envelopes()[2]
    no_time_base = make_offline_fixture_envelopes()[5]
    assert isinstance(base.payload, Instrument)
    values: list[MarketDataEnvelope] = []
    for index, offset in enumerate(offsets):
        if offset is None:
            values.append(
                replace(
                    no_time_base,
                    ingest_sequence=index * 2,
                    dedupe_key=f"replay-test-{index}",
                )
            )
            continue
        event_at = base.event_at + timedelta(seconds=offset)  # type: ignore[operator]
        payload = replace(base.payload, updated_at=event_at)
        values.append(
            replace(
                base,
                payload=payload,
                ingest_sequence=index * 2,
                sequence=index,
                dedupe_key=f"replay-test-{index}",
                event_at=event_at,
            )
        )
    return tuple(values)


def _descriptor(events: tuple[MarketDataEnvelope, ...]) -> ReplaySessionDescriptor:
    return ReplaySessionDescriptor(
        session_id=events[0].session_id,
        status="complete",
        schema_version=SCHEMA_VERSION,
        event_count=len(events),
        first_ingest_sequence=events[0].ingest_sequence,
        last_ingest_sequence=events[-1].ingest_sequence,
    )


def _runtime(
    events: tuple[MarketDataEnvelope, ...],
    *,
    sink: object | None = None,
    options: ReplayOptions | None = None,
    timer: object | None = None,
    source: object | None = None,
) -> tuple[ReplayRuntime, CollectingSink]:
    collector = CollectingSink() if sink is None else sink
    runtime = ReplayRuntime(
        source=InMemoryReplaySource(events) if source is None else source,  # type: ignore[arg-type]
        descriptor=_descriptor(events),
        sink=collector,  # type: ignore[arg-type]
        options=options or ReplayOptions(mode=ReplayMode.FASTEST),
        timer=timer,  # type: ignore[arg-type]
    )
    return runtime, collector  # type: ignore[return-value]


def _wait_terminal(runtime: ReplayRuntime) -> None:
    deadline = monotonic() + 2
    while not runtime.snapshot().state.is_terminal and monotonic() < deadline:
        sleep(0.001)
    assert runtime.snapshot().state.is_terminal


def test_fastest_replays_deterministically_without_waiting() -> None:
    events = _events()
    timer = RecordingTimer()
    runtime, sink = _runtime(events, timer=timer)

    runtime.start()
    _wait_terminal(runtime)

    assert sink.envelopes == list(events)
    assert timer.delays == []
    assert runtime.snapshot().state is ReplayState.COMPLETED
    assert runtime.snapshot().cursor == events[-1].ingest_sequence
    assert runtime.snapshot().emitted_count == len(events)


def test_wait_returns_terminal_snapshot_without_requesting_stop() -> None:
    events = _events()
    runtime, sink = _runtime(events)

    runtime.start()
    snapshot = runtime.wait(2)

    assert snapshot.state is ReplayState.COMPLETED
    assert snapshot.cursor == events[-1].ingest_sequence
    assert sink.envelopes == list(events)


def test_run_executes_synchronously_and_propagates_interrupt_after_stopping() -> None:
    events = _events()
    runtime: ReplayRuntime

    class InterruptingSink(CollectingSink):
        def publish(self, envelope: MarketDataEnvelope) -> None:
            raise KeyboardInterrupt

    runtime, _ = _runtime(events, sink=InterruptingSink())

    with pytest.raises(KeyboardInterrupt):
        runtime.run()

    assert runtime.snapshot().state is ReplayState.STOPPED
    assert runtime.snapshot().cursor is None
    with pytest.raises(RuntimeError, match="single-use"):
        runtime.run()


def test_wait_timeout_does_not_request_stop() -> None:
    events = _events()
    entered = Event()
    release = Event()

    class BlockingSink(CollectingSink):
        def publish(self, envelope: MarketDataEnvelope) -> None:
            entered.set()
            assert release.wait(2)
            super().publish(envelope)

    runtime, _ = _runtime(events, sink=BlockingSink())
    runtime.start()
    assert entered.wait(2)

    with pytest.raises(TimeoutError, match="did not finish"):
        runtime.wait(0)
    assert runtime.snapshot().state is ReplayState.RUNNING

    release.set()
    assert runtime.wait(2).state is ReplayState.COMPLETED


def test_wait_before_start_is_rejected_without_blocking() -> None:
    runtime, _ = _runtime(_events())

    with pytest.raises(RuntimeError, match="not been started"):
        runtime.wait()

    assert runtime.snapshot().state is ReplayState.READY
    runtime.stop()
    assert runtime.wait().state is ReplayState.STOPPED


def test_paced_uses_positive_event_deltas_and_preserves_anchor_across_none() -> None:
    events = _events()
    timer = RecordingTimer()
    runtime, sink = _runtime(
        events,
        options=ReplayOptions(mode=ReplayMode.PACED, speed=2),
        timer=timer,
    )

    runtime.start()
    _wait_terminal(runtime)

    assert sink.envelopes == list(events)
    assert timer.delays == pytest.approx([1.0, 2.0])


def test_initial_cursor_is_exclusive_and_sequence_gaps_are_allowed() -> None:
    events = _events()
    runtime, sink = _runtime(
        events,
        options=ReplayOptions(
            mode=ReplayMode.FASTEST,
            after_ingest_sequence=events[1].ingest_sequence,
        ),
    )

    runtime.start()
    _wait_terminal(runtime)

    assert sink.envelopes == list(events[2:])
    assert runtime.snapshot().cursor == events[-1].ingest_sequence


def test_cursor_equal_to_last_completes_after_integrity_verification() -> None:
    events = _events()

    runtime, sink = _runtime(
        events,
        options=ReplayOptions(
            mode=ReplayMode.FASTEST,
            after_ingest_sequence=events[-1].ingest_sequence,
        ),
    )

    runtime.start()
    _wait_terminal(runtime)

    assert runtime.snapshot().state is ReplayState.COMPLETED
    assert sink.envelopes == []


@pytest.mark.parametrize(
    ("descriptor_changes", "code"),
    [
        ({"status": "recording"}, ReplayFailureCode.SESSION_NOT_COMPLETE),
        ({"schema_version": SCHEMA_VERSION + 1}, ReplayFailureCode.SCHEMA_MISMATCH),
    ],
)
def test_constructor_rejects_ineligible_descriptor(
    descriptor_changes: dict[str, object], code: ReplayFailureCode
) -> None:
    events = _events()
    with pytest.raises(ReplayError) as caught:
        ReplayRuntime(
            source=InMemoryReplaySource(events),
            descriptor=replace(_descriptor(events), **descriptor_changes),
            sink=CollectingSink(),
            options=ReplayOptions(mode=ReplayMode.FASTEST),
        )
    assert caught.value.code is code


def test_constructor_rejects_cursor_beyond_last() -> None:
    events = _events()
    with pytest.raises(ReplayError) as caught:
        ReplayRuntime(
            source=InMemoryReplaySource(events),
            descriptor=_descriptor(events),
            sink=CollectingSink(),
            options=ReplayOptions(
                mode=ReplayMode.FASTEST,
                after_ingest_sequence=events[-1].ingest_sequence + 1,
            ),
        )
    assert caught.value.code is ReplayFailureCode.CURSOR_OUT_OF_RANGE


def test_pause_and_resume_acknowledge_without_publishing_while_paused() -> None:
    events = _events()
    first_entered = Event()
    release_first = Event()

    class BlockingSink(CollectingSink):
        def publish(self, envelope: MarketDataEnvelope) -> None:
            super().publish(envelope)
            if len(self.envelopes) == 1:
                first_entered.set()
                assert release_first.wait(2)

    sink = BlockingSink()
    runtime, _ = _runtime(events, sink=sink)
    runtime.start()
    assert first_entered.wait(2)

    pause_returned = Event()

    def request_pause() -> None:
        runtime.pause()
        pause_returned.set()

    from threading import Thread

    pauser = Thread(target=request_pause)
    pauser.start()
    assert not pause_returned.wait(0.02)
    release_first.set()
    assert pause_returned.wait(2)
    assert runtime.snapshot().state is ReplayState.PAUSED
    paused_count = len(sink.envelopes)
    sleep(0.02)
    assert len(sink.envelopes) == paused_count

    runtime.resume()
    _wait_terminal(runtime)
    pauser.join()
    assert sink.envelopes == list(events)


def test_stop_is_acknowledged_and_idempotent() -> None:
    events = _events()
    entered = Event()
    release = Event()

    class BlockingSink(CollectingSink):
        def publish(self, envelope: MarketDataEnvelope) -> None:
            super().publish(envelope)
            entered.set()
            assert release.wait(2)

    sink = BlockingSink()
    runtime, _ = _runtime(events, sink=sink)
    runtime.start()
    assert entered.wait(2)

    stopped = Event()

    def request_stop() -> None:
        runtime.stop()
        stopped.set()

    from threading import Thread

    stopper = Thread(target=request_stop)
    stopper.start()
    assert not stopped.wait(0.02)
    release.set()
    assert stopped.wait(2)
    assert runtime.snapshot().state is ReplayState.STOPPED
    count = len(sink.envelopes)
    sleep(0.02)
    assert len(sink.envelopes) == count
    runtime.stop()
    stopper.join()


def test_sink_failure_keeps_cursor_before_failed_event_and_is_sanitized() -> None:
    events = _events()

    class FailingSink(CollectingSink):
        def publish(self, envelope: MarketDataEnvelope) -> None:
            if envelope is events[1]:
                raise RuntimeError("secret sink detail")
            super().publish(envelope)

    runtime, _ = _runtime(events, sink=FailingSink())
    runtime.start()
    _wait_terminal(runtime)
    snapshot = runtime.snapshot()

    assert snapshot.state is ReplayState.FAILED
    assert snapshot.failure_code is ReplayFailureCode.SINK_FAILED
    assert snapshot.cursor == events[0].ingest_sequence
    assert "secret" not in repr(snapshot)


@pytest.mark.parametrize("failure_at", ["open", "verify", "iterate"])
def test_source_failure_is_sanitized(failure_at: str) -> None:
    events = _events()

    class FailingSource:
        def open(self, session_id: UUID) -> None:
            if failure_at == "open":
                raise RuntimeError("secret source detail")

        def verify_integrity(self):
            if failure_at == "verify":
                raise RuntimeError("secret integrity detail")
            source = InMemoryReplaySource(events)
            source.open(events[0].session_id)
            return source.verify_integrity()

        def iter_events(self, *, after_ingest_sequence: int | None = None):
            if failure_at == "iterate":
                raise RuntimeError("secret source detail")
            return iter(events)

    runtime, _ = _runtime(events, source=FailingSource())
    runtime.start()
    _wait_terminal(runtime)
    snapshot = runtime.snapshot()

    assert snapshot.state is ReplayState.FAILED
    expected = (
        ReplayFailureCode.INTEGRITY_FAILED
        if failure_at == "verify"
        else ReplayFailureCode.SOURCE_FAILED
    )
    assert snapshot.failure_code is expected
    assert snapshot.cursor is None
    assert "secret" not in repr(snapshot)


def test_integrity_failure_prevents_any_publish() -> None:
    events = _events()

    class InvalidSource(InMemoryReplaySource):
        def verify_integrity(self):
            raise RuntimeError("secret integrity detail")

    runtime, sink = _runtime(events, source=InvalidSource(events))
    runtime.start()
    _wait_terminal(runtime)

    snapshot = runtime.snapshot()
    assert snapshot.state is ReplayState.FAILED
    assert snapshot.failure_code is ReplayFailureCode.INTEGRITY_FAILED
    assert snapshot.cursor is None
    assert sink.envelopes == []
    assert "secret" not in repr(snapshot)


def test_truncated_iterator_cannot_complete() -> None:
    events = _events()

    class TruncatedSource(InMemoryReplaySource):
        def __init__(self, values):
            super().__init__(values)
            self.iteration_count = 0

        def iter_events(self, *, after_ingest_sequence: int | None = None):
            self.iteration_count += 1
            if self.iteration_count == 1:
                return super().iter_events(after_ingest_sequence=after_ingest_sequence)
            return iter(events[:2])

    runtime, sink = _runtime(events, source=TruncatedSource(events))
    runtime.start()
    _wait_terminal(runtime)

    snapshot = runtime.snapshot()
    assert sink.envelopes == list(events[:2])
    assert snapshot.state is ReplayState.FAILED
    assert snapshot.failure_code is ReplayFailureCode.INTEGRITY_FAILED
    assert snapshot.cursor == events[1].ingest_sequence


@pytest.mark.parametrize("returned_indexes", [(4,), (2, 4)])
def test_resume_iterator_cannot_omit_events_and_still_complete(
    returned_indexes: tuple[int, ...],
) -> None:
    events = _events()
    cursor = events[1].ingest_sequence

    class OmissionSource(InMemoryReplaySource):
        def iter_events(self, *, after_ingest_sequence: int | None = None):
            if after_ingest_sequence is None:
                return super().iter_events(after_ingest_sequence=None)
            return iter(tuple(events[index] for index in returned_indexes))

    runtime, sink = _runtime(
        events,
        source=OmissionSource(events),
        options=ReplayOptions(
            mode=ReplayMode.FASTEST,
            after_ingest_sequence=cursor,
        ),
    )
    runtime.start()
    _wait_terminal(runtime)

    snapshot = runtime.snapshot()
    assert sink.envelopes == [events[index] for index in returned_indexes]
    assert snapshot.state is ReplayState.FAILED
    assert snapshot.failure_code is ReplayFailureCode.INTEGRITY_FAILED


def test_timer_failure_keeps_cursor_at_last_success() -> None:
    events = _events((0.0, 2.0))

    class FailingTimer(RecordingTimer):
        def wait(self, delay_seconds: float, wake_event: Event) -> bool:
            raise RuntimeError("secret timer detail")

    runtime, sink = _runtime(
        events,
        options=ReplayOptions(mode=ReplayMode.PACED),
        timer=FailingTimer(),
    )
    runtime.start()
    _wait_terminal(runtime)
    snapshot = runtime.snapshot()

    assert sink.envelopes == [events[0]]
    assert snapshot.state is ReplayState.FAILED
    assert snapshot.failure_code is ReplayFailureCode.TIMER_FAILED
    assert snapshot.cursor == events[0].ingest_sequence


@pytest.mark.parametrize("control", ["pause", "stop"])
def test_callback_can_request_control_without_self_joining(control: str) -> None:
    events = _events()
    callback_returned = Event()
    callback_states: list[ReplayState] = []
    runtime: ReplayRuntime

    class ControllingSink(CollectingSink):
        def publish(self, envelope: MarketDataEnvelope) -> None:
            super().publish(envelope)
            getattr(runtime, control)()
            callback_states.append(runtime.snapshot().state)
            callback_returned.set()

    sink = ControllingSink()
    runtime, _ = _runtime(events, sink=sink)
    runtime.start()
    assert callback_returned.wait(2)
    assert callback_states == [ReplayState.RUNNING]

    if control == "pause":
        deadline = monotonic() + 2
        while runtime.snapshot().state is not ReplayState.PAUSED and monotonic() < deadline:
            sleep(0.001)
        assert runtime.snapshot().state is ReplayState.PAUSED
        runtime.stop()
    else:
        _wait_terminal(runtime)
        assert runtime.snapshot().state is ReplayState.STOPPED


def test_runtime_cannot_restart_after_terminal_state() -> None:
    runtime, _ = _runtime(_events())
    runtime.start()
    _wait_terminal(runtime)
    with pytest.raises(RuntimeError, match="single-use"):
        runtime.start()
