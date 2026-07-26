from dataclasses import FrozenInstanceError
from datetime import datetime
from threading import Event, get_ident
from time import monotonic
from uuid import UUID

import pytest

from tx_trade.broker.capital.contracts import (
    AdapterStoppedError,
    AuthenticationError,
    CommandQueueFullError,
    LiveQuoteInitializationError,
    MonitorError,
    QuoteSnapshotRaw,
    ReadyTimeoutError,
    ReconnectPolicy,
)
from tx_trade.broker.capital.quote_adapter import CapitalQuoteStaAdapter, _Command
from tx_trade.market_data.ingress import BoundedIngress, BoundedStaQuoteQueue
from tx_trade.market_data.models import ConnectionState, TAIPEI
from tx_trade.monitoring.health import (
    ControlledShutdown,
    PipelineHealth,
    SessionImpactTracker,
)
from tx_trade.monitoring.metrics import IngressMetrics

NOW = datetime(2026, 7, 26, 9, 30, tzinfo=TAIPEI)
SESSION = UUID("12345678-1234-5678-1234-567812345678")


class Clock:
    def now(self): return NOW
    def monotonic(self): return monotonic()


class ManualClock:
    def __init__(self):
        self.value = 0.0

    def now(self): return NOW
    def monotonic(self): return self.value
    def advance(self, seconds): self.value += seconds


class FakeBackend:
    def __init__(self, init_error=None):
        self.calls = []
        self.sink = None
        self.actions = []
        self.init_error = init_error
        self.login_result = 0
        self.enter_results = []
        self.leave_results = []
        self.initialize_blocker = None

    def _call(self, name, *args):
        self.calls.append((name, get_ident(), args))

    def co_initialize(self): self._call("co_initialize")
    def initialize(self, path):
        self._call("initialize", path)
        if self.initialize_blocker is not None:
            self.initialize_blocker.wait()
        if self.init_error: raise self.init_error
    def register_events(self, sink):
        self._call("register_events")
        self.sink = sink
    def login(self, account, password):
        self._call("login", account, password)
        if isinstance(self.login_result, Exception): raise self.login_result
        return self.login_result
    def enter_monitor(self):
        self._call("enter_monitor")
        result = self.enter_results.pop(0) if self.enter_results else 0
        if isinstance(result, Exception): raise result
        return result
    def leave_monitor(self):
        self._call("leave_monitor")
        result = self.leave_results.pop(0) if self.leave_results else 0
        if isinstance(result, Exception): raise result
        return result
    def request_quotes(self, csv): self._call("request_quotes", csv); return 0
    def request_ticks(self, csv): self._call("request_ticks", csv); return 0
    def cancel_quotes(self, csv): self._call("cancel_quotes", csv); return 0
    def cancel_ticks(self, csv): self._call("cancel_ticks", csv); return 0
    def lookup_quote(self, market, index):
        self._call("lookup_quote", market, index)
        return QuoteSnapshotRaw(1, 2, 1, 3, 4, None, 5)
    def pump_waiting_messages(self):
        self._call("pump")
        if self.actions:
            self.actions.pop(0)(self.sink)
    def release_events(self): self._call("release_events")
    def release_objects(self): self._call("release_objects")
    def co_uninitialize(self): self._call("co_uninitialize")


def make_adapter(
    backend, command_capacity=2, command_timeout=1, clock=None, **changes
):
    clock = clock or Clock()
    health = PipelineHealth(clock)
    metrics = IngressMetrics()
    impact = SessionImpactTracker(2)
    shutdown = ControlledShutdown()
    ingress = BoundedIngress(
        control_capacity=20, diagnostic_capacity=20, quote_capacity=20,
        tick_capacity=20, dedupe_capacity=20, health=health, metrics=metrics,
        session_impact=impact, shutdown=shutdown,
    )
    sta = BoundedStaQuoteQueue(
        4, health=health, metrics=metrics, session_impact=impact,
        shutdown=shutdown, session_id=SESSION,
    )
    values = dict(
        backend=backend, dll_path="fixture.dll", ingress=ingress,
        sta_queue=sta, clock=clock, health=health, session_impact=impact,
        shutdown=shutdown, command_capacity=command_capacity,
        command_timeout=command_timeout, pump_interval=.001,
        reconnect_policy=ReconnectPolicy(2, (0.0, 0.0)),
        quote_lookup_attempts=1, session_id=SESSION,
    )
    values.update(changes)
    adapter = CapitalQuoteStaAdapter(**values)
    return adapter, ingress


def wait_for(predicate, timeout=.5):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate(): return
    raise AssertionError("condition was not reached")


def test_all_backend_calls_use_one_dedicated_thread_and_cleanup_order():
    backend = FakeBackend()
    adapter, _ = make_adapter(backend)
    caller = get_ident()
    adapter.start()
    adapter.login("acct", "secret")
    adapter.enter_monitor()
    adapter.stop(1)
    ids = {thread_id for _, thread_id, _ in backend.calls}
    assert len(ids) == 1 and caller not in ids
    names = [name for name, *_ in backend.calls]
    assert names[-3:] == ["release_events", "release_objects", "co_uninitialize"]
    adapter.stop(1)
    assert adapter.snapshot().state is ConnectionState.STOPPED


def test_command_is_immutable_and_repr_redacts_values():
    command = _Command("login", ("account", "secret"), __import__("concurrent.futures").futures.Future())
    assert "account" not in repr(command) and "secret" not in repr(command)
    with pytest.raises(FrozenInstanceError):
        command.operation = "x"


def test_full_command_queue_fails_immediately_and_cancelled_command_is_skipped():
    backend = FakeBackend()
    adapter, _ = make_adapter(backend, command_capacity=1)
    adapter._accepting_commands = True
    first = _Command("enter_monitor", (), __import__("concurrent.futures").futures.Future())
    adapter._commands.put_nowait(first)
    with pytest.raises(CommandQueueFullError):
        adapter.enter_monitor()
    assert first.future.cancel()
    adapter._process_commands()
    assert not any(name == "enter_monitor" for name, *_ in backend.calls)


def test_stop_before_start_is_terminal_and_restart_is_forbidden():
    adapter, _ = make_adapter(FakeBackend())
    adapter.stop(1)
    assert adapter.snapshot().state is ConnectionState.STOPPED
    with pytest.raises(Exception):
        adapter.start()
    with pytest.raises(AdapterStoppedError):
        adapter.login("a", "b")


@pytest.mark.parametrize("failure", [7, RuntimeError("sensitive")])
def test_login_failure_is_typed_and_cleans_up(failure):
    backend = FakeBackend()
    backend.login_result = failure
    adapter, _ = make_adapter(backend)
    adapter.start()
    with pytest.raises(AuthenticationError) as caught:
        adapter.login("acct", "secret")
    assert "secret" not in repr(caught.value)
    wait_for(lambda: adapter.snapshot().state is ConnectionState.STOPPED)
    names = [name for name, *_ in backend.calls]
    assert names[-3:] == ["release_events", "release_objects", "co_uninitialize"]
    assert adapter._shutdown.snapshot().reason == "authentication_failure"


@pytest.mark.parametrize("failure", [8, RuntimeError("sensitive")])
def test_initial_monitor_failure_is_typed_and_cleans_partial_state(failure):
    backend = FakeBackend()
    backend.enter_results = [failure]
    adapter, _ = make_adapter(backend)
    adapter.start()
    adapter.login("acct", "secret")
    with pytest.raises(MonitorError):
        adapter.enter_monitor()
    wait_for(lambda: adapter.snapshot().state is ConnectionState.STOPPED)
    names = [name for name, *_ in backend.calls]
    assert names.index("leave_monitor") < max(
        index for index, name in enumerate(names) if name == "release_events"
    )
    assert names[-1] == "co_uninitialize"


def test_start_timeout_returns_before_blocked_backend_then_cleans_up():
    blocker = Event()
    backend = FakeBackend()
    backend.initialize_blocker = blocker
    adapter, _ = make_adapter(backend, startup_timeout=.02)
    try:
        with pytest.raises(LiveQuoteInitializationError, match="timed out"):
            adapter.start()
        assert adapter.snapshot().state is ConnectionState.ERROR
        assert adapter._shutdown.snapshot().reason == "quote_startup_timeout"
    finally:
        blocker.set()
    adapter.stop(1)
    assert adapter.snapshot().state is ConnectionState.STOPPED
    assert [name for name, *_ in backend.calls][-2:] == [
        "release_objects",
        "co_uninitialize",
    ]


def test_ready_timeout_is_typed_and_terminal():
    backend = FakeBackend()
    adapter, _ = make_adapter(backend)
    adapter.start()
    adapter.login("acct", "secret")
    adapter.enter_monitor()
    with pytest.raises(ReadyTimeoutError):
        adapter.wait_until_ready(.02)
    wait_for(lambda: adapter.snapshot().state is ConnectionState.STOPPED)
    assert adapter._shutdown.snapshot().reason == "quote_ready_timeout"


@pytest.mark.parametrize("reach_connected", [False, True])
def test_reconnect_watchdogs_retry_then_exhaust(reach_connected):
    clock = ManualClock()
    backend = FakeBackend()
    adapter, _ = make_adapter(
        backend,
        clock=clock,
        reconnect_connected_timeout=1,
        reconnect_stocks_ready_timeout=1,
    )
    adapter.start()
    adapter.login("acct", "secret")
    adapter.enter_monitor()
    backend.actions.extend([
        lambda sink: sink.OnConnection(3001, 0),
        lambda sink: sink.OnConnection(3003, 0),
    ])
    adapter.wait_until_ready(1)
    backend.actions.append(lambda sink: sink.OnConnection(3002, 0))
    wait_for(lambda: adapter.snapshot().generation == 2)
    if reach_connected:
        backend.actions.append(lambda sink: sink.OnConnection(3001, 0))
        wait_for(lambda: adapter.snapshot().state is ConnectionState.CONNECTED)
    clock.advance(2)
    wait_for(lambda: adapter.snapshot().generation == 3)
    if reach_connected:
        backend.actions.append(lambda sink: sink.OnConnection(3001, 0))
        wait_for(lambda: adapter.snapshot().state is ConnectionState.CONNECTED)
    clock.advance(2)
    wait_for(lambda: adapter.snapshot().state is ConnectionState.STOPPED)
    assert [name for name, *_ in backend.calls].count("enter_monitor") == 3
    assert adapter._shutdown.snapshot().reason == "reconnect_exhausted"
    assert adapter._impact.snapshot(SESSION).is_incomplete


def test_stop_cancels_reconnect_watchdog():
    clock = ManualClock()
    backend = FakeBackend()
    adapter, _ = make_adapter(
        backend, clock=clock, reconnect_connected_timeout=1
    )
    adapter.start()
    adapter.login("acct", "secret")
    adapter.enter_monitor()
    backend.actions.extend([
        lambda sink: sink.OnConnection(3001, 0),
        lambda sink: sink.OnConnection(3003, 0),
        lambda sink: sink.OnConnection(3002, 0),
    ])
    wait_for(lambda: adapter.snapshot().generation == 2)
    adapter.stop(1)
    count = [name for name, *_ in backend.calls].count("enter_monitor")
    clock.advance(100)
    assert [name for name, *_ in backend.calls].count("enter_monitor") == count


def test_reconnect_leave_nonzero_consumes_attempt_before_new_enter():
    clock = ManualClock()
    backend = FakeBackend()
    backend.leave_results = [9, 0]
    adapter, _ = make_adapter(backend, clock=clock)
    adapter.start()
    adapter.login("acct", "secret")
    adapter.enter_monitor()
    backend.actions.extend([
        lambda sink: sink.OnConnection(3001, 0),
        lambda sink: sink.OnConnection(3003, 0),
        lambda sink: sink.OnConnection(3002, 0),
    ])
    wait_for(lambda: adapter.snapshot().generation == 2)
    assert [name for name, *_ in backend.calls].count("leave_monitor") >= 2
    assert adapter._impact.snapshot(SESSION).is_incomplete
    adapter.stop(1)
