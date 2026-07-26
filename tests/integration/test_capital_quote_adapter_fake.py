from datetime import datetime
from threading import get_ident
from time import monotonic
from uuid import UUID

from tx_trade.broker.capital.contracts import QuoteSnapshotRaw, ReconnectPolicy
from tx_trade.broker.capital.quote_adapter import CapitalQuoteStaAdapter
from tx_trade.market_data.ingress import BoundedIngress, BoundedStaQuoteQueue
from tx_trade.market_data.models import CapturedKind, ConnectionState, TAIPEI
from tx_trade.monitoring.health import (
    ControlledShutdown, PipelineHealth, SessionImpactTracker,
)
from tx_trade.monitoring.metrics import IngressMetrics

NOW = datetime(2026, 7, 26, 9, 30, tzinfo=TAIPEI)
SESSION = UUID("12345678-1234-5678-1234-567812345678")


class Clock:
    def now(self): return NOW
    def monotonic(self): return monotonic()


class Fake:
    def __init__(self):
        self.calls = []
        self.actions = []
        self.sink = None
        self.lookup_during_callback = None
        self.lookup_count = 0
        self.tick_results = []

    def record(self, name, *args): self.calls.append((name, get_ident(), args))
    def co_initialize(self): self.record("co_initialize")
    def initialize(self, path): self.record("initialize", path)
    def register_events(self, sink): self.record("register_events"); self.sink = sink
    def login(self, account, password): self.record("login", account, password); return 0
    def enter_monitor(self): self.record("enter_monitor"); return 0
    def leave_monitor(self): self.record("leave_monitor"); return 0
    def request_quotes(self, value): self.record("request_quotes", value); return 0
    def request_ticks(self, value):
        self.record("request_ticks", value)
        return self.tick_results.pop(0) if self.tick_results else 0
    def cancel_quotes(self, value): self.record("cancel_quotes", value); return 0
    def cancel_ticks(self, value): self.record("cancel_ticks", value); return 0
    def lookup_quote(self, market, index):
        self.lookup_count += 1
        self.record("lookup_quote", market, index)
        return QuoteSnapshotRaw(10, 12, 11, 1, 2, None, 20, "TX00", None)
    def pump_waiting_messages(self):
        self.record("pump")
        if self.actions:
            self.actions.pop(0)()
    def release_events(self): self.record("release_events")
    def release_objects(self): self.record("release_objects")
    def co_uninitialize(self): self.record("co_uninitialize")


def setup():
    backend, clock = Fake(), Clock()
    health, metrics = PipelineHealth(clock), IngressMetrics()
    impact, shutdown = SessionImpactTracker(2), ControlledShutdown()
    ingress = BoundedIngress(
        control_capacity=50, diagnostic_capacity=50, quote_capacity=50,
        tick_capacity=50, dedupe_capacity=50, health=health, metrics=metrics,
        session_impact=impact, shutdown=shutdown,
    )
    sta = BoundedStaQuoteQueue(
        5, health=health, metrics=metrics, session_impact=impact,
        shutdown=shutdown, session_id=SESSION,
    )
    adapter = CapitalQuoteStaAdapter(
        backend=backend, dll_path="fixture.dll", ingress=ingress,
        sta_queue=sta, clock=clock, health=health, session_impact=impact,
        shutdown=shutdown, command_capacity=5, command_timeout=1,
        pump_interval=.001, reconnect_policy=ReconnectPolicy(2, (0., 0.)),
        quote_lookup_attempts=1, session_id=SESSION,
    )
    return adapter, backend, ingress


def wait_for(predicate, timeout=1):
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate(): return
    raise AssertionError("condition not reached")


def test_ready_sorted_subscription_quote_enrichment_and_reconnect_resubscribe():
    adapter, backend, ingress = setup()
    adapter.start()
    adapter.login("acct", "secret")
    adapter.subscribe_quotes(["TX01", "TX00", "TX01"])
    adapter.subscribe_ticks(["TX00"])
    backend.actions.extend([
        lambda: backend.sink.OnConnection(3001, 0),
        lambda: backend.sink.OnConnection(3003, 0),
    ])
    adapter.enter_monitor()
    status = adapter.wait_until_ready(1)
    assert status.connection_generation == 1
    wait_for(lambda: adapter.snapshot().state is ConnectionState.SUBSCRIBED)
    calls = [(name, args) for name, _, args in backend.calls]
    assert calls.count(("request_quotes", ("TX00,TX01",))) == 1
    assert calls.count(("request_ticks", ("TX00",))) == 1

    def quote_callback():
        backend.sink.OnNotifyQuoteLONG(0, 7)
        backend.lookup_during_callback = backend.lookup_count

    backend.actions.append(quote_callback)
    wait_for(lambda: backend.lookup_count == 1)
    assert backend.lookup_during_callback == 0
    quote = None
    while quote is None:
        event = ingress.try_pop()
        if event is not None and event.captured_kind is CapturedKind.QUOTE_SNAPSHOT:
            quote = event
    assert quote.connection_generation == 1
    assert quote.sequence == 2

    backend.actions.extend([
        lambda: backend.sink.OnConnection(3002, -1),
        lambda: backend.sink.OnConnection(3001, 0),
        lambda: backend.sink.OnConnection(3003, 0),
    ])
    wait_for(lambda: adapter.snapshot().generation == 2)
    wait_for(lambda: adapter.snapshot().state is ConnectionState.SUBSCRIBED)
    calls = [(name, args) for name, _, args in backend.calls]
    assert calls.count(("request_quotes", ("TX00,TX01",))) == 2
    assert calls.count(("request_ticks", ("TX00",))) == 2
    connection_events = []
    while True:
        event = ingress.try_pop()
        if event is None: break
        if event.captured_kind is CapturedKind.CONNECTION_NOTIFICATION:
            connection_events.append(event)
    generation_two = [event for event in connection_events if event.connection_generation == 2]
    assert generation_two[0].sequence == 0
    adapter.stop(1)
    call_count = len(backend.calls)
    backend.actions.append(lambda: backend.sink.OnConnection(3002, 0))
    assert len(backend.calls) == call_count


def test_partial_resubscribe_retries_only_failed_kind_then_subscribes():
    adapter, backend, _ = setup()
    backend.tick_results = [7, 0]
    adapter.start()
    adapter.login("acct", "secret")
    adapter.subscribe_quotes(["TX00"])
    adapter.subscribe_ticks(["TX00"])
    adapter.enter_monitor()
    backend.actions.extend([
        lambda: backend.sink.OnConnection(3001, 0),
        lambda: backend.sink.OnConnection(3003, 0),
    ])
    wait_for(lambda: adapter.snapshot().state is ConnectionState.SUBSCRIBED)
    names = [name for name, *_ in backend.calls]
    assert names.count("request_quotes") == 1
    assert names.count("request_ticks") == 2
    assert adapter._impact.snapshot(SESSION).is_incomplete
    adapter.stop(1)


def test_resubscribe_exhaustion_is_terminal():
    adapter, backend, _ = setup()
    backend.tick_results = [7, 7]
    adapter.start()
    adapter.login("acct", "secret")
    adapter.subscribe_ticks(["TX00"])
    adapter.enter_monitor()
    backend.actions.extend([
        lambda: backend.sink.OnConnection(3001, 0),
        lambda: backend.sink.OnConnection(3003, 0),
    ])
    wait_for(lambda: adapter.snapshot().state is ConnectionState.STOPPED)
    assert adapter._shutdown.snapshot().reason == "resubscription_exhausted"
