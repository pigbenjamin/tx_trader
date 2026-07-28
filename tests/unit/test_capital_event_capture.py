from datetime import datetime
from time import monotonic
from uuid import UUID

from tx_trade.broker.capital.contracts import QuoteSnapshotRaw
from tx_trade.broker.capital.quote_adapter import CapitalQuoteStaAdapter
from tx_trade.market_data.ingress import BoundedIngress, BoundedStaQuoteQueue
from tx_trade.market_data.models import CapturedKind, TAIPEI
from tx_trade.monitoring.health import (
    ControlledShutdown,
    HealthState,
    PipelineHealth,
    SessionImpactTracker,
)
from tx_trade.monitoring.metrics import IngressMetrics

NOW = datetime(2026, 7, 26, 9, 30, tzinfo=TAIPEI)
SESSION = UUID("12345678-1234-5678-1234-567812345678")


class Clock:
    def now(self):
        return NOW

    def monotonic(self):
        return monotonic()


class Backend:
    def co_initialize(self):
        pass

    def initialize(self, dll_path):
        pass

    def register_events(self, sink):
        self.sink = sink

    def login(self, account, password):
        return 0

    def enter_monitor(self):
        return 0

    def leave_monitor(self):
        return 0

    def request_quotes(self, symbols_csv):
        return 0

    def request_ticks(self, symbols_csv):
        return 0

    def cancel_quotes(self, symbols_csv):
        return 0

    def cancel_ticks(self, symbols_csv):
        return 0

    def lookup_quote(self, market_no, stock_idx):
        return QuoteSnapshotRaw(100, 102, 101, 3, 4, None, 99, "TX00", "TX")

    def pump_waiting_messages(self):
        pass

    def release_events(self):
        pass

    def release_objects(self):
        pass

    def co_uninitialize(self):
        pass


def components(sta_capacity=2):
    clock = Clock()
    health = PipelineHealth(clock)
    metrics = IngressMetrics()
    impact = SessionImpactTracker(2)
    shutdown = ControlledShutdown()
    ingress = BoundedIngress(
        control_capacity=20,
        diagnostic_capacity=20,
        quote_capacity=20,
        tick_capacity=20,
        dedupe_capacity=20,
        health=health,
        metrics=metrics,
        session_impact=impact,
        shutdown=shutdown,
    )
    sta = BoundedStaQuoteQueue(
        sta_capacity,
        health=health,
        metrics=metrics,
        session_impact=impact,
        shutdown=shutdown,
        session_id=SESSION,
    )
    adapter = CapitalQuoteStaAdapter(
        backend=Backend(),
        dll_path="fixture.dll",
        ingress=ingress,
        sta_queue=sta,
        clock=clock,
        health=health,
        session_impact=impact,
        shutdown=shutdown,
        command_capacity=2,
        command_timeout=1,
        pump_interval=0.01,
        quote_lookup_attempts=2,
        session_id=SESSION,
    )
    return adapter, ingress, sta, health, impact


def test_callbacks_copy_complete_raw_values_without_normalization():
    adapter, ingress, *_ = components()
    adapter.OnNotifyTicksLONG(1, 2, 3, 20260726, 93001, 456, 10001, 10003, 10002, 7, 9)
    event = ingress.try_pop()
    assert event.captured_kind is CapturedKind.TICK_NOTIFICATION
    assert event.source == "capital_skcom"
    assert event.payload.close_raw == 10002
    assert event.payload.simulate_raw == 9
    assert event.payload.is_long_callback is True
    assert event.event_at is None and event.trading_day is None
    assert event.sequence == 0


def test_server_time_and_stock_list_remain_raw():
    adapter, ingress, *_ = components()
    adapter.OnNotifyServerTime(9, 30, 1, 123)
    adapter.OnNotifyStockList(0, b"TX00,raw")
    first, second = ingress.try_pop(), ingress.try_pop()
    assert first.payload.total_raw == 123
    assert first.event_at is None
    assert second.payload.stock_list_raw == b"TX00,raw"
    assert second.sequence == 1


def test_quote_callback_only_hands_off_then_runner_enriches():
    adapter, ingress, sta, *_ = components()
    backend = adapter._backend
    calls = 0
    original = backend.lookup_quote

    def lookup(*args):
        nonlocal calls
        calls += 1
        return original(*args)

    backend.lookup_quote = lookup
    adapter.OnNotifyQuoteLONG(0, 8)
    assert calls == 0
    assert ingress.try_pop() is None
    assert sta.depth == 1
    adapter._drain_sta_quotes()
    assert calls == 1
    event = ingress.try_pop()
    assert event.payload.bid_raw == 100
    assert event.payload.last_qty_raw is None
    assert event.raw_payload["total_qty"] == 99


def test_lookup_failures_are_finite_diagnostics_and_damage_session():
    adapter, ingress, _, health, impact = components()

    def fail(*args):
        raise RuntimeError("secret should not escape")

    adapter._backend.lookup_quote = fail
    adapter.OnNotifyQuote(0, 8)
    adapter._drain_sta_quotes()
    diagnostics = [ingress.try_pop(), ingress.try_pop()]
    assert [event.payload.attempt for event in diagnostics] == [1, 2]
    assert all(event.payload.message == "quote lookup failed" for event in diagnostics)
    assert all("secret" not in repr(event) for event in diagnostics)
    assert health.snapshot().state is HealthState.DEGRADED
    assert impact.snapshot(SESSION).is_incomplete


def test_reserved_overflow_becomes_diagnostic_without_lookup():
    adapter, ingress, _, *_ = components(sta_capacity=1)
    adapter.OnNotifyQuote(0, 1)
    adapter.OnNotifyQuoteLONG(0, 2)
    adapter._drain_sta_quotes()
    events = [ingress.try_pop(), ingress.try_pop()]
    diagnostic = next(
        event for event in events if event.captured_kind is CapturedKind.ADAPTER_DIAGNOSTIC
    )
    assert diagnostic.payload.message == "sta quote notification overflow"
    assert diagnostic.payload.raw_notification["stock_idx"] == 2


def test_callback_strict_int_and_taipei_clock():
    adapter, *_ = components()
    assert adapter.OnConnection(True, 0) is None
    assert adapter._shutdown.snapshot().is_requested

    adapter, *_ = components()
    adapter._clock.now = lambda: datetime(2026, 7, 26, 9, 30)
    assert adapter.OnConnection(3001, 0) is None
    assert adapter._shutdown.snapshot().reason == "capital_callback_failure"


def test_quote_generation_sidecar_survives_generation_change():
    adapter, ingress, _, _, impact = components()
    old_sink = adapter._current_sink
    old_sink.OnNotifyQuoteLONG(0, 8)
    with adapter._state_condition:
        adapter._generation = 1
        adapter._current_sink = type(old_sink)(adapter, 1)
    lookup_count = 0

    def lookup(*args):
        nonlocal lookup_count
        lookup_count += 1
        raise AssertionError("stale quote must not be enriched")

    adapter._backend.lookup_quote = lookup
    adapter._drain_sta_quotes()
    event = ingress.try_pop()
    assert lookup_count == 0
    assert event.captured_kind is CapturedKind.ADAPTER_DIAGNOSTIC
    assert event.connection_generation == 0
    assert event.payload.message == "stale quote notification discarded"
    assert event.payload.market_no_raw == 0
    assert event.payload.stock_idx_raw == 8
    assert impact.snapshot(SESSION).is_incomplete
