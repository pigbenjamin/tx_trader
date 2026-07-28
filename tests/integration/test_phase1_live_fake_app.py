from datetime import datetime
from uuid import UUID

import pytest

from tx_trade.app.phase1 import (
    Phase1Dependencies,
    Phase1RuntimeError,
    run_phase1,
)
from tx_trade.market_data.models import (
    CapturedKind,
    CapturedMarketDataEvent,
    CapturedQuoteSnapshot,
    ConnectionState,
    ConnectionStatus,
    SourceMode,
    TAIPEI,
)
from tx_trade.market_data.ports import IngressDecision
from tx_trade.storage import SQLiteMarketDataRepository

NOW = datetime(2026, 7, 26, 9, 30, tzinfo=TAIPEI)
SESSION = UUID("12345678-1234-5678-1234-567812345678")


class FakeClock:
    def now(self):
        return NOW

    def monotonic(self):
        return 0.0


class FakeAdapter:
    def __init__(self, calls, failure_stage=None, **kwargs):
        self.calls = calls
        self.failure_stage = failure_stage
        self.ingress = kwargs["ingress"]
        self.session_id = kwargs["session_id"]
        self.shutdown = kwargs["shutdown"]

    def _fail(self, stage):
        if self.failure_stage == f"interrupt_{stage}":
            raise KeyboardInterrupt
        if self.failure_stage == stage:
            raise RuntimeError("password must never escape")

    def start(self):
        self.calls.append("start")
        self._fail("start")

    def login(self, account, password):
        assert account == "account"
        assert password == "password"
        self.calls.append("login")
        self._fail("login")

    def enter_monitor(self):
        self.calls.append("enter")
        self._fail("enter")

    def wait_until_ready(self, timeout):
        self.calls.append("ready")
        self._fail("ready")
        return ConnectionStatus(ConnectionState.STOCKS_READY, 3003, 0, None, True, NOW, 0)

    def subscribe_quotes(self, symbols):
        self.calls.append(("quotes", tuple(symbols)))
        self._fail("quotes")
        if self.failure_stage == "no_event":
            return
        payload = CapturedQuoteSnapshot(0, 7, 100, 102, 101, 1, 2, 3, True, 0, NOW)
        decision = self.ingress.try_publish(
            CapturedMarketDataEvent(
                CapturedKind.QUOTE_SNAPSHOT,
                payload,
                {"bid_raw": 100},
                "capital_skcom",
                SourceMode.LIVE,
                self.session_id,
                0,
                0,
                None,
                NOW,
                None,
                None,
                None,
                None,
            )
        )
        assert decision is IngressDecision.ACCEPTED

    def subscribe_ticks(self, symbols):
        self.calls.append(("ticks", tuple(symbols)))
        self._fail("ticks")
        if self.failure_stage == "background_shutdown":
            self.shutdown.request_shutdown("fake_background_failure")

    def stop(self, timeout):
        self.calls.append("stop")


def test_live_fake_is_quote_only_persists_and_finalizes(tmp_path):
    calls = []
    dependencies = Phase1Dependencies(
        backend_factory=lambda: object(),
        adapter_factory=lambda **kwargs: FakeAdapter(calls, **kwargs),
        clock=FakeClock(),
        session_id_factory=lambda: SESSION,
        idle=lambda seconds: None,
    )
    result = run_phase1(
        {
            "TX_TRADE_RUNTIME_PRESET": "phase1_live_quote",
            "TX_TRADE_ENABLE_LIVE_QUOTE": "1",
            "TX_TRADE_ACCOUNT": "account",
            "TX_TRADE_PASSWORD": "password",
            "TX_TRADE_SKCOM_DLL_PATH": "fake.dll",
            "TX_TRADE_SYMBOLS": "TX00",
        },
        db_path=str(tmp_path / "live.db"),
        dependencies=dependencies,
        max_live_iterations=1,
    )
    assert result.status == "complete"
    assert result.event_count == 1
    assert calls == [
        "start",
        "login",
        "enter",
        "ready",
        ("quotes", ("TX00",)),
        ("ticks", ("TX00",)),
        "stop",
    ]
    repository = SQLiteMarketDataRepository(result.db_path)
    assert repository.get_session(SESSION).status == "complete"
    repository.close()


@pytest.mark.parametrize("failure_stage", ["start", "login", "enter", "ready", "quotes", "ticks"])
def test_live_failure_is_sanitized_stops_and_finalizes_incomplete(tmp_path, failure_stage):
    calls = []
    db_path = tmp_path / f"{failure_stage}.db"
    dependencies = Phase1Dependencies(
        backend_factory=lambda: object(),
        adapter_factory=lambda **kwargs: FakeAdapter(calls, failure_stage=failure_stage, **kwargs),
        clock=FakeClock(),
        session_id_factory=lambda: SESSION,
        idle=lambda seconds: None,
    )
    with pytest.raises(Phase1RuntimeError) as caught:
        run_phase1(
            {
                "TX_TRADE_RUNTIME_PRESET": "phase1_live_quote",
                "TX_TRADE_ENABLE_LIVE_QUOTE": "1",
                "TX_TRADE_ACCOUNT": "account",
                "TX_TRADE_PASSWORD": "password",
                "TX_TRADE_SKCOM_DLL_PATH": "fake.dll",
                "TX_TRADE_SYMBOLS": "TX00",
            },
            db_path=str(db_path),
            dependencies=dependencies,
            max_live_iterations=1,
        )
    assert str(caught.value) == "live quote recording failed"
    assert caught.value.__cause__ is None
    assert "stop" in calls
    repository = SQLiteMarketDataRepository(db_path)
    assert repository.get_session(SESSION).status == "incomplete"
    repository.close()


@pytest.mark.parametrize(
    "terminal_stage",
    ["no_event", "background_shutdown", "interrupt_login", "interrupt_ticks"],
)
def test_internal_shutdown_interrupt_or_empty_recording_cannot_return_success(
    tmp_path, terminal_stage
):
    calls = []
    db_path = tmp_path / f"{terminal_stage}.db"
    dependencies = Phase1Dependencies(
        backend_factory=lambda: object(),
        adapter_factory=lambda **kwargs: FakeAdapter(calls, failure_stage=terminal_stage, **kwargs),
        clock=FakeClock(),
        session_id_factory=lambda: SESSION,
        idle=lambda seconds: None,
    )
    with pytest.raises(Phase1RuntimeError, match="live quote recording failed"):
        run_phase1(
            {
                "TX_TRADE_RUNTIME_PRESET": "phase1_live_quote",
                "TX_TRADE_ENABLE_LIVE_QUOTE": "1",
                "TX_TRADE_ACCOUNT": "account",
                "TX_TRADE_PASSWORD": "password",
                "TX_TRADE_SKCOM_DLL_PATH": "fake.dll",
                "TX_TRADE_SYMBOLS": "TX00",
            },
            db_path=str(db_path),
            dependencies=dependencies,
            max_live_iterations=1,
        )
    assert "stop" in calls
    repository = SQLiteMarketDataRepository(db_path)
    assert repository.get_session(SESSION).status == "incomplete"
    repository.close()


def test_async_sink_failure_after_acceptance_never_republishes_or_completes(
    tmp_path,
):
    accepted = []

    class AcceptedThenFailsWriter:
        def __init__(self, notifier):
            self.notifier = notifier

        def start(self):
            pass

        def publish(self, envelope):
            accepted.append(envelope)
            self.notifier.notify_storage_failure()

        def flush(self, timeout=None):
            raise RuntimeError("background failure detail")

        def stop(self, timeout=None):
            pass

    db_path = tmp_path / "async-failure.db"
    dependencies = Phase1Dependencies(
        writer_factory=lambda repository, settings, notifier: AcceptedThenFailsWriter(notifier),
        backend_factory=lambda: object(),
        adapter_factory=lambda **kwargs: FakeAdapter([], **kwargs),
        clock=FakeClock(),
        session_id_factory=lambda: SESSION,
        idle=lambda seconds: None,
    )
    with pytest.raises(Phase1RuntimeError, match="live quote recording failed"):
        run_phase1(
            {
                "TX_TRADE_RUNTIME_PRESET": "phase1_live_quote",
                "TX_TRADE_ENABLE_LIVE_QUOTE": "1",
                "TX_TRADE_ACCOUNT": "account",
                "TX_TRADE_PASSWORD": "password",
                "TX_TRADE_SKCOM_DLL_PATH": "fake.dll",
                "TX_TRADE_SYMBOLS": "TX00",
            },
            db_path=str(db_path),
            dependencies=dependencies,
            max_live_iterations=1,
        )
    assert len(accepted) == 1
    repository = SQLiteMarketDataRepository(db_path)
    assert repository.get_session(SESSION).status == "incomplete"
    assert tuple(repository.iter_events(SESSION)) == ()
    repository.close()
