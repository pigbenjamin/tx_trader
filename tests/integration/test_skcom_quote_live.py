"""Explicitly opted-in, quote-only SKCOM live integration test."""

import os
import sys
import types
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

from config import API_DLL_PATH, DEFAULT_ACCOUNT, DEFAULT_PASSWORD
from quote_client import QuoteClient
import quote_client as quote_client_module


LIVE_OPT_IN = "TX_TRADE_RUN_LIVE_QUOTE_TEST"
SYMBOL = "TX00"


def _forbid_order_or_reply(*args, **kwargs):
    raise AssertionError("live quote test must not call Order/Reply APIs")


def test_quote_only_api_creation_and_login_avoid_order_reply(monkeypatch):
    created = []

    fake_sk = types.SimpleNamespace(
        SKCenterLib=object(),
        ISKCenterLib=object(),
        SKQuoteLib=object(),
        ISKQuoteLib=object(),
        SKOrderLib=object(),
        ISKOrderLib=object(),
        SKReplyLib=object(),
        ISKReplyLib=object(),
    )

    monkeypatch.setitem(sys.modules, "comtypes.gen.SKCOMLib", fake_sk)
    monkeypatch.setattr(quote_client_module.comtypes.client, "GetModule", lambda path: None)

    class FakeCenter:
        def SKCenterLib_SetLogPath(self, path):
            return 0

        def SKCenterLib_Debug(self, enabled):
            return 0

        def SKCenterLib_Login(self, account, password):
            return 0

        def SKCenterLib_GetReturnCodeMessage(self, code):
            return "ok"

    fake_center = FakeCenter()
    fake_quote = object()

    def create_object(coclass, interface):
        created.append(coclass)
        if coclass is fake_sk.SKCenterLib:
            return fake_center
        if coclass is fake_sk.SKQuoteLib:
            return fake_quote
        raise AssertionError("quote-only initialization touched Order/Reply COM")

    monkeypatch.setattr(quote_client_module.comtypes.client, "CreateObject", create_object)

    client = QuoteClient(dll_path=__file__, quote_only=True)
    client._dll_candidates = [__file__]
    assert client.initialize() is True
    assert created == [fake_sk.SKCenterLib, fake_sk.SKQuoteLib]
    assert set(client._api_objects) == {"center", "quote"}
    assert client._order_service is None
    assert client._reply_service is None

    monkeypatch.setattr(client, "_register_reply_callback", _forbid_order_or_reply)
    login = client.login("not-a-real-account", "not-a-real-password")
    assert login.get("success") is True
    assert "skip_reply_quote_only" in login.get("steps", [])


def test_leave_monitor_preserves_failed_steps_for_retry():
    class FakeQuote:
        def __init__(self):
            self.tick_calls = 0
            self.stock_calls = 0
            self.leave_calls = 0

        def SKQuoteLib_CancelRequestTicks(self, symbols):
            self.tick_calls += 1
            return 7 if self.tick_calls == 1 else 0

        def SKQuoteLib_CancelRequestStocks(self, symbols):
            self.stock_calls += 1
            return 0

        def SKQuoteLib_LeaveMonitor(self):
            self.leave_calls += 1
            return 9 if self.leave_calls == 1 else 0

    service = FakeQuote()
    client = QuoteClient(quote_only=True)
    client._offline_mode = False
    client._quote_service = service
    client._monitor_active = True
    client._tick_subscriptions.add(SYMBOL)
    client._stock_subscriptions.add(SYMBOL)

    first = client.leave_monitor()
    assert first.get("success") is False
    assert client._tick_subscriptions == {SYMBOL}
    assert client._stock_subscriptions == set()
    assert client._monitor_active is True

    second = client.leave_monitor()
    assert second.get("success") is True
    assert client._tick_subscriptions == set()
    assert client._stock_subscriptions == set()
    assert client._monitor_active is False
    assert service.tick_calls == 2
    assert service.stock_calls == 1
    assert service.leave_calls == 2


@pytest.fixture
def live_quote_client(monkeypatch):
    if sys.platform != "win32":
        pytest.skip("SKCOM live quote test requires Windows")
    if os.getenv(LIVE_OPT_IN) != "1":
        pytest.skip(f"set {LIVE_OPT_IN}=1 to run the live quote-only test")
    if not Path(API_DLL_PATH).is_file():
        pytest.skip(f"SKCOM DLL not found at configured path: {API_DLL_PATH}")
    if not DEFAULT_ACCOUNT or not DEFAULT_PASSWORD:
        pytest.skip("live SKCOM credentials are not configured")

    forbidden_methods = (
        "connect_reply_by_id",
        "order_initialize",
        "order_load_commodity_gw",
        "order_initial_proxy_by_id",
        "get_order_login_type",
    )
    for method_name in forbidden_methods:
        monkeypatch.setattr(QuoteClient, method_name, _forbid_order_or_reply)

    client = QuoteClient(quote_only=True)
    try:
        yield client
    finally:
        cleanup = client.leave_monitor()
        assert cleanup.get("success") is True, f"quote cleanup failed: {cleanup.get('errors')}"


def test_live_login_and_quote_subscription(live_quote_client):
    client = live_quote_client

    assert client.initialize() is True
    login = client.login(DEFAULT_ACCOUNT, DEFAULT_PASSWORD)
    assert login.get("mode") == "api"
    assert login.get("success") is True
    assert login.get("code") == 0
    assert client._offline_mode is False
    assert set(client._api_objects) == {"center", "quote"}
    assert client._order_service is None
    assert client._reply_service is None

    monitor = client.enter_monitor()
    assert monitor.get("success") is True

    ready = client.wait_for_quote_ready(timeout=20.0, interval=0.25)
    assert ready.get("success") is True
    assert ready.get("connected") is True

    stocks = client.request_stocks([SYMBOL])
    assert stocks.get("success") is True
    assert stocks.get("symbols") == [SYMBOL]

    stock = client.get_stock_by_symbol(SYMBOL)
    assert stock.get("success") is True
    assert stock.get("symbol") == SYMBOL
    assert stock.get("object") is not None

    ticks_request = client.request_ticks([SYMBOL])
    assert ticks_request.get("success") is True
    assert ticks_request.get("symbols") == [SYMBOL]

    client.pump_events(duration=1.0)
    event_data = client.get_latest_event_data()
    ticks = event_data.get("ticks", [])
    assert isinstance(ticks, list)
    for tick in ticks:
        assert isinstance(tick, dict)
        assert isinstance(tick.get("market_no"), int)
        assert isinstance(tick.get("stock_idx"), int)
        assert isinstance(tick.get("close"), int)
        assert isinstance(tick.get("qty"), int)
