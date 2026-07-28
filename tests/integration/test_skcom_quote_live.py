"""Explicitly opted-in, quote-only SKCOM live integration test."""

import os
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

LIVE_OPT_IN = "TX_TRADE_RUN_LIVE_QUOTE_TEST"
SYMBOL = "TX00"


class _LiveQuoteContext:
    __slots__ = ("account", "client", "password")

    def __init__(self, client, account, password):
        self.client = client
        self.account = account
        self.password = password

    def __repr__(self):
        return "<live quote context: credentials redacted>"


def _forbid_trading_or_reply_connect(*args, **kwargs):
    raise AssertionError("live quote test must not connect Reply or call Order APIs")


def test_quote_only_api_creates_announcement_reply_before_login(monkeypatch):
    from quote_client import QuoteClient
    import quote_client as quote_client_module

    created = []

    class FakeSK:
        SKCenterLib = object()
        ISKCenterLib = object()
        SKQuoteLib = object()
        ISKQuoteLib = object()
        SKReplyLib = object()
        ISKReplyLib = object()
        _ISKReplyLibEvents = object()

        def __getattr__(self, name):
            if name in {"SKOrderLib", "ISKOrderLib"}:
                raise AssertionError("quote-only initialization accessed Order COM")
            raise AttributeError(name)

    fake_sk = FakeSK()

    monkeypatch.setitem(sys.modules, "comtypes.gen.SKCOMLib", fake_sk)
    monkeypatch.setattr(quote_client_module.comtypes.client, "GetModule", lambda path: None)

    class FakeCenter:
        def SKCenterLib_SetLogPath(self, path):
            return 0

        def SKCenterLib_Debug(self, enabled):
            return 0

        def SKCenterLib_Login(self, account, password):
            calls.append(("login", account, password))
            return 0

        def SKCenterLib_GetReturnCodeMessage(self, code):
            return "ok"

    fake_center = FakeCenter()
    fake_quote = object()
    fake_reply = object()
    calls = []

    def create_object(coclass, interface):
        created.append(coclass)
        if coclass is fake_sk.SKCenterLib:
            return fake_center
        if coclass is fake_sk.SKQuoteLib:
            return fake_quote
        if coclass is fake_sk.SKReplyLib:
            return fake_reply
        raise AssertionError("quote-only initialization touched Order COM")

    monkeypatch.setattr(quote_client_module.comtypes.client, "CreateObject", create_object)

    def get_events(service, sink):
        calls.append(("events", service, sink))
        return object()

    monkeypatch.setattr(quote_client_module.comtypes.client, "GetEvents", get_events)

    client = QuoteClient(dll_path=__file__, quote_only=True)
    client._dll_candidates = [__file__]
    assert client.initialize() is True
    assert created == [fake_sk.SKCenterLib, fake_sk.SKQuoteLib, fake_sk.SKReplyLib]
    assert set(client._api_objects) == {"center", "quote", "reply"}
    assert client._order_service is None
    assert client._reply_service is fake_reply

    login = client.login("not-a-real-account", "not-a-real-password")
    assert login.get("success") is True
    assert "register_reply" in login.get("steps", [])
    assert client._reply_registered is True
    event_call = next(call for call in calls if call[0] == "events")
    assert event_call[1] is fake_reply
    assert event_call[2].OnReplyMessage("ignored-user", "announcement") == -1
    assert [call[0] for call in calls].index("events") < [call[0] for call in calls].index("login")
    for method_name, args in (
        ("connect_reply_by_id", ("forbidden-user",)),
        ("order_initialize", ()),
        ("order_load_commodity_gw", ("forbidden-user",)),
        ("order_initial_proxy_by_id", ("forbidden-user",)),
        ("get_order_login_type", ("forbidden-user",)),
    ):
        with pytest.raises(quote_client_module.QuoteClientUnsupportedOperationError):
            getattr(client, method_name)(*args)


def test_quote_only_login_fails_closed_when_announcement_registration_fails(monkeypatch):
    from quote_client import QuoteClient
    import quote_client as quote_client_module

    calls = []

    class FakeCenter:
        def SKCenterLib_SetLogPath(self, path):
            calls.append("set_log_path")
            return 0

        def SKCenterLib_Debug(self, enabled):
            calls.append("debug")
            return 0

        def SKCenterLib_Login(self, account, password):
            calls.append("login")
            return 0

    class FakeSK:
        _ISKReplyLibEvents = object()

    def fail_registration(service, sink):
        raise RuntimeError("sensitive registration failure")

    monkeypatch.setattr(quote_client_module.comtypes.client, "GetEvents", fail_registration)
    client = QuoteClient(quote_only=True)
    client._offline_mode = False
    client._center_service = FakeCenter()
    client._reply_service = object()
    client._sk_module = FakeSK()

    result = client.login("account-canary", "password-canary")

    assert result == {
        "success": False,
        "mode": "api",
        "steps": ["set_log_path", "debug", "register_reply_failed"],
        "message": "required announcement callback registration failed",
    }
    assert calls == ["set_log_path", "debug"]
    assert client._reply_registered is False
    assert "account-canary" not in repr(result)
    assert "password-canary" not in repr(result)
    assert "sensitive" not in repr(result)


def test_leave_monitor_preserves_failed_steps_for_retry():
    from quote_client import QuoteClient

    disconnected = []

    class Connection:
        def __init__(self, name):
            self.name = name

        def disconnect(self):
            disconnected.append(self.name)

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
    client._quote_event_connection = Connection("quote")
    client._event_connection = Connection("reply")
    client._quote_registered = True
    client._reply_registered = True

    first = client.leave_monitor()
    assert first.get("success") is False
    assert disconnected == ["quote", "reply"]
    assert client._quote_registered is False
    assert client._reply_registered is False
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


def test_leave_before_monitor_disconnects_login_announcement_events():
    from quote_client import QuoteClient

    disconnected = []

    class Connection:
        def __init__(self, name):
            self.name = name

        def disconnect(self):
            disconnected.append(self.name)

    client = QuoteClient(quote_only=True)
    client._offline_mode = False
    client._quote_service = object()
    client._quote_event_connection = Connection("quote")
    client._event_connection = Connection("reply")
    client._quote_registered = True
    client._reply_registered = True

    result = client.leave_monitor()

    assert result.get("success") is True
    assert result.get("steps") == ["already_left"]
    assert disconnected == ["quote", "reply"]
    assert client._quote_registered is False
    assert client._reply_registered is False


def test_non_quote_only_leave_preserves_legacy_reply_connection():
    from quote_client import QuoteClient

    disconnected = []

    class Connection:
        def __init__(self, name):
            self.name = name

        def disconnect(self):
            disconnected.append(self.name)

    reply_connection = Connection("reply")
    client = QuoteClient(quote_only=False)
    client._offline_mode = False
    client._quote_service = object()
    client._quote_event_connection = Connection("quote")
    client._event_connection = reply_connection
    client._quote_registered = True
    client._reply_registered = True

    result = client.leave_monitor()

    assert result.get("success") is True
    assert disconnected == ["quote"]
    assert client._quote_registered is False
    assert client._event_connection is reply_connection
    assert client._reply_registered is True


def test_live_context_repr_redacts_credentials():
    context = _LiveQuoteContext(object(), "account-canary", "password-canary")

    assert repr(context) == "<live quote context: credentials redacted>"
    assert "account-canary" not in repr(context)
    assert "password-canary" not in repr(context)


def test_cleanup_pump_failure_is_sanitized_and_leave_still_runs(monkeypatch):
    from quote_client import QuoteClient
    import quote_client as quote_client_module

    class FakePythonCom:
        @staticmethod
        def PumpWaitingMessages():
            raise RuntimeError("sensitive pump failure")

    class FakeQuote:
        def SKQuoteLib_LeaveMonitor(self):
            return 0

    monkeypatch.setattr(quote_client_module, "pythoncom", FakePythonCom())
    client = QuoteClient(quote_only=True)
    client._offline_mode = False
    client._quote_service = FakeQuote()
    client._monitor_active = True

    result = client.leave_monitor()

    assert result.get("success") is False
    assert client._monitor_active is False
    assert result.get("errors") == [
        {
            "name": "cleanup_message_pump",
            "message": "quote cleanup message pump failed",
        }
    ]
    assert "sensitive" not in repr(result)


@pytest.fixture
def live_quote_client(monkeypatch):
    if sys.platform != "win32":
        pytest.skip("SKCOM live quote test requires Windows")
    if os.getenv(LIVE_OPT_IN) != "1":
        pytest.skip(f"set {LIVE_OPT_IN}=1 to run the live quote-only test")
    dll_path = os.getenv("TX_TRADE_SKCOM_DLL_PATH", "")
    account = os.getenv("TX_TRADE_ACCOUNT", "")
    password = os.getenv("TX_TRADE_PASSWORD", "")
    if not dll_path or not Path(dll_path).is_file():
        pytest.skip("TX_TRADE_SKCOM_DLL_PATH does not name an SKCOM DLL")
    if not account or not password:
        pytest.skip("live SKCOM credentials are not configured")

    from quote_client import QuoteClient

    forbidden_methods = (
        "connect_reply_by_id",
        "order_initialize",
        "order_load_commodity_gw",
        "order_initial_proxy_by_id",
        "get_order_login_type",
    )
    for method_name in forbidden_methods:
        monkeypatch.setattr(QuoteClient, method_name, _forbid_trading_or_reply_connect)

    client = QuoteClient(dll_path=dll_path, quote_only=True)
    try:
        yield _LiveQuoteContext(client, account, password)
    finally:
        cleanup = client.leave_monitor()
        assert cleanup.get("success") is True, f"quote cleanup failed: {cleanup.get('errors')}"


@pytest.mark.live
def test_live_login_and_quote_subscription(live_quote_client):
    client = live_quote_client.client

    assert client.initialize() is True
    login = client.login(live_quote_client.account, live_quote_client.password)
    assert login.get("mode") == "api"
    assert login.get("success") is True
    assert login.get("code") == 0
    assert client._offline_mode is False
    assert set(client._api_objects) == {"center", "quote", "reply"}
    assert client._order_service is None
    assert client._reply_service is not None
    assert client._reply_registered is True

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
