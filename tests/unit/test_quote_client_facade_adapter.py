from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import subprocess
import sys
from threading import Event, Thread

import pytest

from quote_client import (
    QuoteClient,
    QuoteClientStoppedError,
    QuoteClientUnsupportedOperationError,
)
from tx_trade.broker.capital.contracts import AdapterSnapshot, ReadyTimeoutError
from tx_trade.market_data.models import (
    ConnectionState,
    ConnectionStatus,
    TAIPEI,
)


def _ready_status() -> ConnectionStatus:
    return ConnectionStatus(
        state=ConnectionState.STOCKS_READY,
        broker_kind_raw=3003,
        broker_code_raw=0,
        message="ready",
        is_ready=True,
        changed_at=datetime(2026, 7, 26, 9, 0, tzinfo=TAIPEI),
        connection_generation=1,
    )


class _Adapter:
    def __init__(self) -> None:
        self.calls: list[object] = []
        self.state = "stocks_ready"

    def start(self) -> None:
        self.calls.append("start")

    def login(self, account: str, password: str) -> None:
        self.calls.append(("login", account, password))

    def enter_monitor(self) -> None:
        self.calls.append("enter_monitor")

    def wait_until_ready(self, timeout_seconds: float) -> ConnectionStatus:
        self.calls.append(("wait_until_ready", timeout_seconds))
        return _ready_status()

    def subscribe_quotes(self, symbols: list[str]) -> None:
        self.calls.append(("subscribe_quotes", tuple(symbols)))

    def subscribe_ticks(self, symbols: list[str]) -> None:
        self.calls.append(("subscribe_ticks", tuple(symbols)))

    def snapshot(self):
        return {
            "state": self.state,
            "last_kind": 3003,
            "last_code": 0,
        }

    def stop(self, timeout_seconds: float) -> None:
        self.calls.append(("stop", timeout_seconds))


def test_adapter_facade_delegates_lifecycle_and_preserves_keys():
    adapter = _Adapter()
    client = QuoteClient(quote_adapter=adapter)

    assert client.initialize() is True
    login = client.login("account", "password")
    assert set(("success", "mode", "code", "steps", "message")) <= login.keys()
    assert "account" not in repr(login)
    assert "password" not in repr(login)
    assert client.enter_monitor()["success"] is True
    ready = client.wait_until_ready(1.25)
    assert ready["connected"] is True
    assert ready["status"] == {
        "success": True,
        "connected": True,
        "status": "stocks_ready",
        "last_kind": 3003,
        "last_code": 0,
    }
    assert client.is_quote_connected()["last_kind"] == 3003
    assert client.request_stocks(["TX00"])["symbols"] == ["TX00"]
    assert client.request_ticks(["TX00"])["symbols"] == ["TX00"]
    assert client.leave_monitor() == {
        "success": True,
        "steps": ["stop"],
        "errors": [],
    }
    assert client.leave_monitor()["steps"] == ["already_stopped"]
    assert adapter.calls == [
        "start",
        ("login", "account", "password"),
        "enter_monitor",
        ("wait_until_ready", 1.25),
        ("subscribe_quotes", ("TX00",)),
        ("subscribe_ticks", ("TX00",)),
        ("stop", 5.0),
    ]
    with pytest.raises(QuoteClientStoppedError):
        client.initialize()


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("request_server_time", ()),
        ("get_stock_by_symbol", ("TX00",)),
        ("get_tick_long", (0, 1)),
        ("connect_reply_by_id", ("id",)),
        ("order_initialize", ()),
        ("order_load_commodity_gw", ("id",)),
        ("order_initial_proxy_by_id", ("id",)),
        ("get_order_login_type", ("id",)),
    ],
)
def test_adapter_facade_hard_fails_unsupported_operations(method, args):
    client = QuoteClient(quote_adapter=_Adapter())
    with pytest.raises(QuoteClientUnsupportedOperationError):
        getattr(client, method)(*args)


def test_adapter_exception_propagates_without_offline_fallback():
    class BrokenAdapter(_Adapter):
        def start(self) -> None:
            raise LookupError("start failed")

    client = QuoteClient(quote_adapter=BrokenAdapter())
    with pytest.raises(LookupError, match="start failed"):
        client.initialize()
    assert client._offline_mode is False


def test_market_number_is_unsupported_without_adapter_call():
    adapter = _Adapter()
    client = QuoteClient(quote_adapter=adapter)
    client.initialize()
    with pytest.raises(QuoteClientUnsupportedOperationError):
        client.request_stocks(["TX00"], market_no=0)
    with pytest.raises(QuoteClientUnsupportedOperationError):
        client.request_ticks(["TX00"], market_no=0)
    assert adapter.calls == ["start"]


def test_event_snapshot_is_defensive():
    original = {"server_time": None, "stock_list": None, "quotes": [{"x": 1}], "ticks": []}
    client = QuoteClient(quote_adapter=_Adapter(), event_snapshot_provider=lambda: original)
    first = client.get_latest_event_data()
    first["quotes"][0]["x"] = 99
    assert original["quotes"][0]["x"] == 1


def test_stop_failure_is_redacted_terminal_and_idempotent():
    secret = "account-password-secret"

    class BrokenStopAdapter(_Adapter):
        def stop(self, timeout_seconds: float) -> None:
            self.calls.append(("stop", timeout_seconds))
            raise RuntimeError(secret)

    client = QuoteClient(quote_adapter=BrokenStopAdapter())
    client.initialize()
    first = client.leave_monitor()
    assert first == {
        "success": False,
        "steps": ["stop_failed"],
        "errors": [{"name": "stop", "message": "quote adapter stop failed"}],
    }
    assert secret not in repr(first)
    assert client.leave_monitor() == {
        "success": True,
        "steps": ["already_stopped"],
        "errors": [],
    }
    with pytest.raises(QuoteClientStoppedError):
        client.initialize()


def test_concurrent_stop_calls_adapter_once():
    entered = Event()
    release = Event()

    class BlockingStopAdapter(_Adapter):
        def stop(self, timeout_seconds: float) -> None:
            self.calls.append(("stop", timeout_seconds))
            entered.set()
            assert release.wait(2.0)

    adapter = BlockingStopAdapter()
    client = QuoteClient(quote_adapter=adapter)
    client.initialize()
    results: list[dict] = []
    threads = [Thread(target=lambda: results.append(client.leave_monitor())) for _ in range(2)]
    for thread in threads:
        thread.start()
    assert entered.wait(2.0)
    release.set()
    for thread in threads:
        thread.join()

    assert adapter.calls.count(("stop", 5.0)) == 1
    assert sorted(result["steps"][0] for result in results) == [
        "already_stopped",
        "stop",
    ]


def test_wait_ready_projects_real_connection_status_to_legacy_dict():
    status = _ready_status()

    class StatusAdapter(_Adapter):
        def wait_until_ready(self, timeout_seconds: float) -> ConnectionStatus:
            return status

    client = QuoteClient(quote_adapter=StatusAdapter())
    client.initialize()
    ready = client.wait_for_quote_ready(1.0)
    assert ready["status"] == {
        "success": True,
        "connected": True,
        "status": "stocks_ready",
        "last_kind": 3003,
        "last_code": 0,
    }
    assert not isinstance(ready["status"], ConnectionStatus)


@pytest.mark.parametrize(
    "malformed",
    [
        object(),
        type("BadReady", (), {"is_ready": 1})(),
        type(
            "BadKind",
            (),
            {
                "is_ready": True,
                "broker_kind_raw": "3003",
                "broker_code_raw": 0,
            },
        )(),
    ],
)
def test_wait_ready_rejects_malformed_status(malformed):
    class MalformedAdapter(_Adapter):
        def wait_until_ready(self, timeout_seconds: float):
            return malformed

    client = QuoteClient(quote_adapter=MalformedAdapter())
    client.initialize()
    with pytest.raises(TypeError):
        client.wait_for_quote_ready(1.0)


def test_ready_timeout_error_propagates():
    error = ReadyTimeoutError("timed out")

    class TimeoutAdapter(_Adapter):
        def wait_until_ready(self, timeout_seconds: float):
            raise error

    client = QuoteClient(quote_adapter=TimeoutAdapter())
    client.initialize()
    with pytest.raises(ReadyTimeoutError) as raised:
        client.wait_for_quote_ready(1.0)
    assert raised.value is error


def test_is_connected_maps_real_adapter_snapshot():
    snapshot = AdapterSnapshot(
        state=ConnectionState.SUBSCRIBED,
        generation=1,
        callback_sequence=2,
        last_kind=3003,
        last_code=0,
        desired_quotes=frozenset({"TX00"}),
        actual_quotes=frozenset({"TX00"}),
        desired_ticks=frozenset(),
        actual_ticks=frozenset(),
        thread_id=123,
        reconnect_attempts=0,
        accepting_commands=True,
    )

    class SnapshotAdapter(_Adapter):
        def snapshot(self) -> AdapterSnapshot:
            return snapshot

    client = QuoteClient(quote_adapter=SnapshotAdapter())
    client.initialize()
    assert client.is_quote_connected() == {
        "success": True,
        "connected": True,
        "status": "subscribed",
        "last_kind": 3003,
        "last_code": 0,
    }


@pytest.mark.parametrize(
    ("operation", "args"),
    [
        ("login", ("account", "password")),
        ("enter_monitor", ()),
        ("wait_until_ready", (1.0,)),
        ("subscribe_quotes", (["TX00"],)),
        ("subscribe_ticks", (["TX00"],)),
    ],
)
def test_adapter_operation_failure_never_falls_back(operation, args):
    class FailingAdapter(_Adapter):
        pass

    adapter = FailingAdapter()

    def fail(*unused_args):
        raise RuntimeError(f"{operation} failed")

    setattr(adapter, operation, fail)
    client = QuoteClient(quote_adapter=adapter)
    client.initialize()
    client._try_load_api = lambda: pytest.fail("legacy fallback must not run")

    facade_call = {
        "login": lambda: client.login(*args),
        "enter_monitor": client.enter_monitor,
        "wait_until_ready": lambda: client.wait_for_quote_ready(*args),
        "subscribe_quotes": lambda: client.request_stocks(*args),
        "subscribe_ticks": lambda: client.request_ticks(*args),
    }[operation]
    with pytest.raises(RuntimeError, match=f"{operation} failed"):
        facade_call()
    assert client._offline_mode is False


def test_adapter_construction_in_fresh_process_avoids_config_and_logs(tmp_path):
    repository = Path(__file__).resolve().parents[2]
    script = """
import builtins
from pathlib import Path
original_import = builtins.__import__
def guarded_import(name, *args, **kwargs):
    if name == "config":
        raise AssertionError("legacy config imported")
    return original_import(name, *args, **kwargs)
builtins.__import__ = guarded_import
from quote_client import QuoteClient
QuoteClient(quote_adapter=object())
assert not Path("logs").exists()
"""
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(repository)
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
