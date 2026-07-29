import ast
import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tx_trade.broker.capital.com_backend import ComtypesQuoteBackend
from tx_trade.broker.capital.contracts import LiveQuoteInitializationError


def _order_or_reply_sdk_symbols(source: str) -> set[str]:
    tree = ast.parse(source)
    forbidden_parts = (
        "SKOrderLib",
        "ISKOrderLib",
        "SKReplyLib_ConnectByID",
        "ConnectByID",
        "OnStrategyData",
        "SendFutureOrder",
        "SendOrder",
    )
    findings: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            candidate = node.id
        elif isinstance(node, ast.Attribute):
            candidate = node.attr
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            candidate = node.name
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            candidate = node.value
        else:
            continue
        for token in forbidden_parts:
            if token in candidate:
                findings.add(token)
    return findings


def test_package_import_is_side_effect_free():
    before = set(sys.modules)
    importlib.import_module("tx_trade.broker.capital")
    added = set(sys.modules) - before
    assert "pythoncom" not in added
    assert "comtypes.client" not in added
    assert "quote_client" not in added
    assert "config" not in added


def test_production_source_has_no_order_or_reply_stream_sdk_symbols():
    root = Path("tx_trade/broker/capital")
    paths = sorted(root.rglob("*.py"))
    assert paths
    for path in paths:
        findings = _order_or_reply_sdk_symbols(path.read_text("utf-8"))
        assert findings == set(), f"{path}: {sorted(findings)}"


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("sdk.SKOrderLib = sdk.CreateObject()", "SKOrderLib"),
        ("reply.SKReplyLib_ConnectByID(user)", "SKReplyLib_ConnectByID"),
        ("broker.SendFutureOrder(order)", "SendFutureOrder"),
        ("class Sink:\n    def OnStrategyData(self): pass", "OnStrategyData"),
    ],
)
def test_order_sdk_scanner_detects_hostile_fragments(source, expected):
    assert expected in _order_or_reply_sdk_symbols(source)


@pytest.mark.parametrize(
    "source",
    [
        "class CapitalOnNewDataRecord: pass",
        "ON_NEW_DATA_SCHEMA_VERSION = 'capital.on_new_data.v1'",
        "def OnNewData(payload): return payload",
        "class LiveOrder: pass",
    ],
)
def test_order_sdk_scanner_allows_callback_schema_and_domain_names(source):
    assert _order_or_reply_sdk_symbols(source) == set()


def test_actual_backend_creates_exact_center_quote_and_reply(monkeypatch):
    created = []

    class Module:
        SKCenterLib = object()
        ISKCenterLib = object()
        SKQuoteLib = object()
        ISKQuoteLib = object()
        SKReplyLib = object()
        ISKReplyLib = object()
        _ISKReplyLibEvents = object()

        def __getattr__(self, name):
            if "Order" in name:
                raise AssertionError("forbidden coclass accessed")
            raise AttributeError(name)

    module = Module()
    client = SimpleNamespace(
        GetModule=lambda path: module,
        CreateObject=lambda coclass, interface: created.append((coclass, interface)) or object(),
    )
    original = importlib.import_module
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name: client if name == "comtypes.client" else original(name),
    )
    backend = ComtypesQuoteBackend()
    backend.initialize("fixture.dll")
    assert created == [
        (module.SKCenterLib, module.ISKCenterLib),
        (module.SKQuoteLib, module.ISKQuoteLib),
        (module.SKReplyLib, module.ISKReplyLib),
    ]
    assert backend._reply is not None


def test_actual_backend_registers_quote_and_announcement_only_reply_events():
    calls = []
    disconnected = []

    class Connection:
        def __init__(self, name, fail=False):
            self.name = name
            self.fail = fail

        def disconnect(self):
            disconnected.append(self.name)
            if self.fail:
                raise RuntimeError("ignored cleanup failure")

    class Module:
        _ISKReplyLibEvents = object()

    quote = object()
    reply = object()
    quote_sink = object()

    def get_events(service, sink):
        calls.append((service, sink))
        return Connection("quote", fail=True) if service is quote else Connection("reply")

    backend = ComtypesQuoteBackend()
    backend._client = SimpleNamespace(GetEvents=get_events)
    backend._module = Module()
    backend._quote = quote
    backend._reply = reply

    backend.register_events(quote_sink)

    assert calls[0] == (quote, quote_sink)
    assert calls[1][0] is reply
    reply_sink = calls[1][1]
    assert reply_sink.OnReplyMessage("ignored-user", "announcement") == -1
    assert {name for name in type(reply_sink).__dict__ if not name.startswith("_")} == {
        "OnReplyMessage"
    }

    backend.release_events()
    assert disconnected == ["quote", "reply"]
    assert backend._quote_event_connection is None
    assert backend._reply_event_connection is None

    backend.release_objects()
    assert backend._reply is None


def test_actual_backend_never_falls_back_when_loading_fails(monkeypatch):
    client = SimpleNamespace(GetModule=lambda path: (_ for _ in ()).throw(OSError("missing")))
    original = importlib.import_module
    monkeypatch.setattr(
        importlib,
        "import_module",
        lambda name: client if name == "comtypes.client" else original(name),
    )
    backend = ComtypesQuoteBackend()
    with pytest.raises(LiveQuoteInitializationError):
        backend.initialize("missing.dll")


@pytest.mark.parametrize("result,expected", [(0, 0), ((7, 0), 0), ([9, 3], 3)])
def test_actual_backend_paging_uses_integer_zero(result, expected):
    received = []

    class Quote:
        def SKQuoteLib_RequestStocks(self, page, symbols):
            received.append((page, symbols))
            return result

    backend = ComtypesQuoteBackend()
    backend._quote = Quote()
    assert backend.request_quotes("TX00") == expected
    assert received == [(0, "TX00")]


def test_actual_backend_lookup_tuple_orders_and_out_parameter():
    class Stock:
        nBid = 1
        nAsk = 2
        nClose = 3
        nBc = 4
        nAc = 5
        nTQty = 6

    stock = Stock()

    class Module:
        def SKSTOCKLONG(self):
            return Stock()

    backend = ComtypesQuoteBackend()
    backend._module = Module()

    class Quote:
        def __init__(self, result):
            self.result = result

        def SKQuoteLib_GetStockByIndexLONG(self, *args):
            return self.result

    for result in ((stock, 0), (0, stock)):
        backend._quote = Quote(result)
        assert backend.lookup_quote(0, 1).total_qty_raw == 6

    class OutQuote:
        def SKQuoteLib_GetStockByIndexLONG(self, market, index, target):
            target.nBid, target.nAsk, target.nClose = 7, 8, 9
            target.nBc, target.nAc, target.nTQty = 1, 2, 3
            return 0

    backend._quote = OutQuote()
    assert backend.lookup_quote(0, 1).last_raw == 9
