from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

PHASE3A_MODULES = (
    "tx_trade.orders.live_contracts",
    "tx_trade.orders.live_state_machine",
    "tx_trade.orders.live_ports",
    "tx_trade.broker.capital.trading_contracts",
    "tx_trade.broker.capital.reply_parser",
)
TRUSTED_ASSESSMENT_MODULES = (
    "tx_trade.orders.live_reconciliation_assessment_contracts",
    "tx_trade.orders.sqlite_live_reconciliation_assessment",
)
RUNTIME_PERSISTENCE_MODULES = frozenset(
    {
        "tx_trade.orders.sqlite_live_journal_inspection",
        "tx_trade.orders.sqlite_live_order_journal",
        "tx_trade.orders.sqlite_live_reconciliation_assessment",
    }
)
BROKER_RUNTIME_MODULES = frozenset(
    {
        "tx_trade.broker.capital.com_backend",
    }
)
CAPITAL_ROOT = Path("tx_trade/broker/capital")
LIVE_ORDER_ROOT = Path("tx_trade/orders")

_FORBIDDEN_IMPORT_PREFIXES = (
    "pythoncom",
    "comtypes",
    "win32com",
    "dotenv",
    "keyring",
    "quote_client",
    "config",
    "winreg",
    "ctypes",
    "socket",
    "requests",
    "httpx",
    "urllib",
    "http.client",
)
_FORBIDDEN_CALLS = {
    "open",
    "builtins.open",
    "io.open",
    "os.open",
    "os.getenv",
    "os.putenv",
    "os.listdir",
    "os.scandir",
    "os.stat",
    "os.add_dll_directory",
    "pathlib.Path.exists",
    "pathlib.Path.glob",
    "pathlib.Path.is_dir",
    "pathlib.Path.is_file",
    "pathlib.Path.iterdir",
    "pathlib.Path.open",
    "pathlib.Path.read_bytes",
    "pathlib.Path.read_text",
    "pathlib.Path.rglob",
    "pathlib.Path.stat",
    "dotenv.load_dotenv",
    "dotenv.dotenv_values",
    "pythoncom.CoInitialize",
    "pythoncom.CoInitializeEx",
    "comtypes.CoInitialize",
    "comtypes.client.CreateObject",
    "comtypes.client.GetModule",
    "comtypes.client.GetEvents",
    "SKReplyLib_ConnectByID",
    "SKOrderLib_SendFutureOrder",
    "SKOrderLib_CancelOrderBySeqNo",
    "SKOrderLib_CancelOrderByBookNo",
    "SKOrderLib_CorrectPriceBySeqNo",
    "SKOrderLib_CorrectPriceByBookNo",
}
_FORBIDDEN_SDK_MEMBERS = {
    "CreateObject",
    "GetModule",
    "GetEvents",
    "CoInitialize",
    "CoInitializeEx",
    "SKReplyLib_ConnectByID",
    "SKOrderLib_SendFutureOrder",
    "SKOrderLib_CancelOrderBySeqNo",
    "SKOrderLib_CancelOrderByBookNo",
    "SKOrderLib_CorrectPriceBySeqNo",
    "SKOrderLib_CorrectPriceByBookNo",
}
_SENSITIVE_LITERALS = (
    "TX_TRADE_ACCOUNT",
    "TX_TRADE_PASSWORD",
    "TX_TRADE_SKCOM_DLL_PATH",
    "SKOrderLib",
    "SKReplyLib_ConnectByID",
    "SendFutureOrder",
)
_ORDER_SDK_NAME_PARTS = (
    "SKOrderLib",
    "ISKOrderLib",
    "SKReplyLib_ConnectByID",
    "ConnectByID",
    "SendFutureOrder",
    "SendOrder",
)
_TRUSTED_MUTATION_NAME_PARTS = (
    "claim",
    "commit",
    "dispatch",
    "migrate",
    "receipt",
    "resend",
    "write",
)
_PURE_TRUSTED_FORBIDDEN_IMPORT_PREFIXES = (
    "io",
    "json",
    "os",
    "pathlib",
    "shutil",
    "sqlite3",
    "sys",
    "tempfile",
    "tx_trade.broker",
    "tx_trade.orders.live_operator_recovery",
    "tx_trade.orders.live_reconciliation",
    "tx_trade.orders.sqlite_live_journal_inspection",
    "tx_trade.orders.sqlite_live_order_journal",
    "tx_trade.orders.sqlite_live_reconciliation_assessment",
)
_RUNTIME_TRUSTED_APPROVED_SQLITE_IMPORTS = frozenset(
    {
        "tx_trade.orders.sqlite_live_journal_inspection",
        "tx_trade.orders.sqlite_live_order_journal",
    }
)
_TRUSTED_EXTERNAL_IMPORT_PREFIXES = (
    "aiohttp",
    "ftplib",
    "paramiko",
    "skcom",
    "smtplib",
    "ssl",
    "subprocess",
    "urllib3",
    "websockets",
)


def _production_paths() -> tuple[Path, ...]:
    return (
        *sorted(CAPITAL_ROOT.rglob("*.py")),
        *sorted(LIVE_ORDER_ROOT.glob("live_*.py")),
        *sorted(LIVE_ORDER_ROOT.glob("sqlite_live_*.py")),
    )


def _production_modules() -> tuple[str, ...]:
    modules = []
    for path in _production_paths():
        parts = path.with_suffix("").parts
        if parts[-1] == "__init__":
            parts = parts[:-1]
        modules.append(".".join(parts))
    return tuple(dict.fromkeys((*modules, *TRUSTED_ASSESSMENT_MODULES)))


def _is_forbidden_import(module_name: str) -> bool:
    return any(
        module_name == prefix or module_name.startswith(f"{prefix}.")
        for prefix in _FORBIDDEN_IMPORT_PREFIXES
    )


def _resolved_name(node: ast.expr, bindings: dict[str, str]) -> str | None:
    if isinstance(node, ast.Name):
        return bindings.get(node.id, node.id)
    if isinstance(node, ast.Attribute):
        owner = _resolved_name(node.value, bindings)
        return None if owner is None else f"{owner}.{node.attr}"
    if isinstance(node, ast.Call):
        return _resolved_name(node.func, bindings)
    return None


def _module_imports(tree: ast.AST) -> tuple[tuple[str, tuple[str, ...]], ...]:
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend((alias.name, ()) for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.module is None and node.level == 1:
                imports.extend((f"tx_trade.orders.{alias.name}", ()) for alias in node.names)
            elif node.module is not None:
                module_name = node.module
                if node.level == 1:
                    module_name = f"tx_trade.orders.{module_name}"
                imports.append((module_name, tuple(alias.name for alias in node.names)))
    return tuple(imports)


def _trusted_source_violations(source: str, *, runtime: bool) -> set[str]:
    tree = ast.parse(source)
    violations = _safety_violations(source, allow_runtime_io=runtime)
    imports = _module_imports(tree)

    for module_name, imported_names in imports:
        if any(
            module_name == prefix or module_name.startswith(f"{prefix}.")
            for prefix in _TRUSTED_EXTERNAL_IMPORT_PREFIXES
        ):
            violations.add(f"trusted-import:{module_name}")
        module_parts = module_name.lower().split(".")
        if any(
            part in {"config", "configuration", "credentials", "environment"}
            or "credential" in part
            or "password" in part
            for part in module_parts
        ):
            violations.add(f"trusted-import:{module_name}")
        if any(part in module_parts[-1] for part in _TRUSTED_MUTATION_NAME_PARTS):
            violations.add(f"mutation-import:{module_name}")
        if runtime:
            if module_name == "tx_trade.broker" or module_name.startswith("tx_trade.broker."):
                violations.add(f"trusted-import:{module_name}")
            if (
                module_name.startswith("tx_trade.orders.sqlite_live_")
                and module_name not in _RUNTIME_TRUSTED_APPROVED_SQLITE_IMPORTS
            ):
                violations.add(f"trusted-import:{module_name}")
        elif any(
            module_name == prefix or module_name.startswith(f"{prefix}.")
            for prefix in _PURE_TRUSTED_FORBIDDEN_IMPORT_PREFIXES
        ):
            violations.add(f"trusted-import:{module_name}")

        for imported_name in imported_names:
            if "skcom" in imported_name.lower():
                violations.add(f"trusted-import:{module_name}.{imported_name}")
            if any(part in imported_name.lower() for part in _TRUSTED_MUTATION_NAME_PARTS):
                violations.add(f"mutation-import:{module_name}.{imported_name}")

    bindings: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                local_name = alias.asname or alias.name.split(".", maxsplit=1)[0]
                bindings[local_name] = alias.name if alias.asname else local_name
        elif isinstance(node, ast.ImportFrom):
            module_name = node.module
            if node.level == 1:
                module_name = "tx_trade.orders" + (
                    f".{module_name}" if module_name is not None else ""
                )
            if module_name is not None:
                for alias in node.names:
                    bindings[alias.asname or alias.name] = f"{module_name}.{alias.name}"

    for node in ast.walk(tree):
        resolved = _resolved_name(node, bindings) if isinstance(node, ast.expr) else None
        if isinstance(node, ast.Call) and resolved in {
            "builtins.input",
            "input",
            "json.load",
            "json.loads",
        }:
            violations.add(f"operator-input:{resolved}")
        if isinstance(node, ast.Attribute) and resolved == "sys.stdin":
            violations.add("operator-input:sys.stdin")
        if resolved is not None and "skcom" in resolved.lower():
            violations.add(f"broker-runtime-symbol:{resolved}")
        if isinstance(node, (ast.Call, ast.Attribute)) and resolved is not None:
            member = resolved.rsplit(".", maxsplit=1)[-1].lower()
            if any(part in member for part in _TRUSTED_MUTATION_NAME_PARTS):
                violations.add(f"mutation-symbol:{resolved}")

    return violations


def _safety_violations(
    source: str,
    *,
    allow_runtime_io: bool = False,
    allow_broker_integration: bool = False,
) -> set[str]:
    tree = ast.parse(source)
    bindings: dict[str, str] = {}
    violations: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                local_name = alias.asname or alias.name.split(".", maxsplit=1)[0]
                bindings[local_name] = alias.name if alias.asname else local_name
                if not allow_broker_integration and _is_forbidden_import(alias.name):
                    violations.add(f"import:{alias.name}")
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            if not allow_broker_integration and _is_forbidden_import(node.module):
                violations.add(f"import:{node.module}")
            for alias in node.names:
                local_name = alias.asname or alias.name
                bindings[local_name] = f"{node.module}.{alias.name}"

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            called = _resolved_name(node.func, bindings)
            member = node.func.attr if isinstance(node.func, ast.Attribute) else None
            if (
                (not allow_runtime_io and called in _FORBIDDEN_CALLS)
                or (not allow_broker_integration and member in _FORBIDDEN_SDK_MEMBERS)
                or called
                in {
                    "os.getenv",
                    "os.putenv",
                    "dotenv.load_dotenv",
                    "dotenv.dotenv_values",
                }
            ):
                violations.add(f"call:{called or member}")
            if called is not None and called.startswith("os.environ."):
                violations.add(f"environment:{called}")
            if (
                not allow_broker_integration
                and called is not None
                and any(part in called for part in _ORDER_SDK_NAME_PARTS)
            ):
                violations.add(f"order-sdk-call:{called}")
        elif isinstance(node, (ast.Attribute, ast.Name)):
            name = node.attr if isinstance(node, ast.Attribute) else node.id
            if not allow_broker_integration and any(part in name for part in _ORDER_SDK_NAME_PARTS):
                violations.add(f"order-sdk-name:{name}")
        elif isinstance(node, ast.Subscript):
            accessed = _resolved_name(node.value, bindings)
            if accessed == "os.environ":
                violations.add("environment:os.environ[]")
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if node.value in {".env", "./.env"} or node.value.lower().endswith(".dll"):
                violations.add(f"sensitive-literal:{node.value}")
            for token in _SENSITIVE_LITERALS:
                if token in node.value:
                    violations.add(f"sensitive-literal:{token}")

    return violations


def test_phase3_fresh_imports_have_no_external_side_effects() -> None:
    script = r"""
import builtins
import importlib
import importlib.abc
import json
import os
import pathlib
import socket
import sys

targets = json.loads(sys.argv[1])
blocked_imports = (
    "pythoncom",
    "comtypes",
    "win32com",
    "dotenv",
    "keyring",
    "quote_client",
    "config",
    "winreg",
)

class BlockedImportFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == name or fullname.startswith(name + ".") for name in blocked_imports):
            raise AssertionError("forbidden import attempted: " + fullname)
        return None

def forbidden(operation):
    def fail(*args, **kwargs):
        raise AssertionError("external side effect attempted: " + operation)
    return fail

class PoisonEnvironment:
    def __getitem__(self, key):
        raise AssertionError("environment read attempted: " + str(key))
    def __iter__(self):
        raise AssertionError("environment iteration attempted")
    def __len__(self):
        raise AssertionError("environment length attempted")
    def get(self, key, default=None):
        raise AssertionError("environment read attempted: " + str(key))
    def __contains__(self, key):
        raise AssertionError("environment membership attempted: " + str(key))

# Cache the established market-data dependency before poisoning. The production
# modules under test are not pre-imported, so unsafe target code can never run
# once unguarded merely to initialize an unrelated legacy dependency.
for dependency in (
    "tx_trade.market_data.models",
    "tx_trade.market_data.ports",
):
    importlib.import_module(dependency)

before = set(sys.modules)
sys.meta_path.insert(0, BlockedImportFinder())
builtins.open = forbidden("builtins.open")
os.open = forbidden("os.open")
os.getenv = forbidden("os.getenv")
os.environ = PoisonEnvironment()
pathlib.Path.open = forbidden("Path.open")
pathlib.Path.read_bytes = forbidden("Path.read_bytes")
pathlib.Path.read_text = forbidden("Path.read_text")
pathlib.Path.exists = forbidden("Path.exists")
pathlib.Path.is_dir = forbidden("Path.is_dir")
pathlib.Path.is_file = forbidden("Path.is_file")
pathlib.Path.iterdir = forbidden("Path.iterdir")
pathlib.Path.glob = forbidden("Path.glob")
pathlib.Path.rglob = forbidden("Path.rglob")
pathlib.Path.stat = forbidden("Path.stat")
socket.socket = forbidden("socket.socket")
socket.create_connection = forbidden("socket.create_connection")

for module_name in targets:
    importlib.import_module(module_name)

added = set(sys.modules) - before
forbidden_loaded = sorted(
    name
    for name in added
    if any(name == item or name.startswith(item + ".") for item in blocked_imports)
)
print(json.dumps(forbidden_loaded))
"""
    completed = subprocess.run(
        [sys.executable, "-B", "-c", script, json.dumps(_production_modules())],
        cwd=Path.cwd(),
        env=os.environ.copy(),
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == []


def test_phase3_sources_have_no_external_integration_dependencies() -> None:
    runtime_paths = {
        Path(*module_name.split(".")).with_suffix(".py")
        for module_name in RUNTIME_PERSISTENCE_MODULES
    }
    broker_runtime_paths = {
        Path(*module_name.split(".")).with_suffix(".py") for module_name in BROKER_RUNTIME_MODULES
    }
    for path in _production_paths():
        violations = _safety_violations(
            path.read_text(encoding="utf-8"),
            allow_runtime_io=path in runtime_paths,
            allow_broker_integration=path in broker_runtime_paths,
        )
        assert violations == set(), f"{path}: {sorted(violations)}"


def test_trusted_assessment_modules_are_explicit_fresh_import_targets() -> None:
    modules = _production_modules()

    assert set(TRUSTED_ASSESSMENT_MODULES) <= set(modules)
    assert all(modules.count(module_name) == 1 for module_name in TRUSTED_ASSESSMENT_MODULES)


@pytest.mark.parametrize(
    ("module_name", "runtime"),
    [
        ("tx_trade.orders.live_reconciliation_assessment_contracts", False),
        ("tx_trade.orders.sqlite_live_reconciliation_assessment", True),
    ],
)
def test_trusted_assessment_sources_preserve_read_only_boundaries(
    module_name: str,
    runtime: bool,
) -> None:
    path = Path(*module_name.split(".")).with_suffix(".py")
    violations = _trusted_source_violations(
        path.read_text(encoding="utf-8"),
        runtime=runtime,
    )

    assert violations == set(), f"{path}: {sorted(violations)}"


def test_production_path_enumeration_includes_future_capital_and_live_modules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    future_capital = Path("future/capital/future_adapter.py")
    future_live = Path("future/orders/live_future.py")
    future_sqlite_live = Path("future/orders/sqlite_live_future.py")

    class CapitalRoot:
        def rglob(self, pattern: str) -> list[Path]:
            assert pattern == "*.py"
            return [future_capital]

    class LiveRoot:
        def glob(self, pattern: str) -> list[Path]:
            if pattern == "live_*.py":
                return [future_live]
            assert pattern == "sqlite_live_*.py"
            return [future_sqlite_live]

    capital_root = CapitalRoot()
    live_root = LiveRoot()
    monkeypatch.setattr(sys.modules[__name__], "CAPITAL_ROOT", capital_root)
    monkeypatch.setattr(sys.modules[__name__], "LIVE_ORDER_ROOT", live_root)

    assert _production_paths() == (future_capital, future_live, future_sqlite_live)


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("import comtypes.client as client\nclient.CreateObject(object)", "import:comtypes.client"),
        ("from pythoncom import CoInitialize as initialize\ninitialize()", "import:pythoncom"),
        ("import os as operating\noperating.getenv('ACCOUNT')", "call:os.getenv"),
        ("from pathlib import Path\nPath('secret').read_text()", "call:pathlib.Path.read_text"),
        ("adapter.SKReplyLib_ConnectByID('secret')", "call:adapter.SKReplyLib_ConnectByID"),
        ("order.SKOrderLib_SendFutureOrder('secret')", "call:order.SKOrderLib_SendFutureOrder"),
        ("sdk.SKOrderLib = sdk.CreateObject()", "order-sdk-name:SKOrderLib"),
        ("reply.SKReplyLib_ConnectByID(user)", "call:reply.SKReplyLib_ConnectByID"),
        ("broker.SendOrder(order)", "order-sdk-call:broker.SendOrder"),
        ("open('SKCOM.dll', 'rb')", "call:open"),
    ],
)
def test_safety_scanner_detects_alias_aware_hostile_fragments(
    source: str,
    expected: str,
) -> None:
    assert expected in _safety_violations(source)


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("import socket", "import:socket"),
        ("import ctypes", "import:ctypes"),
        ("import os\nos.getenv('ACCOUNT')", "call:os.getenv"),
        ("import os\nvalue = os.environ['ACCOUNT']", "environment:os.environ[]"),
        ("import comtypes.client", "import:comtypes.client"),
        ("broker.SendOrder(order)", "order-sdk-call:broker.SendOrder"),
    ],
)
def test_runtime_persistence_scanner_still_rejects_external_integrations(
    source: str,
    expected: str,
) -> None:
    assert expected in _safety_violations(source, allow_runtime_io=True)


def test_runtime_persistence_scanner_allows_explicit_local_sqlite_io() -> None:
    source = """
import os
import sqlite3
from pathlib import Path

def open_journal(path):
    descriptor = os.open(path, os.O_RDONLY)
    Path(path).read_text(encoding="utf-8")
    return sqlite3.connect(path), descriptor
"""
    assert _safety_violations(source, allow_runtime_io=True) == set()


@pytest.mark.parametrize(
    ("source", "runtime", "expected"),
    [
        ("import sqlite3", False, "trusted-import:sqlite3"),
        ("import json as codec\ncodec.loads(raw)", False, "operator-input:json.loads"),
        ("import sys\nline = sys.stdin.readline()", False, "operator-input:sys.stdin"),
        (
            "from .live_reconciliation_commit_contracts import ReconciliationCommitRequest",
            False,
            "mutation-import:tx_trade.orders.live_reconciliation_commit_contracts",
        ),
        (
            "from tx_trade.broker.capital.quote_adapter import CapitalQuoteStaAdapter",
            True,
            "trusted-import:tx_trade.broker.capital.quote_adapter",
        ),
        (
            "journal.claim_dispatch(command)",
            True,
            "mutation-symbol:journal.claim_dispatch",
        ),
        ("sdk.SKCOMLib.SendOrder(order)", True, "broker-runtime-symbol:sdk.SKCOMLib"),
        ("import app.credentials", True, "trusted-import:app.credentials"),
        ("import websockets", True, "trusted-import:websockets"),
    ],
)
def test_trusted_source_scanner_rejects_hostile_fragments(
    source: str,
    runtime: bool,
    expected: str,
) -> None:
    assert expected in _trusted_source_violations(source, runtime=runtime)


def test_trusted_runtime_scanner_allows_only_approved_sqlite_read_sources() -> None:
    source = """
from . import sqlite_live_journal_inspection
from .sqlite_live_order_journal import SQLiteLiveOrderJournal

inspection = sqlite_live_journal_inspection.inspect_live_journal(source_path)
journal = SQLiteLiveOrderJournal
"""

    assert _trusted_source_violations(source, runtime=True) == set()


def test_future_capital_module_does_not_inherit_broker_runtime_allowance() -> None:
    future_path = CAPITAL_ROOT / "future_adapter.py"
    broker_runtime_paths = {
        Path(*module_name.split(".")).with_suffix(".py") for module_name in BROKER_RUNTIME_MODULES
    }
    source = """
import socket
import comtypes.client
"""

    assert future_path not in broker_runtime_paths
    violations = _safety_violations(
        source,
        allow_broker_integration=future_path in broker_runtime_paths,
    )
    assert "import:socket" in violations
    assert "import:comtypes.client" in violations


@pytest.mark.parametrize(
    "source",
    [
        "class LiveOrder: pass",
        "def cancel(command): return command",
        "def amend(command): return command",
        "class BrokerReplySourcePort: pass",
        "class CapitalOnNewDataRecord: pass",
        "ON_NEW_DATA_SCHEMA_VERSION = 'capital.on_new_data.v1'",
    ],
)
def test_safety_scanner_allows_broker_neutral_domain_words(source: str) -> None:
    assert _safety_violations(source) == set()


def test_lazy_packages_preserve_existing_submodule_attributes() -> None:
    script = """
import tx_trade.orders as orders
import tx_trade.broker.capital as capital

assert orders.contracts.OrderIntent
assert orders.ports.PaperBrokerPort
assert orders.state_machine.validate_order_transition
assert capital.contracts.QuoteSnapshotRaw
assert capital.com_backend.ComtypesQuoteBackend
assert capital.quote_adapter.CapitalQuoteStaAdapter
"""
    completed = subprocess.run(
        [sys.executable, "-B", "-c", script],
        cwd=Path.cwd(),
        env=os.environ.copy(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
