from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

PAPER_EXECUTION_PATHS = (
    Path("tx_trade/orders/execution_policies.py"),
    Path("tx_trade/orders/position_ledger.py"),
    Path("tx_trade/orders/matching.py"),
    Path("tx_trade/orders/paper_broker.py"),
)
SENSITIVE_ENV_NAMES = (
    "TX_TRADE_ACCOUNT",
    "TX_TRADE_PASSWORD",
    "TX_TRADE_SKCOM_DLL_PATH",
)

_NONDETERMINISTIC_CALLS = {
    "uuid.uuid4",
    "time.time",
    "datetime.datetime.today",
    "datetime.datetime.now",
    "datetime.datetime.utcnow",
    "datetime.date.today",
    "os.urandom",
}


def _resolved_name(node: ast.expr, bindings: dict[str, str]) -> str | None:
    if isinstance(node, ast.Name):
        return bindings.get(node.id, node.id)
    if isinstance(node, ast.Attribute):
        owner = _resolved_name(node.value, bindings)
        return None if owner is None else f"{owner}.{node.attr}"
    return None


def _nondeterministic_dependencies(source: str) -> set[str]:
    tree = ast.parse(source)
    bindings: dict[str, str] = {}
    findings: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                local_name = alias.asname or alias.name.split(".", maxsplit=1)[0]
                bindings[local_name] = alias.name if alias.asname else local_name
                module_root = alias.name.split(".", maxsplit=1)[0]
                if module_root in {"random", "secrets"}:
                    findings.add(module_root)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            module_root = node.module.split(".", maxsplit=1)[0]
            if module_root in {"random", "secrets"}:
                findings.add(module_root)
            for alias in node.names:
                local_name = alias.asname or alias.name
                bindings[local_name] = f"{node.module}.{alias.name}"

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        called = _resolved_name(node.func, bindings)
        if called in _NONDETERMINISTIC_CALLS:
            findings.add(called)
        elif called is not None and called.startswith(("random.", "secrets.")):
            findings.add(called)
    return findings


def test_matcher_imports_do_not_load_live_or_configuration_dependencies() -> None:
    script = """
import importlib
import json
import sys

before = set(sys.modules)
for module_name in (
    "tx_trade.orders.execution_policies",
    "tx_trade.orders.position_ledger",
    "tx_trade.orders.matching",
    "tx_trade.orders.paper_broker",
):
    importlib.import_module(module_name)
added = set(sys.modules) - before
forbidden = sorted(
    name
    for name in added
    if name in {
        "pythoncom",
        "comtypes",
        "comtypes.client",
        "win32com",
        "win32com.client",
        "dotenv",
        "dotenv.main",
        "quote_client",
        "config",
    }
    or name.startswith(("tx_trade.broker", "tx_trade.app", "dotenv."))
)
print(json.dumps(forbidden))
"""
    environment = os.environ.copy()
    for name in SENSITIVE_ENV_NAMES:
        environment.pop(name, None)

    completed = subprocess.run(
        [sys.executable, "-B", "-c", script],
        cwd=Path.cwd(),
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == []


def test_matcher_sources_have_no_live_execution_or_credential_symbols() -> None:
    source = "\n".join(path.read_text(encoding="utf-8") for path in PAPER_EXECUTION_PATHS)
    forbidden = (
        "python" + "com",
        "win32" + "com",
        "comtypes" + ".client",
        "SK" + "OrderLib",
        "SK" + "ReplyLib",
        "Connect" + "ByID",
        "Send" + "Order",
        "Order" + "Lib",
        "Reply" + "Lib",
        "TX_TRADE_" + "ACCOUNT",
        "TX_TRADE_" + "PASSWORD",
        "TX_TRADE_" + "SKCOM_DLL_PATH",
        "load_" + "dotenv",
        "dotenv_" + "values",
    )

    assert not any(token in source for token in forbidden)


def test_matcher_sources_have_no_random_or_wall_clock_dependencies() -> None:
    for path in PAPER_EXECUTION_PATHS:
        findings = _nondeterministic_dependencies(path.read_text(encoding="utf-8"))
        assert findings == set(), f"{path}: {sorted(findings)}"


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("import uuid as identifier\nidentifier.uuid4()", "uuid.uuid4"),
        ("from uuid import uuid4 as make_id\nmake_id()", "uuid.uuid4"),
        ("import random as entropy\nentropy.random()", "random"),
        ("from random import randint as choose\nchoose(1, 2)", "random"),
        ("import secrets as secure\nsecure.token_bytes(16)", "secrets"),
        ("from secrets import token_hex as token\ntoken()", "secrets"),
        ("import os as operating_system\noperating_system.urandom(16)", "os.urandom"),
        ("from os import urandom as entropy\nentropy(16)", "os.urandom"),
        ("import time as clock\nclock.time()", "time.time"),
        ("from time import time as current\ncurrent()", "time.time"),
        (
            "import datetime as dates\ndates.datetime.today()",
            "datetime.datetime.today",
        ),
        (
            "from datetime import datetime as DateTime\nDateTime.now()",
            "datetime.datetime.now",
        ),
        (
            "from datetime import datetime\ndatetime.utcnow()",
            "datetime.datetime.utcnow",
        ),
        (
            "from datetime import date as Date\nDate.today()",
            "datetime.date.today",
        ),
    ],
)
def test_scanner_detects_alias_aware_hostile_snippets(
    source: str,
    expected: str,
) -> None:
    assert expected in _nondeterministic_dependencies(source)


@pytest.mark.parametrize(
    "source",
    [
        "from uuid import uuid5\nuuid5(namespace, name)",
        "import hashlib as hashing\nhashing.sha256(payload)",
        "from hashlib import sha256 as digest\ndigest(payload)",
        "from decimal import localcontext\nwith localcontext():\n    pass",
        (
            "from decimal import Decimal, localcontext\n"
            "with localcontext() as context:\n"
            "    value = Decimal('1')"
        ),
    ],
)
def test_scanner_allows_deterministic_dependencies(source: str) -> None:
    assert _nondeterministic_dependencies(source) == set()
