from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ORDER_ROOT = Path("tx_trade/orders")
RESEARCH_PAPER_PATHS = (
    *sorted(Path("tx_trade/strategy").rglob("*.py")),
    Path("tx_trade/app/research_paper_config.py"),
    Path("tx_trade/app/research_output.py"),
    Path("tx_trade/app/research_paper.py"),
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
                resolved = alias.name if alias.asname else local_name
                bindings[local_name] = resolved
                if alias.name.split(".", maxsplit=1)[0] in {"random", "secrets"}:
                    findings.add(alias.name.split(".", maxsplit=1)[0])
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


def test_order_imports_do_not_load_com_broker_config_or_credentials() -> None:
    script = """
import importlib
import json
import sys

before = set(sys.modules)
for module_name in (
    "tx_trade.orders",
    "tx_trade.orders.contracts",
    "tx_trade.orders.ports",
    "tx_trade.orders.state_machine",
):
    importlib.import_module(module_name)
added = set(sys.modules) - before
forbidden = sorted(
    name
    for name in added
    if name in {"pythoncom", "comtypes.client", "quote_client", "config"}
    or name.startswith("tx_trade.broker")
    or name.startswith("tx_trade.app")
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


def test_order_sources_have_no_live_execution_or_credential_symbols() -> None:
    source = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(ORDER_ROOT.rglob("*.py"))
    )
    forbidden = (
        "python" + "com",
        "comtypes" + ".client",
        "SK" + "OrderLib",
        "SK" + "ReplyLib",
        "Connect" + "ByID",
        "Send" + "Order",
        "TX_TRADE_" + "ACCOUNT",
        "TX_TRADE_" + "PASSWORD",
        "TX_TRADE_" + "SKCOM_DLL_PATH",
        "load_" + "dotenv",
        "dotenv_" + "values",
    )

    assert not any(token in source for token in forbidden)


def test_order_sources_have_no_random_or_wall_clock_dependencies() -> None:
    for path in sorted(ORDER_ROOT.rglob("*.py")):
        findings = _nondeterministic_dependencies(path.read_text(encoding="utf-8"))
        assert findings == set(), f"{path}: {sorted(findings)}"


def test_research_paper_sources_have_no_random_or_wall_clock_dependencies() -> None:
    for path in RESEARCH_PAPER_PATHS:
        findings = _nondeterministic_dependencies(path.read_text(encoding="utf-8"))
        assert findings == set(), f"{path}: {sorted(findings)}"


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("import uuid as identifier\nidentifier.uuid4()", "uuid.uuid4"),
        ("from uuid import uuid4 as make_id\nmake_id()", "uuid.uuid4"),
        ("import time as clock\nclock.time()", "time.time"),
        ("from time import time as current_time\ncurrent_time()", "time.time"),
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
        ("import random as entropy\nentropy.random()", "random"),
        ("import secrets as secure\nsecure.token_bytes(16)", "secrets"),
        ("from secrets import token_hex\ntoken_hex()", "secrets"),
        (
            "import os as operating_system\noperating_system.urandom(16)",
            "os.urandom",
        ),
        ("from os import urandom as bytes_from_os\nbytes_from_os(16)", "os.urandom"),
    ],
)
def test_nondeterminism_scanner_detects_alias_aware_malicious_fragments(
    source: str, expected: str
) -> None:
    assert expected in _nondeterministic_dependencies(source)
