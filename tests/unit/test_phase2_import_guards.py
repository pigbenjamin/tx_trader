import json
import os
import subprocess
import sys
from pathlib import Path


REPLAY_ROOT = Path("tx_trade/replay")
PHASE2_APP_SOURCES = (
    Path("tx_trade/app/phase2.py"),
    Path("tx_trade/app/phase2_config.py"),
)
SENSITIVE_ENV_NAMES = (
    "TX_TRADE_ACCOUNT",
    "TX_TRADE_PASSWORD",
    "TX_TRADE_SKCOM_DLL_PATH",
)


def test_replay_imports_do_not_load_com_live_broker_or_root_config():
    script = """
import importlib
import json
import sys

before = set(sys.modules)
for module_name in (
    "tx_trade.replay",
    "tx_trade.replay.runtime",
    "tx_trade.replay.sqlite_source",
    "tx_trade.app.phase2_config",
    "tx_trade.app.phase2",
):
    importlib.import_module(module_name)
added = set(sys.modules) - before
forbidden = sorted(
    name
    for name in added
    if name in {"pythoncom", "comtypes.client", "quote_client", "config"}
    or name.startswith("tx_trade.broker.capital")
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


def test_replay_production_source_has_no_com_order_or_reply_symbols():
    paths = (*sorted(REPLAY_ROOT.rglob("*.py")), *PHASE2_APP_SOURCES)
    source = "\n".join(path.read_text(encoding="utf-8") for path in paths)
    forbidden = (
        "python" + "com",
        "comtypes" + ".client",
        "SK" + "OrderLib",
        "SK" + "ReplyLib",
        "Connect" + "ByID",
        "On" + "NewData",
        "On" + "StrategyData",
        "Send" + "Order",
    )

    assert not any(token in source for token in forbidden)


def test_replay_production_source_does_not_read_credentials_or_dotenv():
    paths = (*sorted(REPLAY_ROOT.rglob("*.py")), *PHASE2_APP_SOURCES)
    source = "\n".join(path.read_text(encoding="utf-8") for path in paths)
    forbidden = (
        "TX_TRADE_" + "ACCOUNT",
        "TX_TRADE_" + "PASSWORD",
        "TX_TRADE_" + "SKCOM_DLL_PATH",
        "load_" + "dotenv",
        "dotenv_" + "values",
        '".' + 'env"',
        "'." + "env'",
    )

    assert not any(token in source for token in forbidden)
