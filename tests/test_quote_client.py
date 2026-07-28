import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from quote_client import QuoteClient

pytestmark = pytest.mark.legacy_com


def test_login_and_monitor():
    client = QuoteClient()
    client.initialize()
    result = client.login("demo", "demo")
    assert isinstance(result, dict)
    assert "success" in result
    assert isinstance(client.enter_monitor(), dict)


def test_request_stocks():
    client = QuoteClient()
    client.initialize()
    results = client.request_stocks(["TX00"])
    assert isinstance(results, dict)
    assert results.get("symbols") == ["TX00"]
