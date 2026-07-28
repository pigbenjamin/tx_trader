from __future__ import annotations

from datetime import datetime
from threading import Thread
from uuid import UUID

from tx_trade.market_data.legacy_quote_snapshot import LegacyQuoteSnapshotProjector
from tx_trade.market_data.models import (
    CapturedAdapterDiagnostic,
    CapturedConnectionNotification,
    CapturedKind,
    CapturedMarketDataEvent,
    CapturedQuoteSnapshot,
    CapturedServerTimeNotification,
    CapturedStockListNotification,
    CapturedTickNotification,
    SourceMode,
    TAIPEI,
)

NOW = datetime(2026, 7, 26, 9, 0, tzinfo=TAIPEI)
SESSION_ID = UUID("00000000-0000-0000-0000-000000000001")


def _event(kind, payload, sequence: int) -> CapturedMarketDataEvent:
    return CapturedMarketDataEvent(
        captured_kind=kind,
        payload=payload,
        raw_payload=None,
        source="test",
        source_mode=SourceMode.LIVE,
        session_id=SESSION_ID,
        connection_generation=0,
        sequence=sequence,
        broker_sequence=None,
        received_at=NOW,
        event_at=None,
        trading_day=None,
        metadata_version=None,
        dedupe_candidate=None,
    )


def _quote(sequence: int, *, long: bool = False):
    payload = CapturedQuoteSnapshot(
        market_no_raw=0,
        stock_idx_raw=sequence,
        bid_raw=1,
        ask_raw=2,
        last_raw=3,
        bid_qty_raw=None,
        ask_qty_raw=None,
        last_qty_raw=None,
        is_long_callback=long,
        callback_sequence=sequence,
        received_at=NOW,
    )
    return _event(CapturedKind.QUOTE_SNAPSHOT, payload, sequence)


def _tick(sequence: int, *, long: bool = False):
    payload = CapturedTickNotification(
        market_no_raw=1,
        stock_idx_raw=2,
        source_pointer_raw=sequence,
        date_raw=20260726,
        time_hms_raw=90102,
        time_subsecond_raw=3,
        bid_raw=4,
        ask_raw=5,
        close_raw=6,
        quantity_raw=7,
        simulate_raw=0,
        is_long_callback=long,
        callback_sequence=sequence,
        received_at=NOW,
    )
    return _event(CapturedKind.TICK_NOTIFICATION, payload, sequence)


def test_projects_exact_legacy_shapes_and_connection_state():
    projector = LegacyQuoteSnapshotProjector()
    connection = CapturedConnectionNotification(3003, 0, 0, NOW)
    server_time = CapturedServerTimeNotification(9, 1, 2, 3, 1, NOW)
    stock_list = CapturedStockListNotification(4, "TX00", 2, NOW)

    projector.project(_event(CapturedKind.CONNECTION_NOTIFICATION, connection, 0))
    projector.project(_event(CapturedKind.SERVER_TIME_NOTIFICATION, server_time, 1))
    projector.project(_event(CapturedKind.STOCK_LIST_NOTIFICATION, stock_list, 2))
    projector.project(_quote(3))
    projector.project(_quote(4, long=True))
    projector.project(_tick(5, long=True))
    snapshot = projector.snapshot()

    assert snapshot["connection"] == {
        "stocks_ready": True,
        "last_kind": 3003,
        "last_code": 0,
    }
    assert snapshot["server_time"] == {
        "hour": 9,
        "minute": 1,
        "second": 2,
        "total": 3,
    }
    assert snapshot["stock_list"] == {
        "market_no": 4,
        "product_data": "TX00",
    }
    assert snapshot["quotes"] == [
        {"market_no": 0, "stock_idx": 3},
        {"market_no": 0, "stock_idx": 4, "long": True},
    ]
    assert snapshot["ticks"] == [
        {
            "market_no": 1,
            "stock_idx": 2,
            "ptr": 5,
            "date": 20260726,
            "timehms": 90102,
            "timemillismicros": 3,
            "bid": 4,
            "ask": 5,
            "close": 6,
            "qty": 7,
            "simulate": 0,
            "long": True,
        }
    ]


def test_histories_are_bounded_and_snapshots_are_defensive():
    projector = LegacyQuoteSnapshotProjector(
        quote_capacity=2, tick_capacity=1, diagnostic_capacity=1
    )
    for sequence in range(3):
        projector.project(_quote(sequence))
        projector.project(_tick(sequence))

    first = projector.snapshot()
    assert [item["stock_idx"] for item in first["quotes"]] == [1, 2]
    assert [item["ptr"] for item in first["ticks"]] == [2]
    first["quotes"][0]["stock_idx"] = 999
    first["connection"]["stocks_ready"] = True
    second = projector.snapshot()
    assert second["quotes"][0]["stock_idx"] == 1
    assert second["connection"]["stocks_ready"] is False


def test_diagnostic_is_additive_and_does_not_create_market_data():
    projector = LegacyQuoteSnapshotProjector(diagnostic_capacity=1)
    payload = CapturedAdapterDiagnostic(
        diagnostic_kind="adapter_error",
        market_no_raw=None,
        stock_idx_raw=None,
        error_code_raw=7,
        message="safe",
        received_at=NOW,
        attempt=1,
        connection_generation=0,
        callback_sequence=0,
        raw_notification={"nested": {"value": 1}},
    )
    projector.project(_event(CapturedKind.ADAPTER_DIAGNOSTIC, payload, 0))
    snapshot = projector.snapshot()
    assert snapshot["quotes"] == []
    assert snapshot["ticks"] == []
    assert snapshot["diagnostics"][0]["raw_notification"] == {"nested": {"value": 1}}


def test_project_and_snapshot_are_thread_safe():
    projector = LegacyQuoteSnapshotProjector(quote_capacity=25)

    def writer(start: int) -> None:
        for sequence in range(start, start + 100):
            projector.project(_quote(sequence))
            projector.snapshot()

    threads = [Thread(target=writer, args=(offset,)) for offset in (0, 100)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(projector.snapshot()["quotes"]) == 25
