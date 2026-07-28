from __future__ import annotations

import json
from decimal import Decimal
from uuid import UUID

import pytest

from tx_trade.market_data.fixtures import (
    OFFLINE_FIXTURE_SESSION_ID,
    make_offline_fixture_envelopes,
)
from tx_trade.market_data.models import Quote
from tx_trade.orders.contracts import (
    OrderIntent,
    OrderSide,
    OrderType,
    PaperBrokerLimits,
    TimeInForce,
)
from tx_trade.orders.paper_broker import (
    PaperBroker,
    PaperBrokerCheckpointError,
)
from tx_trade.orders.ports import (
    CheckpointablePaperBrokerPort,
    TransactionalPaperBrokerPort,
)
from tx_trade.research.contracts import CheckpointKind, VersionedCheckpoint

RUN_ID = UUID("11111111-1111-1111-1111-111111111111")
LIMITS = PaperBrokerLimits(
    max_orders=20,
    max_open_orders=20,
    max_fills=50,
    max_events=100,
    max_market_data_records=100,
    max_instrument_versions=20,
)


def _broker() -> PaperBroker:
    return PaperBroker(
        paper_run_id=RUN_ID,
        expected_source_session_id=OFFLINE_FIXTURE_SESSION_ID,
        limits=LIMITS,
    )


def _intent(client_order_id: str) -> OrderIntent:
    quote = make_offline_fixture_envelopes()[3].payload
    assert isinstance(quote, Quote)
    return OrderIntent(
        strategy_id="strategy",
        client_order_id=client_order_id,
        account_id="paper",
        instrument_id=quote.instrument_id,
        side=OrderSide.BUY,
        quantity=Decimal("1"),
        order_type=OrderType.MARKET,
        limit_price=None,
        time_in_force=TimeInForce.DAY,
        day_trade=False,
        created_at=quote.received_at,
    )


def test_checkpoint_round_trip_is_byte_stable_and_preserves_next_ids() -> None:
    broker = _broker()
    assert isinstance(broker, TransactionalPaperBrokerPort)
    assert isinstance(broker, CheckpointablePaperBrokerPort)
    envelopes = make_offline_fixture_envelopes()
    broker.process_market_data(envelopes[2])
    accepted = broker.submit(_intent("first"))
    broker.process_market_data(envelopes[3])

    checkpoint = broker.export_checkpoint()
    restored = PaperBroker.restore_checkpoint(checkpoint)

    assert checkpoint.kind is CheckpointKind.BROKER
    assert restored.snapshot() == broker.snapshot()
    assert restored.export_checkpoint() == checkpoint
    assert restored.submit(_intent("first")) == broker.submit(_intent("first"))
    assert restored.submit(_intent("second")) == broker.submit(_intent("second"))
    assert accepted.paper_order_id == broker.list_orders()[0].paper_order_id


@pytest.mark.parametrize(
    "payload",
    (
        b"{}",
        b'{"execution_config":null,"limits":null,"paper_run_id":null,"state":null}',
        b'{"x":1,"x":2}',
    ),
)
def test_checkpoint_rejects_corrupt_payload_without_echoing_it(payload: bytes) -> None:
    checkpoint = VersionedCheckpoint.create(
        kind=CheckpointKind.BROKER,
        schema_version=1,
        payload=payload,
    )
    with pytest.raises(
        PaperBrokerCheckpointError,
        match=r"^paper broker checkpoint is invalid$",
    ) as raised:
        PaperBroker.restore_checkpoint(checkpoint)
    assert payload.decode("utf-8") not in str(raised.value)


def test_checkpoint_rejects_unknown_version_and_kind() -> None:
    checkpoint = _broker().export_checkpoint()
    unknown_version = VersionedCheckpoint.create(
        kind=CheckpointKind.BROKER,
        schema_version=2,
        payload=checkpoint.payload,
    )
    wrong_kind = VersionedCheckpoint.create(
        kind=CheckpointKind.COORDINATOR,
        schema_version=1,
        payload=checkpoint.payload,
    )
    with pytest.raises(PaperBrokerCheckpointError):
        PaperBroker.restore_checkpoint(unknown_version)
    with pytest.raises(PaperBrokerCheckpointError):
        PaperBroker.restore_checkpoint(wrong_kind)


def _accepted_checkpoint_document() -> dict[str, object]:
    broker = _broker()
    broker.submit(_intent("first"))
    return json.loads(broker.export_checkpoint().payload)


def _checkpoint_from_document(document: dict[str, object]) -> VersionedCheckpoint:
    return VersionedCheckpoint.create(
        kind=CheckpointKind.BROKER,
        schema_version=1,
        payload=json.dumps(
            document, ensure_ascii=True, separators=(",", ":"), sort_keys=True
        ).encode(),
    )


@pytest.mark.parametrize("mutation", ("missing_cache", "valid_digest", "projection"))
def test_checkpoint_rejects_cross_collection_tampering(mutation: str) -> None:
    document = _accepted_checkpoint_document()
    state = document["state"]
    assert isinstance(state, dict)
    if mutation == "missing_cache":
        state["submit_outcomes"] = {"$map": []}
    elif mutation == "valid_digest":
        outcomes = state["submit_outcomes"]
        assert isinstance(outcomes, dict)
        outcome_map = outcomes["$map"]
        assert isinstance(outcome_map, list)
        record = outcome_map[0][1]
        assert isinstance(record, dict)
        record["primary_digest"] = "0" * 64
    else:
        orders = state["orders"]
        assert isinstance(orders, dict)
        order_map = orders["$map"]
        assert isinstance(order_map, list)
        order = order_map[0][1]
        assert isinstance(order, dict)
        order["status"] = {"$enum": "OrderStatus", "value": "cancelled"}

    with pytest.raises(PaperBrokerCheckpointError):
        PaperBroker.restore_checkpoint(_checkpoint_from_document(document))


def test_checkpoint_sanitizes_deeply_nested_valid_digest_payload() -> None:
    payload = ("[" * 1_000 + "null" + "]" * 1_000).encode()
    checkpoint = VersionedCheckpoint.create(
        kind=CheckpointKind.BROKER,
        schema_version=1,
        payload=payload,
    )

    with pytest.raises(
        PaperBrokerCheckpointError,
        match=r"^paper broker checkpoint is invalid$",
    ) as raised:
        PaperBroker.restore_checkpoint(checkpoint)

    assert raised.value.__cause__ is None
    assert payload.decode() not in str(raised.value)
