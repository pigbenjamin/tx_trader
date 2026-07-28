from __future__ import annotations

from hashlib import sha256
from uuid import UUID

import pytest

from tx_trade.market_data.fixtures import make_offline_fixture_envelopes
from tx_trade.orders import PaperBrokerLimits
from tx_trade.orders.paper_broker import PaperBroker
from tx_trade.research.contracts import CheckpointKind, VersionedCheckpoint
from tx_trade.strategy import (
    NoOpStrategy,
    PaperReplayCoordinator,
    StrategyCheckpointError,
    StrategyDecision,
    StrategyExecutionMode,
    StrategyRegistration,
)

RUN_ID = UUID("7eb1504b-fd69-467f-b2b8-867925855c82")
FINGERPRINT = f"sha256:{sha256(b'noop-v1').hexdigest()}"


class CountingStrategy:
    def __init__(self) -> None:
        self.calls = 0

    def decide(self, envelope: object, context: object) -> StrategyDecision:
        self.calls += 1
        return StrategyDecision(commands=())


def _broker() -> PaperBroker:
    return PaperBroker(
        paper_run_id=RUN_ID,
        limits=PaperBrokerLimits(
            max_orders=10,
            max_open_orders=10,
            max_fills=10,
            max_events=20,
            max_market_data_records=10,
            max_instrument_versions=10,
        ),
    )


def test_checkpoint_round_trip_is_byte_stable_and_retry_does_not_evaluate() -> None:
    envelope = make_offline_fixture_envelopes()[0]
    strategy = CountingStrategy()
    registration = StrategyRegistration("noop", strategy, FINGERPRINT)
    broker = _broker()
    coordinator = PaperReplayCoordinator(
        broker=broker,
        registrations=(registration,),
        mode=StrategyExecutionMode.PAPER,
        max_decision_records=2,
    )
    coordinator.publish(envelope)

    checkpoint = coordinator.export_checkpoint()
    restored_strategy = CountingStrategy()
    restored = PaperReplayCoordinator.restore_checkpoint(
        checkpoint,
        broker=broker,
        registrations=(StrategyRegistration("noop", restored_strategy, FINGERPRINT),),
    )
    restored.publish(envelope)

    assert checkpoint.kind is CheckpointKind.COORDINATOR
    assert restored.export_checkpoint() == checkpoint
    assert strategy.calls == 1
    assert restored_strategy.calls == 0


def test_observe_only_pending_decision_round_trips_without_evaluation() -> None:
    envelope = make_offline_fixture_envelopes()[0]
    original = CountingStrategy()
    coordinator = PaperReplayCoordinator(
        broker=_broker(),
        registrations=(StrategyRegistration("noop", original),),
        mode=StrategyExecutionMode.OBSERVE_ONLY,
        max_decision_records=1,
    )
    coordinator.publish(envelope)
    restored_strategy = CountingStrategy()
    restored = PaperReplayCoordinator.restore_checkpoint(
        coordinator.export_checkpoint(),
        broker=_broker(),
        registrations=(StrategyRegistration("noop", restored_strategy),),
    )

    restored.publish(envelope)

    assert restored_strategy.calls == 0
    assert restored.decision_records()[0].batch_result is None


def test_restore_rejects_registration_identity_and_corruption_without_echo() -> None:
    coordinator = PaperReplayCoordinator(
        broker=_broker(),
        registrations=(StrategyRegistration("noop", NoOpStrategy(), FINGERPRINT),),
        mode=StrategyExecutionMode.PAPER,
        max_decision_records=1,
    )
    checkpoint = coordinator.export_checkpoint()
    with pytest.raises(StrategyCheckpointError):
        PaperReplayCoordinator.restore_checkpoint(
            checkpoint,
            broker=_broker(),
            registrations=(StrategyRegistration("other", NoOpStrategy(), FINGERPRINT),),
        )

    payload = b'{"mode":{"$enum":"StrategyExecutionMode","value":"paper"},"x":"secret"}'
    corrupt = VersionedCheckpoint.create(
        kind=CheckpointKind.COORDINATOR,
        schema_version=1,
        payload=payload,
    )
    with pytest.raises(
        StrategyCheckpointError,
        match=r"^strategy coordinator checkpoint is invalid$",
    ) as raised:
        PaperReplayCoordinator.restore_checkpoint(
            corrupt,
            broker=_broker(),
            registrations=(StrategyRegistration("noop", NoOpStrategy(), FINGERPRINT),),
        )
    assert "secret" not in str(raised.value)


def test_restore_rejects_unknown_version_and_kind() -> None:
    coordinator = PaperReplayCoordinator(
        broker=_broker(),
        registrations=(StrategyRegistration("noop", NoOpStrategy()),),
        mode=StrategyExecutionMode.PAPER,
        max_decision_records=1,
    )
    checkpoint = coordinator.export_checkpoint()
    invalid = (
        VersionedCheckpoint.create(
            kind=CheckpointKind.COORDINATOR,
            schema_version=2,
            payload=checkpoint.payload,
        ),
        VersionedCheckpoint.create(
            kind=CheckpointKind.BROKER,
            schema_version=1,
            payload=checkpoint.payload,
        ),
    )
    for item in invalid:
        with pytest.raises(StrategyCheckpointError):
            PaperReplayCoordinator.restore_checkpoint(
                item,
                broker=_broker(),
                registrations=(StrategyRegistration("noop", NoOpStrategy()),),
            )


def test_restore_sanitizes_deeply_nested_json_recursion() -> None:
    payload = b'{"x":' + (b"[" * 1_100) + b"0" + (b"]" * 1_100) + b"}"
    checkpoint = VersionedCheckpoint.create(
        kind=CheckpointKind.COORDINATOR,
        schema_version=1,
        payload=payload,
    )

    with pytest.raises(
        StrategyCheckpointError,
        match=r"^strategy coordinator checkpoint is invalid$",
    ) as raised:
        PaperReplayCoordinator.restore_checkpoint(
            checkpoint,
            broker=_broker(),
            registrations=(StrategyRegistration("noop", NoOpStrategy()),),
        )

    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True
    assert payload.decode("utf-8") not in str(raised.value)
