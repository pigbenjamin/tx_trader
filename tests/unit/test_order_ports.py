from __future__ import annotations

from uuid import UUID

from tx_trade.orders import (
    CancelIntent,
    OrderCommandResult,
    OrderIntent,
    PaperBrokerPort,
    PaperEvent,
    PaperEventSink,
    PaperOrder,
    PaperPosition,
)


class FakeBroker:
    def submit(self, intent: OrderIntent) -> OrderCommandResult:
        raise NotImplementedError

    def cancel(self, request: CancelIntent) -> OrderCommandResult:
        raise NotImplementedError

    def get_order(self, paper_order_id: UUID) -> PaperOrder | None:
        return None

    def list_orders(self) -> tuple[PaperOrder, ...]:
        return ()

    def list_positions(self) -> tuple[PaperPosition, ...]:
        return ()

    def get_position(
        self,
        strategy_id: str,
        account_id: str,
        instrument_id: str,
    ) -> PaperPosition | None:
        return None


class FakeEventSink:
    def __init__(self) -> None:
        self.events: list[PaperEvent] = []

    def publish(self, event: PaperEvent) -> None:
        self.events.append(event)


def test_fake_objects_structurally_satisfy_runtime_protocols() -> None:
    assert isinstance(FakeBroker(), PaperBrokerPort)
    assert isinstance(FakeEventSink(), PaperEventSink)


def test_broker_port_does_not_require_market_data_input() -> None:
    method_names = {name for name in PaperBrokerPort.__dict__ if not name.startswith("_")}

    assert method_names == {
        "submit",
        "cancel",
        "get_order",
        "list_orders",
        "list_positions",
        "get_position",
    }
