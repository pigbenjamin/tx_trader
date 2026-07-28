"""Broker-neutral order contracts and paper-execution boundaries."""

from .contracts import (
    ExecutionProvenance,
    OrderIntent,
    OrderSide,
    OrderStatus,
    OrderType,
    PaperEvent,
    PaperEventPayload,
    PaperEventType,
    PaperFill,
    PaperOrder,
    PaperPosition,
    PaperRejection,
    RejectionCode,
    TimeInForce,
    canonical_json,
    to_canonical_primitive,
)
from .ports import OrderCommandResult, PaperBrokerPort, PaperEventSink
from .state_machine import InvalidOrderTransition, can_transition, validate_order_transition

__all__ = [
    "ExecutionProvenance",
    "InvalidOrderTransition",
    "OrderCommandResult",
    "OrderIntent",
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "PaperEvent",
    "PaperEventPayload",
    "PaperEventSink",
    "PaperEventType",
    "PaperBrokerPort",
    "PaperFill",
    "PaperOrder",
    "PaperPosition",
    "PaperRejection",
    "RejectionCode",
    "TimeInForce",
    "can_transition",
    "canonical_json",
    "to_canonical_primitive",
    "validate_order_transition",
]
