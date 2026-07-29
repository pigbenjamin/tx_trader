"""Broker-neutral order contracts and paper-execution boundaries.

Public compatibility exports are resolved lazily so importing a focused
submodule such as :mod:`tx_trade.orders.live_contracts` does not import the
paper runtime or consult process configuration.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .contracts import (
        CancelIntent,
        DEFAULT_EXECUTION_CONFIG,
        DEFAULT_EXECUTION_CONFIG_FINGERPRINT,
        ExecutionProvenance,
        FeePolicyKind,
        FeeRoundingMode,
        InstrumentMetadataSnapshot,
        MatchDisposition,
        MatchResult,
        MatchSkipReason,
        OrderIntent,
        OrderSide,
        OrderStatus,
        OrderType,
        PaperBrokerLimits,
        PaperBrokerSnapshot,
        PaperCommand,
        PaperDecision,
        PaperDecisionBatchResult,
        PaperEvent,
        PaperEventPayload,
        PaperEventType,
        PaperExecutionConfig,
        PaperFeeRule,
        PaperFeeSchedule,
        PaperFill,
        PaperOrder,
        PaperPosition,
        PaperRejection,
        RejectionCode,
        SlippageConfig,
        SlippageMode,
        TimeInForce,
        canonical_json,
        to_canonical_primitive,
    )
    from .ports import (
        CheckpointablePaperBrokerPort,
        OrderCommandResult,
        PaperBrokerPort,
        PaperEventSink,
        TransactionalPaperBrokerPort,
    )
    from .state_machine import InvalidOrderTransition, can_transition, validate_order_transition

_CONTRACT_EXPORTS = {
    "CancelIntent",
    "DEFAULT_EXECUTION_CONFIG",
    "DEFAULT_EXECUTION_CONFIG_FINGERPRINT",
    "ExecutionProvenance",
    "FeePolicyKind",
    "FeeRoundingMode",
    "InstrumentMetadataSnapshot",
    "MatchDisposition",
    "MatchResult",
    "MatchSkipReason",
    "OrderIntent",
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "PaperBrokerLimits",
    "PaperBrokerSnapshot",
    "PaperCommand",
    "PaperDecision",
    "PaperDecisionBatchResult",
    "PaperEvent",
    "PaperEventPayload",
    "PaperEventType",
    "PaperExecutionConfig",
    "PaperFeeRule",
    "PaperFeeSchedule",
    "PaperFill",
    "PaperOrder",
    "PaperPosition",
    "PaperRejection",
    "RejectionCode",
    "SlippageConfig",
    "SlippageMode",
    "TimeInForce",
    "canonical_json",
    "to_canonical_primitive",
}
_PORT_EXPORTS = {
    "CheckpointablePaperBrokerPort",
    "OrderCommandResult",
    "PaperBrokerPort",
    "PaperEventSink",
    "TransactionalPaperBrokerPort",
}
_STATE_MACHINE_EXPORTS = {
    "InvalidOrderTransition",
    "can_transition",
    "validate_order_transition",
}
_SUBMODULE_EXPORTS = {"contracts", "ports", "state_machine"}

__all__ = sorted(_CONTRACT_EXPORTS | _PORT_EXPORTS | _STATE_MACHINE_EXPORTS)


def __getattr__(name: str) -> Any:
    if name in _SUBMODULE_EXPORTS:
        module = import_module(f".{name}", __name__)
        globals()[name] = module
        return module
    if name in _CONTRACT_EXPORTS:
        module = import_module(".contracts", __name__)
    elif name in _PORT_EXPORTS:
        module = import_module(".ports", __name__)
    elif name in _STATE_MACHINE_EXPORTS:
        module = import_module(".state_machine", __name__)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__) | _SUBMODULE_EXPORTS)
