"""Capital integration with side-effect-free lazy compatibility exports.

Importing a contracts/parser submodule must not load a COM backend or start a
runtime. Existing Phase 1 package-level imports remain available on demand.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .com_backend import ComtypesQuoteBackend
    from .contracts import (
        AdapterSnapshot,
        AdapterStoppedError,
        AuthenticationError,
        CapitalAdapterError,
        CommandQueueFullError,
        LiveQuoteInitializationError,
        MonitorError,
        QuoteComBackend,
        QuoteSnapshotRaw,
        ReadyTimeoutError,
        ReconnectPolicy,
        SubscriptionError,
    )
    from .quote_adapter import CapitalQuoteStaAdapter

_CONTRACT_EXPORTS = {
    "AdapterSnapshot",
    "AdapterStoppedError",
    "AuthenticationError",
    "CapitalAdapterError",
    "CommandQueueFullError",
    "LiveQuoteInitializationError",
    "MonitorError",
    "QuoteComBackend",
    "QuoteSnapshotRaw",
    "ReadyTimeoutError",
    "ReconnectPolicy",
    "SubscriptionError",
}
_BACKEND_EXPORTS = {"ComtypesQuoteBackend"}
_ADAPTER_EXPORTS = {"CapitalQuoteStaAdapter"}
_SUBMODULE_EXPORTS = {"contracts", "com_backend", "quote_adapter"}

__all__ = sorted(_CONTRACT_EXPORTS | _BACKEND_EXPORTS | _ADAPTER_EXPORTS)


def __getattr__(name: str) -> Any:
    if name in _SUBMODULE_EXPORTS:
        module = import_module(f".{name}", __name__)
        globals()[name] = module
        return module
    if name in _CONTRACT_EXPORTS:
        module = import_module(".contracts", __name__)
    elif name in _BACKEND_EXPORTS:
        module = import_module(".com_backend", __name__)
    elif name in _ADAPTER_EXPORTS:
        module = import_module(".quote_adapter", __name__)
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(module, name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__) | _SUBMODULE_EXPORTS)
