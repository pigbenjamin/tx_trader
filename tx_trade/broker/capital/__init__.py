"""Capital live quote integration.

Importing this package never initializes COM or loads a vendor library.
"""

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

__all__ = [
    "AdapterSnapshot",
    "AdapterStoppedError",
    "AuthenticationError",
    "CapitalAdapterError",
    "CapitalQuoteStaAdapter",
    "CommandQueueFullError",
    "ComtypesQuoteBackend",
    "LiveQuoteInitializationError",
    "MonitorError",
    "QuoteComBackend",
    "QuoteSnapshotRaw",
    "ReadyTimeoutError",
    "ReconnectPolicy",
    "SubscriptionError",
]
