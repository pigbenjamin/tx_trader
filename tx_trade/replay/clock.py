"""Interruptible timing abstraction for replay pacing."""

from __future__ import annotations

import math
import time
from threading import Event
from typing import Protocol, runtime_checkable


@runtime_checkable
class ReplayTimer(Protocol):
    def monotonic(self) -> float: ...

    def wait(self, delay_seconds: float, wake_event: Event) -> bool: ...


class SystemReplayTimer:
    """Production timer whose waits can be interrupted by a control event."""

    def monotonic(self) -> float:
        return time.monotonic()

    def wait(self, delay_seconds: float, wake_event: Event) -> bool:
        if isinstance(delay_seconds, bool) or not isinstance(delay_seconds, (int, float)):
            raise TypeError("delay_seconds must be a real number")
        delay = float(delay_seconds)
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("delay_seconds must be finite and non-negative")
        if not isinstance(wake_event, Event):
            raise TypeError("wake_event must be threading.Event")
        return wake_event.wait(delay)
