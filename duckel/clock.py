"""
The wall clock, as a value a caller is handed.

A unit that stamps a time takes a `Clock` as an argument. Production passes `system_clock`.
A test passes a function that returns a fixed time, so nothing sleeps and nothing drifts.
"""

import time
from collections.abc import Callable

# Seconds since the epoch, on each call.
Clock = Callable[[], float]


def system_clock() -> float:
    """Seconds since the epoch, read from the wall clock."""
    return time.time()
