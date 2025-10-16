"""
Metric protocol and timing decorator with [0,1] clamping.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Protocol, Tuple


class Metric(Protocol):
    """Standard metric interface returning (score, ms)."""

    name: str

    def compute(self, ctx: Dict[str, Any]) -> Tuple[float, int]:
        """Compute metric using ctx; return (score in [0,1], latency ms)."""
        ...


def timed(fn: Callable[..., float]) -> Callable[..., Tuple[float, int]]:
    """Wrap fn to return (clamped score, elapsed ms)."""

    def wrapper(*args: Any, **kwargs: Any) -> Tuple[float, int]:
        # High-precision timing for accurate performance measurement
        t0 = time.perf_counter()
        score = float(fn(*args, **kwargs))
        dt_ms = int((time.perf_counter() - t0) * 1000)

        # Enforce score normalization and ensure latency >= 1ms (avoid zero in fast runs)
        return max(0.0, min(1.0, score)), max(1, dt_ms)

    return wrapper
