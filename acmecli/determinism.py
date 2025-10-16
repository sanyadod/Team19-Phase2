"""
Deterministic seeding helpers for reproducible runs.

Reads SEED and DETERMINISTIC from the environment and seeds stdlib and
optional libraries (NumPy, PyTorch) when available.
"""

from __future__ import annotations

import os
import random
from typing import Any, Optional


def _truthy(s: Optional[str]) -> bool:
    if not s:
        return False
    return (s or "").strip().lower() in {"1", "true", "yes", "on"}


def set_global_determinism(seed: Optional[int] = None) -> int:
    """Seed stdlib and optional libs; return the seed used.

    Best-effort only; silently skips unavailable libraries.
    """
    if seed is None:
        try:
            seed = int(os.getenv("SEED", "0"))
        except ValueError:
            seed = 0

    # Python's built-in RNG
    random.seed(seed)

    # NumPy (optional)
    try:  # pragma: no cover - optional
        import importlib

        np: Any = importlib.import_module("numpy")
        np.random.seed(seed)
    except Exception:
        pass

    # PyTorch (optional)
    try:  # pragma: no cover - optional
        import importlib

        torch: Any = importlib.import_module("torch")
        torch.manual_seed(seed)
        cuda = getattr(torch, "cuda", None)
        if cuda and callable(getattr(cuda, "is_available", None)) and cuda.is_available():
            manual_seed_all = getattr(cuda, "manual_seed_all", None)
            if callable(manual_seed_all):
                manual_seed_all(seed)
        if _truthy(os.getenv("DETERMINISTIC")):
            # Make algorithms deterministic when possible
            try:
                uda = getattr(torch, "use_deterministic_algorithms", None)
                if callable(uda):
                    uda(True)
            except Exception:
                pass
            try:
                backends = getattr(torch, "backends", None)
                cudnn = getattr(backends, "cudnn", None) if backends else None
                if cudnn is not None:
                    setattr(cudnn, "deterministic", True)
                    setattr(cudnn, "benchmark", False)
            except Exception:
                pass
            # CUDA matmul determinism hint for some setups
            os.environ.setdefault("CUBLAS_WORKSPACE_CONFIG", ":16:8")
    except Exception:
        pass

    # Note: PYTHONHASHSEED must be set before interpreter start to take effect for hashing
    # We still set it for child processes spawned later.
    os.environ.setdefault("PYTHONHASHSEED", str(seed))

    return seed
