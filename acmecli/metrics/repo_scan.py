# acmecli/metrics/repo_scan.py

import time

# -----------------------------
# Helpers
# -----------------------------
def _latency(start: float) -> int:
    """Return elapsed time in ms, with a minimum of 1."""
    return max(1, int((time.perf_counter() - start) * 1000))


def _clamp(x: float) -> float:
    """Clamp a value into [0, 1]."""
    return max(0.0, min(1.0, float(x)))


# -----------------------------
# SIZE SCORE
# -----------------------------
def size_score(total_bytes: int):
    """
    Size score in [0,1].

    We map model size to a decaying score so that models around a few
    hundred MB get a reasonable mid-range score and larger models get
    smaller scores.
    """
    start = time.perf_counter()

    if total_bytes is None or total_bytes <= 0:
        # If we don't know the size, be neutral-ish rather than harsh
        return 1.0, _latency(start)

    # HuggingFace-scale models (~400MB) should land around ~0.5
    score = 1.0 / (1.0 + (float(total_bytes) / 400_000_000.0))

    return _clamp(score), _latency(start)


# -----------------------------
# LICENSE SCORE
# -----------------------------
def license_score(license_text: str):
    """
    Very simple heuristic license scorer.

    Apache / MIT / LGPL are treated as "good" (1.0),
    GPL is treated as incompatible (0.0),
    everything else is neutral (0.5).
    """
    start = time.perf_counter()

    if not license_text:
        return 0.5, _latency(start)

    text = license_text.lower()

    if "apache" in text:
        return 1.0, _latency(start)
    if "mit" in text:
        return 1.0, _latency(start)
    if "lgpl" in text:
        return 1.0, _latency(start)
    if "gpl" in text:
        return 0.0, _latency(start)

    return 0.5, _latency(start)


# -----------------------------
# BUS FACTOR SCORE
# -----------------------------
def bus_factor_score(contributors: int):
    """
    Bus factor score in [0,1].

    - 0 or negative contributors -> 0.0
    - 1 contributor              -> 0.2 (very risky)
    - 2..9 contributors          -> linear ramp up to 1.0
    - >=10 contributors          -> 1.0
    """
    start = time.perf_counter()

    if contributors is None:
        return 0.0, _latency(start)

    try:
        contributors = int(contributors)
    except (TypeError, ValueError):
        return 0.0, _latency(start)

    if contributors <= 0:
        return 0.0, _latency(start)

    if contributors == 1:
        return 0.2, _latency(start)

    if contributors >= 10:
        return 1.0, _latency(start)

    # For 2..9, scale linearly so that more contributors => better score
    score = contributors / 10.0
    return _clamp(score), _latency(start)


# -----------------------------
# RAMP UP
# -----------------------------
def rampup_score(readme, quickstart, tutorials, api_docs, reproducibility):
    """
    Ramp-up score is the average of the five documentation signals,
    each already expected to be in [0,1].
    """
    start = time.perf_counter()

    vals = [readme, quickstart, tutorials, api_docs, reproducibility]
    total = 0.0
    for v in vals:
        try:
            total += float(v)
        except (TypeError, ValueError):
            # Treat missing / bad values as 0
            total += 0.0

    score = total / 5.0
    return _clamp(score), _latency(start)


# -----------------------------
# DATASET + CODE PRESENCE
# -----------------------------
def dataset_and_code_score(dataset_present, code_present):
    """
    0.5 credit for dataset_present, 0.5 for code_present.
    """
    start = time.perf_counter()

    score = 0.0
    if bool(dataset_present):
        score += 0.5
    if bool(code_present):
        score += 0.5

    return _clamp(score), _latency(start)


# -----------------------------
# DATASET QUALITY
# -----------------------------
def dataset_quality_score(source, license, splits, ethics):
    """
    Average the four dataset quality sub-scores, each in [0,1].
    """
    start = time.perf_counter()

    vals = [source, license, splits, ethics]
    total = 0.0
    for v in vals:
        try:
            total += float(v)
        except (TypeError, ValueError):
            total += 0.0

    score = total / 4.0
    return _clamp(score), _latency(start)


# -----------------------------
# CODE QUALITY
# -----------------------------
def code_quality_score(flake8_errors, isort_sorted, mypy_errors):
    """
    Start from 1.0 and subtract penalties for style / type issues.
    """
    start = time.perf_counter()

    score = 1.0

    try:
        flake8_errors = int(flake8_errors)
    except (TypeError, ValueError):
        flake8_errors = 0

    try:
        mypy_errors = int(mypy_errors)
    except (TypeError, ValueError):
        mypy_errors = 0

    if flake8_errors > 0:
        score -= 0.3
    if not bool(isort_sorted):
        score -= 0.2
    if mypy_errors > 0:
        score -= 0.3

    return _clamp(score), _latency(start)


# -----------------------------
# PERFORMANCE CLAIMS
# -----------------------------
def perf_claims_score(benchmarks_present, citations_present):
    """
    Performance claims score in [0,1].

    - No benchmarks and no citations -> 0.0
    - Exactly one of benchmarks / citations -> 0.5
    - Both benchmarks and citations present -> 1.0
    """
    start = time.perf_counter()

    b = bool(benchmarks_present)
    c = bool(citations_present)

    if not b and not c:
        return 0.0, _latency(start)
    if b and c:
        return 1.0, _latency(start)

    # exactly one present
    return 0.5, _latency(start)
