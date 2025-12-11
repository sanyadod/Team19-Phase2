"""
Fundamental metric algorithms with timing wrappers used by the scoring engine.
"""

from __future__ import annotations

from .base import timed


@timed
def size_score(total_bytes: int, L: int = 50_000_000, U: int = 500_000_000) -> float:
    """Linear size score between L (best) and U (worst); clamped to [0,1]."""
    if U <= L:
        return 0.0
    return max(0.0, min(1.0, (U - total_bytes) / (U - L)))


@timed
def license_score(license_text: str) -> float:
    """Simple license compliance heuristic: allowed->1.0, forbidden->0.0, else 0.5."""
    if not license_text:
        return 0.5  # Unknown license requires legal assessment
    tex = license_text.lower().strip()

    # Common OSI permissive and weak copyleft licenses acceptable for commercial use
    ok_tokens = (
        "lgpl-2.1",
        "lgpl v2.1",
        "gnu lesser general public license v2.1",
        "apache-2.0",
        "apache 2.0",
        "mit",
        "bsd-2",
        "bsd-3",
        "bsd 2",
        "bsd 3",
        "mpl-2.0",
        "mpl 2.0",
        "cc-by-4.0",  # for datasets/models allowing attribution
        "unlicense",
    )
    if any(tok in tex for tok in ok_tokens):
        return 1.0

    # Strong copyleft (e.g., GPL-3.0-only) typically incompatible for proprietary use
    bad_tokens = (
        "gpl-3.0",
        "gpl v3",
        "gnu general public license v3",
        "agpl",
    )
    if any(tok in tex for tok in bad_tokens):
        return 0.0

    # Ambiguous or custom licenses – require review
    return 0.5


@timed
def rampup_score(
    readme: float, quickstart: float, tutorials: float, api_docs: float, reproducibility: float
) -> float:
    """Average of five doc-quality signals used as ramp-up proxy."""
    # Equal weighting ensures all documentation aspects are valued
    vals = [readme, quickstart, tutorials, api_docs, reproducibility]
    return sum(vals) / 5.0


@timed
def bus_factor_score(contributors: int, k: int = 5) -> float:
    """Saturating contributors/(contributors+k) bus-factor curve."""
    c = max(0, contributors)
    return c / (c + k) if c + k > 0 else 0.0


@timed
def dataset_and_code_score(dataset_present: bool, code_present: bool) -> float:
    """Mean of dataset_present and code_present booleans."""
    return (int(bool(dataset_present)) + int(bool(code_present))) / 2.0


@timed
def dataset_quality_score(source: float, license_: float, splits: float, ethics: float) -> float:
    """Average of four dataset-doc signals (source, license, splits, ethics)."""
    # Equal weighting ensures all compliance aspects are addressed
    return (source + license_ + splits + ethics) / 4.0


@timed
def code_quality_score(
    flake8_errors: int, isort_sorted: bool, mypy_errors: int, emax: int = 50, tmax: int = 20
) -> float:
    """Weighted combo of flake8 (40%), isort (20%), mypy (40%)."""
    # Calculate individual component scores with linear degradation
    flake8_score = max(0.0, 1.0 - (flake8_errors / emax)) if emax > 0 else 0.0
    isort_score = 1.0 if isort_sorted else 0.0
    mypy_score = max(0.0, 1.0 - (mypy_errors / tmax)) if tmax > 0 else 0.0

    # Weighted combination emphasizing critical quality metrics
    return 0.4 * flake8_score + 0.2 * isort_score + 0.4 * mypy_score


@timed
def perf_claims_score(benchmarks_present: bool, citations_present: bool) -> float:
    """Mean of benchmark/citation booleans as credibility score."""
    return (int(bool(benchmarks_present)) + int(bool(citations_present))) / 2.0
