"""
Critical Path Smoke Tests for Model Trustworthiness Assessment System

This module implements essential validation tests that verify core system functionality
across all trustworthiness dimensions. These smoke tests serve as the first line of
defense against regressions and ensure that fundamental scoring algorithms maintain
their mathematical properties and business logic integrity.

The test suite validates score normalization, monotonic behavior, and boundary conditions
that are critical for deployment readiness assessment. All tests are designed to execute
rapidly while providing comprehensive coverage of essential system behaviors that impact
production reliability.

Test Categories:
- Score Range Validation: Ensures all metrics return values in [0,1] range
- Mathematical Properties: Verifies monotonic and boundary behaviors
- Business Logic: Confirms scoring logic aligns with trustworthiness requirements
- Integration Validation: Tests metric interaction and system stability
"""

from acmecli.metrics.repo_scan import (
    bus_factor_score,
    code_quality_score,
    dataset_and_code_score,
    dataset_quality_score,
    license_score,
    perf_claims_score,
    rampup_score,
    size_score,
)


def test_metrics_return_in_range():
    """
    Validate that all scoring metrics return normalized values in the required [0,1] range.

    This critical validation ensures that score aggregation and comparison operations
    remain mathematically sound across all trustworthiness dimensions. Score range
    compliance is essential for weighted scoring algorithms and business reporting.

    Tested Scenarios:
    - Size scoring with typical model dimensions
    - License compliance with standard LGPL format
    - Documentation quality with mixed completion levels
    - Team sustainability with realistic contributor counts
    - Implementation completeness with various asset availability
    - Data quality with partial documentation compliance
    - Code quality with realistic error counts
    - Performance validation with complete evidence
    """
    assert 0 <= size_score(100_000_000)[0] <= 1
    assert 0 <= license_score("LGPL-2.1")[0] <= 1
    assert 0 <= rampup_score(1, 1, 0, 1, 0)[0] <= 1
    assert 0 <= bus_factor_score(10)[0] <= 1
    assert 0 <= dataset_and_code_score(True, False)[0] <= 1
    assert 0 <= dataset_quality_score(1, 1, 0.5, 0)[0] <= 1
    assert 0 <= code_quality_score(10, True, 5)[0] <= 1
    assert 0 <= perf_claims_score(True, True)[0] <= 1


def test_bus_factor_monotonic():
    """
    Verify that team sustainability scoring increases monotonically with contributor count.

    This test validates the mathematical foundation of the bus factor algorithm,
    ensuring that higher contributor counts consistently yield better sustainability
    scores. Monotonic behavior is essential for meaningful comparison between models
    and reliable ranking in enterprise selection processes.

    The test examines three representative points across the contributor spectrum
    to confirm the saturating function maintains proper ordering throughout its range.
    """
    a, _ = bus_factor_score(1)  # Single contributor (high risk)
    b, _ = bus_factor_score(5)  # Small team (moderate risk)
    c, _ = bus_factor_score(20)  # Large team (low risk)
    assert a < b < c


def test_license_unclear_when_missing():
    """
    Validate that missing license information results in moderate risk assessment.

    Confirms that the license scoring algorithm properly handles unknown legal status
    by assigning a 0.5 score, indicating the need for legal review rather than
    automatic approval or rejection. This balanced approach supports enterprise
    compliance workflows while avoiding false negatives.

    The 0.5 score represents appropriate caution for deployment decisions when
    legal compliance cannot be automatically determined from available metadata.
    """
    s, _ = license_score("")
    assert s == 0.5


def test_perf_claims_half_when_one_present():
    """
    Verify balanced scoring when performance evidence is partially available.

    Tests the performance credibility algorithm's handling of incomplete validation
    evidence, ensuring that partial documentation (either benchmarks or citations)
    receives appropriate intermediate scoring rather than binary pass/fail assessment.

    This behavior supports nuanced evaluation of model trustworthiness when complete
    performance validation is unavailable, enabling informed deployment decisions
    based on available evidence quality.
    """
    s1, _ = perf_claims_score(True, False)  # Benchmarks only
    s2, _ = perf_claims_score(False, True)  # Citations only
    assert s1 == 0.5 and s2 == 0.5
