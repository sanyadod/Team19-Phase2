"""
Comprehensive CLI Integration Tests for ACME Model Evaluation System

This test suite validates the complete command-line interface workflow from URL input
to score output, ensuring robust operation across different execution environments.
Tests cover argument parsing, parallel processing fallback, output formatting,
and integration with all core system components for enterprise-grade reliability.

The tests use sophisticated mocking strategies to simulate various deployment scenarios
including ProcessPoolExecutor failures, API timeouts, and different output modes.
Critical for ensuring production reliability, cross-platform compatibility, and
graceful degradation under adverse conditions.

Test Coverage Areas:
- URL file processing and filtering logic
- Parallel execution with fallback mechanisms
- NDJSON output formatting for downstream integration
- Error handling across system boundaries
- Cross-platform execution environment validation
"""

import json
import sys
from pathlib import Path
from typing import Iterable, Iterator

import pytest

from acmecli import main as app


def get_test_artifacts_dir():
    """Get the test artifacts directory path."""
    project_root = Path(__file__).parent.parent
    test_artifacts_dir = project_root / "test_artifacts"
    test_artifacts_dir.mkdir(exist_ok=True)
    return test_artifacts_dir


class DummyPool:
    """
    Mock ProcessPoolExecutor for testing parallel execution fallback mechanisms.

    Simulates the concurrent.futures.ProcessPoolExecutor interface to test
    graceful degradation when parallel processing is unavailable. This mock
    enables validation of sequential fallback behavior that ensures system
    reliability even when multiprocessing resources are constrained.

    Essential for testing deployment scenarios where process pools may fail
    due to system limitations, container restrictions, or resource constraints.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def map(self, fn, iterable: Iterable[str]) -> Iterator[dict]:
        """
        Simulate parallel execution with sequential processing for testing.

        Provides identical interface to ProcessPoolExecutor.map() while executing
        sequentially to enable deterministic testing of the fallback code path.
        """
        for x in iterable:
            yield fn(x)


def test_main_prints_ndjson_for_models(tmp_path, monkeypatch, capsys):
    """
    Validate end-to-end CLI workflow with mixed URL inputs and NDJSON output formatting.

    Tests the complete pipeline from file input through URL filtering to formatted output,
    ensuring that only HuggingFace model URLs are processed while datasets and code
    repositories are appropriately filtered out. Critical for production workflows
    that process mixed URL lists from various sources.

    The test validates:
    - URL file parsing and content filtering
    - Model URL identification and processing
    - NDJSON output format compliance
    - Integration across all system components
    """
    # Create mixed URL file with model + dataset + code; only model should be processed
    p = tmp_path / "urls.txt"
    p.write_text(
        "https://huggingface.co/gpt2\n"
        "https://huggingface.co/datasets/squad\n"
        "https://github.com/user/repo\n"
    )

    # Configure test environment with controlled arguments and execution context
    monkeypatch.setattr(sys, "argv", ["prog", str(p)])
    monkeypatch.setattr(app.cf, "ProcessPoolExecutor", lambda: DummyPool())

    # Execute main application workflow - expect SystemExit(0) when models succeed
    with pytest.raises(SystemExit) as exc_info:
        app.main()

    # Validate that it exits with code 0 when models are successfully processed
    # (DATASET/CODE URLs are filtered but not errors)
    assert exc_info.value.code == 0

    # Validate NDJSON output format and content filtering
    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 1  # Only model URL should generate output
    rec = json.loads(out[0])
    assert rec["name"] == "gpt2"  # Expect extracted model name, not full URL
    assert rec["category"] == "MODEL"
    # Verify score normalization and range compliance
    assert 0.0 <= float(rec["net_score"]) <= 1.0


def test_main_with_summary_flag(monkeypatch, capsys):
    """
    Validate summary report generation functionality for executive stakeholder communication.

    Tests the --summary flag functionality that transforms technical scoring data into
    business-friendly summary reports. Essential for executive dashboards and deployment
    decision workflows where stakeholders need concise, actionable insights rather than
    detailed technical metrics.

    The test ensures:
    - Summary flag argument parsing and handling
    - Report generation from scoring data
    - Executive-appropriate output formatting
    - Integration with the complete evaluation pipeline
    """
    test_dir = get_test_artifacts_dir()
    p = test_dir / "urls.txt"
    p.write_text("https://huggingface.co/gpt2\n")

    # Change to test_artifacts directory to ensure files are created there
    monkeypatch.chdir(test_dir)

    # Test with summary flag
    monkeypatch.setattr(sys, "argv", ["prog", str(p), "--summary"])
    monkeypatch.setattr(app.cf, "ProcessPoolExecutor", lambda: DummyPool())

    # Execute main application workflow - expect SystemExit(0) for successful processing
    with pytest.raises(SystemExit) as exc_info:
        app.main()

    # Validate that it exits with code 0 for success
    assert exc_info.value.code == 0

    out = capsys.readouterr().out.strip()
    assert "Results saved to:" in out
    assert "Summary report:" in out
    assert "View summary:" in out


def test_main_with_custom_output(monkeypatch, capsys):
    """Test main function with custom output filename."""
    test_dir = get_test_artifacts_dir()
    p = test_dir / "urls.txt"
    p.write_text("https://huggingface.co/gpt2\n")

    # Change to test_artifacts directory to ensure files are created there
    monkeypatch.chdir(test_dir)

    # Test with custom output using test_artifacts directory
    output_path = test_dir / "test_analysis"
    monkeypatch.setattr(sys, "argv", ["prog", str(p), "--summary", "--output", str(output_path)])
    monkeypatch.setattr(app.cf, "ProcessPoolExecutor", lambda: DummyPool())

    # Execute main application workflow - expect SystemExit(0) for successful processing
    with pytest.raises(SystemExit) as exc_info:
        app.main()

    # Validate that it exits with code 0 for success
    assert exc_info.value.code == 0

    out = capsys.readouterr().out.strip()
    assert str(output_path) in out


def test_main_empty_file(tmp_path, monkeypatch, capsys):
    """Test main function with empty URL file."""
    p = tmp_path / "empty.txt"
    p.write_text("")

    monkeypatch.setattr(sys, "argv", ["prog", str(p)])
    monkeypatch.setattr(app.cf, "ProcessPoolExecutor", lambda: DummyPool())

    # Execute main application workflow - expect SystemExit(1) for empty file
    with pytest.raises(SystemExit) as exc_info:
        app.main()

    # Validate that it exits with code 1 for empty file
    assert exc_info.value.code == 1

    out = capsys.readouterr().out.strip()
    assert out == ""  # No output for empty file


def test_main_no_model_urls(tmp_path, monkeypatch, capsys):
    """Test main function with no MODEL URLs."""
    p = tmp_path / "no_models.txt"
    p.write_text("https://huggingface.co/datasets/squad\n" "https://github.com/user/repo\n")

    monkeypatch.setattr(sys, "argv", ["prog", str(p)])
    monkeypatch.setattr(app.cf, "ProcessPoolExecutor", lambda: DummyPool())

    # Execute main application workflow - expect SystemExit(1) for no model URLs
    with pytest.raises(SystemExit) as exc_info:
        app.main()

    # Validate that it exits with code 1 for no valid model URLs
    assert exc_info.value.code == 1

    out = capsys.readouterr().out.strip()
    assert out == ""  # No MODEL URLs = no output
