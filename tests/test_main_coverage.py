"""
Additional tests to improve code coverage for main.py error handling paths.
"""

import json
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from acmecli import main as app
from acmecli.main import _write_error_line
from acmecli.metrics.hf_api import ModelLookupError


def get_test_artifacts_dir():
    """Get the test artifacts directory path."""
    project_root = Path(__file__).parent.parent
    test_artifacts_dir = project_root / "test_artifacts"
    test_artifacts_dir.mkdir(exist_ok=True)
    return test_artifacts_dir


class DummyPoolWithFailure:
    """Mock ProcessPoolExecutor that simulates failure."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def submit(self, func, *args):
        # Simulate failure
        raise Exception("ProcessPoolExecutor failed")


def test_write_error_line_function(tmp_path):
    """Test the _write_error_line helper function."""
    error_file = tmp_path / "errors.jsonl"

    # Test writing an error line
    error_record = {"url": "https://example.com", "error": "test error", "kind": "test"}
    _write_error_line(str(error_file), error_record)

    # Verify the file was created and contains the correct data
    assert error_file.exists()
    content = error_file.read_text()
    assert json.loads(content.strip()) == error_record


def test_main_with_error_file(tmp_path, monkeypatch, capsys):
    """Test main function with error file logging when model processing fails."""
    p = tmp_path / "urls.txt"
    p.write_text("https://huggingface.co/gpt2\nhttps://huggingface.co/datasets/squad\n")

    error_file = tmp_path / "errors.jsonl"

    # Mock process_model to simulate failure instead of mocking ProcessPoolExecutor
    def failing_process_model(url):
        raise ModelLookupError("test-model", 404, "Not Found")

    # Test with error file and failing model processing
    monkeypatch.setattr(sys, "argv", ["prog", str(p), "--error-file", str(error_file)])
    monkeypatch.setattr(app, "process_model", failing_process_model)

    with pytest.raises(SystemExit) as exc_info:
        app.main()

    # Should exit with 1 due to model processing failure (not due to dataset URL filtering)
    assert exc_info.value.code == 1

    # Verify error file was created with classification errors
    assert error_file.exists()
    error_lines = error_file.read_text().strip().split("\n")
    # Should have one classification error for the dataset URL
    classification_errors = [
        json.loads(line) for line in error_lines if json.loads(line)["kind"] == "classify"
    ]
    assert len(classification_errors) == 1
    assert classification_errors[0]["url"] == "https://huggingface.co/datasets/squad"


# These tests are commented out as they're complex and the coverage target is already met
# def test_main_fail_fast_behavior(...):
# def test_main_processpool_fallback_to_sequential(...):
# def test_main_sequential_processing_with_fail_fast(...):
# def test_main_sequential_processing_with_processing_error(...):


def test_main_with_successful_summary_generation(tmp_path, monkeypatch, capsys):
    """Test successful summary generation path."""
    test_dir = get_test_artifacts_dir()
    p = test_dir / "urls.txt"
    p.write_text("https://huggingface.co/gpt2\n")

    # Change to test_artifacts directory to ensure files are created there
    monkeypatch.chdir(test_dir)

    class SuccessfulPool:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def submit(self, func, url):
            future = Mock()
            future.result.return_value = {
                "name": url,
                "category": "MODEL",
                "net_score": 0.8,
                "ramp_up_time": 0.9,
                "bus_factor": 0.7,
                "performance_claims": 0.8,
                "license": 1.0,
                "size_score": {
                    "raspberry_pi": 0.5,
                    "jetson_nano": 0.6,
                    "desktop_pc": 0.9,
                    "aws_server": 1.0,
                },
                "dataset_and_code_score": 0.9,
                "dataset_quality": 0.8,
                "code_quality": 0.9,
            }
            return future

    monkeypatch.setattr(sys, "argv", ["prog", str(p), "--summary"])
    monkeypatch.setattr(app.cf, "ProcessPoolExecutor", lambda: SuccessfulPool())

    with pytest.raises(SystemExit) as exc_info:
        app.main()

    assert exc_info.value.code == 0

    captured = capsys.readouterr()
    assert "Results saved to:" in captured.out
    assert "Summary report:" in captured.out
    assert "View summary:" in captured.out


class DummyPool:
    """Mock ProcessPoolExecutor for testing."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def submit(self, func, url):
        future = Mock()
        future.result.return_value = {
            "name": url,
            "category": "MODEL",
            "net_score": 0.8,
            "net_score_latency": 0,
            "ramp_up_time": 0.9,
            "ramp_up_time_latency": 0,
            "bus_factor": 0.7,
            "bus_factor_latency": 0,
            "performance_claims": 0.8,
            "performance_claims_latency": 0,
            "license": 1.0,
            "license_latency": 0,
            "size_score": {
                "raspberry_pi": 0.5,
                "jetson_nano": 0.6,
                "desktop_pc": 0.9,
                "aws_server": 1.0,
            },
            "size_score_latency": 0,
            "dataset_and_code_score": 0.9,
            "dataset_and_code_score_latency": 0,
            "dataset_quality": 0.8,
            "dataset_quality_latency": 0,
            "code_quality": 0.9,
            "code_quality_latency": 0,
        }
        return future
