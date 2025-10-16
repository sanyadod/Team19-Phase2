"""Tests for report generation functionality."""

import tempfile
from pathlib import Path
from unittest.mock import patch

from acmecli.report import (
    extract_model_name,
    format_score,
    generate_summary_report,
    parse_model_results,
)


def test_extract_model_name():
    """Test model name extraction from URLs."""
    assert extract_model_name("https://huggingface.co/gpt2") == "gpt2"
    assert extract_model_name("https://huggingface.co/bert-base-uncased") == "bert-base-uncased"
    assert (
        extract_model_name("https://huggingface.co/microsoft/DialoGPT-medium") == "DialoGPT-medium"
    )
    assert extract_model_name("not-a-huggingface-url") == "not-a-huggingface-url"


def test_format_score():
    """Test score formatting with ratings."""
    assert format_score(0.95) == "95.0% (Excellent)"
    assert format_score(0.80) == "80.0% (Excellent)"
    assert format_score(0.75) == "75.0% (Good)"
    assert format_score(0.60) == "60.0% (Good)"
    assert format_score(0.45) == "45.0% (Acceptable)"
    assert format_score(0.40) == "40.0% (Acceptable)"
    assert format_score(0.25) == "25.0% (Poor)"
    assert format_score(0.0) == "0.0% (Poor)"


def test_parse_model_results_empty():
    """Test parsing empty results."""
    result = parse_model_results([])
    assert result["total_models"] == 0
    assert result["models"] == []


def test_parse_model_results_single_model():
    """Test parsing single model result."""
    models = [
        {
            "name": "gpt2",
            "net_score": 0.75,
            "license": 0.5,
            "size_score": {"raspberry_pi": 0.8, "desktop_pc": 1.0},
        }
    ]

    result = parse_model_results(models)

    assert result["total_models"] == 1
    assert len(result["models"]) == 1
    assert result["statistics"]["average_score"] == 0.75
    assert result["categories"]["good"] == 1
    assert result["categories"]["excellent"] == 0
    assert result["compliance"]["non_compliant"] == 1
    assert result["device_compatibility"]["raspberry_pi"] == 1


def test_parse_model_results_multiple_models():
    """Test parsing multiple models with different scores."""
    models = [
        {
            "name": "gpt2",
            "net_score": 0.85,  # Excellent
            "license": 1.0,  # Compliant
            "size_score": {"raspberry_pi": 0.6, "desktop_pc": 1.0},
        },
        {
            "name": "bert",
            "net_score": 0.45,  # Acceptable
            "license": 0.0,  # Non-compliant
            "size_score": {"raspberry_pi": 0.4, "desktop_pc": 0.8},
        },
        {
            "name": "distilbert",
            "net_score": 0.25,  # Poor
            "license": 0.5,  # Non-compliant
            "size_score": {"raspberry_pi": 0.9, "desktop_pc": 1.0},
        },
    ]

    result = parse_model_results(models)

    assert result["total_models"] == 3
    assert result["categories"]["excellent"] == 1
    assert result["categories"]["good"] == 0
    assert result["categories"]["acceptable"] == 1
    assert result["categories"]["poor"] == 1
    assert result["compliance"]["lgpl_compliant"] == 1
    assert result["compliance"]["non_compliant"] == 2
    assert result["device_compatibility"]["raspberry_pi"] == 2  # gpt2 and distilbert


def test_generate_summary_report():
    """Test summary report generation."""
    models = [
        {
            "name": "gpt2",
            "net_score": 0.75,
            "license": 0.5,
            "size_score": {"raspberry_pi": 0.8, "desktop_pc": 1.0},
        }
    ]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp:
        report_path = generate_summary_report(models, tmp.name)

        # Check that file was created
        assert Path(report_path).exists()

        # Read and verify content
        content = Path(report_path).read_text()
        assert "ACME MODEL EVALUATION SUMMARY REPORT" in content
        assert "Total Models Evaluated: 1" in content
        assert "Average Quality Score: 75.0%" in content
        assert "gpt2" in content

        # Cleanup
        Path(report_path).unlink()


@patch("acmecli.report.datetime")
def test_generate_summary_report_with_timestamp(mock_datetime):
    """Test summary report includes timestamp."""
    # Mock datetime
    mock_datetime.now.return_value.strftime.return_value = "2025-09-21 14:30:00"

    models = [
        {
            "name": "test",
            "net_score": 0.5,
            "license": 0.0,
            "size_score": {"raspberry_pi": 0.5, "desktop_pc": 0.5},
        }
    ]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp:
        report_path = generate_summary_report(models, tmp.name)
        content = Path(report_path).read_text()

        assert "Generated: 2025-09-21 14:30:00" in content
        Path(report_path).unlink()
