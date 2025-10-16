"""
Comprehensive Test Suite for LLM-Enhanced Documentation Analysis

This test suite validates the sophisticated LLM integration system that enhances model
evaluation through advanced natural language processing of README documentation.
Tests cover both OpenAI API integration and local fallback analysis to ensure
robust operation across different deployment environments.

Key test scenarios include API availability detection, documentation quality scoring,
metric enhancement algorithms, and error handling resilience. Critical for ensuring
the LLM integration meets specification requirements while maintaining reliability.
"""

from unittest.mock import patch

from acmecli.llm_analysis import (
    _analyze_readme_locally,
    analyze_readme_with_llm,
    enhance_ramp_up_time_with_llm,
)


def test_analyze_readme_locally_empty():
    """
    Validate baseline behavior with empty documentation content.

    Tests the local analysis system's handling of models with no README
    documentation, ensuring graceful degradation and meaningful default scores.
    """
    result = _analyze_readme_locally("", "test-model")

    assert result["documentation_quality"] == 0.0
    assert result["ease_of_use"] == 0.0
    assert result["examples_present"] is False
    assert result["installation_instructions"] is False
    assert result["usage_examples"] is False


def test_analyze_readme_locally_comprehensive():
    """Test local README analysis with comprehensive content."""
    readme = """
    # Test Model

    ## Installation
    ```bash
    pip install test-model
    ```

    ## Usage
    ```python
    from test_model import Model
    model = Model()
    result = model.predict(data)
    ```

    ## Examples
    Here are some usage examples...

    ## API Documentation
    Detailed API reference...
    """

    result = _analyze_readme_locally(readme, "test-model")

    assert result["documentation_quality"] == 1.0  # All sections present
    assert result["examples_present"] is True
    assert result["installation_instructions"] is True
    assert result["usage_examples"] is True
    assert result["code_blocks_count"] == 2  # Four ``` marks = 2 blocks


def test_enhance_ramp_up_time_with_llm():
    """Test ramp-up time enhancement with LLM analysis."""
    readme = (
        "# Model\nInstall: pip install model\nUsage: model.predict()\nExample: ```python\ncode```"
    )

    enhanced_score = enhance_ramp_up_time_with_llm(0.5, readme, "test-model")

    # Should enhance the base score
    assert 0.0 <= enhanced_score <= 1.0
    # The enhancement might increase or decrease depending on content quality


def test_analyze_readme_with_llm_fallback():
    """Test that LLM analysis falls back to local analysis."""
    readme = "# Simple README\nBasic documentation"

    result = analyze_readme_with_llm(readme, "test-model")

    # Should return analysis results
    assert "documentation_quality" in result
    assert "ease_of_use" in result
    assert "examples_present" in result


@patch("acmecli.llm_analysis.os.getenv")
def test_llm_no_api_key(mock_getenv):
    """Test LLM analysis when no API key is available."""
    mock_getenv.return_value = None  # No API key

    readme = "# Test\nSome content"
    result = analyze_readme_with_llm(readme, "test")

    # Should still work with local analysis
    assert "documentation_quality" in result
