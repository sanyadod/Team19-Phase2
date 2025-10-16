"""
Coverage tests for LLM analysis using the Purdue GenAI provider.
"""

import json
import os
from unittest.mock import Mock, patch

from acmecli.llm_analysis import analyze_readme_with_llm, enhance_ramp_up_time_with_llm


def _env_purdue():
    return {
        "LLM_PROVIDER": "purdue",
        "PURDUE_GENAI_BASE_URL": "https://genai.rcac.purdue.edu",
        "PURDUE_GENAI_PATH": "/api/chat/completions",
        "PURDUE_GENAI_API_KEY": "test-key",
        "PURDUE_GENAI_MODEL": "llama4:latest",
    }


def test_purdue_api_success():
    readme_content = "This is a comprehensive README with examples and setup instructions."
    model_name = "test-model"

    # Mock Purdue response with OpenAI-like shape
    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "choices": [
            {
                "message": {
                    "content": json.dumps(
                        {
                            "documentation_quality": 0.9,
                            "ease_of_use": 0.8,
                            "examples_present": True,
                        }
                    )
                }
            }
        ]
    }

    with patch.dict(os.environ, _env_purdue(), clear=True):
        with patch("acmecli.llm_providers.requests.post", return_value=mock_resp):
            result = analyze_readme_with_llm(readme_content, model_name)

    assert "documentation_quality" in result
    assert "ease_of_use" in result
    assert "examples_present" in result
    # Merged local fields also present
    assert "installation_instructions" in result
    assert "usage_examples" in result


def test_purdue_api_empty_response_falls_back():
    readme_content = "Test README content."
    model_name = "test-model"

    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"choices": [{"message": {"content": None}}]}

    with patch.dict(os.environ, _env_purdue(), clear=True):
        with patch("acmecli.llm_providers.requests.post", return_value=mock_resp):
            result = analyze_readme_with_llm(readme_content, model_name)

    # Fallback to local analysis
    assert "examples_present" in result
    assert "usage_examples" in result


def test_purdue_api_request_error_falls_back():
    readme_content = "Test README content."
    model_name = "test-model"

    with patch.dict(os.environ, _env_purdue(), clear=True):
        with patch("acmecli.llm_providers.requests.post", side_effect=Exception("API error")):
            result = analyze_readme_with_llm(readme_content, model_name)

    assert "examples_present" in result
    assert "usage_examples" in result


def test_enhance_ramp_up_time_with_llm_success():
    base_score = 0.6
    readme_content = "Comprehensive README with detailed setup instructions and examples."
    model_name = "test-model"

    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "choices": [
            {"message": {"content": json.dumps({"documentation_quality": 0.7, "ease_of_use": 0.8})}}
        ]
    }

    with patch.dict(os.environ, _env_purdue(), clear=True):
        with patch("acmecli.llm_providers.requests.post", return_value=mock_resp):
            result = enhance_ramp_up_time_with_llm(base_score, readme_content, model_name)

    assert isinstance(result, float)
    assert 0.0 <= result <= 1.0


def test_enhance_ramp_up_time_with_llm_api_error():
    base_score = 0.7
    readme_content = "Test README content."
    model_name = "test-model"

    with patch.dict(os.environ, _env_purdue(), clear=True):
        with patch("acmecli.llm_providers.requests.post", side_effect=Exception("API error")):
            result = enhance_ramp_up_time_with_llm(base_score, readme_content, model_name)

    assert isinstance(result, float)
    assert 0.0 <= result <= 1.0


def test_enhance_ramp_up_time_with_llm_invalid_response():
    base_score = 0.5
    readme_content = "Test README content."
    model_name = "test-model"

    mock_resp = Mock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"choices": [{"message": {"content": "Not valid JSON"}}]}

    with patch.dict(os.environ, _env_purdue(), clear=True):
        with patch("acmecli.llm_providers.requests.post", return_value=mock_resp):
            result = enhance_ramp_up_time_with_llm(base_score, readme_content, model_name)

    assert isinstance(result, float)
    assert 0.0 <= result <= 1.0
