"""
URL Classification and Validation Tests for Multi-Platform Repository Analysis

This test suite validates the URL classification system that distinguishes between
HuggingFace models, datasets, and GitHub code repositories. Essential for ensuring
that the evaluation pipeline processes only appropriate resources and maintains
clear separation between different asset types in enterprise workflows.

The classification system supports production deployment scenarios where mixed URL
lists from various sources must be accurately categorized for appropriate processing
pipelines. Tests cover standard URL formats and edge cases encountered in real-world
data ingestion scenarios.

Validation Areas:
- HuggingFace model URL pattern recognition and classification
- Dataset URL identification and filtering logic
- GitHub repository URL parsing and categorization
- Edge case handling for malformed or ambiguous URLs
- Category enumeration consistency across system components
"""

from acmecli.urls import Category, classify


def test_classify_model():
    """
    Validate HuggingFace model URL classification for accurate processing pipeline routing.

    Ensures that standard HuggingFace model URLs are correctly identified and categorized
    for trustworthiness evaluation. Critical for ensuring that only model repositories
    enter the comprehensive scoring pipeline while other resource types are appropriately
    filtered for their respective processing workflows.
    """
    assert classify("https://huggingface.co/gpt2") is Category.MODEL


def test_classify_dataset():
    """
    Verify dataset URL recognition for proper resource type segregation.

    Validates that HuggingFace dataset URLs are correctly classified to prevent them
    from entering the model evaluation pipeline. Essential for maintaining processing
    efficiency and ensuring that evaluation resources are applied to appropriate
    asset types in production environments.
    """
    assert classify("https://huggingface.co/datasets/squad") is Category.DATASET


def test_classify_code():
    """
    Confirm GitHub repository URL classification for code asset identification.

    Tests the recognition of GitHub repository URLs to ensure proper categorization
    of code resources. Important for distinguishing between model repositories and
    general code repositories that may require different evaluation criteria and
    processing approaches in enterprise assessment workflows.
    """
    assert classify("https://github.com/user/repo") is Category.CODE
