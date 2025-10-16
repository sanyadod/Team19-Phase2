"""
Simple URL classifier for Hugging Face resources (MODEL/DATASET/CODE).
"""

from enum import Enum


class Category(str, Enum):
    """Resource category enum used by classify()."""

    MODEL = "MODEL"  # Pre-trained models ready for inference or fine-tuning
    DATASET = "DATASET"  # Training/evaluation datasets with documentation
    CODE = "CODE"  # Source code repositories and implementations


def classify(url: str) -> Category:
    """Classify URL as MODEL, DATASET, or CODE (heuristic pattern matching)."""
    u = url.lower()
    if "huggingface.co/datasets" in u:
        return Category.DATASET
    if "huggingface.co" in u:
        return Category.MODEL
    return Category.CODE
