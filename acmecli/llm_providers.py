"""
LLM provider abstraction (Purdue GenAI only).

Env:
- LLM_PROVIDER=purdue
- PURDUE_GENAI_BASE_URL, PURDUE_GENAI_API_KEY, PURDUE_GENAI_MODEL, PURDUE_GENAI_PATH
- PURDUE_GENAI_TIMEOUT (optional)

Contract: analyze_readme(model, readme) -> {documentation_quality, ease_of_use, examples_present}
"""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)


def _extract_json_object(s: str) -> str:
    """
    Accept model output that may contain ```json fences or extra text.
    Return the best-effort JSON object substring, or "" if none found.
    """
    if not s:
        return ""

    text = s.strip()

    # Remove ```json / ``` fences if present
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    # Best-effort: find first {...} JSON object
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1].strip()

    return ""


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def _normalize_score(x: Any) -> float:
    """
    Convert score to float and normalize to [0,1].
    Accepts either 0-1 or 0-10 (normalizes down if >1).
    """
    try:
        v = float(x)
    except Exception:
        return 0.0

    # If model used 0-10 scale, normalize
    if v > 1.0:
        v = v / 10.0

    return _clamp01(v)


class LLMProvider(ABC):
    @abstractmethod
    def analyze_readme(self, model_name: str, readme: str) -> Dict[str, Any]:
        raise NotImplementedError


class PurdueGenAIProvider(LLMProvider):
    def __init__(self, base_url: str, api_key: str, model: str, path: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.path = path if path.startswith("/") else f"/{path}"

    def analyze_readme(self, model_name: str, readme: str) -> Dict[str, Any]:
        url = f"{self.base_url}{self.path}"
        timeout = int(os.getenv("PURDUE_GENAI_TIMEOUT", "60"))

        prompt = (
            f"Analyze README for model '{model_name}'.\n"
            "Return ONLY valid JSON (no markdown, no extra text) with keys:\n"
            "documentation_quality (number 0-1), ease_of_use (number 0-1), examples_present (boolean).\n\n"
            f"README:\n{readme[:2000]}\n"
        )

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0,
            "max_tokens": 150,
            "stream": False,
        }

        try:
            r = requests.post(url, headers=headers, json=payload, timeout=timeout)
            text = r.text or ""

            if r.status_code != 200:
                logger.warning(
                    "Purdue GenAI HTTP %s model=%s url=%s body=%r",
                    r.status_code, self.model, url, text[:500]
                )
                return {"documentation_quality": 0.0, "ease_of_use": 0.0, "examples_present": False}

            try:
                data = r.json()
            except Exception:
                logger.warning(
                    "Purdue GenAI non-JSON response model=%s url=%s body=%r",
                    self.model, url, text[:500]
                )
                return {"documentation_quality": 0.0, "ease_of_use": 0.0, "examples_present": False}

            try:
                content = data["choices"][0]["message"]["content"]
            except Exception:
                logger.warning("Unexpected GenAI response shape: %r", str(data)[:500])
                return {"documentation_quality": 0.0, "ease_of_use": 0.0, "examples_present": False}

            # NEW: tolerate code fences / extra text around JSON
            json_str = _extract_json_object(content)
            if not json_str:
                logger.warning("Model output missing JSON object model=%s output=%r", self.model, content[:500])
                return {"documentation_quality": 0.0, "ease_of_use": 0.0, "examples_present": False}

            try:
                parsed = json.loads(json_str)
            except Exception:
                logger.warning("Model output not JSON model=%s output=%r", self.model, content[:500])
                return {"documentation_quality": 0.0, "ease_of_use": 0.0, "examples_present": False}

            return {
                "documentation_quality": _normalize_score(parsed.get("documentation_quality", 0.0)),
                "ease_of_use": _normalize_score(parsed.get("ease_of_use", 0.0)),
                "examples_present": bool(parsed.get("examples_present", False)),
            }

        except requests.exceptions.ReadTimeout:
            logger.warning("Purdue GenAI timeout (timeout=%ss) model=%s", timeout, self.model)
            return {"documentation_quality": 0.0, "ease_of_use": 0.0, "examples_present": False}
        except Exception as e:
            logger.warning("Purdue GenAI analyze_readme failed: %s", e)
            return {"documentation_quality": 0.0, "ease_of_use": 0.0, "examples_present": False}


def get_llm_provider() -> Optional[LLMProvider]:
    raw = os.getenv("LLM_PROVIDER", "purdue")
    provider = (raw or "purdue").strip().lower()

    if provider != "purdue":
        logger.info("Unsupported LLM_PROVIDER '%s' (only 'purdue' supported)", provider)
        return None

    base_url = os.getenv("PURDUE_GENAI_BASE_URL")
    api_key = os.getenv("PURDUE_GENAI_API_KEY")

    # Defaults that match Purdue's docs you pasted
    model = os.getenv("PURDUE_GENAI_MODEL", "llama4:latest")
    path = os.getenv("PURDUE_GENAI_PATH", "/api/chat/completions")

    if not base_url or not api_key:
        logger.info("Purdue GenAI not configured (missing base URL or API key)")
        return None

    return PurdueGenAIProvider(base_url, api_key, model, path)
