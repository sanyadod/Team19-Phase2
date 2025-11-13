from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, abort, jsonify, request


def _find_results_file() -> Path:
    """
    Locate the Phase 1 results JSON/NDJSON file.

    Priority:
      1. ACME_PHASE1_RESULTS env var (absolute or relative path)
      2. test_artifacts/phase1_results.jsonl
      3. test_artifacts/phase1_results.json
    """
    env_path = os.getenv("ACME_PHASE1_RESULTS")
    candidates: List[Path] = []
    if env_path:
        candidates.append(Path(env_path))

    candidates.extend(
        [
            Path("test_artifacts/phase1_results.jsonl"),
            Path("test_artifacts/phase1_results.json"),
        ]
    )

    for p in candidates:
        if p.is_file():
            return p

    raise FileNotFoundError(
        "No Phase 1 results JSON found. "
        "Set ACME_PHASE1_RESULTS or place a file in test_artifacts/ named "
        "phase1_results.jsonl or phase1_results.json."
    )


def _load_phase1_scores() -> Dict[str, Dict[str, Any]]:
    """
    Load Phase 1 scores into a dict keyed by model name.

    Supports:
      - NDJSON (.jsonl): one JSON object per line
      - JSON (.json): either a list[object] or a single object
    """
    path = _find_results_file()
    scores: Dict[str, Dict[str, Any]] = {}

    if path.suffix == ".jsonl":
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                name = str(rec.get("name"))
                if not name:
                    continue
                scores[name] = rec
    else:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)

        if isinstance(data, dict):
            # Single record – require "name"
            name = str(data.get("name"))
            if not name:
                raise ValueError("Phase 1 JSON dict is missing 'name' field")
            scores[name] = data
        elif isinstance(data, list):
            for rec in data:
                if not isinstance(rec, dict):
                    continue
                name = str(rec.get("name"))
                if not name:
                    continue
                scores[name] = rec
        else:
            raise ValueError("Unsupported Phase 1 JSON structure")

    return scores


def create_app() -> Flask:
    """
    Factory to create the Flask app used by the grading harness.
    """
    app = Flask(__name__)

    # Load once at startup (v0 requirement: use stored JSON, not live scoring).
    phase1_scores = _load_phase1_scores()

    @app.get("/rate")
    def rate() -> Any:
        """
        Return Phase 1 metrics (NetScore + sub-scores) for a given model.

        Query params (v0):
          - model: the model name or URL used as 'name' in Phase 1 output

        Response shape (example):
        {
          "name": "...",
          "category": "MODEL",
          "net_score": 0.83,
          "metrics": {
            "license": 1.0,
            "dataset_and_code_score": 0.5,
            "code_quality": 0.7,
            "ramp_up_time": 0.8,
            "bus_factor": 0.9,
            "performance_claims": 0.5,
            "dataset_quality": 0.75,
            "size_score": { ... }  # device-specific scores
          }
        }
        """
        model = request.args.get("model")
        if not model:
            abort(400, description="Missing required 'model' query parameter")

        rec = phase1_scores.get(model)
        if rec is None:
            # Optional: also allow lookup by bare model name if you stored URLs
            # e.g., "https://huggingface.co/gpt2" vs "gpt2"
            candidates = {
                name: r
                for name, r in phase1_scores.items()
                if name.split("/")[-1] == model
            }
            if len(candidates) == 1:
                rec = next(iter(candidates.values()))
            else:
                abort(404, description=f"No Phase 1 scores found for model '{model}'")

        # Phase 1 metric keys come from compute_all_scores(...) in scoring.py
        payload = {
            "name": rec.get("name"),
            "category": rec.get("category", "MODEL"),
            "net_score": rec.get("net_score"),
            "metrics": {
                "license": rec.get("license"),
                "dataset_and_code_score": rec.get("dataset_and_code_score"),
                "code_quality": rec.get("code_quality"),
                "ramp_up_time": rec.get("ramp_up_time"),
                "bus_factor": rec.get("bus_factor"),
                "performance_claims": rec.get("performance_claims"),
                "dataset_quality": rec.get("dataset_quality"),
                "size_score": rec.get("size_score"),
            },
        }
        return jsonify(payload)

    return app


# Allow `python -m acmecli.service` locally
if __name__ == "__main__":
    flask_app = create_app()
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
