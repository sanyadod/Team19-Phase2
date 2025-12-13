from flask import Flask, request, jsonify, abort
from botocore.exceptions import ClientError, NoCredentialsError
from flask_cors import CORS
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import math

DYNAMODB = boto3.resource("dynamodb", region_name="us-east-1")
META_TABLE = DYNAMODB.Table("artifact")

from acmecli.baseline.modeldb import (
    get_model_item,
    compute_netscore,
    compute_treescore,
)

from acmecli.metrics.hf_api import (
    build_context_from_api,
    popularity_downloads_likes,
    freshness_days_since_update,
)

from acmecli.metrics.repo_scan import (
    license_score,
    rampup_score,
    bus_factor_score,
    dataset_and_code_score,
    dataset_quality_score,
    code_quality_score,
    perf_claims_score,
)

app = Flask(__name__)
CORS(app)

# ---------- Helpers ----------

def _score_from_context(name: str, context: dict) -> dict:
    """
    Turn HF context into a ModelRating-style dict.
    This MUST match the OpenAPI ModelRating schema.
    """

    lat = context.get("latencies", {})

    # ----- Core scores using proper metric functions -----
    
    # Size scores using 1/(1+(S/C)^a) formula (from scoring.py)
    total_bytes = int(context.get("total_bytes", 50_000_000))
    S = max(0.0, float(total_bytes))
    params = {
        "raspberry_pi": (180_000_000.0, 1.4),  # ~180MB capacity
        "jetson_nano": (350_000_000.0, 1.4),  # ~350MB capacity
        "desktop_pc": (2_000_000_000.0, 1.8),  # very forgiving
        "aws_server": (4_000_000_000.0, 1.8),
    }
    size_score = {}
    for device, (C, a) in params.items():
        ratio = (S / C) if C > 0 else 0.0
        score = 1.0 / (1.0 + math.pow(max(0.0, ratio), a))
        size_score[device] = max(0.0, min(1.0, score))

    # Ramp up time: average of all 5 docs fields (from repo_scan.py)
    docs = context.get("docs", {}) or {}
    ramp_up_time, _ = rampup_score(
        docs.get("readme", 0.0),
        docs.get("quickstart", 0.0),
        docs.get("tutorials", 0.0),
        docs.get("api_docs", 0.0),
        docs.get("reproducibility", 0.0),
    )

    # License score using proper function (from repo_scan.py)
    license_score_val, _ = license_score(context.get("license_text", ""))

    # Bus factor using proper function (from repo_scan.py)
    contributors = int(context.get("contributors", 1) or 1)
    bus_factor, _ = bus_factor_score(contributors)

    # Dataset & code score using proper function (from repo_scan.py)
    dataset_present = bool(context.get("dataset_present", False))
    code_present = bool(context.get("code_present", True))
    dataset_and_code_score, _ = dataset_and_code_score(dataset_present, code_present)

    # Dataset quality using proper function (from repo_scan.py)
    dataset_doc = context.get("dataset_doc", {}) or {}
    dataset_quality, _ = dataset_quality_score(
        dataset_doc.get("source", 0.0),
        dataset_doc.get("license", 0.0),
        dataset_doc.get("splits", 0.0),
        dataset_doc.get("ethics", 0.0),
    )

    # Code quality using proper function (from repo_scan.py)
    flake8_errors = int(context.get("flake8_errors", 0) or 0)
    mypy_errors = int(context.get("mypy_errors", 0) or 0)
    isort_sorted = bool(context.get("isort_sorted", False))
    code_quality, _ = code_quality_score(flake8_errors, isort_sorted, mypy_errors)

    # Performance claims using proper function (from repo_scan.py)
    perf = context.get("perf", {}) or {}
    perf_claims, _ = perf_claims_score(
        perf.get("benchmarks", False),
        perf.get("citations", False),
    )

    # Reproducibility & reviewedness – temporary simple values
    reproducibility = 0.0   # You can improve using docs/HF metadata or GitHub later
    reviewedness = -1.0

    # Tree score (Phase 2) – for now, None or 0.0 – you already have compute_treescore for DB-backed models
    tree_score = None

    # ----- Net score (same formula as compute_netscore) -----
    net_score = (
        0.20 * license_score_val +
        0.20 * dataset_and_code_score +
        0.15 * code_quality +
        0.15 * ramp_up_time +
        0.10 * bus_factor +
        0.10 * perf_claims +
        0.05 * dataset_quality +
        0.05 * max(size_score.values())  # crude aggregate size suitability
    )

    # ----- Latencies -----
    size_latency = float(lat.get("size_score_latency", 1))
    license_latency = float(lat.get("license_latency", 1))
    ramp_latency = float(lat.get("ramp_up_time_latency", 1))
    bus_latency = float(lat.get("bus_factor_latency", 1))
    dac_latency = float(lat.get("dataset_and_code_score_latency", 1))
    dqual_latency = float(lat.get("dataset_quality_latency", 1))
    cqual_latency = float(lat.get("code_quality_latency", 1))
    perf_latency = float(lat.get("performance_claims_latency", 1))

    # Approximate net_score_latency as sum of contributors
    net_score_latency = (
        size_latency + license_latency + ramp_latency + bus_latency +
        dac_latency + dqual_latency + cqual_latency + perf_latency
    )

    # Reuse some latencies for repro/ reviewedness/ tree for now
    repro_latency = 1.0
    rev_latency = 1.0
    tree_latency = 1.0

    def ms_to_s(x: float) -> float:
        return float(x) / 1000.0
    
    size_latency_s = ms_to_s(size_latency)
    license_latency_s = ms_to_s(license_latency)
    ramp_latency_s = ms_to_s(ramp_latency)
    bus_latency_s = ms_to_s(bus_latency)
    dac_latency_s = ms_to_s(dac_latency)
    dqual_latency_s = ms_to_s(dqual_latency)
    cqual_latency_s = ms_to_s(cqual_latency)
    perf_latency_s = ms_to_s(perf_latency)

    net_score_latency_s = ms_to_s(net_score_latency)

    # and return *_latency fields using the *_s values


    return {
        "name": name,
        "category": "huggingface-model",
        "net_score": float(net_score),
        "net_score_latency": float(net_score_latency),
        "ramp_up_time": float(ramp_up_time),
        "ramp_up_time_latency": float(ramp_latency),
        "bus_factor": float(bus_factor),
        "bus_factor_latency": float(bus_latency),
        "performance_claims": float(perf_claims),
        "performance_claims_latency": float(perf_latency),
        "license": float(license_score_val),
        "license_latency": float(license_latency),
        "dataset_and_code_score": float(dataset_and_code_score),
        "dataset_and_code_score_latency": float(dac_latency),
        "dataset_quality": float(dataset_quality),
        "dataset_quality_latency": float(dqual_latency),
        "code_quality": float(code_quality),
        "code_quality_latency": float(cqual_latency),
        "reproducibility": float(reproducibility),
        "reproducibility_latency": float(repro_latency),
        "reviewedness": float(reviewedness),
        "reviewedness_latency": float(rev_latency),
        "tree_score": 0.0 if tree_score is None else float(tree_score),
        "tree_score_latency": float(tree_latency),
        "size_score": size_score,
        "size_score_latency": float(size_latency),
    }

def _require_auth() -> str:
    if request.method == "OPTIONS":
        return ""

    token = (
        request.headers.get("X-Authorization")
        or request.headers.get("Authorization")
        or ""
    ).strip()

    # Auth is OPTIONAL for baseline — don't 403 if missing
    return token




def _load_model_or_404(model_id: str):
    try:
        item = get_model_item(model_id)
    except (ClientError, NoCredentialsError):
        abort(500, description="The model registry encountered a database error.")
    if not item:
        abort(404, description="Model does not exist.")
    return item


# ---------- /rate/v0 ----------

@app.get("/rate/v0/<model_id>")
def rate_v0(model_id: str):
    """
    Rate (v0) – return stored Phase 1 metrics from DynamoDB.
    NetScore + sub-scores, no extra Phase 2 metrics.
    """
    _require_auth()
    item = _load_model_or_404(model_id)

    # Use stored net_score as v0 result (no recompute)
    body = {
        "model_id": model_id,
        "version": item.get("version"),
        "net_score": float(item.get("net_score", 0.0)),
        "size_score": float(item.get("size_score", 0.0)),
        "license_score": float(item.get("license_score", 0.0)),
        "rampup_score": float(item.get("rampup_score", 0.0)),
        "bus_factor": float(item.get("bus_factor", 0.0)),
        "dataset_and_code": float(item.get("dataset_and_code", 0.0)),
        "dataset_quality": float(item.get("dataset_quality", 0.0)),
        "code_quality": float(item.get("code_quality", 0.0)),
        "perf_claims": float(item.get("perf_claims", 0.0)),
    }
    return jsonify(body), 200

# ---------- /rate/v1 ----------

@app.get("/rate/v1/<model_id>")
def rate_v1(model_id: str):
    """
    Rate (v1) – Compute full metrics and return a ModelRating object.
    """
    _require_auth()

    # Look up artifact in DynamoDB
    try:
        resp = META_TABLE.get_item(Key={"id": model_id})
    except (ClientError, NoCredentialsError):
        abort(500, description="The model registry encountered a database error.")

    item = resp.get("Item")
    if not item:
        abort(404, description="Model does not exist.")

    if item.get("artifact_type") != "model":
        abort(400, description="The artifact_id is not a model artifact.")


    source_url = item.get("source_url")
    if not source_url:
        abort(500, description="Model source_url is missing; cannot rate.")

    # Build HF context (this is where all the heavy lifting happens)
    try:
        context = build_context_from_api(source_url)
    except Exception as e:
        # If HF lookup fails, comply with spec's 500-language
        abort(500, description="The artifact rating system encountered an error while computing at least one metric.")

    # Convert context into ModelRating fields
    model_name = item.get("filename") or item.get("name") or model_id
    rating = _score_from_context(model_name, context)

    return jsonify(rating), 200


@app.route("/artifact/model/<model_id>/rate", methods=["GET", "OPTIONS"])
def model_artifact_rate(model_id: str):
    if request.method == "OPTIONS":
        return ("", 200)
    return rate_v1(model_id)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
