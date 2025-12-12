from flask import Flask, request, jsonify, abort
from botocore.exceptions import ClientError, NoCredentialsError
from flask_cors import CORS
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

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

app = Flask(__name__)
CORS(app)

# ---------- Helpers ----------

def _score_from_context(name: str, context: dict) -> dict:
    """
    Turn HF context into a ModelRating-style dict.
    This MUST match the OpenAPI ModelRating schema.
    """

    lat = context.get("latencies", {})

    # ----- Core scores (placeholder mapping) -----
    # You probably already have a module that does this more precisely.
    # If so, REPLACE these with calls into that module.

    # Size (bytes -> 0..1 for each hardware tier)
    total_bytes = float(context.get("total_bytes", 50_000_000))
    total_mb = total_bytes / (1024 * 1024)

    def size_score_for(max_mb: float) -> float:
        # 1.0 if <= 10% of tier, 0.0 if >= tier, linear in between
        if total_mb >= max_mb:
            return 0.0
        if total_mb <= 0.1 * max_mb:
            return 1.0
        return max(0.0, min(1.0, 1 - (total_mb - 0.1 * max_mb) / (0.9 * max_mb)))

    size_score = {
        "raspberry_pi": size_score_for(500),    # 500 MB
        "jetson_nano": size_score_for(2000),   # 2 GB
        "desktop_pc":  size_score_for(8000),   # 8 GB
        "aws_server":  size_score_for(16000),  # 16 GB
    }

    # Docs quality -> ramp up time (0..1)
    docs = context.get("docs", {}) or {}
    ramp_up_time = float(docs.get("readme", 0.5))

    # License quality from context.license_text (you might have a better mapping)
    license_text = str(context.get("license_text", "")).lower()
    if "apache" in license_text:
        license_score = 1.0
    elif "mit" in license_text or "bsd" in license_text:
        license_score = 0.9
    elif "gpl" in license_text:
        license_score = 0.4
    elif license_text:
        license_score = 0.6
    else:
        license_score = 0.3

    # Bus factor from contributors
    contributors = int(context.get("contributors", 1) or 1)
    bus_factor = max(0.0, min(1.0, contributors / (contributors + 5.0)))

    # Dataset & code presence
    dataset_present = bool(context.get("dataset_present"))
    code_present = bool(context.get("code_present", True))
    dataset_and_code_score = 0.0
    if dataset_present:
        dataset_and_code_score += 0.6
    if code_present:
        dataset_and_code_score += 0.4

    # Dataset quality details
    dataset_doc = context.get("dataset_doc", {}) or {}
    dataset_quality = float(
        0.25 * dataset_doc.get("source", 0.0) +
        0.25 * dataset_doc.get("license", 0.0) +
        0.25 * dataset_doc.get("splits", 0.0) +
        0.25 * dataset_doc.get("ethics", 0.0)
    )

    # Code quality heuristics
    flake8_errors = int(context.get("flake8_errors", 10) or 10)
    mypy_errors = int(context.get("mypy_errors", 5) or 5)
    isort_sorted = bool(context.get("isort_sorted", False))

    code_quality = 1.0
    code_quality -= min(0.7, flake8_errors * 0.03)
    code_quality -= min(0.3, mypy_errors * 0.03)
    if not isort_sorted:
        code_quality -= 0.05
    code_quality = max(0.0, min(1.0, code_quality))

    # Performance claims: benchmarks + citations
    perf = context.get("perf", {}) or {}
    perf_claims = 0.0
    if perf.get("benchmarks"):
        perf_claims += 0.6
    if perf.get("citations"):
        perf_claims += 0.4

    # Reproducibility & reviewedness – temporary simple values
    reproducibility = 0.0   # You can improve using docs/HF metadata or GitHub later
    reviewedness = -1.0

    # Tree score (Phase 2) – for now, None or 0.0 – you already have compute_treescore for DB-backed models
    tree_score = None

    # ----- Net score (same formula as compute_netscore) -----
    net_score = (
        0.20 * license_score +
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
        "license": float(license_score),
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
    
    token = request.headers.get("X-Authorization")
    if not token or not token.strip():
        abort(403, description="Authentication failed due to invalid or missing AuthenticationToken.")
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
    """
    Implements GET /artifact/model/{id}/rate from the OpenAPI spec.

    - OPTIONS: CORS preflight, no auth, no DB.
    - GET:     Delegate to the v1 rating logic.
    """
    if request.method == "OPTIONS":
        # Preflight should succeed with no auth/DB work
        return ("", 200)

    # Actual GET
    return rate_v1(model_id)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)

