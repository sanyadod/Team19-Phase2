from flask import Flask, request, jsonify, abort
from botocore.exceptions import ClientError, NoCredentialsError
from flask_cors import CORS
import time
from acmecli.baseline.modeldb import (
    get_model_item,
    compute_netscore,
    compute_treescore,
)

app = Flask(__name__)
CORS(app)

# ---------- Helpers ----------

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
import time
...
@app.get("/rate/v1/<model_id>")
def rate_v1(model_id: str):
    """
    Rate (v1) – Compute full metrics and return a ModelRating object.
    """
    _require_auth()
    item = _load_model_or_404(model_id)

    # Phase 1 recomputed NetScore + measure latency if you want
    t0 = time.perf_counter()
    net_score = compute_netscore(item)
    net_score_latency = time.perf_counter() - t0

    # Phase 2 metrics
    treescore = compute_treescore(item)
    reproducibility = float(item.get("reproducibility", 0.0))
    reviewedness = float(item.get("reviewedness", 0.0))

    # Map your stored scores into the spec fields
    ramp_up_time = float(item.get("rampup_score", 0.0))
    bus_factor = float(item.get("bus_factor", 0.0))
    performance_claims = float(item.get("perf_claims", 0.0))
    license_score = float(item.get("license_score", 0.0))
    dataset_and_code_score = float(item.get("dataset_and_code", 0.0))
    dataset_quality = float(item.get("dataset_quality", 0.0))
    code_quality = float(item.get("code_quality", 0.0))
    size_score_base = float(item.get("size_score", 0.0))

    body = {
        # high-level info
        "name": item.get("name", f"model-{model_id}"),
        "category": item.get("category", "unknown"),

        # overall score
        "net_score": float(net_score),
        "net_score_latency": float(net_score_latency),

        # ramp-up
        "ramp_up_time": ramp_up_time,
        "ramp_up_time_latency": 0.0,

        # bus factor
        "bus_factor": bus_factor,
        "bus_factor_latency": 0.0,

        # performance claims
        "performance_claims": performance_claims,
        "performance_claims_latency": 0.0,

        # license
        "license": license_score,
        "license_latency": 0.0,

        # dataset + code
        "dataset_and_code_score": dataset_and_code_score,
        "dataset_and_code_score_latency": 0.0,

        "dataset_quality": dataset_quality,
        "dataset_quality_latency": 0.0,

        "code_quality": code_quality,
        "code_quality_latency": 0.0,

        # phase 2 metrics
        "reproducibility": reproducibility,
        "reproducibility_latency": 0.0,

        "reviewedness": reviewedness,
        "reviewedness_latency": 0.0,

        "tree_score": float(treescore) if treescore is not None else 0.0,
        "tree_score_latency": 0.0,

        # size_score as an object for different deployment targets
        "size_score": {
            "raspberry_pi": size_score_base,
            "jetson_nano": size_score_base,
            "desktop_pc": size_score_base,
            "aws_server": size_score_base,
        },
        "size_score_latency": 0.0,
    }

    return jsonify(body), 200


@app.route("/artifact/model/<model_id>/rate", methods=["GET", "OPTIONS"])
def model_artifact_rate(model_id: str):
    """
    Implements GET /artifact/model/{id}/rate from the OpenAPI spec.

    - OPTIONS: CORS preflight, no auth, no DB.
    - GET:     Delegate to the v1 rating logic.
    """
    # if request.method == "OPTIONS":
    #     # Preflight should succeed with no auth/DB work
    #     return ("", 204)

    # Actual GET
    return rate_v1(model_id)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)

