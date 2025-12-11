from flask import Flask, request, jsonify, abort
from botocore.exceptions import ClientError, NoCredentialsError
from flask_cors import CORS

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

@app.get("/rate/v1/<model_id>")
def rate_v1(model_id: str):
    """
    Rate (v1) – Compute full metrics:
      - recomputed NetScore from sub-scores
      - Reproducibility (0–1)
      - Reviewedness (0 to –1)
      - Treescore (parent avg)
    """
    _require_auth()
    item = _load_model_or_404(model_id)

    # Phase 1 recomputed NetScore
    net_score = compute_netscore(item)

    # Phase 2 metrics
    treescore = compute_treescore(item)
    reproducibility = float(item.get("reproducibility", 0.0))
    reviewedness = float(item.get("reviewedness", -1.0))

    body = {
        "model_id": model_id,
        "version": item.get("version"),

        # Phase 1
        "net_score": net_score,
        "size_score": float(item.get("size_score", 0.0)),
        "license_score": float(item.get("license_score", 0.0)),
        "rampup_score": float(item.get("rampup_score", 0.0)),
        "bus_factor": float(item.get("bus_factor", 0.0)),
        "dataset_and_code": float(item.get("dataset_and_code", 0.0)),
        "dataset_quality": float(item.get("dataset_quality", 0.0)),
        "code_quality": float(item.get("code_quality", 0.0)),
        "perf_claims": float(item.get("perf_claims", 0.0)),

        # Phase 2
        "reproducibility": reproducibility,
        "reviewedness": reviewedness,
        "treescore": treescore,
    }

    return jsonify(body), 200

@app.route("/artifact/model/<model_id>/rate", methods=["GET", "OPTIONS"])
def model_artifact_rate(model_id: str):
    """
    Implements GET /artifact/model/{id}/rate from the OpenAPI spec.

    - OPTIONS: CORS preflight, no auth, no DB.
    - GET:     Delegate to the v1 rating logic.
    """
    if request.method == "OPTIONS":
        # Preflight should succeed with no auth/DB work
        return ("", 204)

    # Actual GET
    return rate_v1(model_id)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)

