from flask import Flask, request, abort, jsonify
import logging
import json

# Import your existing upload module
import acmecli.baseline.upload as upload_module

app = Flask(__name__)
logger = logging.getLogger(__name__)

VALID_TYPES = {"model", "dataset", "code"}
DEFAULT_TYPE = "model"

# Import Phase 1 scoring (if available)
try:
    from acmecli.metrics.hf_api import build_context_from_api
    from acmecli.scoring import compute_all_scores
    SCORING_AVAILABLE = True
    logger.info("Phase 1 scoring system loaded successfully")
except ImportError as e:
    logger.warning(f"Phase 1 scoring not available: {e}")
    SCORING_AVAILABLE = False


def score_model(url: str) -> dict:
    if not SCORING_AVAILABLE:
        logger.warning("Using mock scores - Phase 1 scoring not available")
        return {
            "net_score": 0.75,
            "license": 0.8,
            "ramp_up_time": 0.7,
            "bus_factor": 0.6,
            "dataset_and_code_score": 0.9,
            "dataset_quality": 0.7,
            "code_quality": 0.8,
            "performance_claims": 0.6,
            "size_score": {
                "raspberry_pi": 0.5,
                "jetson_nano": 0.6,
                "desktop_pc": 0.9,
                "aws_server": 1.0
            }
        }

    try:
        logger.info(f"Building context for {url}")
        ctx = build_context_from_api(url)

        logger.info(f"Computing scores for {url}")
        scores = compute_all_scores(ctx)

        return scores
    except Exception as e:
        logger.error(f"Failed to score model {url}: {e}", exc_info=True)
        raise


def check_ingestibility(scores: dict) -> tuple:
    required_metrics = [
        "license",
        "ramp_up_time",
        "bus_factor",
        "dataset_and_code_score",
        "dataset_quality",
        "code_quality",
        "performance_claims"
    ]

    failing_metrics = []
    for metric in required_metrics:
        score = scores.get(metric, 0)
        if score < 0.5:
            failing_metrics.append(f"{metric}={score:.2f}")

    if failing_metrics:
        reason = (
            "Model does not meet ingest criteria. "
            f"Failing metrics: {', '.join(failing_metrics)}"
        )
        logger.warning(reason)
        return False, reason

    logger.info("Model meets all ingest criteria (all metrics >= 0.5)")
    return True, "Model meets all ingest criteria"


@app.route("/artifacts/ingest", methods=["POST"])
def ingest_artifact():
    try:
        payload = request.get_json(silent=True)
        if payload is None:
            abort(
                400,
                description="There is missing field(s) in the artifact_data or it is formed improperly."
            )

        if "url" not in payload:
            abort(400, description="Missing required field 'url' in request body.")

        url = payload.get("url", "").strip()
        if not url:
            abort(400, description="URL cannot be empty.")

        # OPTIONAL but IMPORTANT: handle name explicitly
        name = payload.get("name")
        if name is not None:
            name = name.strip()
            if not name:
                abort(400, description="Artifact name cannot be empty.")
            logger.info(f"Using provided artifact name: {name}")
        else:
            logger.info("No artifact name provided; inferring from URL")

        artifact_type = payload.get("type", DEFAULT_TYPE)
        if artifact_type not in VALID_TYPES:
            abort(
                400,
                description=f"Invalid artifact type. Must be one of: {', '.join(VALID_TYPES)}."
            )

        if "huggingface.co/" not in url:
            abort(400, description="URL must be a HuggingFace model URL")

        logger.info(f"Ingest request: type={artifact_type}, url={url}")

        # Step 1: score
        scores = score_model(url)

        # Step 2: ingestibility
        is_ingestible, reason = check_ingestibility(scores)
        if not is_ingestible:
            abort(400, description=reason)

        # Step 3: forward payload to upload module
        upload_payload = {"url": url}
        if name is not None:
            upload_payload["name"] = name

        with app.test_request_context(
            f"/artifact/{artifact_type}",
            method="POST",
            json=upload_payload,
            content_type="application/json"
        ):
            response_body, status_code = upload_module.create_artifact(artifact_type)

            if isinstance(response_body, str):
                response_data = json.loads(response_body)
            else:
                response_data = response_body.get_json()

            response_data["scores"] = scores

            logger.info(
                f"Successfully ingested artifact "
                f"(ID: {response_data.get('metadata', {}).get('id')})"
            )

            return jsonify(response_data), status_code

    except Exception as e:
        logger.error(f"Unexpected error in ingest_artifact: {e}", exc_info=True)
        abort(500, description="The artifact registry encountered an error.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5006, debug=True)
