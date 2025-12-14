from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")

VALID_TYPES = {"model", "dataset", "code"}

# Import license scoring function
try:
    from acmecli.metrics.repo_scan import license_score
    from acmecli.metrics.hf_api import build_context_from_api, get_model_license
    SCORING_AVAILABLE = True
except ImportError as e:
    logger.warning(f"License scoring not available: {e}")
    SCORING_AVAILABLE = False


def _valid_type(artifact_type: str) -> bool:
    return artifact_type in VALID_TYPES


def _valid_id(artifact_id: str) -> bool:
    if not artifact_id:
        return False
    return all(c.isalnum() or c in "-._" for c in artifact_id)


def _fetch_metadata(artifact_type: str, artifact_id: str) -> dict:
    """Fetch artifact metadata from DynamoDB."""
    try:
        resp = META_TABLE.get_item(Key={"id": artifact_id})
    except ClientError as e:
        logger.error(f"DynamoDB get_item failed: {e}", exc_info=True)
        abort(500, description="The artifact storage encountered an error.")

    item = resp.get("Item")
    if not item:
        abort(404, description="Artifact does not exist.")

    if item.get("artifact_type") != artifact_type:
        abort(404, description="Artifact does not exist.")

    return item


def _get_license_text(artifact_type: str, metadata: dict) -> str:
    """
    Get license text for an artifact.
    Priority:
    1. Check if license_text is stored in metadata (from ingest)
    2. Check if license_score is stored (can infer from score)
    3. For models, fetch from HuggingFace API if source_url is available
    4. Return empty string if none found
    """
    # First, check if license_text is directly stored
    license_text = metadata.get("license_text", "")
    if license_text:
        return str(license_text)
    
    # For models ingested via /artifacts/ingest, try to fetch from source URL
    if artifact_type == "model" and SCORING_AVAILABLE:
        source_url = metadata.get("source_url", "")
        if source_url and "huggingface.co/" in source_url:
            try:
                logger.info(f"Fetching license from HuggingFace for {source_url}")
                context = build_context_from_api(source_url)
                license_text = context.get("license_text", "")
                if license_text:
                    return str(license_text)
            except Exception as e:
                logger.warning(f"Failed to fetch license from HuggingFace: {e}")
    
    return ""


@app.route("/artifact/<artifact_type>/<artifact_id>/license-check", methods=["POST"])
def license_check(artifact_type: str, artifact_id: str):
    """
    POST /artifact/<artifact_type>/<artifact_id>/license-check
    Check license compliance for an artifact.
    """
    # Validate artifact type
    if not _valid_type(artifact_type):
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )
    
    # Validate artifact ID
    if not _valid_id(artifact_id):
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )

    # Fetch artifact metadata
    metadata = _fetch_metadata(artifact_type, artifact_id)
    
    # Get license text
    license_text = _get_license_text(artifact_type, metadata)
    
    # Compute license score
    if SCORING_AVAILABLE:
        score, latency = license_score(license_text)
    else:
        # Fallback: simple heuristic
        if not license_text:
            score = 0.5
        else:
            license_lower = license_text.lower()
            if any(tok in license_lower for tok in ["lgpl-2.1", "apache-2.0", "mit", "bsd", "mpl-2.0", "cc-by-4.0", "unlicense"]):
                score = 1.0
            elif any(tok in license_lower for tok in ["gpl-3.0", "gpl v3", "agpl"]):
                score = 0.0
            else:
                score = 0.5
    
    # Determine compliance status
    # 1.0 = compliant, 0.0 = non-compliant, 0.5 = unclear
    is_compliant = (score == 1.0)
    
    result = {
        "is_compliant": is_compliant,
        "license": license_text if license_text else None,
        "score": float(score)
    }
    
    return jsonify(result), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5007, debug=True)

