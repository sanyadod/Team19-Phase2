from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging

app = Flask(__name__)
logger = logging.getLogger(__name__)

S3_BUCKET_DEFAULT = "ece-registry"
AWS_REGION = "us-east-1"

s3_client = boto3.client("s3", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = dynamodb.Table("artifact")   # <- your DynamoDB table name

VALID_TYPES = {"model", "dataset", "code"}


# -------- Auth / validation helpers --------

def _require_auth() -> str:
    token = request.headers.get("X-Authorization")
    if not token or not token.strip():
        abort(403, description="Authentication failed due to invalid or missing AuthenticationToken.")
    return token


def _valid_type(artifact_type: str) -> bool:
    return artifact_type in VALID_TYPES


def _valid_id(artifact_id: str) -> bool:
    if not artifact_id:
        return False
    # Allow alphanumeric, hyphens, dots, and underscores
    return all(c.isalnum() or c in "-._" for c in artifact_id)


# -------- DynamoDB + S3 helpers --------

def _fetch_metadata(artifact_type: str, artifact_id: str) -> dict:
    """
    Look up artifact metadata in DynamoDB.
    Schema (what you created in the console):
      id           (partition key, e.g. "1")
      artifact_type
      s3_bucket
      s3_key
      ...
    """
    try:
        resp = META_TABLE.get_item(Key={"id": artifact_id})
    except ClientError as e:
        logger.error("DynamoDB get_item failed: %s", e, exc_info=True)
        abort(500, description="The artifact cost calculator encountered an error.")

    item = resp.get("Item")
    if not item:
        abort(404, description="Artifact does not exist.")

    if item.get("artifact_type") != artifact_type:
        abort(404, description="Artifact does not exist.")

    return item


def _find_s3_key_and_size(artifact_type: str, artifact_id: str):
    """
    Use DynamoDB metadata to get the correct S3 bucket/key, then head_object
    to retrieve the size.
    """
    meta = _fetch_metadata(artifact_type, artifact_id)

    bucket = meta.get("s3_bucket", S3_BUCKET_DEFAULT)
    key = meta["s3_key"]  # e.g. "model/bert.zip"

    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            abort(404, description="Artifact does not exist.")
        logger.error("S3 head_object failed: %s", e, exc_info=True)
        abort(500, description="The artifact cost calculator encountered an error.")

    size_bytes = head.get("ContentLength", 0)
    if size_bytes <= 0:
        logger.warning(f"Found artifact with 0 bytes at {bucket}/{key}")
        abort(404, description="Artifact does not exist or has zero size.")

    return key, size_bytes


def _get_artifact_size_mb(artifact_type: str, artifact_id: str) -> float:
    key, size_bytes = _find_s3_key_and_size(artifact_type, artifact_id)

    size_mb = size_bytes / (1024 * 1024)
    if size_mb < 0.01:
        return round(size_mb, 6)  # small files
    else:
        return round(size_mb, 2)  # larger files


# -------- Endpoint --------

@app.route("/artifact/<artifact_type>/<artifact_id>/cost", methods=["GET"])
def get_artifact_cost(artifact_type: str, artifact_id: str):
    _require_auth()

    if not _valid_type(artifact_type) or not _valid_id(artifact_id):
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )

    # parse the dependency flag
    dep_raw = request.args.get("dependency", "false")
    dependency = dep_raw.lower() in ("true", "1", "yes")

    # compute the standalone cost and total cost (here: just size in MB)
    standalone_cost = _get_artifact_size_mb(artifact_type, artifact_id)
    total_cost = standalone_cost

    result = {
        artifact_id: {
            "total_cost": total_cost,
        }
    }
    if dependency:
        result[artifact_id]["standalone_cost"] = standalone_cost

    return jsonify(result), 200


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5002, debug=True)