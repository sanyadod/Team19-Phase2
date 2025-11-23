from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)

S3_BUCKET_DEFAULT = "ece-registry"
AWS_REGION = "us-east-1"

s3_client = boto3.client("s3", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = dynamodb.Table("artifact")  

VALID_TYPES = {"model", "dataset", "code"}

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
        abort(500, description="The artifact cost calculator encountered an error.")

    content_length = head.get("ContentLength", 0)
    # Ensure ContentLength is converted to int (handles cases where it might be Unset or other types)
    size_bytes = int(content_length) if content_length is not None else 0
    
    if size_bytes <= 0:
        abort(404, description="Artifact does not exist or has zero size.")

    return key, size_bytes


def _get_artifact_size_mb(artifact_type: str, artifact_id: str) -> float:
    key, size_bytes = _find_s3_key_and_size(artifact_type, artifact_id)

    size_mb = size_bytes / (1024 * 1024)
    
    if size_mb < 0.01:
        rounded_size = round(size_mb, 6)  # small files
        return rounded_size
    else:
        rounded_size = round(size_mb, 2)  # larger files
        return rounded_size

@app.route("/artifact/<artifact_type>/<artifact_id>/cost", methods=["GET"])
def get_artifact_cost(artifact_type: str, artifact_id: str):
    #_require_auth()

    if not _valid_type(artifact_type):
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )
    
    if not _valid_id(artifact_id):
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
    standalone_cost_raw = _get_artifact_size_mb(artifact_type, artifact_id)
    # Ensure values are Python floats (not Decimal or other numeric types) to avoid type comparison issues
    standalone_cost = float(standalone_cost_raw)
    total_cost = float(standalone_cost)

    result = {
        artifact_id: {
            "total_cost": total_cost,
            "standalone_cost": standalone_cost,
        }
    }

    return jsonify(result), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=True)