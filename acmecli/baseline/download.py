from flask import Flask, request, Response, abort, jsonify
import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)

# --- CONFIG ---
S3_BUCKET = "ece-registry"
s3_client = boto3.client("s3")

# mapping from ?part= to the filename we expect in S3
PART_TO_FILENAME = {
    "all": "full_package.zip",
    "weights": "weights.bin",
    "dataset": "dataset_subset.zip",
}


def _filename_for_part(part: str) -> str:
    filename = PART_TO_FILENAME.get(part)
    if filename is None:
        abort(400, description="invalid part. must be one of: all, weights, dataset")
    return filename


def check_file_exists(bucket, key):
    """
    Check if a file exists in S3 without downloading it.
    Returns True if exists, False otherwise.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            return False
        # For other errors, assume it doesn't exist
        return False


def resolve_s3_key(artifact_type: str, artifact_id: str, part: str) -> str:

    filename = _filename_for_part(part)

    candidate_keys = []
    if artifact_id and artifact_id not in ("_", "-"):
        candidate_keys.append(f"{artifact_type}/{artifact_id}/{filename}")
    # Always try flat layout as well
    candidate_keys.append(f"{artifact_type}/{filename}")

    for key in candidate_keys:
        if check_file_exists(S3_BUCKET, key):
            return key

    abort(404, description="file not found in object store")


def fetch_s3_object(bucket, key):
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        # If the object doesn't exist or bucket/key wrong -> 404
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            abort(404, description="file not found in object store")
        # anything else -> 500
        abort(500, description="storage error")
    body_bytes = obj["Body"].read()
    download_name = key.split("/")[-1]
    return body_bytes, download_name


# ---- Swagger-compatible JSON endpoint ----
VALID_TYPES = {"model", "dataset", "code"}
SWAGGER_TO_S3_PREFIX = {
    "model": "models",  
    "dataset": "dataset",
    "code": "code",
}


def _stable_positive_int_from_string(value: str) -> int:
    return (hash(value) & 0x7FFFFFFF)


@app.route("/artifacts/<artifact_type>/<artifact_id>", methods=["GET"])
def get_artifact_info(artifact_type, artifact_id):
    """
    GET /artifacts/{artifact_type}/{id}

    Returns JSON with metadata and a URL pointing to the existing download endpoint.
    Requires X-Authorization header. Valid artifact_type values: model, dataset, code
    """

    # Auth check per Swagger (presence is sufficient for baseline)
    token = request.headers.get("X-Authorization")
    if not token:
        abort(403, description="Authentication failed due to invalid or missing AuthenticationToken.")

    # Validate type and id
    if artifact_type not in VALID_TYPES or not artifact_id:
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id or it is formed improperly, or is invalid."
            ),
        )

    # Map swagger type to actual S3 prefix used by the bucket
    s3_prefix = SWAGGER_TO_S3_PREFIX.get(artifact_type, artifact_type)
    resolved_key = resolve_s3_key(s3_prefix, artifact_id, part="all")

    # Build a downloadable HTTP URL to our existing downloader
    # Use request.host_url to construct absolute URL
    base = request.host_url.rstrip("/")
    # Use the original swagger-visible type in URL path for consistency? The downloader expects S3 prefix.
    # The downloader route uses /artifact/<artifact_type>/<artifact_id>/download where artifact_type is the S3 prefix.
    download_url = f"{base}/artifact/{s3_prefix}/{artifact_id}/download?part=all"

    response_body = {
        "metadata": {
            "name": artifact_id,
            "id": _stable_positive_int_from_string(artifact_id),
            "type": artifact_type,
        },
        "data": {
            # Prefer HTTP link to trigger file download via our existing endpoint
            "url": download_url,
            # Optionally include the resolved storage key for debugging/traceability (not in swagger schema):
            # "storage_key": resolved_key,
        },
    }

    return jsonify(response_body), 200


@app.route("/artifact/<artifact_type>/<artifact_id>/download", methods=["GET"])
def download_artifact(artifact_type, artifact_id):
    """
    GET /artifact/<artifact_type>/<artifact_id>/download?part=all|weights|dataset

    - artifact_type: e.g., 'models', 'dataset', or 'code'
    - artifact_id: unique string ID for that artifact (optional for flat layout)
    - part:
        all      -> full_package.zip
        weights  -> weights.bin
        dataset  -> dataset_subset.zip
    """

    # default ?part=all
    part = request.args.get("part", "all")

    # resolve key dynamically (nested or flat)
    s3_key = resolve_s3_key(artifact_type, artifact_id, part)

    # pull from S3
    file_bytes, filename = fetch_s3_object(S3_BUCKET, s3_key)

    # send it back
    resp = Response(
        file_bytes,
        mimetype="application/octet-stream",
        direct_passthrough=True,
    )
    resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp


if __name__ == "__main__":
    # run Flask locally
    app.run(host="0.0.0.0", port=5001, debug=True)
