# acmecli/baseline/upload.py

from __future__ import annotations
import io
import json
import time
import hashlib
import zipfile
from typing import Dict, Any

from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)

# --- CONFIG ---
S3_BUCKET = "ece-registry"
AWS_REGION = "us-east-1"
PRESIGNED_EXPIRES_IN = 3600  # seconds
MAX_UNCOMPRESSED_BYTES = 200 * 1024 * 1024  # 200 MB

s3_client = boto3.client("s3", region_name=AWS_REGION)

# Optional: allow only these top-level prefixes (aligns with your bucket layout)
ALLOWED_TOP_LEVEL_PREFIXES = {"models", "models2"}


def _require_auth() -> None:
    """Baseline check mirroring download.py's swagger auth style."""
    token = request.headers.get("X-Authorization")
    if not token:
        abort(403, description="Authentication failed due to invalid or missing AuthenticationToken.")


def _safe_zip_check(blob: bytes) -> None:
    """Basic supply-chain hygiene: path traversal + zip bomb guard."""
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        total = 0
        for zi in zf.infolist():
            p = zi.filename.replace("\\", "/")
            # Disallow absolute or traversal paths
            if p.startswith("/") or ".." in p.split("/"):
                raise ValueError(f"Unsafe path in zip entry: {zi.filename}")
            total += zi.file_size
            if total > MAX_UNCOMPRESSED_BYTES:
                raise ValueError("Zip appears too large when extracted (possible zip bomb).")


def _head_exists(bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            return False
        return False


@app.route("/upload/initiate", methods=["POST"])
def initiate_upload():
    """
    POST /upload/initiate
    JSON body:
      {
        "target_prefix": "models",           # top-level folder in S3 (e.g., models/models2)
        "model_name": "org/model",           # required
        "version": "1.0.0",                  # required
        "content_type": "application/zip"    # optional, defaults to zip
      }

    Returns a presigned PUT URL for direct upload to S3 + the S3 key we expect.
    """
    _require_auth()

    try:
        payload: Dict[str, Any] = request.get_json(force=True)
    except Exception:
        abort(400, description="Invalid JSON payload.")

    target_prefix = str(payload.get("target_prefix", "")).strip() or "models"
    if target_prefix not in ALLOWED_TOP_LEVEL_PREFIXES:
        abort(400, description=f"invalid target_prefix; allowed: {sorted(ALLOWED_TOP_LEVEL_PREFIXES)}")

    model_name = str(payload.get("model_name", "")).strip()
    version = str(payload.get("version", "")).strip()

    if not model_name or "/" not in model_name:
        abort(400, description="model_name must be like 'org/model'.")
    if not version:
        abort(400, description="version is required.")

    org, name = model_name.split("/", 1)
    key = f"{target_prefix}/{org}/{name}/{version}/model.zip"

    # If something already exists at that key, refuse (idempotency/safety)
    if _head_exists(S3_BUCKET, key):
        abort(409, description="An object already exists at this location.")

    content_type = payload.get("content_type", "application/zip")
    try:
        url = s3_client.generate_presigned_url(
            ClientMethod="put_object",
            Params={"Bucket": S3_BUCKET, "Key": key, "ContentType": content_type},
            ExpiresIn=PRESIGNED_EXPIRES_IN,
        )
    except ClientError:
        abort(500, description="Failed to generate presigned URL.")

    upload_id = f"ul_{int(time.time()*1000)}"
    resp = {
        "upload_id": upload_id,
        "s3_key": key,
        "presigned_url": url,
        "expires_in": PRESIGNED_EXPIRES_IN,
    }
    return jsonify(resp), 200


@app.route("/upload/complete", methods=["POST"])
def complete_upload():
    """
    POST /upload/complete
    JSON body:
      {
        "s3_key": "models/org/name/1.0.0/model.zip",  # required (from /initiate)
        "expected_sha256": "<hex>",                   # optional (client-calculated)
        "validate_zip": true                          # optional (default true)
      }

    Verifies the object exists (HEAD), optionally validates sha256 and zip safety,
    and returns metadata.
    """
    _require_auth()

    try:
        payload: Dict[str, Any] = request.get_json(force=True)
    except Exception:
        abort(400, description="Invalid JSON payload.")

    s3_key = str(payload.get("s3_key", "")).strip()
    if not s3_key:
        abort(400, description="s3_key is required.")

    validate_zip = bool(payload.get("validate_zip", True))
    expected_sha256 = payload.get("expected_sha256")

    # Check that object exists
    try:
        head = s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            abort(404, description="file not found in object store")
        abort(500, description="storage error")

    size = int(head.get("ContentLength", 0))
    etag = head.get("ETag", "").strip('"')

    # Optionally download object to validate hash/zip
    computed_sha256 = None
    if validate_zip or expected_sha256:
        try:
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
            blob = obj["Body"].read()
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404", "NotFound"):
                abort(404, description="file not found in object store")
            abort(500, description="storage error")

        computed_sha256 = hashlib.sha256(blob).hexdigest()

        if expected_sha256 and computed_sha256 != expected_sha256:
            abort(400, description="SHA-256 digest mismatch.")

        if validate_zip:
            try:
                _safe_zip_check(blob)
            except ValueError as ve:
                abort(400, description=f"ZIP validation failed: {ve}")

    # Minimal structured response; sidecar/DB can be added later
    response_body = {
        "s3_key": s3_key,
        "size_bytes": size,
        "etag": etag,
        "sha256": computed_sha256 if computed_sha256 else None,
        "validated": bool(validate_zip),
        "status": "READY",
    }
    return jsonify(response_body), 200


if __name__ == "__main__":
    # run Flask locally
    app.run(host="0.0.0.0", port=5002, debug=True)
