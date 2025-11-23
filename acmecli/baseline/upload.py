# acmecli/baseline/upload.py

from __future__ import annotations
import io
import json
import time
import hashlib
import zipfile
import logging
from typing import Dict, Any, Optional
from urllib.parse import urlparse

from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import requests

app = Flask(__name__)
logger = logging.getLogger(__name__)

# --- CONFIG ---
S3_BUCKET_DEFAULT = "ece-registry"
AWS_REGION = "us-east-1"
MAX_UNCOMPRESSED_BYTES = 200 * 1024 * 1024  # 200 MB

S3_CLIENT = boto3.client("s3", region_name=AWS_REGION)
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")

# Valid artifact categories - determines which S3 folder artifacts are stored in:
# - "model" -> stored in model/ folder
# - "dataset" -> stored in dataset/ folder  
# - "code" -> stored in code/ folder
VALID_TYPES = {"model", "dataset", "code"}


def _require_auth() -> str:
    """Baseline check mirroring download.py's swagger auth style."""
    token = request.headers.get("X-Authorization")
    if not token or not token.strip():
        abort(403, description="Authentication failed due to invalid or missing AuthenticationToken.")
    return token


def _valid_type(artifact_type: str) -> bool:
    return artifact_type in VALID_TYPES


def _valid_id(artifact_id: str) -> bool:
    if not artifact_id:
        return False
    return all(c.isalnum() or c in "-._" for c in artifact_id)


def _generate_artifact_id() -> str:
    """Generate a unique artifact ID (numeric string)."""
    # Use timestamp-based ID similar to examples in spec
    return str(int(time.time() * 1000))


def _extract_name_from_url(url: str) -> str:
    """Extract a reasonable name from a URL."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    # Get the last segment of the path
    name = path.split("/")[-1] if path else "artifact"
    # Remove common extensions
    if "." in name:
        name = name.rsplit(".", 1)[0]
    return name or "artifact"


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


def _download_and_store(url: str, s3_key: str, bucket: str = S3_BUCKET_DEFAULT) -> tuple[int, str]:
    """Download from URL and store in S3. Returns (size_bytes, sha256)."""
    try:
        response = requests.get(url, timeout=300, stream=True)
        response.raise_for_status()
        
        blob = response.content
        size = len(blob)
        sha256 = hashlib.sha256(blob).hexdigest()
        
        # Validate zip if it appears to be a zip
        if url.endswith(".zip") or "zip" in response.headers.get("Content-Type", "").lower():
            try:
                _safe_zip_check(blob)
            except ValueError as ve:
                logger.warning(f"ZIP validation warning for {url}: {ve}")
        
        # Upload to S3
        S3_CLIENT.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=blob,
            ContentType=response.headers.get("Content-Type", "application/octet-stream")
        )
        
        return size, sha256
    except requests.RequestException as e:
        logger.error(f"Failed to download from {url}: {e}", exc_info=True)
        abort(500, description="Failed to download artifact from source URL.")
    except ClientError as e:
        logger.error(f"Failed to upload to S3: {e}", exc_info=True)
        abort(500, description="The artifact storage encountered an error.")


def _check_artifact_exists(artifact_id: str) -> bool:
    """Check if artifact already exists in DynamoDB."""
    try:
        resp = META_TABLE.get_item(Key={"id": artifact_id})
        return "Item" in resp
    except ClientError:
        return False


@app.post("/artifact/<artifact_type>")
def create_artifact(artifact_type: str):
    """
    POST /artifact/{artifact_type}
    Register a new artifact by providing a downloadable source url.
    
    The artifact_type in the URL path determines the category and S3 folder:
    - "model" -> stores in S3 folder: model/
    - "dataset" -> stores in S3 folder: dataset/
    - "code" -> stores in S3 folder: code/
    
    Request body:
      {
        "url": "https://huggingface.co/google-bert/bert-base-uncased"
      }
    
    Returns:
      {
        "metadata": {
          "name": "bert-base-uncased",
          "id": "9078563412",
          "type": "model"
        },
        "data": {
          "url": "https://huggingface.co/google-bert/bert-base-uncased",
          "download_url": "https://ec2-10-121-34-12/download/bert-base-uncased"
        }
      }
    """
    # 403 on missing/invalid auth
    _require_auth()
    
    # Validate artifact_type (category) - must be one of: model, dataset, or code
    # This determines which S3 folder the artifact will be stored in
    if not _valid_type(artifact_type):
        abort(400, description="There is missing field(s) in the artifact_type or artifact_id or it is formed improperly, or is invalid.")
    
    logger.info(f"Creating artifact of type: {artifact_type} (will be stored in {artifact_type}/ folder)")
    
    # Parse request body
    try:
        payload: Dict[str, Any] = request.get_json(force=True)
    except Exception:
        abort(400, description="There is missing field(s) in the artifact_data or it is formed improperly (must include a single url).")
    
    if not payload or "url" not in payload:
        abort(400, description="There is missing field(s) in the artifact_data or it is formed improperly (must include a single url).")
    
    source_url = str(payload["url"]).strip()
    if not source_url:
        abort(400, description="There is missing field(s) in the artifact_data or it is formed improperly (must include a single url).")
    
    # Generate unique artifact ID
    artifact_id = _generate_artifact_id()
    # Ensure uniqueness (retry if collision, though unlikely)
    max_retries = 10
    for _ in range(max_retries):
        if not _check_artifact_exists(artifact_id):
            break
        artifact_id = _generate_artifact_id()
    else:
        abort(500, description="Failed to generate unique artifact ID.")
    
    # Extract name from URL
    artifact_name = _extract_name_from_url(source_url)
    
    # Determine S3 key based on artifact_type (category)
    # This ensures artifacts are organized in separate folders: model/, dataset/, or code/
    s3_key = f"{artifact_type}/{artifact_id}.zip"
    logger.info(f"Storing artifact in S3 at: {s3_key} (category: {artifact_type})")
    
    # Download and store in S3
    try:
        size_bytes, sha256 = _download_and_store(source_url, s3_key)
    except Exception as e:
        logger.error(f"Error downloading/storing artifact: {e}", exc_info=True)
        abort(500, description="The artifact storage encountered an error.")
    
    # Store metadata in DynamoDB
    try:
        META_TABLE.put_item(
            Item={
                "id": artifact_id,
                "artifact_type": artifact_type,
                "s3_bucket": S3_BUCKET_DEFAULT,
                "s3_key": s3_key,
                "filename": artifact_name,
                "source_url": source_url,
                "size_bytes": size_bytes,
                "sha256": sha256,
            }
        )
    except ClientError as e:
        logger.error(f"DynamoDB put_item failed: {e}", exc_info=True)
        # Clean up S3 object if DynamoDB write fails
        try:
            S3_CLIENT.delete_object(Bucket=S3_BUCKET_DEFAULT, Key=s3_key)
        except Exception:
            pass
        abort(500, description="The artifact storage encountered an error.")
    
    # Generate download URL (presigned URL for S3)
    try:
        download_url = S3_CLIENT.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET_DEFAULT, "Key": s3_key},
            ExpiresIn=3600,  # 1 hour
        )
    except ClientError as e:
        logger.error(f"Error generating presigned URL: {e}", exc_info=True)
        download_url = f"https://{S3_BUCKET_DEFAULT}.s3.{AWS_REGION}.amazonaws.com/{s3_key}"
    
    # Return response matching spec
    response_body = {
        "metadata": {
            "name": artifact_name,
            "id": artifact_id,
            "type": artifact_type,
        },
        "data": {
            "url": source_url,
            "download_url": download_url,
        },
    }
    
    return jsonify(response_body), 201


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # run Flask locally
    app.run(host="0.0.0.0", port=5002, debug=True)
