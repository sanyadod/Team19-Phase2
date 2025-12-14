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
from boto3.dynamodb.conditions import Attr

app = Flask(__name__)
logger = logging.getLogger(__name__)

# --- CONFIG ---
S3_BUCKET_DEFAULT = "ece-registry"
AWS_REGION = "us-east-1"
MAX_UNCOMPRESSED_BYTES = 200 * 1024 * 1024  # 200 MB

S3_CLIENT = boto3.client("s3", region_name=AWS_REGION)
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")

VALID_TYPES = {"model", "dataset", "code"}

def _valid_type(artifact_type: str) -> bool:
    return artifact_type in VALID_TYPES


def _generate_artifact_id() -> str:
    """Generate a unique artifact ID (numeric string)."""
    return str(int(time.time() * 1000))


def _extract_name_from_url(url: str) -> str:
    """Extract a reasonable name from a URL."""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    name = path.split("/")[-1] if path else "artifact"
    if "." in name:
        name = name.rsplit(".", 1)[0]
    return name or "artifact"


def _safe_zip_check(blob: bytes) -> None:
    """Basic supply-chain hygiene: path traversal + zip bomb guard."""
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        total = 0
        for zi in zf.infolist():
            p = zi.filename.replace("\\", "/")
            if p.startswith("/") or ".." in p.split("/"):
                raise ValueError(f"Unsafe path in zip entry: {zi.filename}")
            total += zi.file_size
            if total > MAX_UNCOMPRESSED_BYTES:
                raise ValueError("Zip appears too large when extracted (possible zip bomb).")


def _validate_model_zip(blob: bytes) -> None:
    """
    Validate that a blob is a valid ZIP file containing config.json.
    Raises ValueError if validation fails.
    """
    # Check magic bytes (PK\x03\x04)
    if not blob.startswith(b'PK\x03\x04'):
        logger.warning("ZIP validation failed: Missing PK magic bytes (first bytes: %s)", 
                      blob[:10] if len(blob) >= 10 else blob)
        raise ValueError("File is not a valid ZIP file (missing PK header).")
    
    # Attempt to open as ZIP
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            # Check for config.json anywhere in the archive
            zip_files = zf.namelist()
            config_found = False
            for name in zip_files:
                normalized = name.replace("\\", "/").strip("/")
                if normalized == "config.json" or normalized.endswith("/config.json"):
                    config_found = True
                    logger.debug("Found config.json at path: %s", name)
                    break
            
            if not config_found:
                logger.warning("ZIP validation failed: config.json not found in archive (searched %d files)", 
                              len(zip_files))
                raise ValueError("ZIP file does not contain config.json.")
    except zipfile.BadZipFile as e:
        logger.warning("ZIP validation failed: BadZipFile error: %s", e)
        raise ValueError("File is not a valid ZIP file.") from e


def _download_and_store(url: str, s3_key: str, artifact_type: str, bucket: str = S3_BUCKET_DEFAULT) -> tuple[int, str]:
    """
    Download from URL and store in S3. Returns (size_bytes, sha256).
    For model artifacts, validates that the file is a ZIP containing config.json.
    """
    try:
        # Use browser-like User-Agent and allow redirects
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, allow_redirects=True, timeout=300, stream=True, headers=headers)
        response.raise_for_status()

        blob = response.content
        size = len(blob)
        
        # For model artifacts, validate ZIP before storing
        if artifact_type == "model":
            try:
                _validate_model_zip(blob)
                logger.info("Model ZIP validation passed: size=%d bytes, contains config.json", size)
            except ValueError as ve:
                logger.error("Model ZIP validation failed: %s", ve)
                abort(400, description="There is missing field(s) in the artifact_data or it is formed improperly (must include a single url).")
        
        # For non-model artifacts, perform basic zip safety check if it appears to be a zip
        elif url.endswith(".zip") or "zip" in response.headers.get("Content-Type", "").lower():
            try:
                _safe_zip_check(blob)
            except ValueError as ve:
                logger.warning("Non-model ZIP safety check failed (continuing anyway): %s", ve)
                pass
        
        sha256 = hashlib.sha256(blob).hexdigest()

        # Only store in S3 after validation passes
        S3_CLIENT.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=blob,
            ContentType=response.headers.get("Content-Type", "application/octet-stream"),
        )

        return size, sha256
    except requests.RequestException as e:
        abort(500, description="Failed to download artifact from source URL.")
    except ClientError as e:
        abort(500, description="The artifact storage encountered an error.")


def _artifact_exists_by_source(artifact_type: str, url: str) -> bool:
    """
    Check if an artifact with the same artifact_type + source_url already exists.
    Used for returning 409 Conflict ("Artifact exists already").
    """
    try:
        resp = META_TABLE.scan(
            FilterExpression=Attr("artifact_type").eq(artifact_type)
                            & Attr("source_url").eq(url),
            ProjectionExpression="id",
        )
        items = resp.get("Items", [])
        
        if items:
            return True
        else:
            return False
    except ClientError as e:
        # Treat storage failure as server error, not "no duplicate"
        abort(500, description="The artifact storage encountered an error.")


@app.post("/artifact/<artifact_type>")
def create_artifact(artifact_type: str):
    """
    POST /artifact/{artifact_type}
    Register a new artifact by providing a downloadable source url.
    """
    # 400 if artifact_type invalid
    if not _valid_type(artifact_type):
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_data or it is formed improperly "
                "(must include a single url)."
            ),
        )

    # Parse JSON body (ArtifactData)
    payload: Optional[Dict[str, Any]] = request.get_json(silent=True)

    if payload is None and request.data:
        try:
            payload = json.loads(request.data.decode("utf-8"))
        except Exception as e:
            abort(
                400,
                description=(
                    "There is missing field(s) in the artifact_data or it is formed improperly "
                    "(must include a single url)."
                ),
            )

    if payload is None:
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_data or it is formed improperly "
                "(must include a single url)."
            ),
        )

    if not isinstance(payload, dict):
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_data or it is formed improperly "
                "(must include a single url)."
            ),
        )

    if "url" not in payload:
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_data or it is formed improperly "
                "(must include a single url)."
            ),
        )

    source_url = str(payload["url"]).strip()
    if not source_url:
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_data or it is formed improperly "
                "(must include a single url)."
            ),
        )

    # 409 if this artifact already exists (same type + same source_url)
    if _artifact_exists_by_source(artifact_type, source_url):
        abort(409, description="Artifact exists already.")

    # Generate unique artifact ID
    artifact_id = _generate_artifact_id()

    # Use provided name if available, otherwise extract from URL
    if "name" in payload and payload["name"]:
        artifact_name = str(payload["name"]).strip()
        if not artifact_name:
            abort(400, description="Artifact name cannot be empty if provided.")
    else:
        # Extract human-readable name from URL
        artifact_name = _extract_name_from_url(source_url)

    '''
    # Generate unique artifact ID
    artifact_id = _generate_artifact_id()

    # Extract human-readable name from URL
    artifact_name = _extract_name_from_url(source_url)
    '''

    # S3 key: keep artifacts organized by type
    s3_key = f"{artifact_type}/{artifact_id}.zip"

    # Download and store in S3 (with validation for model artifacts)
    size_bytes, sha256 = _download_and_store(source_url, s3_key, artifact_type)

    # Write metadata to DynamoDB
    # Store "name" consistently, keep "filename" for backward compatibility
    db_item = {
        "id": artifact_id,
        "artifact_type": artifact_type,
        "s3_bucket": S3_BUCKET_DEFAULT,
        "s3_key": s3_key,
        "name": artifact_name,  # Primary name field
        "filename": artifact_name,  # Backward compatibility
        "source_url": source_url,
        "size_bytes": size_bytes,
        "sha256": sha256,
    }
    
    try:
        META_TABLE.put_item(Item=db_item)
    except ClientError as e:
        try:
            S3_CLIENT.delete_object(Bucket=S3_BUCKET_DEFAULT, Key=s3_key)
        except Exception as cleanup_error:
            pass
        abort(500, description="The artifact storage encountered an error.")

    # Response matches YAML spec: data.url contains the source URL
    # Download link is provided via GET /artifacts/{artifact_type}/{id} endpoint
    response_body = {
        "metadata": {
            "name": artifact_name,
            "id": artifact_id,
            "type": artifact_type,
        },
        "data": {
            "url": source_url,
        },
    }

    return jsonify(response_body), 201


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=True)
