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


def _download_and_store(url: str, s3_key: str, bucket: str = S3_BUCKET_DEFAULT) -> tuple[int, str]:
    """Download from URL and store in S3. Returns (size_bytes, sha256)."""
    logger.info("Starting download from URL: %s", url)
    logger.info("Target S3 location: bucket=%s, key=%s", bucket, s3_key)
    
    try:
        logger.info("Sending HTTP GET request to URL")
        response = requests.get(url, timeout=300, stream=True)
        response.raise_for_status()
        logger.info("HTTP request successful: status_code=%d, content_type=%s", 
                    response.status_code, response.headers.get("Content-Type", "unknown"))

        blob = response.content
        size = len(blob)
        logger.info("Downloaded content: size=%d bytes", size)
        
        sha256 = hashlib.sha256(blob).hexdigest()
        logger.info("Computed SHA-256: %s", sha256)

        if url.endswith(".zip") or "zip" in response.headers.get("Content-Type", "").lower():
            logger.info("Content appears to be ZIP, performing safety validation")
            try:
                _safe_zip_check(blob)
                logger.info("ZIP validation passed")
            except ValueError as ve:
                logger.warning("ZIP validation warning for %s: %s", url, ve)

        logger.info("Uploading to S3: bucket=%s, key=%s", bucket, s3_key)
        S3_CLIENT.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=blob,
            ContentType=response.headers.get("Content-Type", "application/octet-stream"),
        )
        logger.info("Successfully uploaded to S3")

        return size, sha256
    except requests.RequestException as e:
        logger.error("Failed to download from %s: %s", url, e, exc_info=True)
        abort(500, description="Failed to download artifact from source URL.")
    except ClientError as e:
        logger.error("Failed to upload to S3: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")


def _artifact_exists_by_source(artifact_type: str, url: str) -> bool:
    """
    Check if an artifact with the same artifact_type + source_url already exists.
    Used for returning 409 Conflict ("Artifact exists already").
    """
    logger.info("Checking for existing artifact: type=%s, url=%s", artifact_type, url)
    logger.info("Scanning DynamoDB table: %s", META_TABLE.table_name)
    
    try:
        resp = META_TABLE.scan(
            FilterExpression=Attr("artifact_type").eq(artifact_type)
                            & Attr("source_url").eq(url),
            ProjectionExpression="id",
        )
        items = resp.get("Items", [])
        logger.info("DynamoDB scan found %d matching items", len(items))
        
        if items:
            logger.info("Existing artifact found: %s", items[0].get("id", "unknown"))
            return True
        else:
            logger.info("No existing artifact found")
            return False
    except ClientError as e:
        logger.error("DynamoDB scan for duplicate failed: %s", e, exc_info=True)
        # Treat storage failure as server error, not "no duplicate"
        abort(500, description="The artifact storage encountered an error.")


@app.post("/artifact/<artifact_type>")
def create_artifact(artifact_type: str):
    """
    POST /artifact/{artifact_type}
    Register a new artifact by providing a downloadable source url.
    """
    logger.info("POST /artifact/%s called. Content-Type: %s, Data: %s", 
                artifact_type, request.content_type, request.data[:200] if request.data else "None")

    # 400 if artifact_type invalid
    if not _valid_type(artifact_type):
        logger.warning("Invalid artifact_type: %s (valid types: %s)", artifact_type, VALID_TYPES)
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_data or it is formed improperly "
                "(must include a single url)."
            ),
        )

    logger.info("Creating artifact of type: %s", artifact_type)

    # Parse JSON body (ArtifactData)
    payload: Optional[Dict[str, Any]] = request.get_json(silent=True)

    if payload is None and request.data:
        try:
            payload = json.loads(request.data.decode("utf-8"))
            logger.info("Parsed JSON from raw data: %s", payload)
        except Exception as e:
            logger.error("Failed to parse request body as JSON: %s", e)
            abort(
                400,
                description=(
                    "There is missing field(s) in the artifact_data or it is formed improperly "
                    "(must include a single url)."
                ),
            )

    if payload is None:
        logger.warning("Request body is None or not JSON")
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_data or it is formed improperly "
                "(must include a single url)."
            ),
        )

    logger.info("Parsed payload: %s", payload)

    if not isinstance(payload, dict):
        logger.warning("Invalid payload type: %s (expected dict)", type(payload))
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_data or it is formed improperly "
                "(must include a single url)."
            ),
        )

    if "url" not in payload:
        logger.warning("Missing 'url' field in payload. Keys: %s", list(payload.keys()))
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_data or it is formed improperly "
                "(must include a single url)."
            ),
        )

    source_url = str(payload["url"]).strip()
    if not source_url:
        logger.warning("URL field is empty or whitespace")
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_data or it is formed improperly "
                "(must include a single url)."
            ),
        )

    logger.info("Source URL: %s", source_url)

    # 409 if this artifact already exists (same type + same source_url)
    logger.info("Checking if artifact already exists: type=%s, url=%s", artifact_type, source_url)
    if _artifact_exists_by_source(artifact_type, source_url):
        logger.warning("Artifact already exists: type=%s, url=%s", artifact_type, source_url)
        abort(409, description="Artifact exists already.")

    logger.info("Artifact does not exist, proceeding with creation")

    # Generate unique artifact ID
    artifact_id = _generate_artifact_id()
    logger.info("Generated artifact ID: %s", artifact_id)

    # Extract human-readable name from URL
    artifact_name = _extract_name_from_url(source_url)
    logger.info("Extracted artifact name from URL: %s", artifact_name)

    # S3 key: keep artifacts organized by type
    s3_key = f"{artifact_type}/{artifact_id}.zip"
    logger.info("S3 key: %s (bucket: %s)", s3_key, S3_BUCKET_DEFAULT)

    # Download and store in S3
    logger.info("Downloading artifact from URL: %s", source_url)
    size_bytes, sha256 = _download_and_store(source_url, s3_key)
    logger.info("Downloaded and stored in S3: size=%d bytes, sha256=%s", size_bytes, sha256)

    # Write metadata to DynamoDB
    logger.info("Writing metadata to DynamoDB table: %s", META_TABLE.table_name)
    db_item = {
        "id": artifact_id,
        "artifact_type": artifact_type,
        "s3_bucket": S3_BUCKET_DEFAULT,
        "s3_key": s3_key,
        "filename": artifact_name,
        "source_url": source_url,
        "size_bytes": size_bytes,
        "sha256": sha256,
    }
    logger.info("DynamoDB item: %s", db_item)
    
    try:
        META_TABLE.put_item(Item=db_item)
        logger.info("Successfully wrote metadata to DynamoDB")
    except ClientError as e:
        logger.error("DynamoDB put_item failed: %s", e, exc_info=True)
        logger.warning("Cleaning up S3 object due to DynamoDB failure: %s", s3_key)
        try:
            S3_CLIENT.delete_object(Bucket=S3_BUCKET_DEFAULT, Key=s3_key)
            logger.info("S3 object deleted successfully")
        except Exception as cleanup_error:
            logger.error("Failed to delete S3 object during cleanup: %s", cleanup_error)
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

    logger.info("POST /artifact/%s completed successfully: id=%s, type=%s, name=%s, s3_key=%s, size=%d bytes", 
                artifact_type, artifact_id, artifact_type, artifact_name, s3_key, size_bytes)

    return jsonify(response_body), 201


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=True)
