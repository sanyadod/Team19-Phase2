from __future__ import annotations
import io
import json
import time
import hashlib
import zipfile
import logging
from typing import Dict, Any, Optional, List
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

def _valid_id(artifact_id: str) -> bool:
    """Validate artifact ID format."""
    if not artifact_id:
        return False
    return all(c.isalnum() or c in "-._" for c in artifact_id)


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
    try:
        response = requests.get(url, timeout=300, stream=True)
        response.raise_for_status()

        blob = response.content
        size = len(blob)
        
        sha256 = hashlib.sha256(blob).hexdigest()

        if url.endswith(".zip") or "zip" in response.headers.get("Content-Type", "").lower():
            try:
                _safe_zip_check(blob)
            except ValueError as ve:
                pass

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


def _validate_and_normalize_parents(parents_input: Any, artifact_type: str) -> List[str]:
    """
    Validate and normalize parents field from request.
    Returns validated list of parent IDs that exist in DynamoDB as models.
    """
    # Log raw input
    logger.warning("Raw parents input: %s (type: %s)", parents_input, type(parents_input).__name__)
    
    # Normalize to list: handle single string, list, or None
    if parents_input is None:
        return []
    
    if isinstance(parents_input, str):
        # Single string: convert to list
        parents_list = [parents_input.strip()] if parents_input.strip() else []
    elif isinstance(parents_input, list):
        parents_list = [str(p).strip() for p in parents_input if p]
    else:
        abort(400, description="Invalid 'parents' field: must be a list of strings or a single string.")
    
    if not parents_list:
        logger.warning("Validated parents list: [] (empty or all empty strings)")
        return []
    
    # Validate each parent ID format
    invalid_format = []
    for parent_id in parents_list:
        if not _valid_id(parent_id):
            invalid_format.append(parent_id)
    
    if invalid_format:
        abort(400, description=f"Invalid parent ID format(s): {invalid_format}. Parent IDs must contain only alphanumeric characters, hyphens, dots, and underscores.")
    
    # Validate that each parent exists in DynamoDB as a model
    validated_parents = []
    missing_parents = []
    
    for parent_id in parents_list:
        try:
            resp = META_TABLE.get_item(Key={"id": parent_id})
            item = resp.get("Item")
            
            if not item:
                missing_parents.append(parent_id)
            elif item.get("artifact_type") != "model":
                missing_parents.append(parent_id)
                logger.warning("Parent ID %s exists but is not a model (type: %s)", 
                             parent_id, item.get("artifact_type"))
            else:
                validated_parents.append(parent_id)
        except ClientError as e:
            # Treat DynamoDB errors as server errors
            logger.error("DynamoDB error checking parent ID %s: %s", parent_id, e)
            abort(500, description="The artifact storage encountered an error.")
    
    if missing_parents:
        abort(400, description=f"Parent ID(s) not found in registry as models: {missing_parents}")
    
    logger.warning("Validated parents list stored: %s", validated_parents)
    return validated_parents


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

    # S3 key: keep artifacts organized by type
    s3_key = f"{artifact_type}/{artifact_id}.zip"

    # Download and store in S3
    size_bytes, sha256 = _download_and_store(source_url, s3_key)

    # Validate and normalize parents field (only for models)
    validated_parents = []
    if artifact_type == "model" and "parents" in payload:
        validated_parents = _validate_and_normalize_parents(payload.get("parents"), artifact_type)

    # Write metadata to DynamoDB
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
    
    # Add parents field if validated (only for models)
    if artifact_type == "model" and validated_parents:
        db_item["parents"] = validated_parents
    
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
