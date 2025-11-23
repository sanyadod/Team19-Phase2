from __future__ import annotations
import io
import json
import time
import hashlib
import zipfile
from typing import Dict, Any, Optional
from urllib.parse import urlparse

from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import requests
from boto3.dynamodb.conditions import Attr

app = Flask(__name__)

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

    # Extract human-readable name from URL
    artifact_name = _extract_name_from_url(source_url)

    # S3 key: keep artifacts organized by type
    s3_key = f"{artifact_type}/{artifact_id}.zip"

    # Download and store in S3
    size_bytes, sha256 = _download_and_store(source_url, s3_key)

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
