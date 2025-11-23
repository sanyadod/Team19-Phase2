from flask import Flask, request, abort, jsonify
import boto3
from botocore.exceptions import ClientError
import logging
from typing import List, Dict, Any

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
S3_BUCKET_DEFAULT = "ece-registry"
S3_CLIENT = boto3.client("s3", region_name=AWS_REGION)

DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")

VALID_TYPES = {"model", "dataset", "code"}
MAX_RESULTS = 1000  # Maximum number of artifacts to return (to prevent 413)


def _require_auth() -> str:
    """
    Simple auth check (currently disabled in endpoints).
    """
    token = request.headers.get("X-Authorization")
    if not token or not token.strip():
        abort(
            403,
            description="Authentication failed due to invalid or missing AuthenticationToken.",
        )
    return token


def _valid_type(artifact_type: str) -> bool:
    return artifact_type in VALID_TYPES


def _valid_id(artifact_id: str) -> bool:
    if not artifact_id:
        return False
    return all(c.isalnum() or c in "-._" for c in artifact_id)


def _fetch_metadata(artifact_type: str, artifact_id: str) -> dict:
    """Read artifact metadata from DynamoDB."""
    try:
        resp = META_TABLE.get_item(Key={"id": artifact_id})
    except ClientError as e:
        logger.error(
            "DynamoDB get_item FAILED for artifact_id=%s: %s",
            artifact_id,
            e,
            exc_info=True,
        )
        logger.error(
            "DynamoDB error code: %s, error message: %s, table: %s",
            e.response.get("Error", {}).get("Code", "Unknown"),
            e.response.get("Error", {}).get("Message", "Unknown"),
            META_TABLE.table_name,
        )
        abort(500, description="The artifact storage encountered an error.")

    item = resp.get("Item")
    if not item:
        logger.error(
            "Artifact NOT FOUND in DynamoDB: artifact_type=%s, artifact_id=%s, table=%s",
            artifact_type,
            artifact_id,
            META_TABLE.table_name,
        )
        logger.error(
            "DynamoDB response: Item key was 'id'=%s, but no item returned", artifact_id
        )
        abort(404, description="Artifact does not exist.")

    if item.get("artifact_type") != artifact_type:
        logger.error(
            "Artifact TYPE MISMATCH: expected=%s, found=%s, artifact_id=%s, item_keys=%s",
            artifact_type,
            item.get("artifact_type"),
            artifact_id,
            list(item.keys()),
        )
        logger.error("Full item data: %s", item)
        abort(404, description="Artifact does not exist.")

    return item


def _generate_presigned_url(bucket: str, key: str) -> str:
    """Create a temporary URL for downloading the object from S3."""
    try:
        url = S3_CLIENT.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=3600,  # 1 hour
        )
        if not url or not isinstance(url, str) or not url.startswith("https://"):
            logger.error(
                "Presigned URL GENERATION FAILED: invalid URL format, bucket=%s, key=%s, url=%s",
                bucket,
                key,
                url,
            )
            abort(500, description="The artifact storage encountered an error.")
        return url
    except ClientError as e:
        logger.error(
            "S3 presigned URL generation FAILED: bucket=%s, key=%s, error=%s",
            bucket,
            key,
            e,
            exc_info=True,
        )
        logger.error(
            "S3 error code: %s, error message: %s, error response: %s",
            e.response.get("Error", {}).get("Code", "Unknown"),
            e.response.get("Error", {}).get("Message", "Unknown"),
            e.response,
        )
        abort(500, description="The artifact storage encountered an error.")
    except Exception as e:
        logger.error(
            "Unexpected error generating presigned URL: bucket=%s, key=%s, error=%s",
            bucket,
            key,
            e,
            exc_info=True,
        )
        abort(500, description="The artifact storage encountered an error.")


@app.post("/artifacts")
def list_artifacts():
    """
    POST /artifacts
    BASELINE: Get artifacts from the registry based on query.
    Accepts an array of ArtifactQuery objects and returns matching artifacts.
    """
    try:
        queries: List[Dict[str, Any]] = request.get_json(force=True, silent=True)
        if queries is None:
            queries = request.get_json(silent=True)
    except Exception as e:
        logger.error("JSON PARSE FAILED: %s", e, exc_info=True)
        logger.error(
            "Request data: %s, content_type: %s", request.data, request.content_type
        )
        abort(
            400,
            description="There is missing field(s) in the artifact_query or it is formed improperly, or is invalid.",
        )

    if queries is None:
        logger.error(
            "Request body is None or not JSON. Content-Type: %s, Data length: %d, Data: %s",
            request.content_type,
            len(request.data) if request.data else 0,
            request.data,
        )
        abort(
            400,
            description="There is missing field(s) in the artifact_query or it is formed improperly, or is invalid.",
        )

    if not isinstance(queries, list) or not queries:
        logger.error(
            "Invalid queries format: type=%s, value=%s, is_list=%s, is_empty=%s",
            type(queries),
            queries,
            isinstance(queries, list),
            not queries if isinstance(queries, list) else "N/A",
        )
        abort(
            400,
            description="There is missing field(s) in the artifact_query or it is formed improperly, or is invalid.",
        )

    # Get pagination offset
    offset_str = request.args.get("offset")
    offset = int(offset_str) if offset_str and offset_str.isdigit() else 0

    results: List[Dict[str, Any]] = []

    try:
        # Scan all items from DynamoDB (for "*" query or name matching)
        response = META_TABLE.scan()
        all_items = response.get("Items", [])

        # Handle pagination token if present
        while "LastEvaluatedKey" in response:
            response = META_TABLE.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            all_items.extend(response.get("Items", []))

        # Process each query
        for idx, query in enumerate(queries):
            if not isinstance(query, dict) or "name" not in query:
                logger.error(
                    "Invalid query #%d: not a dict or missing 'name' field: %s",
                    idx + 1,
                    query,
                )
                continue

            query_name = str(query.get("name", "")).strip()
            query_types = query.get("types", [])

            # Filter by query - find exact match and return only one result per query
            for item in all_items:
                artifact_type = item.get("artifact_type", "")
                artifact_name = item.get("filename", "")
                artifact_id = item.get("id", "")

                # Filter by type if specified
                if query_types and artifact_type not in query_types:
                    continue

                # Match by name: "*" means all, otherwise exact match only
                if query_name == "*":
                    results.append(
                        {
                            "name": artifact_name,
                            "id": artifact_id,
                            "type": artifact_type,
                        }
                    )
                elif query_name == artifact_name:  # Exact match only
                    results.append(
                        {
                            "name": artifact_name,
                            "id": artifact_id,
                            "type": artifact_type,
                        }
                    )
                    break  # Only return first exact match

        # Remove duplicates (same artifact might match multiple queries)
        seen_ids = set()
        unique_results = []
        for result in results:
            if result["id"] not in seen_ids:
                seen_ids.add(result["id"])
                unique_results.append(result)

        results = unique_results

        # Check if too many results
        if len(results) > MAX_RESULTS:
            logger.error(
                "Too many results: %d exceeds MAX_RESULTS=%d",
                len(results),
                MAX_RESULTS,
            )
            abort(413, description="Too many artifacts returned.")

        # Apply pagination
        page_size = 100  # Default page size
        total = len(results)
        end_idx = min(offset + page_size, total)
        paginated_results = results[offset:end_idx]

        # Calculate next offset
        next_offset = str(end_idx) if end_idx < total else None

        # Build response with offset header
        response_obj = jsonify(paginated_results)
        if next_offset:
            response_obj.headers.add("offset", next_offset)

        logger.info(
            "POST /artifacts: success, returning %d/%d artifacts",
            len(paginated_results),
            total,
        )
        return response_obj, 200

    except ClientError as e:
        logger.error("DynamoDB scan FAILED: %s", e, exc_info=True)
        logger.error(
            "DynamoDB error code: %s, error message: %s, table: %s",
            e.response.get("Error", {}).get("Code", "Unknown"),
            e.response.get("Error", {}).get("Message", "Unknown"),
            META_TABLE.table_name,
        )
        abort(500, description="The artifact storage encountered an error.")
    except Exception as e:
        logger.error("Unexpected error listing artifacts: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")


@app.get("/artifacts/<artifact_type>/<artifact_id>")
def get_artifact(artifact_type: str, artifact_id: str):
    """
    BASELINE: Return artifact metadata and a URL + download_url (not raw bytes).
    """

    # If you later need auth, uncomment:
    # _require_auth()

    # 400 on bad type/id
    if not _valid_type(artifact_type):
        logger.error(
            "Invalid artifact_type: %s (valid types: %s)", artifact_type, VALID_TYPES
        )
        abort(
            400,
            description="There is missing field(s) in the artifact_type or artifact_id ",
        )

    if not _valid_id(artifact_id):
        logger.error(
            "Invalid artifact_id: %s (must be alphanumeric with -._ allowed)",
            artifact_id,
        )
        abort(
            400,
            description="There is missing field(s) in the artifact_type or artifact_id ",
        )

    # 404 if artifact not found
    meta = _fetch_metadata(artifact_type, artifact_id)

    bucket = meta.get("s3_bucket", S3_BUCKET_DEFAULT)
    key = meta["s3_key"]  # e.g. "model/12345.zip"
    filename = meta.get("filename", artifact_id)
    source_url = meta.get("source_url", "")

    # Use ID and type from DynamoDB to ensure consistency
    db_artifact_id = str(meta.get("id", artifact_id))
    db_artifact_type = str(meta.get("artifact_type", artifact_type))

    # Always include url and download_url
    presigned_url = _generate_presigned_url(bucket, key)

    data = {
        "url": source_url,
        "download_url": presigned_url,
    }

    body = {
        "metadata": {
            "name": filename,
            "id": db_artifact_id,
            "type": db_artifact_type,
        },
        "data": data,
    }

    # Basic validation of response structure
    if "metadata" not in body or "data" not in body:
        logger.error("Invalid response structure: %s", body)
        abort(500, description="The artifact storage encountered an error.")

    if (
        "name" not in body["metadata"]
        or "id" not in body["metadata"]
        or "type" not in body["metadata"]
    ):
        logger.error("Invalid metadata: %s", body["metadata"])
        abort(500, description="The artifact storage encountered an error.")

    if "url" not in body["data"] or "download_url" not in body["data"]:
        logger.error("Invalid data: %s", body["data"])
        abort(500, description="The artifact storage encountered an error.")

    logger.info(
        "GET /artifacts/%s/%s: success, id=%s, download_url_len=%d",
        artifact_type,
        artifact_id,
        db_artifact_id,
        len(data["download_url"]),
    )
    return jsonify(body), 200


if __name__ == "__main__":
    # Run Flask dev server
    app.run(host="0.0.0.0", port=5001, debug=True)
