from flask import Flask, request, abort, jsonify
import boto3
from botocore.exceptions import ClientError
import logging
from typing import List, Dict, Any, Optional

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


def _fetch_metadata(artifact_type: str, artifact_id: str) -> dict:
    """Read artifact metadata from DynamoDB."""
    logger.info("Fetching metadata from DynamoDB: artifact_type=%s, artifact_id=%s", artifact_type, artifact_id)
    logger.info("DynamoDB table: %s", META_TABLE.table_name)
    
    try:
        logger.info("Calling DynamoDB get_item with key: id=%s", artifact_id)
        resp = META_TABLE.get_item(Key={"id": artifact_id})
        logger.info("DynamoDB get_item response received: %s", "Item found" if resp.get("Item") else "No item found")
    except ClientError as e:
        logger.error("DynamoDB get_item failed for artifact_id=%s: %s", artifact_id, e, exc_info=True)
        logger.error("Error code: %s, Error message: %s", 
                    e.response.get("Error", {}).get("Code", "Unknown"),
                    e.response.get("Error", {}).get("Message", "Unknown"))
        abort(500, description="The artifact storage encountered an error.")

    item = resp.get("Item")
    if not item:
        logger.warning("Artifact not found in DynamoDB: artifact_type=%s, artifact_id=%s", artifact_type, artifact_id)
        abort(404, description="Artifact does not exist.")

    logger.info("Metadata retrieved: artifact_type=%s, s3_bucket=%s, s3_key=%s, filename=%s",
                item.get("artifact_type"), item.get("s3_bucket"), item.get("s3_key"), item.get("filename"))

    if item.get("artifact_type") != artifact_type:
        logger.warning("Artifact type mismatch: expected=%s, found=%s, artifact_id=%s",
                      artifact_type, item.get("artifact_type"), artifact_id)
        abort(404, description="Artifact does not exist.")

    return item


def _generate_presigned_url(bucket: str, key: str) -> str:
    """Create a temporary URL for downloading the object from S3."""
    logger.info("Generating presigned URL: bucket=%s, key=%s, expires_in=3600 seconds", bucket, key)
    
    try:
        logger.info("Calling S3 generate_presigned_url for get_object operation")
        url = S3_CLIENT.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=3600,  # 1 hour
        )
        logger.info("Presigned URL generated successfully: length=%d characters, expires in 1 hour", len(url))
        logger.debug("Presigned URL: %s", url)
        return url
    except ClientError as e:
        logger.error("Error generating presigned URL for bucket=%s, key=%s: %s", bucket, key, e, exc_info=True)
        logger.error("S3 error code: %s, error message: %s",
                    e.response.get("Error", {}).get("Code", "Unknown"),
                    e.response.get("Error", {}).get("Message", "Unknown"))
        abort(500, description="The artifact storage encountered an error.")


@app.post("/artifacts")
def list_artifacts():
    """
    POST /artifacts
    BASELINE: Get artifacts from the registry based on query.
    Accepts an array of ArtifactQuery objects and returns matching artifacts.
    """
    # Parse request body
    logger.info("POST /artifacts called")
    logger.info("Request details: method=POST, path=/artifacts, content_type=%s", request.content_type)
    logger.info("Request headers: %s", dict(request.headers))
    logger.info("Request data preview (first 200 chars): %s", request.data[:200] if request.data else "None")
    logger.info("Query parameters: %s", dict(request.args))
    
    try:
        logger.info("Attempting to parse JSON from request body")
        queries: List[Dict[str, Any]] = request.get_json(force=True, silent=True)
        if queries is None:
            logger.info("First JSON parse attempt returned None, trying without force=True")
            queries = request.get_json(silent=True)
    except Exception as e:
        logger.error("Failed to parse JSON from request body: %s", e, exc_info=True)
        logger.error("Request data that failed to parse: %s", request.data)
        abort(400, description="There is missing field(s) in the artifact_query or it is formed improperly, or is invalid.")
    
    if queries is None:
        logger.warning("Request body is None or not JSON. Content-Type: %s, Data length: %d",
                      request.content_type, len(request.data) if request.data else 0)
        abort(400, description="There is missing field(s) in the artifact_query or it is formed improperly, or is invalid.")
    
    if not isinstance(queries, list) or not queries:
        logger.warning("Queries is not a list or is empty: type=%s, value=%s", type(queries), queries)
        abort(400, description="There is missing field(s) in the artifact_query or it is formed improperly, or is invalid.")
    
    logger.info("Successfully parsed queries: count=%d, queries=%s", len(queries), queries)
    
    # Get pagination offset
    offset_str = request.args.get("offset")
    offset = int(offset_str) if offset_str and offset_str.isdigit() else 0
    logger.info("Pagination offset: %d (from query param: %s)", offset, offset_str)
    
    results: List[Dict[str, Any]] = []
    
    try:
        # Scan all items from DynamoDB (for "*" query or name matching)
        logger.info("Starting DynamoDB scan operation on table: %s", META_TABLE.table_name)
        response = META_TABLE.scan()
        all_items = response.get("Items", [])
        logger.info("Initial DynamoDB scan returned %d items", len(all_items))
        
        # Handle pagination token if present
        scan_count = 1
        while "LastEvaluatedKey" in response:
            logger.info("DynamoDB scan pagination: LastEvaluatedKey present, performing additional scan (scan #%d)", scan_count + 1)
            response = META_TABLE.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            additional_items = response.get("Items", [])
            all_items.extend(additional_items)
            logger.info("Additional scan returned %d items, total items so far: %d", len(additional_items), len(all_items))
            scan_count += 1
        
        logger.info("DynamoDB scan completed: total items retrieved=%d, total scans performed=%d", len(all_items), scan_count)
        
        # Process each query
        logger.info("Processing %d query/queries", len(queries))
        for idx, query in enumerate(queries):
            logger.info("Processing query #%d: %s", idx + 1, query)
            
            if not isinstance(query, dict) or "name" not in query:
                logger.warning("Skipping invalid query #%d: not a dict or missing 'name' field: %s", idx + 1, query)
                continue
            
            query_name = str(query.get("name", "")).strip()
            query_types = query.get("types", [])
            logger.info("Query #%d details: name='%s', types=%s", idx + 1, query_name, query_types)
            
            matches_for_query = 0
            # Filter by query
            for item in all_items:
                artifact_type = item.get("artifact_type", "")
                artifact_name = item.get("filename", "")
                artifact_id = item.get("id", "")
                
                # Filter by type if specified
                if query_types and artifact_type not in query_types:
                    continue
                
                # Match by name: "*" means all, otherwise exact or partial match
                if query_name == "*" or query_name.lower() in artifact_name.lower():
                    results.append({
                        "name": artifact_name,
                        "id": artifact_id,
                        "type": artifact_type,
                    })
                    matches_for_query += 1
            
            logger.info("Query #%d matched %d artifacts", idx + 1, matches_for_query)
        
        logger.info("Total matches before deduplication: %d", len(results))
        
        # Remove duplicates (same artifact might match multiple queries)
        seen_ids = set()
        unique_results = []
        for result in results:
            if result["id"] not in seen_ids:
                seen_ids.add(result["id"])
                unique_results.append(result)
        
        results = unique_results
        logger.info("After deduplication: %d unique artifacts", len(results))
        
        # Check if too many results
        if len(results) > MAX_RESULTS:
            logger.warning("Too many results: %d exceeds MAX_RESULTS=%d", len(results), MAX_RESULTS)
            abort(413, description="Too many artifacts returned.")
        
        # Apply pagination
        page_size = 100  # Default page size
        total = len(results)
        end_idx = min(offset + page_size, total)
        paginated_results = results[offset:end_idx]
        
        logger.info("Pagination: total=%d, offset=%d, page_size=%d, end_idx=%d, returning %d items",
                   total, offset, page_size, end_idx, len(paginated_results))
        
        # Calculate next offset
        next_offset = str(end_idx) if end_idx < total else None
        if next_offset:
            logger.info("Next offset available: %s", next_offset)
        else:
            logger.info("No next offset (all results returned)")
        
        # Build response with offset header
        logger.info("POST /artifacts completed successfully: found %d total artifacts, returning %d artifacts",
                   total, len(paginated_results))
        response_obj = jsonify(paginated_results)
        if next_offset:
            response_obj.headers.add("offset", next_offset)
            logger.info("Added 'offset' header to response: %s", next_offset)
        
        return response_obj, 200
        
    except ClientError as e:
        logger.error("DynamoDB scan failed: %s", e, exc_info=True)
        logger.error("DynamoDB error code: %s, error message: %s",
                    e.response.get("Error", {}).get("Code", "Unknown"),
                    e.response.get("Error", {}).get("Message", "Unknown"))
        abort(500, description="The artifact storage encountered an error.")
    except Exception as e:
        logger.error("Unexpected error listing artifacts: %s", e, exc_info=True)
        logger.error("Error type: %s", type(e).__name__)
        abort(500, description="The artifact storage encountered an error.")


@app.get("/artifacts/<artifact_type>/<artifact_id>")
def get_artifact(artifact_type: str, artifact_id: str):
    """
    BASELINE: Return artifact metadata and a URL (not raw bytes)
    """
    logger.info("GET /artifacts/%s/%s called", artifact_type, artifact_id)
    logger.info("Request details: method=GET, path=/artifacts/%s/%s", artifact_type, artifact_id)
    logger.info("Request headers: %s", dict(request.headers))
    logger.info("Query parameters: %s", dict(request.args))
    
    # 403 on missing/invalid auth
    # _require_auth()

    # 400 on bad type/id
    logger.info("Validating artifact_type and artifact_id")
    if not _valid_type(artifact_type):
        logger.warning("Invalid artifact_type: %s (valid types: %s)", artifact_type, VALID_TYPES)
        abort(
            400,
            description="There is missing field(s) in the artifact_type or artifact_id ")
    
    if not _valid_id(artifact_id):
        logger.warning("Invalid artifact_id: %s (must be alphanumeric with -._ allowed)", artifact_id)
        abort(
            400,
            description="There is missing field(s) in the artifact_type or artifact_id ")
    
    logger.info("Validation passed: artifact_type=%s, artifact_id=%s", artifact_type, artifact_id)

    # 404 if artifact not found
    logger.info("Fetching metadata for artifact")
    meta = _fetch_metadata(artifact_type, artifact_id)

    bucket = meta.get("s3_bucket", S3_BUCKET_DEFAULT)
    key = meta["s3_key"]  # e.g. "model/bert.zip"
    filename = meta.get("filename", artifact_id)
    source_url = meta.get("source_url", "")
    
    logger.info("S3 location: bucket=%s, key=%s, filename=%s", bucket, key, filename)
    logger.info("Metadata retrieved: size_bytes=%s, sha256=%s, source_url=%s", 
                meta.get("size_bytes", "unknown"), 
                meta.get("sha256", "unknown")[:16] + "..." if meta.get("sha256") else "unknown",
                source_url)

    logger.info("Generating presigned URL for S3 object")
    presigned_url = _generate_presigned_url(bucket, key)

    body = {
        "metadata": {
            "name": filename,         
            "id": artifact_id,
            "type": artifact_type,
        },
        "data": {
            "url": source_url,  # Original source URL used during ingest
            "download_url": presigned_url,  # Presigned URL for downloading
        },
    }

    logger.info("Response structure: metadata keys=%s, data keys=%s", 
                list(body["metadata"].keys()), list(body["data"].keys()))
    logger.info("Response data: url=%s (length=%d), download_url present=%s (length=%d)",
                source_url, len(source_url) if source_url else 0, 
                "yes" if presigned_url else "no", len(presigned_url) if presigned_url else 0)
    logger.info("GET /artifacts/%s/%s completed successfully: artifact retrieved, filename=%s, source_url=%s, presigned_url_length=%d",
                artifact_type, artifact_id, filename, source_url, len(presigned_url))
    logger.debug("Full response body: %s", body)
    logger.debug("Response body metadata: name=%s, id=%s, type=%s, source_url=%s", filename, artifact_id, artifact_type, source_url)

    return jsonify(body), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)