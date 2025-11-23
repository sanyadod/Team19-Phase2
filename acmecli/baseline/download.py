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
    try:
        resp = META_TABLE.get_item(Key={"id": artifact_id})
    except ClientError as e:
        logger.error("DynamoDB get_item failed: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")

    item = resp.get("Item")
    if not item:
        abort(404, description="Artifact does not exist.")

    if item.get("artifact_type") != artifact_type:
        # type/id mismatch
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
        return url
    except ClientError as e:
        logger.error("Error generating presigned URL: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")


@app.post("/artifacts")
def list_artifacts():
    """
    POST /artifacts
    BASELINE: Get artifacts from the registry based on query.
    Accepts an array of ArtifactQuery objects and returns matching artifacts.
    """
    # 403 on missing/invalid auth
    _require_auth()
    
    # Parse request body
    try:
        queries: List[Dict[str, Any]] = request.get_json(force=True)
    except Exception:
        abort(400, description="There is missing field(s) in the artifact_query or it is formed improperly, or is invalid.")
    
    if not isinstance(queries, list) or not queries:
        abort(400, description="There is missing field(s) in the artifact_query or it is formed improperly, or is invalid.")
    
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
            response = META_TABLE.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            all_items.extend(response.get("Items", []))
        
        # Process each query
        for query in queries:
            if not isinstance(query, dict) or "name" not in query:
                continue
            
            query_name = str(query.get("name", "")).strip()
            query_types = query.get("types", [])
            
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
        
        return response_obj, 200
        
    except ClientError as e:
        logger.error("DynamoDB scan failed: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")
    except Exception as e:
        logger.error("Error listing artifacts: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")


@app.get("/artifacts/<artifact_type>/<artifact_id>")
def get_artifact(artifact_type: str, artifact_id: str):
    """
    BASELINE: Return artifact metadata and a URL (not raw bytes)
    """
    # 403 on missing/invalid auth
    # _require_auth()

    # 400 on bad type/id
    if not _valid_type(artifact_type) or not _valid_id(artifact_id):
        abort(
            400,
            description="There is missing field(s) in the artifact_type or artifact_id ")

    # 404 if artifact not found
    meta = _fetch_metadata(artifact_type, artifact_id)

    bucket = meta.get("s3_bucket", S3_BUCKET_DEFAULT)
    key = meta["s3_key"]  # e.g. "model/bert.zip"
    filename = meta.get("filename", artifact_id)

    url = _generate_presigned_url(bucket, key)

    body = {
        "metadata": {
            "name": filename,         
            "id": artifact_id,
            "type": artifact_type,
        },
        "data": {
            "url": url,               
        },
    }

    return jsonify(body), 200

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5001, debug=True)