from flask import Flask, jsonify, request, abort
import boto3
from botocore.exceptions import ClientError
import logging
from typing import List, Dict, Any

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")

MAX_RESULTS = 1000  # Prevent DoS by returning too many results
PAGE_SIZE = 100     # Results per page


@app.route("/artifacts", methods=["GET"])
def list_all_artifacts():
    # Get pagination offset from query params
    offset_str = request.args.get("offset")
    offset = int(offset_str) if offset_str and offset_str.isdigit() else 0
    
    try:
        # Scan DynamoDB for all artifacts
        response = META_TABLE.scan()
        all_items = response.get("Items", [])
        
        # Handle pagination token if present
        while "LastEvaluatedKey" in response:
            response = META_TABLE.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            all_items.extend(response.get("Items", []))
        
        # Build result list
        results = []
        for item in all_items:
            artifact_type = item.get("artifact_type", "")
            artifact_name = item.get("filename", "")
            artifact_id_raw = item.get("id", "")
            
            # Cast id to int for responses if possible
            try:
                artifact_id = int(artifact_id_raw)
            except (TypeError, ValueError):
                artifact_id = artifact_id_raw
            
            results.append({
                "name": artifact_name,
                "id": artifact_id,
                "type": artifact_type,
            })
        
        # Check if result set is too large
        if len(results) > MAX_RESULTS:
            logger.warning(f"Too many results: {len(results)} exceeds MAX_RESULTS={MAX_RESULTS}")
            abort(413, description="Too many artifacts returned.")
        
        # Apply pagination
        total = len(results)
        end_idx = min(offset + PAGE_SIZE, total) 
        paginated = results[offset:end_idx]
        
        # Calculate next offset
        next_offset = str(end_idx) if end_idx < total else None
        
        # Build response with offset header
        resp = jsonify(paginated)
        if next_offset:
            resp.headers.add("offset", next_offset)
        
        logger.info(
            "GET /artifacts: returned %d/%d artifacts (offset=%d)",
            len(paginated),
            total,
            offset
        )
        return resp, 200
        
    except ClientError as e:
        logger.error("DynamoDB error in /artifacts: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")
        
    except Exception as e:
        logger.error("Unexpected error in /artifacts: %s", e, exc_info=True) 
        abort(500, description="The artifact storage encountered an error.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5004, debug=True)