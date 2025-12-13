from flask import Flask, jsonify, request, abort
import boto3
from botocore.exceptions import ClientError
import logging
import re

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")

MAX_RESULTS = 1000
PAGE_SIZE = 100


@app.route("/artifacts", methods=["GET"])
def list_all_artifacts():
    offset_str = request.args.get("offset")
    offset = int(offset_str) if offset_str and offset_str.isdigit() else 0

    try:
        response = META_TABLE.scan()
        items = response.get("Items", [])
        while "LastEvaluatedKey" in response:
            response = META_TABLE.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            items.extend(response.get("Items", []))
    except ClientError:
        abort(500, description="The artifact storage encountered an error.")

    results = []
    for item in items:
        try:
            artifact_id = int(item.get("id"))
        except (TypeError, ValueError):
            artifact_id = item.get("id")

        results.append({
            "name": item.get("filename"),
            "id": artifact_id,
            "type": item.get("artifact_type"),
        })

    if len(results) > MAX_RESULTS:
        abort(413, description="Too many artifacts returned.")

    end_idx = min(offset + PAGE_SIZE, len(results))
    page = results[offset:end_idx]

    resp = jsonify(page)
    if end_idx < len(results):
        resp.headers.add("offset", str(end_idx))

    return resp, 200


@app.route("/artifacts", methods=["POST"])
def read_artifacts():
    """POST /artifacts - Query artifacts by ID or name."""
    try:
        queries = request.get_json(force=True, silent=True)
        if queries is None:
            queries = request.get_json(silent=True)
    except Exception as e:
        logger.error("JSON parse failed: %s", e, exc_info=True)
        abort(400, description="There is missing field(s) in the artifact_query or it is formed improperly, or is invalid.")

    if queries is None:
        abort(400, description="There is missing field(s) in the artifact_query or it is formed improperly, or is invalid.")

    if not isinstance(queries, list) or not queries:
        abort(400, description="There is missing field(s) in the artifact_query or it is formed improperly, or is invalid.")

    try:
        response = META_TABLE.scan()
        items = response.get("Items", [])
        while "LastEvaluatedKey" in response:
            response = META_TABLE.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            items.extend(response.get("Items", []))
    except ClientError as e:
        logger.error("DynamoDB scan failed: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")

    results = []

    for query in queries:
        if not isinstance(query, dict):
            continue
            
        q_id = query.get("id")
        q_name = query.get("name")
        q_types = query.get("types", [])

        matches = []

        for item in items:
            item_id = item.get("id")
            item_name = item.get("filename")
            item_type = item.get("artifact_type")

            # Filter by type if specified
            if q_types and item_type not in q_types:
                continue

            # ID has absolute priority - if ID is provided, match by ID only
            if q_id is not None:
                # Compare as strings to handle both int and string IDs
                if str(item_id) == str(q_id):
                    matches = [item]
                    break
                continue

            # If no ID specified, match by name
            if q_name is None:
                continue
                
            if q_name == "*":
                matches.append(item)
            elif item_name and q_name == item_name:  # Exact match (not regex)
                matches.append(item)

        if matches:
            # Sort matches - handle both int and string IDs gracefully
            def sort_key(x):
                item_id = x.get("id")
                try:
                    return (0, int(item_id))  # Int IDs sort first
                except (TypeError, ValueError):
                    return (1, str(item_id))  # String IDs sort after
            
            matches.sort(key=sort_key)
            chosen = matches[0]
            
            # Convert ID to int if possible (like list_all_artifacts does)
            chosen_id = chosen.get("id")
            try:
                artifact_id = int(chosen_id)
            except (TypeError, ValueError):
                artifact_id = chosen_id
            
            results.append({
                "name": chosen.get("filename"),
                "id": artifact_id,
                "type": chosen.get("artifact_type"),
            })

    return jsonify(results), 200




if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5004, debug=True)
