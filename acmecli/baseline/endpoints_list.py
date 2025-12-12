from flask import Flask, jsonify, request, abort
import boto3
from botocore.exceptions import ClientError
import logging

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
    queries = request.get_json(silent=True)
    if not isinstance(queries, list) or not queries:
        abort(400, description="Invalid artifact query")

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

    for query in queries:
        q_id = query.get("id")
        q_name = query.get("name")
        q_types = query.get("types", [])

        matched = []

        for item in items:
            if q_types and item.get("artifact_type") not in q_types:
                continue

            # ID match has highest priority
            if q_id is not None:
                if str(item.get("id")) == str(q_id):
                    matched = [{
                        "name": item.get("filename"),
                        "id": item.get("id"),
                        "type": item.get("artifact_type"),
                    }]
                break

            # Name match
            if q_name == "*":
                matched.append({
                    "name": item.get("filename"),
                    "id": item.get("id"),
                    "type": item.get("artifact_type"),
                })
            elif q_name is not None and item.get("filename") == q_name:
                matched = [{
                    "name": item.get("filename"),
                    "id": item.get("id"),
                    "type": item.get("artifact_type"),
                }]
                break

        results.extend(matched)

    return jsonify(results), 200


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5004, debug=True)
