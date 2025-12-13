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
    """
    Artifact Read Endpoint
    ----------------------
    Accepts a list of queries and returns matching artifacts.

    Query fields:
      - id (optional)
      - name (optional, can be "*")
      - types (optional list)

    Returns:
      - Flat list of artifacts: {id, name, type}
    """

    # ---------- Step 0: Validate request ----------
    queries = request.get_json(silent=True)

    if not isinstance(queries, list) or len(queries) == 0:
        abort(400, description="Invalid artifact query")

    # ---------- Step 1: Load ALL artifacts ----------
    try:
        response = META_TABLE.scan()
        all_items = response.get("Items", [])

        while "LastEvaluatedKey" in response:
            response = META_TABLE.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            all_items.extend(response.get("Items", []))

    except ClientError:
        abort(500, description="The artifact storage encountered an error.")

    results = []

    # ---------- Step 2: Process queries independently ----------
    for query in queries:
        q_id = query.get("id")
        q_name = query.get("name")
        q_types = query.get("types")

        # Start with all artifacts
        candidates = all_items

        # ---------- Step 3: Apply type filter ----------
        if isinstance(q_types, list) and len(q_types) > 0:
            candidates = [
                a for a in candidates
                if a.get("artifact_type") in q_types
            ]

        # ---------- Step 4: ID lookup (highest priority) ----------
        if q_id is not None:
            match = None
            for a in candidates:
                if str(a.get("id")) == str(q_id):
                    match = a
                    break

            if match:
                results.append({
                    "id": match.get("id"),
                    "name": match.get("filename"),
                    "type": match.get("artifact_type")
                })

            continue  # move to next query

        # ---------- Step 5: Name lookup ----------
        if q_name is not None:

            # ----- Wildcard -----
            if q_name == "*":
                for a in candidates:
                    results.append({
                        "id": a.get("id"),
                        "name": a.get("filename"),
                        "type": a.get("artifact_type")
                    })
                continue

            # ----- Exact name match -----
            name_matches = [
                a for a in candidates
                if a.get("filename") == q_name
            ]

            if not name_matches:
                continue

            # Deterministic selection: LOWEST numeric ID
            def id_as_int(x):
                try:
                    return int(x.get("id"))
                except Exception:
                    return float("inf")

            chosen = min(name_matches, key=id_as_int)

            results.append({
                "id": chosen.get("id"),
                "name": chosen.get("filename"),
                "type": chosen.get("artifact_type")
            })

    # ---------- Step 6: Return results ----------
    return jsonify(results), 200


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5004, debug=True)
