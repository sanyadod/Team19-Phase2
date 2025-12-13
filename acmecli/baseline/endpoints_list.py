from flask import Flask, jsonify, request, abort
import boto3
from botocore.exceptions import ClientError
import logging

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")


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


    queries = request.get_json(silent=True)

    if not isinstance(queries, list) or len(queries) == 0:
        abort(400, description="Invalid artifact query")


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


    for query in queries:
        q_id = query.get("id")
        q_name = query.get("name")
        q_types = query.get("types")

        # Start with all artifacts
        candidates = all_items


        if isinstance(q_types, list) and len(q_types) > 0:
            candidates = [
                a for a in candidates
                if a.get("artifact_type") in q_types
            ]


        if q_id is not None:

            id_matches = []
            for a in candidates:
                if str(a.get("id")) == str(q_id):
                    id_matches.append(a)

            if id_matches:
                def id_as_int(x):
                    try:
                        return int(x.get("id"))
                    except Exception:
                        return float("inf")
                
                match = min(id_matches, key=id_as_int)
                results.append({
                    "id": match.get("id"),
                    "name": match.get("filename"),
                    "type": match.get("artifact_type")
                })

            continue  # move to next query


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


            name_matches = []
            for a in candidates:
                artifact_filename = a.get("filename")
                # Exact string match - no normalization, no trimming
                if artifact_filename is not None and str(artifact_filename) == str(q_name):
                    name_matches.append(a)

            if not name_matches:
                continue


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


    return jsonify(results), 200


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5004, debug=True)
