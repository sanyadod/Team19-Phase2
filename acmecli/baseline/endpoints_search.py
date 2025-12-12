# acmecli/baseline/endpoints_search.py

from flask import Flask, request, jsonify, abort
import logging
import os
import re
from botocore.exceptions import ClientError

# Mock or real DB
USE_MOCK = os.getenv("USE_MOCK_AWS", "false").lower() == "true"

if USE_MOCK:
    from acmecli.baseline.mockdb import scan_all_items
else:
    import boto3
    DYNAMODB = boto3.resource("dynamodb", region_name="us-east-1")
    META_TABLE = DYNAMODB.Table("artifact")

app = Flask(__name__)
logger = logging.getLogger(__name__)


@app.route("/artifact/byRegEx", methods=["POST"])
def search_by_regex():
    """
    POST /artifact/byRegEx
    Body:
    {
        "regex": "bert"
    }
    """


    token = request.headers.get("X-Authorization")
    if not token or not token.strip():
        abort(403, description="Authentication failed due to invalid or missing AuthenticationToken.")


    payload = request.get_json(silent=True)
    if not payload or "regex" not in payload:
        abort(400, description="Missing required field 'regex'.")

    regex = payload.get("regex")


    try:
        pattern = re.compile(regex, re.IGNORECASE)
    except re.error:
        abort(400, description="Invalid regex pattern.")

    try:

        if USE_MOCK:
            items = scan_all_items()
        else:
            response = META_TABLE.scan()
            items = response.get("Items", [])
            while "LastEvaluatedKey" in response:
                response = META_TABLE.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
                items.extend(response.get("Items", []))


        results = []
        for item in items:
            name = item.get("filename", "")
            if pattern.search(name):
                results.append({
                    "id": int(item["id"]) if str(item["id"]).isdigit() else item["id"],
                    "name": name,
                    "type": item.get("artifact_type", "")
                })

        logger.info("Regex search '%s' returned %d results", regex, len(results))
        return jsonify(results), 200

    except ClientError as e:
        logger.error("DynamoDB error during regex search", exc_info=True)
        abort(500, description="The artifact storage encountered an error.")
