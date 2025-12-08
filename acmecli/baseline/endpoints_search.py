from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging
import re
import os

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")

MAX_RESULTS = 1000


def search_artifacts_internal(regex_str):
    """
    Shared function: perform regex search on artifacts based on filename,
    type, and source_url. Used by BOTH endpoints.
    """
    # Validate regex
    try:
        pattern = re.compile(regex_str, re.IGNORECASE)
    except re.error:
        abort(400, description="Invalid regex pattern")

    # Scan DynamoDB
    try:
        response = META_TABLE.scan()
        all_items = response.get("Items", [])

        while "LastEvaluatedKey" in response:
            response = META_TABLE.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            all_items.extend(response.get("Items", []))

    except ClientError as e:
        logger.error("DynamoDB scan failed: %s", e)
        abort(500, description="The artifact storage encountered an error.")

    results = []
    for item in all_items:
        searchable = f"{item.get('filename','')} {item.get('artifact_type','')} {item.get('source_url','')}"
        if pattern.search(searchable):
            results.append({
                "name": item.get("filename", ""),
                "id": item.get("id", ""),
                "type": item.get("artifact_type", "")
            })

    # Remove duplicates
    unique = []
    seen = set()
    for r in results:
        if r["id"] not in seen:
            seen.add(r["id"])
            unique.append(r)

    return jsonify(unique), 200


@app.post("/artifact/byRegEx")
def search_by_regex_post():
    payload = request.get_json(silent=True) or {}
    regex_str = payload.get("regex")
    if not regex_str:
        abort(400, description="Missing regex field")

    return search_artifacts_internal(regex_str)



@app.get("/artifacts/search")
def search_artifacts_get():
    regex_str = request.args.get("regex") or request.args.get("q")
    if not regex_str:
        abort(400, description="Missing regex query parameter")

    return search_artifacts_internal(regex_str)
