from flask import Flask, abort
import boto3
from botocore.exceptions import ClientError
import logging

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")


@app.route("/artifacts/<artifact_type>/<artifact_id>", methods=["DELETE"])
def delete_artifact(artifact_type, artifact_id):



    try:
        resp = META_TABLE.get_item(
            Key={"id": artifact_id}
        )
    except ClientError as e:
        logger.error("DynamoDB get_item failed", exc_info=True)
        abort(500, description="The artifact storage encountered an error.")

    item = resp.get("Item")
    if not item:
        abort(404, description="Artifact does not exist.")

    # Type must match (same logic as download)
    if item.get("artifact_type") != artifact_type:
        abort(404, description="Artifact does not exist.")

    try:
        META_TABLE.delete_item(
            Key={"id": artifact_id}
        )
    except ClientError as e:
        logger.error("DynamoDB delete_item failed", exc_info=True)
        abort(500, description="The artifact storage encountered an error.")

    return "", 200
