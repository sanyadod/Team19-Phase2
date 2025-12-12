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
    # DO NOT cast artifact_id — must stay string
    artifact_id_key = artifact_id

    # Check existence (EXACT SAME KEY AS DOWNLOAD)
    try:
        response = META_TABLE.get_item(
            Key={"id": artifact_id_key}
        )
    except ClientError as e:
        logger.error(f"DynamoDB get_item failed: {e}", exc_info=True)
        abort(500, description="The artifact storage encountered an error.")
        return

    if "Item" not in response:
        abort(404, description="Artifact does not exist.")
        return

    # Delete artifact
    try:
        META_TABLE.delete_item(
            Key={"id": artifact_id_key}
        )
    except ClientError as e:
        logger.error(f"DynamoDB delete_item failed: {e}", exc_info=True)
        abort(500, description="The artifact storage encountered an error.")
        return

    return "", 200
