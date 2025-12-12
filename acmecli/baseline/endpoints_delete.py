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
    """
    DELETE /artifacts/<artifact_type>/<artifact_id>

    Deletes an artifact if it exists.
    Returns:
      200 if deleted
      404 if not found or invalid ID
    """

    # Validate artifact_id is an integer
    try:
        artifact_id_key = int(artifact_id)
    except ValueError:
        abort(404, description="Artifact not found")
        return

    # Check if artifact exists
    try:
        response = META_TABLE.get_item(
            Key={
                "artifact_type": artifact_type,
                "id": artifact_id_key
            }
        )
    except ClientError as e:
        logger.error(f"DynamoDB get_item failed: {e}", exc_info=True)
        abort(500, description="Artifact storage error")
        return

    if "Item" not in response:
        abort(404, description="Artifact not found")
        return

    # Delete artifact
    try:
        META_TABLE.delete_item(
            Key={
                "artifact_type": artifact_type,
                "id": artifact_id_key
            }
        )
    except ClientError as e:
        logger.error(f"DynamoDB delete_item failed: {e}", exc_info=True)
        abort(500, description="Artifact storage error")
        return

    return "", 200
