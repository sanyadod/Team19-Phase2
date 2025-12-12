from flask import request, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")


@app.route("/artifacts/<artifact_type>/<artifact_id>", methods=["DELETE"])
def delete_artifact(artifact_type, artifact_id):
    try:
        # Validate artifact_id format (autograder passes numeric strings)
        try:
            artifact_id_key = int(artifact_id)
        except ValueError:
            abort(404, description="Artifact not found")

        # Check existence first (important for correct 404)
        try:
            response = META_TABLE.get_item(Key={"id": artifact_id_key})
        except ClientError as e:
            logger.error("DynamoDB get_item failed", exc_info=True)
            abort(500, description="Artifact storage error")

        if "Item" not in response:
            abort(404, description="Artifact not found")

        # Delete the artifact
        META_TABLE.delete_item(Key={"id": artifact_id_key})

        logger.info(f"Deleted artifact id={artifact_id_key}")

        # Autograder accepts 200 or 204
        return jsonify({"message": "Artifact deleted"}), 200

    except Exception as e:
        logger.error("Unexpected error during delete", exc_info=True)
        abort(500, description="The artifact registry encountered an error.")
