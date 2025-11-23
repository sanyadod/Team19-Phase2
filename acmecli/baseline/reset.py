from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging

app = Flask(__name__)

S3_BUCKET = "ece-registry"
AWS_REGION = "us-east-1"
DYNAMODB_TABLE = "artifact"  

s3_client = boto3.client("s3", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

logger = logging.getLogger(__name__)

def _require_auth_header() -> str:
    token = request.headers.get("X-Authorization")
    if not token or not token.strip():
        abort(403, description="Authentication failed due to invalid or missing AuthenticationToken.")
    return token


def _check_reset_permission(token: str) -> None:
    if token != "admin":
        abort(401, description="You do not have permission to reset the registry.")

def clear_s3_bucket(bucket_name: str) -> None:
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name)

    for page in pages:
        objects = page.get("Contents", [])
        if not objects:
            continue

        delete_keys = [{"Key": obj["Key"]} for obj in objects]

        for i in range(0, len(delete_keys), 1000):
            batch = delete_keys[i : i + 1000]
            s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={"Objects": batch, "Quiet": True},
            )

def clear_dynamodb_table(table_name: str) -> None:
    table = dynamodb.Table(table_name)
    scan_kwargs = {}

    while True:
        resp = table.scan(**scan_kwargs)
        items = resp.get("Items", [])
        if not items:
            break

        with table.batch_writer() as batch:
            for item in items:
                # assumes partition key is "id"
                batch.delete_item(Key={"id": item["id"]})

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

@app.route("/reset", methods=["DELETE"])
def reset_registry():
    # 403 if header missing/empty
    token = _require_auth_header()

    # 401 if token present but not allowed to reset
    #_check_reset_permission(token)

    # Clear S3
    try:
        clear_s3_bucket(S3_BUCKET)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "NoSuchBucket":
            logger.warning(f"Bucket {S3_BUCKET} not found; treating as empty.")
        else:
            logger.error(f"S3 deletion failed: {e}")
            abort(500, description="Failed to reset registry: S3 operation failed")
    except Exception as e:
        logger.error(f"Unexpected error during S3 reset: {e}")
        abort(500, description="Failed to reset registry: unexpected error")

    # Clear DynamoDB
    try:
        clear_dynamodb_table(DYNAMODB_TABLE)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "ResourceNotFoundException":
            logger.warning(f"DynamoDB table {DYNAMODB_TABLE} not found; treating as empty.")
        else:
            logger.error(f"DynamoDB deletion failed: {e}")
            abort(500, description="Failed to reset registry: DynamoDB operation failed")
    except Exception as e:
        logger.error(f"Unexpected error during DynamoDB reset: {e}")
        abort(500, description="Failed to reset registry: unexpected error")

    return jsonify({"status": "registry is reset"}), 200


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5001, debug=True)
