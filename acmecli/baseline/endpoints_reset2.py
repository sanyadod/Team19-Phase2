from flask import Flask, jsonify, request, abort
import boto3
from botocore.exceptions import ClientError
import logging
from typing import List, Dict, Any

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
S3_BUCKET = "ece-registry"
DYNAMODB_TABLE = "artifact"

s3_client = boto3.client("s3", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE)

def _clear_s3_bucket(bucket_name: str) -> None:
     """delete all objects from s3 bucket"""
     paginator = s3_client.get_paginator("list_objects_v2")
     pages = paginator.paginate(Bucket=bucket_name)

     for page in pages:
          objects = page.get("Contents",[])
          if not objects:
               continue
          delete_keys = [{"Key": obj["Key"]} for obj in objects]

          for i in range(0, len(delete_keys), 1000):
               batch = delete_keys[i : i + 1000]
               s3_client.delete_objects(
                    Bucket = bucket_name,
                    Delete = {"Objects": batch, "Quiet": True},

               )

def _clear_dynamodb_table(table) -> None:
     """delete all rows from dynamodb artifact table"""
     try:
          scan_kwargs = {}
          while True:
               resp = table.scan(**scan_kwargs)
               items = resp.get("Items", [])
               if not items:
                    break

               with table.batch_writer() as batch:
                    for item in items:
                         batch.delete_item(Key={"id": item["id"]})

               if "lastEvaluatedKey" not in resp:
                    break

               scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
     
     except ClientError as e:
          logger.error("DynamoDB deletion failed: %s", e, exc_info=True)
          abort(500, description="The artifact storage encountered an error.")

@app.post("/reset")
def reset_registry():
     """POST /RESET
     RESET THE ARTIFACT REGISTRY BY DELETING ALL DYNAMODB ENTRIES AND CLEARING THE S3 STORAGE"""

     try:
          _clear_s3_bucket(S3_BUCKET)
     except Exception as e:
          logger.error("Failed clearning S3: %s", e, exc_info=True)
          abort(500, description="Failed to reset registry (s3 failure).")

     try:
          _clear_dynamodb_table(table)
     except Exception as e:
          logger.error("Failed clearning DynamoDB: %s", e, exc_info=True)
          abort(500, description="Failed to reset registry (DynamoDB failure).")

     logger.info("Registry reset successfully.")
     return jsonify({"status":"ok", "message": "Registry reset completed."}), 200

     