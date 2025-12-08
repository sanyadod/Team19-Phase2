from flask import Flask, jsonify, request, abort
import boto3
from botocore.exceptions import ClientError
import logging
from typing import List, Dict, Any


app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")

MAX_RESULTS = 1000


@app.get("/artifacts")
def list_all_artifacts():
     """
     GET /artifacts
     return a paginated list of all arfiacts in the registry
     """
     offset_str = request.args.get("offset")
     offset = int(offset_str) if offset_str and offset_str.isdigit() else 0

     try:
          response = META_TABLE.scan()
          all_items = response.get("Items",[])

          while "LastEvaluatedKey" in response:
               response = META_TABLE.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
               all_items.extend(response.get("Items",[]))
          
          results = []


          for item in all_items:
               artifact_type = item.get("artifact_type","")
               artifact_name = item.get("filename","")
               artifact_id_raw = item.get("id","")

               try:
                    artifact_id = int(artifact_id_raw)
               except (TypeError, ValueError):
                    artifact_id = artifact_id_raw

               results.append(
                    {
                         "name" :artifact_name,
                         "id":artifact_id,
                         "type":artifact_type,
                    }
               )
          if len(results) > MAX_RESULTS:
               abort(413, description="Too many artifacts returned.")

          page_size = 100
          total = len(results)
          end_idx = min(offset + page_size + total)
          paginated = results[offset:end_idx]

          next_offset = str(end_idx) if end_idx < total else None

          resp = jsonify(paginated)
          if next_offset:
               resp.headers.add("offset", next_offset)

          logger.info(
               "GET /artifacts: returned %d/%d artifacts (offset=%d)",
               len(paginated),
               total,
               offset
          )
          return resp, 200

     except ClientError as e:
          logger.error("DynamoDB error in /artifacts: %s", e, exc_info=True)
          abort(500, description="The artifact storage encountered an error.")
     except Exception as e:
          logger.error("Unexpected error in /artifacts:%s, e, exc_info=True")
          abort(500, description="The artifact storage encountered an error.")

