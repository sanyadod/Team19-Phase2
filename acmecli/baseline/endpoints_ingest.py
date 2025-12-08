from flask import Flask, request, abort
import logging

import acmecli.baseline.upload as upload_module

app = Flask(__name__)
logger = logging.getLogger(__name__)

VALID_TYPES = {"moel", "dataset", "code"}
DEFAULT_TYPE = "model"

app.post("/artifacts/ingest")
def ingest_artifact():
     """
     post /artifacts/ingest
     ingest an artifact using the existing upload logic
     """

     payload = request.get_json(silent=True)

     if payload is None:
          abort(
               400,
               description = "There is missing field(s)in the artifact data or it is formed improperly.",
          )

     artifact_type = payload.get("type", DEFAULT_TYPE)

     if artifact_type not in VALID_TYPES:
          abort(
               400, 
               description = "Invalid artifact type. Must be one of :model, dataset, code.",

          )

     logger.info("ingesting artifact using upload.create_artifact(type=%s)", artifact_type)

     return upload_module.create_artifact(artifact_type)

