from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging
from typing import Dict, List, Any

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")

VALID_TYPES = {"model", "dataset", "code"}


# -----------------------------
# Helpers
# -----------------------------

def _valid_type(artifact_type: str) -> bool:
    return artifact_type in VALID_TYPES


def _valid_id(artifact_id: str) -> bool:
    return bool(artifact_id and artifact_id.strip())


def _fetch_metadata(artifact_type: str, artifact_id: str) -> Dict[str, Any]:
    """Fetch artifact metadata from DynamoDB."""
    try:
        resp = META_TABLE.get_item(Key={"id": artifact_id})
    except ClientError as e:
        logger.error("DynamoDB get_item failed: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")

    item = resp.get("Item")
    if not item:
        abort(404, description="Artifact does not exist.")

    if item.get("artifact_type") != artifact_type:
        abort(404, description="Artifact does not exist.")

    return item


def _fetch_parent_metadata(parent_id: str) -> Dict[str, Any] | None:
    """Best-effort lookup for parent artifact name."""
    try:
        resp = META_TABLE.get_item(Key={"id": parent_id})
        return resp.get("Item")
    except ClientError:
        return None


# -----------------------------
# Lineage builder (BASELINE)
# -----------------------------

def _build_lineage_graph(start_artifact: Dict[str, Any], artifact_id: str) -> Dict[str, Any]:
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    seen_ids = set()

    # Always include the starting artifact
    start_id = str(start_artifact.get("id", artifact_id))
    start_name = (
        start_artifact.get("filename")
        or start_artifact.get("name")
        or start_id
    )

    nodes.append({
        "artifact_id": start_id,
        "name": str(start_name),
        "source": "config_json",
    })
    seen_ids.add(start_id)

    # Parents are optional
    if "parents" not in start_artifact:
        return {"nodes": nodes, "edges": edges}

    parents = start_artifact.get("parents")
    if not isinstance(parents, list):
        abort(
            400,
            description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.",
        )

    for parent_id in parents:
        if not parent_id:
            continue

        parent_id_str = str(parent_id)

        # Best-effort lookup for parent name
        parent_meta = _fetch_parent_metadata(parent_id_str)
        parent_name = (
            parent_meta.get("filename")
            if parent_meta and parent_meta.get("filename")
            else parent_id_str
        )

        if parent_id_str not in seen_ids:
            nodes.append({
                "artifact_id": parent_id_str,
                "name": str(parent_name),
                "source": "config_json",
            })
            seen_ids.add(parent_id_str)

        edges.append({
            "from_node_artifact_id": parent_id_str,
            "to_node_artifact_id": start_id,
            "relationship": "base_model",
        })

    return {"nodes": nodes, "edges": edges}


# -----------------------------
# Route
# -----------------------------

@app.route("/artifact/<artifact_type>/<artifact_id>/lineage", methods=["GET"])
def get_lineage(artifact_type: str, artifact_id: str):
    # ❗ NO AUTH FOR BASELINE (matches license-check)

    if not _valid_type(artifact_type):
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )

    if not _valid_id(artifact_id):
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )

    metadata = _fetch_metadata(artifact_type, artifact_id)

    if not isinstance(metadata, dict):
        abort(
            400,
            description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.",
        )

    graph = _build_lineage_graph(metadata, artifact_id)
    return jsonify(graph), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5008, debug=True)
