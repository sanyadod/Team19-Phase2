from flask import Flask, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging
from typing import Dict, List, Any, Optional

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
    if not artifact_id:
        return False
    return all(c.isalnum() or c in "-._" for c in artifact_id)


def _fetch_metadata(artifact_type: str, artifact_id: str) -> Dict[str, Any]:
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


def _fetch_metadata_optional(artifact_type: str, artifact_id: str) -> Optional[Dict[str, Any]]:
    """Fetch metadata without aborting if artifact doesn't exist. Returns None if not found."""
    try:
        resp = META_TABLE.get_item(Key={"id": artifact_id})
    except ClientError as e:
        logger.error("DynamoDB get_item failed: %s", e, exc_info=True)
        return None

    item = resp.get("Item")
    if not item:
        return None

    if item.get("artifact_type") != artifact_type:
        return None

    return item


def _scan_all_artifacts() -> List[Dict[str, Any]]:
    try:
        response = META_TABLE.scan()
        items = response.get("Items", [])

        while "LastEvaluatedKey" in response:
            response = META_TABLE.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            items.extend(response.get("Items", []))

        return items
    except ClientError as e:
        logger.error("DynamoDB scan failed: %s", e, exc_info=True)
        return []


# -----------------------------
# Lineage builder (BASELINE)
# -----------------------------

def _build_lineage_graph(start_artifact: Dict[str, Any], artifact_id: str) -> Dict[str, Any]:
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    seen_ids = set()

    # --- Start node ---
    start_id = str(start_artifact["id"])
    start_name = start_artifact.get("filename", start_id)

    nodes.append({
        "artifact_id": start_id,
        "name": start_name,
        "source": "config_json",
    })
    seen_ids.add(start_id)

    # --- Parents (direct only) ---
    parents = start_artifact.get("parents", [])
    if not isinstance(parents, list):
        abort(
            400,
            description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.",
        )

    for parent_id in parents:
        parent_id_str = str(parent_id)
        if not parent_id_str or parent_id_str == start_id:
            continue

        # Fetch parent metadata to get proper name
        # CRITICAL: Only include parents that exist in the system
        # "Lineage only includes data available from the models currently uploaded to the system"
        parent_artifact = _fetch_metadata_optional("model", parent_id_str)
        if not parent_artifact:
            # Skip parents that don't exist in the system
            continue

        parent_name = parent_artifact.get("filename", parent_id_str)

        if parent_id_str not in seen_ids:
            nodes.append({
                "artifact_id": parent_id_str,
                "name": parent_name,
                "source": "config_json",
            })
            seen_ids.add(parent_id_str)

        edges.append({
            "from_node_artifact_id": parent_id_str,
            "to_node_artifact_id": start_id,
            "relationship": "base_model",
        })

    # --- Children (direct only) ---
    # Lineage is ONLY between models, not datasets or code artifacts
    all_artifacts = _scan_all_artifacts()

    for item in all_artifacts:
        # CRITICAL: Only include models in lineage graph
        if item.get("artifact_type") != "model":
            continue

        item_parents = item.get("parents", [])
        if not isinstance(item_parents, list):
            continue

        item_id_str = str(item.get("id", ""))
        if not item_id_str or item_id_str == start_id:
            continue

        # Check if this item has start_id as a parent
        parent_ids = [str(p) for p in item_parents]
        if start_id in parent_ids:
            if item_id_str not in seen_ids:
                nodes.append({
                    "artifact_id": item_id_str,
                    "name": item.get("filename", item_id_str),
                    "source": "config_json",
                })
                seen_ids.add(item_id_str)

            edges.append({
                "from_node_artifact_id": start_id,
                "to_node_artifact_id": item_id_str,
                "relationship": "base_model",
            })

    return {"nodes": nodes, "edges": edges}


# -----------------------------
# Route
# -----------------------------

@app.route("/artifact/model/<artifact_id>/lineage", methods=["GET"])
def get_lineage(artifact_id: str):
    # ❌ No auth for baseline
    # Route is model-specific per OpenAPI spec: /artifact/model/{id}/lineage

    # 🔴 Critical: reject malformed IDs BEFORE DynamoDB
    if not _valid_id(artifact_id):
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )

    metadata = _fetch_metadata("model", artifact_id)

    if not isinstance(metadata, dict):
        abort(
            400,
            description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.",
        )

    graph = _build_lineage_graph(metadata, artifact_id)
    return jsonify(graph), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5008, debug=True)
