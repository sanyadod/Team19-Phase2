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
    logger.info("=" * 80)
    logger.info("BUILDING LINEAGE GRAPH")
    logger.info("=" * 80)
    logger.info(f"Requested artifact_id: {artifact_id}")
    logger.info(f"Start artifact metadata keys: {list(start_artifact.keys())}")
    logger.info(f"Start artifact id: {start_artifact.get('id')}")
    logger.info(f"Start artifact type: {start_artifact.get('artifact_type')}")
    logger.info(f"Start artifact filename: {start_artifact.get('filename')}")
    
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    seen_ids = set()

    # --- Start node ---
    start_id = str(start_artifact["id"])
    start_name = start_artifact.get("filename", start_id)
    
    logger.info(f"Start node - id: {start_id}, name: {start_name}")

    nodes.append({
        "artifact_id": start_id,
        "name": start_name,
        "source": "config_json",
    })
    seen_ids.add(start_id)

    # --- Parents (direct only) ---
    parents = start_artifact.get("parents", [])
    logger.info(f"Parents from metadata: {parents} (type: {type(parents)})")
    
    if not isinstance(parents, list):
        logger.error(f"Parents is not a list! Type: {type(parents)}, Value: {parents}")
        abort(
            400,
            description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.",
        )

    logger.info(f"Processing {len(parents)} parent(s)")
    parents_found = 0
    parents_skipped_not_exist = 0
    parents_skipped_self = 0
    
    for parent_id in parents:
        parent_id_str = str(parent_id)
        logger.info(f"  Processing parent: {parent_id_str} (original type: {type(parent_id)})")
        
        if not parent_id_str or parent_id_str == start_id:
            logger.info(f"    SKIPPED: parent is empty or same as start_id")
            parents_skipped_self += 1
            continue

        # Fetch parent metadata to get proper name
        # CRITICAL: Only include parents that exist in the system
        # "Lineage only includes data available from the models currently uploaded to the system"
        logger.info(f"    Fetching parent metadata for: {parent_id_str}")
        parent_artifact = _fetch_metadata_optional("model", parent_id_str)
        if not parent_artifact:
            logger.info(f"    SKIPPED: Parent {parent_id_str} does not exist in system")
            parents_skipped_not_exist += 1
            continue

        parent_name = parent_artifact.get("filename", parent_id_str)
        logger.info(f"    FOUND: Parent {parent_id_str} -> name: {parent_name}")

        if parent_id_str not in seen_ids:
            nodes.append({
                "artifact_id": parent_id_str,
                "name": parent_name,
                "source": "config_json",
            })
            seen_ids.add(parent_id_str)
            logger.info(f"    Added parent node: {parent_id_str}")

        edges.append({
            "from_node_artifact_id": parent_id_str,
            "to_node_artifact_id": start_id,
            "relationship": "base_model",
        })
        logger.info(f"    Added edge: {parent_id_str} -> {start_id}")
        parents_found += 1
    
    logger.info(f"Parents summary: found={parents_found}, skipped_not_exist={parents_skipped_not_exist}, skipped_self={parents_skipped_self}")

    # --- Children (direct only) ---
    # Lineage is ONLY between models, not datasets or code artifacts
    logger.info("Scanning for children...")
    all_artifacts = _scan_all_artifacts()
    logger.info(f"Total artifacts scanned: {len(all_artifacts)}")
    
    children_found = 0
    children_skipped_not_model = 0
    children_skipped_no_parents = 0
    children_skipped_not_child = 0
    
    for item in all_artifacts:
        item_type = item.get("artifact_type")
        item_id_str = str(item.get("id", ""))
        
        # CRITICAL: Only include models in lineage graph
        if item_type != "model":
            children_skipped_not_model += 1
            continue

        item_parents = item.get("parents", [])
        if not isinstance(item_parents, list):
            logger.info(f"  Item {item_id_str}: parents is not a list, skipping")
            children_skipped_no_parents += 1
            continue

        if not item_id_str or item_id_str == start_id:
            continue

        # Check if this item has start_id as a parent
        parent_ids = [str(p) for p in item_parents]
        logger.info(f"  Checking item {item_id_str} (parents: {parent_ids})")
        
        if start_id in parent_ids:
            logger.info(f"    FOUND CHILD: {item_id_str} has {start_id} as parent")
            if item_id_str not in seen_ids:
                nodes.append({
                    "artifact_id": item_id_str,
                    "name": item.get("filename", item_id_str),
                    "source": "config_json",
                })
                seen_ids.add(item_id_str)
                logger.info(f"    Added child node: {item_id_str}")

            edges.append({
                "from_node_artifact_id": start_id,
                "to_node_artifact_id": item_id_str,
                "relationship": "base_model",
            })
            logger.info(f"    Added edge: {start_id} -> {item_id_str}")
            children_found += 1
        else:
            children_skipped_not_child += 1
    
    logger.info(f"Children summary: found={children_found}, skipped_not_model={children_skipped_not_model}, skipped_no_parents={children_skipped_no_parents}, skipped_not_child={children_skipped_not_child}")

    logger.info("=" * 80)
    logger.info("FINAL LINEAGE GRAPH")
    logger.info("=" * 80)
    logger.info(f"Total nodes: {len(nodes)}")
    for i, node in enumerate(nodes):
        logger.info(f"  Node {i+1}: artifact_id={node.get('artifact_id')}, name={node.get('name')}, source={node.get('source')}")
    
    logger.info(f"Total edges: {len(edges)}")
    for i, edge in enumerate(edges):
        logger.info(f"  Edge {i+1}: {edge.get('from_node_artifact_id')} -> {edge.get('to_node_artifact_id')} ({edge.get('relationship')})")
    
    logger.info("=" * 80)
    
    return {"nodes": nodes, "edges": edges}


# -----------------------------
# Route
# -----------------------------

@app.route("/artifact/model/<artifact_id>/lineage", methods=["GET"])
def get_lineage(artifact_id: str):
    # ❌ No auth for baseline
    # Route is model-specific per OpenAPI spec: /artifact/model/{id}/lineage
    
    logger.info("=" * 80)
    logger.info("LINEAGE ENDPOINT CALLED")
    logger.info("=" * 80)
    logger.info(f"Requested artifact_id: {artifact_id}")

    # 🔴 Critical: reject malformed IDs BEFORE DynamoDB
    if not _valid_id(artifact_id):
        logger.error(f"Invalid artifact_id format: {artifact_id}")
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )

    logger.info(f"Fetching metadata for model: {artifact_id}")
    metadata = _fetch_metadata("model", artifact_id)
    logger.info(f"Metadata retrieved. Keys: {list(metadata.keys()) if isinstance(metadata, dict) else 'NOT A DICT'}")

    if not isinstance(metadata, dict):
        logger.error(f"Metadata is not a dict! Type: {type(metadata)}, Value: {metadata}")
        abort(
            400,
            description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.",
        )

    graph = _build_lineage_graph(metadata, artifact_id)
    logger.info("Returning lineage graph")
    return jsonify(graph), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5008, debug=True)
