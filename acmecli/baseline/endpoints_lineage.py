from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging
from typing import Dict, List, Any, Set

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")

VALID_TYPES = {"model", "dataset", "code"}


def _require_auth() -> str:
    """Check for X-Authorization header."""
    token = request.headers.get("X-Authorization")
    if not token or not token.strip():
        abort(403, description="Authentication failed due to invalid or missing AuthenticationToken.")
    return token


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


def _find_artifact_by_id(artifact_id: int, all_artifacts: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    """Find an artifact by ID."""
    for item in all_artifacts:
        try:
            if int(item.get("id")) == artifact_id:
                return item
        except Exception:
            continue
    return None


# -----------------------------
# Lineage builder (BASELINE)
# -----------------------------

def _build_lineage_graph(start_artifact: Dict[str, Any], artifact_id: str) -> Dict[str, Any]:
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    seen_ids: Set[int] = set()
    visited: Set[int] = set()
    all_artifacts = _scan_all_artifacts()

    # Helper to add node
    def add_node(node_id: int, artifact: Dict[str, Any] | None = None) -> int:
        if node_id in seen_ids:
            return node_id
        
        if artifact:
            node_name = artifact.get("filename", str(node_id))
        else:
            node_name = str(node_id)
        
        nodes.append({
            "artifact_id": node_id,
            "name": node_name,
            "source": "config_json",
        })
        seen_ids.add(node_id)
        return node_id

    # Helper to walk up (ancestors) - TRANSITIVE CLOSURE
    def walk_up(parent_id: int) -> None:
        if parent_id in visited:
            return
        visited.add(parent_id)
        
        parent_artifact = _find_artifact_by_id(parent_id, all_artifacts)
        add_node(parent_id, parent_artifact)
        
        if parent_artifact:
            parent_parents = parent_artifact.get("parents", [])
            if isinstance(parent_parents, list):
                for grandparent_id in parent_parents:
                    try:
                        grandparent_id_int = int(grandparent_id)
                    except Exception:
                        continue
                    
                    grandparent_artifact = _find_artifact_by_id(grandparent_id_int, all_artifacts)
                    add_node(grandparent_id_int, grandparent_artifact)
                    
                    edges.append({
                        "from_node_artifact_id": grandparent_id_int,
                        "to_node_artifact_id": parent_id,
                        "relationship": "parent"
                    })
                    walk_up(grandparent_id_int)

    # Helper to walk down (descendants) - TRANSITIVE CLOSURE
    def walk_down(child_id: int) -> None:
        if child_id in visited:
            return
        visited.add(child_id)
        
        child_artifact = _find_artifact_by_id(child_id, all_artifacts)
        add_node(child_id, child_artifact)
        
        for item in all_artifacts:
            item_parents = item.get("parents", [])
            if not isinstance(item_parents, list):
                continue
            
            try:
                item_id_int = int(item["id"])
            except Exception:
                continue
            
            if child_id in [int(p) for p in item_parents if str(p).isdigit()]:
                add_node(item_id_int, item)
                
                edges.append({
                    "from_node_artifact_id": child_id,
                    "to_node_artifact_id": item_id_int,
                    "relationship": "parent"
                })
                walk_down(item_id_int)

    # --- Start node ---
    start_id = int(start_artifact["id"])
    start_name = start_artifact.get("filename", str(start_id))

    nodes.append({
        "artifact_id": start_id,
        "name": start_name,
        "source": "config_json",
    })
    seen_ids.add(start_id)
    visited.add(start_id)

    # --- Parents (direct + transitive) ---
    parents = start_artifact.get("parents", [])
    if not isinstance(parents, list):
        abort(
            400,
            description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.",
        )

    for parent_id in parents:
        try:
            parent_id_int = int(parent_id)
        except Exception:
            continue

        parent_artifact = _find_artifact_by_id(parent_id_int, all_artifacts)
        add_node(parent_id_int, parent_artifact)

        edges.append({
            "from_node_artifact_id": parent_id_int,
            "to_node_artifact_id": start_id,
            "relationship": "parent"
        })
        
        # Recurse up to get all ancestors
        walk_up(parent_id_int)

    # --- Children (direct + transitive) ---
    for item in all_artifacts:
        item_parents = item.get("parents", [])
        if not isinstance(item_parents, list):
            continue

        try:
            item_id_int = int(item["id"])
        except Exception:
            continue

        if start_id in [int(p) for p in item_parents if str(p).isdigit()]:
            add_node(item_id_int, item)

            edges.append({
                "from_node_artifact_id": start_id,
                "to_node_artifact_id": item_id_int,
                "relationship": "parent"
            })
            
            # Recurse down to get all descendants
            walk_down(item_id_int)

    return {"nodes": nodes, "edges": edges}


# -----------------------------
# Route
# -----------------------------

@app.route("/artifact/<artifact_type>/<artifact_id>/lineage", methods=["GET"])
def get_lineage(artifact_type: str, artifact_id: str):
    # Require authentication
    _require_auth()

    if not _valid_type(artifact_type):
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )

    # 🔴 Critical: reject malformed IDs BEFORE DynamoDB
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
