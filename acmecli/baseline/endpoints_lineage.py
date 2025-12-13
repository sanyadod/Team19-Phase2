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


def _valid_type(artifact_type: str) -> bool:
    return artifact_type in VALID_TYPES


def _valid_id(artifact_id: str) -> bool:
    """Minimal validation - treat IDs as opaque strings."""
    return bool(artifact_id and artifact_id.strip())


def _fetch_metadata(artifact_type: str, artifact_id: str) -> dict:
    """Fetch artifact metadata from DynamoDB."""
    try:
        resp = META_TABLE.get_item(Key={"id": artifact_id})
    except ClientError as e:
        logger.error(f"DynamoDB get_item failed: {e}", exc_info=True)
        abort(500, description="The artifact storage encountered an error.")

    item = resp.get("Item")
    if not item:
        abort(404, description="Artifact does not exist.")

    if item.get("artifact_type") != artifact_type:
        abort(404, description="Artifact does not exist.")

    return item


def _get_all_artifacts() -> List[Dict[str, Any]]:
    """Get all artifacts from DynamoDB."""
    try:
        response = META_TABLE.scan()
        items = response.get("Items", [])
        
        # Handle pagination
        while "LastEvaluatedKey" in response:
            response = META_TABLE.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            items.extend(response.get("Items", []))
        
        return items
    except ClientError as e:
        logger.error(f"DynamoDB scan failed: {e}", exc_info=True)
        return []


def _normalize_id(id_value: Any) -> str:
    """Normalize ID to string for consistent comparison and storage."""
    if id_value is None:
        return ""
    return str(id_value)


def _find_artifact_by_id(artifact_id: str, all_artifacts: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    """Find an artifact by ID in the all_artifacts list."""
    normalized_id = _normalize_id(artifact_id)
    for item in all_artifacts:
        if _normalize_id(item.get("id")) == normalized_id:
            return item
    return None


def _build_lineage_graph(start_artifact: Dict[str, Any], artifact_id: str, all_artifacts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build a transitive lineage graph starting from the given artifact.
    Recursively includes all ancestors (parents of parents) and descendants (children of children).
    """
    # Track all nodes and edges we've discovered
    nodes: Dict[str, Dict[str, Any]] = {}  # id -> node data
    edges: List[Dict[str, Any]] = []
    visited: Set[str] = set()  # Track visited nodes to avoid cycles
    
    def walk_up(parent_id: str) -> None:
        """Recursively walk up the parent chain."""
        normalized_parent_id = _normalize_id(parent_id)
        if normalized_parent_id in visited or not normalized_parent_id:
            return
        
        visited.add(normalized_parent_id)
        
        # Find the parent artifact
        parent_artifact = _find_artifact_by_id(parent_id, all_artifacts)
        if not parent_artifact:
            return
        
        # Add parent node
        parent_name = parent_artifact.get("filename") or parent_artifact.get("name") or normalized_parent_id
        nodes[normalized_parent_id] = {
            "artifact_id": parent_artifact.get("id"),  # Keep original type
            "name": str(parent_name),
            "source": "config_json"
        }
        
        # Get parent's parents and recurse
        parent_parents = parent_artifact.get("parents", [])
        if isinstance(parent_parents, list):
            for grandparent_id in parent_parents:
                normalized_grandparent_id = _normalize_id(grandparent_id)
                if normalized_grandparent_id:
                    # Add edge: grandparent -> parent
                    edges.append({
                        "from_node_artifact_id": grandparent_id,  # Keep original type
                        "to_node_artifact_id": parent_artifact.get("id"),  # Keep original type
                        "relationship": "parent"
                    })
                    # Recurse
                    walk_up(grandparent_id)
    
    def walk_down(child_id: str) -> None:
        """Recursively walk down the child chain."""
        normalized_child_id = _normalize_id(child_id)
        if normalized_child_id in visited or not normalized_child_id:
            return
        
        visited.add(normalized_child_id)
        
        # Find the child artifact
        child_artifact = _find_artifact_by_id(child_id, all_artifacts)
        if not child_artifact:
            return
        
        # Add child node
        child_name = child_artifact.get("filename") or child_artifact.get("name") or normalized_child_id
        nodes[normalized_child_id] = {
            "artifact_id": child_artifact.get("id"),  # Keep original type
            "name": str(child_name),
            "source": "config_json"
        }
        
        # Find children of this child (grandchildren)
        for item in all_artifacts:
            item_parents = item.get("parents", [])
            if isinstance(item_parents, list):
                if any(_normalize_id(p) == normalized_child_id for p in item_parents):
                    grandchild_id = item.get("id")
                    normalized_grandchild_id = _normalize_id(grandchild_id)
                    if normalized_grandchild_id:
                        # Add edge: child -> grandchild
                        edges.append({
                            "from_node_artifact_id": child_artifact.get("id"),  # Keep original type
                            "to_node_artifact_id": grandchild_id,  # Keep original type
                            "relationship": "parent"
                        })
                        # Recurse
                        walk_down(grandchild_id)
    
    # Start with the artifact itself
    normalized_start_id = _normalize_id(artifact_id)
    visited.add(normalized_start_id)
    
    start_name = start_artifact.get("filename") or start_artifact.get("name") or normalized_start_id
    nodes[normalized_start_id] = {
        "artifact_id": start_artifact.get("id"),  # Keep original type
        "name": str(start_name),
        "source": "config_json"
    }
    
    # Walk up: get all ancestors
    parents = start_artifact.get("parents", [])
    if isinstance(parents, list):
        for parent_id in parents:
            normalized_parent_id = _normalize_id(parent_id)
            if normalized_parent_id:
                # Add edge: parent -> start
                edges.append({
                    "from_node_artifact_id": parent_id,  # Keep original type
                    "to_node_artifact_id": start_artifact.get("id"),  # Keep original type
                    "relationship": "parent"
                })
                # Recurse up
                walk_up(parent_id)
    
    # Walk down: get all descendants
    for item in all_artifacts:
        item_parents = item.get("parents", [])
        if isinstance(item_parents, list):
            if any(_normalize_id(p) == normalized_start_id for p in item_parents):
                child_id = item.get("id")
                normalized_child_id = _normalize_id(child_id)
                if normalized_child_id:
                    # Add edge: start -> child
                    edges.append({
                        "from_node_artifact_id": start_artifact.get("id"),  # Keep original type
                        "to_node_artifact_id": child_id,  # Keep original type
                        "relationship": "parent"
                    })
                    # Recurse down
                    walk_down(child_id)
    
    return {
        "nodes": list(nodes.values()),
        "edges": edges
    }


@app.route("/artifact/<artifact_type>/<artifact_id>/lineage", methods=["GET"])
def get_lineage(artifact_type: str, artifact_id: str):
    """
    GET /artifact/<artifact_type>/<artifact_id>/lineage
    Get the lineage graph for an artifact, including its parents and children.
    """
    # Require authentication
    _require_auth()
    
    # Validate artifact type
    if not _valid_type(artifact_type):
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )
    
    # Validate artifact ID (minimal validation - treat IDs as opaque strings)
    if not artifact_id or not artifact_id.strip():
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )

    # Verify artifact exists
    metadata = _fetch_metadata(artifact_type, artifact_id)
    
    # Check if metadata is malformed (missing required fields for lineage)
    # If artifact exists but has no valid structure, return 400
    if not isinstance(metadata, dict):
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    # Get all artifacts to build the graph
    all_artifacts = _get_all_artifacts()
    
    # Build lineage graph
    try:
        graph = _build_lineage_graph(metadata, artifact_id, all_artifacts)
    except Exception as e:
        logger.error(f"Error building lineage graph: {e}", exc_info=True)
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    # Ensure graph has the correct structure
    if not isinstance(graph, dict) or "nodes" not in graph or "edges" not in graph:
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    return jsonify(graph), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5008, debug=True)

