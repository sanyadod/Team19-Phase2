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


def _normalize_id_for_comparison(id_value: Any) -> str:
    """Normalize ID to string for comparison purposes only. Treat IDs as opaque."""
    if id_value is None:
        return ""
    return str(id_value)


def _find_artifact_by_id(artifact_id: Any, all_artifacts: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    """Find an artifact by ID in the all_artifacts list. Treat IDs as opaque."""
    normalized_id = _normalize_id_for_comparison(artifact_id)
    for item in all_artifacts:
        if _normalize_id_for_comparison(item.get("id")) == normalized_id:
            return item
    return None


def _build_lineage_graph(start_artifact: Dict[str, Any], artifact_id: Any, all_artifacts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build a transitive lineage graph starting from the given artifact.
    Recursively includes all ancestors (parents of parents) and descendants (children of children).
    Treats IDs as opaque values (like upload/download endpoints do).
    
    Lineage semantics:
    - If artifact A has parents = [B, C], then B and C are parents of A
    - Edges represent: parent -> child (from_node -> to_node)
    - So edges are: B -> A, C -> A
    """
    # Track all nodes and edges we've discovered
    # Use normalized string IDs as keys for comparison, but preserve original ID types
    nodes: Dict[str, Dict[str, Any]] = {}  # normalized_id -> node data
    edges: List[Dict[str, Any]] = []
    visited: Set[str] = set()  # Track visited nodes to avoid cycles
    
    def add_node(node_id: Any, artifact: Dict[str, Any] | None = None) -> None:
        """Helper to add a node to the graph. Treats ID as opaque."""
        normalized_id = _normalize_id_for_comparison(node_id)
        if normalized_id in nodes:
            return
        
        # Use original ID type from artifact if available, otherwise use node_id as-is
        if artifact:
            artifact_id_value = artifact.get("id", node_id)
            node_name = artifact.get("filename") or artifact.get("name") or _normalize_id_for_comparison(artifact_id_value)
        else:
            artifact_id_value = node_id
            node_name = _normalize_id_for_comparison(node_id)
        
        nodes[normalized_id] = {
            "artifact_id": artifact_id_value,  # Keep original type (opaque)
            "name": str(node_name),
            "source": "config_json"
        }
    
    def walk_up(parent_id: Any) -> None:
        """Recursively walk up the parent chain."""
        normalized_parent_id = _normalize_id_for_comparison(parent_id)
        if normalized_parent_id in visited or not normalized_parent_id:
            return
        
        visited.add(normalized_parent_id)
        
        # Find the parent artifact
        parent_artifact = _find_artifact_by_id(parent_id, all_artifacts)
        
        # Add parent node (even if not found in DB - include all referenced artifacts)
        add_node(parent_id, parent_artifact)
        
        # Get parent's parents and recurse
        if parent_artifact:
            parent_parents = parent_artifact.get("parents", [])
            if isinstance(parent_parents, list):
                for grandparent_id in parent_parents:
                    normalized_grandparent_id = _normalize_id_for_comparison(grandparent_id)
                    if normalized_grandparent_id:
                        # Add edge: grandparent -> parent
                        # Use original ID types from artifacts
                        grandparent_artifact = _find_artifact_by_id(grandparent_id, all_artifacts)
                        grandparent_id_value = grandparent_artifact.get("id", grandparent_id) if grandparent_artifact else grandparent_id
                        parent_id_value = parent_artifact.get("id", parent_id) if parent_artifact else parent_id
                        
                        edges.append({
                            "from_node_artifact_id": grandparent_id_value,  # Keep original type
                            "to_node_artifact_id": parent_id_value,  # Keep original type
                            "relationship": "parent"
                        })
                        # Recurse up
                        walk_up(grandparent_id)
    
    def walk_down(child_id: Any) -> None:
        """Recursively walk down the child chain."""
        normalized_child_id = _normalize_id_for_comparison(child_id)
        if normalized_child_id in visited or not normalized_child_id:
            return
        
        visited.add(normalized_child_id)
        
        # Find the child artifact
        child_artifact = _find_artifact_by_id(child_id, all_artifacts)
        
        # Add child node (even if not found in DB - include all referenced artifacts)
        add_node(child_id, child_artifact)
        
        # Find children of this child (grandchildren)
        for item in all_artifacts:
            item_parents = item.get("parents", [])
            if isinstance(item_parents, list):
                # Check if this item has child_id as a parent
                item_id = item.get("id")
                normalized_item_id = _normalize_id_for_comparison(item_id)
                normalized_child_id_str = _normalize_id_for_comparison(child_id)
                
                for p in item_parents:
                    if _normalize_id_for_comparison(p) == normalized_child_id_str:
                        # Add edge: child -> grandchild
                        child_id_value = child_artifact.get("id", child_id) if child_artifact else child_id
                        item_id_value = item.get("id", item_id)
                        
                        edges.append({
                            "from_node_artifact_id": child_id_value,  # Keep original type
                            "to_node_artifact_id": item_id_value,  # Keep original type
                            "relationship": "parent"
                        })
                        # Recurse down
                        walk_down(item_id)
                        break
    
    # Start with the artifact itself
    normalized_start_id = _normalize_id_for_comparison(artifact_id)
    visited.add(normalized_start_id)
    add_node(artifact_id, start_artifact)
    
    # Get the actual ID value from start_artifact (keep original type)
    start_artifact_id = start_artifact.get("id", artifact_id)
    
    # Walk up: get all ancestors
    parents = start_artifact.get("parents", [])
    if isinstance(parents, list):
        for parent_id in parents:
            normalized_parent_id = _normalize_id_for_comparison(parent_id)
            if normalized_parent_id:
                # Find parent to get its original ID type
                parent_artifact = _find_artifact_by_id(parent_id, all_artifacts)
                parent_id_value = parent_artifact.get("id", parent_id) if parent_artifact else parent_id
                
                # Add edge: parent -> start
                edges.append({
                    "from_node_artifact_id": parent_id_value,  # Keep original type
                    "to_node_artifact_id": start_artifact_id,  # Keep original type
                    "relationship": "parent"
                })
                # Recurse up
                walk_up(parent_id)
    
    # Walk down: get all descendants
    for item in all_artifacts:
        item_parents = item.get("parents", [])
        if isinstance(item_parents, list):
            # Check if this item has artifact_id as a parent
            item_id = item.get("id")
            normalized_item_id = _normalize_id_for_comparison(item_id)
            
            for p in item_parents:
                if _normalize_id_for_comparison(p) == normalized_start_id:
                    # Add edge: start -> child
                    child_id_value = item.get("id", item_id)
                    
                    edges.append({
                        "from_node_artifact_id": start_artifact_id,  # Keep original type
                        "to_node_artifact_id": child_id_value,  # Keep original type
                        "relationship": "parent"
                    })
                    # Recurse down
                    walk_down(item_id)
                    break
    
    # Ensure graph consistency: all edges reference existing nodes
    node_ids = {_normalize_id_for_comparison(n["artifact_id"]) for n in nodes.values()}
    valid_edges = []
    for edge in edges:
        from_id = _normalize_id_for_comparison(edge["from_node_artifact_id"])
        to_id = _normalize_id_for_comparison(edge["to_node_artifact_id"])
        if from_id in node_ids and to_id in node_ids:
            valid_edges.append(edge)
        else:
            logger.warning(f"Skipping edge with missing node: {edge}")
    
    return {
        "nodes": list(nodes.values()),
        "edges": valid_edges
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
    
    # Validate artifact ID (minimal validation - treat IDs as opaque)
    if not artifact_id or not artifact_id.strip():
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )

    # Verify artifact exists (treat ID as opaque, like download.py does)
    metadata = _fetch_metadata(artifact_type, artifact_id)
    
    # Check if metadata is malformed (missing required fields for lineage)
    # If artifact exists but has no valid structure, return 400
    if not isinstance(metadata, dict):
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    # Get all artifacts to build the graph (treat IDs as opaque, no normalization)
    all_artifacts = _get_all_artifacts()
    
    # Build lineage graph (treat IDs as opaque, like download.py does)
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

