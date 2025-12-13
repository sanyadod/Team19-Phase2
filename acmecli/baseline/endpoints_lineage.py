from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging
from typing import Dict, List, Any, Set, Optional

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


def _find_artifact_by_id(artifact_id: Any, all_artifacts: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Find an artifact by ID in the all_artifacts list. Treat IDs as opaque."""
    normalized_id = _normalize_id_for_comparison(artifact_id)
    for item in all_artifacts:
        if _normalize_id_for_comparison(item.get("id")) == normalized_id:
            return item
    return None


def _build_lineage_graph(start_artifact: Dict[str, Any], artifact_id: Any) -> Dict[str, Any]:
    """
    Build a baseline lineage graph from the artifact's metadata.
    
    Baseline lineage:
    - Always includes the artifact itself
    - Includes parents declared in metadata (if present)
    - Does NOT infer children (no reverse traversal)
    - Does NOT recurse
    """
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()
    
    # Get the artifact's ID (treat as opaque string)
    artifact_id_value = start_artifact.get("id", artifact_id)
    artifact_id_str = str(artifact_id_value)  # Ensure string type
    
    # Always add the artifact itself as a node
    artifact_name = start_artifact.get("filename") or start_artifact.get("name") or artifact_id_str
    nodes.append({
        "artifact_id": artifact_id_str,
        "name": str(artifact_name),
        "source": "config_json"
    })
    seen_ids.add(artifact_id_str)
    
    # If no parents field, return single-node graph with empty edges
    if "parents" not in start_artifact:
        return {
            "nodes": nodes,
            "edges": edges
        }
    
    # Parents must be a list - return 400 if malformed
    parents = start_artifact.get("parents")
    if not isinstance(parents, list):
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    # Process each parent in the parents list (baseline: parents are IDs only, not dicts)
    for parent_id in parents:
        # Baseline only accepts ID strings/numbers, not dicts
        if isinstance(parent_id, dict):
            abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
        
        if not parent_id:
            continue
        
        parent_artifact_id_str = str(parent_id)
        
        # Skip if duplicate (already seen)
        if parent_artifact_id_str in seen_ids:
            continue
        
        # Skip if parent ID equals artifact ID (self-reference)
        if parent_artifact_id_str == artifact_id_str:
            continue
        
        # Try to get parent name from DynamoDB
        parent_name = None
        try:
            parent_resp = META_TABLE.get_item(Key={"id": parent_artifact_id_str})
            parent_item = parent_resp.get("Item")
            if parent_item:
                parent_name = parent_item.get("filename") or parent_item.get("name")
        except ClientError:
            # If lookup fails, we'll use the ID as the name
            pass
        
        # Add parent node
        nodes.append({
            "artifact_id": parent_artifact_id_str,
            "name": str(parent_name) if parent_name else parent_artifact_id_str,
            "source": "config_json"
        })
        seen_ids.add(parent_artifact_id_str)
        
        # Add edge: parent -> artifact (always use "base_model" for baseline)
        edges.append({
            "from_node_artifact_id": parent_artifact_id_str,
            "to_node_artifact_id": artifact_id_str,
            "relationship": "base_model"  # Always "base_model" for baseline
        })
    
    return {
        "nodes": nodes,
        "edges": edges
    }


@app.route("/artifact/<artifact_type>/<artifact_id>/lineage", methods=["GET"])
def get_lineage(artifact_type: str, artifact_id: str):
    """
    GET /artifact/<artifact_type>/<artifact_id>/lineage
    Get the lineage graph for an artifact, including its direct parents.
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
    
    # Check if metadata is malformed
    if not isinstance(metadata, dict):
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    # Build lineage graph from metadata only (no reverse traversal)
    try:
        graph = _build_lineage_graph(metadata, artifact_id)
    except Exception as e:
        logger.error(f"Error building lineage graph: {e}", exc_info=True)
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    # Ensure graph has the correct structure
    if not isinstance(graph, dict) or "nodes" not in graph or "edges" not in graph:
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    return jsonify(graph), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5008, debug=True)

