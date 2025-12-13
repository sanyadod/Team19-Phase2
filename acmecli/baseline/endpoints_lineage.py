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
    Build a baseline one-hop lineage graph for the given artifact.
    
    Baseline lineage includes:
    - The artifact itself
    - Its direct parents (one hop up)
    - Its direct children (one hop down)
    
    IDs are treated as opaque strings (like upload/download endpoints).
    Only includes artifacts that exist in DynamoDB.
    """
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()
    
    # Get the start artifact's ID (treat as opaque string, like other endpoints)
    # DynamoDB may return Decimal or other types, so convert to string explicitly
    start_id_raw = start_artifact.get("id", artifact_id)
    start_id = str(start_id_raw)  # Convert to string explicitly for schema compliance
    normalized_start_id = _normalize_id_for_comparison(start_id)
    
    # Step 1: Add the starting artifact
    start_name = start_artifact.get("filename") or start_artifact.get("name") or start_id
    nodes.append({
        "artifact_id": start_id,  # String type for schema compliance
        "name": str(start_name),
        "source": "config_json"
    })
    seen_ids.add(normalized_start_id)
    
    # Step 2: Add direct parents (only if they exist in DynamoDB)
    parents = start_artifact.get("parents", [])
    if isinstance(parents, list):
        for parent_id in parents:
            normalized_parent_id = _normalize_id_for_comparison(parent_id)
            if not normalized_parent_id or normalized_parent_id in seen_ids:
                continue
            
            # Only include parent if it exists in DynamoDB
            parent_artifact = _find_artifact_by_id(parent_id, all_artifacts)
            if parent_artifact:
                parent_id_raw = parent_artifact.get("id", parent_id)
                parent_id_value = str(parent_id_raw)  # Convert to string explicitly
                parent_name = parent_artifact.get("filename") or parent_artifact.get("name") or parent_id_value
                
                nodes.append({
                    "artifact_id": parent_id_value,  # String type for schema compliance
                    "name": str(parent_name),
                    "source": "config_json"
                })
                seen_ids.add(normalized_parent_id)
                
                # Add edge: parent -> start
                edges.append({
                    "from_node_artifact_id": parent_id_value,  # String type for schema compliance
                    "to_node_artifact_id": start_id,  # String type for schema compliance
                    "relationship": "base_model"
                })
    
    # Step 3: Add direct children (only artifacts that exist in DynamoDB)
    for artifact in all_artifacts:
        artifact_parents = artifact.get("parents", [])
        if not isinstance(artifact_parents, list):
            continue
        
        # Check if this artifact has start_id as a parent
        artifact_id_raw = artifact.get("id")
        if not artifact_id_raw:
            continue
        
        artifact_id_value = str(artifact_id_raw)  # Convert to string explicitly
        normalized_artifact_id = _normalize_id_for_comparison(artifact_id_value)
        
        # Check if start_id is in this artifact's parents list
        for p in artifact_parents:
            if _normalize_id_for_comparison(p) == normalized_start_id:
                # This is a direct child
                if normalized_artifact_id not in seen_ids:
                    child_name = artifact.get("filename") or artifact.get("name") or artifact_id_value
                    nodes.append({
                        "artifact_id": artifact_id_value,  # String type for schema compliance
                        "name": str(child_name),
                        "source": "config_json"
                    })
                    seen_ids.add(normalized_artifact_id)
                
                # Add edge: start -> child
                edges.append({
                    "from_node_artifact_id": start_id,  # String type for schema compliance
                    "to_node_artifact_id": artifact_id_value,  # String type for schema compliance
                    "relationship": "base_model"
                })
                break  # Only add one edge per child
    
    return {
        "nodes": nodes,
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

