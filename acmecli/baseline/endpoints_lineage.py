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
    if not artifact_id:
        return False
    return all(c.isalnum() or c in "-._" for c in artifact_id)


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


def _convert_id(id_value: Any) -> Any:
    """Convert ID to int if possible, otherwise keep as string."""
    try:
        return int(id_value)
    except (TypeError, ValueError):
        return str(id_value)


def _build_lineage_graph(artifact_id: str, artifact_type: str, all_artifacts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build a lineage graph starting from the given artifact.
    Includes the artifact itself, its parents, and any children.
    """
    # Find the starting artifact
    start_artifact = None
    for item in all_artifacts:
        if str(item.get("id")) == str(artifact_id) and item.get("artifact_type") == artifact_type:
            start_artifact = item
            break
    
    if not start_artifact:
        return {"nodes": [], "edges": []}
    
    # Collect all related artifacts (parents and children)
    node_ids: Set[str] = {str(artifact_id)}
    edges: List[Dict[str, Any]] = []
    
    # Get parents from the artifact
    parents = start_artifact.get("parents", [])
    if not isinstance(parents, list):
        parents = []
    
    for parent_id in parents:
        parent_id_str = str(parent_id)
        # Only add parent if it exists in the database
        parent_exists = any(str(item.get("id")) == parent_id_str for item in all_artifacts)
        if parent_exists:
            node_ids.add(parent_id_str)
            edges.append({
                "from_node_artifact_id": _convert_id(parent_id),
                "to_node_artifact_id": _convert_id(artifact_id),
                "relationship": "base_model"
            })
    
    # Find children (artifacts that have this artifact as a parent)
    for item in all_artifacts:
        item_id = str(item.get("id"))
        item_parents = item.get("parents", [])
        if not isinstance(item_parents, list):
            item_parents = []
        
        if str(artifact_id) in [str(p) for p in item_parents]:
            node_ids.add(item_id)
            edges.append({
                "from_node_artifact_id": _convert_id(artifact_id),
                "to_node_artifact_id": _convert_id(item.get("id")),
                "relationship": "base_model"
            })
    
    # Build nodes list
    nodes: List[Dict[str, Any]] = []
    for node_id in node_ids:
        # Find the artifact for this node
        node_artifact = None
        for item in all_artifacts:
            if str(item.get("id")) == node_id:
                node_artifact = item
                break
        
        if node_artifact:
            nodes.append({
                "artifact_id": _convert_id(node_artifact.get("id")),
                "name": node_artifact.get("filename", str(node_artifact.get("id", ""))),
                "source": "config_json"
            })
    
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
    
    # Validate artifact ID
    if not _valid_id(artifact_id):
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
    graph = _build_lineage_graph(artifact_id, artifact_type, all_artifacts)
    
    return jsonify(graph), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5008, debug=True)

