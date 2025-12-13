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


def _convert_id(id_value: Any) -> int:
    """
    Convert ID to int. 
    The OpenAPI schema requires artifact_id to always be an integer.
    If conversion fails, raise an error as this indicates malformed data.
    """
    try:
        # Handle None or empty values
        if id_value is None:
            raise ValueError("ID cannot be None")
        
        # Convert to int - this handles strings, numbers, etc.
        result = int(id_value)
        
        # Ensure it's a valid positive integer (artifact IDs should be positive)
        if result < 0:
            raise ValueError(f"ID must be non-negative, got {result}")
        
        return result
    except (TypeError, ValueError) as e:
        # If we can't convert to int, this is a data quality issue
        # Log it and raise to trigger 400 response
        logger.error(f"Invalid artifact ID format: {id_value} (type: {type(id_value)}), error: {e}")
        raise ValueError(f"Cannot convert artifact ID to integer: {id_value}") from e


def _build_lineage_graph(start_artifact: Dict[str, Any], artifact_id: str, all_artifacts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build a lineage graph starting from the given artifact.
    Includes the artifact itself, its parents, and any children.
    """
    # Collect all related artifacts (parents and children)
    node_ids: Set[str] = {str(artifact_id)}
    edges: List[Dict[str, Any]] = []
    
    # Get parents from the artifact
    parents = start_artifact.get("parents", [])
    if not isinstance(parents, list):
        parents = []
    
    for parent_id in parents:
        try:
            parent_id_str = str(parent_id)
            # Only add parent if it exists in the database
            parent_exists = any(str(item.get("id")) == parent_id_str for item in all_artifacts)
            if parent_exists:
                node_ids.add(parent_id_str)
                # Convert IDs to int - if this fails, skip this parent
                from_id = _convert_id(parent_id)
                to_id = _convert_id(artifact_id)
                edges.append({
                    "from_node_artifact_id": from_id,
                    "to_node_artifact_id": to_id,
                    "relationship": "base_model"
                })
        except (ValueError, TypeError) as e:
            # Skip malformed parent IDs
            logger.warning(f"Skipping malformed parent ID: {parent_id}, error: {e}")
            continue
    
    # Find children (artifacts that have this artifact as a parent)
    for item in all_artifacts:
        item_id = str(item.get("id"))
        item_parents = item.get("parents", [])
        if not isinstance(item_parents, list):
            item_parents = []
        
        if str(artifact_id) in [str(p) for p in item_parents]:
            try:
                node_ids.add(item_id)
                # Convert IDs to int - if this fails, skip this child
                from_id = _convert_id(artifact_id)
                to_id = _convert_id(item.get("id"))
                edges.append({
                    "from_node_artifact_id": from_id,
                    "to_node_artifact_id": to_id,
                    "relationship": "base_model"
                })
            except (ValueError, TypeError) as e:
                # Skip malformed child IDs
                logger.warning(f"Skipping malformed child ID: {item.get('id')}, error: {e}")
                continue
    
    # Build nodes list - always include the starting artifact first
    nodes: List[Dict[str, Any]] = []
    
    # Add the starting artifact first
    # The starting artifact ID must be convertible to int (this is validated earlier)
    try:
        artifact_id_int = _convert_id(start_artifact.get("id"))
        artifact_name = start_artifact.get("filename") or start_artifact.get("name") or str(start_artifact.get("id", ""))
        nodes.append({
            "artifact_id": artifact_id_int,
            "name": str(artifact_name),
            "source": "config_json"
        })
    except (ValueError, TypeError) as e:
        # If the starting artifact ID is malformed, this is a critical error
        logger.error(f"Starting artifact has malformed ID: {start_artifact.get('id')}, error: {e}")
        raise ValueError(f"Starting artifact ID cannot be converted to integer: {start_artifact.get('id')}") from e
    
    # Add other nodes (parents and children)
    for node_id in node_ids:
        if str(node_id) == str(artifact_id):
            continue  # Already added
        
        # Find the artifact for this node
        node_artifact = None
        for item in all_artifacts:
            if str(item.get("id")) == node_id:
                node_artifact = item
                break
        
        if node_artifact:
            try:
                node_id_int = _convert_id(node_artifact.get("id"))
                node_name = node_artifact.get("filename") or node_artifact.get("name") or str(node_artifact.get("id", ""))
                nodes.append({
                    "artifact_id": node_id_int,
                    "name": str(node_name),
                    "source": "config_json"
                })
            except (ValueError, TypeError) as e:
                # Skip malformed node IDs
                logger.warning(f"Skipping node with malformed ID: {node_artifact.get('id')}, error: {e}")
                continue
    
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
    
    # Validate that artifact_id can be converted to integer (required by OpenAPI schema)
    try:
        artifact_id_int = _convert_id(artifact_id)
    except (ValueError, TypeError) as e:
        logger.error(f"Artifact ID cannot be converted to integer: {artifact_id}, error: {e}")
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

