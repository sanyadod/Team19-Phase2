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


def _normalize_id_to_int(id_value: Any) -> int:
    """Convert ID to integer. Raises ValueError if conversion fails."""
    if id_value is None:
        raise ValueError("ID cannot be None")
    try:
        return int(id_value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"Cannot convert ID to integer: {id_value}") from e


def _normalize_id_for_comparison(id_value: Any) -> str:
    """Normalize ID to string for comparison purposes only."""
    if id_value is None:
        return ""
    return str(id_value)


def _find_artifact_by_id(artifact_id: int, all_artifacts: List[Dict[str, Any]]) -> Dict[str, Any] | None:
    """Find an artifact by ID in the all_artifacts list."""
    for item in all_artifacts:
        try:
            item_id = int(item.get("id"))
            if item_id == artifact_id:
                return item
        except (TypeError, ValueError):
            continue
    return None


def _build_lineage_graph(start_artifact: Dict[str, Any], artifact_id: int, all_artifacts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Build a transitive lineage graph starting from the given artifact.
    Recursively includes all ancestors (parents of parents) and descendants (children of children).
    All IDs are normalized to integers to match OpenAPI schema.
    """
    # Track all nodes and edges we've discovered
    nodes: Dict[int, Dict[str, Any]] = {}  # id (int) -> node data
    edges: List[Dict[str, Any]] = []
    visited: Set[int] = set()  # Track visited nodes to avoid cycles
    
    def walk_up(parent_id: int) -> None:
        """Recursively walk up the parent chain."""
        if parent_id in visited:
            return
        
        visited.add(parent_id)
        
        # Find the parent artifact
        parent_artifact = _find_artifact_by_id(parent_id, all_artifacts)
        if not parent_artifact:
            return
        
        # Add parent node (ID is already normalized to int)
        parent_name = parent_artifact.get("filename") or parent_artifact.get("name") or str(parent_id)
        nodes[parent_id] = {
            "artifact_id": parent_id,  # Always integer
            "name": str(parent_name),
            "source": "config_json"
        }
        
        # Get parent's parents and recurse
        parent_parents = parent_artifact.get("parents", [])
        if isinstance(parent_parents, list):
            for grandparent_id_raw in parent_parents:
                try:
                    grandparent_id = _normalize_id_to_int(grandparent_id_raw)
                    # Add edge: grandparent -> parent
                    edges.append({
                        "from_node_artifact_id": grandparent_id,  # Always integer
                        "to_node_artifact_id": parent_id,  # Always integer
                        "relationship": "parent"
                    })
                    # Recurse
                    walk_up(grandparent_id)
                except ValueError:
                    # Skip malformed parent IDs
                    logger.warning(f"Skipping malformed grandparent ID: {grandparent_id_raw}")
                    continue
    
    def walk_down(child_id: int) -> None:
        """Recursively walk down the child chain."""
        if child_id in visited:
            return
        
        visited.add(child_id)
        
        # Find the child artifact
        child_artifact = _find_artifact_by_id(child_id, all_artifacts)
        if not child_artifact:
            return
        
        # Add child node (ID is already normalized to int)
        child_name = child_artifact.get("filename") or child_artifact.get("name") or str(child_id)
        nodes[child_id] = {
            "artifact_id": child_id,  # Always integer
            "name": str(child_name),
            "source": "config_json"
        }
        
        # Find children of this child (grandchildren)
        for item in all_artifacts:
            item_parents = item.get("parents", [])
            if isinstance(item_parents, list):
                # Check if this item has child_id as a parent
                try:
                    item_id = int(item.get("id"))
                    for p in item_parents:
                        try:
                            if int(p) == child_id:
                                # Add edge: child -> grandchild
                                edges.append({
                                    "from_node_artifact_id": child_id,  # Always integer
                                    "to_node_artifact_id": item_id,  # Always integer
                                    "relationship": "parent"
                                })
                                # Recurse
                                walk_down(item_id)
                                break
                        except (TypeError, ValueError):
                            continue
                except (TypeError, ValueError):
                    continue
    
    # Start with the artifact itself (ID is already normalized to int)
    visited.add(artifact_id)
    
    start_name = start_artifact.get("filename") or start_artifact.get("name") or str(artifact_id)
    nodes[artifact_id] = {
        "artifact_id": artifact_id,  # Always integer
        "name": str(start_name),
        "source": "config_json"
    }
    
    # Walk up: get all ancestors
    parents = start_artifact.get("parents", [])
    if isinstance(parents, list):
        for parent_id_raw in parents:
            try:
                parent_id = _normalize_id_to_int(parent_id_raw)
                # Add edge: parent -> start
                edges.append({
                    "from_node_artifact_id": parent_id,  # Always integer
                    "to_node_artifact_id": artifact_id,  # Always integer
                    "relationship": "parent"
                })
                # Recurse up
                walk_up(parent_id)
            except ValueError:
                # Skip malformed parent IDs
                logger.warning(f"Skipping malformed parent ID: {parent_id_raw}")
                continue
    
    # Walk down: get all descendants
    for item in all_artifacts:
        item_parents = item.get("parents", [])
        if isinstance(item_parents, list):
            # Check if this item has artifact_id as a parent
            for p in item_parents:
                try:
                    if int(p) == artifact_id:
                        try:
                            child_id = int(item.get("id"))
                            # Add edge: start -> child
                            edges.append({
                                "from_node_artifact_id": artifact_id,  # Always integer
                                "to_node_artifact_id": child_id,  # Always integer
                                "relationship": "parent"
                            })
                            # Recurse down
                            walk_down(child_id)
                        except (TypeError, ValueError):
                            continue
                        break
                except (TypeError, ValueError):
                    continue
    
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
    
    # Validate artifact ID (minimal validation)
    if not artifact_id or not artifact_id.strip():
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )
    
    # Normalize artifact_id to integer (OpenAPI schema requires integers)
    try:
        artifact_id_int = _normalize_id_to_int(artifact_id)
    except ValueError as e:
        logger.error(f"Artifact ID cannot be converted to integer: {artifact_id}, error: {e}")
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )

    # Verify artifact exists (use string for DynamoDB lookup)
    metadata = _fetch_metadata(artifact_type, artifact_id)
    
    # Check if metadata is malformed (missing required fields for lineage)
    # If artifact exists but has no valid structure, return 400
    if not isinstance(metadata, dict):
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    # Normalize the starting artifact's ID to integer
    try:
        metadata["id"] = _normalize_id_to_int(metadata.get("id"))
        # Normalize parent IDs to integers
        if "parents" in metadata and isinstance(metadata["parents"], list):
            normalized_parents = []
            for p in metadata["parents"]:
                try:
                    normalized_parents.append(_normalize_id_to_int(p))
                except ValueError:
                    logger.warning(f"Skipping malformed parent ID in metadata: {p}")
            metadata["parents"] = normalized_parents
    except ValueError as e:
        logger.error(f"Starting artifact has malformed ID: {metadata.get('id')}, error: {e}")
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    # Get all artifacts to build the graph
    all_artifacts = _get_all_artifacts()
    
    # Normalize all artifact IDs and parent IDs to integers
    normalized_artifacts = []
    for item in all_artifacts:
        try:
            normalized_item = item.copy()
            normalized_item["id"] = _normalize_id_to_int(item.get("id"))
            # Normalize parent IDs
            if "parents" in normalized_item and isinstance(normalized_item["parents"], list):
                normalized_parents = []
                for p in normalized_item["parents"]:
                    try:
                        normalized_parents.append(_normalize_id_to_int(p))
                    except ValueError:
                        logger.warning(f"Skipping malformed parent ID: {p}")
                normalized_item["parents"] = normalized_parents
            normalized_artifacts.append(normalized_item)
        except ValueError:
            # Skip artifacts with malformed IDs
            logger.warning(f"Skipping artifact with malformed ID: {item.get('id')}")
            continue
    
    # Build lineage graph (use normalized integer ID)
    try:
        graph = _build_lineage_graph(metadata, artifact_id_int, normalized_artifacts)
    except Exception as e:
        logger.error(f"Error building lineage graph: {e}", exc_info=True)
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    # Ensure graph has the correct structure
    if not isinstance(graph, dict) or "nodes" not in graph or "edges" not in graph:
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    return jsonify(graph), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5008, debug=True)

