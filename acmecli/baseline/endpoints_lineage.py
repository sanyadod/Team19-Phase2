from flask import Flask, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging
import json
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


def _scan_all_models() -> Dict[str, Dict[str, Any]]:
    """
    Scan DynamoDB once to get all models.
    Returns a dict mapping model_id -> model_metadata.
    """
    try:
        response = META_TABLE.scan()
        items = response.get("Items", [])

        while "LastEvaluatedKey" in response:
            response = META_TABLE.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            items.extend(response.get("Items", []))

        # Filter to models only and build map
        models = {}
        for item in items:
            if item.get("artifact_type") == "model":
                model_id = str(item.get("id", ""))
                if model_id:
                    models[model_id] = item

        logger.info(f"Scanned {len(items)} total items, found {len(models)} models")
        return models
    except ClientError as e:
        logger.error("DynamoDB scan failed: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")


def _build_name_to_id_map(models: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Build a map from human-readable model names to artifact IDs.
    Prefers: item["name"] > item["filename"] > item["id"]
    """
    name_to_id = {}
    for model_id, model_data in models.items():
        # Try name first, then filename, then id as fallback
        name = model_data.get("name") or model_data.get("filename") or model_id
        name_str = str(name).strip()
        if name_str:
            # If multiple models have the same name, last one wins (or we could handle collisions)
            name_to_id[name_str] = model_id
    
    logger.info(f"Built name_to_id map with {len(name_to_id)} entries")
    return name_to_id


def parse_config_json(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse config_json from a model item.
    config_json may be a dict or a JSON string.
    Returns None if config_json is missing or malformed.
    """
    config_json = item.get("config_json")
    if config_json is None:
        return None
    
    # If it's already a dict, return it
    if isinstance(config_json, dict):
        return config_json
    
    # If it's a string, try to parse it
    if isinstance(config_json, str):
        try:
            return json.loads(config_json)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to parse config_json string: {e}")
            return None
    
    # Unknown type
    logger.warning(f"config_json has unexpected type: {type(config_json)}")
    return None


def extract_parent_name(config: Dict[str, Any]) -> Optional[str]:
    """
    Extract parent/base-model name from config.json.
    Checks in order: base_model_name_or_path, name_or_path, _name_or_path
    Returns None if none found.
    """
    if not isinstance(config, dict):
        return None
    
    # Try in order of preference
    for key in ["base_model_name_or_path", "name_or_path", "_name_or_path"]:
        value = config.get(key)
        if value:
            parent_name = str(value).strip()
            if parent_name:
                return parent_name
    
    return None


def _build_parents_and_children_maps(models: Dict[str, Dict[str, Any]], 
                                     name_to_id: Dict[str, str]) -> tuple:
    """
    Build two maps from all models by parsing config.json:
    - parents_map[child_id] = [parent_id1, parent_id2, ...]
    - children_map[parent_id] = [child_id1, child_id2, ...]
    
    Only includes parent IDs that exist in the models dict.
    """
    parents_map: Dict[str, List[str]] = {}
    children_map: Dict[str, List[str]] = {}
    
    for model_id, model_data in models.items():
        # Parse config_json
        config = parse_config_json(model_data)
        if config is None:
            # No config_json or malformed - treat as no parent
            continue
        
        # Extract parent name from config
        parent_name = extract_parent_name(config)
        if not parent_name:
            # No parent name found in config
            continue
        
        # Convert parent name to ID
        parent_id = name_to_id.get(parent_name)
        if not parent_id:
            # Parent name not found in registry - ignore it
            logger.debug(f"Parent name '{parent_name}' not found in registry for model {model_id}")
            continue
        
        # Only include if parent exists in our models
        if parent_id in models:
            # Build parents_map (child -> parents)
            if model_id not in parents_map:
                parents_map[model_id] = []
            parents_map[model_id].append(parent_id)
            
            # Build children_map (parent -> children)
            if parent_id not in children_map:
                children_map[parent_id] = []
            children_map[parent_id].append(model_id)
    
    logger.info(f"Built maps: {len(parents_map)} models with parents, {len(children_map)} models with children")
    return parents_map, children_map


def _find_ancestors(start_id: str, parents_map: Dict[str, List[str]]) -> set:
    """
    Use BFS to find all ancestors (parents, grandparents, etc.) of start_id.
    """
    ancestors = set()
    queue = [start_id]
    visited = set()
    
    while queue:
        current_id = queue.pop(0)
        if current_id in visited:
            continue
        visited.add(current_id)
        
        # Get parents of current node
        parents = parents_map.get(current_id, [])
        for parent_id in parents:
            if parent_id not in visited and parent_id not in ancestors:
                ancestors.add(parent_id)
                queue.append(parent_id)
    
    logger.info(f"Found {len(ancestors)} ancestors for {start_id}")
    return ancestors


def _find_descendants(start_id: str, children_map: Dict[str, List[str]]) -> set:
    """
    Use BFS to find all descendants (children, grandchildren, etc.) of start_id.
    """
    descendants = set()
    queue = [start_id]
    visited = set()
    
    while queue:
        current_id = queue.pop(0)
        if current_id in visited:
            continue
        visited.add(current_id)
        
        # Get children of current node
        children = children_map.get(current_id, [])
        for child_id in children:
            if child_id not in visited and child_id not in descendants:
                descendants.add(child_id)
                queue.append(child_id)
    
    logger.info(f"Found {len(descendants)} descendants for {start_id}")
    return descendants


def _build_lineage_graph(start_id: str, models: Dict[str, Dict[str, Any]], 
                         parents_map: Dict[str, List[str]], 
                         children_map: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Build the full transitive lineage graph for start_id.
    """
    logger.info("=" * 80)
    logger.info(f"BUILDING LINEAGE GRAPH for {start_id}")
    logger.info("=" * 80)
    
    # Find all ancestors and descendants
    ancestors = _find_ancestors(start_id, parents_map)
    descendants = _find_descendants(start_id, children_map)
    
    # Final node set: start + ancestors + descendants
    all_node_ids = {start_id} | ancestors | descendants
    logger.info(f"Total nodes in lineage: {len(all_node_ids)} (start: 1, ancestors: {len(ancestors)}, descendants: {len(descendants)})")
    
    # Build nodes list
    nodes = []
    for node_id in sorted(all_node_ids):  # Sort for consistent output
        model_data = models.get(node_id, {})
        # Get human name: prefer name, then filename, then id
        node_name = model_data.get("name") or model_data.get("filename") or node_id
        nodes.append({
            "artifact_id": node_id,
            "name": str(node_name),
            "source": "config_json",
        })
    
    # Build edges: all parent->child relationships where BOTH endpoints are in all_node_ids
    edges = []
    edge_set = set()  # For deduplication
    
    for child_id in all_node_ids:
        parents = parents_map.get(child_id, [])
        for parent_id in parents:
            # Only include edge if parent is also in our node set
            if parent_id in all_node_ids:
                edge_key = (parent_id, child_id)
                if edge_key not in edge_set:
                    edges.append({
                        "from_node_artifact_id": parent_id,
                        "to_node_artifact_id": child_id,
                        "relationship": "base_model",
                    })
                    edge_set.add(edge_key)
    
    logger.info(f"Created {len(nodes)} nodes and {len(edges)} edges")
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
    logger.info(f"LINEAGE ENDPOINT CALLED for artifact_id: {artifact_id}")
    logger.info("=" * 80)

    # Validate artifact_id format
    if not _valid_id(artifact_id):
        logger.error(f"Invalid artifact_id format: {artifact_id}")
        abort(
            400,
            description=(
                "There is missing field(s) in the artifact_type or artifact_id "
                "or it is formed improperly, or is invalid."
            ),
        )

    # Do ONE scan to get all models
    logger.info("Scanning DynamoDB for all models...")
    models = _scan_all_models()
    
    # Check if start model exists
    start_id = artifact_id
    if start_id not in models:
        logger.error(f"Model {start_id} not found in registry")
        abort(404, description="Artifact does not exist.")
    
    start_model = models[start_id]
    
    # Validate config_json if present (check for malformed JSON)
    config_json = start_model.get("config_json")
    if config_json is not None:
        parsed_config = parse_config_json(start_model)
        if parsed_config is None and isinstance(config_json, str):
            # config_json exists as string but failed to parse
            logger.error(f"Model {start_id} has malformed config_json")
            abort(
                400,
                description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.",
            )
    
    # Build name_to_id map
    logger.info("Building name_to_id map...")
    name_to_id = _build_name_to_id_map(models)
    
    # Build parent/child maps from config.json
    logger.info("Building parent/child relationship maps from config.json...")
    parents_map, children_map = _build_parents_and_children_maps(models, name_to_id)
    
    # Build lineage graph
    graph = _build_lineage_graph(start_id, models, parents_map, children_map)
    
    logger.info("Returning lineage graph")
    return jsonify(graph), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5008, debug=True)
