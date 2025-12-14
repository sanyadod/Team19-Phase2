from flask import Flask, jsonify, abort, request
import boto3
from botocore.exceptions import ClientError
import logging
import json
import io
import zipfile
from collections import deque
from typing import Dict, List, Any, Optional, Set, Tuple

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")
S3_CLIENT = boto3.client("s3", region_name=AWS_REGION)


# -----------------------------
# Helpers
# -----------------------------

def _valid_id(artifact_id: str) -> bool:
    if not artifact_id:
        return False
    return all(c.isalnum() or c in "-._" for c in artifact_id)

def _scan_all_models() -> Dict[str, Dict[str, Any]]:
    """Scan DynamoDB once, return {model_id -> item} for artifact_type == 'model'."""
    try:
        resp = META_TABLE.scan()
        items = resp.get("Items", [])
        while "LastEvaluatedKey" in resp:
            resp = META_TABLE.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
            items.extend(resp.get("Items", []))

        models: Dict[str, Dict[str, Any]] = {}
        for it in items:
            if it.get("artifact_type") == "model":
                mid = str(it.get("id", "")).strip()
                if mid:
                    models[mid] = it

        logger.info("Scanned %d total items, found %d models", len(items), len(models))
        return models
    except ClientError as e:
        logger.error("DynamoDB scan failed: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")

def _display_name(item: Dict[str, Any], fallback_id: str) -> str:
    return str(item.get("name") or item.get("filename") or fallback_id)

def load_config_json_from_s3_zip(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Load config.json from S3 zip file.
    Reads bucket=item["s3_bucket"] and key=item["s3_key"],
    downloads the zip, finds config.json anywhere in the archive,
    parses it as JSON and returns a dict.
    Returns None if missing/unreadable.
    On S3/Dynamo errors, aborts with HTTP 500.
    """
    bucket = item.get("s3_bucket")
    key = item.get("s3_key")
    
    if not bucket or not key:
        logger.debug("Item missing s3_bucket or s3_key")
        return None
    
    try:
        # Download the file from S3
        logger.debug("Downloading file from S3: bucket=%s, key=%s", bucket, key)
        response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
        file_data = response["Body"].read()
        
        # Check if file is actually a zip file by checking magic bytes
        if not file_data.startswith(b'PK\x03\x04') and not file_data.startswith(b'PK\x05\x06'):
            logger.debug("File s3://%s/%s is not a zip file (missing PK header)", bucket, key)
            return None
        
        # Open as zipfile and search for config.json
        with zipfile.ZipFile(io.BytesIO(file_data)) as zf:
            config_path = None
            # Search for config.json in root or nested folders
            for name in zf.namelist():
                # Normalize path separators and check if it's config.json
                normalized = name.replace("\\", "/").strip("/")
                if normalized == "config.json" or normalized.endswith("/config.json"):
                    config_path = name
                    logger.debug("Found config.json at path: %s in zip %s", config_path, key)
                    break
            
            if not config_path:
                logger.debug("config.json not found in zip %s", key)
                return None
            
            # Read and parse config.json
            try:
                config_content = zf.read(config_path)
                config_dict = json.loads(config_content.decode("utf-8"))
                logger.info("Successfully loaded config.json from path '%s' in zip s3://%s/%s", 
                          config_path, bucket, key)
                return config_dict
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning("Failed to parse config.json from %s in zip %s: %s", config_path, key, e)
                return None
                
    except ClientError as e:
        logger.error("S3 get_object failed: bucket=%s, key=%s, error=%s", bucket, key, e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")
    except zipfile.BadZipFile as e:
        logger.debug("File s3://%s/%s is not a valid zip file: %s", bucket, key, e)
        return None
    except IOError as e:
        logger.warning("IO error reading zip file s3://%s/%s: %s", bucket, key, e)
        return None
    except Exception as e:
        logger.error("Unexpected error loading config.json: bucket=%s, key=%s, error=%s", bucket, key, e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")

def _extract_parent_name_from_config(cfg: Dict[str, Any]) -> Optional[str]:
    """
    Extract parent/base-model name from config.json.
    Prefer conservative keys that indicate a base model, NOT self-reference.
    Do NOT use _name_or_path or name_or_path as they usually refer to self.
    """
    if not isinstance(cfg, dict):
        return None

    # Conservative keys that indicate a parent/base model
    # Order matters: most specific first
    for key in ["base_model_name_or_path", "pretrained_model_name_or_path", "base_model"]:
        val = cfg.get(key)
        if isinstance(val, str) and val.strip():
            parent_name = val.strip()
            logger.debug("Extracted parent name '%s' from config key '%s'", parent_name, key)
            return parent_name

    return None

def _build_name_to_id_map(models: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Map human-readable names to ids so config_json fallback can resolve parent names.
    """
    m: Dict[str, str] = {}
    for mid, item in models.items():
        nm = _display_name(item, mid).strip()
        if nm:
            m[nm] = mid
    return m

def _as_list(x: Any) -> List[str]:
    """Normalize list-ish parent fields."""
    if x is None:
        return []
    if isinstance(x, list):
        return [str(v).strip() for v in x if str(v).strip()]
    # sometimes stored as single string
    if isinstance(x, str) and x.strip():
        return [x.strip()]
    return []

def _choose_parent_ids(item: Dict[str, Any], models: Dict[str, Dict[str, Any]], 
                       name_to_id: Dict[str, str], config_cache: Dict[str, Optional[Dict[str, Any]]]) -> List[str]:
    """
    Determine parent model IDs for a given model item.
    Priority:
      1) direct stored parent id fields (single)
      2) stored list fields (parents / parent_uuids / parent_ids)
      3) config_json from S3 zip (name -> id)
    Returns only parents that exist in `models`.
    """
    model_id = str(item.get("id", ""))
    
    # 1) single stored parent id fields 
    single_keys = ["parent_uuid", "parent_id", "parent_artifact_id", "base_model_id"]
    for k in single_keys:
        if k in item and item[k]:
            pid = str(item[k]).strip()
            if pid in models:
                logger.debug("Model %s: Found parent %s from field %s", model_id, pid, k)
                return [pid]

    # 2) list stored fields
    list_keys = ["parents", "parent_uuids", "parent_ids", "base_model_ids"]
    for k in list_keys:
        if k in item and item[k]:
            pids = [pid for pid in _as_list(item[k]) if pid in models]
            if pids:
                logger.debug("Model %s: Found parents %s from field %s", model_id, pids, k)
                return pids

    # 3) config_json from S3 zip (name -> id)
    # Check cache first
    if model_id not in config_cache:
        config_cache[model_id] = load_config_json_from_s3_zip(item)
    
    cfg = config_cache[model_id]
    if cfg:
        pname = _extract_parent_name_from_config(cfg)
        if pname:
            logger.info("Model %s: Extracted base model name '%s' from config.json", model_id, pname)
            pid = name_to_id.get(pname)
            if pid and pid in models:
                logger.info("Model %s: Resolved parent name '%s' -> parent_id '%s'", model_id, pname, pid)
                return [pid]
            else:
                logger.info("Model %s: Parent name '%s' not found in registry (resolved to: %s)", 
                           model_id, pname, pid)

    return []

def _build_parent_child_maps_lazy(models: Dict[str, Dict[str, Any]], 
                                   start_id: str, 
                                   max_depth: int = 10) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """
    Build parent/child maps lazily by only loading config.json for visited nodes.
    Uses BFS starting from start_id for ancestors, then discovers descendants.
    parents_map[child_id] = [parent_ids]
    children_map[parent_id] = [child_ids]
    """
    name_to_id = _build_name_to_id_map(models)
    config_cache: Dict[str, Optional[Dict[str, Any]]] = {}
    parents_map: Dict[str, List[str]] = {}
    children_map: Dict[str, List[str]] = {}
    
    # BFS to discover ancestors (parents) - only load config.json as needed
    ancestors_visited: Set[str] = set()
    queue = deque([(start_id, 0)])  # (model_id, depth)
    
    logger.info("Building parent/child maps lazily starting from %s (max_depth=%d)", start_id, max_depth)
    
    # Phase 1: Discover ancestors by following parent links
    while queue:
        current_id, depth = queue.popleft()
        
        if current_id in ancestors_visited or depth > max_depth:
            continue
        
        ancestors_visited.add(current_id)
        
        if current_id not in models:
            continue
        
        item = models[current_id]
        
        # Load config.json for this node (cached)
        parent_ids = _choose_parent_ids(item, models, name_to_id, config_cache)
        
        if parent_ids:
            parents_map[current_id] = parent_ids
            for pid in parent_ids:
                children_map.setdefault(pid, []).append(current_id)
                # Continue BFS for parents
                if pid not in ancestors_visited and depth < max_depth:
                    queue.append((pid, depth + 1))
    
    # Phase 2: Discover descendants (children) - check all models for children of visited nodes
    # Use BFS starting from start_id to find all descendants
    descendants_visited: Set[str] = set()
    queue = deque([(start_id, 0)])
    
    while queue:
        current_id, depth = queue.popleft()
        
        if current_id in descendants_visited or depth > max_depth:
            continue
        
        descendants_visited.add(current_id)
        
        # Check all models to see if they have current_id as a parent
        for child_id, item in models.items():
            if child_id in descendants_visited:
                continue
            
            # Load config.json if needed to check parent relationship
            parent_ids = _choose_parent_ids(item, models, name_to_id, config_cache)
            
            # If current_id is a parent of this child
            if current_id in parent_ids:
                if child_id not in parents_map:
                    parents_map[child_id] = []
                if current_id not in parents_map[child_id]:
                    parents_map[child_id].append(current_id)
                children_map.setdefault(current_id, []).append(child_id)
                # Continue BFS for this child
                if child_id not in descendants_visited and depth < max_depth:
                    queue.append((child_id, depth + 1))
    
    logger.info("Built maps: %d models with parents, %d models with children (loaded %d config.json files)",
                len(parents_map), len(children_map), len(config_cache))
    return parents_map, children_map

def _bfs_ancestors(start_id: str, parents_map: Dict[str, List[str]]) -> Set[str]:
    ancestors: Set[str] = set()
    q = deque([start_id])
    seen: Set[str] = set()

    while q:
        cur = q.popleft()
        if cur in seen:
            continue
        seen.add(cur)

        for pid in parents_map.get(cur, []):
            if pid not in ancestors:
                ancestors.add(pid)
                q.append(pid)
    return ancestors

def _bfs_descendants(start_id: str, children_map: Dict[str, List[str]]) -> Set[str]:
    descendants: Set[str] = set()
    q = deque([start_id])
    seen: Set[str] = set()

    while q:
        cur = q.popleft()
        if cur in seen:
            continue
        seen.add(cur)

        for cid in children_map.get(cur, []):
            if cid not in descendants:
                descendants.add(cid)
                q.append(cid)
    return descendants

def _build_lineage_graph(start_id: str,
                         models: Dict[str, Dict[str, Any]],
                         parents_map: Dict[str, List[str]],
                         children_map: Dict[str, List[str]]) -> Dict[str, Any]:
    ancestors = _bfs_ancestors(start_id, parents_map)
    descendants = _bfs_descendants(start_id, children_map)
    node_ids = {start_id} | ancestors | descendants

    # Nodes
    nodes = []
    for nid in sorted(node_ids):
        item = models.get(nid, {})
        nodes.append({
            "artifact_id": nid,
            "name": _display_name(item, nid),
            "source": "config_json",
        })

    # Edges: parent -> child, relationship "base_model"
    edges = []
    edge_seen: Set[Tuple[str, str]] = set()
    for child_id in node_ids:
        for parent_id in parents_map.get(child_id, []):
            if parent_id in node_ids:
                key = (parent_id, child_id)
                if key not in edge_seen:
                    edge_seen.add(key)
                    edges.append({
                        "from_node_artifact_id": parent_id,
                        "to_node_artifact_id": child_id,
                        "relationship": "base_model",
                    })

    return {"nodes": nodes, "edges": edges}


# -----------------------------
# Route
# -----------------------------

@app.route("/artifact/model/<artifact_id>/lineage", methods=["GET"])
def get_lineage(artifact_id: str):
    # NOTE: No auth enforcement for baseline

    if not _valid_id(artifact_id):
        abort(400, description="There is missing field(s) in the artifact_type or artifact_id or it is formed improperly, or is invalid.")

    # Scan DynamoDB once to get all models
    models = _scan_all_models()
    if artifact_id not in models:
        abort(404, description="Artifact does not exist.")

    # Build parent/child maps lazily (only load config.json for visited nodes)
    parents_map, children_map = _build_parent_child_maps_lazy(models, artifact_id, max_depth=10)
    
    # Build lineage graph
    graph = _build_lineage_graph(artifact_id, models, parents_map, children_map)
    return jsonify(graph), 200
