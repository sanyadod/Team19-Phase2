from flask import Flask, jsonify, abort, request
import boto3
from botocore.exceptions import ClientError
import logging
import json
import io
import zipfile
from collections import deque
from typing import Dict, List, Any, Optional, Set, Tuple
from decimal import Decimal

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

def _normalize_id(id_value: Any) -> str:
    """Normalize an ID value from DynamoDB to a string, handling Decimal and other types."""
    if id_value is None:
        return ""
    if isinstance(id_value, Decimal):
        return str(id_value).strip()
    return str(id_value).strip()

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
            artifact_type = it.get("artifact_type")
            if artifact_type == "model":
                mid = _normalize_id(it.get("id"))
                if mid:
                    models[mid] = it
                else:
                    logger.warning("Found model item with empty or missing id: %s", it)

        logger.info("DynamoDB scan completed: %d total items scanned, %d models found", len(items), len(models))
        return models
    except ClientError as e:
        logger.error("DynamoDB scan failed with ClientError: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")
    except Exception as e:
        logger.error("Unexpected error during DynamoDB scan: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")

def _display_name(item: Dict[str, Any], fallback_id: str) -> str:
    return str(item.get("name") or item.get("filename") or fallback_id)

def load_config_json_from_s3(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Load config.json from S3 zip file.
    Reads bucket=item["s3_bucket"] and key=item["s3_key"],
    downloads the zip, finds config.json anywhere in the archive,
    parses it as JSON and returns a dict.
    Returns None if missing/unreadable.
    On S3/Dynamo errors, aborts with HTTP 500.
    """
    model_id = str(item.get("id", "unknown"))
    bucket = item.get("s3_bucket")
    key = item.get("s3_key")
    
    if not bucket or not key:
        logger.warning("Model %s: Missing s3_bucket or s3_key", model_id)
        return None
    
    try:
        response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
        file_data = response["Body"].read()
        
        # Check if file is actually a zip file by checking magic bytes
        if not file_data.startswith(b'PK\x03\x04') and not file_data.startswith(b'PK\x05\x06'):
            if file_data.startswith(b'<!doctype') or file_data.startswith(b'<html') or file_data.startswith(b'<HTML'):
                logger.warning("Model %s: File s3://%s/%s appears to be HTML instead of zip", model_id, bucket, key)
            else:
                logger.warning("Model %s: File s3://%s/%s is not a zip file", model_id, bucket, key)
            return None
        
        with zipfile.ZipFile(io.BytesIO(file_data)) as zf:
            zip_files = zf.namelist()
            
            config_path = None
            for name in zip_files:
                normalized = name.replace("\\", "/").strip("/")
                if normalized == "config.json" or normalized.endswith("/config.json"):
                    config_path = name
                    break
            
            if not config_path:
                logger.warning("Model %s: Missing config.json in zip s3://%s/%s", model_id, bucket, key)
                return None
            
            try:
                config_content = zf.read(config_path)
                config_dict = json.loads(config_content.decode("utf-8"))
                return config_dict
            except json.JSONDecodeError as e:
                logger.warning("Model %s: JSON decode error in config.json from %s in zip %s: %s (line %d, col %d)", 
                              model_id, config_path, key, e.msg, e.lineno, e.colno)
                return None
            except UnicodeDecodeError as e:
                logger.warning("Model %s: Unicode decode error in config.json from %s in zip %s: %s", 
                              model_id, config_path, key, e)
                return None
                
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_msg = e.response.get('Error', {}).get('Message', str(e))
        logger.error("Model %s: S3 get_object failed: bucket=%s, key=%s, error_code=%s, error=%s", 
                    model_id, bucket, key, error_code, error_msg, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")
    except zipfile.BadZipFile as e:
        logger.warning("Model %s: File s3://%s/%s is not a valid zip file: %s", model_id, bucket, key, e)
        return None
    except IOError as e:
        logger.warning("Model %s: IO error reading zip file s3://%s/%s: %s", model_id, bucket, key, e)
        return None
    except Exception as e:
        logger.error("Model %s: Unexpected error loading config.json: bucket=%s, key=%s, error=%s", 
                    model_id, bucket, key, e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")

def _extract_parent_name_from_config(cfg: Dict[str, Any]) -> Tuple[Optional[str], List[str]]:
    """
    Extract parent/base-model name from config.json.
    Prefer conservative keys that indicate a base model, NOT self-reference.
    Do NOT use _name_or_path or name_or_path as they usually refer to self.
    Returns (parent_name, keys_checked) tuple.
    """
    if not isinstance(cfg, dict):
        return None, []

    keys_to_check = ["base_model_name_or_path", "pretrained_model_name_or_path", "base_model"]
    
    for key in keys_to_check:
        val = cfg.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip(), keys_to_check

    return None, keys_to_check

def _get_filename_stem(filename: str) -> str:
    """Extract stem from filename by removing common extensions."""
    if not filename:
        return filename
    
    # Handle common extensions (order matters - try longer first)
    extensions = ['.tar.gz', '.tar.bz2', '.tar.xz', '.zip', '.onnx', '.tar', '.gz', '.bz2', '.xz']
    
    filename_lower = filename.lower()
    for ext in extensions:
        if filename_lower.endswith(ext):
            return filename[:-len(ext)]
    
    return filename

def _extract_hf_model_tail(name: str) -> Optional[str]:
    """Extract model name from HuggingFace-style path like 'org/model' -> 'model'."""
    if not name:
        return None
    
    # Check if it looks like a HuggingFace path (contains exactly one slash)
    parts = name.split('/')
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[1]
    
    return None

def _build_name_to_id_map(models: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Map multiple variants of each model to its ID:
    - id
    - name (if present)
    - filename (if present)
    - filename stem (strip extensions)
    - lowercase versions of all above
    - HuggingFace-style tail (if applicable)
    """
    m: Dict[str, str] = {}
    duplicates = []
    
    def _add_mapping(key: str, model_id: str):
        """Helper to add mapping and track duplicates."""
        if key:
            if key in m and m[key] != model_id:
                duplicates.append((key, model_id, m[key]))
            m[key] = model_id
    
    for mid, item in models.items():
        # Map ID (original and lowercase)
        _add_mapping(mid, mid)
        _add_mapping(mid.lower(), mid)
        
        # Map name (if present)
        name = item.get("name")
        if name:
            name_str = str(name).strip()
            if name_str:
                _add_mapping(name_str, mid)
                _add_mapping(name_str.lower(), mid)
                
                # Extract HuggingFace-style tail
                hf_tail = _extract_hf_model_tail(name_str)
                if hf_tail:
                    _add_mapping(hf_tail, mid)
                    _add_mapping(hf_tail.lower(), mid)
        
        # Map filename (if present)
        filename = item.get("filename")
        if filename:
            filename_str = str(filename).strip()
            if filename_str:
                _add_mapping(filename_str, mid)
                _add_mapping(filename_str.lower(), mid)
                
                # Map filename stem (without extension)
                stem = _get_filename_stem(filename_str)
                if stem and stem != filename_str:
                    _add_mapping(stem, mid)
                    _add_mapping(stem.lower(), mid)
                
                # Extract HuggingFace-style tail from filename
                hf_tail = _extract_hf_model_tail(filename_str)
                if hf_tail:
                    _add_mapping(hf_tail, mid)
                    _add_mapping(hf_tail.lower(), mid)
                    # Also try stem of the tail
                    hf_tail_stem = _get_filename_stem(hf_tail)
                    if hf_tail_stem and hf_tail_stem != hf_tail:
                        _add_mapping(hf_tail_stem, mid)
                        _add_mapping(hf_tail_stem.lower(), mid)
    
    if duplicates:
        logger.warning("Found %d duplicate name mappings (last id wins)", len(duplicates))
    
    return m

def _as_list(x: Any) -> List[str]:
    """Normalize list-ish parent fields. Handles DynamoDB types (Decimal, sets, etc.)."""
    if x is None:
        return []
    if isinstance(x, list):
        # Handle list of various types (strings, Decimals, etc.)
        result = []
        for v in x:
            # Convert Decimal to string, handle other types
            if isinstance(v, Decimal):
                v_str = str(v)
            else:
                v_str = str(v).strip()
            if v_str:
                result.append(v_str)
        return result
    # Handle DynamoDB sets
    if isinstance(x, (set, frozenset)):
        result = []
        for v in x:
            if isinstance(v, Decimal):
                v_str = str(v)
            else:
                v_str = str(v).strip()
            if v_str:
                result.append(v_str)
        return result
    # sometimes stored as single string
    if isinstance(x, str) and x.strip():
        return [x.strip()]
    # Handle Decimal (though unlikely for parent IDs)
    if isinstance(x, Decimal):
        v_str = str(x).strip()
        return [v_str] if v_str else []
    return []

def _choose_parent_ids(item: Dict[str, Any], models: Dict[str, Dict[str, Any]], 
                       name_to_id: Dict[str, str], config_cache: Dict[str, Optional[Dict[str, Any]]]) -> List[str]:
    """
    Determine parent model IDs for a given model item.
    DynamoDB 'parents' field is the authoritative source. S3 config.json is a best-effort fallback.
    Returns only parents that exist in `models`.
    """
    model_id = str(item.get("id", ""))
    
    # DynamoDB 'parents' field is the authoritative source
    raw_parents = item.get("parents")
    parents_from_db = _as_list(raw_parents)
    
    if parents_from_db:
        # Log raw value and parsed list
        logger.warning("Model %s: Raw parents value from DynamoDB: %s", model_id, raw_parents)
        logger.warning("Model %s: Parsed parent ID list: %s", model_id, parents_from_db)
        
        # Filter to only include parent IDs that exist in models registry
        valid_parents = [pid for pid in parents_from_db if pid in models]
        
        if valid_parents:
            logger.warning("Model %s: Using parents from DynamoDB: %s (filtered from %d to %d valid)", 
                         model_id, valid_parents, len(parents_from_db), len(valid_parents))
            return valid_parents
        else:
            logger.warning("Model %s: DynamoDB parents field has %d entries, but none exist in models registry", 
                         model_id, len(parents_from_db))
            if len(parents_from_db) > 0:
                logger.warning("Model %s: Invalid parent IDs (not in registry): %s", 
                             model_id, [pid for pid in parents_from_db if pid not in models])
    
    # Best-effort fallback: Try S3 config.json (non-blocking)
    # This is optional and should not prevent edge creation if DynamoDB parents exist
    try:
        if model_id not in config_cache:
            config_cache[model_id] = load_config_json_from_s3(item)
        
        cfg = config_cache[model_id]
        if cfg is None:
            # S3 fallback failed, but that's okay - DynamoDB is authoritative
            return []
        
        # Extract parent name from config.json
        pname, keys_checked = _extract_parent_name_from_config(cfg)
        
        if not pname:
            # No parent key found in config.json, but that's okay
            return []
        
        # Try multiple lookup strategies for config.json parent name
        pid = None
        
        # Strategy 1: Exact lookup
        pid = name_to_id.get(pname)
        if pid and pid in models:
            logger.warning("Model %s: Parent resolved from config.json: '%s' -> %s (exact match)", 
                          model_id, pname, pid)
            return [pid]
        
        # Strategy 2: Lowercase lookup
        pid = name_to_id.get(pname.lower())
        if pid and pid in models:
            logger.warning("Model %s: Parent resolved from config.json: '%s' -> %s (lowercase match)", 
                          model_id, pname, pid)
            return [pid]
        
        # Strategy 3: Stem lookup (for cases where pname includes .zip or extension)
        stem = _get_filename_stem(pname)
        if stem and stem != pname:
            pid = name_to_id.get(stem)
            if pid and pid in models:
                logger.warning("Model %s: Parent resolved from config.json: '%s' -> %s (stem match: '%s')", 
                              model_id, pname, pid, stem)
                return [pid]
            
            # Try lowercase stem
            pid = name_to_id.get(stem.lower())
            if pid and pid in models:
                logger.warning("Model %s: Parent resolved from config.json: '%s' -> %s (lowercase stem match: '%s')", 
                              model_id, pname, pid, stem)
                return [pid]
        
        # Strategy 4: HuggingFace-style tail extraction
        hf_tail = _extract_hf_model_tail(pname)
        if hf_tail:
            pid = name_to_id.get(hf_tail)
            if pid and pid in models:
                logger.warning("Model %s: Parent resolved from config.json: '%s' -> %s (HF tail match: '%s')", 
                              model_id, pname, pid, hf_tail)
                return [pid]
            
            pid = name_to_id.get(hf_tail.lower())
            if pid and pid in models:
                logger.warning("Model %s: Parent resolved from config.json: '%s' -> %s (lowercase HF tail match: '%s')", 
                              model_id, pname, pid, hf_tail)
                return [pid]
        
        # Config.json parent name could not be resolved
        available_keys = sorted(name_to_id.keys())[:10]
        logger.warning("Model %s: Parent name '%s' from config.json not found in registry. "
                      "Available keys (first 10): %s", model_id, pname, available_keys)
    
    except Exception as e:
        # S3/config.json errors should not block lineage if DynamoDB parents exist
        # Log but don't fail
        logger.warning("Model %s: Error in S3 config.json fallback (non-blocking): %s", model_id, e)
    
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
    
    # Phase 1: Discover ancestors by following parent links
    ancestors_visited: Set[str] = set()
    queue = deque([(start_id, 0)])
    
    while queue:
        current_id, depth = queue.popleft()
        
        if current_id in ancestors_visited or depth > max_depth:
            continue
        
        ancestors_visited.add(current_id)
        
        if current_id not in models:
            logger.warning("Model %s not found in models dict", current_id)
            continue
        
        item = models[current_id]
        
        try:
            parent_ids = _choose_parent_ids(item, models, name_to_id, config_cache)
        except Exception as e:
            logger.error("Error choosing parent IDs for %s: %s", current_id, e, exc_info=True)
            continue
        
        if parent_ids:
            parents_map[current_id] = parent_ids
            for pid in parent_ids:
                children_map.setdefault(pid, []).append(current_id)
                if pid not in ancestors_visited and depth < max_depth:
                    queue.append((pid, depth + 1))
    
    # Phase 2: Discover descendants (children)
    descendants_visited: Set[str] = set()
    queue = deque([(start_id, 0)])
    
    while queue:
        current_id, depth = queue.popleft()
        
        if current_id in descendants_visited or depth > max_depth:
            continue
        
        descendants_visited.add(current_id)
        
        for child_id, item in models.items():
            if child_id in descendants_visited:
                continue
            
            try:
                parent_ids = _choose_parent_ids(item, models, name_to_id, config_cache)
                
                if current_id in parent_ids:
                    if child_id not in parents_map:
                        parents_map[child_id] = []
                    if current_id not in parents_map[child_id]:
                        parents_map[child_id].append(current_id)
                    children_map.setdefault(current_id, []).append(child_id)
                    
                    if child_id not in descendants_visited and depth < max_depth:
                        queue.append((child_id, depth + 1))
            except Exception as e:
                logger.error("Error checking parent relationship for child %s: %s", child_id, e, exc_info=True)
                continue
    
    logger.info("Built parent/child maps: %d models with parents, %d models with children", 
               len(parents_map), len(children_map))
    
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

        parent_ids = parents_map.get(cur, [])
        
        for pid in parent_ids:
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

        child_ids = children_map.get(cur, [])
        
        for cid in child_ids:
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

    nodes = []
    for nid in sorted(node_ids):
        item = models.get(nid, {})
        node_name = _display_name(item, nid)
        nodes.append({
            "artifact_id": nid,
            "name": node_name,
            "source": "config_json",
        })
    
    edges = []
    edge_seen: Set[Tuple[str, str]] = set()
    for child_id in node_ids:
        parent_ids_for_child = parents_map.get(child_id, [])
        for parent_id in parent_ids_for_child:
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
    """
    Retrieve the lineage graph for a model artifact.
    Returns ancestors, the model itself, and descendants based on config.json analysis.
    """
    logger.info("Lineage request for artifact_id: %s", artifact_id)
    
    # NOTE: No auth enforcement for baseline
    
    if not _valid_id(artifact_id):
        logger.error("Invalid artifact_id format: %s", artifact_id)
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    try:
        models = _scan_all_models()
    except Exception as e:
        logger.error("Failed to scan DynamoDB for models: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")
    
    if artifact_id not in models:
        logger.warning("Artifact not found: artifact_id=%s", artifact_id)
        abort(404, description="Artifact does not exist.")
    
    try:
        parents_map, children_map = _build_parent_child_maps_lazy(models, artifact_id, max_depth=10)
    except Exception as e:
        logger.error("Failed to build parent/child maps: %s", e, exc_info=True)
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    try:
        graph = _build_lineage_graph(artifact_id, models, parents_map, children_map)
        logger.info("Lineage graph built: %d nodes, %d edges", len(graph.get("nodes", [])), len(graph.get("edges", [])))
        
        if len(graph.get("edges", [])) == 0:
            logger.warning("Lineage graph has no edges - no parent/child relationships found")
    except Exception as e:
        logger.error("Failed to build lineage graph: %s", e, exc_info=True)
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    return jsonify(graph), 200
