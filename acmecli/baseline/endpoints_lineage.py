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
    logger.debug("Starting DynamoDB scan for all models...")
    try:
        resp = META_TABLE.scan()
        items = resp.get("Items", [])
        logger.debug("Initial scan returned %d items", len(items))
        
        page_count = 1
        while "LastEvaluatedKey" in resp:
            logger.debug("Fetching next page of results (page %d)...", page_count + 1)
            resp = META_TABLE.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
            new_items = resp.get("Items", [])
            items.extend(new_items)
            logger.debug("Page %d returned %d items (total: %d)", page_count + 1, len(new_items), len(items))
            page_count += 1

        models: Dict[str, Dict[str, Any]] = {}
        for it in items:
            artifact_type = it.get("artifact_type")
            if artifact_type == "model":
                # Normalize ID (handle DynamoDB types like Decimal)
                mid = _normalize_id(it.get("id"))
                if mid:
                    models[mid] = it
                    logger.debug("Added model: id=%s, name=%s", mid, it.get("name", "unknown"))
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
    
    logger.debug("load_config_json_from_s3: model_id=%s, bucket=%s, key=%s", model_id, bucket, key)
    
    if not bucket or not key:
        logger.warning("Model %s: Missing s3_bucket or s3_key (bucket=%s, key=%s)", model_id, bucket, key)
        return None
    
    try:
        # Download the file from S3
        logger.debug("Model %s: Downloading file from S3: s3://%s/%s", model_id, bucket, key)
        response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
        file_data = response["Body"].read()
        file_size = len(file_data)
        logger.debug("Model %s: Downloaded %d bytes from S3", model_id, file_size)
        
        # Check if file is actually a zip file by checking magic bytes
        if not file_data.startswith(b'PK\x03\x04') and not file_data.startswith(b'PK\x05\x06'):
            # Check if it's HTML (common error case)
            if file_data.startswith(b'<!doctype') or file_data.startswith(b'<html') or file_data.startswith(b'<HTML'):
                logger.warning("Model %s: File s3://%s/%s appears to be HTML instead of zip (first 100 chars: %s). "
                              "This may indicate an upload issue or S3 redirect error.", 
                              model_id, bucket, key, file_data[:100].decode('utf-8', errors='ignore'))
            else:
                logger.warning("Model %s: File s3://%s/%s is not a zip file (missing PK header, first bytes: %s)", 
                              model_id, bucket, key, file_data[:10] if len(file_data) >= 10 else file_data)
            return None
        
        # Open as zipfile and search for config.json
        logger.debug("Model %s: Opening zip file and searching for config.json...", model_id)
        with zipfile.ZipFile(io.BytesIO(file_data)) as zf:
            zip_files = zf.namelist()
            logger.debug("Model %s: Zip contains %d files", model_id, len(zip_files))
            
            config_path = None
            # Search for config.json in root or nested folders
            for name in zip_files:
                # Normalize path separators and check if it's config.json
                normalized = name.replace("\\", "/").strip("/")
                if normalized == "config.json" or normalized.endswith("/config.json"):
                    config_path = name
                    logger.debug("Model %s: Found config.json at path: %s", model_id, config_path)
                    break
            
            if not config_path:
                logger.warning("Model %s: Missing config.json in zip s3://%s/%s (searched %d files)", 
                              model_id, bucket, key, len(zip_files))
                logger.debug("Model %s: Sample zip file names: %s", model_id, zip_files[:10] if len(zip_files) > 10 else zip_files)
                return None
            
            # Read and parse config.json
            try:
                logger.debug("Model %s: Reading config.json from zip path: %s", model_id, config_path)
                config_content = zf.read(config_path)
                config_size = len(config_content)
                logger.debug("Model %s: Read %d bytes from config.json", model_id, config_size)
                
                config_dict = json.loads(config_content.decode("utf-8"))
                logger.info("Model %s: Successfully loaded and parsed config.json from path '%s' in zip s3://%s/%s", 
                          model_id, config_path, bucket, key)
                logger.debug("Model %s: Config.json keys: %s", model_id, list(config_dict.keys())[:20])
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

def _extract_parent_name_from_config(cfg: Dict[str, Any]) -> Optional[str]:
    """
    Extract parent/base-model name from config.json.
    Prefer conservative keys that indicate a base model, NOT self-reference.
    Do NOT use _name_or_path or name_or_path as they usually refer to self.
    """
    logger.debug("_extract_parent_name_from_config: Starting extraction")
    
    if not isinstance(cfg, dict):
        logger.debug("_extract_parent_name_from_config: Config is not a dict (type: %s)", type(cfg))
        return None

    # Conservative keys that indicate a parent/base model
    # Order matters: most specific first
    keys_to_check = ["base_model_name_or_path", "pretrained_model_name_or_path", "base_model"]
    logger.debug("_extract_parent_name_from_config: Checking keys: %s", keys_to_check)
    
    for key in keys_to_check:
        val = cfg.get(key)
        logger.debug("_extract_parent_name_from_config: Key '%s' = %s (type: %s)", key, val, type(val))
        if isinstance(val, str) and val.strip():
            parent_name = val.strip()
            logger.info("_extract_parent_name_from_config: Extracted parent name '%s' from config key '%s'", 
                       parent_name, key)
            return parent_name

    logger.debug("_extract_parent_name_from_config: No parent name found in config keys: %s", keys_to_check)
    logger.debug("_extract_parent_name_from_config: Available config keys: %s", list(cfg.keys())[:20])
    return None

def _build_name_to_id_map(models: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Map human-readable names to ids so config_json fallback can resolve parent names.
    """
    logger.debug("_build_name_to_id_map: Building name-to-id map from %d models", len(models))
    m: Dict[str, str] = {}
    duplicates = []
    
    for mid, item in models.items():
        nm = _display_name(item, mid).strip()
        if nm:
            if nm in m:
                duplicates.append((nm, mid, m[nm]))
                logger.warning("_build_name_to_id_map: Duplicate name '%s' found: id1=%s, id2=%s", nm, m[nm], mid)
            m[nm] = mid
            logger.debug("_build_name_to_id_map: Mapped name '%s' -> id '%s'", nm, mid)
    
    if duplicates:
        logger.warning("_build_name_to_id_map: Found %d duplicate names (last id wins)", len(duplicates))
    
    logger.info("_build_name_to_id_map: Built map with %d entries", len(m))
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
    Only uses config.json from S3 zip (name -> id).
    Returns only parents that exist in `models`.
    """
    model_id = str(item.get("id", ""))
    logger.debug("_choose_parent_ids: Starting for model_id=%s", model_id)
    
    # Load config.json from S3 zip
    logger.debug("Model %s: Loading config.json from S3...", model_id)
    # Check cache first
    if model_id not in config_cache:
        logger.debug("Model %s: Config.json not in cache, loading from S3...", model_id)
        config_cache[model_id] = load_config_json_from_s3(item)
    else:
        logger.debug("Model %s: Using cached config.json result", model_id)
    
    cfg = config_cache[model_id]
    if cfg:
        logger.debug("Model %s: Config.json loaded successfully, extracting parent name...", model_id)
        pname = _extract_parent_name_from_config(cfg)
        if pname:
            logger.info("Model %s: Extracted parent model name '%s' from config.json", model_id, pname)
            logger.debug("Model %s: Looking up parent name '%s' in name_to_id map (map has %d entries)", 
                        model_id, pname, len(name_to_id))
            pid = name_to_id.get(pname)
            if pid and pid in models:
                logger.info("Model %s: Resolved parent artifact_id '%s' for parent name '%s'", model_id, pid, pname)
                return [pid]
            else:
                logger.warning("Model %s: Parent name '%s' not found in registry (resolved to: %s, exists in models: %s)", 
                              model_id, pname, pid, pid in models if pid else False)
                if pname and pname not in name_to_id:
                    logger.debug("Model %s: Parent name '%s' not in name_to_id map. Available names: %s", 
                               model_id, pname, list(name_to_id.keys())[:10])
        else:
            logger.debug("Model %s: No parent name extracted from config.json", model_id)
    else:
        logger.debug("Model %s: Config.json not available or failed to load (file may not be a valid zip)", model_id)

    logger.info("Model %s: No parents found (config.json missing or no parent reference)", model_id)
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
    logger.debug("_build_parent_child_maps_lazy: Starting with start_id=%s, max_depth=%d, total_models=%d", 
               start_id, max_depth, len(models))
    
    name_to_id = _build_name_to_id_map(models)
    logger.debug("_build_parent_child_maps_lazy: Built name_to_id map with %d entries", len(name_to_id))
    
    config_cache: Dict[str, Optional[Dict[str, Any]]] = {}
    parents_map: Dict[str, List[str]] = {}
    children_map: Dict[str, List[str]] = {}
    
    # BFS to discover ancestors (parents) - only load config.json as needed
    ancestors_visited: Set[str] = set()
    queue = deque([(start_id, 0)])  # (model_id, depth)
    
    logger.info("_build_parent_child_maps_lazy: Phase 1 - Building parent/child maps lazily starting from %s (max_depth=%d)", 
               start_id, max_depth)
    
    # Phase 1: Discover ancestors by following parent links
    phase1_iterations = 0
    while queue:
        current_id, depth = queue.popleft()
        phase1_iterations += 1
        
        if current_id in ancestors_visited:
            logger.debug("_build_parent_child_maps_lazy: Phase 1 - Skipping already visited: %s", current_id)
            continue
        
        if depth > max_depth:
            logger.debug("_build_parent_child_maps_lazy: Phase 1 - Max depth reached for %s (depth=%d > max_depth=%d)", 
                        current_id, depth, max_depth)
            continue
        
        ancestors_visited.add(current_id)
        logger.debug("_build_parent_child_maps_lazy: Phase 1 - Processing: id=%s, depth=%d", current_id, depth)
        
        if current_id not in models:
            logger.warning("_build_parent_child_maps_lazy: Phase 1 - Model %s not found in models dict", current_id)
            continue
        
        item = models[current_id]
        
        # Load config.json for this node (cached)
        try:
            parent_ids = _choose_parent_ids(item, models, name_to_id, config_cache)
        except Exception as e:
            logger.error("_build_parent_child_maps_lazy: Phase 1 - Error choosing parent IDs for %s: %s", 
                        current_id, e, exc_info=True)
            continue
        
        if parent_ids:
            parents_map[current_id] = parent_ids
            logger.debug("_build_parent_child_maps_lazy: Phase 1 - Model %s has %d parents: %s", 
                        current_id, len(parent_ids), parent_ids)
            for pid in parent_ids:
                children_map.setdefault(pid, []).append(current_id)
                # Continue BFS for parents
                if pid not in ancestors_visited and depth < max_depth:
                    queue.append((pid, depth + 1))
                    logger.debug("_build_parent_child_maps_lazy: Phase 1 - Queued parent %s (depth=%d)", pid, depth + 1)
        else:
            logger.debug("_build_parent_child_maps_lazy: Phase 1 - Model %s has no parents", current_id)
    
    logger.info("_build_parent_child_maps_lazy: Phase 1 completed: visited %d models in %d iterations, "
               "found %d models with parents", len(ancestors_visited), phase1_iterations, len(parents_map))
    
    # Phase 2: Discover descendants (children) - check all models for children of visited nodes
    # Use BFS starting from start_id to find all descendants
    descendants_visited: Set[str] = set()
    queue = deque([(start_id, 0)])
    
    logger.info("_build_parent_child_maps_lazy: Phase 2 - Discovering descendants starting from %s", start_id)
    
    phase2_iterations = 0
    while queue:
        current_id, depth = queue.popleft()
        phase2_iterations += 1
        
        if current_id in descendants_visited:
            logger.debug("_build_parent_child_maps_lazy: Phase 2 - Skipping already visited: %s", current_id)
            continue
        
        if depth > max_depth:
            logger.debug("_build_parent_child_maps_lazy: Phase 2 - Max depth reached for %s (depth=%d > max_depth=%d)", 
                        current_id, depth, max_depth)
            continue
        
        descendants_visited.add(current_id)
        logger.debug("_build_parent_child_maps_lazy: Phase 2 - Processing: id=%s, depth=%d, checking %d models for children", 
                    current_id, depth, len(models))
        
        # Check all models to see if they have current_id as a parent
        children_found = 0
        for child_id, item in models.items():
            if child_id in descendants_visited:
                continue
            
            try:
                # Load config.json if needed to check parent relationship
                parent_ids = _choose_parent_ids(item, models, name_to_id, config_cache)
                
                # If current_id is a parent of this child
                if current_id in parent_ids:
                    if child_id not in parents_map:
                        parents_map[child_id] = []
                    if current_id not in parents_map[child_id]:
                        parents_map[child_id].append(current_id)
                    children_map.setdefault(current_id, []).append(child_id)
                    children_found += 1
                    logger.debug("_build_parent_child_maps_lazy: Phase 2 - Found child: %s -> %s", 
                               current_id, child_id)
                    # Continue BFS for this child
                    if child_id not in descendants_visited and depth < max_depth:
                        queue.append((child_id, depth + 1))
                        logger.debug("_build_parent_child_maps_lazy: Phase 2 - Queued child %s (depth=%d)", 
                                   child_id, depth + 1)
            except Exception as e:
                logger.error("_build_parent_child_maps_lazy: Phase 2 - Error checking parent relationship "
                           "for child %s: %s", child_id, e, exc_info=True)
                continue
        
        if children_found > 0:
            logger.debug("_build_parent_child_maps_lazy: Phase 2 - Model %s has %d children", 
                        current_id, children_found)
    
    logger.info("_build_parent_child_maps_lazy: Phase 2 completed: visited %d models in %d iterations", 
               len(descendants_visited), phase2_iterations)
    logger.info("_build_parent_child_maps_lazy: Final maps: %d models with parents, %d models with children "
               "(loaded %d config.json files)", len(parents_map), len(children_map), len(config_cache))
    
    return parents_map, children_map

def _bfs_ancestors(start_id: str, parents_map: Dict[str, List[str]]) -> Set[str]:
    logger.debug("_bfs_ancestors: Starting BFS from %s", start_id)
    ancestors: Set[str] = set()
    q = deque([start_id])
    seen: Set[str] = set()

    iterations = 0
    while q:
        cur = q.popleft()
        iterations += 1
        
        if cur in seen:
            continue
        seen.add(cur)
        logger.debug("_bfs_ancestors: Processing node %s", cur)

        parent_ids = parents_map.get(cur, [])
        logger.debug("_bfs_ancestors: Node %s has %d parents: %s", cur, len(parent_ids), parent_ids)
        
        for pid in parent_ids:
            if pid not in ancestors:
                ancestors.add(pid)
                q.append(pid)
                logger.debug("_bfs_ancestors: Added ancestor %s, queued for processing", pid)
    
    logger.debug("_bfs_ancestors: Completed in %d iterations, found %d ancestors", iterations, len(ancestors))
    return ancestors

def _bfs_descendants(start_id: str, children_map: Dict[str, List[str]]) -> Set[str]:
    logger.debug("_bfs_descendants: Starting BFS from %s", start_id)
    descendants: Set[str] = set()
    q = deque([start_id])
    seen: Set[str] = set()

    iterations = 0
    while q:
        cur = q.popleft()
        iterations += 1
        
        if cur in seen:
            continue
        seen.add(cur)
        logger.debug("_bfs_descendants: Processing node %s", cur)

        child_ids = children_map.get(cur, [])
        logger.debug("_bfs_descendants: Node %s has %d children: %s", cur, len(child_ids), child_ids)
        
        for cid in child_ids:
            if cid not in descendants:
                descendants.add(cid)
                q.append(cid)
                logger.debug("_bfs_descendants: Added descendant %s, queued for processing", cid)
    
    logger.debug("_bfs_descendants: Completed in %d iterations, found %d descendants", iterations, len(descendants))
    return descendants

def _build_lineage_graph(start_id: str,
                         models: Dict[str, Dict[str, Any]],
                         parents_map: Dict[str, List[str]],
                         children_map: Dict[str, List[str]]) -> Dict[str, Any]:
    logger.debug("_build_lineage_graph: Building graph for start_id=%s", start_id)
    
    logger.debug("_build_lineage_graph: Computing ancestors...")
    ancestors = _bfs_ancestors(start_id, parents_map)
    logger.debug("_build_lineage_graph: Found %d ancestors: %s", len(ancestors), list(ancestors))
    
    logger.debug("_build_lineage_graph: Computing descendants...")
    descendants = _bfs_descendants(start_id, children_map)
    logger.debug("_build_lineage_graph: Found %d descendants: %s", len(descendants), list(descendants))
    
    node_ids = {start_id} | ancestors | descendants
    logger.info("_build_lineage_graph: Total nodes in graph: %d (start: 1, ancestors: %d, descendants: %d)", 
               len(node_ids), len(ancestors), len(descendants))

    # Nodes
    logger.debug("_build_lineage_graph: Building node list...")
    nodes = []
    for nid in sorted(node_ids):
        item = models.get(nid, {})
        node_name = _display_name(item, nid)
        nodes.append({
            "artifact_id": nid,
            "name": node_name,
            "source": "config_json",
        })
        logger.debug("_build_lineage_graph: Added node: artifact_id=%s, name=%s", nid, node_name)
    
    logger.info("_build_lineage_graph: Created %d nodes", len(nodes))

    # Edges: parent -> child, relationship "base_model"
    logger.debug("_build_lineage_graph: Building edge list...")
    edges = []
    edge_seen: Set[Tuple[str, str]] = set()
    for child_id in node_ids:
        parent_ids_for_child = parents_map.get(child_id, [])
        logger.debug("_build_lineage_graph: Child %s has %d parents in parents_map", 
                    child_id, len(parent_ids_for_child))
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
                    logger.debug("_build_lineage_graph: Added edge: %s -> %s (base_model)", parent_id, child_id)
                else:
                    logger.debug("_build_lineage_graph: Skipped duplicate edge: %s -> %s", parent_id, child_id)
            else:
                logger.debug("_build_lineage_graph: Parent %s not in node_ids, skipping edge to %s", 
                            parent_id, child_id)
    
    logger.info("_build_lineage_graph: Created %d edges", len(edges))
    logger.debug("_build_lineage_graph: Graph structure: nodes=%d, edges=%d", len(nodes), len(edges))

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
    logger.info("=== LINEAGE REQUEST START ===")
    logger.info("Requested artifact_id: %s", artifact_id)
    
    # NOTE: No auth enforcement for baseline
    
    # Validate artifact_id format
    logger.debug("Validating artifact_id format: %s", artifact_id)
    if not _valid_id(artifact_id):
        logger.error("Invalid artifact_id format: %s", artifact_id)
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    logger.info("Artifact_id format validation passed")
    
    # Scan DynamoDB once to get all models
    logger.info("Scanning DynamoDB for all models...")
    try:
        models = _scan_all_models()
        logger.info("DynamoDB scan completed: found %d total models", len(models))
    except Exception as e:
        logger.error("Failed to scan DynamoDB for models: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")
    
    # Check if artifact exists
    logger.debug("Checking if artifact_id %s exists in models", artifact_id)
    if artifact_id not in models:
        logger.warning("Artifact not found: artifact_id=%s (checked %d models)", artifact_id, len(models))
        abort(404, description="Artifact does not exist.")
    
    logger.info("Artifact found: artifact_id=%s, name=%s", artifact_id, _display_name(models[artifact_id], artifact_id))
    
    # Log available model IDs for debugging
    all_model_ids = sorted(models.keys())
    logger.debug("Available model IDs in registry: %s", all_model_ids[:20] if len(all_model_ids) > 20 else all_model_ids)
    
    # Build parent/child maps lazily (only load config.json for visited nodes)
    logger.info("Building parent/child relationship maps...")
    try:
        parents_map, children_map = _build_parent_child_maps_lazy(models, artifact_id, max_depth=10)
        logger.info("Parent/child maps built: %d models with parents, %d models with children", 
                   len(parents_map), len(children_map))
        
        # Log detailed summary of relationships found
        if len(parents_map) > 0:
            logger.info("Models with parents: %s", list(parents_map.keys())[:10])
        if len(children_map) > 0:
            logger.info("Models with children: %s", list(children_map.keys())[:10])
    except Exception as e:
        logger.error("Failed to build parent/child maps: %s", e, exc_info=True)
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    # Build lineage graph
    logger.info("Building lineage graph structure...")
    try:
        graph = _build_lineage_graph(artifact_id, models, parents_map, children_map)
        logger.info("Lineage graph built: %d nodes, %d edges", len(graph.get("nodes", [])), len(graph.get("edges", [])))
        logger.debug("Graph nodes: %s", [n.get("artifact_id") for n in graph.get("nodes", [])])
        logger.debug("Graph edges: %s", [(e.get("from_node_artifact_id"), e.get("to_node_artifact_id")) 
                                         for e in graph.get("edges", [])])
        
        # Summary log
        if len(graph.get("edges", [])) == 0:
            logger.warning("Lineage graph has no edges - no parent/child relationships found. "
                          "This may indicate: 1) Parent relationships not stored in DynamoDB metadata, "
                          "2) S3 files are not valid zip files (check upload process), "
                          "3) config.json files missing or don't contain parent model references")
        else:
            logger.info("Lineage graph successfully built with %d relationships", len(graph.get("edges", [])))
    except Exception as e:
        logger.error("Failed to build lineage graph: %s", e, exc_info=True)
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")
    
    logger.info("=== LINEAGE REQUEST SUCCESS ===")
    return jsonify(graph), 200
