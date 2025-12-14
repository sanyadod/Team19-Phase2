from flask import Flask, jsonify, abort, request
import boto3
from botocore.exceptions import ClientError
import logging
import json
from collections import deque
from typing import Dict, List, Any, Optional, Set, Tuple

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")


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

def _parse_config_json(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    cfg = item.get("config_json")
    if cfg is None:
        return None
    if isinstance(cfg, dict):
        return cfg
    if isinstance(cfg, str):
        try:
            return json.loads(cfg)
        except Exception:
            return None
    return None

def _extract_parent_name_from_config(cfg: Dict[str, Any]) -> Optional[str]:
    """
    LAST resort: attempt to extract a parent/base-model reference from config.json.
    These keys vary widely; keep it conservative.
    """
    if not isinstance(cfg, dict):
        return None

    # Most common HF-ish fields (may or may not mean "parent")
    for key in ["base_model_name_or_path", "pretrained_model_name_or_path", "_name_or_path", "name_or_path"]:
        val = cfg.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()

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

def _choose_parent_ids(item: Dict[str, Any], models: Dict[str, Dict[str, Any]], name_to_id: Dict[str, str]) -> List[str]:
    """
    Determine parent model IDs for a given model item.
    Priority:
      1) direct stored parent id fields (single)
      2) stored list fields (parents / parent_uuids / parent_ids)
      3) config_json fallback (name -> id)
    Returns only parents that exist in `models`.
    """
    # 1) single stored parent id fields 
    single_keys = ["parent_uuid", "parent_id", "parent_artifact_id", "base_model_id"]
    for k in single_keys:
        if k in item and item[k]:
            pid = str(item[k]).strip()
            if pid in models:
                return [pid]

    # 2) list stored fields
    list_keys = ["parents", "parent_uuids", "parent_ids", "base_model_ids"]
    for k in list_keys:
        if k in item and item[k]:
            pids = [pid for pid in _as_list(item[k]) if pid in models]
            if pids:
                return pids

    # 3) config_json fallback (name -> id)
    cfg = _parse_config_json(item)
    if cfg:
        pname = _extract_parent_name_from_config(cfg)
        if pname:
            pid = name_to_id.get(pname)
            if pid and pid in models:
                return [pid]

    return []

def _build_parent_child_maps(models: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """
    parents_map[child_id] = [parent_ids]
    children_map[parent_id] = [child_ids]
    """
    name_to_id = _build_name_to_id_map(models)

    parents_map: Dict[str, List[str]] = {}
    children_map: Dict[str, List[str]] = {}

    for child_id, item in models.items():
        parent_ids = _choose_parent_ids(item, models, name_to_id)
        if not parent_ids:
            continue

        parents_map[child_id] = parent_ids
        for pid in parent_ids:
            children_map.setdefault(pid, []).append(child_id)

    logger.info("Built maps: %d models with parents, %d models with children",
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
    # NOTE: You said no auth; leaving it unenforced.

    if not _valid_id(artifact_id):
        abort(400, description="There is missing field(s) in the artifact_type or artifact_id or it is formed improperly, or is invalid.")

    models = _scan_all_models()
    if artifact_id not in models:
        abort(404, description="Artifact does not exist.")

    parents_map, children_map = _build_parent_child_maps(models)
    graph = _build_lineage_graph(artifact_id, models, parents_map, children_map)
    return jsonify(graph), 200
