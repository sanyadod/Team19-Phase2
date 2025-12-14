from flask import Flask, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging
from collections import deque
from typing import Dict, List, Any, Set, Tuple
from decimal import Decimal

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


def _normalize_id(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, Decimal):
        return str(v).strip()
    return str(v).strip()


def _display_name(item: Dict[str, Any], fallback_id: str) -> str:
    return str(item.get("name") or item.get("filename") or fallback_id)


def _as_list(x: Any) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        return [_normalize_id(v) for v in x if _normalize_id(v)]
    if isinstance(x, (set, frozenset)):
        return [_normalize_id(v) for v in x if _normalize_id(v)]
    if isinstance(x, str) and x.strip():
        return [_normalize_id(x)]
    if isinstance(x, Decimal):
        return [_normalize_id(x)]
    return []


def _scan_all_models() -> Dict[str, Dict[str, Any]]:
    try:
        resp = META_TABLE.scan()
        items = resp.get("Items", [])

        while "LastEvaluatedKey" in resp:
            resp = META_TABLE.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
            items.extend(resp.get("Items", []))

        models: Dict[str, Dict[str, Any]] = {}
        for it in items:
            if it.get("artifact_type") == "model":
                mid = _normalize_id(it.get("id"))
                if mid:
                    models[mid] = it

        return models
    except ClientError as e:
        logger.error("DynamoDB scan failed: %s", e, exc_info=True)
        abort(500, description="The artifact storage encountered an error.")


def _choose_parent_ids(item: Dict[str, Any], models: Dict[str, Dict[str, Any]]) -> List[str]:
    parents = _as_list(item.get("parents"))
    return [pid for pid in parents if pid in models]


def _build_parent_child_maps(models: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    parents_map: Dict[str, List[str]] = {}
    children_map: Dict[str, List[str]] = {}

    for child_id, item in models.items():
        parent_ids = _choose_parent_ids(item, models)
        if parent_ids:
            parents_map[child_id] = parent_ids
            for pid in parent_ids:
                children_map.setdefault(pid, []).append(child_id)

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

    nodes = []
    for nid in sorted(node_ids):
        item = models.get(nid, {})
        nodes.append({
            "artifact_id": nid,
            "name": _display_name(item, nid),
            "source": "registry_metadata",
        })

    edges = []
    seen_edges: Set[Tuple[str, str]] = set()
    for child_id in node_ids:
        for parent_id in parents_map.get(child_id, []):
            if parent_id in node_ids:
                key = (parent_id, child_id)
                if key not in seen_edges:
                    seen_edges.add(key)
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
    if not _valid_id(artifact_id):
        abort(400, description="The lineage graph cannot be computed because the artifact metadata is missing or malformed.")

    models = _scan_all_models()

    if artifact_id not in models:
        abort(404, description="Artifact does not exist.")

    parents_map, children_map = _build_parent_child_maps(models)
    graph = _build_lineage_graph(artifact_id, models, parents_map, children_map)

    return jsonify(graph), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=True)
