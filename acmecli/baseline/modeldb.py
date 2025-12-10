import logging
from typing import Dict, List, Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)

# Re-use the same table name as download/cost/reset
MODEL_TABLE = DYNAMODB.Table("artifact")

PHASE1_FIELDS = [
    "size_score",
    "license_score",
    "rampup_score",
    "bus_factor",
    "dataset_and_code",
    "dataset_quality",
    "code_quality",
    "perf_claims",
    "net_score",
]


def get_model_item(model_id: str) -> Dict[str, Any]:
    """
    Load a single model item from DynamoDB.
    Assumes partition key 'id' and attribute 'artifact_type' == 'model'.
    """
    try:
        resp = MODEL_TABLE.get_item(Key={"id": model_id})
    except ClientError as e:
        logger.error("DynamoDB get_item failed: %s", e, exc_info=True)
        raise

    item = resp.get("Item")
    if not item or item.get("artifact_type") != "model":
        return None
    return item


def scan_models() -> List[Dict[str, Any]]:
    """
    Return ALL model items (artifact_type == 'model') from the table.
    Simple scan with pagination (fine for project scale).
    """
    items: List[Dict[str, Any]] = []
    scan_kwargs: Dict[str, Any] = {}

    while True:
        resp = MODEL_TABLE.scan(**scan_kwargs)
        batch = resp.get("Items", [])
        for it in batch:
            if it.get("artifact_type") == "model":
                items.append(it)

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_kwargs["ExclusiveStartKey"] = last_key

    return items


def compute_netscore(item: Dict[str, Any]) -> float:
    """
    Compute NetScore from Phase 1 sub-scores stored on the item.
    """
    m = item
    return (
        0.20 * float(m.get("license_score", 0.0)) +
        0.20 * float(m.get("dataset_and_code", 0.0)) +
        0.15 * float(m.get("code_quality", 0.0)) +
        0.15 * float(m.get("rampup_score", 0.0)) +
        0.10 * float(m.get("bus_factor", 0.0)) +
        0.10 * float(m.get("perf_claims", 0.0)) +
        0.05 * float(m.get("dataset_quality", 0.0)) +
        0.05 * float(m.get("size_score", 0.0))
    )


def compute_treescore(item: Dict[str, Any]) -> float | None:
    """
    Treescore = average of parents' net_score values, if any.
    Parents are stored as a list of model_ids in 'parents'.
    """
    parents = item.get("parents") or []
    if not parents:
        return None

    scores = []
    for pid in parents:
        try:
            parent = get_model_item(pid)
        except ClientError:
            parent = None
        if parent and "net_score" in parent:
            scores.append(float(parent["net_score"]))

    if not scores:
        return None
    return sum(scores) / len(scores)


def put_model_from_phase1(obj: Dict[str, Any]) -> None:
    """
    Helper for seeding from Phase 1 JSON.

    Expected keys in obj:
      id, version,
      size_score, license_score, rampup_score, bus_factor,
      dataset_and_code, dataset_quality, code_quality, perf_claims,
      net_score (or will be computed if missing)

    You can adjust the field names to match your actual Phase 1 JSON.
    """
    model_id = obj["id"]
    version = obj.get("version", "0.0.0")

    # If JSON does not include net_score, compute it here
    if "net_score" not in obj:
        obj["net_score"] = compute_netscore(obj)

    item: Dict[str, Any] = {
        "id": model_id,
        "artifact_type": "model",
        "version": version,
        "name": model_id,

        # Phase 1 scores
        "size_score": float(obj["size_score"]),
        "license_score": float(obj["license_score"]),
        "rampup_score": float(obj["rampup_score"]),
        "bus_factor": float(obj["bus_factor"]),
        "dataset_and_code": float(obj["dataset_and_code"]),
        "dataset_quality": float(obj["dataset_quality"]),
        "code_quality": float(obj["code_quality"]),
        "perf_claims": float(obj["perf_claims"]),
        "net_score": float(obj["net_score"]),

        # Phase 2 extras
        "reproducibility": float(obj.get("reproducibility", 0.0)),
        "reviewedness": float(obj.get("reviewedness", -1.0)),
        "parents": obj.get("parents", []),
    }

    MODEL_TABLE.put_item(Item=item)
