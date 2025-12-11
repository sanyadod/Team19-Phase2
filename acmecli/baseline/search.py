from flask import Flask, request, jsonify, abort
import re

from acmecli.baseline.modeldb import scan_models

app = Flask(__name__)


# ---------- Auth helper ----------

def _require_auth() -> str:
    token = request.headers.get("X-Authorization")
    if not token or not token.strip():
        abort(403, description="Authentication failed due to invalid or missing AuthenticationToken.")
    return token


# ---------- Simple semantic version handling ----------

def _parse_version(vstr: str) -> tuple[int, int, int]:
    parts = vstr.strip().split(".")
    nums = [int(p) for p in parts if p != ""]
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums[:3])


def _cmp_ver(a: tuple[int, int, int], b: tuple[int, int, int]) -> int:
    return (a > b) - (a < b)


def _match_rel(v: tuple[int, int, int], op: str, tgt: tuple[int, int, int]) -> bool:
    c = _cmp_ver(v, tgt)
    if op in ("", "=", "=="):
        return c == 0
    if op == ">":
        return c > 0
    if op == "<":
        return c < 0
    if op == ">=":
        return c >= 0
    if op == "<=":
        return c <= 0
    raise ValueError(f"Unsupported operator: {op}")


def _matches_version_spec(version: str, spec: str) -> bool:
    """
    Support:
      ^1.2.3   -> >=1.2.3 and <2.0.0
      ~1.4     -> >=1.4.0 and <1.5.0
      >=1.4.0,<2.0.0   (comma-separated relational constraints)
    """
    spec = spec.strip()
    if not spec:
        return True

    v = _parse_version(version)

    # caret
    if spec.startswith("^"):
        base = _parse_version(spec[1:])
        if _cmp_ver(v, base) < 0:
            return False
        return v[0] == base[0]

    # tilde
    if spec.startswith("~"):
        base = _parse_version(spec[1:])
        if _cmp_ver(v, base) < 0:
            return False
        return v[0] == base[0] and v[1] == base[1]

    # comma-separated relational constraints
    parts = [s.strip() for s in spec.split(",") if s.strip()]
    for part in parts:
        m = re.match(r"(<=|>=|<|>|==|=)?\s*(\d+(?:\.\d+){0,2})$", part)
        if not m:
            raise ValueError(f"Invalid version constraint: {part}")
        op, vstr = m.groups()
        op = op or "=="
        tgt = _parse_version(vstr)
        if not _match_rel(v, op, tgt):
            return False

    return True


# ---------- /search ----------

@app.get("/search")
def search_models():
    """
    /search?q=regex&version=spec

    - q: regex applied to model 'name' or 'card_text'
    - version: semantic version spec (^, ~, bounded ranges)
    """
    _require_auth()

    regex_q = request.args.get("q")
    version_spec = request.args.get("version")

    # load all models from DynamoDB
    models = scan_models()

    # 1) regex filter
    if regex_q:
        try:
            pattern = re.compile(regex_q, re.IGNORECASE)
        except re.error as e:
            abort(400, description=f"Invalid regex: {e}")

        models = [
            m for m in models
            if pattern.search(m.get("name", "") or "") or
               pattern.search(m.get("card_text", "") or "")
        ]

    # 2) version filter
    if version_spec:
        filtered = []
        for m in models:
            v = m.get("version")
            if not v:
                continue
            try:
                if _matches_version_spec(v, version_spec):
                    filtered.append(m)
            except ValueError as e:
                abort(400, description=str(e))
        models = filtered

    # minimal response
    results = []
    for m in models:
        results.append({
            "model_id": m["id"],
            "name": m.get("name"),
            "version": m.get("version"),
            "net_score": float(m.get("net_score", 0.0)),
        })

    return jsonify(results), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)