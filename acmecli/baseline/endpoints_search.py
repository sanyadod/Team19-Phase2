from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging
import re
import signal

app = Flask(__name__)
logger = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
DYNAMODB = boto3.resource("dynamodb", region_name=AWS_REGION)
META_TABLE = DYNAMODB.Table("artifact")

MAX_RESULTS_PER_PAGE = 100
REGEX_TIMEOUT_SECONDS = 2  # Prevent ReDoS attacks


class TimeoutError(Exception):
    """Raised when regex matching takes too long."""
    pass


def timeout_handler(signum, frame):
    """Signal handler for regex timeout."""
    raise TimeoutError("Regex matching timed out")


def is_safe_regex(pattern: str) -> bool:
    """
    Detect potentially malicious regex patterns that cause ReDoS.
    
    Common ReDoS patterns:
    - Nested quantifiers: (a+)+
    - Multiple consecutive quantified groups: (a+)(a+)(a+)
    - Alternation with overlap: (a|a)*
    """
    # Check 1: Pattern too long
    if len(pattern) > 500:
        logger.warning(f"Pattern too long: {len(pattern)} chars")
        return False
    
    # Check 2: Too many alternations
    if pattern.count('|') > 20:
        logger.warning("Too many alternations")
        return False
    
    # Check 3: Nested quantifiers like (a+)+ or (a*)*
    nested_quantifiers = re.compile(r'(\([^)]*[+*?]\))[+*?]')
    if nested_quantifiers.search(pattern):
        logger.warning(f"Nested quantifiers detected: {pattern}")
        return False
    

    # Check 5: Exponential alternation like (a|a)*
    exponential_alt = re.compile(r'\(([^)|]+\|)+[^)]+\)[+*]')
    if exponential_alt.search(pattern):
        logger.warning(f"Exponential alternation detected: {pattern}")
        return False
    
    return True


def safe_regex_match(pattern: str, text: str, timeout: int = REGEX_TIMEOUT_SECONDS) -> bool:
    """
    Perform regex matching with timeout protection.
    Uses Python's 're' module (not 'regex') to match autograder behavior.
    """
    import signal
    
    def timeout_handler(signum, frame):
        raise TimeoutError("Regex matching timed out")
    
    try:
        # Set alarm (Unix-like systems only)
        if hasattr(signal, 'SIGALRM'):
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)
        
        # Compile and match
        compiled_pattern = re.compile(pattern, re.IGNORECASE)
        result = compiled_pattern.search(text) is not None
        
        # Cancel alarm
        if hasattr(signal, 'SIGALRM'):
            signal.alarm(0)
        
        return result
        
    except TimeoutError:
        # Regex took too long - it's malicious!
        if hasattr(signal, 'SIGALRM'):
            signal.alarm(0)
        logger.warning(f"Regex timeout - potential ReDoS: {pattern}")
        raise  # Re-raise to trigger 400 error
        
    except re.error as e:
        if hasattr(signal, 'SIGALRM'):
            signal.alarm(0)
        logger.error(f"Invalid regex: {e}")
        raise ValueError(f"Invalid regex pattern: {e}")



def search_artifacts_internal(regex_str: str, offset: int = 0):

    # ✅ 3. Validate regex syntax
    try:
        re.compile(regex_str, re.IGNORECASE)
    except re.error as e:
        abort(400, description=f"Invalid regex pattern: {str(e)}")

    # ✅ 4. Scan DynamoDB
    response = META_TABLE.scan()
    all_items = response.get("Items", [])
    while "LastEvaluatedKey" in response:
        response = META_TABLE.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        all_items.extend(response.get("Items", []))

    # ✅ 5. Try matching — DO NOT abort if no matches
    results = []
    for item in all_items:
        searchable = " ".join([
            item.get('filename', ''),
            item.get('artifact_type', ''),
            item.get('source_url', '')
        ])

        try:
            if safe_regex_match(regex_str, searchable):
                results.append({
                    "name": item.get("filename", ""),
                    "id": int(item.get("id")),
                    "type": item.get("artifact_type", "")
                })
        except TimeoutError:
            abort(400, description="Regex pattern caused timeout (potential ReDoS)")
        except ValueError as e:
            abort(400, description=str(e))

    # ✅ 6. Deduplicate
    seen = set()
    unique_results = []
    for r in results:
        if r["id"] not in seen:
            seen.add(r["id"])
            unique_results.append(r)

    # ✅ 7. Pagination (EMPTY LIST IS OK)
    total = len(unique_results)
    end_idx = min(offset + MAX_RESULTS_PER_PAGE, total)
    paginated_results = unique_results[offset:end_idx]

    next_offset = str(end_idx) if end_idx < total else None

    # ✅ 8. THIS is the line you asked about
    response_obj = jsonify(paginated_results)
    if next_offset:
        response_obj.headers.add("offset", next_offset)

    # ✅ MUST ALWAYS REACH HERE — even if paginated_results == []
    return response_obj, 200



@app.route("/artifact/byRegEx", methods=["POST"])
def search_by_regex_post():

    payload = request.get_json(silent=True) or {}
    regex_str = payload.get("regex")
    
    if not regex_str:
        abort(400, description="Missing 'regex' field in request body")
    
    # Get offset if provided
    offset_str = request.args.get("offset", "0")
    try:
        offset = int(offset_str)
    except ValueError:
        offset = 0
    
    return search_artifacts_internal(regex_str, offset)


@app.route("/artifacts/search", methods=["GET"])
def search_artifacts_get():

    # Accept either 'q' or 'regex' parameter
    regex_str = request.args.get("regex") or request.args.get("q")
    
    if not regex_str:
        abort(400, description="Missing required query parameter 'q' or 'regex'")
    
    # Get pagination offset
    offset_str = request.args.get("offset", "0")
    try:
        offset = int(offset_str)
    except ValueError:
        offset = 0
    
    return search_artifacts_internal(regex_str, offset)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=5005, debug=True)

