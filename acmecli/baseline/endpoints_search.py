from flask import Flask, request, jsonify, abort
import boto3
from botocore.exceptions import ClientError
import logging
import re
import signal
from multiprocessing import Process, Queue

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


def _regex_worker(pattern: str, text: str, result_queue: Queue):
    """Run regex matching in a separate process."""
    try:
        compiled_pattern = re.compile(pattern, re.IGNORECASE)
        result = compiled_pattern.search(text) is not None
        result_queue.put(('success', result))
    except re.error as e:
        result_queue.put(('error', ValueError(f"Invalid regex pattern: {e}")))
    except Exception as e:
        result_queue.put(('error', e))


def safe_regex_match(pattern: str, text: str, timeout: int = REGEX_TIMEOUT_SECONDS) -> bool:
    """
    Perform regex matching with timeout protection.
    Uses multiprocessing to actually kill slow regex operations.
    Detects catastrophic backtracking (ReDoS attacks).
    """
    result_queue = Queue()
    process = Process(target=_regex_worker, args=(pattern, text, result_queue))
    process.start()
    process.join(timeout=timeout)
    
    # If process is still running, it timed out - kill it
    if process.is_alive():
        process.terminate()
        process.join(timeout=1)  # Give it a moment to die
        if process.is_alive():
            process.kill()  # Force kill if needed
        logger.warning(f"Regex timeout - potential ReDoS: {pattern}")
        raise TimeoutError("Regex matching timed out")
    
    # Get result from queue
    if not result_queue.empty():
        status, value = result_queue.get()
        if status == 'error':
            if isinstance(value, ValueError):
                logger.error(f"Invalid regex: {value}")
            raise value
        return value
    
    # If we get here, something went wrong
    return False


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
     
        searchable_parts = []
        
        for key, value in item.items():
            if isinstance(value, str):
                searchable_parts.append(value)
        
        searchable = " ".join(searchable_parts)


        try:
            if safe_regex_match(regex_str, searchable):
                # Convert ID to int if possible, otherwise keep as string
                artifact_id = item.get("id")
                try:
                    artifact_id = int(artifact_id)
                except (TypeError, ValueError):
                    pass
                
                results.append({
                    "name": item.get("filename", ""),
                    "id": artifact_id,
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
