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
    # Check for nested quantifiers
    nested_quantifiers = re.compile(r'(\(.*[+*]{1,2}.*\))[+*]')
    if nested_quantifiers.search(pattern):
        logger.warning(f"Detected nested quantifiers in pattern: {pattern}")
        return False
    
    # Check for extremely long patterns
    if len(pattern) > 500:
        logger.warning(f"Pattern too long ({len(pattern)} chars)")
        return False
    
    # Check for excessive alternations
    if pattern.count('|') > 20:
        logger.warning(f"Too many alternations in pattern")
        return False
    
    return True


def safe_regex_match(pattern: str, text: str, timeout: int = REGEX_TIMEOUT_SECONDS) -> bool:

    # Set up timeout signal (Unix-like systems only)
    try:
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)
        
        compiled_pattern = re.compile(pattern, re.IGNORECASE)
        result = compiled_pattern.search(text) is not None
        
        signal.alarm(0)  # Cancel alarm
        return result
        
    except AttributeError:
        # Windows doesn't support SIGALRM, fall back to simple match
        compiled_pattern = re.compile(pattern, re.IGNORECASE)
        return compiled_pattern.search(text) is not None
        
    except TimeoutError:
        signal.alarm(0)
        raise
        
    except re.error as e:
        signal.alarm(0)
        logger.error(f"Invalid regex pattern: {e}")
        raise ValueError(f"Invalid regex pattern: {e}")


def search_artifacts_internal(regex_str: str, offset: int = 0):

    # Validate regex is not empty
    if not regex_str or not regex_str.strip():
        abort(400, description="Regex pattern cannot be empty")
    
    regex_str = regex_str.strip()
    
    logger.info(f"Searching artifacts with pattern: {regex_str}, offset: {offset}")
    
    # Check for malicious regex
    if not is_safe_regex(regex_str):
        logger.warning(f"Potentially malicious regex detected: {regex_str}")
        abort(400, description="Malicious regex pattern detected")
    
    # Validate regex by trying to compile it
    try:
        re.compile(regex_str, re.IGNORECASE)
    except re.error as e:
        logger.error(f"Invalid regex pattern: {e}")
        abort(400, description=f"Invalid regex pattern: {str(e)}")
    
    # Scan DynamoDB for all artifacts
    try:
        response = META_TABLE.scan()
        all_items = response.get("Items", [])
        
        while "LastEvaluatedKey" in response:
            response = META_TABLE.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            all_items.extend(response.get("Items", []))
            
    except ClientError as e:
        logger.error(f"DynamoDB scan failed: {e}", exc_info=True)
        abort(500, description="The artifact storage encountered an error.")
    
    # Search through artifacts
    results = []
    for item in all_items:
        # Build searchable text from multiple fields
        searchable_parts = [
            item.get('filename', ''),
            item.get('artifact_type', ''),
            item.get('source_url', '')
        ]
        searchable = ' '.join(searchable_parts)
        
        # Perform safe regex match
        try:
            if safe_regex_match(regex_str, searchable):
                artifact_id_raw = item.get("id", "")
                
                # Cast id to int if possible (for consistency)
                try:
                    artifact_id = int(artifact_id_raw)
                except (TypeError, ValueError):
                    artifact_id = artifact_id_raw
                
                results.append({
                    "name": item.get("filename", ""),
                    "id": artifact_id,
                    "type": item.get("artifact_type", "")
                })
                
        except TimeoutError:
            logger.error(f"Regex timeout on artifact {item.get('id')}")
            abort(400, description="Regex pattern caused timeout (potential ReDoS)")
        except ValueError as e:
            abort(400, description=str(e))
    
    # Remove duplicates (by ID)
    unique_results = []
    seen_ids = set()
    for r in results:
        if r["id"] not in seen_ids:
            seen_ids.add(r["id"])
            unique_results.append(r)
    
    # Apply pagination
    total = len(unique_results)
    end_idx = min(offset + MAX_RESULTS_PER_PAGE, total)
    paginated_results = unique_results[offset:end_idx]
    
    # Calculate next offset
    next_offset = str(end_idx) if end_idx < total else None
    
    # Build response with offset header
    response_obj = jsonify(paginated_results)
    if next_offset:
        response_obj.headers.add("offset", next_offset)
    
    logger.info(f"Search returned {len(paginated_results)}/{total} matching artifacts")
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