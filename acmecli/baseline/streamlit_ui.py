# acmecli/baseline/streamlit_ui.py

import io
import hashlib
import time
import traceback
import zipfile
from collections import defaultdict
from typing import List
from urllib.parse import quote

import boto3
import requests
import streamlit as st
from botocore.exceptions import ClientError

# ---- S3 config ----
S3_BUCKET = "ece-registry"
AWS_REGION = "us-east-1"

# Initialize AWS clients once at module level for better performance
s3_client = boto3.client("s3", region_name=AWS_REGION)
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
meta_table = dynamodb.Table("artifact")

# Create a requests session for connection pooling and better performance
@st.cache_resource
def get_requests_session():
    """Create a cached requests session for connection pooling."""
    session = requests.Session()
    # Set default timeout
    session.timeout = 60
    return session

VALID_TYPES = ["model", "code", "dataset"]
TYPE_TO_S3_PREFIX = {
    "model": "model",
    "code": "code",
    "dataset": "dataset",
}


@st.cache_data(ttl=300)  # Cache for 5 minutes
def list_top_level_prefixes() -> List[str]:
    """
    Return top-level prefixes in the bucket (e.g., ['models', 'models2']).
    Cached for 5 minutes to improve performance.
    """
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=S3_BUCKET, Delimiter="/")
        prefixes: List[str] = []
        for page in pages:
            for pref in page.get("CommonPrefixes", []):
                name = pref["Prefix"].strip("/")
                if name:
                    prefixes.append(name)
        return sorted(set(prefixes))
    except ClientError:
        return []


@st.cache_data(ttl=300)  # Cache for 5 minutes
def list_artifact_ids_for_prefix(prefix: str) -> List[str]:
    """
    Return immediate child names under the given prefix (directories or direct files).
    Cached for 5 minutes to improve performance.
    """
    prefix = prefix.rstrip("/")
    prefix_with_slash = f"{prefix}/"
    ids: List[str] = []
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix_with_slash, Delimiter="/")
        seen = set()
        for page in pages:
            for pref in page.get("CommonPrefixes", []):
                name = pref["Prefix"][len(prefix_with_slash):].strip("/")
                if name and name not in seen:
                    seen.add(name)
                    ids.append(name)
            for obj in page.get("Contents", []):
                key = obj["Key"]
                remainder = key[len(prefix_with_slash):]
                if remainder:
                    name = remainder.split("/")[0]
                    if name and name not in seen:
                        seen.add(name)
                        ids.append(name)
        return sorted(ids)
    except ClientError:
        return []


def _safe_zip_check(blob: bytes, *, max_uncompressed_bytes: int = 200 * 1024 * 1024) -> None:
    """
    Lightweight safety check for uploaded ZIPs:
    - no path traversal (../ or absolute)
    - bounded uncompressed size (simple zip-bomb guard)
    """
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        total = 0
        for zi in zf.infolist():
            p = zi.filename.replace("\\", "/")
            # Disallow absolute paths and traversal
            if p.startswith("/") or ".." in p.split("/"):
                raise ValueError(f"Unsafe path in zip entry: {zi.filename}")
            total += zi.file_size
            if total > max_uncompressed_bytes:
                raise ValueError(
                    "Zip appears too large when extracted (possible zip bomb). "
                    "Please upload a smaller/safer archive."
                )


st.set_page_config(page_title="Artifact Registry", page_icon="📦", layout="centered")

# Add accessibility improvements with custom CSS
st.markdown("""
<style>
    /* Skip to main content link for screen readers */
    .skip-link {
        position: absolute;
        top: -40px;
        left: 0;
        background: #000;
        color: #fff;
        padding: 8px;
        text-decoration: none;
        z-index: 100;
    }
    .skip-link:focus {
        top: 0;
    }
    /* Ensure sufficient focus indicators */
    *:focus {
        outline: 3px solid #0066cc;
        outline-offset: 2px;
    }
    /* Improve color contrast for error messages */
    .stAlert {
        border-left: 4px solid;
    }
</style>
""", unsafe_allow_html=True)

# Skip to main content link (hidden but accessible via keyboard)
st.markdown('<a href="#main-content" class="skip-link">Skip to main content</a>', unsafe_allow_html=True)

# Main content with proper heading hierarchy
st.markdown('<div id="main-content" role="main">', unsafe_allow_html=True)
st.title("Artifact Registry")
st.write("Upload, download, and manage artifacts in the registry.")

DEFAULT_BACKEND = "http://127.0.0.1:5001"
backend_url = st.text_input(
    "Backend URL", 
    value=DEFAULT_BACKEND,
    help="Enter the URL of the backend server. Default is http://127.0.0.1:5001",
    label_visibility="visible"
)

# upload UI
st.divider()
st.header("Upload Artifact")
st.markdown('<span aria-label="Upload Artifact section">📤</span>', unsafe_allow_html=True)

st.write("Upload an artifact file. Select the category to determine which folder it will be stored in (model/, dataset/, or code/).")

# Category selection - model/, dataset/, or code/
upload_artifact_type = st.selectbox(
    "Artifact Category", 
    options=VALID_TYPES, 
    index=0, 
    format_func=lambda x: x.title(),
    help="Select the category: model, dataset, or code. This determines which S3 folder the artifact will be stored in.",
    key="upload_type"
)

# Artifact name input
artifact_name = st.text_input(
    "Artifact Name", 
    placeholder="e.g., bert-base-uncased",
    help="Enter a name for the artifact (will be extracted from filename if not provided)"
)

uploaded_file = st.file_uploader("Choose artifact ZIP file", type=["zip"], help="Upload a ZIP file containing your artifact")

if st.button("Upload Artifact", type="primary", key="upload_btn"):
    if not uploaded_file:
        st.error("Error: Please choose a .zip file to upload.", icon="⚠️")
    else:
        try:
            blob = uploaded_file.getvalue()
            size = len(blob)
            
            # Compute hash for integrity
            sha256 = hashlib.sha256(blob).hexdigest()
            
            # Quick safety checks to avoid bad archives
            _safe_zip_check(blob)
            
            # Generate artifact ID (timestamp-based, matching upload.py)
            artifact_id = str(int(time.time() * 1000))
            
            # Extract name from filename if needed
            if not artifact_name or not artifact_name.strip():
                filename = uploaded_file.name
                artifact_name = filename.rsplit(".", 1)[0] if "." in filename else filename
            
            # Determine S3 key based on category: model/, dataset/, or code/
            s3_key = f"{upload_artifact_type}/{artifact_id}.zip"
            
            with st.spinner(f"Uploading {upload_artifact_type} artifact to S3..."):
                # Upload to S3 in the correct folder
                s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=blob,
                    ContentType="application/zip",
                )
            
            # Register in DynamoDB (using cached table resource)
            with st.spinner("Registering artifact in DynamoDB..."):
                try:
                    meta_table.put_item(
                        Item={
                            "id": artifact_id,
                            "artifact_type": upload_artifact_type,
                            "s3_bucket": S3_BUCKET,
                            "s3_key": s3_key,
                            "filename": artifact_name.strip(),
                            "size_bytes": size,
                            "sha256": sha256,
                        }
                    )
                except ClientError as e:
                    st.warning(f"⚠️ Uploaded to S3 but DynamoDB registration failed: {e}")
                    # Continue to show success for S3 upload
            
            # Confirm upload and show metadata
            st.success("Success: Artifact uploaded successfully!")
            st.markdown("### Upload Details")
            st.write(f"**Artifact ID:** `{artifact_id}`")
            st.write(f"**Name:** {artifact_name.strip()}")
            st.write(f"**Category:** {upload_artifact_type}")
            st.write(f"**Stored in:** `{upload_artifact_type}/` folder")
            st.write(f"**S3 Key:** `{s3_key}`")
            st.write(f"**Size:** {size:,} bytes ({size / 1024 / 1024:.2f} MB)")
            st.write(f"**SHA-256:** `{sha256}`")
            
            st.info(f"Information: The artifact has been stored in the `{upload_artifact_type}/` folder in S3 and registered in DynamoDB.")

        except ValueError as ve:
            st.error(f"Validation error: {ve}", icon="⚠️")
        except ClientError as e:
            st.error(f"AWS error: {e}", icon="⚠️")
        except Exception as ex:
            st.error(f"Unexpected error: {ex}", icon="⚠️")

# download UI
st.divider()
st.header("Download Artifact")
st.markdown('<span aria-label="Download Artifact section">⬇️</span>', unsafe_allow_html=True)

st.write("Enter the artifact ID (the numeric ID shown after upload) to download the artifact.")

artifact_type = st.selectbox(
    "Artifact type", 
    options=VALID_TYPES, 
    index=0, 
    format_func=lambda x: x.title(),
    help="Select the type of artifact to download: model, code, or dataset",
    label_visibility="visible"
)

artifact_id = st.text_input(
    "Artifact ID", 
    placeholder="Enter artifact ID (e.g., 1234567890)",
    value="",
    help="The artifact ID is the numeric ID generated when you upload an artifact. It's shown in the upload success message."
)

if st.button("Download from server", type="primary"):
    if not artifact_id or not artifact_id.strip():
        st.error("Error: Please enter an artifact ID.", icon="⚠️")
    else:
        artifact_id = artifact_id.strip()
        url = f"{backend_url}/artifacts/{quote(artifact_type, safe='')}/{quote(artifact_id, safe='')}"
        
        with st.spinner("Requesting file from server..."):
            try:
                # Use cached session for better performance
                session = get_requests_session()
                headers = {"X-Authorization": "baseline"}
                resp = session.get(url, headers=headers, timeout=60)
                
                if resp.status_code == 200:
                    # Parse JSON response to get presigned URL
                    data = resp.json()
                    presigned_url = data.get("data", {}).get("url")
                    metadata = data.get("metadata", {})
                    filename = metadata.get("name", f"{artifact_id}.zip")
                    
                    if not presigned_url:
                        st.error("Error: No download URL found in server response.", icon="⚠️")
                        with st.expander("Server Response (for debugging)"):
                            st.json(data)
                    else:
                        # Download from presigned URL using cached session
                        with st.spinner("Downloading file from S3..."):
                            session = get_requests_session()
                            file_resp = session.get(presigned_url, timeout=300)
                            if file_resp.status_code == 200:
                                st.success("Success: File ready. Click below to save it.")
                                st.download_button(
                                    label=f"Save {filename}",
                                    data=file_resp.content,
                                    file_name=filename,
                                    mime="application/zip",
                                    key="save_btn",
                                    help=f"Download the artifact file: {filename}"
                                )
                            else:
                                st.error(f"Error: Failed to download from S3 - Status code: {file_resp.status_code}", icon="⚠️")
                                st.error(f"Error details: {file_resp.text[:200]}", icon="⚠️")
                                
                elif resp.status_code == 400:
                    error_text = resp.text[:500] if resp.text else "Invalid request"
                    st.error(f"Error: Invalid request - {error_text}", icon="⚠️")
                    st.info("Tip: Make sure the artifact ID is valid (alphanumeric, hyphens, dots, underscores only)")
                elif resp.status_code == 403:
                    st.error("Error: Authentication failed. Please check your authorization token.", icon="⚠️")
                elif resp.status_code == 404:
                    st.error("Error: Artifact not found (404).", icon="⚠️")
                    st.info(f"Verification steps:\n- The artifact ID '{artifact_id}' exists in DynamoDB\n- The artifact type '{artifact_type}' matches the uploaded category\n- The artifact was successfully uploaded")
                elif resp.status_code == 500:
                    error_text = resp.text[:500] if resp.text else "Server error"
                    st.error(f"Error: Server error - {error_text}", icon="⚠️")
                else:
                    error_text = resp.text[:500] if resp.text else f"Unexpected response ({resp.status_code})"
                    st.error(f"Error: Server returned {resp.status_code} - {error_text}", icon="⚠️")
            except requests.RequestException as e:
                st.error(f"Error: Request failed - {e}", icon="⚠️")
                st.info(f"Tip: Make sure the backend server is running at {backend_url}")
            except Exception as ex:
                st.error(f"Error: Unexpected error - {ex}", icon="⚠️")
                with st.expander("Technical Details (for debugging)"):
                    st.code(traceback.format_exc())

with st.expander("Tips and Help"):
    st.markdown(
        "**Getting Started:**\n"
        "- Start Flask: `python acmecli/baseline/backend.py`\n"
        "- Start Streamlit: `streamlit run acmecli/baseline/streamlit_ui.py`\n"
        "- The app uses the `/artifacts/<type>/<id>` endpoint which returns a presigned S3 URL for download.\n\n"
        "**Accessibility:**\n"
        "- All form fields have descriptive labels\n"
        "- Use Tab key to navigate between form elements\n"
        "- Error messages are clearly labeled\n"
        "- Screen reader users can skip to main content using the skip link"
    )

# cost UI
st.divider()
st.header("Artifact Cost Calculator")
st.markdown('<span aria-label="Artifact Cost Calculator section">💰</span>', unsafe_allow_html=True)

st.write("Calculate the storage cost (size in MB) for an artifact, optionally including dependencies.")

# Reuse the same artifact selection from download section
cost_artifact_type = st.selectbox(
    "Artifact type", 
    options=VALID_TYPES, 
    index=0, 
    format_func=lambda x: x.title(),
    key="cost_type"
)

cost_artifact_id = st.text_input("Artifact ID", placeholder="Enter artifact ID (e.g., bert.zip)", value="", key="cost_id")

include_dependencies = st.checkbox("Include dependencies", value=False, help="When enabled, shows standalone_cost and total_cost including dependencies")

if st.button("Calculate Cost", type="primary", key="cost_btn"):
    if not cost_artifact_id or not cost_artifact_id.strip():
        st.error("Error: Please enter an artifact ID.", icon="⚠️")
    else:
        # Build the cost endpoint URL
        url = f"{backend_url}/artifact/{quote(cost_artifact_type, safe='')}/{quote(cost_artifact_id, safe='')}/cost"
        if include_dependencies:
            url += "?dependency=true"
        
        with st.spinner("Calculating cost..."):
            try:
                # Use cached session for better performance
                session = get_requests_session()
                headers = {"X-Authorization": "baseline"}
                resp = session.get(url, headers=headers, timeout=60)
                
                if resp.status_code == 200:
                    cost_data = resp.json()
                    
                    if cost_artifact_id in cost_data:
                        artifact_cost = cost_data[cost_artifact_id]
                        total_cost = artifact_cost.get("total_cost", 0)
                        standalone_cost = artifact_cost.get("standalone_cost")
                        
                        st.success("Success: Cost calculated successfully!")
                        
                        # Display cost information
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Total Cost", f"{total_cost} MB")
                        if standalone_cost is not None:
                            with col2:
                                st.metric("Standalone Cost", f"{standalone_cost} MB")
                    else:
                        st.error(f"Error: Unexpected response format - {cost_data}", icon="⚠️")
                        
                elif resp.status_code == 400:
                    st.error(f"Error: Invalid request - {resp.text[:200]}", icon="⚠️")
                elif resp.status_code == 403:
                    st.error("Error: Authentication failed. Please check your authorization token.", icon="⚠️")
                elif resp.status_code == 404:
                    st.error("Error: Artifact not found (404). Verify the artifact ID exists in S3.", icon="⚠️")
                elif resp.status_code == 500:
                    st.error(f"Error: Server error - {resp.text[:200]}", icon="⚠️")
                else:
                    st.error(f"Error: Unexpected response ({resp.status_code}) - {resp.text[:200]}", icon="⚠️")
                    
            except requests.RequestException as e:
                st.error(f"Request failed: {e}")
            except Exception as ex:
                st.error(f"Unexpected error: {ex}")


# license check UI
st.divider()
st.header("License Check")
st.markdown('<span aria-label="License Check section">📜</span>', unsafe_allow_html=True)

st.write("Check license compliance for an artifact. This will analyze the license text and determine if it's compliant.")

license_artifact_type = st.selectbox(
    "Artifact type",
    options=VALID_TYPES,
    index=0,
    format_func=lambda x: x.title(),
    key="license_type"
)

license_artifact_id = st.text_input(
    "Artifact ID",
    placeholder="Enter artifact ID",
    value="",
    key="license_id"
)

if st.button("Check License", type="primary", key="license_btn"):
    if not license_artifact_id or not license_artifact_id.strip():
        st.error("Error: Please enter an artifact ID.", icon="⚠️")
    else:
        url = f"{backend_url}/artifact/{quote(license_artifact_type, safe='')}/{quote(license_artifact_id.strip(), safe='')}/license-check"
        
        with st.spinner("Checking license compliance..."):
            try:
                # Use cached session for better performance
                session = get_requests_session()
                headers = {"X-Authorization": "baseline", "Content-Type": "application/json"}
                resp = session.post(url, headers=headers, timeout=60)
                
                if resp.status_code == 200:
                    data = resp.json()
                    is_compliant = data.get("is_compliant", False)
                    license_text = data.get("license")
                    score = data.get("score", 0.0)
                    
                    st.success("Success: License check completed!")
                    
                    # Display compliance status
                    if is_compliant:
                        st.success(f"**Status:** Compliant (Score: {score:.2f})")
                    else:
                        if score == 0.0:
                            st.error(f"**Status:** Non-compliant (Score: {score:.2f})", icon="⚠️")
                        else:
                            st.warning(f"**Status:** Unclear (Score: {score:.2f})")
                    
                    # Display license text if available
                    if license_text:
                        with st.expander("View License Text"):
                            st.text(license_text)
                    else:
                        st.info("Information: No license text found for this artifact.")
                    
                    # Display score breakdown
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("License Score", f"{score:.2f}")
                    with col2:
                        compliance_text = "Yes" if is_compliant else "No"
                        st.metric("Compliance", compliance_text)
                        
                elif resp.status_code == 400:
                    st.error(f"Error: Invalid request - {resp.text[:200]}", icon="⚠️")
                elif resp.status_code == 403:
                    st.error("Error: Authentication failed. Please check your authorization token.", icon="⚠️")
                elif resp.status_code == 404:
                    st.error("Error: Artifact not found (404). Verify the artifact ID exists.", icon="⚠️")
                elif resp.status_code == 500:
                    st.error(f"Error: Server error - {resp.text[:200]}", icon="⚠️")
                else:
                    st.error(f"Error: Unexpected response ({resp.status_code}) - {resp.text[:200]}", icon="⚠️")
                    
            except requests.RequestException as e:
                st.error(f"Error: Request failed - {e}", icon="⚠️")
            except Exception as ex:
                st.error(f"Error: Unexpected error - {ex}", icon="⚠️")


# rate UI
st.divider()
st.header("Rate Model")
st.markdown('<span aria-label="Rate Model section">⭐</span>', unsafe_allow_html=True)

st.write("Get detailed rating metrics for a model artifact, including net score, license score, code quality, and more.")

rate_model_id = st.text_input(
    "Model ID",
    placeholder="Enter model artifact ID",
    value="",
    key="rate_id"
)

if st.button("Rate Model", type="primary", key="rate_btn"):
    if not rate_model_id or not rate_model_id.strip():
        st.error("Error: Please enter a model ID.", icon="⚠️")
    else:
        url = f"{backend_url}/artifact/model/{quote(rate_model_id.strip(), safe='')}/rate"
        
        with st.spinner("Computing model rating... This may take a moment."):
            try:
                # Use cached session for better performance
                session = get_requests_session()
                headers = {"X-Authorization": "baseline"}
                resp = session.get(url, headers=headers, timeout=120)
                
                if resp.status_code == 200:
                    data = resp.json()
                    
                    st.success("Success: Model rating computed successfully!")
                    
                    # Display main metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Net Score", f"{data.get('net_score', 0.0):.3f}")
                    with col2:
                        st.metric("License Score", f"{data.get('license', 0.0):.3f}")
                    with col3:
                        st.metric("Code Quality", f"{data.get('code_quality', 0.0):.3f}")
                    
                    # Display detailed metrics in expandable sections
                    with st.expander("Detailed Metrics"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write("**Performance Metrics:**")
                            st.write(f"- Ramp-up Time: {data.get('ramp_up_time', 0.0):.3f}")
                            st.write(f"- Bus Factor: {data.get('bus_factor', 0.0):.3f}")
                            st.write(f"- Performance Claims: {data.get('performance_claims', 0.0):.3f}")
                            st.write(f"- Dataset Quality: {data.get('dataset_quality', 0.0):.3f}")
                            st.write(f"- Dataset & Code Score: {data.get('dataset_and_code_score', 0.0):.3f}")
                        
                        with col2:
                            st.write("**Additional Metrics:**")
                            st.write(f"- Reproducibility: {data.get('reproducibility', 0.0):.3f}")
                            st.write(f"- Reviewedness: {data.get('reviewedness', -1.0):.3f}")
                            st.write(f"- Tree Score: {data.get('tree_score', 0.0):.3f}")
                            
                            # Size scores for different hardware tiers
                            size_score = data.get('size_score', {})
                            if size_score:
                                st.write("**Size Scores:**")
                                for tier, score in size_score.items():
                                    st.write(f"- {tier.replace('_', ' ').title()}: {score:.3f}")
                    
                    # Display latencies if available
                    if any(key.endswith('_latency') for key in data.keys()):
                        with st.expander("Latency Information"):
                            latency_keys = [k for k in data.keys() if k.endswith('_latency')]
                            for key in sorted(latency_keys):
                                latency_ms = data.get(key, 0)
                                latency_s = latency_ms / 1000.0 if latency_ms > 0 else 0
                                st.write(f"- {key.replace('_latency', '').replace('_', ' ').title()}: {latency_s:.3f}s ({latency_ms:.1f}ms)")
                    
                    # Show raw JSON in expander
                    with st.expander("Raw JSON Response (for debugging)"):
                        st.json(data)
                        
                elif resp.status_code == 400:
                    st.error(f"Error: Invalid request - {resp.text[:200]}", icon="⚠️")
                    st.info("Tip: Make sure the artifact ID corresponds to a model artifact.")
                elif resp.status_code == 403:
                    st.error("Error: Authentication failed. Please check your authorization token.", icon="⚠️")
                elif resp.status_code == 404:
                    st.error("Error: Model not found (404). Verify the model ID exists.", icon="⚠️")
                elif resp.status_code == 500:
                    st.error(f"Error: Server error - {resp.text[:200]}", icon="⚠️")
                    st.info("Note: This might indicate that the model's source URL is missing or invalid.")
                else:
                    st.error(f"Error: Unexpected response ({resp.status_code}) - {resp.text[:200]}", icon="⚠️")
                    
            except requests.RequestException as e:
                st.error(f"Error: Request failed - {e}", icon="⚠️")
            except Exception as ex:
                st.error(f"Error: Unexpected error - {ex}", icon="⚠️")


# search UI
st.divider()
st.header("Search Artifacts")
st.markdown('<span aria-label="Search Artifacts section">🔍</span>', unsafe_allow_html=True)

st.write("Search for artifacts using a regular expression pattern. Matches against artifact names, IDs, and README content.")

search_method = st.radio(
    "Search Method",
    options=["GET (Query Parameter)", "POST (JSON Body)"],
    index=0,
    key="search_method"
)

search_regex = st.text_input(
    "Regex Pattern",
    placeholder="e.g., bert|gpt|resnet",
    value="",
    help="Enter a regular expression pattern to search for. The search is case-insensitive."
)

search_offset = st.number_input(
    "Offset (for pagination)",
    min_value=0,
    value=0,
    help="Starting offset for paginated results (0 = first page)",
    key="search_offset"
)

if st.button("Search Artifacts", type="primary", key="search_btn"):
    if not search_regex or not search_regex.strip():
        st.error("Error: Please enter a regex pattern.", icon="⚠️")
    else:
        regex_pattern = search_regex.strip()
        
        with st.spinner("Searching artifacts... This may take a moment."):
            try:
                headers = {"X-Authorization": "baseline"}
                
                # Use cached session for better performance
                session = get_requests_session()
                
                if search_method == "GET (Query Parameter)":
                    url = f"{backend_url}/artifacts/search"
                    params = {"regex": regex_pattern, "offset": str(search_offset)}
                    resp = session.get(url, headers=headers, params=params, timeout=60)
                else:
                    url = f"{backend_url}/artifact/byRegEx"
                    if search_offset > 0:
                        url += f"?offset={search_offset}"
                    payload = {"regex": regex_pattern}
                    headers["Content-Type"] = "application/json"
                    resp = session.post(url, headers=headers, json=payload, timeout=60)
                
                if resp.status_code == 200:
                    results = resp.json()
                    
                    # Check for pagination header
                    next_offset = resp.headers.get("offset")
                    
                    if results:
                        st.success(f"Success: Found {len(results)} artifact(s)!")
                        
                        if next_offset:
                            st.info(f"More results available. Next offset: {next_offset}")
                        
                        # Display results in a table
                        st.dataframe(results, use_container_width=True)
                        
                        # Display individual results with details
                        st.write("**Search Results:**")
                        for idx, result in enumerate(results, 1):
                            with st.expander(f"Result {idx}: {result.get('name', 'Unknown')} (ID: {result.get('id')})"):
                                st.write(f"**Name:** {result.get('name', 'N/A')}")
                                st.write(f"**ID:** {result.get('id')}")
                                st.write(f"**Type:** {result.get('type', 'N/A')}")
                    else:
                        st.info("Information: No results found, but the search completed successfully.")
                        
                elif resp.status_code == 400:
                    error_text = resp.text[:500] if resp.text else "Invalid request"
                    st.error(f"Error: Invalid request - {error_text}", icon="⚠️")
                    st.info("Tip: Make sure your regex pattern is valid. Common issues:\n- Unclosed brackets or parentheses\n- Invalid escape sequences\n- Patterns that cause ReDoS (nested quantifiers)")
                elif resp.status_code == 403:
                    st.error("Error: Authentication failed. Please check your authorization token.", icon="⚠️")
                elif resp.status_code == 404:
                    st.error("Error: No artifacts found matching the regex pattern.", icon="⚠️")
                elif resp.status_code == 500:
                    st.error(f"Error: Server error - {resp.text[:200]}", icon="⚠️")
                else:
                    st.error(f"Error: Unexpected response ({resp.status_code}) - {resp.text[:200]}", icon="⚠️")
                    
            except requests.RequestException as e:
                st.error(f"Error: Request failed - {e}", icon="⚠️")
            except Exception as ex:
                st.error(f"Error: Unexpected error - {ex}", icon="⚠️")


# lineage UI
st.divider()
st.header("Model Lineage")
st.markdown('<span aria-label="Model Lineage section">🌳</span>', unsafe_allow_html=True)

st.write("View the lineage graph for a model artifact, showing its ancestors (base models) and descendants (derived models).")

lineage_model_id = st.text_input(
    "Model ID",
    placeholder="Enter model artifact ID",
    value="",
    key="lineage_id"
)

if st.button("Get Lineage", type="primary", key="lineage_btn"):
    if not lineage_model_id or not lineage_model_id.strip():
        st.error("Error: Please enter a model ID.", icon="⚠️")
    else:
        url = f"{backend_url}/artifact/model/{quote(lineage_model_id.strip(), safe='')}/lineage"
        
        with st.spinner("Building lineage graph... This may take a moment."):
            try:
                # Use cached session for better performance
                session = get_requests_session()
                headers = {"X-Authorization": "baseline"}
                resp = session.get(url, headers=headers, timeout=120)
                
                if resp.status_code == 200:
                    data = resp.json()
                    nodes = data.get("nodes", [])
                    edges = data.get("edges", [])
                    
                    st.success(f"Success: Lineage graph retrieved! Found {len(nodes)} node(s) and {len(edges)} edge(s).")
                    
                    if nodes:
                        # Display nodes
                        st.subheader("Nodes in Lineage")
                        st.dataframe(nodes, use_container_width=True)
                        
                        # Display edges
                        if edges:
                            st.subheader("Relationships")
                            st.dataframe(edges, use_container_width=True)
                            
                            # Visual representation
                            st.subheader("Lineage Visualization")
                            st.write("**Lineage Structure:**")
                            
                            # Build a simple text representation
                            children_map = defaultdict(list)
                            for edge in edges:
                                parent = edge.get("from_node_artifact_id")
                                child = edge.get("to_node_artifact_id")
                                children_map[parent].append(child)
                            
                            # Find root nodes (nodes with no incoming edges)
                            all_parents = {e.get("from_node_artifact_id") for e in edges}
                            all_children = {e.get("to_node_artifact_id") for e in edges}
                            root_nodes = all_parents - all_children
                            
                            def format_node_name(node_id):
                                node = next((n for n in nodes if n.get("artifact_id") == node_id), None)
                                return node.get("name", node_id) if node else node_id
                            
                            if root_nodes:
                                st.write("**Ancestors (Base Models):**")
                                for root in sorted(root_nodes):
                                    st.write(f"- {format_node_name(root)} (ID: {root})")
                            
                            # Show the target model
                            target_name = format_node_name(lineage_model_id.strip())
                            st.write(f"**Target Model:** {target_name} (ID: {lineage_model_id.strip()})")
                            
                            # Show descendants
                            if lineage_model_id.strip() in children_map:
                                st.write("**Descendants (Derived Models):**")
                                for child in sorted(children_map[lineage_model_id.strip()]):
                                    st.write(f"- {format_node_name(child)} (ID: {child})")
                            else:
                                st.info("Information: This model has no known descendants.")
                        else:
                            st.info("Information: No relationships found. This model appears to be isolated (no known parent or child models).")
                    else:
                        st.warning("Warning: Lineage graph is empty.")
                    
                    # Show raw JSON in expander
                    with st.expander("Raw JSON Response (for debugging)"):
                        st.json(data)
                        
                elif resp.status_code == 400:
                    st.error(f"Error: Invalid request - {resp.text[:200]}", icon="⚠️")
                    st.info("Note: The lineage graph cannot be computed. This might be due to missing or malformed metadata.")
                elif resp.status_code == 403:
                    st.error("Error: Authentication failed. Please check your authorization token.", icon="⚠️")
                elif resp.status_code == 404:
                    st.error("Error: Model not found (404). Verify the model ID exists.", icon="⚠️")
                elif resp.status_code == 500:
                    st.error(f"Error: Server error - {resp.text[:200]}", icon="⚠️")
                else:
                    st.error(f"Error: Unexpected response ({resp.status_code}) - {resp.text[:200]}", icon="⚠️")
                    
            except requests.RequestException as e:
                st.error(f"Error: Request failed - {e}", icon="⚠️")
            except Exception as ex:
                st.error(f"Error: Unexpected error - {ex}", icon="⚠️")


# reset registry UI
st.divider()
st.header("Reset Registry")
st.markdown('<span aria-label="Reset Registry section">🔄</span>', unsafe_allow_html=True)

st.warning("Warning: Danger Zone - This will delete ALL artifacts from the S3 bucket. This action cannot be undone!")

if st.button("Reset Registry", type="primary"):
    with st.spinner("Resetting registry... This may take a moment."):
        try:
            # Use cached session for better performance
            session = get_requests_session()
            url = f"{backend_url}/reset"
            # Reset endpoint requires "admin" token
            headers = {"X-Authorization": "admin"}
            resp = session.delete(url, headers=headers, timeout=300)
            
            if resp.status_code == 200:
                st.success("Success: Registry reset successfully!")
                st.info("Information: All artifacts have been deleted from the S3 bucket. Please refresh the page to see the updated state.")
                # Force refresh of model folders
                st.rerun()
            elif resp.status_code == 401:
                st.error("Error: Permission denied. Admin token required to reset the registry.", icon="⚠️")
            elif resp.status_code == 403:
                st.error("Error: Authentication failed. Please check your authorization token.", icon="⚠️")
            elif resp.status_code == 500:
                st.error(f"Error: Server error - {resp.text[:200]}", icon="⚠️")
            else:
                st.error(f"Error: Unexpected response ({resp.status_code}) - {resp.text[:200]}", icon="⚠️")
        except requests.RequestException as e:
            st.error(f"Error: Request failed - {e}", icon="⚠️")
        except Exception as ex:
            st.error(f"Error: Unexpected error - {ex}", icon="⚠️")

# Close main content div at the end
st.markdown('</div>', unsafe_allow_html=True)