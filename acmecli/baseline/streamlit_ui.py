# acmecli/baseline/streamlit_ui.py

import io
import hashlib
import zipfile
from typing import List
from urllib.parse import quote

import boto3
import requests
import streamlit as st
from botocore.exceptions import ClientError

# ---- S3 config ----
S3_BUCKET = "ece-registry"  # override via st.secrets / env if desired
AWS_REGION = "us-east-1"

s3_client = boto3.client("s3", region_name=AWS_REGION)

VALID_TYPES = ["model", "code", "dataset"]
TYPE_TO_S3_PREFIX = {
    "model": "model",
    "code": "code",
    "dataset": "dataset",
}


def list_top_level_prefixes() -> List[str]:
    """
    Return top-level prefixes in the bucket (e.g., ['models', 'models2']).
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


def list_artifact_ids_for_prefix(prefix: str) -> List[str]:
    """
    Return immediate child names under the given prefix (directories or direct files).
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


# ---- Page config ----
st.set_page_config(page_title="Artifact Download", page_icon="📦", layout="centered")

st.title("Artifact Download")
st.write("Select an artifact type and ID to retrieve the corresponding package from the Flask server.")

# Backend base URL (Flask server)
DEFAULT_BACKEND = "http://127.0.0.1:5001"
backend_url = st.text_input("Backend URL", value=DEFAULT_BACKEND)

# Load available top-level prefixes (used by the upload helper below)
with st.spinner("Loading top-level prefixes from S3..."):
    model_folders = list_top_level_prefixes()

if not model_folders:
    st.warning("No top-level folders found in the S3 bucket.")
    st.stop()

# ---- Download UI ----
artifact_type = st.selectbox("Artifact type", options=VALID_TYPES, index=0, format_func=lambda x: x.title())

artifact_id = st.text_input("Artifact ID", placeholder="Enter artifact ID (e.g., bert.zip)", value="")

st.divider()

if st.button("Download from server", type="primary"):
    if not artifact_id or not artifact_id.strip():
        st.error("Please enter an artifact ID.")
    else:
        url = f"{backend_url}/artifacts/{quote(artifact_type, safe='')}/{quote(artifact_id, safe='')}"
        with st.spinner("Requesting file from server..."):
            try:
                # Send request with authentication header
                headers = {"X-Authorization": "baseline"}
                resp = requests.get(url, headers=headers, timeout=60)
                
                if resp.status_code == 200:
                    # Parse JSON response to get presigned URL
                    data = resp.json()
                    presigned_url = data.get("data", {}).get("url")
                    metadata = data.get("metadata", {})
                    filename = metadata.get("name", f"{artifact_id}.zip")
                    
                    if not presigned_url:
                        st.error("No download URL found in server response.")
                    else:
                        # Download from presigned URL
                        with st.spinner("Downloading file from S3..."):
                            file_resp = requests.get(presigned_url, timeout=300)
                            if file_resp.status_code == 200:
                                st.success("File ready. Click below to save it.")
                                st.download_button(
                                    label=f"⬇️ Save {filename}",
                                    data=file_resp.content,
                                    file_name=filename,
                                    mime="application/zip",
                                    key="save_btn",
                                )
                            else:
                                st.error(f"Failed to download from S3: {file_resp.status_code}")
                                
                elif resp.status_code == 400:
                    st.error(f"Invalid request: {resp.text[:200]}")
                elif resp.status_code == 403:
                    st.error("❌ Authentication failed. Please check your authorization token.")
                elif resp.status_code == 404:
                    st.error("Artifact not found (404). Verify the artifact ID exists in DynamoDB.")
                elif resp.status_code == 500:
                    st.error(f"❌ Server error: {resp.text[:200]}")
                else:
                    st.error(f"Server returned {resp.status_code}: {resp.text[:200]}")
            except requests.RequestException as e:
                st.error(f"Request failed: {e}")
            except Exception as ex:
                st.error(f"Unexpected error: {ex}")

with st.expander("Tips"):
    st.markdown(
        "- Start Flask: `python acmecli/baseline/backend.py`\n"
        "- Start Streamlit: `streamlit run acmecli/baseline/streamlit_ui.py`\n"
        "- The app uses the `/artifacts/<type>/<id>` endpoint which returns a presigned S3 URL for download."
    )

# ---- Cost UI ----
st.divider()
st.header("💰 Artifact Cost Calculator")

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
        st.error("Please enter an artifact ID.")
    else:
        # Build the cost endpoint URL
        url = f"{backend_url}/artifact/{quote(cost_artifact_type, safe='')}/{quote(cost_artifact_id, safe='')}/cost"
        if include_dependencies:
            url += "?dependency=true"
        
        with st.spinner("Calculating cost..."):
            try:
                # Send default token (backend requires X-Authorization header)
                headers = {"X-Authorization": "baseline"}
                resp = requests.get(url, headers=headers, timeout=60)
                
                if resp.status_code == 200:
                    cost_data = resp.json()
                    
                    if cost_artifact_id in cost_data:
                        artifact_cost = cost_data[cost_artifact_id]
                        total_cost = artifact_cost.get("total_cost", 0)
                        standalone_cost = artifact_cost.get("standalone_cost")
                        
                        st.success("✅ Cost calculated successfully!")
                        
                        # Display cost information
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Total Cost", f"{total_cost} MB")
                        if standalone_cost is not None:
                            with col2:
                                st.metric("Standalone Cost", f"{standalone_cost} MB")
                    else:
                        st.error(f"Unexpected response format: {cost_data}")
                        
                elif resp.status_code == 400:
                    st.error(f"Invalid request: {resp.text[:200]}")
                elif resp.status_code == 403:
                    st.error("❌ Authentication failed. Please check your authorization token.")
                elif resp.status_code == 404:
                    st.error("Artifact not found (404). Verify the artifact ID exists in S3.")
                elif resp.status_code == 500:
                    st.error(f"❌ Server error: {resp.text[:200]}")
                else:
                    st.error(f"Unexpected response ({resp.status_code}): {resp.text[:200]}")
                    
            except requests.RequestException as e:
                st.error(f"Request failed: {e}")
            except Exception as ex:
                st.error(f"Unexpected error: {ex}")

# ---- Upload UI (Create) ----
st.divider()
st.header("📤 Upload a Model Package")

col1, col2 = st.columns(2)
with col1:
    target_prefix = st.selectbox("Target top-level folder (S3 prefix)", options=model_folders, index=0)
with col2:
    version = st.text_input("Version (semver)", "1.0.0")

model_name = st.text_input("Model name (e.g., org/model)", "")
uploaded_file = st.file_uploader("Choose model ZIP", type=["zip"])

if st.button("Upload to S3", type="primary"):
    if not model_name or "/" not in model_name:
        st.error("Please enter model name like 'org/model'.")
    elif not uploaded_file:
        st.error("Please choose a .zip file.")
    else:
        try:
            org, name = model_name.split("/", 1)
            key = f"{target_prefix}/{org}/{name}/{version}/model.zip"

            blob = uploaded_file.getvalue()
            # Compute hash for integrity display
            sha256 = hashlib.sha256(blob).hexdigest()

            # Quick safety checks to avoid bad archives
            _safe_zip_check(blob)

            # Upload to S3
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=blob,
                ContentType="application/zip",
            )

            # Confirm upload and show metadata
            head = s3_client.head_object(Bucket=S3_BUCKET, Key=key)
            size = head.get("ContentLength", 0)
            etag = head.get("ETag", "").strip('"')

            st.success(f"✅ Uploaded {model_name}@{version} → s3://{S3_BUCKET}/{key}")
            st.write(f"- Size: **{size} bytes**")
            st.write(f"- ETag: `{etag}`")
            st.write(f"- SHA-256: `{sha256}`")

        except ClientError as e:
            st.error(f"AWS error: {e}")
        except ValueError as ve:
            st.error(f"Validation error: {ve}")
        except Exception as ex:
            st.error(f"Unexpected error: {ex}")

# ---- Reset Registry UI ----
st.divider()
st.header("🔄 Reset Registry")

st.warning("⚠️ **Danger Zone**: This will delete ALL artifacts from the S3 bucket. This action cannot be undone!")

if st.button("Reset Registry", type="primary"):
    with st.spinner("Resetting registry... This may take a moment."):
        try:
            url = f"{backend_url}/reset"
            # Reset endpoint requires "admin" token
            headers = {"X-Authorization": "admin"}
            resp = requests.delete(url, headers=headers, timeout=300)
            
            if resp.status_code == 200:
                st.success("✅ Registry reset successfully!")
                st.info("All artifacts have been deleted from the S3 bucket. Please refresh the page to see the updated state.")
                # Force refresh of model folders
                st.rerun()
            elif resp.status_code == 401:
                st.error("❌ Permission denied. Admin token required to reset the registry.")
            elif resp.status_code == 403:
                st.error("❌ Authentication failed. Please check your authorization token.")
            elif resp.status_code == 500:
                st.error(f"❌ Server error: {resp.text[:200]}")
            else:
                st.error(f"❌ Unexpected response ({resp.status_code}): {resp.text[:200]}")
        except requests.RequestException as e:
            st.error(f"❌ Request failed: {e}")
        except Exception as ex:
            st.error(f"❌ Unexpected error: {ex}")