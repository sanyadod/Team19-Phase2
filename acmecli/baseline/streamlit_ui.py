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
st.set_page_config(page_title="Artifact Registry", page_icon="📦", layout="centered")

st.title("Artifact Registry")
st.write("Upload, download, and manage artifacts in the registry.")

# Backend base URL (Flask server)
DEFAULT_BACKEND = "http://127.0.0.1:5001"
backend_url = st.text_input("Backend URL", value=DEFAULT_BACKEND)

# ---- Upload UI (Create) ----
st.divider()
st.header("📤 Upload Artifact")

st.write("Upload an artifact file. Select the category to determine which folder it will be stored in (model/, dataset/, or code/).")

# Category selection - determines which folder (model/, dataset/, or code/)
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
        st.error("Please choose a .zip file to upload.")
    else:
        try:
            import time
            
            blob = uploaded_file.getvalue()
            size = len(blob)
            
            # Compute hash for integrity
            sha256 = hashlib.sha256(blob).hexdigest()
            
            # Quick safety checks to avoid bad archives
            _safe_zip_check(blob)
            
            # Generate artifact ID (timestamp-based, matching upload.py)
            artifact_id = str(int(time.time() * 1000))
            
            # Extract name from filename if not provided
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
            
            # Register in DynamoDB
            dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
            meta_table = dynamodb.Table("artifact")
            
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
            st.success(f"✅ Artifact uploaded successfully!")
            st.write(f"**Artifact ID:** `{artifact_id}`")
            st.write(f"**Name:** {artifact_name.strip()}")
            st.write(f"**Category:** {upload_artifact_type}")
            st.write(f"**Stored in:** `{upload_artifact_type}/` folder")
            st.write(f"**S3 Key:** `{s3_key}`")
            st.write(f"**Size:** {size:,} bytes ({size / 1024 / 1024:.2f} MB)")
            st.write(f"**SHA-256:** `{sha256}`")
            
            st.info(f"💡 The artifact has been stored in the `{upload_artifact_type}/` folder in S3 and registered in DynamoDB.")

        except ValueError as ve:
            st.error(f"❌ Validation error: {ve}")
        except ClientError as e:
            st.error(f"❌ AWS error: {e}")
        except Exception as ex:
            st.error(f"❌ Unexpected error: {ex}")

# ---- Download UI ----
st.divider()
st.header("⬇️ Download Artifact")

st.write("Enter the artifact ID (the numeric ID shown after upload) to download the artifact.")

artifact_type = st.selectbox("Artifact type", options=VALID_TYPES, index=0, format_func=lambda x: x.title())

artifact_id = st.text_input(
    "Artifact ID", 
    placeholder="Enter artifact ID (e.g., 1234567890)",
    value="",
    help="The artifact ID is the numeric ID generated when you upload an artifact. It's shown in the upload success message."
)

if st.button("Download from server", type="primary"):
    if not artifact_id or not artifact_id.strip():
        st.error("Please enter an artifact ID.")
    else:
        artifact_id = artifact_id.strip()
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
                        st.json(data)  # Show full response for debugging
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
                                st.error(f"Response: {file_resp.text[:200]}")
                                
                elif resp.status_code == 400:
                    error_text = resp.text[:500] if resp.text else "Invalid request"
                    st.error(f"❌ Invalid request: {error_text}")
                    st.info(f"Make sure the artifact ID is valid (alphanumeric, hyphens, dots, underscores only)")
                elif resp.status_code == 403:
                    st.error("❌ Authentication failed. Please check your authorization token.")
                elif resp.status_code == 404:
                    st.error("❌ Artifact not found (404).")
                    st.info(f"Verify that:\n- The artifact ID '{artifact_id}' exists in DynamoDB\n- The artifact type '{artifact_type}' matches the uploaded category\n- The artifact was successfully uploaded")
                elif resp.status_code == 500:
                    error_text = resp.text[:500] if resp.text else "Server error"
                    st.error(f"❌ Server error: {error_text}")
                else:
                    error_text = resp.text[:500] if resp.text else f"Unexpected response ({resp.status_code})"
                    st.error(f"❌ Server returned {resp.status_code}: {error_text}")
            except requests.RequestException as e:
                st.error(f"❌ Request failed: {e}")
                st.info(f"Make sure the backend server is running at {backend_url}")
            except Exception as ex:
                st.error(f"❌ Unexpected error: {ex}")
                import traceback
                st.code(traceback.format_exc())

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