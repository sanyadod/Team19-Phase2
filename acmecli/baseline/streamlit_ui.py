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
st.write("Select a model folder and part, then download from the Flask server.")

# Backend base URL (Flask server)
DEFAULT_BACKEND = "http://127.0.0.1:5001"
backend_url = st.text_input("Backend URL", value=DEFAULT_BACKEND)

# Load available top-level prefixes (model folders)
with st.spinner("Loading model folders from S3..."):
    model_folders = list_top_level_prefixes()

if not model_folders:
    st.warning("No top-level folders found in the S3 bucket.")
    st.stop()

# ---- Download UI ----
model = st.selectbox("Model folder", options=model_folders, index=0)
part = st.selectbox("Part", options=["dataset", "weights", "all"], index=0)

st.divider()

if st.button("Download from server", type="primary"):
    if not model:
        st.error("Please select a model folder.")
    else:
        # artifact_type is the selected folder; artifact_id is placeholder '_' for flat layout
        url = f"{backend_url}/artifact/{quote(model, safe='')}/_/download?part={part}"
        with st.spinner("Requesting file from server..."):
            try:
                resp = requests.get(url, timeout=60)
                if resp.status_code == 200:
                    cd = resp.headers.get("Content-Disposition", "")
                    filename = "download.bin"
                    if "filename=" in cd:
                        filename = cd.split("filename=")[-1].strip('"')

                    st.success("File ready. Click below to save it.")
                    st.download_button(
                        label=f"⬇️ Save {filename}",
                        data=resp.content,
                        file_name=filename,
                        mime=resp.headers.get("Content-Type", "application/octet-stream"),
                        key="save_btn",
                    )
                elif resp.status_code == 404:
                    st.error("File not found (404). Try another part or verify S3 contents.")
                else:
                    st.error(f"Server returned {resp.status_code}: {resp.text[:200]}")
            except requests.RequestException as e:
                st.error(f"Request failed: {e}")

with st.expander("Tips"):
    st.markdown(
        "- Start Flask: `python acmecli/baseline/download.py`\n"
        "- Start Streamlit: `streamlit run acmecli/baseline/streamlit_ui.py`\n"
        "- The app resolves both nested (<type>/<id>/file) and flat (<type>/file) layouts dynamically."
    )

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
