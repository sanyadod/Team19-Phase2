
import streamlit as st
import requests
import boto3
from botocore.exceptions import ClientError
from urllib.parse import quote
from typing import List

# ---- S3 config (used for listing top-level "models" only) ----
S3_BUCKET = "ece-registry"
s3_client = boto3.client("s3", region_name="us-east-1")


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


# ---- Page config ----
st.set_page_config(page_title="Artifact Download", page_icon="📦", layout="centered")

st.title("Artifact Download")
st.write("Select a model folder and part, then download from the Flask server.")

# Backend base URL (Flask server)
DEFAULT_BACKEND = "http://127.0.0.1:5001"
backend_url = st.text_input("Backend URL", value=DEFAULT_BACKEND)

# Single dropdown for model (top-level prefix) selection
with st.spinner("Loading model folders from S3..."):
    model_folders = list_top_level_prefixes()

if not model_folders:
    st.warning("No top-level folders found in the S3 bucket.")
    st.stop()

model = st.selectbox("Model folder", options=model_folders, index=0)

# Part selection
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