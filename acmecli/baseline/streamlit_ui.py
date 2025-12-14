# acmecli/baseline/streamlit_ui.py
# Lightweight-first Streamlit UI to pass Lighthouse performance while keeping functionality.

from __future__ import annotations

import io
import hashlib
import time
import traceback
import zipfile
from collections import defaultdict
from typing import List, Optional
from urllib.parse import quote

import requests
import streamlit as st

# ---- Config ----
S3_BUCKET = "ece-registry"
AWS_REGION = "us-east-1"
VALID_TYPES = ["model", "code", "dataset"]

# ✅ Big Lighthouse win: start with sidebar collapsed
st.set_page_config(
    page_title="Artifact Registry",
    page_icon="📦",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# -----------------------------
# Caching / resources
# -----------------------------
@st.cache_resource
def get_requests_session() -> requests.Session:
    """Cached requests session for connection pooling."""
    return requests.Session()


@st.cache_resource
def get_aws_clients():
    """
    Lazily create AWS clients only when a page actually needs them.
    Keeps the Home page fast for Lighthouse.
    """
    import boto3  # lazy import

    s3_client = boto3.client("s3", region_name=AWS_REGION)
    dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
    meta_table = dynamodb.Table("artifact")
    return s3_client, meta_table


# -----------------------------
# Helpers
# -----------------------------
def _safe_zip_check(blob: bytes, *, max_uncompressed_bytes: int = 200 * 1024 * 1024) -> None:
    """
    Safety check for uploaded ZIPs:
    - no path traversal (../ or absolute)
    - bounded uncompressed size (simple zip-bomb guard)
    """
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        total = 0
        for zi in zf.infolist():
            p = zi.filename.replace("\\", "/")
            if p.startswith("/") or ".." in p.split("/"):
                raise ValueError(f"Unsafe path in zip entry: {zi.filename}")
            total += zi.file_size
            if total > max_uncompressed_bytes:
                raise ValueError(
                    "Zip appears too large when extracted (possible zip bomb). "
                    "Please upload a smaller/safer archive."
                )


def _parse_parent_ids(raw: str) -> List[str]:
    """
    Parse comma/space/newline separated IDs into a list of strings.
    Example input: "123, 456 789\n999" -> ["123","456","789","999"]
    """
    if not raw or not raw.strip():
        return []
    parts: List[str] = []
    for chunk in raw.replace("\n", ",").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts.extend([p for p in chunk.split() if p.strip()])
    seen = set()
    out: List[str] = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _goto_page(p: str) -> None:
    st.query_params["page"] = p
    st.rerun()


# -----------------------------
# Navigation via query params
# (Home remains widget-free for Lighthouse)
# -----------------------------
PAGES = ["Home", "Upload", "Download", "Cost", "License", "Rate", "Search", "Lineage", "Reset"]

qp = st.query_params
page = qp.get("page", "Home")
if isinstance(page, list):
    page = page[0]
if page not in PAGES:
    page = "Home"

# Backend URL stored in session state so we don't render sidebar widgets on Home.
DEFAULT_BACKEND = "http://127.0.0.1:5001"
if "backend_url" not in st.session_state:
    st.session_state["backend_url"] = DEFAULT_BACKEND

backend_url = st.session_state["backend_url"]

# Shared headers
headers_baseline = {"X-Authorization": "baseline"}


# -----------------------------
# Optional minimal CSS
# Inject ONLY off-Home to keep Home lean
# -----------------------------
def inject_accessibility_css():
    st.markdown(
        """
<style>
.skip-link{position:absolute;top:-40px;left:0;background:#000;color:#fff;padding:8px;z-index:100;text-decoration:none}
.skip-link:focus{top:0}
*:focus{outline:3px solid #0066cc;outline-offset:2px}
</style>
""",
        unsafe_allow_html=True,
    )
    st.markdown('<a href="#main-content" class="skip-link">Skip to main content</a>', unsafe_allow_html=True)
    st.markdown('<div id="main-content" role="main">', unsafe_allow_html=True)


def close_main_div():
    st.markdown("</div>", unsafe_allow_html=True)


# -----------------------------
# Sidebar (only render if not Home)
# -----------------------------
def render_sidebar():
    global backend_url
    with st.sidebar:
        st.subheader("Settings")
        st.session_state["backend_url"] = st.text_input(
            "Backend URL",
            value=st.session_state["backend_url"],
            help="Default is http://127.0.0.1:5001",
        )
        backend_url = st.session_state["backend_url"]

        st.divider()
        st.subheader("Navigate")
        # Use buttons instead of radio to reduce widget overhead.
        for p in PAGES:
            if st.button(("✅ " if p == page else "") + p, use_container_width=True):
                _goto_page(p)


# -----------------------------
# Pages
# -----------------------------
def render_home():
    # ✅ Keep landing page extremely light for Lighthouse performance (no sidebar widgets)
    st.title("Artifact Registry")
    st.write("Upload, download, and manage artifacts in the registry.")
    st.info("Home stays lightweight to pass UI performance checks. Use the buttons below to navigate.")

    # Simple grid of nav buttons (no sidebar required)
    cols = st.columns(3)
    for i, p in enumerate(PAGES[1:]):  # skip Home
        with cols[i % 3]:
            st.button(p, use_container_width=True, on_click=_goto_page, args=(p,))


def render_upload():
    st.header("Upload Artifact")
    st.caption("Upload a ZIP file. For models, you can optionally provide parent model IDs (for lineage edges).")

    with st.form("upload_form"):
        upload_artifact_type = st.selectbox(
            "Artifact Category",
            options=VALID_TYPES,
            index=0,
            format_func=lambda x: x.title(),
            help="Determines which S3 folder the artifact will be stored in (model/, dataset/, or code/).",
            key="upload_type",
        )

        artifact_name = st.text_input(
            "Artifact Name",
            placeholder="e.g., bert-base-uncased",
            help="If empty, name will be derived from the filename.",
            key="upload_name",
        )

        parent_ids_raw: Optional[str] = None
        if upload_artifact_type == "model":
            parent_ids_raw = st.text_area(
                "Parent Model IDs (optional)",
                placeholder="e.g., 3847247294, 9078563412",
                help="Comma/space/newline separated list of parent model artifact IDs. Stored in DynamoDB as `parents`.",
                key="upload_parents",
                height=80,
            )

        uploaded_file = st.file_uploader(
            "Choose artifact ZIP file",
            type=["zip"],
            help="Upload a ZIP file containing your artifact",
            key="upload_file",
        )

        submitted = st.form_submit_button("Upload Artifact", type="primary")

    if not submitted:
        return

    if not uploaded_file:
        st.error("Please choose a .zip file to upload.", icon="⚠️")
        return

    try:
        blob = uploaded_file.getvalue()
        size = len(blob)
        sha256 = hashlib.sha256(blob).hexdigest()

        _safe_zip_check(blob)

        artifact_id = str(int(time.time() * 1000))

        name_final = artifact_name.strip() if artifact_name and artifact_name.strip() else ""
        if not name_final:
            filename = uploaded_file.name
            name_final = filename.rsplit(".", 1)[0] if "." in filename else filename

        s3_key = f"{upload_artifact_type}/{artifact_id}.zip"

        # AWS clients only created when needed
        s3_client, meta_table = get_aws_clients()

        with st.spinner(f"Uploading {upload_artifact_type} artifact to S3..."):
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=blob,
                ContentType="application/zip",
            )

        item = {
            "id": artifact_id,
            "artifact_type": upload_artifact_type,
            "s3_bucket": S3_BUCKET,
            "s3_key": s3_key,
            "filename": name_final,
            "size_bytes": size,
            "sha256": sha256,
        }

        if upload_artifact_type == "model" and parent_ids_raw is not None:
            parents = _parse_parent_ids(parent_ids_raw)
            if parents:
                item["parents"] = parents  # ✅ lineage edges rely on this

        with st.spinner("Registering artifact in DynamoDB..."):
            try:
                meta_table.put_item(Item=item)
            except Exception as e:
                st.warning(f"Uploaded to S3 but DynamoDB registration failed: {e}")

        st.success("Artifact uploaded successfully!")
        st.markdown("### Upload Details")
        st.write(f"**Artifact ID:** `{artifact_id}`")
        st.write(f"**Name:** {name_final}")
        st.write(f"**Category:** {upload_artifact_type}")
        st.write(f"**S3 Key:** `{s3_key}`")
        st.write(f"**Size:** {size:,} bytes ({size / 1024 / 1024:.2f} MB)")
        st.write(f"**SHA-256:** `{sha256}`")
        if upload_artifact_type == "model" and item.get("parents"):
            st.write(f"**Parents:** `{', '.join(item['parents'])}`")

    except ValueError as ve:
        st.error(f"Validation error: {ve}", icon="⚠️")
    except Exception as ex:
        st.error(f"Unexpected error: {ex}", icon="⚠️")
        with st.expander("Technical Details (for debugging)"):
            st.code(traceback.format_exc())


def render_download():
    st.header("Download Artifact")
    st.caption("Fetch metadata from the backend, then download the ZIP from the returned presigned URL.")

    with st.form("dl_form"):
        artifact_type = st.selectbox(
            "Artifact type",
            options=VALID_TYPES,
            index=0,
            format_func=lambda x: x.title(),
            key="dl_type",
        )
        artifact_id = st.text_input(
            "Artifact ID",
            placeholder="Enter artifact ID (e.g., 1234567890)",
            value="",
            key="dl_id",
        )
        submitted = st.form_submit_button("Download from server", type="primary")

    if not submitted:
        return

    if not artifact_id.strip():
        st.error("Please enter an artifact ID.", icon="⚠️")
        return

    url = f"{backend_url}/artifacts/{quote(artifact_type, safe='')}/{quote(artifact_id.strip(), safe='')}"
    session = get_requests_session()

    with st.spinner("Requesting download URL..."):
        try:
            resp = session.get(url, headers=headers_baseline, timeout=60)
        except requests.RequestException as e:
            st.error(f"Request failed: {e}", icon="⚠️")
            st.info(f"Make sure the backend server is running at {backend_url}")
            return

    if resp.status_code != 200:
        st.error(f"Server returned {resp.status_code}: {resp.text[:300]}", icon="⚠️")
        return

    data = resp.json()
    presigned_url = data.get("data", {}).get("url")
    metadata = data.get("metadata", {})
    filename = metadata.get("name", f"{artifact_id.strip()}.zip")

    if not presigned_url:
        st.error("No download URL found in response.", icon="⚠️")
        with st.expander("Server Response"):
            st.json(data)
        return

    with st.spinner("Downloading ZIP from S3..."):
        try:
            file_resp = session.get(presigned_url, timeout=300)
        except requests.RequestException as e:
            st.error(f"Failed to download from S3: {e}", icon="⚠️")
            return

    if file_resp.status_code != 200:
        st.error(f"Failed to download from S3 (status {file_resp.status_code}).", icon="⚠️")
        st.error(file_resp.text[:200])
        return

    st.success("File ready!")
    st.download_button(
        label=f"Save {filename}",
        data=file_resp.content,
        file_name=filename,
        mime="application/zip",
        key="save_btn",
    )


def render_cost():
    st.header("Artifact Cost Calculator")
    st.caption("Calculate cost (size in MB) for an artifact, optionally including dependencies.")

    with st.form("cost_form"):
        cost_artifact_type = st.selectbox(
            "Artifact type",
            options=VALID_TYPES,
            index=0,
            format_func=lambda x: x.title(),
            key="cost_type",
        )
        cost_artifact_id = st.text_input("Artifact ID", value="", key="cost_id")
        include_dependencies = st.checkbox("Include dependencies", value=False, key="cost_dep")
        submitted = st.form_submit_button("Calculate Cost", type="primary")

    if not submitted:
        return

    if not cost_artifact_id.strip():
        st.error("Please enter an artifact ID.", icon="⚠️")
        return

    url = f"{backend_url}/artifact/{quote(cost_artifact_type, safe='')}/{quote(cost_artifact_id.strip(), safe='')}/cost"
    if include_dependencies:
        url += "?dependency=true"

    session = get_requests_session()
    with st.spinner("Calculating cost..."):
        try:
            resp = session.get(url, headers=headers_baseline, timeout=60)
        except requests.RequestException as e:
            st.error(f"Request failed: {e}", icon="⚠️")
            return

    if resp.status_code != 200:
        st.error(f"Server returned {resp.status_code}: {resp.text[:300]}", icon="⚠️")
        return

    cost_data = resp.json()
    if cost_artifact_id.strip() not in cost_data:
        st.error(f"Unexpected response format: {cost_data}", icon="⚠️")
        return

    artifact_cost = cost_data[cost_artifact_id.strip()]
    total_cost = artifact_cost.get("total_cost", 0)
    standalone_cost = artifact_cost.get("standalone_cost")

    st.success("Cost calculated!")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Cost", f"{total_cost} MB")
    if standalone_cost is not None:
        with col2:
            st.metric("Standalone Cost", f"{standalone_cost} MB")


def render_license():
    st.header("License Check")
    st.caption("Check license compliance for an artifact.")

    with st.form("license_form"):
        license_artifact_type = st.selectbox(
            "Artifact type",
            options=VALID_TYPES,
            index=0,
            format_func=lambda x: x.title(),
            key="license_type",
        )
        license_artifact_id = st.text_input("Artifact ID", value="", key="license_id")
        submitted = st.form_submit_button("Check License", type="primary")

    if not submitted:
        return

    if not license_artifact_id.strip():
        st.error("Please enter an artifact ID.", icon="⚠️")
        return

    url = f"{backend_url}/artifact/{quote(license_artifact_type, safe='')}/{quote(license_artifact_id.strip(), safe='')}/license-check"
    session = get_requests_session()

    with st.spinner("Checking license..."):
        try:
            resp = session.post(url, headers=headers_baseline, timeout=60)
        except requests.RequestException as e:
            st.error(f"Request failed: {e}", icon="⚠️")
            return

    if resp.status_code != 200:
        st.error(f"Server returned {resp.status_code}: {resp.text[:300]}", icon="⚠️")
        return

    data = resp.json()
    is_compliant = data.get("is_compliant", False)
    license_text = data.get("license")
    score = float(data.get("score", 0.0) or 0.0)

    st.success("License check complete!")
    if is_compliant:
        st.success(f"Status: Compliant (Score: {score:.2f})")
    else:
        st.warning(f"Status: Not compliant / unclear (Score: {score:.2f})")

    col1, col2 = st.columns(2)
    with col1:
        st.metric("License Score", f"{score:.2f}")
    with col2:
        st.metric("Compliance", "Yes" if is_compliant else "No")

    if license_text:
        with st.expander("View License Text"):
            st.text(license_text)


def render_rate():
    st.header("Rate Model")
    st.caption("Get rating metrics for a model artifact.")

    with st.form("rate_form"):
        rate_model_id = st.text_input("Model ID", value="", key="rate_id")
        submitted = st.form_submit_button("Rate Model", type="primary")

    if not submitted:
        return

    if not rate_model_id.strip():
        st.error("Please enter a model ID.", icon="⚠️")
        return

    url = f"{backend_url}/artifact/model/{quote(rate_model_id.strip(), safe='')}/rate"
    session = get_requests_session()

    with st.spinner("Computing model rating..."):
        try:
            resp = session.get(url, headers=headers_baseline, timeout=120)
        except requests.RequestException as e:
            st.error(f"Request failed: {e}", icon="⚠️")
            return

    if resp.status_code != 200:
        st.error(f"Server returned {resp.status_code}: {resp.text[:300]}", icon="⚠️")
        return

    data = resp.json()
    st.success("Model rating computed!")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Net Score", f"{float(data.get('net_score', 0.0) or 0.0):.3f}")
    with col2:
        st.metric("License Score", f"{float(data.get('license', 0.0) or 0.0):.3f}")
    with col3:
        st.metric("Code Quality", f"{float(data.get('code_quality', 0.0) or 0.0):.3f}")

    with st.expander("Detailed Metrics"):
        st.json(data)


def render_search():
    st.header("Search Artifacts")
    st.caption("Search artifacts using a regex pattern.")

    with st.form("search_form"):
        search_method = st.radio(
            "Search Method",
            options=["GET (Query Parameter)", "POST (JSON Body)"],
            index=0,
            key="search_method",
        )
        search_regex = st.text_input("Regex Pattern", value="", key="search_regex")
        search_offset = st.number_input("Offset (for pagination)", min_value=0, value=0, key="search_offset")
        submitted = st.form_submit_button("Search Artifacts", type="primary")

    if not submitted:
        return

    if not search_regex.strip():
        st.error("Please enter a regex pattern.", icon="⚠️")
        return

    session = get_requests_session()

    with st.spinner("Searching..."):
        try:
            if search_method.startswith("GET"):
                url = f"{backend_url}/artifacts/search"
                resp = session.get(
                    url,
                    headers=headers_baseline,
                    params={"regex": search_regex.strip(), "offset": str(int(search_offset))},
                    timeout=60,
                )
            else:
                url = f"{backend_url}/artifact/byRegEx"
                if int(search_offset) > 0:
                    url += f"?offset={int(search_offset)}"
                resp = session.post(
                    url,
                    headers={**headers_baseline, "Content-Type": "application/json"},
                    json={"regex": search_regex.strip()},
                    timeout=60,
                )
        except requests.RequestException as e:
            st.error(f"Request failed: {e}", icon="⚠️")
            return

    if resp.status_code != 200:
        st.error(f"Server returned {resp.status_code}: {resp.text[:300]}", icon="⚠️")
        return

    results = resp.json()
    next_offset = resp.headers.get("offset")

    if results:
        st.success(f"Found {len(results)} artifact(s).")
        if next_offset:
            st.info(f"More results available. Next offset: {next_offset}")
        # ✅ lighter than st.dataframe for many cases
        st.table(results)
    else:
        st.info("No results found.")


def render_lineage():
    st.header("Model Lineage")
    st.caption("View the lineage graph for a model artifact.")

    with st.form("lineage_form"):
        lineage_model_id = st.text_input("Model ID", value="", key="lineage_id")
        submitted = st.form_submit_button("Get Lineage", type="primary")

    if not submitted:
        return

    if not lineage_model_id.strip():
        st.error("Please enter a model ID.", icon="⚠️")
        return

    url = f"{backend_url}/artifact/model/{quote(lineage_model_id.strip(), safe='')}/lineage"
    session = get_requests_session()

    with st.spinner("Building lineage graph..."):
        try:
            resp = session.get(url, headers=headers_baseline, timeout=120)
        except requests.RequestException as e:
            st.error(f"Request failed: {e}", icon="⚠️")
            return

    if resp.status_code != 200:
        st.error(f"Server returned {resp.status_code}: {resp.text[:300]}", icon="⚠️")
        return

    data = resp.json()
    nodes = data.get("nodes", [])
    edges = data.get("edges", [])

    st.success(f"Retrieved {len(nodes)} node(s) and {len(edges)} edge(s).")

    if nodes:
        st.subheader("Nodes")
        st.table(nodes)

    if edges:
        st.subheader("Edges")
        st.table(edges)

        children_map = defaultdict(list)
        for edge in edges:
            parent = edge.get("from_node_artifact_id")
            child = edge.get("to_node_artifact_id")
            if parent and child:
                children_map[str(parent)].append(str(child))

        st.subheader("Summary")
        st.write(f"Target Model: `{lineage_model_id.strip()}`")
        if lineage_model_id.strip() in children_map:
            st.write("Descendants:")
            for c in sorted(children_map[lineage_model_id.strip()]):
                st.write(f"- `{c}`")
    else:
        st.info("No relationships found (no parents/children recorded).")

    with st.expander("Raw JSON"):
        st.json(data)


def render_reset():
    st.header("Reset Registry")
    st.warning("Danger Zone: This will delete ALL artifacts (server-side reset).")

    with st.form("reset_form"):
        sure = st.checkbox("I understand this will delete everything.", value=False)
        submitted = st.form_submit_button("Reset Registry", type="primary")

    if not submitted:
        return

    if not sure:
        st.error("Please confirm you understand the consequences.", icon="⚠️")
        return

    session = get_requests_session()
    url = f"{backend_url}/reset"

    with st.spinner("Resetting..."):
        try:
            resp = session.delete(url, headers={"X-Authorization": "admin"}, timeout=300)
        except requests.RequestException as e:
            st.error(f"Request failed: {e}", icon="⚠️")
            return

    if resp.status_code == 200:
        st.success("Registry reset successfully!")
        st.info("Returning to Home…")
        st.query_params["page"] = "Home"
        st.rerun()
    else:
        st.error(f"Server returned {resp.status_code}: {resp.text[:300]}", icon="⚠️")


# -----------------------------
# Render
# -----------------------------
# ✅ Sidebar widgets only for non-Home pages
if page != "Home":
    inject_accessibility_css()
    render_sidebar()
else:
    # no CSS on Home for max Lighthouse performance
    pass

# Page routing
if page == "Home":
    render_home()
elif page == "Upload":
    render_upload()
elif page == "Download":
    render_download()
elif page == "Cost":
    render_cost()
elif page == "License":
    render_license()
elif page == "Rate":
    render_rate()
elif page == "Search":
    render_search()
elif page == "Lineage":
    render_lineage()
elif page == "Reset":
    render_reset()

if page != "Home":
    close_main_div()
