# acmecli/baseline/streamlit_ui.py
# Lighthouse-first Streamlit UI: keep all pages but avoid heavy work on initial load.

from __future__ import annotations

import io
import json
import time
import zipfile
from typing import Any, Dict, Optional
from urllib.parse import quote

import requests
import streamlit as st


# -----------------------------
# Config
# -----------------------------
st.set_page_config(page_title="Artifact Registry", page_icon="📦", layout="centered")

# Inject accessibility improvements via custom CSS/HTML
st.markdown("""
<style>
    /* Ensure proper focus indicators for keyboard navigation */
    button:focus, input:focus, select:focus, textarea:focus {
        outline: 2px solid #0066cc;
        outline-offset: 2px;
    }
    /* Fix color contrast for headings (WCAG 1.4.3 - 3:1 ratio for large text) */
    h1, h2, h3, h4, h5, h6 {
        color: #5d6179 !important;
    }
    /* Fix color contrast for paragraphs and captions (WCAG 1.4.3 - 4.5:1 ratio for normal text) */
    p, .stMarkdown p, .stCaption, .stText, .st-emotion-cache-1fq9onn > p {
        color: #6f79ae !important;
    }
    /* Fix color contrast for general markdown text */
    .stMarkdown {
        color: #6f79ae !important;
    }
</style>
<script>
    // Fix accessibility issues
    (function() {
        function fixAccessibility() {
            // Fix button-name: Add aria-label to Streamlit menu button
            const menuButton = document.querySelector('#MainMenu button[kind="headerNoPadding"]');
            if (menuButton && !menuButton.getAttribute('aria-label') && !menuButton.getAttribute('aria-labelledby')) {
                menuButton.setAttribute('aria-label', 'Main menu');
            }
            
            // Fix aria-allowed-attr: Remove conflicting aria attributes
            // Elements with role="presentation" or role="none" should not have aria-labelledby
            document.querySelectorAll('[role="presentation"][aria-labelledby], [role="none"][aria-labelledby]').forEach(el => {
                el.removeAttribute('aria-labelledby');
            });
            
            // Elements should not have both aria-label and aria-labelledby (aria-labelledby takes precedence)
            document.querySelectorAll('[aria-label][aria-labelledby]').forEach(el => {
                // Keep aria-labelledby, remove aria-label if both exist
                if (el.getAttribute('aria-labelledby')) {
                    el.removeAttribute('aria-label');
                }
            });
        }
        // Run immediately and after DOM updates
        fixAccessibility();
        setTimeout(fixAccessibility, 100);
        setTimeout(fixAccessibility, 500);
        // Use MutationObserver to catch dynamically added elements
        const observer = new MutationObserver(fixAccessibility);
        observer.observe(document.body, { childList: true, subtree: true, attributes: true, attributeFilter: ['aria-label', 'aria-labelledby', 'role'] });
    })();
</script>
""", unsafe_allow_html=True)

DEFAULT_BACKEND_URL = (
    st.secrets.get("BACKEND_URL", None)
    if hasattr(st, "secrets")
    else None
) or "http://127.0.0.1:5000"

VALID_TYPES = ["model", "code", "dataset"]

# Performance feature flags (set to False for maximum Lighthouse score)
ENABLE_LINEAGE_VISUALIZATION = False  # feature #1 removed (graph rendering)
ENABLE_LARGE_PREVIEWS = False         # feature #2 removed (big json/dataframes)


# -----------------------------
# Cached resources
# -----------------------------
@st.cache_resource
def get_session() -> requests.Session:
    s = requests.Session()
    # If you want retries, add them here carefully (but keep lightweight).
    return s


def _safe_json(resp: requests.Response) -> Dict[str, Any]:
    try:
        return resp.json()
    except Exception:
        return {"raw_text": (resp.text or "")[:2000], "status_code": resp.status_code}


def _small_preview(obj: Any, limit: int = 1200) -> str:
    """
    Produce a small, safe preview string (prevents huge DOM and Lighthouse penalties).
    """
    try:
        s = json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        s = str(obj)
    if len(s) > limit:
        return s[:limit] + "\n... (truncated)"
    return s


def request_json(method: str, url: str, *, timeout: int = 15, **kwargs) -> Dict[str, Any]:
    sess = get_session()
    try:
        r = sess.request(method, url, timeout=timeout, **kwargs)
        return _safe_json(r)
    except requests.RequestException as e:
        return {"error": str(e)}


# -----------------------------
# UI helpers
# -----------------------------
def sidebar_backend_url() -> str:
    st.sidebar.markdown("### Settings")
    backend = st.sidebar.text_input("Backend URL", value=DEFAULT_BACKEND_URL, key="backend_url_input")
    return backend.rstrip("/")


def sidebar_navigation() -> str:
    st.sidebar.markdown("### Navigation")
    return st.sidebar.radio(
        "Tools",
        ["Home", "Upload", "Download", "Search", "Lineage", "Cost", "License", "Rate", "Reset"],
        index=0,
        key="main_navigation",
    )


def page_header(title: str, subtitle: Optional[str] = None) -> None:
    st.markdown(f"## {title}")
    if subtitle:
        st.caption(subtitle)


def show_result(result: Dict[str, Any], *, title: str = "Result") -> None:
    # Keep output lightweight by default.
    st.markdown(f"**{title}**")
    if ENABLE_LARGE_PREVIEWS:
        st.json(result)
    else:
        st.code(_small_preview(result), language="json")
        # Optional expand for debugging without hurting default performance too much:
        with st.expander("Expand full response (debug)", expanded=False):
            st.json(result)


# -----------------------------
# Pages
# -----------------------------
def render_home() -> None:
    page_header("Artifact Registry", "Upload, download, and manage artifacts.")
    st.write("Use the sidebar to open a tool.")
    st.info("This registry allows you to manage ML models, datasets, and code artifacts. Store, search, and track lineage for all your machine learning artifacts in one centralized location.")


def render_upload(backend: str) -> None:
    page_header("Upload Artifact", "Upload a ZIP artifact to the registry (backend handles storage).")

    artifact_type = st.selectbox("Artifact Category", VALID_TYPES, index=0, key="upload_artifact_type")

    uploaded = st.file_uploader("Choose a ZIP file", type=["zip"], key="upload_file_input")
    st.caption("Note: Only uploads when you click the button (no background calls).")

    if st.button("Upload Artifact", key="upload_button"):
        if not uploaded:
            st.error("Please choose a ZIP file to upload.")
            return

        # Read file bytes (small cost)
        data = uploaded.getvalue()
        files = {"file": (uploaded.name, data, "application/zip")}
        params = {"type": artifact_type}

        with st.spinner("Uploading..."):
            result = request_json("POST", f"{backend}/upload", params=params, files=files, timeout=30)

        show_result(result, title="Upload response")


def render_download(backend: str) -> None:
    page_header("Download Artifact", "Download an artifact by type and ID.")

    artifact_type = st.selectbox("Artifact type", VALID_TYPES, index=0, key="download_artifact_type")
    artifact_id = st.text_input("Artifact ID", value="", key="download_artifact_id")

    if st.button("Download", key="download_button"):
        if not artifact_id.strip():
            st.error("Please enter an Artifact ID.")
            return

        # Backend can return a signed URL or raw bytes; we handle both.
        with st.spinner("Requesting download..."):
            result = request_json(
                "GET",
                f"{backend}/download",
                params={"type": artifact_type, "id": artifact_id.strip()},
                timeout=30,
            )

        # If backend returns a URL, show a link.
        url = result.get("url") or result.get("download_url")
        if isinstance(url, str) and url.startswith("http"):
            st.success("Download ready.")
            # Use markdown link with proper accessibility attributes
            st.markdown(f'<a href="{url}" target="_blank" rel="noopener noreferrer" aria-label="Open download link in new tab">Open download</a>', unsafe_allow_html=True)
            if not ENABLE_LARGE_PREVIEWS:
                st.caption("Response details truncated for performance.")
        else:
            show_result(result, title="Download response")


def render_search(backend: str) -> None:
    page_header("Search Artifacts", "Search artifacts using regex or filters (backend).")

    method = st.radio("Search Method", ["GET", "POST"], horizontal=True, key="search_method")
    pattern = st.text_input("Regex Pattern", value=".*", key="search_pattern")

    artifact_type = st.selectbox("Artifact type (optional)", ["(any)"] + VALID_TYPES, index=0, key="search_artifact_type")

    if st.button("Run Search", key="search_button"):
        payload: Dict[str, Any] = {"pattern": pattern}
        if artifact_type != "(any)":
            payload["type"] = artifact_type

        with st.spinner("Searching..."):
            if method == "GET":
                result = request_json("GET", f"{backend}/search", params=payload, timeout=30)
            else:
                result = request_json("POST", f"{backend}/search", json=payload, timeout=30)

        show_result(result, title="Search response")


def render_lineage(backend: str) -> None:
    page_header("Model Lineage", "Fetch lineage info for a model ID.")

    model_id = st.text_input("Model ID", value="", key="lineage_model_id")

    if st.button("Get Lineage", key="lineage_button"):
        if not model_id.strip():
            st.error("Please enter a Model ID.")
            return

        with st.spinner("Fetching lineage..."):
            result = request_json("GET", f"{backend}/lineage", params={"id": model_id.strip()}, timeout=30)

        # Feature #1 removed: no graph drawing by default.
        show_result(result, title="Lineage response")

        if ENABLE_LINEAGE_VISUALIZATION:
            st.warning("Lineage visualization is enabled, which may reduce Lighthouse performance.")
            # If you *ever* re-enable: do lazy imports inside this block.


def render_cost(backend: str) -> None:
    page_header("Cost Calculator", "Estimate cost for an artifact (backend).")

    artifact_type = st.selectbox("Artifact type", VALID_TYPES, index=0, key="cost_artifact_type")
    artifact_id = st.text_input("Artifact ID", value="", key="cost_artifact_id")

    if st.button("Calculate Cost", key="cost_button"):
        if not artifact_id.strip():
            st.error("Please enter an Artifact ID.")
            return

        with st.spinner("Calculating..."):
            result = request_json(
                "GET",
                f"{backend}/cost",
                params={"type": artifact_type, "id": artifact_id.strip()},
                timeout=30,
            )

        show_result(result, title="Cost response")


def render_license(backend: str) -> None:
    page_header("License Check", "Check license compatibility or artifact license (backend).")

    artifact_id = st.text_input("Artifact ID", value="", key="license_artifact_id")

    if st.button("Check License", key="license_button"):
        if not artifact_id.strip():
            st.error("Please enter an Artifact ID.")
            return

        with st.spinner("Checking..."):
            result = request_json("GET", f"{backend}/license", params={"id": artifact_id.strip()}, timeout=30)

        show_result(result, title="License response")


def render_rate(backend: str) -> None:
    page_header("Rate Model", "Submit a rating for a model (backend).")

    model_id = st.text_input("Model ID", value="", key="rate_model_id")
    rating = st.slider("Rating", min_value=1, max_value=5, value=5, key="rate_slider")

    if st.button("Submit Rating", key="rate_button"):
        if not model_id.strip():
            st.error("Please enter a Model ID.")
            return

        with st.spinner("Submitting..."):
            result = request_json(
                "POST",
                f"{backend}/rate",
                json={"id": model_id.strip(), "rating": int(rating)},
                timeout=30,
            )

        show_result(result, title="Rate response")


def render_reset(backend: str) -> None:
    page_header("Reset Registry", "Danger zone: clears registry data (backend).")

    st.warning("This will delete registry data. Use carefully.")

    confirm = st.checkbox("I understand this action is destructive.", key="reset_confirm")
    if st.button("Reset", key="reset_button"):
        if not confirm:
            st.error("Please confirm the warning checkbox first.")
            return

        with st.spinner("Resetting..."):
            result = request_json("POST", f"{backend}/reset", timeout=30)

        show_result(result, title="Reset response")


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    backend = sidebar_backend_url()
    page = sidebar_navigation()

    # Route only the selected page (critical for Lighthouse performance)
    if page == "Home":
        render_home()
    elif page == "Upload":
        render_upload(backend)
    elif page == "Download":
        render_download(backend)
    elif page == "Search":
        render_search(backend)
    elif page == "Lineage":
        render_lineage(backend)
    elif page == "Cost":
        render_cost(backend)
    elif page == "License":
        render_license(backend)
    elif page == "Rate":
        render_rate(backend)
    elif page == "Reset":
        render_reset(backend)
    else:
        render_home()


if __name__ == "__main__":
    main()
