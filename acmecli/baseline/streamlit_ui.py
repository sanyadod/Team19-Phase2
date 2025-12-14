# acmecli/baseline/streamlit_ui.py
# Lighthouse-first Streamlit UI: keep all pages but avoid heavy work on initial load.

from __future__ import annotations

import json
from typing import Any, Dict, Optional

import requests
import streamlit as st


# -----------------------------
# Config
# -----------------------------
st.set_page_config(page_title="Artifact Registry", page_icon="📦", layout="centered")

# -----------------------------
# Accessibility + contrast fixes (CSS ONLY)
# -----------------------------
st.markdown(
    """
<style>
/* -----------------------------
   High-contrast defaults
   ----------------------------- */
:root {
  --text-high: #F9FAFB;
  --text-med:  #E5E7EB;
  --text-low:  #D1D5DB;
  --bg-main:   #0B1220;
  --bg-card:   #111827;
  --bg-input:  #1F2937;
  --border:    #374151;
  --focus:     #60A5FA;
}

.stApp {
  background-color: var(--bg-main) !important;
}

/* Default text */
html, body, [class*="st-"], [class*="css"] {
  color: var(--text-med) !important;
}

/* Headings */
h1, h2, h3, h4, h5, h6 {
  color: var(--text-high) !important;
}

/* Paragraphs / markdown / labels */
p, .stMarkdown, .stMarkdown p, .stCaption, .stText, small, label {
  color: var(--text-med) !important;
}

/* Sidebar */
section[data-testid="stSidebar"] {
  background-color: var(--bg-card) !important;
}
section[data-testid="stSidebar"] * {
  color: var(--text-med) !important;
}

/* Inputs */
input, textarea, select, [data-baseweb="input"] input {
  background-color: var(--bg-input) !important;
  color: var(--text-high) !important;
  border-color: var(--border) !important;
}

/* Placeholders */
::placeholder {
  color: var(--text-low) !important;
  opacity: 1 !important;
}

/* Buttons */
button, [role="button"] {
  color: var(--text-high) !important;
}
button[kind="primary"] {
  background-color: #2563EB !important;
}
button[kind="secondary"], button[kind="tertiary"] {
  background-color: transparent !important;
  border: 1px solid var(--border) !important;
}

/* Focus indicators */
button:focus, input:focus, select:focus, textarea:focus,
button:focus-visible, input:focus-visible, select:focus-visible, textarea:focus-visible,
[data-baseweb="input"] input:focus, [data-baseweb="input"] input:focus-visible {
  outline: 3px solid var(--focus) !important;
  outline-offset: 2px !important;
}

/* -----------------------------
   Fix button-name safely
   ----------------------------- */

/* Hide only the icon-only kebab menu */
button[kind="headerNoPadding"] {
  display: none !important;
}

/* Hide main menu + footer (OK for accessibility) */
#MainMenu { visibility: hidden; }
footer { visibility: hidden; }

/* DO NOT hide header or toolbar — sidebar toggle lives there */

/* Radios / checkboxes */
div[role="radiogroup"] * {
  color: var(--text-med) !important;
}
</style>
""",
    unsafe_allow_html=True,
)

# -----------------------------
# App constants
# -----------------------------
DEFAULT_BACKEND_URL = (
    st.secrets.get("BACKEND_URL", None) if hasattr(st, "secrets") else None
) or "http://127.0.0.1:5000"

VALID_TYPES = ["model", "code", "dataset"]

ENABLE_LINEAGE_VISUALIZATION = False
ENABLE_LARGE_PREVIEWS = False


# -----------------------------
# Cached resources
# -----------------------------
@st.cache_resource
def get_session() -> requests.Session:
    return requests.Session()


def _safe_json(resp: requests.Response) -> Dict[str, Any]:
    try:
        return resp.json()
    except Exception:
        return {"raw_text": (resp.text or "")[:2000], "status_code": resp.status_code}


def _small_preview(obj: Any, limit: int = 1200) -> str:
    try:
        s = json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        s = str(obj)
    return s[:limit] + "\n... (truncated)" if len(s) > limit else s


def request_json(method: str, url: str, *, timeout: int = 15, **kwargs) -> Dict[str, Any]:
    try:
        r = get_session().request(method, url, timeout=timeout, **kwargs)
        return _safe_json(r)
    except requests.RequestException as e:
        return {"error": str(e)}


# -----------------------------
# UI helpers
# -----------------------------
def sidebar_backend_url() -> str:
    st.sidebar.markdown("### Settings")
    backend = st.sidebar.text_input("Backend URL", value=DEFAULT_BACKEND_URL)
    return backend.rstrip("/")


def sidebar_navigation() -> str:
    st.sidebar.markdown("### Navigation")
    return st.sidebar.radio(
        "Tools",
        ["Home", "Upload", "Download", "Search", "Lineage", "Cost", "License", "Rate", "Reset"],
        index=0,
    )


def page_header(title: str, subtitle: Optional[str] = None) -> None:
    st.markdown(f"## {title}")
    if subtitle:
        st.caption(subtitle)


def show_result(result: Dict[str, Any], *, title: str = "Result") -> None:
    st.markdown(f"**{title}**")
    st.code(_small_preview(result), language="json")
    with st.expander("Expand full response (debug)", expanded=False):
        st.json(result)


# -----------------------------
# Pages
# -----------------------------
def render_home() -> None:
    page_header("Artifact Registry", "Upload, download, and manage artifacts.")
    st.write("Use the sidebar to open a tool.")
    st.info(
        "This registry allows you to manage ML models, datasets, and code artifacts. "
        "Store, search, and track lineage in one centralized location."
    )


def render_upload(backend: str) -> None:
    page_header("Upload Artifact")
    artifact_type = st.selectbox("Artifact Category", VALID_TYPES)
    uploaded = st.file_uploader("Choose a ZIP file", type=["zip"])

    if st.button("Upload Artifact"):
        if not uploaded:
            st.error("Please choose a ZIP file.")
            return
        files = {"file": (uploaded.name, uploaded.getvalue(), "application/zip")}
        with st.spinner("Uploading..."):
            show_result(
                request_json("POST", f"{backend}/upload", params={"type": artifact_type}, files=files),
                title="Upload response",
            )


def render_download(backend: str) -> None:
    page_header("Download Artifact")
    artifact_type = st.selectbox("Artifact type", VALID_TYPES)
    artifact_id = st.text_input("Artifact ID")

    if st.button("Download"):
        if not artifact_id.strip():
            st.error("Enter an Artifact ID.")
            return
        result = request_json("GET", f"{backend}/download", params={"type": artifact_type, "id": artifact_id})
        url = result.get("url") or result.get("download_url")
        if isinstance(url, str):
            st.markdown(
                f'<a href="{url}" target="_blank" rel="noopener noreferrer">Open download</a>',
                unsafe_allow_html=True,
            )
        else:
            show_result(result)


def render_search(backend: str) -> None:
    page_header("Search Artifacts")
    method = st.radio("Search Method", ["GET", "POST"], horizontal=True)
    pattern = st.text_input("Regex Pattern", value=".*")

    if st.button("Run Search"):
        payload = {"pattern": pattern}
        result = request_json(method, f"{backend}/search", params=payload if method == "GET" else None, json=payload if method == "POST" else None)
        show_result(result)


def render_lineage(backend: str) -> None:
    page_header("Model Lineage")
    model_id = st.text_input("Model ID")

    if st.button("Get Lineage"):
        if not model_id.strip():
            st.error("Enter a Model ID.")
            return
        show_result(request_json("GET", f"{backend}/lineage", params={"id": model_id}))


def render_cost(backend: str) -> None:
    page_header("Cost Calculator")
    artifact_type = st.selectbox("Artifact type", VALID_TYPES)
    artifact_id = st.text_input("Artifact ID")

    if st.button("Calculate Cost"):
        show_result(request_json("GET", f"{backend}/cost", params={"type": artifact_type, "id": artifact_id}))


def render_license(backend: str) -> None:
    page_header("License Check")
    artifact_id = st.text_input("Artifact ID")

    if st.button("Check License"):
        show_result(request_json("GET", f"{backend}/license", params={"id": artifact_id}))


def render_rate(backend: str) -> None:
    page_header("Rate Model")
    model_id = st.text_input("Model ID")
    rating = st.slider("Rating", 1, 5, 5)

    if st.button("Submit Rating"):
        show_result(request_json("POST", f"{backend}/rate", json={"id": model_id, "rating": rating}))


def render_reset(backend: str) -> None:
    page_header("Reset Registry")
    confirm = st.checkbox("I understand this is destructive")

    if st.button("Reset"):
        if not confirm:
            st.error("Please confirm first.")
            return
        show_result(request_json("POST", f"{backend}/reset"))


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    backend = sidebar_backend_url()
    page = sidebar_navigation()

    {
        "Home": render_home,
        "Upload": lambda: render_upload(backend),
        "Download": lambda: render_download(backend),
        "Search": lambda: render_search(backend),
        "Lineage": lambda: render_lineage(backend),
        "Cost": lambda: render_cost(backend),
        "License": lambda: render_license(backend),
        "Rate": lambda: render_rate(backend),
        "Reset": lambda: render_reset(backend),
    }.get(page, render_home)()


if __name__ == "__main__":
    main()
