# ACME Trustworthy Model Registry

## Overview
This project implements a **Trustworthy Model Registry** for evaluating, storing, and serving machine learning artifacts according to ACME Corporation’s trustworthiness requirements.

The system evolved from a Phase 1 CLI-based scoring tool into a **fully deployed backend registry with REST APIs and a browser-based interface**, hosted on **AWS**. It supports artifact ingestion, rating, querying, search, lineage tracking, cost and license analysis, and registry administration.

All baseline Phase 2 functional and non-functional requirements described in the project specification have been implemented and validated using a combination of automated tests, manual testing, and autograder verification.

---

### Key Features:
- **Upload Artifact** – Create an artifact from a valid URL
- **Ingest Artifact** – Download and store ingestible artifacts
- **Get Artifact** – Query and list stored artifacts
- **Download Artifact** – Retrieve artifact metadata and content
- **Delete Artifact** – Remove artifacts by ID
- **Reset Registry** – Clear all stored artifacts and restore default state
- **Search Artifacts** – Regex-based search with semantic version filtering
- **Rate Artifacts** – Compute and retrieve trustworthiness scores for model artifacts
- **Artifact Lineage** – Track parent–child relationships between model artifacts
- **Cost Analysis** – Estimate artifact-related cost metrics
- **License Inspection** – Inspect and report license compatibility


### Trust & Quality Evaluation
- **Rate Artifact (`/rate`)**
  - Returns all Phase 1 metrics
  - Includes Phase 2 metrics:
    - **Reproducibility**
    - **Reviewedness**
    - **TreeScore** (derived from lineage graph)
  - Supports both legacy (`v0`) and full (`v1`) rating formats

### Search & Discovery
- **Search (`/search`)**
  - Regex-based search over model names and model cards
  - Semantic version filtering (`^`, `~`, relational ranges)
  - Guaranteed to return a subset of enumeration results

### Lineage & Analysis
- **Lineage Graph**
  - Extracted from model metadata
  - Displays parent–child relationships between models
- **Cost Analysis**
  - Estimates download size cost for models and sub-artifacts
- **License Compatibility Check**
  - Validates compatibility between model licenses and associated GitHub repositories

### Interfaces
- **Programmatic Interface**
  - Fully REST-compliant API following the provided OpenAPI specification
- **Human Interface**
  - Browser-based UI with multiple functional views
  - Styled, navigable interface (not a single query box)

### Deployment & Operations
- **AWS Deployment**
  - EC2-hosted Flask backend
  - DynamoDB for persistent artifact metadata
- **Observability**
  - `/health` endpoint for system status
  - Log-based monitoring for recent activity

---

### Installation
**Prerequisites**
- Python >= 3.8
- AWS credentials configured for DynamoDB access

```bash
git clone https://github.com/sanyadod/Team19-Phase2.git
cd Team19-Phase2
python -m venv .venv
source .venv/bin/activate

# install runtime dependencies from pyproject.toml
pip install -e .
```

### Running the System
```bash
python acmecli/baseline/backend.py
```

## Testing & Validation

The system is validated using a combination of:

- **Automated unit tests**
- **Manual test**
- **Feature-level and integration tests**
- **End-to-end system tests**
- **Autograder verification**

### Test Coverage
- Unit, feature, and system tests collectively exceed the required coverage threshold
- Error paths and negative cases are explicitly tested where applicable

### UI Testing
- Selenium-based automated tests validate the web interface
- Tests cover:
  - Page navigation
  - Form validation
  - Backend integration
  - Search, rate, download, and registry views

All tests can be run locally using test specific file.

## Specification Compliance

This system complies with:
- The provided **OpenAPI specification**
- All **baseline Phase 2 functional requirements**
- Required **non-functional requirements**, including CI/CD, testing, and AWS deployment

The system is deployed and accessible via a single public endpoint and was validated using the course autograder.
