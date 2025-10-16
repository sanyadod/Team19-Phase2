# ACME Model Scoring CLI

## Overview
This tool is a **CLI program** that evaluates pre-trained Hugging Face models according to Sarah's requirements.
It reads a list of URLs, computes multiple metrics, and outputs results as **NDJSON lines** for each MODEL URL.

**Key Features:**
- Real-time Hugging Face API integration for accurate model data
- NDJSON output format for machine processing
- Human-readable summary reports with rankings and recommendations
- Parallel processing with cross-platform compatibility

The CLI is implemented in **Python 3.11+** with strict code quality enforced by `flake8`, `isort`, and `mypy`.
Tests are written in `pytest`, with coverage measured via `coverage`.

---

## CLI Commands

### Install
```bash
./run install
```
Installs all required dependencies (runtime + dev tools).

### Score Models
```bash
./run urls.txt
```
- Reads a newline-delimited file of URLs.
- Filters for MODEL URLs (Hugging Face).
- For each model, prints **one NDJSON line** with all metrics and latencies.

**Example output:**
```json
{"name":"https://huggingface.co/gpt2","category":"MODEL","net_score":0.90,...}
```

### Score Models with Summary Report
```bash
./run urls.txt --summary
```
- Same as above, but also generates **human-readable summary files**.
- Creates timestamped `.jsonl` and `_summary.txt` files.
- Provides executive summary, rankings, and recommendations.

**Custom output filename:**
```bash
./run urls.txt --summary --output my_analysis
```

**Example summary output:**
```
🤖 ACME MODEL EVALUATION SUMMARY REPORT
========================================
Generated: 2025-09-21 13:27:12
Total Models Evaluated: 3

📊 EXECUTIVE SUMMARY
Average Quality Score: 65.2% (Good)

🏆 TOP MODELS RANKING
1. bert-base-uncased (Score: 72.1% - Good)
2. gpt2 (Score: 60.7% - Good)
3. distilbert-base-uncased (Score: 62.8% - Good)

💡 RECOMMENDATIONS
⚠️ No LGPL-2.1 compliant models found.
🥧 2 models suitable for Raspberry Pi deployment.
```

### Run Tests
```bash
./run test
```
Runs tests and coverage. Always prints:
```
X/Y test cases passed. Z% line coverage achieved.
```

---

## Project Structure

```
acmecli/
  __init__.py
  main.py              # CLI entrypoint
  logging_cfg.py       # sets up logging via $LOG_FILE, $LOG_LEVEL
  io_utils.py          # read/write URLs + NDJSON
  urls.py              # URL classifier (MODEL/DATASET/CODE)
  scoring.py           # combines metrics into output dict
  metrics/
    base.py           # Metric protocol + timing decorator
    repo_scan.py      # Metrics from repo/dataset inspection
    hf_api.py         # Metrics from Hugging Face API (stub for now)
tests/
  test_smoke.py       # starter unit tests
test_artifacts/        # Test-generated files (ignored by Git)
  README.md           # Documentation of test artifacts
  *.jsonl             # NDJSON test results and data files
  *_summary.txt       # Generated summary reports from tests
  urls.txt            # Test input files
run                   # CLI shim (install, test, scoring)
pyproject.toml        # deps + lint/test/type configs
```

---

## Metrics

Each metric is implemented as a function that returns `(score ∈ [0,1], latency_ms)`.

| Metric | Formula / Rule | Source |
|--------|---------------|--------|
| **Size** | Linear decay: `(U - S) / (U - L)`, clipped to [0,1] | Repo scan |
| **License** | 1.0 if LGPL-2.1 compatible, 0.5 if unclear, 0.0 if incompatible | Repo scan |
| **Ramp Up Time** | Average of 5 flags (README, Quickstart, Tutorials, API docs, Reproducibility) | Repo scan |
| **Bus Factor** | Contributors / (Contributors + k), k=5 | Repo scan/API |
| **Dataset & Code** | (DatasetFlag + CodeFlag) / 2 | Repo scan |
| **Dataset Quality** | (Source + License + Splits + Ethics) / 4 | Repo scan |
| **Code Quality** | 0.4·Flake8 + 0.2·Isort + 0.4·Mypy | Linting tools |
| **Perf. Claims** | (Benchmarks + Citations) / 2 | Repo/API |

### NetScore
```
NetScore = 0.20*License + 0.20*DatasetAndCode + 0.15*CodeQuality
         + 0.15*RampUp + 0.10*BusFactor + 0.10*PerformanceClaims
         + 0.05*DatasetQuality + 0.05*Size
```

---

## How It Works

1. **URL File Parsing**
   Reads `urls.txt`, filters for Hugging Face MODEL URLs.

2. **Context Builder (`build_ctx_from_url`)**
   - Milestone 2: returns **placeholder values**
   - Milestone 3: will query Hugging Face API + scan repos

3. **Metric Computation**
   Each metric is decorated with `@timed`, so we record its runtime latency.

4. **Scoring & Output**
   `compute_all_scores()` gathers metric results, builds the NDJSON object, and computes NetScore.

5. **Parallel Execution**
   Models are processed in parallel using `ProcessPoolExecutor`.

---

## Testing

- Tests are located in `tests/`
- Run with:
```bash
./run test
```
- Current tests: basic smoke tests for all metrics
- Coverage target: **≥80%** before final delivery
- Plan: expand with more unit + integration tests in Milestone 3

### Test Artifacts Directory

The `test_artifacts/` directory contains files generated during test execution:

- **Purpose**: Stores all test-generated files to keep the project root clean
- **Contents**: NDJSON test data, summary reports, evaluation results, and temporary test files
- **Git Status**: Directory is ignored by Git (see `.gitignore`)
- **Management**: Files persist after test runs for debugging purposes, can be safely deleted
- **Organization**: Replaces previous behavior where test files were created in the project root

This directory ensures a clean workspace while maintaining all test functionality and providing easy access to test-generated files for debugging.

---

## Development Guidelines

- **Code Style**
  - Pass `flake8`, `isort`, `mypy`
  - Type annotations required

- **Testing**
  - Every function should have at least one test
  - Another teammate should add an extra test for each module

- **Error Handling**
  - On fatal error, program exits with code **1**
  - Always print a clear error message to stderr
  - Logs go to `$LOG_FILE` at verbosity `$LOG_LEVEL`

- **Pull Requests**
  - Must pass CI before merging
  - Require peer review

---
