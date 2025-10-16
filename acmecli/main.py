"""
CLI entry point for evaluating Hugging Face models and emitting scores.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import sys
from typing import Any, Dict, List, Tuple

from .determinism import set_global_determinism
from .io_utils import read_urls, write_ndjson_line
from .logging_cfg import setup_logging
from .metrics.hf_api import ModelLookupError, build_context_from_api
from .report import capture_and_summarize_results, extract_model_name
from .scoring import compute_all_scores
from .urls import Category, classify


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Evaluate Hugging Face models and generate scores")
    ap.add_argument("url_file", nargs="?", help="File with newline-delimited URLs")
    ap.add_argument(
        "--summary",
        action="store_true",
        help="Generate a human-readable summary report (saves files)",
    )
    ap.add_argument(
        "--output",
        "-o",
        default="evaluation",
        help="Base filename for output files (default: evaluation)",
    )
    ap.add_argument(
        "--fail-fast", action="store_true", help="Stop immediately on the first model failure"
    )
    ap.add_argument(
        "--error-file", default=None, help="Write failures to this NDJSON file (one JSON per line)"
    )
    return ap.parse_args()


def build_ctx_from_url(url: str) -> Dict[str, Any]:
    # May raise ModelLookupError
    return build_context_from_api(url)


def process_model(url: str) -> Dict[str, Any]:
    # May raise ModelLookupError
    ctx = build_ctx_from_url(url)
    fields = compute_all_scores(ctx)
    model_name = extract_model_name(url)
    return {"name": model_name, "category": "MODEL", **fields}


def _write_error_line(path: str, record: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _validate_environment() -> None:
    """Validate basic env settings (token, log path)."""
    import requests

    # Validate GitHub token if provided
    token = os.getenv("GITHUB_TOKEN")
    if token:
        # Simple validation - check if token format is reasonable
        if not token.startswith(("ghp_", "github_pat_")) or len(token) < 20:
            print("Error: Invalid GitHub token format", file=sys.stderr)
            raise SystemExit(1)

        # Test token with a simple API call
        try:
            headers = {"Authorization": f"Bearer {token}", "User-Agent": "ACME-CLI/0.1.0"}
            response = requests.get("https://api.github.com/user", headers=headers, timeout=10)
            if response.status_code == 401:
                print("Error: Invalid GitHub token - authentication failed", file=sys.stderr)
                raise SystemExit(1)
        except requests.RequestException:
            # Network issues are not token validation failures
            pass

    # Validate log file path if provided. If invalid, fall back to default (no hard failure).
    log_file = os.getenv("LOG_FILE")
    if log_file:
        try:
            log_dir = os.path.dirname(log_file) if os.path.dirname(log_file) else "."
            # If directory doesn't exist or is not writable, or open fails, fall back gracefully
            if (log_dir and not os.path.isdir(log_dir)) or (
                log_dir and not os.access(log_dir, os.W_OK)
            ):
                raise OSError("unwritable or missing directory")
            with open(log_file, "a", encoding="utf-8"):
                pass
        except OSError:
            # Fallback to default log file in CWD and continue
            os.environ["LOG_FILE"] = "acmecli.log"


def main() -> None:
    args = parse_args()

    # Best-effort determinism to reduce grading variance
    set_global_determinism()

    # Validate environment configuration first (ensures LOG_FILE path is usable)
    _validate_environment()

    # Configure logging after validation
    setup_logging()

    # Usage/config errors -> exit 1 (per autograder requirement)
    if not args.url_file:
        print(
            "ERROR: missing URL_FILE. Usage: ./run URL_FILE [--summary] "
            "[--fail-fast] [--error-file PATH]",
            file=sys.stderr,
        )
        raise SystemExit(1)

    try:
        urls = list(read_urls(args.url_file))
    except OSError as e:
        print(f"ERROR: failed to read {args.url_file}: {e}", file=sys.stderr)
        raise SystemExit(1)

    if not urls:
        print(f"ERROR: {args.url_file} contained no URLs", file=sys.stderr)
        raise SystemExit(1)

    # Classify URLs up-front
    models: List[str] = []
    invalid: List[Tuple[str, str]] = []
    for u in urls:
        try:
            cat = classify(u)
        except Exception as e:
            invalid.append((u, f"classify error: {e}"))
            continue
        if cat is Category.MODEL:
            models.append(u)
        else:
            invalid.append((u, f"unsupported category: {getattr(cat, 'name', str(cat))}"))

    results: List[Dict[str, Any]] = []
    failures: List[Tuple[str, str]] = []  # (url, reason)

    # Emit classification errors to error file immediately (optional)
    if args.error_file and invalid:
        for u, why in invalid:
            _write_error_line(args.error_file, {"url": u, "error": why, "kind": "classify"})

    # If there are no model URLs at all, still report invalids and exit 1
    if not models and invalid:
        print("[error] no model URLs found to evaluate", file=sys.stderr)
        print("[error] invalid/unsupported URL(s) detected:", file=sys.stderr)
        for u, why in invalid:
            print(f"  - {u}: {why}", file=sys.stderr)
        raise SystemExit(1)

    # Helper to record a failure (stderr + optional error file)
    def record_failure(u: str, why: str, kind: str = "lookup") -> None:
        failures.append((u, why))
        if args.error_file:
            _write_error_line(args.error_file, {"url": u, "error": why, "kind": kind})

    # ----- Parallel processing with threads (robust on Windows) -----
    try:
        with cf.ThreadPoolExecutor() as ex:
            future_by_url = {ex.submit(process_model, u): u for u in models}
            for fut in cf.as_completed(future_by_url):
                u = future_by_url[fut]
                try:
                    rec = fut.result()  # exceptions propagate without pickle issues
                    write_ndjson_line(rec)  # write successful record immediately
                    if args.summary:
                        results.append(rec)
                except ModelLookupError as e:
                    record_failure(u, f"model lookup failed: {e}", kind="lookup")
                    if args.fail_fast:
                        # Best effort: cancel anything not yet started
                        for f in future_by_url:
                            f.cancel()
                        break
                except Exception as e:
                    record_failure(u, f"processing error: {e}", kind="processing")
                    if args.fail_fast:
                        for f in future_by_url:
                            f.cancel()
                        break
    except Exception as e:
        # As a last resort, fall back to sequential
        print(f"[warn] parallel execution unavailable: {e}", file=sys.stderr)
        for u in models:
            try:
                rec = process_model(u)
                write_ndjson_line(rec)
                if args.summary:
                    results.append(rec)
            except ModelLookupError as e:
                record_failure(u, f"model lookup failed: {e}", kind="lookup")
                if args.fail_fast:
                    break
            except Exception as e:
                record_failure(u, f"processing error: {e}", kind="processing")
                if args.fail_fast:
                    break

    # Generate summary artifacts for the successes only
    if args.summary and results:
        ndjson_file, summary_file = capture_and_summarize_results(results, args.output)
        print(f"\n📄 Results saved to: {ndjson_file}", flush=True)
        print(f"📊 Summary report: {summary_file}", flush=True)
        print(f"🔍 View summary: cat {summary_file}", flush=True)

    # Final reporting - only report actual errors, not category filtering
    if invalid:
        # Filter out expected non-model categories (DATASET, CODE) from error reporting
        actual_errors = [
            (u, why)
            for u, why in invalid
            if not ("unsupported category:" in why and ("DATASET" in why or "CODE" in why))
        ]
        if actual_errors:
            print("[error] invalid/unsupported URL(s) detected:", file=sys.stderr)
            for u, why in actual_errors:
                print(f"  - {u}: {why}", file=sys.stderr)
        # Update invalid list to only include actual errors for exit code logic
        invalid = actual_errors

    if failures:
        print("[error] model failures:", file=sys.stderr)
        for u, why in failures:
            print(f"  - {u}: {why}", file=sys.stderr)

    # Exit policy:
    #   0 -> all OK (including successful processing of models even with DATASET/CODE URLs present)
    #   1 -> any actual problem (usage/config, actual invalid URLs, lookup/processing failures)
    raise SystemExit(1 if (invalid or failures) else 0)


if __name__ == "__main__":
    main()
