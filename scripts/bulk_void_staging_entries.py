#!/usr/bin/env python3
"""Command to run: python3 void_staging_entries.py staging_ids.csv --base-url http://YOUR_IP:PORT --token 'YOUR_JWT' --reason 'Bulk void from CSV'"""
"""Bulk-void staging entries listed in a single-column CSV file."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path


KNOWN_HEADERS = {"id", "staging_entry_id", "staging entry id"}
BATCH_SIZE = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Void staging entries in batches using the bulk operations API."
    )
    parser.add_argument("csv_file", type=Path, help="Single-column CSV of staging entry IDs")
    parser.add_argument(
        "--base-url",
        required=True,
        help="API base URL, e.g. http://10.0.0.5:8080 or https://api.example.com",
    )
    parser.add_argument(
        "--token",
        required=True,
        help="JWT token (without the 'Bearer ' prefix)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30,
        help="Request timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--reason",
        help="Optional reason recorded for the bulk void operation",
    )
    return parser.parse_args()


def read_ids(csv_file: Path) -> list[str]:
    ids: list[str] = []

    with csv_file.open(newline="", encoding="utf-8-sig") as handle:
        for line_number, row in enumerate(csv.reader(handle), start=1):
            if not row or all(not cell.strip() for cell in row):
                continue
            if len(row) != 1:
                raise ValueError(
                    f"{csv_file}:{line_number}: expected one column, found {len(row)}"
                )

            entry_id = row[0].strip()
            if not ids and entry_id.lower() in KNOWN_HEADERS:
                continue
            if not entry_id:
                continue
            ids.append(entry_id)

    return ids


def void_batch(
    base_url: str,
    token: str,
    entry_ids: list[str],
    reason: str | None,
    timeout: float,
) -> list[dict[str, object]]:
    url = f"{base_url.rstrip('/')}/staging_entries/bulk_operations"
    payload = {
        "action": {"void": {"reason": reason}},
        "selection": {"selection_type": "ids", "ids": entry_ids},
    }
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8", errors="replace")

    try:
        results = json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError(f"API returned invalid JSON: {body[:500]}") from error
    if not isinstance(results, list) or not all(isinstance(item, dict) for item in results):
        raise ValueError(f"API returned an unexpected response: {body[:500]}")
    return results


def chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[start : start + size] for start in range(0, len(items), size)]


def print_result(item: dict[str, object]) -> str:
    primary_id = str(item.get("primary_id", "<unknown>"))
    status = str(item.get("status", "unknown"))

    if status == "success":
        print(f"  OK      {primary_id}")
    elif status == "skipped":
        print(f"  SKIPPED {primary_id} - {item.get('reason', 'no reason returned')}")
    elif status == "failed":
        print(f"  FAILED  {primary_id} - {item.get('error', 'no error returned')}")
    else:
        print(f"  FAILED  {primary_id} - unexpected status: {status}")
        status = "failed"

    return status


def main() -> int:
    args = parse_args()

    try:
        entry_ids = read_ids(args.csv_file)
    except (OSError, UnicodeError, csv.Error, ValueError) as error:
        print(f"Could not read CSV: {error}", file=sys.stderr)
        return 2

    if not entry_ids:
        print("No staging entry IDs found.", file=sys.stderr)
        return 2

    succeeded = 0
    skipped = 0
    failed = 0
    batches = chunks(entry_ids, BATCH_SIZE)

    for index, batch in enumerate(batches, start=1):
        print(f"Batch {index}/{len(batches)}: sending {len(batch)} IDs")
        try:
            results = void_batch(
                args.base_url,
                args.token,
                batch,
                args.reason,
                args.timeout,
            )
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace").strip()
            detail = f" - {body}" if body else ""
            print(
                f"  BATCH FAILED (HTTP {error.code}){detail}",
                file=sys.stderr,
            )
            failed += len(batch)
            continue
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as error:
            print(f"  BATCH FAILED ({error})", file=sys.stderr)
            failed += len(batch)
            continue

        for result in results:
            status = print_result(result)
            if status == "success":
                succeeded += 1
            elif status == "skipped":
                skipped += 1
            else:
                failed += 1

        missing_results = len(batch) - len(results)
        if missing_results > 0:
            print(
                f"  WARNING: API omitted {missing_results} result(s) from this batch",
                file=sys.stderr,
            )
            failed += missing_results

    print(f"Finished: {succeeded} succeeded, {skipped} skipped, {failed} failed.")
    return 1 if failed or skipped else 0


if __name__ == "__main__":
    raise SystemExit(main())
