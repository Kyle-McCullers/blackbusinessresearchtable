"""
Orchestrator — discovers and runs all adapters, resolves entities,
geocodes new records, writes to DuckDB, exports businesses.csv.

Usage (from scripts/ directory):
    python -m pipeline.run
    python -m pipeline.run --snapshot-id 2026-Q2
"""
import argparse
import csv
import importlib
import importlib.util
import inspect
import json
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = REPO_ROOT / "data"
DB_PATH = DATA_DIR / "bbrt.duckdb"
CSV_PATH = DATA_DIR / "businesses.csv"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
ADAPTERS_DIR = Path(__file__).resolve().parent.parent / "adapters"

# Allow imports from scripts/ when run as module
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.adapter_base import AdapterBase
from pipeline.db import (
    open_db, upsert_source, write_businesses,
    write_snapshot_meta, get_registry, upsert_registry,
)
from pipeline.entity_resolver import resolve
from pipeline.geocoder import batch_geocode
from pipeline.export import export_csv, write_summary


def current_snapshot_id(d: date = None) -> str:
    d = d or date.today()
    quarter = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{quarter}"


def discover_adapters(adapters_dir: Path = ADAPTERS_DIR) -> list:
    """
    Import every .py file in adapters_dir and return one instance
    of each concrete AdapterBase subclass found.
    """
    adapters = []
    for path in sorted(adapters_dir.glob("*.py")):
        if path.name.startswith("_"):
            continue
        module_name = f"adapters.{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            print(f"  [WARN] Could not import {path.name}: {e}")
            continue
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if (issubclass(cls, AdapterBase)
                    and cls is not AdapterBase
                    and cls.SOURCE_ID
                    and cls.__module__ == module.__name__):
                adapters.append(cls())
    return adapters


def run(snapshot_id: str = None) -> None:
    snapshot_id = snapshot_id or current_snapshot_id()
    print(f"\n=== BBRT Pipeline — {snapshot_id} ===\n")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    con = open_db(DB_PATH)
    registry = get_registry(con)

    # Count records in previous snapshot for dropped-record calculation
    prior_count_row = con.execute(
        "SELECT COUNT(*) FROM businesses WHERE snapshot_id = "
        "(SELECT MAX(snapshot_id) FROM snapshots)"
    ).fetchone()
    prior_count = prior_count_row[0] if prior_count_row else 0

    adapters = discover_adapters()
    print(f"Discovered {len(adapters)} adapter(s): "
          f"{', '.join(a.SOURCE_ID for a in adapters)}\n")

    all_records: list[dict] = []
    sources_run: list[str] = []
    sources_failed: list[str] = []

    for adapter in adapters:
        print(f"  Running {adapter.SOURCE_ID}...")
        upsert_source(con, adapter)
        try:
            records = adapter.run()
            # Tag each record with source metadata
            for r in records:
                r["source_id"] = adapter.SOURCE_ID
                r["confidence"] = adapter.CONFIDENCE
            print(f"    {len(records)} records fetched.")
            all_records.extend(records)
            sources_run.append(adapter.SOURCE_ID)
        except Exception as e:
            print(f"    [ERROR] {adapter.SOURCE_ID} failed: {e}")
            sources_failed.append(adapter.SOURCE_ID)

    print(f"\nResolving entity identity for {len(all_records)} records...")
    review_log: list[dict] = []
    all_records, new_entries = resolve(all_records, registry, snapshot_id, review_log)
    print(f"  {len(new_entries)} new businesses. "
          f"{len(review_log)} uncertain matches logged for review.")

    if review_log:
        review_path = SNAPSHOTS_DIR / f"{snapshot_id}-match-review.csv"
        with open(review_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=review_log[0].keys())
            writer.writeheader()
            writer.writerows(review_log)
        print(f"  Match review log: {review_path}")

    print(f"\nGeocoding new records...")
    coords = batch_geocode(all_records)
    for r in all_records:
        if r["business_id"] in coords:
            lat, lon = coords[r["business_id"]]
            r["latitude"] = str(lat)
            r["longitude"] = str(lon)
    geocoded_count = sum(1 for r in all_records if r.get("latitude"))
    print(f"  {geocoded_count}/{len(all_records)} records have coordinates.")

    print(f"\nWriting to DuckDB...")
    write_businesses(con, all_records, snapshot_id)
    upsert_registry(con, snapshot_id, new_entries)

    records_dropped = max(prior_count - len(all_records), 0)
    write_snapshot_meta(con, snapshot_id, len(all_records),
                        records_dropped, sources_run, sources_failed)

    print(f"Writing businesses.csv...")
    export_csv(con, CSV_PATH, snapshot_id)

    summary_path = SNAPSHOTS_DIR / f"{snapshot_id}-summary.txt"
    write_summary(summary_path, snapshot_id, len(all_records),
                  records_dropped, sources_run, sources_failed)
    print(f"Summary: {summary_path}")

    con.close()
    print(f"\nDone. {len(all_records)} records in snapshot {snapshot_id}.")
    if sources_failed:
        print(f"FAILED SOURCES: {', '.join(sources_failed)}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the BBRT quarterly pipeline")
    parser.add_argument("--snapshot-id", default=None,
                        help="Override snapshot ID (default: current quarter)")
    args = parser.parse_args()
    run(snapshot_id=args.snapshot_id)
