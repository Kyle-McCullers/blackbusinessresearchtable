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
    write_snapshot_meta, get_registry, upsert_registry, carry_forward_records,
    source_counts,
)
from pipeline.entity_resolver import resolve
from pipeline.geocoder import batch_geocode
from pipeline.export import export_csv, write_summary
from pipeline.coverage import export_sources_csv, export_meta_json, export_parquet
from pipeline.firms import refresh_all_snapshots, export_firm_files


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
                try:
                    adapters.append(cls())
                except (ValueError, FileNotFoundError) as e:
                    print(f"  [SKIP] {cls.SOURCE_ID}: {e}")
    return adapters


SHRINK_RATIO = 0.5   # a source returning under half its prior count is flagged


def source_health(prior_counts: dict, fresh_counts: dict, sources_failed) -> dict:
    """Flag sources whose fresh data looks wrong. Retention keeps the record whole
    regardless; this makes the problem visible so a human fixes the source.

    Returns {source_id: reason} for failed, empty, or sharply shrunken sources.
    """
    flagged = {}
    for sid in sources_failed:
        flagged[sid] = "failed"
    for sid, prior in prior_counts.items():
        if sid in flagged:
            continue
        fresh = fresh_counts.get(sid)
        if fresh is None:
            continue                     # source did not run (e.g. manual file absent)
        if fresh == 0:
            flagged[sid] = f"returned 0 records (prior {prior})"
        elif prior and fresh < SHRINK_RATIO * prior:
            flagged[sid] = f"returned {fresh} records (prior {prior})"
    return flagged


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

    prior_source_counts = source_counts(con, snapshot_id)

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

    # RETENTION (DECISIONS 2026-10-15): once in the record, always in the record.
    # Every row in the prior snapshot that this run did not re-observe is carried into
    # the new snapshot. That covers a source that did not run, one that failed, one
    # that returned nothing, and individual firms a source stopped listing. Carried
    # records already carry business_id + coords, so they are injected AFTER
    # resolution/geocoding, keep their original last_verified, and their registry rows
    # are left untouched (no last_seen bump: they were not freshly observed).
    observed = {(r["source_id"], r["business_id"]) for r in all_records}
    fresh_counts = {}
    for r in all_records:
        fresh_counts[r["source_id"]] = fresh_counts.get(r["source_id"], 0) + 1
    for sid in sources_run:
        fresh_counts.setdefault(sid, 0)
    carried = carry_forward_records(con, snapshot_id, observed)
    sources_carried = sorted({r["source_id"] for r in carried})
    if carried:
        print(f"\nRetaining {len(carried)} previously-recorded records not re-observed "
              f"this run, from: {', '.join(sources_carried)}")
        all_records.extend(carried)
    flagged = source_health(prior_source_counts, fresh_counts, sources_failed)

    print(f"\nWriting to DuckDB...")
    write_businesses(con, all_records, snapshot_id)
    upsert_registry(con, snapshot_id, new_entries)

    # Report what is STORED, not what was attempted: rows sharing a
    # (business_id, snapshot_id, source_id) key are skipped on insert, so
    # len(all_records) can overstate the snapshot (2026-Q4 dry run: 37,856 vs 36,961).
    stored = con.execute(
        "SELECT COUNT(*) FROM businesses WHERE snapshot_id = ?", [snapshot_id]
    ).fetchone()[0]
    if stored != len(all_records):
        print(f"  NOTE: {len(all_records) - stored} duplicate-key rows skipped on insert.")
    records_dropped = max(prior_count - stored, 0)
    write_snapshot_meta(con, snapshot_id, stored,
                        records_dropped, sources_run, sources_failed,
                        sources_carried_forward=sources_carried)

    print(f"Building one-row-per-firm tables...")
    latest = refresh_all_snapshots(con)
    data_dir = CSV_PATH.parent
    export_firm_files(latest, data_dir)           # businesses.csv (firms), listings.csv, review/
    export_sources_csv(con, data_dir / "sources.csv", snapshot_id)
    export_meta_json(con, data_dir / "meta.json", snapshot_id)
    export_parquet(data_dir / "businesses.csv", data_dir / "businesses.parquet")
    export_parquet(data_dir / "listings.csv", data_dir / "listings.parquet")
    firms_total = len(latest["firms"])
    firms_headline = sum(1 for f in latest["firms"] if f["certification_status"] != "former")
    print(f"  {stored} listings -> {firms_total} firms ({firms_headline} in the headline count); "
          f"{len(latest['near'])} near-matches left for review")

    former = con.execute(
        "SELECT COUNT(*) FROM businesses WHERE snapshot_id = ? AND certification_status = 'former'",
        [snapshot_id]).fetchone()[0]
    summary_path = SNAPSHOTS_DIR / f"{snapshot_id}-summary.txt"
    write_summary(summary_path, snapshot_id, stored,
                  records_dropped, sources_run, sources_failed,
                  sources_carried_forward=sources_carried,
                  records_retained=len(carried), sources_flagged=flagged,
                  records_former=former, firms_total=firms_total, firms_headline=firms_headline)
    print(f"Summary: {summary_path}")

    con.close()
    print(f"\nDone. {stored} records in snapshot {snapshot_id}.")
    # Nothing is lost when a source misbehaves (retention), so a flagged source does
    # not block publication. It is surfaced as a GitHub Actions warning and in the
    # snapshot summary so the stale source gets fixed. Only a run in which NO adapter
    # produced fresh data is treated as an error.
    for sid, why in sorted(flagged.items()):
        print(f"::warning title=Stale source {sid}::{sid} {why}; prior records retained")
    if flagged:
        print(f"FLAGGED SOURCES (records retained from prior snapshot): "
              f"{', '.join(sorted(flagged))}")
    if not sources_run:
        print("ERROR: no adapter ran successfully.")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the BBRT quarterly pipeline")
    parser.add_argument("--snapshot-id", default=None,
                        help="Override snapshot ID (default: current quarter)")
    args = parser.parse_args()
    run(snapshot_id=args.snapshot_id)
