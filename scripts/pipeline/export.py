import csv
import re
from datetime import date
from pathlib import Path

import duckdb

_SAFE_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")

EXPORT_COLUMNS = [
    "business_id", "business_name", "owner_name", "year_founded",
    "address_street", "address_city", "address_state", "address_zip",
    "latitude", "longitude", "industry", "naics_code", "certification",
    "description", "website", "phone", "email",
    "instagram_handle", "facebook_url", "tiktok_handle",
    "yelp_url", "google_maps_url",
    "discloses_google_maps", "discloses_yelp", "discloses_instagram",
    "data_source", "last_verified", "confidence",
]


def export_csv(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
    snapshot_id: str,
) -> None:
    """
    Export the current snapshot to businesses.csv.
    For businesses present in multiple sources in the same snapshot,
    prefer confidence='confirmed_black' over 'mbe_unverified'.
    """
    for col in EXPORT_COLUMNS:
        assert _SAFE_IDENTIFIER.match(col), f"Unsafe column name in EXPORT_COLUMNS: {col!r}"
    cols = ", ".join(EXPORT_COLUMNS)
    rows = con.execute(
        f"""
        WITH ranked AS (
            SELECT
                {cols},
                ROW_NUMBER() OVER (
                    PARTITION BY business_id
                    ORDER BY CASE confidence
                        WHEN 'confirmed_black' THEN 0
                        ELSE 1
                    END
                ) AS rn
            FROM businesses
            WHERE snapshot_id = ?
        )
        SELECT {cols} FROM ranked WHERE rn = 1
        ORDER BY business_name
        """,
        [snapshot_id],
    ).fetchall()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(EXPORT_COLUMNS)
        writer.writerows(rows)


def write_summary(
    output_path: Path,
    snapshot_id: str,
    records_total: int,
    records_dropped: int,
    sources_run: list[str],
    sources_failed: list[str],
) -> None:
    """Write a human-readable run report.

    Note: overwrites output_path if it already exists.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"BBRT Quarterly Pipeline — {snapshot_id}",
        f"Run date: {date.today()}",
        f"",
        f"Records in snapshot: {records_total}",
        f"Records dropped from prior snapshot: {records_dropped}",
        f"",
        f"Sources run ({len(sources_run)}): {', '.join(sources_run) or 'none'}",
        f"Sources failed ({len(sources_failed)}): {', '.join(sources_failed) or 'none'}",
    ]
    output_path.write_text("\n".join(lines) + "\n")
