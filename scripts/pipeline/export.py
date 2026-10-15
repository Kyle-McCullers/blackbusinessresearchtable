import csv
import re
from datetime import date
from pathlib import Path

import duckdb

_SAFE_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")

# Contact fields are PRIVATE: withheld from the public CSV and released to verified
# researchers on request (DECISIONS 2026-09-23). They stay in the database.
PRIVATE_COLUMNS = ["owner_name", "phone", "email"]

EXPORT_COLUMNS = [
    "business_id", "business_name", "year_founded",
    "address_street", "address_city", "address_state", "address_zip",
    "latitude", "longitude", "industry", "naics_code", "certification",
    "description", "website",
    "instagram_handle", "facebook_url", "tiktok_handle",
    "yelp_url", "google_maps_url",
    "discloses_google_maps", "discloses_yelp", "discloses_instagram",
    "data_source", "last_verified", "confidence",
]

_STATE_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI",
    "wyoming": "WY", "district of columbia": "DC", "washington dc": "DC",
    "washingtondc": "DC", "puerto rico": "PR",
}
_VALID_ABBR = set(_STATE_ABBR.values())


def normalize_state(value) -> str:
    """Map a state full name or abbreviation to its 2-letter code.

    Unrecognized values (e.g. a truncated "So") return "" so they cannot create a
    stray entry in the site's state dropdown; the raw value stays in the database.
    """
    v = (value or "").strip()
    if v.upper() in _VALID_ABBR:
        return v.upper()
    return _STATE_ABBR.get(re.sub(r"\s+", " ", v.lower()), "")


_EMAIL_RE = re.compile(r"[\w.+%-]+@[\w-]+(?:\.[\w-]+)*\.[A-Za-z]{2,}")
_FREE_TEXT_COLUMNS = ("description", "address_street")


def scrub_emails(row: dict) -> dict:
    """Remove email addresses that sources put outside the `email` column.

    Some programs list an email in the website field (4,166 rows in 2026-Q2) or inside
    free text. A website that IS an email is blanked; emails embedded in free text are
    cut out. Raw values stay in the database.
    """
    out = dict(row)
    site = (out.get("website") or "").strip()
    bare = re.sub(r"^(?:mailto:|https?://)", "", site, flags=re.I).rstrip("/")
    if _EMAIL_RE.fullmatch(bare):
        out["website"] = ""
    for col in _FREE_TEXT_COLUMNS:
        if out.get(col):
            out[col] = re.sub(r"\s{2,}", " ", _EMAIL_RE.sub("", out[col])).strip()
    return out


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

    cleaned = []
    for r in rows:
        d = scrub_emails(dict(zip(EXPORT_COLUMNS, r)))
        d["address_state"] = normalize_state(d["address_state"])
        cleaned.append([d[c] for c in EXPORT_COLUMNS])
    rows = cleaned

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
    sources_carried_forward: list[str] = None,
) -> None:
    """Write a human-readable run report.

    Note: overwrites output_path if it already exists.
    """
    sources_carried_forward = sources_carried_forward or []
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
        f"Sources carried forward ({len(sources_carried_forward)}): "
        f"{', '.join(sources_carried_forward) or 'none'}",
    ]
    output_path.write_text("\n".join(lines) + "\n")
