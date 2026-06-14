"""
North Carolina HUB (Historically Underutilized Business) adapter.

Source: NC Office for Historically Underutilized Businesses / NC electronic
Vendor Portal "Vendor Details" export, downloaded manually to CSV.
Manual-capture source (see or_cobid for the pattern).

The export is the full state vendor list (~72k rows). HUB-certified Black firms
are selected with TWO fields:
  HUB == "Certified" AND HUBCategory == "Black"
Confidence: confirmed_black — HUBCategory is an explicit, published per-firm
ethnicity field and HUB == "Certified" confirms an active certification.

Distinct HUB values observed (2026-06-13 full export, verbatim):
  "" (62,301 — non-HUB vendors), "Certified" (6,278), "Not Certified" (3,639)
Distinct HUBCategory values observed (verbatim):
  "" (65,342), "Black" (3,988), "Female" (1,597), "Hispanic" (661),
  "Asian American" (343), "American Indian" (168), "Disabled" (108),
  "Disadvantaged" (11)
HUBCategory == "Black" breaks down by HUB status as:
  Certified 3,615 · "" (blank) 370 · Not Certified 3
We keep only the 3,615 actively-certified Black firms; the 370 blank-status and
3 not-certified Black rows are excluded as they lack an active HUB certification.

File layout: a flat CSV with the header on the first row (no preamble),
utf-8 with BOM. Records are deduplicated on company name + street.
"""
import csv
import os
import sys
from datetime import date
from glob import glob
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DEFAULT_FILE_ENV = "NC_HUB_FILE"
MANUAL_DIR = (
    Path.home() / "University of Michigan Dropbox" / "Kyle McCullers"
    / "Projects and Proposals" / "Black Business Research Table"
    / "data" / "manual downloads"
)
FILE_GLOB = "North Carolina Vendor Details*.csv"
ENCODING = "utf-8-sig"
HUB_FIELD = "HUB"
HUB_CERTIFIED = "Certified"
CATEGORY_FIELD = "HUBCategory"
BLACK_VALUE = "Black"


class NcHubAdapter(AdapterBase):
    SOURCE_ID   = "nc_hub"
    SOURCE_NAME = "North Carolina HUB"
    PROGRAM     = "HUB"
    GEOGRAPHY   = "North Carolina"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "Name":             "business_name",
        "MainContactName":  "owner_name",
        "MainContactEmail": "email",
        "MainContactPhone": "phone",
        "AddressLine1":     "address_street",
        "City":             "address_city",
        "State":            "address_state",
        "ZipCode":          "address_zip",
        "URL":              "website",
    }

    def __init__(self, file_path: Path = None):
        self._file_path = _resolve_file(file_path, DEFAULT_FILE_ENV, FILE_GLOB, "North Carolina HUB")

    def fetch(self) -> list[dict]:
        with open(self._file_path, encoding=ENCODING, newline="") as f:
            rows = list(csv.reader(f))
        col, _, data = _locate(rows, CATEGORY_FIELD, self._file_path)
        hub_i = col[HUB_FIELD]
        cat_i = col[CATEGORY_FIELD]

        seen, out = set(), []
        for row in data:
            if len(row) <= max(hub_i, cat_i):
                continue
            if row[hub_i].strip() != HUB_CERTIFIED or row[cat_i].strip() != BLACK_VALUE:
                continue
            rec = {name: (row[i] if i < len(row) else "") for name, i in col.items()}
            key = (rec.get("Name", "").strip().lower(),
                   rec.get("AddressLine1", "").strip().lower())
            if key in seen:
                continue
            seen.add(key)
            out.append(rec)
        return out

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for sr in raw:
            rec = self.map_record(sr)
            # The NC export spells out state names ("North Carolina"); normalize to
            # the USPS 2-letter code used by every other adapter. (~6% of HUB-certified
            # Black firms are HQ'd out of state but still NC-certified — kept as-is.)
            rec["address_state"] = _STATE_ABBR.get(
                rec["address_state"].strip().lower(), rec["address_state"].strip())
            rec["certification"] = "HUB"
            rec["last_verified"] = str(date.today())
            records.append(rec)
        return records


_STATE_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "district of columbia": "DC", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
}


def _resolve_file(file_path, env_var, file_glob, label):
    path = file_path or os.environ.get(env_var, "")
    if path:
        p = Path(path)
    else:
        matches = sorted(glob(str(MANUAL_DIR / file_glob)))
        if not matches:
            raise FileNotFoundError(
                f"{label} file not found. Save the directory export to "
                f"'{MANUAL_DIR}' (matching '{file_glob}') or set {env_var}."
            )
        p = Path(matches[-1])
    if not p.exists():
        raise FileNotFoundError(f"{label} file not found: {p}")
    return p


def _locate(rows, marker, path):
    """Find the header row carrying `marker`; return (first-occurrence col map,
    marker index, data rows after the header)."""
    header_idx = None
    for i, row in enumerate(rows):
        if any((c or "").strip() == marker for c in row):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError(f"No header row containing '{marker}' in {path}")
    header = rows[header_idx]
    col = {}
    for i, name in enumerate(header):
        name = (name or "").strip()
        if name and name not in col:
            col[name] = i
    return col, col[marker], rows[header_idx + 1:]
