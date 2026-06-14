"""
Arkansas Minority & Women-Owned Business Registry adapter.

Source: Arkansas Economic Development Commission / AASIS minority & women-owned
business registry, exported manually to CSV. Manual-capture source (see
or_cobid for the same pattern): a human downloads the directory and saves it to
the manual-downloads folder; the pipeline reads whatever file is present and
carries the source forward on quarters where no file is provided.

Filter: VendorCategory in {"African American", "African-American"} (the registry
uses both spellings).
Confidence: confirmed_black — VendorCategory is an explicit, published per-firm
ethnicity field.

Distinct VendorCategory values observed (2026-06-13 full export, verbatim):
  "Women-Owned" (616), "African American" (472), "African-American" (443),
  "Hispanic American" (91), "Asian American" (81), "Service-Disabled Veteran"
  (36), "American Indian" (35), "Asian-American" (10), "Pacific Islander
  American" (5), "Woman-Owned" (4), "Woman Owned" (4), "Hispanic-American" (2),
  "Service Disabled Veteran" (2), "" (2)

File layout: a flat CSV with the header on the first row (no preamble),
utf-8 with BOM. A firm appears once per row; records are deduplicated on
company name + street.
"""
import csv
import os
import sys
from datetime import date
from glob import glob
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DEFAULT_FILE_ENV = "AR_MWBE_FILE"
MANUAL_DIR = (
    Path.home() / "University of Michigan Dropbox" / "Kyle McCullers"
    / "Projects and Proposals" / "Black Business Research Table"
    / "data" / "manual downloads"
)
FILE_GLOB = "Arkansas*.csv"
ENCODING = "utf-8-sig"
ETHNICITY_FIELD = "VendorCategory"
BLACK_VALUES = {"african american", "african-american"}  # compared lowercased


class ArMwbeAdapter(AdapterBase):
    SOURCE_ID   = "ar_mwbe"
    SOURCE_NAME = "Arkansas Minority & Women Business Registry"
    PROGRAM     = "MWBE"
    GEOGRAPHY   = "Arkansas"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "CompanyName":         "business_name",
        "BusinessDescription": "description",
        "Phone":               "phone",
        "Street":              "address_street",
        "City":                "address_city",
        "StateCode":           "address_state",
        "Zip":                 "address_zip",
        "ContactEmail":        "email",
        "Website":             "website",
        "NaicsCode":           "naics_code",
    }

    def __init__(self, file_path: Path = None):
        self._file_path = _resolve_file(file_path, DEFAULT_FILE_ENV, FILE_GLOB, "Arkansas M/WBE")

    def fetch(self) -> list[dict]:
        with open(self._file_path, encoding=ENCODING, newline="") as f:
            rows = list(csv.reader(f))
        col, eth_i, data = _locate(rows, ETHNICITY_FIELD, self._file_path)

        seen, out = set(), []
        for row in data:
            if len(row) <= eth_i or row[eth_i].strip().lower() not in BLACK_VALUES:
                continue
            rec = {name: (row[i] if i < len(row) else "") for name, i in col.items()}
            key = (rec.get("CompanyName", "").strip().lower(),
                   rec.get("Street", "").strip().lower())
            if key in seen:
                continue
            seen.add(key)
            out.append(rec)
        return out

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for sr in raw:
            rec = self.map_record(sr)
            first = (sr.get("ContactFirstName") or "").strip()
            last = (sr.get("ContactLastName") or "").strip()
            rec["owner_name"] = " ".join(filter(None, [first, last]))
            rec["certification"] = "MBE"
            rec["last_verified"] = str(date.today())
            records.append(rec)
        return records


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
        p = Path(matches[-1])  # newest by name
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
        if name and name not in col:   # first occurrence wins (handles duplicate columns)
            col[name] = i
    return col, col[marker], rows[header_idx + 1:]
