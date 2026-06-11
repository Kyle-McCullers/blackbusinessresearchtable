"""
Nevada NDOT DBE directory adapter.

Source: Nevada DOT Unified Certification Program DBE directory (dbesystem/
B2Gnow), exported manually to CSV ("Download Entire Directory to Excel" → CSV).
Manual-capture source (see or_cobid for the same pattern).

Filter: Ethnicity == "BLACK AMERICAN" (matched case-insensitively).
Confidence: confirmed_black — Ethnicity is an explicit, published per-firm field.

Distinct Ethnicity values observed (2026-06-11 full export, verbatim UPPERCASE):
  "CAUCASIAN", "BLACK AMERICAN", "HISPANIC AMERICAN", "ASIAN-PACIFIC AMERICAN",
  "SUBCONTINENT ASIAN AMERICAN", "NATIVE AMERICAN", "OTHER MINORITY"

File layout: title/preamble precedes the header row (located dynamically by the
row containing "Ethnicity"); latin-1 encoded. The export has DUPLICATE
City/State/Zip columns (physical + mailing) — the first occurrence (physical
address) is used. Zip codes carry a leading tab, stripped on mapping.
Certs are flagged under the USDOT Interim Final Rule reevaluation but the
directory data is still exportable.
"""
import csv
import os
import sys
from datetime import date
from glob import glob
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DEFAULT_FILE_ENV = "NV_DBE_FILE"
MANUAL_DIR = (
    Path.home() / "University of Michigan Dropbox" / "Kyle McCullers"
    / "Projects and Proposals" / "Black Business Research Table"
    / "data" / "manual downloads"
)
FILE_GLOB = "Nevada Directory*.csv"
ENCODING = "latin-1"
ETHNICITY_FIELD = "Ethnicity"
BLACK_VALUE = "BLACK AMERICAN"   # compared case-insensitively


class NvDbeAdapter(AdapterBase):
    SOURCE_ID   = "nv_dbe"
    SOURCE_NAME = "Nevada NDOT DBE"
    PROGRAM     = "DBE"
    GEOGRAPHY   = "Nevada"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "Company Name":     "business_name",
        "Physical Address": "address_street",
        "City":             "address_city",
        "State":            "address_state",
        "Zip":              "address_zip",
        "Phone":            "phone",
        "Email":            "email",
        "Website":          "website",
        "Capability":       "description",
    }

    def __init__(self, file_path: Path = None):
        path = file_path or os.environ.get(DEFAULT_FILE_ENV, "")
        if path:
            self._file_path = Path(path)
        else:
            matches = sorted(glob(str(MANUAL_DIR / FILE_GLOB)))
            if not matches:
                raise FileNotFoundError(
                    f"Nevada DBE file not found. Save the NDOT directory CSV to "
                    f"'{MANUAL_DIR}' (matching '{FILE_GLOB}') or set {DEFAULT_FILE_ENV}."
                )
            self._file_path = Path(matches[-1])
        if not self._file_path.exists():
            raise FileNotFoundError(f"Nevada DBE file not found: {self._file_path}")

    def fetch(self) -> list[dict]:
        with open(self._file_path, encoding=ENCODING, newline="") as f:
            rows = list(csv.reader(f))
        col, eth_i, data = _locate(rows, ETHNICITY_FIELD, self._file_path)

        seen, out = set(), []
        for row in data:
            if len(row) <= eth_i or row[eth_i].strip().upper() != BLACK_VALUE:
                continue
            rec = {name: (row[i] if i < len(row) else "") for name, i in col.items()}
            key = (rec.get("Company Name", "").strip().lower(),
                   rec.get("Physical Address", "").strip().lower())
            if key in seen:
                continue
            seen.add(key)
            out.append(rec)
        return out

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for sr in raw:
            rec = self.map_record(sr)
            first = (sr.get("Owner First") or "").strip()
            last = (sr.get("Owner Last") or "").strip()
            rec["owner_name"] = " ".join(filter(None, [first, last]))
            rec["certification"] = (sr.get("Certification Type") or "DBE").strip()
            rec["last_verified"] = str(date.today())
            records.append(rec)
        return records


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
