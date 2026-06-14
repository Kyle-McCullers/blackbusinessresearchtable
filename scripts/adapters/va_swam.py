"""
Virginia SWaM / DBE certified-firm adapter.

Source: Virginia Department of Small Business & Supplier Diversity (SBSD)
directory "Directory Listing Export", downloaded manually to .xlsx.
Manual-capture source (see or_cobid for the pattern).

Filter: Ethnicity == "Black or African American".
Confidence: confirmed_black — Ethnicity is an explicit, published per-firm field.

Distinct Ethnicity values observed (2026-06-13 full export, verbatim):
  "" (8,147 — non-minority certs), "Black or African American" (4,844),
  "Asian American" (1,185), "Hispanic American" (967), "Subcontinent Asian
  American" (136), "Asian Pacific American" (108), "Native American or American
  Indian/Eskimo or Aleut" (73), "Other" (25), "Asian/Pacific Islander" (22),
  "White or Caucasian American" (10)

File layout: a single-sheet .xlsx whose header repeats three certification
column blocks (SWaM / MWAA / DBE), so "Company Name", "Ethnicity", "Mailing *"
etc. each appear three times. The first-occurrence-wins column map reads the
first (SWaM) block; verified that every "Black or African American" row is
populated in that first block (no firm is Black only in a later block), so no
firm is lost. Records are deduplicated on company name + mailing zip.
"""
import os
import sys
from datetime import date
from glob import glob
from pathlib import Path

import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DEFAULT_FILE_ENV = "VA_SWAM_FILE"
MANUAL_DIR = (
    Path.home() / "University of Michigan Dropbox" / "Kyle McCullers"
    / "Projects and Proposals" / "Black Business Research Table"
    / "data" / "manual downloads"
)
FILE_GLOB = "Virginia Directory Listing Export*.xlsx"
ETHNICITY_FIELD = "Ethnicity"
BLACK_VALUE = "Black or African American"


class VaSwamAdapter(AdapterBase):
    SOURCE_ID   = "va_swam"
    SOURCE_NAME = "Virginia SWaM/DBE"
    PROGRAM     = "SWaM"
    GEOGRAPHY   = "Virginia"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "Company Name":      "business_name",
        "Contact Name":      "owner_name",
        "Contact Phone":     "phone",
        "Contact Email":     "email",
        "Mailing Address":   "address_street",
        "Mailing City":      "address_city",
        "Mailing State":     "address_state",
        "Mailing Zip":       "address_zip",
        "Business website":  "website",
        "Certification Type": "certification",
    }

    def __init__(self, file_path: Path = None):
        path = file_path or os.environ.get(DEFAULT_FILE_ENV, "")
        if path:
            self._file_path = Path(path)
        else:
            matches = sorted(glob(str(MANUAL_DIR / FILE_GLOB)))
            if not matches:
                raise FileNotFoundError(
                    f"Virginia SWaM file not found. Save the directory export to "
                    f"'{MANUAL_DIR}' (matching '{FILE_GLOB}') or set {DEFAULT_FILE_ENV}."
                )
            self._file_path = Path(matches[-1])
        if not self._file_path.exists():
            raise FileNotFoundError(f"Virginia SWaM file not found: {self._file_path}")

    def fetch(self) -> list[dict]:
        rows = _xlsx_rows(self._file_path)
        col, eth_i, data = _locate(rows, ETHNICITY_FIELD, self._file_path)

        seen, out = set(), []
        for row in data:
            if len(row) <= eth_i or row[eth_i].strip() != BLACK_VALUE:
                continue
            rec = {name: (row[i] if i < len(row) else "") for name, i in col.items()}
            key = (rec.get("Company Name", "").strip().lower(),
                   rec.get("Mailing Zip", "").strip())
            if key in seen:
                continue
            seen.add(key)
            out.append(rec)
        return out

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for sr in raw:
            rec = self.map_record(sr)
            if not rec.get("certification"):
                rec["certification"] = "SWaM"
            rec["last_verified"] = str(date.today())
            records.append(rec)
        return records


def _xlsx_rows(path):
    """Read an .xlsx into a list of rows (each a list of stripped strings)."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    ws.reset_dimensions()
    rows = []
    for row in ws.iter_rows(values_only=True):
        rows.append(["" if c is None else str(c).strip() for c in row])
    wb.close()
    return rows


def _locate(rows, marker, path):
    """Find the header row carrying `marker`; return (first-occurrence col map,
    marker index, data rows after the header). First occurrence wins, so the
    repeated SWaM/MWAA/DBE column blocks collapse to the first (SWaM) block."""
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
