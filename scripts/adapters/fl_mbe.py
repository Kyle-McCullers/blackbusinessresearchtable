"""
Florida certified MBE adapter (African American).

Source: Florida Office of Supplier Diversity (OSD) certified-vendor directory,
filtered in the portal to ethnicity = African American AND status = certified,
then exported to Excel. The directory is exported in three alphabetical-by-county
files (A-G, H-M, N-Z), all combined here. Manual-capture source (see or_cobid).

Filter: none applied in-adapter — the source export is ALREADY pre-filtered to
African American, certified firms (the ethnicity selection happens in the portal
before download, so the files contain no per-row ethnicity column).
Confidence: confirmed_black — the disclosure is explicit at the source: every
row is a firm the OSD directory lists under African American ethnicity. The
filename records the provenance ("African American, certified").

Columns observed (2026-06-13 export, header on row 1):
  "Vendor Name", "Contact", "Email", "Address", "City", "State", "Phone Number"

File layout: three .xlsx files, header on the first row, sheet "Vendors".
Records are deduplicated on vendor name + address across all three files.
"""
import os
import sys
from datetime import date
from glob import glob
from pathlib import Path

import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DEFAULT_FILE_ENV = "FL_MBE_FILES"  # ':'-separated paths override the glob
MANUAL_DIR = (
    Path.home() / "University of Michigan Dropbox" / "Kyle McCullers"
    / "Projects and Proposals" / "Black Business Research Table"
    / "data" / "manual downloads"
)
FILE_GLOB = "Florida Directory (African American*.xlsx"
MARKER = "Vendor Name"


class FlMbeAdapter(AdapterBase):
    SOURCE_ID   = "fl_mbe"
    SOURCE_NAME = "Florida OSD Certified MBE (African American)"
    PROGRAM     = "MBE"
    GEOGRAPHY   = "Florida"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "Vendor Name":  "business_name",
        "Contact":      "owner_name",
        "Email":        "email",
        "Address":      "address_street",
        "City":         "address_city",
        "State":        "address_state",
        "Phone Number": "phone",
    }

    def __init__(self, file_paths: list = None):
        if file_paths:
            self._files = [Path(p) for p in file_paths]
        else:
            env = os.environ.get(DEFAULT_FILE_ENV, "")
            if env:
                self._files = [Path(p) for p in env.split(os.pathsep) if p]
            else:
                matches = sorted(glob(str(MANUAL_DIR / FILE_GLOB)))
                if not matches:
                    raise FileNotFoundError(
                        f"Florida MBE files not found. Save the OSD exports to "
                        f"'{MANUAL_DIR}' (matching '{FILE_GLOB}') or set {DEFAULT_FILE_ENV}."
                    )
                self._files = [Path(p) for p in matches]
        for p in self._files:
            if not p.exists():
                raise FileNotFoundError(f"Florida MBE file not found: {p}")

    def fetch(self) -> list[dict]:
        seen, out = set(), []
        for path in self._files:
            rows = _xlsx_rows(path)
            col, _, data = _locate(rows, MARKER, path)
            for row in data:
                rec = {name: (row[i] if i < len(row) else "") for name, i in col.items()}
                if not rec.get("Vendor Name", "").strip():
                    continue
                key = (rec.get("Vendor Name", "").strip().lower(),
                       rec.get("Address", "").strip().lower())
                if key in seen:
                    continue
                seen.add(key)
                out.append(rec)
        return out

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for sr in raw:
            rec = self.map_record(sr)
            rec["certification"] = "MBE"
            rec["last_verified"] = str(date.today())
            records.append(rec)
        return records


def _xlsx_rows(path):
    """Read an .xlsx into a list of rows (each a list of stripped strings)."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    ws.reset_dimensions()   # read_only workbooks can report a bogus A1:A1 dimension
    rows = []
    for row in ws.iter_rows(values_only=True):
        rows.append(["" if c is None else str(c).strip() for c in row])
    wb.close()
    return rows


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
