"""
South Carolina SMBCC (Small & Minority Business Contracting & Certification) adapter.

Source: SC Advance — Small Business Division. The statewide certification list
is published as a dated .xlsx linked from the division landing page (the
filename changes each refresh, so we scrape the current link rather than
hard-coding a dated URL).
Landing: https://advance.sc.gov/small-business-division
Filter: Class code in {01, 02, 05} (African American owners).
Confidence: confirmed_black — the published "Class" column carries an explicit
minority classification, verified verbatim against the live file.

Distinct Class values observed (2026-06-02 file):
  "01 - African American Male Owners"      (Black)
  "02 - African American Female Owners"    (Black)
  "05 - DLT Certified AA Male/Female"      (Black)
  "03- Caucasian Female Owners"
  "04 - Hispanic Male/Female Owners"
  "07 - Native American Male/Female"
  "09 - Asian Pacific or Other"

File layout: a variable-length preamble (title + filter description) precedes
the header row, so the header is located dynamically (the row containing both
"Class" and "Certification ID") rather than assumed at a fixed offset.

Auto-fetching (no manual download); suitable for the autonomous cron.
"""
import io
import re
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import openpyxl
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

LANDING_URL = "https://advance.sc.gov/small-business-division"
CLASS_FIELD = "Class"
BLACK_CODES = {"01", "02", "05"}
HEADER_MARKERS = {"Class", "Certification ID"}
_UA = {"User-Agent": "Mozilla/5.0 (BBRT research data pipeline)"}


class ScSmbccAdapter(AdapterBase):
    SOURCE_ID   = "sc_smbcc"
    SOURCE_NAME = "South Carolina SMBCC"
    PROGRAM     = "MBE"
    GEOGRAPHY   = "South Carolina"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "Organization Lookup": "business_name",
        "Business Address":    "address_street",
        "Business City":       "address_city",
        "Business State":      "address_state",
        "Business Zip":        "address_zip",
        "Year Established":    "year_founded",
        "Business Phone":      "phone",
        "Business Email":      "email",
        "Services":            "description",
    }

    def fetch(self) -> list[dict]:
        """Discover the current .xlsx, download it, and return Black-owned rows."""
        landing = requests.get(LANDING_URL, headers=_UA, timeout=30)
        landing.raise_for_status()
        match = re.search(r'href=["\']([^"\']*\.xlsx[^"\']*)', landing.text, re.I)
        if not match:
            raise ValueError(
                f"No .xlsx link found on the SC SMBCC landing page: {LANDING_URL}"
            )
        xlsx_url = urljoin(LANDING_URL, match.group(1))

        resp = requests.get(xlsx_url, headers=_UA, timeout=60)
        resp.raise_for_status()
        wb = openpyxl.load_workbook(io.BytesIO(resp.content), read_only=True, data_only=True)
        try:
            rows = list(wb.active.iter_rows(values_only=True))
        finally:
            wb.close()

        header_idx = _find_header(rows)
        if header_idx is None:
            raise ValueError("Could not locate the SC SMBCC header row (Class/Certification ID)")
        header = rows[header_idx]
        col = {str(n).strip(): i for i, n in enumerate(header) if n is not None}
        class_i = col[CLASS_FIELD]

        out = []
        for row in rows[header_idx + 1:]:
            class_val = str(row[class_i] or "").strip()
            if class_val[:2] not in BLACK_CODES:
                continue
            out.append({name: row[i] for name, i in col.items()})
        return out

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for source_row in raw:
            record = self.map_record(source_row)
            record["source_business_id"] = str(
                source_row.get("Vendor Registration Number") or ""
            ).strip()
            record["certification"] = "MBE"
            record["last_verified"] = str(date.today())
            records.append(record)
        return records


def _find_header(rows: list) -> int | None:
    """Return the index of the header row (the one carrying the marker columns)."""
    for i, row in enumerate(rows):
        values = {str(c).strip() for c in row if c is not None}
        if HEADER_MARKERS <= values:
            return i
    return None
