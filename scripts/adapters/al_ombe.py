"""
Alabama OMBE (Office of Minority Business Enterprise) adapter.

Source: ADECA OMBE certified businesses Excel file.
URL: https://adeca.alabama.gov/ombe/
Filter: Ethnic Group == "B" (Black American)
Confidence: confirmed_black — race/ethnicity is an explicit field.

File format notes:
- Section header rows (category names) intersperse the data — skipped.
- Empty rows intersperse the data — skipped.
- "City, State, Zip" is a single combined column; parsed into components.
- Expiration Date may be a datetime or a string like "R4 10/11/2026".

Set AL_OMBE_FILE env var to the downloaded .xlsx path, or pass file_path=.
"""
import os
import re
import sys
from datetime import date
from pathlib import Path

import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DEFAULT_FILE_ENV = "AL_OMBE_FILE"
SOURCE_FILE = (
    Path.home()
    / "University of Michigan Dropbox"
    / "Kyle McCullers"
    / "Data"
    / "US State(s) Administrative Data"
    / "Alabama"
    / "OMBE-certified-businesses.xlsx"
)

HEADER_ROW = 0   # 0-indexed; row 0 is the column header
BLACK_CODE = "B"


class AlOmbeAdapter(AdapterBase):
    SOURCE_ID   = "al_ombe"
    SOURCE_NAME = "Alabama OMBE Certified Businesses"
    PROGRAM     = "OMBE"
    GEOGRAPHY   = "Alabama"
    CONFIDENCE  = "confirmed_black"

    # City/State/Zip is combined — parsed in parse(); not in FIELD_MAP.
    FIELD_MAP = {
        "Business Name":  "business_name",
        "Address":        "address_street",
        "Phone":          "phone",
        "Email Address":  "email",
        "Website":        "website",
        "NAICS Codes":    "naics_code",
    }

    def __init__(self, file_path: Path = None):
        path = file_path or os.environ.get(DEFAULT_FILE_ENV, "")
        if path:
            self._file_path = Path(path)
        else:
            self._file_path = SOURCE_FILE
        if not self._file_path.exists():
            raise FileNotFoundError(f"Alabama OMBE file not found: {self._file_path}")

    def fetch(self) -> list[dict]:
        """Load the OMBE xlsx and return Black-owned business rows."""
        wb = openpyxl.load_workbook(str(self._file_path), read_only=True)
        try:
            ws = wb.active
            all_rows = list(ws.iter_rows(values_only=True))
        finally:
            wb.close()

        header = all_rows[HEADER_ROW]
        col = {str(name).strip(): i for i, name in enumerate(header) if name is not None}

        ethnic_col = col.get("Ethnic Group")
        if ethnic_col is None:
            raise ValueError(f"Expected 'Ethnic Group' column; found: {list(col.keys())}")

        raw = []
        for row in all_rows[HEADER_ROW + 1:]:
            # Skip section header rows (only col A has a value) and blank rows
            non_null = [v for v in row if v is not None]
            if len(non_null) <= 1:
                continue
            if str(row[ethnic_col] or "").strip() != BLACK_CODE:
                continue
            raw_row = {str(name).strip(): row[idx]
                       for name, idx in col.items() if name}
            raw.append(raw_row)

        return raw

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for source_row in raw:
            record = self.map_record(source_row)

            # Parse "City, ST  Zip" → separate fields
            city, state, zipcode = _parse_city_state_zip(
                str(source_row.get("City, State, Zip") or "")
            )
            record["address_city"] = city
            record["address_state"] = state
            record["address_zip"] = zipcode

            # Use business name as source ID (no stable numeric ID in this file)
            record["source_business_id"] = str(
                source_row.get("Business Name") or ""
            ).strip()
            record["certification"] = "OMBE"
            record["last_verified"] = str(date.today())
            records.append(record)
        return records


def _parse_city_state_zip(value: str) -> tuple[str, str, str]:
    """
    Parse combined 'City, ST  Zip' field.
    Examples: 'Adamsville, AL 35005' | 'Clanton, AL  35045'
    Returns (city, state, zip) — empty strings on parse failure.
    """
    m = re.match(r'^(.+),\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', value.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
    return "", "", ""
