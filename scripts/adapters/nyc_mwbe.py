"""
NYC MWBE Directory adapter.

Source: NYC MWBE_9.9.2025.xlsx
Ethnicity filter: Ethnicity == "Black"
Header row: 6 (1-indexed); rows 1-5 are metadata.
"""
import re
import sys
from pathlib import Path

import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

SOURCE_FILE = (
    Path.home()
    / "University of Michigan Dropbox"
    / "Kyle McCullers"
    / "Data"
    / "US State(s) Administrative Data"
    / "New York"
    / "NYC MWBE_9.9.2025.xlsx"
)

XLSX_HEADER_ROW = 6  # 1-indexed


def _extract_year(value) -> str:
    if value is None:
        return ""
    if hasattr(value, "year"):
        return str(value.year)
    match = re.search(r"\b(19|20)\d{2}\b", str(value))
    return match.group() if match else ""


class NycMwbeAdapter(AdapterBase):
    SOURCE_ID   = "nyc_mwbe"
    SOURCE_NAME = "NYC MWBE Directory 2025"
    PROGRAM     = "MWBE"
    GEOGRAPHY   = "NYC"
    CONFIDENCE  = "confirmed_black"

    # Columns that map 1:1 to BBRT fields.
    # owner_name and year_founded require transformation (handled in parse).
    FIELD_MAP = {
        "Vendor Formal Name":  "business_name",
        "Address Line 1":      "address_street",
        "City":                "address_city",
        "State":               "address_state",
        "Zip":                 "address_zip",
        "NAICS Sector":        "industry",
        "6 digit NAICS code":  "naics_code",
        "Certification":       "certification",
        "Business Description": "description",
        "Website":             "website",
        "Telephone":           "phone",
        "Email":               "email",
    }

    def __init__(self, source_file: Path = SOURCE_FILE):
        self._source_file = source_file

    def fetch(self) -> list[dict]:
        """Load xlsx and return list of raw row dicts for Black-owned businesses."""
        wb = openpyxl.load_workbook(self._source_file, read_only=True)
        ws = wb.active
        all_rows = list(ws.iter_rows(values_only=True))
        wb.close()

        header = all_rows[XLSX_HEADER_ROW - 1]
        col = {name: i for i, name in enumerate(header) if name}

        raw = []
        for row in all_rows[XLSX_HEADER_ROW:]:
            if row[col["Ethnicity"]] != "Black":
                continue
            raw_row = {name: row[idx] for name, idx in col.items()}
            raw.append(raw_row)
        return raw

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for source_row in raw:
            record = self.map_record(source_row)

            # owner_name: combine First Name + Last Name
            first = str(source_row.get("First Name") or "").strip()
            last  = str(source_row.get("Last Name")  or "").strip()
            record["owner_name"] = " ".join(filter(None, [first, last]))

            # year_founded: extract from Date of Establishment
            record["year_founded"] = _extract_year(
                source_row.get("Date of Establishment")
            )

            # source_business_id: the source's own unique identifier
            record["source_business_id"] = str(
                source_row.get("Account Number") or ""
            ).strip()

            record["last_verified"] = "2025-09-09"

            records.append(record)
        return records
