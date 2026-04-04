"""
Indiana IDOA (Indiana Department of Administration) Diversity Certification adapter.

Source: Indiana Diversity Certified Businesses list — Excel export from IDOA.
Filter: Ethnic Group == "AFA" (African American)
Confidence: confirmed_black — race/ethnicity is an explicit certification field.

File format notes:
- Row 1 is a title row ("Diversity Certified Businesses / 12510").
- Row 2 is the column header row. Data starts at row 3.
- Companies appear multiple times — one row per UNSPSC commodity code. Deduplication
  is done on Bidder ID (one record per unique firm).
- Application Type values for AFA businesses: "MBE", "WBE", "IVBE".
- All AFA records have Application Status == "CERT" (only active certifications
  are included in the export).

Set IN_IDOA_FILE env var to the downloaded .xlsx path, or pass file_path=.
"""
import os
import sys
from datetime import date
from pathlib import Path

import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DEFAULT_FILE_ENV = "IN_IDOA_FILE"
SOURCE_FILE = (
    Path.home()
    / "University of Michigan Dropbox"
    / "Kyle McCullers"
    / "Data"
    / "US State(s) Administrative Data"
    / "Indiana"
    / "certification_list.xlsx"
)

TITLE_ROW = 1    # 1-based row index of the title; skipped
HEADER_ROW = 2   # 1-based row index of column headers
DATA_START = 3   # 1-based row index where data begins
BLACK_CODE = "AFA"


class InIdoaAdapter(AdapterBase):
    SOURCE_ID   = "in_idoa"
    SOURCE_NAME = "Indiana IDOA Diversity Certified Businesses"
    PROGRAM     = "MBE"
    GEOGRAPHY   = "Indiana"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "Company Name":    "business_name",
        "Mailing Address 1": "address_street",
        "City":            "address_city",
        "State":           "address_state",
        "Zip Code":        "address_zip",
        "Email ID":        "email",
        "Phone":           "phone",
    }

    def __init__(self, file_path: Path = None):
        path = file_path or os.environ.get(DEFAULT_FILE_ENV, "")
        if path:
            self._file_path = Path(path)
        else:
            self._file_path = SOURCE_FILE
        if not self._file_path.exists():
            raise FileNotFoundError(f"Indiana IDOA file not found: {self._file_path}")

    def fetch(self) -> list[dict]:
        """
        Load the IDOA xlsx and return one row per unique AFA-certified firm.

        Deduplicates on Bidder ID — the source assigns one Bidder ID per firm
        regardless of how many UNSPSC commodity codes it holds.
        """
        wb = openpyxl.load_workbook(str(self._file_path), read_only=True, data_only=True)
        try:
            ws = wb.active
            all_rows = list(ws.iter_rows(min_row=HEADER_ROW, values_only=True))
        finally:
            wb.close()

        header = all_rows[0]  # first row of our slice is the column header
        col = {str(name).strip(): i for i, name in enumerate(header) if name is not None}

        ethnic_col = col.get("Ethnic Group")
        bidder_col = col.get("Bidder ID")
        if ethnic_col is None:
            raise ValueError(f"Expected 'Ethnic Group' column; found: {list(col.keys())}")
        if bidder_col is None:
            raise ValueError(f"Expected 'Bidder ID' column; found: {list(col.keys())}")

        seen_bidder_ids = set()
        raw = []
        for row in all_rows[1:]:  # skip the header row itself
            if str(row[ethnic_col] or "").strip() != BLACK_CODE:
                continue
            bidder_id = str(row[bidder_col] or "").strip()
            if bidder_id in seen_bidder_ids:
                continue
            seen_bidder_ids.add(bidder_id)
            raw_row = {str(name).strip(): row[idx]
                       for name, idx in col.items() if name}
            raw.append(raw_row)

        return raw

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for source_row in raw:
            record = self.map_record(source_row)

            # Combine owner first + last name
            first = str(source_row.get("First Name") or "").strip()
            last  = str(source_row.get("LastName") or "").strip()
            record["owner_name"] = " ".join(filter(None, [first, last]))

            record["source_business_id"] = str(
                source_row.get("Bidder ID") or ""
            ).strip()
            record["certification"] = str(
                source_row.get("Application Type") or "MBE"
            ).strip()
            record["last_verified"] = str(date.today())
            records.append(record)
        return records
