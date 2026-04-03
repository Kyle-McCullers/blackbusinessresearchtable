"""
Maryland MBE (Minority Business Enterprise) Program adapter.

Source: MDOT OMBE certified vendor directory — B2Gnow/Gob2G CSV export.
URL: https://marylandmdbe.gob2g.com/FrontEnd/searchcertifieddirectory.asp
Filter: Minority Status in {"African American", "African American / Female"}
Confidence: confirmed_black — race/ethnicity is an explicit certification field.

File format notes:
- First 5 rows are metadata; row 5 (0-indexed) is the header.
- Encoding: latin-1 (the file uses Windows-1252 smart quotes).
- Each firm appears once per certification type (MBE, DBE, SBE, ACDBE).
  Deduplication is done on Certification Number — one record per firm.
- Zip codes have a leading tab character; stripped in fetch().

How to download the file quarterly:
  1. Go to https://marylandmdbe.gob2g.com/FrontEnd/searchcertifieddirectory.asp
  2. Under "Search by Reference → Minority Status", hold Cmd and select both
     "African American" AND "African American / Female"
  3. Click Search, enter on-screen code, click "Download to CSV"
  4. Set MD_MBE_FILE env var to the downloaded path.
"""
import csv
import io
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DEFAULT_FILE_ENV = "MD_MBE_FILE"
METADATA_ROWS = 5  # rows before the header row in the MDOT CSV export
ENCODING = "latin-1"

# Minority Status values that indicate African American / Black ownership
AA_STATUSES = {"African American", "African American / Female"}


class MdMbeAdapter(AdapterBase):
    SOURCE_ID   = "md_mbe"
    SOURCE_NAME = "Maryland MBE Program"
    PROGRAM     = "MBE"
    GEOGRAPHY   = "Maryland"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "Company Name":      "business_name",
        "Physical Address":  "address_street",
        "City":              "address_city",
        "State":             "address_state",
        "Zip":               "address_zip",
        "Phone":             "phone",
        "Email":             "email",
        "Website":           "website",
    }

    def __init__(self, file_path: Path = None):
        path = file_path or os.environ.get(DEFAULT_FILE_ENV, "")
        if not path:
            raise ValueError(
                f"{DEFAULT_FILE_ENV} environment variable is required for the Maryland MBE adapter. "
                "Download the African American filtered CSV from "
                "https://marylandmdbe.gob2g.com/FrontEnd/searchcertifieddirectory.asp "
                "and set the env var to its path."
            )
        self._file_path = Path(path)
        if not self._file_path.exists():
            raise FileNotFoundError(f"Maryland MBE file not found: {self._file_path}")

    def fetch(self) -> list[dict]:
        """
        Load the MDOT CSV export.

        - Skips the 5-row metadata header.
        - Filters for African American / African American Female ownership.
        - Deduplicates on Certification Number (each firm appears once per
          cert type; we keep the first occurrence).
        """
        with open(self._file_path, encoding=ENCODING) as f:
            lines = f.readlines()

        data = io.StringIO("".join(lines[METADATA_ROWS:]))
        reader = csv.DictReader(data)

        seen_cert_numbers = set()
        rows = []
        for raw in reader:
            # Normalize all keys and values
            row = {k.strip(): (v.strip() if isinstance(v, str) else ("" if v is None else v))
                   for k, v in raw.items() if k}
            # Strip leading tab from zip codes
            if "Zip" in row:
                row["Zip"] = row["Zip"].strip()
            # Filter to African American owners
            if row.get("Minority Status") not in AA_STATUSES:
                continue
            # Deduplicate by Certification Number
            cert_num = row.get("Certification Number", "")
            if cert_num in seen_cert_numbers:
                continue
            seen_cert_numbers.add(cert_num)
            rows.append(row)

        return rows

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for source_row in raw:
            record = self.map_record(source_row)

            # Combine owner first + last name
            first = source_row.get("Owner First", "").strip()
            last = source_row.get("Owner Last", "").strip()
            record["owner_name"] = " ".join(filter(None, [first, last]))

            record["source_business_id"] = source_row.get("Certification Number", "").strip()
            record["certification"] = "MBE"
            record["last_verified"] = str(date.today())
            records.append(record)
        return records
