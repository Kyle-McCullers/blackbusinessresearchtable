"""
Maryland MBE (Minority Business Enterprise) Program adapter.

Source: MDOT OMBE certified vendor directory via B2Gnow/Gob2G portal.
URL: https://marylandmdbe.gob2g.com/FrontEnd/searchcertifieddirectory.asp
Filter: Minority Status = "African American" + "African American / Female"
Confidence: confirmed_black — race/ethnicity is an explicit certification field.

Data access: The portal requires a human-triggered download (client-side CAPTCHA).
Download the filtered CSV quarterly and point this adapter at the file:
  1. Go to https://marylandmdbe.gob2g.com/FrontEnd/searchcertifieddirectory.asp
  2. Under "Search by Reference → Minority Status", select:
       "African American" AND "African American / Female"
  3. Click Search, then "Download to CSV" (enter the on-screen code)
  4. Set MD_MBE_FILE env var or pass file_path= to the adapter constructor.
"""
import csv
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DEFAULT_FILE_ENV = "MD_MBE_FILE"


class MdMbeAdapter(AdapterBase):
    SOURCE_ID   = "md_mbe"
    SOURCE_NAME = "Maryland MBE Program"
    PROGRAM     = "MBE"
    GEOGRAPHY   = "Maryland"
    CONFIDENCE  = "confirmed_black"

    # B2Gnow export column names (from marylandmdbe.gob2g.com CSV export).
    # Column names are quoted strings; leading/trailing spaces stripped in fetch().
    FIELD_MAP = {
        "Firm Name":      "business_name",
        "Address":        "address_street",
        "City":           "address_city",
        "State":          "address_state",
        "Zip":            "address_zip",
        "Phone":          "phone",
        "Email":          "email",
        "Web Site":       "website",
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
        """Load the pre-downloaded B2Gnow CSV export."""
        rows = []
        with open(self._file_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append({k.strip(): v.strip() if isinstance(v, str) else v
                              for k, v in row.items()})
        return rows

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for source_row in raw:
            record = self.map_record(source_row)
            # Firm ID / VendorID is the stable identifier in B2Gnow exports
            record["source_business_id"] = source_row.get("Firm ID", "").strip()
            record["certification"] = "MBE"
            record["last_verified"] = str(date.today())
            records.append(record)
        return records
