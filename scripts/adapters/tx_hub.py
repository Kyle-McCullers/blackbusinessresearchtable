"""
Texas HUB (Historically Underutilized Business) Program adapter.

Source: Texas Comptroller of Public Accounts — open CSV, no authentication.
Filter: ELIGIBILITY CODE == 'BL' (Black American).
Confidence: confirmed_black — race/ethnicity is an explicit certification field.

Context: In January 2026, Acting Comptroller Kelly Hancock canceled all race/sex-based
HUB certifications, converting the program to VetHUB (service-disabled veterans only).
All BL records now carry STATUS CODE 'D' (decertified) or 'I' (inactive).
A lawsuit seeking reinstatement was filed March 2, 2026. This adapter captures the
full historical record; hub_status is preserved in source_fields for tracking.
"""
import csv
import io
import sys
from datetime import date
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

HUB_CSV_URL = "https://comptroller.texas.gov/auto-data/purchasing/hub_name.csv"


class TxHubAdapter(AdapterBase):
    SOURCE_ID   = "tx_hub"
    SOURCE_NAME = "Texas HUB Program (Historical)"
    PROGRAM     = "HUB"
    GEOGRAPHY   = "Texas"
    CONFIDENCE  = "confirmed_black"

    # Column names in the CSV have inconsistent leading spaces — normalized via
    # _strip_keys() in fetch(). Values shown here are post-strip.
    FIELD_MAP = {
        "VENDOR NAME":        "business_name",
        "VENDOR ADDRESS LINE 1": "address_street",
        "CITY":               "address_city",
        "STATE":              "address_state",
        "ZIP CODE":           "address_zip",
        "INTERNET ADDRESS":   "website",
        "PHONE NUMBER":       "phone",
        "BUSINESS DESCRIPTION": "description",
    }

    def fetch(self) -> list[dict]:
        """Download the HUB CSV and return BL (Black American) rows."""
        response = requests.get(HUB_CSV_URL, timeout=60)
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        rows = [_strip_keys(row) for row in reader]
        return [row for row in rows if row.get("ELIGIBILITY CODE", "").strip() == "BL"]

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for source_row in raw:
            record = self.map_record(source_row)
            record["source_business_id"] = source_row.get("VENDOR ID NUMBER", "").strip()
            record["certification"] = "HUB"
            record["last_verified"] = str(date.today())
            records.append(record)
        return records


def _strip_keys(row: dict) -> dict:
    """Strip leading/trailing whitespace from all column names and values."""
    return {k.strip(): v.strip() if isinstance(v, str) else v for k, v in row.items()}
