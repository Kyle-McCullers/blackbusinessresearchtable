"""
Delaware Office of Supplier Diversity (OSD) adapter.

Source: Delaware Open Data (Socrata) — OSD 2025 certified business directory.
Public CSV/JSON API, no authentication.
Dataset: data.delaware.gov resource 8dxf-6ahp
Filter: ddd_baa == "YES"   (one boolean column per disadvantage group)
Confidence: confirmed_black — ddd_baa is the published "Black or African
American" ownership flag, verified verbatim against the live API.

The OSD dataset uses a column-per-group layout rather than one categorical
ethnicity field. The disadvantage-group columns observed (2026-06-10):
  ddd_baa  = Black or African American
  ddd_ha   = Hispanic American
  ddd_aapa = Asian / Asian-Pacific American
  ddd_saa  = Subcontinent Asian American
  ddd_na   = Native American
  ddd_f    = Female
  ddd_v    = Veteran
  ddd_sdvi = Service-Disabled Veteran
  ddd_id   = Individual with Disability
Each is "YES" or blank. 335 firms have ddd_baa = YES.

Fully auto-fetching (no manual download); suitable for the autonomous cron.
NOTE: use dataset 8dxf-6ahp (2025). The older s4ev-nzhm now requires login.
"""
import csv
import io
import sys
from datetime import date
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DE_CSV_URL = "https://data.delaware.gov/resource/8dxf-6ahp.csv"
FETCH_LIMIT = 50000
BLACK_FIELD = "ddd_baa"
BLACK_VALUE = "YES"


class DeOsdAdapter(AdapterBase):
    SOURCE_ID   = "de_osd"
    SOURCE_NAME = "Delaware Office of Supplier Diversity"
    PROGRAM     = "MWBE"
    GEOGRAPHY   = "Delaware"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "name":               "business_name",
        "primarycontactname": "owner_name",
        "address":            "address_street",
        "city":               "address_city",
        "state":              "address_state",
        "zipcode":            "address_zip",
        "phonenumber":        "phone",
        "email":              "email",
        "description":        "description",
    }

    def fetch(self) -> list[dict]:
        """Pull the OSD directory and keep only Black/African American firms."""
        response = requests.get(DE_CSV_URL, params={"$limit": FETCH_LIMIT}, timeout=60)
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        return [
            row for row in reader
            if (row.get(BLACK_FIELD) or "").strip() == BLACK_VALUE
        ]

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for source_row in raw:
            record = self.map_record(source_row)
            record["source_business_id"] = (
                source_row.get("certificatenumber") or ""
            ).strip()
            record["certification"] = "MBE"
            record["last_verified"] = str(date.today())
            records.append(record)
        return records
