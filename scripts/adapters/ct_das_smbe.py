"""
Connecticut DAS Supplier Diversity (SBE/MBE) adapter.

Source: Connecticut Open Data (Socrata) — DAS certified Small Business
Enterprise / Minority Business Enterprise directory. Public CSV/JSON API,
no authentication.
Dataset: data.ct.gov resource y4ub-e4dd
Filter: class_description_detailed == "Black American"
Confidence: confirmed_black — race/ethnicity is an explicit, published per-firm
field (class_description_detailed), verified verbatim against the live API.

Observed distinct values of class_description_detailed (2026-06-10):
  "Black American", "Hispanic American",
  "Asian Pacific American and Pacific Islander", "Iberian Peninsula",
  "American Indian", "No minority race/ethnicity identified"

This is a fully auto-fetching source (no manual download), suitable for the
autonomous quarterly cron.
"""
import csv
import io
import sys
from datetime import date
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

CT_CSV_URL = "https://data.ct.gov/resource/y4ub-e4dd.csv"
# Socrata caps unpaginated responses at 1,000 rows; raise the limit to pull the
# full directory (~7,800 rows) in one request.
FETCH_LIMIT = 50000
BLACK_VALUE = "Black American"
ETHNICITY_FIELD = "class_description_detailed"


class CtDasSmbeAdapter(AdapterBase):
    SOURCE_ID   = "ct_das_smbe"
    SOURCE_NAME = "Connecticut DAS Supplier Diversity"
    PROGRAM     = "MBE"
    GEOGRAPHY   = "Connecticut"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "vendorname":                            "business_name",
        "business_address1":                     "address_street",
        "townnamecrosswalk_standardized_town":   "address_city",
        "business_state":                        "address_state",
        "zip":                                   "address_zip",
        "gs_code":                               "naics_code",
        "goods_and_services":                    "industry",
        "product":                               "description",
    }

    def fetch(self) -> list[dict]:
        """Pull the CT directory and keep only Black American certified firms."""
        response = requests.get(CT_CSV_URL, params={"$limit": FETCH_LIMIT}, timeout=60)
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        return [
            row for row in reader
            if (row.get(ETHNICITY_FIELD) or "").strip() == BLACK_VALUE
        ]

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for source_row in raw:
            record = self.map_record(source_row)
            # certification_type is "MBE" or "SBE"; default MBE.
            record["certification"] = (
                (source_row.get("certification_type") or "").strip() or "MBE"
            )
            # The dataset ships authoritative coordinates as a WKT POINT; use them
            # so these records skip the geocoder entirely.
            lat, lon = _parse_point(source_row.get("location", ""))
            if lat and lon:
                record["latitude"] = lat
                record["longitude"] = lon
            record["last_verified"] = str(date.today())
            records.append(record)
        return records


def _parse_point(wkt: str) -> tuple[str, str]:
    """Parse 'POINT (lon lat)' → ('lat', 'lon'). Returns ('','') if unparseable."""
    if not wkt or "POINT" not in wkt.upper():
        return "", ""
    try:
        inner = wkt[wkt.index("(") + 1:wkt.index(")")].strip()
        lon_str, lat_str = inner.split()
        return lat_str, lon_str
    except (ValueError, IndexError):
        return "", ""
