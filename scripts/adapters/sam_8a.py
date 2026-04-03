"""
SAM.gov 8(a) Business Development Program adapter.

Source: SAM.gov Entity Management API v3
Filter: sbaBusinessTypeDesc="8(a) Business Development"
Confidence: mbe_unverified — 8(a) certifies socially disadvantaged businesses
broadly (Black, Hispanic, Native American, Asian Pacific, etc.); race/ethnicity
is not exposed in the API.
"""
import os
import sys
import time
import warnings
from datetime import date
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

SAM_API_URL = "https://api.sam.gov/entity-information/v3/entities"
PAGE_SIZE = 10  # SAM.gov API maximum
REQUEST_DELAY = 0.25  # seconds between pages — stay well under rate limit


class SamEightAAdapter(AdapterBase):
    SOURCE_ID   = "sam_8a"
    SOURCE_NAME = "SAM.gov 8(a) Certified Businesses"
    PROGRAM     = "8(a)"
    GEOGRAPHY   = "National"
    CONFIDENCE  = "mbe_unverified"

    FIELD_MAP = {
        "legalBusinessName":    "business_name",
        "addressLine1":         "address_street",
        "city":                 "address_city",
        "stateOrProvinceCode":  "address_state",
        "zipCode":              "address_zip",
        "entityURL":            "website",
        "primaryNaics":         "naics_code",
    }

    def __init__(self, api_key: str = None):
        key = api_key or os.environ.get("SAM_GOV_API_KEY", "")
        if not key:
            raise ValueError(
                "SAM_GOV_API_KEY environment variable is required for the SAM.gov adapter. "
                "Get a key at sam.gov under your profile settings."
            )
        self._api_key = key

    def fetch(self) -> list[dict]:
        """
        Paginate through all 8(a) certified entities from the SAM.gov API.
        Returns a list of flat source-row dicts ready for map_record().
        """
        entities = []
        page = 0

        while True:
            params = {
                "api_key": self._api_key,
                "sbaBusinessTypeDesc": "8(a) Business Development",
                "includeSections": "entityRegistration,coreData,assertions",
                "size": PAGE_SIZE,
                "page": page,
            }
            response = requests.get(SAM_API_URL, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            total = data.get("totalRecords", 0)
            batch = data.get("entityData") or []
            entities.extend(batch)

            if not batch or len(entities) >= total:
                break

            page += 1
            time.sleep(REQUEST_DELAY)

        return [_flatten(e) for e in entities]

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for source_row in raw:
            record = self.map_record(source_row)
            record["source_business_id"] = source_row.get("ueiSAM", "")
            record["certification"] = "8(a)"
            record["last_verified"] = str(date.today())
            records.append(record)
        return records


def _flatten(entity: dict) -> dict:
    """
    Flatten a nested SAM.gov entity dict into a single-level dict
    suitable for map_record(). Keys that collide are prefixed with
    their section name.
    """
    reg = entity.get("entityRegistration") or {}
    addr = (entity.get("coreData") or {}).get("physicalAddress") or {}
    info = (entity.get("coreData") or {}).get("entityInformation") or {}
    goods = (entity.get("assertions") or {}).get("goodsAndServices") or {}

    return {
        "ueiSAM":               reg.get("ueiSAM", ""),
        "legalBusinessName":    reg.get("legalBusinessName", ""),
        "addressLine1":         addr.get("addressLine1", ""),
        "city":                 addr.get("city", ""),
        "stateOrProvinceCode":  addr.get("stateOrProvinceCode", ""),
        "zipCode":              addr.get("zipCode", ""),
        "entityURL":            info.get("entityURL", ""),
        "primaryNaics":         goods.get("primaryNaics", ""),
        # Extras → land in source_fields via map_record()
        "cageCode":             reg.get("cageCode", ""),
        "registrationStatus":   reg.get("registrationStatus", ""),
        "registrationExpirationDate": reg.get("registrationExpirationDate", ""),
    }
