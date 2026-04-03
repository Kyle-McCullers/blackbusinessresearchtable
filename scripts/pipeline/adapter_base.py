from abc import ABC, abstractmethod

# Every record dict produced by map_record() has two kinds of keys:
#   1. The fields in BBRT_FIELDS (standard schema, all default to "").
#   2. "source_fields" — a dict of every source column not covered by FIELD_MAP.
#      It is NOT in BBRT_FIELDS. Downstream consumers (DuckDB layer, CSV exporter)
#      must handle it explicitly as a separate JSON column.
BBRT_FIELDS = [
    "business_id", "business_name", "owner_name", "year_founded",
    "address_street", "address_city", "address_state", "address_zip",
    "latitude", "longitude", "industry", "naics_code", "certification",
    "description", "website", "phone", "email",
    "instagram_handle", "facebook_url", "tiktok_handle",
    "yelp_url", "google_maps_url",
    "discloses_google_maps", "discloses_yelp", "discloses_instagram",
    "data_source", "last_verified",
]


class AdapterBase(ABC):
    SOURCE_ID   = ""
    SOURCE_NAME = ""
    PROGRAM     = ""    # "MWBE" | "DBE" | "8(a)" | "CBE" | "HUB"
    GEOGRAPHY   = ""    # "NYC" | "TX" | "National" | etc.
    CONFIDENCE  = ""    # "confirmed_black" | "mbe_unverified"
    FIELD_MAP   = {}    # {source_column_name: bbrt_field_name}

    @abstractmethod
    def fetch(self):
        """Download or call the source. Return raw data in any form."""

    @abstractmethod
    def parse(self, raw) -> list[dict]:
        """Map raw source data to BBRT schema. Return list of record dicts."""

    def run(self) -> list[dict]:
        """Fetch and parse. Called by the orchestrator."""
        raw = self.fetch()
        return self.parse(raw)

    def map_record(self, source_row: dict) -> dict:
        """
        Apply FIELD_MAP to one source row.
        - Keys in FIELD_MAP are mapped to their BBRT field names.
        - Keys not in FIELD_MAP go into source_fields dict.
        - All BBRT fields not set by FIELD_MAP default to "".
        - data_source is set from SOURCE_NAME.
        """
        record = {field: "" for field in BBRT_FIELDS}
        source_fields = {}
        for src_key, value in source_row.items():
            str_value = "" if value is None else str(value).strip()
            if src_key in self.FIELD_MAP:
                record[self.FIELD_MAP[src_key]] = str_value
            else:
                source_fields[src_key] = str_value
        record["source_fields"] = source_fields
        record["data_source"] = self.SOURCE_NAME
        return record
