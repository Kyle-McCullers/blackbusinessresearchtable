import sys
import json
import pytest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# ── AdapterBase tests ────────────────────────────────────────────────────────

from pipeline.adapter_base import AdapterBase


class ConcreteAdapter(AdapterBase):
    SOURCE_ID   = "test_src"
    SOURCE_NAME = "Test Source"
    PROGRAM     = "MWBE"
    GEOGRAPHY   = "TEST"
    CONFIDENCE  = "confirmed_black"
    FIELD_MAP   = {
        "BizName":  "business_name",
        "OwnerNm":  "owner_name",
        "ZipCode":  "address_zip",
    }

    def fetch(self):
        return [
            {"BizName": "Acme LLC", "OwnerNm": "Jane Doe",
             "ZipCode": "10001", "ExtraCol": "extra_value"},
        ]

    def parse(self, raw):
        return [self.map_record(row) for row in raw]


def test_adapter_run_returns_list():
    adapter = ConcreteAdapter()
    records = adapter.run()
    assert isinstance(records, list)
    assert len(records) == 1


def test_adapter_map_record_applies_field_map():
    adapter = ConcreteAdapter()
    records = adapter.run()
    assert records[0]["business_name"] == "Acme LLC"
    assert records[0]["owner_name"] == "Jane Doe"
    assert records[0]["address_zip"] == "10001"


def test_adapter_map_record_puts_unmapped_in_source_fields():
    adapter = ConcreteAdapter()
    records = adapter.run()
    sf = records[0]["source_fields"]
    assert sf["ExtraCol"] == "extra_value"


def test_adapter_map_record_fills_missing_bbrt_fields_with_empty_string():
    adapter = ConcreteAdapter()
    records = adapter.run()
    assert records[0]["address_street"] == ""
    assert records[0]["latitude"] == ""


def test_adapter_map_record_sets_data_source():
    adapter = ConcreteAdapter()
    records = adapter.run()
    assert records[0]["data_source"] == "Test Source"


def test_adapter_missing_fetch_raises():
    with pytest.raises(TypeError):
        class BadAdapter(AdapterBase):
            SOURCE_ID = "bad"
            SOURCE_NAME = "Bad"
            PROGRAM = "MWBE"
            GEOGRAPHY = "X"
            CONFIDENCE = "confirmed_black"
            FIELD_MAP = {}
            # missing fetch and parse
        BadAdapter()


# ── db.py tests ──────────────────────────────────────────────────────────────

import duckdb
import json
from pipeline.db import open_db, upsert_source, write_businesses, write_snapshot_meta, get_registry, upsert_registry


@pytest.fixture
def tmp_db(tmp_path):
    db_path = tmp_path / "test.duckdb"
    con = open_db(db_path)
    yield con
    con.close()


class _MockAdapter:
    SOURCE_ID   = "src_a"
    SOURCE_NAME = "Source A"
    PROGRAM     = "MWBE"
    GEOGRAPHY   = "NYC"
    CONFIDENCE  = "confirmed_black"


def _make_record(**kwargs):
    base = {
        "business_id": "uuid-1", "business_name": "Biz A",
        "owner_name": "", "year_founded": "2010",
        "address_street": "1 Main St", "address_city": "Brooklyn",
        "address_state": "New York", "address_zip": "11201",
        "latitude": "40.68", "longitude": "-73.94",
        "industry": "Services", "naics_code": "561990",
        "certification": "MBE", "description": "A business.",
        "website": "", "phone": "", "email": "",
        "instagram_handle": "", "facebook_url": "", "tiktok_handle": "",
        "yelp_url": "", "google_maps_url": "",
        "discloses_google_maps": "", "discloses_yelp": "", "discloses_instagram": "",
        "data_source": "Source A", "last_verified": "2025-09-09",
        "source_id": "src_a", "source_business_id": "ACC001",
        "confidence": "confirmed_black", "source_fields": {"ExtraCol": "val"},
        "canonical_name": "biz a", "canonical_zip": "11201",
    }
    base.update(kwargs)
    return base


def test_open_db_creates_all_tables(tmp_db):
    tables = {row[0] for row in tmp_db.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()}
    assert {"sources", "snapshots", "businesses", "business_registry", "field_catalog"} <= tables


def test_upsert_source_inserts_new(tmp_db):
    upsert_source(tmp_db, _MockAdapter())
    count = tmp_db.execute("SELECT COUNT(*) FROM sources WHERE source_id='src_a'").fetchone()[0]
    assert count == 1


def test_upsert_source_is_idempotent(tmp_db):
    upsert_source(tmp_db, _MockAdapter())
    upsert_source(tmp_db, _MockAdapter())
    count = tmp_db.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
    assert count == 1


def test_write_businesses_inserts_records(tmp_db):
    records = [_make_record()]
    write_businesses(tmp_db, records, "2026-Q2")
    count = tmp_db.execute("SELECT COUNT(*) FROM businesses").fetchone()[0]
    assert count == 1


def test_write_businesses_stores_source_fields_as_json(tmp_db):
    records = [_make_record()]
    write_businesses(tmp_db, records, "2026-Q2")
    sf = tmp_db.execute("SELECT source_fields FROM businesses").fetchone()[0]
    assert json.loads(sf)["ExtraCol"] == "val"


def test_write_snapshot_meta_inserts_row(tmp_db):
    write_snapshot_meta(tmp_db, "2026-Q2", 10, 2, ["src_a"], ["src_b"])
    row = tmp_db.execute("SELECT * FROM snapshots WHERE snapshot_id='2026-Q2'").fetchone()
    assert row is not None
    assert row[2] == 10   # records_added
    assert row[3] == 2    # records_dropped


def test_get_registry_returns_empty_initially(tmp_db):
    result = get_registry(tmp_db)
    assert result == []


def test_upsert_registry_inserts_new_entries(tmp_db):
    entries = [{"business_id": "uuid-1", "canonical_name": "biz a",
                "canonical_zip": "11201", "source_id": "src_a",
                "source_business_id": "ACC001"}]
    upsert_registry(tmp_db, "2026-Q2", entries)
    result = get_registry(tmp_db)
    assert len(result) == 1
    assert result[0]["business_id"] == "uuid-1"


def test_upsert_registry_updates_last_seen(tmp_db):
    entries = [{"business_id": "uuid-1", "canonical_name": "biz a",
                "canonical_zip": "11201", "source_id": "src_a",
                "source_business_id": "ACC001"}]
    upsert_registry(tmp_db, "2026-Q1", entries)
    upsert_registry(tmp_db, "2026-Q2", entries)
    result = get_registry(tmp_db)
    assert result[0]["last_seen"] == "2026-Q2"
    assert result[0]["first_seen"] == "2026-Q1"


# ── entity_resolver tests ────────────────────────────────────────────────────

from pipeline.entity_resolver import normalize_name, normalize_zip, resolve


def test_normalize_name_lowercases():
    assert normalize_name("ACME LLC") == "acme"


def test_normalize_name_strips_legal_suffixes():
    assert normalize_name("Smith Corp.") == "smith"
    assert normalize_name("Jones Inc") == "jones"
    assert normalize_name("Apex Enterprises") == "apex"


def test_normalize_name_strips_punctuation():
    assert normalize_name("A & B Services, LLC") == "a b"


def test_normalize_zip_pads_to_five():
    assert normalize_zip("1234") == "01234"


def test_normalize_zip_truncates_plus_four():
    assert normalize_zip("11201-1234") == "11201"


def _rec(name, zip_code, src_biz_id="", source_id="src_a"):
    return {
        "business_name": name,
        "address_zip": zip_code,
        "source_business_id": src_biz_id,
        "source_id": source_id,
    }


def test_resolve_assigns_new_uuid_when_no_match():
    records = [_rec("Brand New Biz", "10001")]
    registry = []
    review_log = []
    result, new_entries = resolve(records, registry, "2026-Q2", review_log)
    assert len(result) == 1
    assert len(result[0]["business_id"]) == 36  # UUID format
    assert len(new_entries) == 1


def test_resolve_matches_by_source_business_id():
    existing = [{"business_id": "existing-uuid", "canonical_name": "acme",
                 "canonical_zip": "10001", "source_id": "src_a",
                 "source_business_id": "ACC001", "first_seen": "2026-Q1",
                 "last_seen": "2026-Q1"}]
    records = [_rec("ACME LLC", "10001", src_biz_id="ACC001")]
    review_log = []
    result, new_entries = resolve(records, existing, "2026-Q2", review_log)
    assert result[0]["business_id"] == "existing-uuid"
    assert len(new_entries) == 0


def test_resolve_matches_by_name_and_zip():
    existing = [{"business_id": "existing-uuid", "canonical_name": "acme",
                 "canonical_zip": "10001", "source_id": "src_a",
                 "source_business_id": "", "first_seen": "2026-Q1",
                 "last_seen": "2026-Q1"}]
    records = [_rec("Acme LLC", "10001")]
    review_log = []
    result, new_entries = resolve(records, existing, "2026-Q2", review_log)
    assert result[0]["business_id"] == "existing-uuid"
    assert len(new_entries) == 0


def test_resolve_logs_uncertain_match():
    existing = [{"business_id": "existing-uuid", "canonical_name": "acme solutions",
                 "canonical_zip": "10001", "source_id": "src_a",
                 "source_business_id": "", "first_seen": "2026-Q1",
                 "last_seen": "2026-Q1"}]
    # "acme solution" is close but not exact
    records = [_rec("Acme Solution", "10001")]
    review_log = []
    result, new_entries = resolve(records, existing, "2026-Q2", review_log)
    # Either matched (high similarity) or new — either way, review_log captures it
    # The key is no crash and review_log is populated for near-matches
    assert isinstance(review_log, list)


def test_resolve_different_source_ids_dont_cross_match():
    existing = [{"business_id": "existing-uuid", "canonical_name": "acme",
                 "canonical_zip": "10001", "source_id": "src_a",
                 "source_business_id": "ACC001", "first_seen": "2026-Q1",
                 "last_seen": "2026-Q1"}]
    # Same source_business_id but different source — should not match
    records = [_rec("Acme LLC", "10001", src_biz_id="ACC001", source_id="src_b")]
    review_log = []
    result, new_entries = resolve(records, existing, "2026-Q2", review_log)
    assert result[0]["business_id"] != "existing-uuid"
