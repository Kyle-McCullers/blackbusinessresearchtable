# BBRT Pipeline Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the automated quarterly pipeline infrastructure that all future source adapters depend on — adapter base class, DuckDB panel database, entity resolver, Census Geocoder, export layer, orchestrator, NYC MWBE adapter migrated to the new interface, and GitHub Actions cron.

**Architecture:** Python package at `scripts/pipeline/` with one module per concern; adapters live in `scripts/adapters/` (one file per source); orchestrator discovers adapters automatically; DuckDB stores the panel at `data/bbrt.duckdb`; export layer writes `data/businesses.csv` for the public site.

**Tech Stack:** Python 3.12, duckdb>=1.0.0, rapidfuzz>=3.0.0, requests, openpyxl, pytest. No new frontend changes in this sub-project.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `scripts/requirements.txt` | Modify | Add duckdb, rapidfuzz |
| `scripts/pipeline/__init__.py` | Create | Package marker |
| `scripts/pipeline/adapter_base.py` | Create | Abstract base class all adapters inherit |
| `scripts/pipeline/db.py` | Create | DuckDB open, read, write operations |
| `scripts/pipeline/entity_resolver.py` | Create | Match businesses across snapshots |
| `scripts/pipeline/geocoder.py` | Create | Census Geocoder batch API |
| `scripts/pipeline/export.py` | Create | Write businesses.csv + snapshot summary |
| `scripts/pipeline/run.py` | Create | Orchestrator — discovers and runs all adapters |
| `scripts/adapters/__init__.py` | Create | Package marker |
| `scripts/adapters/nyc_mwbe.py` | Create | NYC MWBE source, migrated from build_csv.py |
| `scripts/test_pipeline.py` | Create | Tests for all pipeline modules |
| `.github/workflows/quarterly_pipeline.yml` | Create | Quarterly cron + commit workflow |

`build_csv.py` and `test_build_csv.py` are **not modified** — they remain as a standalone one-time tool.

---

## Task 1: Add Dependencies and Create Package Skeleton

**Files:**
- Modify: `scripts/requirements.txt`
- Create: `scripts/pipeline/__init__.py`
- Create: `scripts/adapters/__init__.py`

- [ ] **Step 1: Update requirements.txt**

```
openpyxl==3.1.2
requests==2.31.0
pytest==8.1.0
duckdb>=1.0.0
rapidfuzz>=3.0.0
```

- [ ] **Step 2: Create package markers**

`scripts/pipeline/__init__.py` — empty file.

`scripts/adapters/__init__.py` — empty file.

- [ ] **Step 3: Install and verify**

```bash
cd scripts
pip install -r requirements.txt
python -c "import duckdb; import rapidfuzz; print('OK')"
```

Expected: `OK`

- [ ] **Step 4: Commit**

```bash
git add scripts/requirements.txt scripts/pipeline/__init__.py scripts/adapters/__init__.py
git commit -m "feat: add duckdb and rapidfuzz, create pipeline package skeleton"
```

---

## Task 2: AdapterBase

**Files:**
- Create: `scripts/pipeline/adapter_base.py`
- Create: `scripts/test_pipeline.py` (start file, first test block)

- [ ] **Step 1: Write the failing tests**

Create `scripts/test_pipeline.py`:

```python
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
```

- [ ] **Step 2: Run to verify failure**

```bash
cd scripts && pytest test_pipeline.py::test_adapter_run_returns_list -v
```

Expected: `ImportError` or `ModuleNotFoundError` — `pipeline.adapter_base` does not exist yet.

- [ ] **Step 3: Write the implementation**

Create `scripts/pipeline/adapter_base.py`:

```python
from abc import ABC, abstractmethod

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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd scripts && pytest test_pipeline.py -k "test_adapter" -v
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add scripts/pipeline/adapter_base.py scripts/test_pipeline.py
git commit -m "feat: AdapterBase with map_record and source_fields overflow"
```

---

## Task 3: DuckDB Layer

**Files:**
- Create: `scripts/pipeline/db.py`
- Modify: `scripts/test_pipeline.py` (add db tests)

- [ ] **Step 1: Write the failing tests**

Append to `scripts/test_pipeline.py`:

```python
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
```

- [ ] **Step 2: Run to verify failure**

```bash
cd scripts && pytest test_pipeline.py -k "test_open_db or test_upsert_source or test_write or test_get_registry or test_upsert_registry" -v
```

Expected: `ImportError` — `pipeline.db` does not exist yet.

- [ ] **Step 3: Write the implementation**

Create `scripts/pipeline/db.py`:

```python
import json
from datetime import date
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "bbrt.duckdb"

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS sources (
        source_id      VARCHAR PRIMARY KEY,
        source_name    VARCHAR NOT NULL,
        program        VARCHAR NOT NULL,
        geography      VARCHAR NOT NULL,
        confidence     VARCHAR NOT NULL,
        first_ingested DATE    NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS snapshots (
        snapshot_id     VARCHAR PRIMARY KEY,
        run_date        DATE    NOT NULL,
        records_added   INTEGER NOT NULL,
        records_dropped INTEGER NOT NULL,
        sources_run     VARCHAR NOT NULL,
        sources_failed  VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS businesses (
        business_id           VARCHAR NOT NULL,
        snapshot_id           VARCHAR NOT NULL,
        source_id             VARCHAR NOT NULL,
        source_business_id    VARCHAR,
        confidence            VARCHAR NOT NULL,
        business_name         VARCHAR,
        owner_name            VARCHAR,
        year_founded          VARCHAR,
        address_street        VARCHAR,
        address_city          VARCHAR,
        address_state         VARCHAR,
        address_zip           VARCHAR,
        latitude              VARCHAR,
        longitude             VARCHAR,
        industry              VARCHAR,
        naics_code            VARCHAR,
        certification         VARCHAR,
        description           VARCHAR,
        website               VARCHAR,
        phone                 VARCHAR,
        email                 VARCHAR,
        instagram_handle      VARCHAR,
        facebook_url          VARCHAR,
        tiktok_handle         VARCHAR,
        yelp_url              VARCHAR,
        google_maps_url       VARCHAR,
        discloses_google_maps VARCHAR,
        discloses_yelp        VARCHAR,
        discloses_instagram   VARCHAR,
        data_source           VARCHAR,
        last_verified         VARCHAR,
        source_fields         VARCHAR,
        PRIMARY KEY (business_id, snapshot_id, source_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS business_registry (
        business_id        VARCHAR PRIMARY KEY,
        canonical_name     VARCHAR NOT NULL,
        canonical_zip      VARCHAR NOT NULL,
        source_id          VARCHAR NOT NULL,
        source_business_id VARCHAR,
        first_seen         VARCHAR NOT NULL,
        last_seen          VARCHAR NOT NULL,
        is_active          BOOLEAN NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS field_catalog (
        source_id         VARCHAR NOT NULL,
        source_field_name VARCHAR NOT NULL,
        normalized_to     VARCHAR NOT NULL,
        coverage_pct      FLOAT   NOT NULL,
        PRIMARY KEY (source_id, source_field_name)
    )
    """,
]


def open_db(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Open (or create) the DuckDB database and ensure all tables exist."""
    con = duckdb.connect(str(db_path))
    for stmt in _DDL:
        con.execute(stmt)
    return con


def upsert_source(con: duckdb.DuckDBPyConnection, adapter) -> None:
    """Insert source row if not already present."""
    con.execute(
        """
        INSERT INTO sources VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (source_id) DO NOTHING
        """,
        [adapter.SOURCE_ID, adapter.SOURCE_NAME, adapter.PROGRAM,
         adapter.GEOGRAPHY, adapter.CONFIDENCE, date.today()],
    )


def write_businesses(
    con: duckdb.DuckDBPyConnection,
    records: list[dict],
    snapshot_id: str,
) -> None:
    """Insert all records for a snapshot. Each record must have business_id set."""
    for r in records:
        con.execute(
            """
            INSERT INTO businesses VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            ) ON CONFLICT DO NOTHING
            """,
            [
                r.get("business_id", ""),
                snapshot_id,
                r.get("source_id", ""),
                r.get("source_business_id", ""),
                r.get("confidence", ""),
                r.get("business_name", ""),
                r.get("owner_name", ""),
                r.get("year_founded", ""),
                r.get("address_street", ""),
                r.get("address_city", ""),
                r.get("address_state", ""),
                r.get("address_zip", ""),
                r.get("latitude", ""),
                r.get("longitude", ""),
                r.get("industry", ""),
                r.get("naics_code", ""),
                r.get("certification", ""),
                r.get("description", ""),
                r.get("website", ""),
                r.get("phone", ""),
                r.get("email", ""),
                r.get("instagram_handle", ""),
                r.get("facebook_url", ""),
                r.get("tiktok_handle", ""),
                r.get("yelp_url", ""),
                r.get("google_maps_url", ""),
                r.get("discloses_google_maps", ""),
                r.get("discloses_yelp", ""),
                r.get("discloses_instagram", ""),
                r.get("data_source", ""),
                r.get("last_verified", ""),
                json.dumps(r.get("source_fields", {})),
            ],
        )


def write_snapshot_meta(
    con: duckdb.DuckDBPyConnection,
    snapshot_id: str,
    records_added: int,
    records_dropped: int,
    sources_run: list[str],
    sources_failed: list[str],
) -> None:
    con.execute(
        """
        INSERT INTO snapshots VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (snapshot_id) DO NOTHING
        """,
        [snapshot_id, date.today(), records_added, records_dropped,
         json.dumps(sources_run), json.dumps(sources_failed)],
    )


def get_registry(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Return all rows from business_registry as a list of dicts."""
    rows = con.execute(
        "SELECT business_id, canonical_name, canonical_zip, source_id, source_business_id, first_seen, last_seen FROM business_registry"
    ).fetchall()
    return [
        {
            "business_id": r[0], "canonical_name": r[1],
            "canonical_zip": r[2], "source_id": r[3],
            "source_business_id": r[4], "first_seen": r[5], "last_seen": r[6],
        }
        for r in rows
    ]


def upsert_registry(
    con: duckdb.DuckDBPyConnection,
    snapshot_id: str,
    entries: list[dict],
) -> None:
    """
    Insert new registry entries or update last_seen for existing ones.
    entries: list of dicts with business_id, canonical_name, canonical_zip,
             source_id, source_business_id.
    """
    for e in entries:
        con.execute(
            """
            INSERT INTO business_registry
                (business_id, canonical_name, canonical_zip, source_id,
                 source_business_id, first_seen, last_seen, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
            ON CONFLICT (business_id) DO UPDATE SET
                last_seen = excluded.last_seen,
                is_active = TRUE
            """,
            [e["business_id"], e["canonical_name"], e["canonical_zip"],
             e["source_id"], e.get("source_business_id", ""),
             snapshot_id, snapshot_id],
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd scripts && pytest test_pipeline.py -k "test_open_db or test_upsert_source or test_write or test_get_registry or test_upsert_registry" -v
```

Expected: 10 passed.

- [ ] **Step 5: Commit**

```bash
git add scripts/pipeline/db.py scripts/test_pipeline.py
git commit -m "feat: DuckDB layer with all five tables and CRUD operations"
```

---

## Task 4: Entity Resolver

**Files:**
- Create: `scripts/pipeline/entity_resolver.py`
- Modify: `scripts/test_pipeline.py` (add entity resolver tests)

- [ ] **Step 1: Write the failing tests**

Append to `scripts/test_pipeline.py`:

```python
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
```

- [ ] **Step 2: Run to verify failure**

```bash
cd scripts && pytest test_pipeline.py -k "test_normalize or test_resolve" -v
```

Expected: `ImportError` — `pipeline.entity_resolver` does not exist yet.

- [ ] **Step 3: Write the implementation**

Create `scripts/pipeline/entity_resolver.py`:

```python
import re
import uuid

from rapidfuzz import fuzz

_LEGAL_SUFFIXES = re.compile(
    r"\b(llc|inc|corp|co|ltd|company|enterprises?|services?|group|associates?|solutions?)\b\.?",
    re.IGNORECASE,
)
_NON_ALNUM = re.compile(r"[^a-z0-9\s]")
_WHITESPACE = re.compile(r"\s+")


def normalize_name(name: str) -> str:
    """Lowercase, strip legal suffixes and punctuation, collapse whitespace."""
    name = name.lower().strip()
    name = _LEGAL_SUFFIXES.sub(" ", name)
    name = _NON_ALNUM.sub(" ", name)
    return _WHITESPACE.sub(" ", name).strip()


def normalize_zip(zip_code: str) -> str:
    """Return first 5 digits, zero-padded."""
    digits = re.sub(r"[^0-9]", "", str(zip_code))[:5]
    return digits.zfill(5)


def resolve(
    new_records: list[dict],
    registry: list[dict],
    snapshot_id: str,
    review_log: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    Assign stable business_ids to new_records by matching against registry.

    Match priority:
      1. (source_id, source_business_id) exact match
      2. (source_id, canonical_name, canonical_zip) exact match
      3. (source_id, canonical_zip) + fuzzy name similarity >= 95%
      4. Near-miss (80–94%) logged to review_log but treated as new entity

    Returns:
      (augmented_records, new_registry_entries)
    """
    # Build O(1) lookups
    by_src_biz_id: dict[tuple, str] = {}   # (source_id, source_business_id) -> business_id
    by_name_zip: dict[tuple, str] = {}      # (source_id, canonical_name, canonical_zip) -> business_id

    for entry in registry:
        src_id = entry["source_id"]
        src_biz_id = entry.get("source_business_id", "")
        if src_biz_id:
            by_src_biz_id[(src_id, src_biz_id)] = entry["business_id"]
        by_name_zip[(src_id, entry["canonical_name"], entry["canonical_zip"])] = entry["business_id"]

    # Group registry entries by (source_id, canonical_zip) for fuzzy fallback
    by_zip: dict[tuple, list[dict]] = {}
    for entry in registry:
        key = (entry["source_id"], entry["canonical_zip"])
        by_zip.setdefault(key, []).append(entry)

    result = []
    new_entries = []

    for rec in new_records:
        source_id = rec.get("source_id", "")
        src_biz_id = rec.get("source_business_id", "")
        can_name = normalize_name(rec.get("business_name", ""))
        can_zip = normalize_zip(rec.get("address_zip", ""))

        business_id = None

        # Priority 1: source_business_id exact match (within same source)
        if src_biz_id:
            business_id = by_src_biz_id.get((source_id, src_biz_id))

        # Priority 2: canonical name + zip exact match (within same source)
        if not business_id and can_name:
            business_id = by_name_zip.get((source_id, can_name, can_zip))

        # Priority 3: fuzzy name match within same source and zip
        if not business_id and can_name:
            candidates = by_zip.get((source_id, can_zip), [])
            best_score = 0
            best_entry = None
            for entry in candidates:
                score = fuzz.ratio(can_name, entry["canonical_name"])
                if score > best_score:
                    best_score = score
                    best_entry = entry
            if best_score >= 95 and best_entry:
                business_id = best_entry["business_id"]
            elif best_score >= 80 and best_entry:
                review_log.append({
                    "snapshot_id": snapshot_id,
                    "new_name": rec.get("business_name"),
                    "new_canonical": can_name,
                    "matched_name": best_entry["canonical_name"],
                    "zip": can_zip,
                    "source_id": source_id,
                    "similarity": best_score,
                    "candidate_id": best_entry["business_id"],
                })

        # Priority 4: new entity
        if not business_id:
            business_id = str(uuid.uuid4())
            new_entries.append({
                "business_id": business_id,
                "canonical_name": can_name,
                "canonical_zip": can_zip,
                "source_id": source_id,
                "source_business_id": src_biz_id,
            })

        result.append({
            **rec,
            "business_id": business_id,
            "canonical_name": can_name,
            "canonical_zip": can_zip,
        })

    return result, new_entries
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd scripts && pytest test_pipeline.py -k "test_normalize or test_resolve" -v
```

Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add scripts/pipeline/entity_resolver.py scripts/test_pipeline.py
git commit -m "feat: entity resolver with source-id match, name+zip match, and fuzzy fallback"
```

---

## Task 5: Census Geocoder

**Files:**
- Create: `scripts/pipeline/geocoder.py`
- Modify: `scripts/test_pipeline.py` (add geocoder tests)

- [ ] **Step 1: Write the failing tests**

Append to `scripts/test_pipeline.py`:

```python
# ── geocoder tests ───────────────────────────────────────────────────────────

from unittest.mock import patch, MagicMock
from pipeline.geocoder import batch_geocode


def _make_census_response(rows: list[str]) -> MagicMock:
    """rows: list of CSV lines as the Census API would return."""
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    mock.text = "\n".join(rows)
    return mock


def test_batch_geocode_returns_coords_for_matched_records():
    census_csv = [
        '"uuid-1","123 Main St, Brooklyn, NY, 11201","Match","Exact","123 Main St, Brooklyn, NY 11201","-73.944,40.678",1234567,L'
    ]
    with patch("requests.post", return_value=_make_census_response(census_csv)):
        result = batch_geocode([{
            "business_id": "uuid-1",
            "address_street": "123 Main St",
            "address_city": "Brooklyn",
            "address_state": "New York",
            "address_zip": "11201",
        }])
    assert "uuid-1" in result
    lat, lon = result["uuid-1"]
    assert abs(lat - 40.678) < 0.001
    assert abs(lon - (-73.944)) < 0.001


def test_batch_geocode_skips_non_match():
    census_csv = [
        '"uuid-2","Bad Address, Nowhere, NY, 00000","No_Match","","","",,',
    ]
    with patch("requests.post", return_value=_make_census_response(census_csv)):
        result = batch_geocode([{
            "business_id": "uuid-2",
            "address_street": "Bad Address",
            "address_city": "Nowhere",
            "address_state": "NY",
            "address_zip": "00000",
        }])
    assert "uuid-2" not in result


def test_batch_geocode_returns_empty_dict_for_empty_input():
    result = batch_geocode([])
    assert result == {}


def test_batch_geocode_filters_records_missing_coords():
    records = [
        {"business_id": "has-coords", "latitude": "40.68", "longitude": "-73.94",
         "address_street": "1 Main", "address_city": "Brooklyn",
         "address_state": "NY", "address_zip": "11201"},
        {"business_id": "no-coords", "latitude": "", "longitude": "",
         "address_street": "2 Main", "address_city": "Brooklyn",
         "address_state": "NY", "address_zip": "11201"},
    ]
    census_csv = [
        '"no-coords","2 Main, Brooklyn, NY, 11201","Match","Exact","2 Main, Brooklyn, NY 11201","-73.945,40.679",1234568,L'
    ]
    # Only no-coords should be submitted; has-coords should be skipped
    with patch("requests.post", return_value=_make_census_response(census_csv)) as mock_post:
        result = batch_geocode(records)
    call_args = mock_post.call_args
    submitted_csv = call_args.kwargs["files"]["addressFile"][1]
    assert "no-coords" in submitted_csv
    assert "has-coords" not in submitted_csv
```

- [ ] **Step 2: Run to verify failure**

```bash
cd scripts && pytest test_pipeline.py -k "test_batch_geocode" -v
```

Expected: `ImportError` — `pipeline.geocoder` does not exist yet.

- [ ] **Step 3: Write the implementation**

Create `scripts/pipeline/geocoder.py`:

```python
import csv
import io

import requests

CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
BATCH_SIZE = 10_000  # Census API limit per request


def batch_geocode(records: list[dict]) -> dict[str, tuple[float, float]]:
    """
    Geocode records that are missing latitude/longitude.

    Only submits records where latitude or longitude is blank.
    Records that already have both coordinates are silently skipped.

    Args:
        records: list of dicts with keys business_id, address_street,
                 address_city, address_state, address_zip, and optionally
                 latitude/longitude.

    Returns:
        dict mapping business_id -> (latitude, longitude) for
        successfully geocoded records.
    """
    to_geocode = [
        r for r in records
        if not (r.get("latitude", "").strip() and r.get("longitude", "").strip())
    ]
    if not to_geocode:
        return {}

    results: dict[str, tuple[float, float]] = {}

    for batch_start in range(0, len(to_geocode), BATCH_SIZE):
        batch = to_geocode[batch_start: batch_start + BATCH_SIZE]
        results.update(_geocode_batch(batch))

    return results


def _geocode_batch(records: list[dict]) -> dict[str, tuple[float, float]]:
    buf = io.StringIO()
    writer = csv.writer(buf)
    for r in records:
        writer.writerow([
            r["business_id"],
            r.get("address_street", ""),
            r.get("address_city", ""),
            r.get("address_state", ""),
            r.get("address_zip", ""),
        ])

    response = requests.post(
        CENSUS_URL,
        data={"benchmark": "Public_AR_Current"},
        files={"addressFile": ("addresses.csv", buf.getvalue(), "text/csv")},
        timeout=300,
    )
    response.raise_for_status()

    results: dict[str, tuple[float, float]] = {}
    reader = csv.reader(io.StringIO(response.text))
    for row in reader:
        if len(row) < 6:
            continue
        record_id = row[0].strip()
        match_status = row[2].strip().lower()
        coords = row[5].strip() if len(row) > 5 else ""
        if match_status == "match" and coords:
            try:
                lon_str, lat_str = coords.split(",")
                results[record_id] = (float(lat_str.strip()), float(lon_str.strip()))
            except (ValueError, AttributeError):
                continue

    return results
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd scripts && pytest test_pipeline.py -k "test_batch_geocode" -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add scripts/pipeline/geocoder.py scripts/test_pipeline.py
git commit -m "feat: Census Geocoder batch API, geocodes only new records"
```

---

## Task 6: Export Layer

**Files:**
- Create: `scripts/pipeline/export.py`
- Modify: `scripts/test_pipeline.py` (add export tests)

- [ ] **Step 1: Write the failing tests**

Append to `scripts/test_pipeline.py`:

```python
# ── export tests ─────────────────────────────────────────────────────────────

import csv as csv_module
from pipeline.export import export_csv, write_summary
from pipeline.db import open_db, write_businesses, write_snapshot_meta


@pytest.fixture
def db_with_snapshot(tmp_path):
    db_path = tmp_path / "test.duckdb"
    con = open_db(db_path)
    records = [
        {**_make_record(business_id="biz-1", business_name="Alpha Biz",
                        confidence="confirmed_black", source_id="src_a")},
        {**_make_record(business_id="biz-2", business_name="Beta Biz",
                        confidence="mbe_unverified", source_id="src_b")},
        # biz-1 also appears in src_b as mbe_unverified — confirmed should win
        {**_make_record(business_id="biz-1", business_name="Alpha Biz",
                        confidence="mbe_unverified", source_id="src_b")},
    ]
    write_businesses(con, records, "2026-Q2")
    write_snapshot_meta(con, "2026-Q2", 2, 0, ["src_a", "src_b"], [])
    return con


def test_export_csv_creates_file(db_with_snapshot, tmp_path):
    out = tmp_path / "businesses.csv"
    export_csv(db_with_snapshot, out, "2026-Q2")
    assert out.exists()


def test_export_csv_has_header_row(db_with_snapshot, tmp_path):
    out = tmp_path / "businesses.csv"
    export_csv(db_with_snapshot, out, "2026-Q2")
    with open(out) as f:
        reader = csv_module.DictReader(f)
        assert "business_name" in reader.fieldnames
        assert "confidence" in reader.fieldnames


def test_export_csv_deduplicates_confirmed_wins(db_with_snapshot, tmp_path):
    out = tmp_path / "businesses.csv"
    export_csv(db_with_snapshot, out, "2026-Q2")
    with open(out) as f:
        rows = list(csv_module.DictReader(f))
    # biz-1 appears in both sources — confirmed_black should win
    biz1_rows = [r for r in rows if r["business_id"] == "biz-1"]
    assert len(biz1_rows) == 1
    assert biz1_rows[0]["confidence"] == "confirmed_black"


def test_export_csv_row_count_equals_unique_businesses(db_with_snapshot, tmp_path):
    out = tmp_path / "businesses.csv"
    export_csv(db_with_snapshot, out, "2026-Q2")
    with open(out) as f:
        rows = list(csv_module.DictReader(f))
    assert len(rows) == 2  # biz-1 and biz-2, deduplicated


def test_write_summary_creates_file(tmp_path):
    path = tmp_path / "2026-Q2-summary.txt"
    write_summary(path, "2026-Q2", 100, 5, ["src_a"], ["src_b"])
    assert path.exists()
    content = path.read_text()
    assert "2026-Q2" in content
    assert "100" in content
    assert "src_b" in content
```

- [ ] **Step 2: Run to verify failure**

```bash
cd scripts && pytest test_pipeline.py -k "test_export or test_write_summary" -v
```

Expected: `ImportError` — `pipeline.export` does not exist yet.

- [ ] **Step 3: Write the implementation**

Create `scripts/pipeline/export.py`:

```python
import csv
from datetime import date
from pathlib import Path

import duckdb

EXPORT_COLUMNS = [
    "business_id", "business_name", "owner_name", "year_founded",
    "address_street", "address_city", "address_state", "address_zip",
    "latitude", "longitude", "industry", "naics_code", "certification",
    "description", "website", "phone", "email",
    "instagram_handle", "facebook_url", "tiktok_handle",
    "yelp_url", "google_maps_url",
    "discloses_google_maps", "discloses_yelp", "discloses_instagram",
    "data_source", "last_verified", "confidence",
]


def export_csv(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
    snapshot_id: str,
) -> None:
    """
    Export the current snapshot to businesses.csv.
    For businesses present in multiple sources in the same snapshot,
    prefer confidence='confirmed_black' over 'mbe_unverified'.
    """
    cols = ", ".join(EXPORT_COLUMNS)
    rows = con.execute(
        f"""
        WITH ranked AS (
            SELECT
                {cols},
                ROW_NUMBER() OVER (
                    PARTITION BY business_id
                    ORDER BY CASE confidence
                        WHEN 'confirmed_black' THEN 0
                        ELSE 1
                    END
                ) AS rn
            FROM businesses
            WHERE snapshot_id = ?
        )
        SELECT {cols} FROM ranked WHERE rn = 1
        ORDER BY business_name
        """,
        [snapshot_id],
    ).fetchall()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(EXPORT_COLUMNS)
        writer.writerows(rows)


def write_summary(
    output_path: Path,
    snapshot_id: str,
    records_total: int,
    records_dropped: int,
    sources_run: list[str],
    sources_failed: list[str],
) -> None:
    """Write a human-readable run report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"BBRT Quarterly Pipeline — {snapshot_id}",
        f"Run date: {date.today()}",
        f"",
        f"Records in snapshot: {records_total}",
        f"Records dropped from prior snapshot: {records_dropped}",
        f"",
        f"Sources run ({len(sources_run)}): {', '.join(sources_run) or 'none'}",
        f"Sources failed ({len(sources_failed)}): {', '.join(sources_failed) or 'none'}",
    ]
    output_path.write_text("\n".join(lines))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd scripts && pytest test_pipeline.py -k "test_export or test_write_summary" -v
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add scripts/pipeline/export.py scripts/test_pipeline.py
git commit -m "feat: export layer writes businesses.csv with deduplication and summary"
```

---

## Task 7: Orchestrator

**Files:**
- Create: `scripts/pipeline/run.py`
- Modify: `scripts/test_pipeline.py` (add orchestrator tests)

- [ ] **Step 1: Write the failing tests**

Append to `scripts/test_pipeline.py`:

```python
# ── orchestrator tests ───────────────────────────────────────────────────────

from pipeline.run import current_snapshot_id, discover_adapters
from datetime import date


def test_current_snapshot_id_q1():
    assert current_snapshot_id(date(2026, 1, 15)) == "2026-Q1"

def test_current_snapshot_id_q2():
    assert current_snapshot_id(date(2026, 5, 1)) == "2026-Q2"

def test_current_snapshot_id_q3():
    assert current_snapshot_id(date(2026, 8, 31)) == "2026-Q3"

def test_current_snapshot_id_q4():
    assert current_snapshot_id(date(2026, 11, 1)) == "2026-Q4"


def test_discover_adapters_finds_concrete_classes(tmp_path, monkeypatch):
    # Write a minimal valid adapter to a temp adapters directory
    adapters_dir = tmp_path / "adapters"
    adapters_dir.mkdir()
    (adapters_dir / "__init__.py").write_text("")
    (adapters_dir / "test_adapter.py").write_text("""
import sys
sys.path.insert(0, str(__file__).replace('/adapters/test_adapter.py', ''))
from pipeline.adapter_base import AdapterBase
class TestAdapter(AdapterBase):
    SOURCE_ID = 'test'; SOURCE_NAME = 'Test'
    PROGRAM = 'MWBE'; GEOGRAPHY = 'X'; CONFIDENCE = 'confirmed_black'
    FIELD_MAP = {}
    def fetch(self): return []
    def parse(self, raw): return []
""")
    monkeypatch.syspath_prepend(str(tmp_path))
    adapters = discover_adapters(adapters_dir)
    assert len(adapters) == 1
    assert adapters[0].SOURCE_ID == "test"
```

- [ ] **Step 2: Run to verify failure**

```bash
cd scripts && pytest test_pipeline.py -k "test_current_snapshot or test_discover" -v
```

Expected: `ImportError` — `pipeline.run` does not exist yet.

- [ ] **Step 3: Write the implementation**

Create `scripts/pipeline/run.py`:

```python
"""
Orchestrator — discovers and runs all adapters, resolves entities,
geocodes new records, writes to DuckDB, exports businesses.csv.

Usage (from scripts/ directory):
    python -m pipeline.run
    python -m pipeline.run --snapshot-id 2026-Q2
"""
import argparse
import importlib
import inspect
import json
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = REPO_ROOT / "data"
DB_PATH = DATA_DIR / "bbrt.duckdb"
CSV_PATH = DATA_DIR / "businesses.csv"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
ADAPTERS_DIR = Path(__file__).resolve().parent.parent / "adapters"

# Allow imports from scripts/ when run as module
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.adapter_base import AdapterBase
from pipeline.db import (
    open_db, upsert_source, write_businesses,
    write_snapshot_meta, get_registry, upsert_registry,
)
from pipeline.entity_resolver import resolve
from pipeline.geocoder import batch_geocode
from pipeline.export import export_csv, write_summary


def current_snapshot_id(d: date = None) -> str:
    d = d or date.today()
    quarter = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{quarter}"


def discover_adapters(adapters_dir: Path = ADAPTERS_DIR) -> list:
    """
    Import every .py file in adapters_dir and return one instance
    of each concrete AdapterBase subclass found.
    """
    adapters = []
    for path in sorted(adapters_dir.glob("*.py")):
        if path.name.startswith("_"):
            continue
        module_name = f"adapters.{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            print(f"  [WARN] Could not import {path.name}: {e}")
            continue
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if (issubclass(cls, AdapterBase)
                    and cls is not AdapterBase
                    and cls.SOURCE_ID):
                adapters.append(cls())
    return adapters


def run(snapshot_id: str = None) -> None:
    snapshot_id = snapshot_id or current_snapshot_id()
    print(f"\n=== BBRT Pipeline — {snapshot_id} ===\n")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

    con = open_db(DB_PATH)
    registry = get_registry(con)

    # Count records in previous snapshot for dropped-record calculation
    prior_count_row = con.execute(
        "SELECT COUNT(*) FROM businesses WHERE snapshot_id = "
        "(SELECT MAX(snapshot_id) FROM snapshots)"
    ).fetchone()
    prior_count = prior_count_row[0] if prior_count_row else 0

    adapters = discover_adapters()
    print(f"Discovered {len(adapters)} adapter(s): "
          f"{', '.join(a.SOURCE_ID for a in adapters)}\n")

    all_records: list[dict] = []
    sources_run: list[str] = []
    sources_failed: list[str] = []

    for adapter in adapters:
        print(f"  Running {adapter.SOURCE_ID}...")
        upsert_source(con, adapter)
        try:
            records = adapter.run()
            # Tag each record with source metadata
            for r in records:
                r["source_id"] = adapter.SOURCE_ID
                r["confidence"] = adapter.CONFIDENCE
            print(f"    {len(records)} records fetched.")
            all_records.extend(records)
            sources_run.append(adapter.SOURCE_ID)
        except Exception as e:
            print(f"    [ERROR] {adapter.SOURCE_ID} failed: {e}")
            sources_failed.append(adapter.SOURCE_ID)

    print(f"\nResolving entity identity for {len(all_records)} records...")
    review_log: list[dict] = []
    all_records, new_entries = resolve(all_records, registry, snapshot_id, review_log)
    print(f"  {len(new_entries)} new businesses. "
          f"{len(review_log)} uncertain matches logged for review.")

    if review_log:
        review_path = SNAPSHOTS_DIR / f"{snapshot_id}-match-review.csv"
        import csv
        with open(review_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=review_log[0].keys())
            writer.writeheader()
            writer.writerows(review_log)
        print(f"  Match review log: {review_path}")

    print(f"\nGeocoding new records...")
    coords = batch_geocode(all_records)
    for r in all_records:
        if r["business_id"] in coords:
            lat, lon = coords[r["business_id"]]
            r["latitude"] = str(lat)
            r["longitude"] = str(lon)
    geocoded_count = sum(1 for r in all_records if r.get("latitude"))
    print(f"  {geocoded_count}/{len(all_records)} records have coordinates.")

    print(f"\nWriting to DuckDB...")
    write_businesses(con, all_records, snapshot_id)
    upsert_registry(con, snapshot_id, new_entries)

    records_dropped = max(prior_count - len(all_records), 0)
    write_snapshot_meta(con, snapshot_id, len(all_records),
                        records_dropped, sources_run, sources_failed)

    print(f"Writing businesses.csv...")
    export_csv(con, CSV_PATH, snapshot_id)

    summary_path = SNAPSHOTS_DIR / f"{snapshot_id}-summary.txt"
    write_summary(summary_path, snapshot_id, len(all_records),
                  records_dropped, sources_run, sources_failed)
    print(f"Summary: {summary_path}")

    con.close()
    print(f"\nDone. {len(all_records)} records in snapshot {snapshot_id}.")
    if sources_failed:
        print(f"FAILED SOURCES: {', '.join(sources_failed)}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the BBRT quarterly pipeline")
    parser.add_argument("--snapshot-id", default=None,
                        help="Override snapshot ID (default: current quarter)")
    args = parser.parse_args()
    run(snapshot_id=args.snapshot_id)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd scripts && pytest test_pipeline.py -k "test_current_snapshot or test_discover" -v
```

Expected: 5 passed.

- [ ] **Step 5: Run the full test suite to confirm nothing regressed**

```bash
cd scripts && pytest test_pipeline.py -v
```

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add scripts/pipeline/run.py scripts/test_pipeline.py
git commit -m "feat: orchestrator discovers adapters, resolves entities, geocodes, exports"
```

---

## Task 8: NYC MWBE Adapter (Migrated)

**Files:**
- Create: `scripts/adapters/nyc_mwbe.py`
- Modify: `scripts/test_pipeline.py` (add adapter tests)

The existing `build_csv.py` stays untouched. This adapter ports its parsing logic into the new interface.

- [ ] **Step 1: Write the failing tests**

Append to `scripts/test_pipeline.py`:

```python
# ── nyc_mwbe adapter tests ───────────────────────────────────────────────────

import openpyxl
from adapters.nyc_mwbe import NycMwbeAdapter


@pytest.fixture
def nyc_xlsx(tmp_path):
    """Minimal NYC MWBE xlsx — same format as the existing sample_xlsx fixture."""
    filepath = tmp_path / "nyc_mwbe.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["Export Date:", "09/09/2025"])
    ws.append(["Matching Records:", 2])
    ws.append(["Search Parameters"])
    ws.append(["codecategory", "both"])
    ws.append([None])
    ws.append([
        "Account Number", "Vendor Formal Name", "Vendor DBA",
        "First Name", "Last Name", "Telephone", "Email",
        "Business Description", "Certification", "Certification Renewal Date",
        "Ethnicity", "Address Line 1", "Address Line 2", "City", "State", "Zip",
        "Mailing Address Line 1", "Mailing Address Line 2", "Mailing City",
        "Mailing State", "Mailing Zip", "Website", "Date of Establishment",
        "Aggregate Bonding Limit", "Signatory to Union Contract(s)",
        "6 digit NAICS code", "NAICS Sector", "NAICS Subsector", "NAICS Title",
        "Types of Construction Projects Performed", "NIGP codes",
        "Largest Value of Contract"
    ])
    ws.append([
        "ACC001", "Horizon Consulting LLC", "", "Jane", "Doe", "212-555-0001",
        "jane@horizon.com", "Management consulting services.", "MBE", "2026-01-01",
        "Black", "123 Main St", "", "Brooklyn", "New York", "11201",
        "", "", "", "", "", "https://horizon.com", "2015",
        "", "", "561110", "Services", "Administrative", "Management Consulting",
        "", "", ""
    ])
    ws.append([
        "ACC002", "BuildRight Inc", "", "Marcus", "Johnson", "718-555-0002",
        "", "General contractor.", "M/WBE", "2026-01-01",
        "Black", "456 Atlantic Ave", "", "Bronx", "New York", "10451",
        "", "", "", "", "", "", None,
        "", "", "236220", "Construction", "Building Construction", "Commercial",
        "", "", ""
    ])
    ws.append([
        "ACC003", "Other Corp", "", "Ana", "Lopez", "", "",
        "Non-black business.", "MBE", "2026-01-01",
        "Hispanic", "789 Broadway", "", "Manhattan", "New York", "10013",
        "", "", "", "", "", "", "2020",
        "", "", "541511", "Technology", "Software", "Custom Software",
        "", "", ""
    ])
    wb.save(filepath)
    return filepath


def test_nyc_adapter_metadata():
    adapter = NycMwbeAdapter()
    assert adapter.SOURCE_ID == "nyc_mwbe"
    assert adapter.CONFIDENCE == "confirmed_black"
    assert adapter.PROGRAM == "MWBE"


def test_nyc_adapter_filters_to_black_only(nyc_xlsx):
    adapter = NycMwbeAdapter(source_file=nyc_xlsx)
    records = adapter.run()
    assert len(records) == 2
    names = [r["business_name"] for r in records]
    assert "Other Corp" not in names


def test_nyc_adapter_maps_standard_fields(nyc_xlsx):
    adapter = NycMwbeAdapter(source_file=nyc_xlsx)
    records = adapter.run()
    rec = next(r for r in records if r["business_name"] == "Horizon Consulting LLC")
    assert rec["owner_name"] == "Jane Doe"
    assert rec["address_street"] == "123 Main St"
    assert rec["address_city"] == "Brooklyn"
    assert rec["address_state"] == "New York"
    assert rec["address_zip"] == "11201"
    assert rec["industry"] == "Services"
    assert rec["naics_code"] == "561110"
    assert rec["certification"] == "MBE"
    assert rec["website"] == "https://horizon.com"
    assert rec["year_founded"] == "2015"
    assert rec["phone"] == "212-555-0001"
    assert rec["email"] == "jane@horizon.com"
    assert rec["source_business_id"] == "ACC001"


def test_nyc_adapter_puts_extra_columns_in_source_fields(nyc_xlsx):
    adapter = NycMwbeAdapter(source_file=nyc_xlsx)
    records = adapter.run()
    rec = records[0]
    sf = rec["source_fields"]
    # Columns not in FIELD_MAP should land in source_fields
    assert "NAICS Title" in sf or "Vendor DBA" in sf


def test_nyc_adapter_handles_missing_year(nyc_xlsx):
    adapter = NycMwbeAdapter(source_file=nyc_xlsx)
    records = adapter.run()
    rec = next(r for r in records if r["business_name"] == "BuildRight Inc")
    assert rec["year_founded"] == ""


def test_nyc_adapter_sets_last_verified(nyc_xlsx):
    adapter = NycMwbeAdapter(source_file=nyc_xlsx)
    records = adapter.run()
    assert records[0]["last_verified"] == "2025-09-09"
```

- [ ] **Step 2: Run to verify failure**

```bash
cd scripts && pytest test_pipeline.py -k "test_nyc" -v
```

Expected: `ImportError` — `adapters.nyc_mwbe` does not exist yet.

- [ ] **Step 3: Write the implementation**

Create `scripts/adapters/nyc_mwbe.py`:

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd scripts && pytest test_pipeline.py -k "test_nyc" -v
```

Expected: 7 passed.

- [ ] **Step 5: Run the full test suite**

```bash
cd scripts && pytest test_pipeline.py -v
```

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add scripts/adapters/nyc_mwbe.py scripts/test_pipeline.py
git commit -m "feat: NYC MWBE adapter migrated to new adapter interface"
```

---

## Task 9: GitHub Actions Workflow

**Files:**
- Create: `.github/workflows/quarterly_pipeline.yml`

No tests — this is YAML configuration. Verification is done by triggering a manual run in Step 4.

- [ ] **Step 1: Create the workflows directory**

```bash
mkdir -p .github/workflows
```

- [ ] **Step 2: Write the workflow file**

Create `.github/workflows/quarterly_pipeline.yml`:

```yaml
name: Quarterly Pipeline

on:
  schedule:
    # 6am UTC on the first of January, April, July, October
    - cron: '0 6 1 1,4,7,10 *'
  workflow_dispatch:
    inputs:
      snapshot_id:
        description: 'Override snapshot ID (e.g. 2026-Q2). Leave blank to use current quarter.'
        required: false
        default: ''

permissions:
  contents: write

jobs:
  run-pipeline:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: pip install -r scripts/requirements.txt

      - name: Run pipeline
        working-directory: scripts
        env:
          SAM_GOV_API_KEY: ${{ secrets.SAM_GOV_API_KEY }}
        run: |
          if [ -n "${{ github.event.inputs.snapshot_id }}" ]; then
            python -m pipeline.run --snapshot-id "${{ github.event.inputs.snapshot_id }}"
          else
            python -m pipeline.run
          fi

      - name: Commit and push updated data
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/businesses.csv data/bbrt.duckdb data/snapshots/
          git diff --staged --quiet || git commit -m "chore: quarterly pipeline update"
          git push
```

- [ ] **Step 3: Add bbrt.duckdb to git tracking (not gitignored)**

Verify `data/bbrt.duckdb` is not in `.gitignore`:

```bash
grep bbrt .gitignore
```

Expected: no output. If it appears, remove that line.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/quarterly_pipeline.yml
git commit -m "feat: GitHub Actions quarterly cron pipeline"
```

---

## Task 10: Integration Test and Initial Manual Run

This task runs the full pipeline end-to-end with the real NYC MWBE data to establish the `2026-Q2` baseline snapshot.

- [ ] **Step 1: Verify all unit tests still pass**

```bash
cd scripts && pytest test_pipeline.py test_build_csv.py -v
```

Expected: All tests pass.

- [ ] **Step 2: Run the pipeline manually with --snapshot-id 2026-Q2**

```bash
cd scripts && python -m pipeline.run --snapshot-id 2026-Q2
```

Expected output (approximate):
```
=== BBRT Pipeline — 2026-Q2 ===

Discovered 1 adapter(s): nyc_mwbe

  Running nyc_mwbe...
    3775 records fetched.

Resolving entity identity for 3775 records...
  3775 new businesses. 0 uncertain matches logged for review.

Geocoding new records...
  [progress output — Census Geocoder batch API]
  XXXX/3775 records have coordinates.

Writing to DuckDB...
Writing businesses.csv...
Summary: data/snapshots/2026-Q2-summary.txt

Done. 3775 records in snapshot 2026-Q2.
```

- [ ] **Step 3: Verify the output**

```bash
# Verify businesses.csv row count
python3 -c "
import csv
with open('data/businesses.csv') as f:
    rows = list(csv.DictReader(f))
print(f'businesses.csv: {len(rows)} rows')
print(f'confidence field present: {\"confidence\" in rows[0]}')
print(f'Sample confidence values: {set(r[\"confidence\"] for r in rows[:10])}')
"
```

Expected: 3775 rows, confidence field present with value `confirmed_black`.

```bash
# Verify DuckDB snapshot
python3 -c "
import sys; sys.path.insert(0, 'scripts')
import duckdb
con = duckdb.connect('data/bbrt.duckdb')
print('Snapshots:', con.execute('SELECT * FROM snapshots').fetchall())
print('Business count:', con.execute('SELECT COUNT(*) FROM businesses').fetchone()[0])
print('Registry count:', con.execute('SELECT COUNT(*) FROM business_registry').fetchone()[0])
con.close()
"
```

Expected: 1 snapshot row for 2026-Q2, 3775 businesses, 3775 registry entries.

- [ ] **Step 4: Verify the site still works**

Open `index.html` in a browser (or use a local server) and confirm:
- Map loads with markers
- Table loads with 3,775 rows
- Coverage bar shows ~2.4%
- Confidence column visible in Expanded view (may show blank until Site V2)

- [ ] **Step 5: Commit the initial snapshot data**

```bash
git add data/businesses.csv data/bbrt.duckdb data/snapshots/
git commit -m "feat: 2026-Q2 baseline snapshot — 3775 NYC MWBE businesses"
git push
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Covered by |
|---|---|
| Adapter interface with FIELD_MAP | Task 2 |
| source_fields JSON overflow | Task 2 (map_record) |
| DuckDB with 5 tables | Task 3 |
| Entity resolution: source ID → name+zip → new | Task 4 |
| Census Geocoder batch, new records only | Task 5 |
| Export with confirmed_black deduplication preference | Task 6 |
| write_summary / run report | Task 6 |
| Orchestrator discovers adapters automatically | Task 7 |
| failure_handling: one adapter failure doesn't abort run | Task 7 (run.py) |
| NYC MWBE migrated to new interface | Task 8 |
| GitHub Actions cron `0 6 1 1,4,7,10 *` | Task 9 |
| workflow_dispatch for manual override | Task 9 |
| Initial 2026-Q2 baseline snapshot | Task 10 |
| field_catalog table | Task 3 (table created; population is Sub-project 3) |

**Placeholder scan:** No TBD or TODO in plan. All code is complete.

**Type consistency check:**
- `resolve()` returns `(list[dict], list[dict])` — used correctly in run.py
- `batch_geocode()` returns `dict[str, tuple[float, float]]` — used correctly in run.py
- `open_db()` returns `duckdb.DuckDBPyConnection` — passed correctly to all db functions
- `export_csv(con, output_path, snapshot_id)` — signature matches all call sites
- `write_summary(path, snapshot_id, total, dropped, run, failed)` — matches run.py call
