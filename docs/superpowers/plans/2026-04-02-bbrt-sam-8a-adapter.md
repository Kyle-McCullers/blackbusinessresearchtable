# BBRT SAM.gov 8(a) Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a SAM.gov 8(a) adapter to the BBRT pipeline that fetches all nationally certified 8(a) small businesses and stores them as `mbe_unverified` records.

**Architecture:** One new file (`scripts/adapters/sam_8a.py`) following the existing `AdapterBase` pattern. The orchestrator auto-discovers it. All tests are mocked — no live API calls in the test suite.

**Tech Stack:** Python 3.12, requests (already in requirements.txt), unittest.mock, pytest.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `scripts/adapters/sam_8a.py` | Create | SAM.gov 8(a) adapter — fetch (paginated API), parse (flatten + map fields) |
| `scripts/test_pipeline.py` | Modify | Append SAM.gov adapter tests |

No other files change. The orchestrator and GitHub Actions workflow already reference `SAM_GOV_API_KEY`.

---

## Background: SAM.gov Entity API

The adapter queries:
```
GET https://api.sam.gov/entity-information/v3/entities
```

Key parameters:
- `api_key` — your SAM.gov public API key (from `SAM_GOV_API_KEY` env var)
- `sbaBusinessTypeDesc=8(a) Business Development` — filters to 8(a) certified firms
- `includeSections=entityRegistration,coreData,assertions` — returns only the sections we need
- `size=10` — API maximum per page
- `page=0` — 0-indexed page number

Response shape:
```json
{
  "totalRecords": 7432,
  "entityData": [
    {
      "entityRegistration": {
        "ueiSAM": "ABC123DEF456",
        "legalBusinessName": "Horizon Consulting LLC"
      },
      "coreData": {
        "physicalAddress": {
          "addressLine1": "123 Main St",
          "city": "Atlanta",
          "stateOrProvinceCode": "GA",
          "zipCode": "30301"
        },
        "entityInformation": {
          "entityURL": "https://horizon.com"
        }
      },
      "assertions": {
        "goodsAndServices": {
          "primaryNaics": "541611"
        }
      }
    }
  ]
}
```

---

## Task 1: SAM.gov 8(a) Adapter

**Files:**
- Create: `scripts/adapters/sam_8a.py`
- Modify: `scripts/test_pipeline.py`

- [ ] **Step 1: Write the failing tests**

Append to `scripts/test_pipeline.py`:

```python
# ── sam_8a adapter tests ─────────────────────────────────────────────────────

import os
from unittest.mock import patch, MagicMock
from adapters.sam_8a import SamEightAAdapter


def _make_sam_response(entities: list[dict], total: int) -> MagicMock:
    mock = MagicMock()
    mock.raise_for_status.return_value = None
    mock.json.return_value = {"totalRecords": total, "entityData": entities}
    return mock


def _make_entity(uei="UEI001", name="Acme LLC", street="123 Main St",
                 city="Atlanta", state="GA", zipcode="30301",
                 url="https://acme.com", naics="541611"):
    return {
        "entityRegistration": {"ueiSAM": uei, "legalBusinessName": name},
        "coreData": {
            "physicalAddress": {
                "addressLine1": street,
                "city": city,
                "stateOrProvinceCode": state,
                "zipCode": zipcode,
            },
            "entityInformation": {"entityURL": url},
        },
        "assertions": {"goodsAndServices": {"primaryNaics": naics}},
    }


def test_sam_adapter_metadata():
    adapter = SamEightAAdapter(api_key="test-key")
    assert adapter.SOURCE_ID == "sam_8a"
    assert adapter.CONFIDENCE == "mbe_unverified"
    assert adapter.PROGRAM == "8(a)"
    assert adapter.GEOGRAPHY == "National"


def test_sam_adapter_raises_without_api_key(monkeypatch):
    monkeypatch.delenv("SAM_GOV_API_KEY", raising=False)
    with pytest.raises(ValueError, match="SAM_GOV_API_KEY"):
        SamEightAAdapter()


def test_sam_adapter_uses_env_api_key(monkeypatch):
    monkeypatch.setenv("SAM_GOV_API_KEY", "env-key-123")
    adapter = SamEightAAdapter()
    assert adapter._api_key == "env-key-123"


def test_sam_adapter_paginates_all_pages():
    entity = _make_entity()
    # totalRecords=15 → 2 pages (10 + 5)
    responses = [
        _make_sam_response([entity] * 10, total=15),
        _make_sam_response([entity] * 5, total=15),
    ]
    with patch("requests.get", side_effect=responses):
        raw = SamEightAAdapter(api_key="k").fetch()
    assert len(raw) == 15


def test_sam_adapter_maps_standard_fields():
    entity = _make_entity(
        uei="UEI999", name="Horizon Consulting LLC",
        street="123 Main St", city="Atlanta", state="GA",
        zipcode="30301", url="https://horizon.com", naics="541611",
    )
    with patch("requests.get", return_value=_make_sam_response([entity], 1)):
        records = SamEightAAdapter(api_key="k").run()
    rec = records[0]
    assert rec["business_name"] == "Horizon Consulting LLC"
    assert rec["address_street"] == "123 Main St"
    assert rec["address_city"] == "Atlanta"
    assert rec["address_state"] == "GA"
    assert rec["address_zip"] == "30301"
    assert rec["website"] == "https://horizon.com"
    assert rec["naics_code"] == "541611"
    assert rec["certification"] == "8(a)"


def test_sam_adapter_sets_uei_as_source_business_id():
    entity = _make_entity(uei="MYUEI123")
    with patch("requests.get", return_value=_make_sam_response([entity], 1)):
        records = SamEightAAdapter(api_key="k").run()
    assert records[0]["source_business_id"] == "MYUEI123"


def test_sam_adapter_puts_extra_fields_in_source_fields():
    entity = _make_entity()
    # Add a field not in FIELD_MAP to the flattened source row
    # The adapter flattens nested SAM data; anything beyond FIELD_MAP lands in source_fields
    with patch("requests.get", return_value=_make_sam_response([entity], 1)):
        records = SamEightAAdapter(api_key="k").run()
    assert "source_fields" in records[0]
    assert isinstance(records[0]["source_fields"], dict)


def test_sam_adapter_handles_missing_optional_field():
    # entityURL missing → website should be ""
    entity = _make_entity()
    del entity["coreData"]["entityInformation"]
    with patch("requests.get", return_value=_make_sam_response([entity], 1)):
        records = SamEightAAdapter(api_key="k").run()
    assert records[0]["website"] == ""


def test_sam_adapter_returns_empty_on_zero_results():
    with patch("requests.get", return_value=_make_sam_response([], 0)):
        records = SamEightAAdapter(api_key="k").run()
    assert records == []
```

- [ ] **Step 2: Run to verify failure**

```bash
cd scripts && source venv/bin/activate && pytest test_pipeline.py -k "test_sam" -v
```

Expected: `ImportError` — `adapters.sam_8a` does not exist yet.

- [ ] **Step 3: Create `scripts/adapters/sam_8a.py`**

```python
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
```

- [ ] **Step 4: Run the SAM adapter tests**

```bash
cd scripts && source venv/bin/activate && pytest test_pipeline.py -k "test_sam" -v
```

Expected: 9 passed.

- [ ] **Step 5: Run the full suite to confirm no regressions**

```bash
cd scripts && source venv/bin/activate && pytest test_pipeline.py test_build_csv.py -v 2>&1 | tail -5
```

Expected: 76 passed (67 existing + 9 new).

- [ ] **Step 6: Commit**

```bash
git add scripts/adapters/sam_8a.py scripts/test_pipeline.py
git commit -m "feat: SAM.gov 8(a) adapter — national mbe_unverified businesses"
```

---

## Task 2: Live API Verification

This task runs the adapter against the real SAM.gov API to confirm the filter parameter works and records come back with the expected shape.

**Files:** None modified. This is a manual verification step.

- [ ] **Step 1: Set the API key in your environment**

```bash
export SAM_GOV_API_KEY="your-key-here"
```

- [ ] **Step 2: Run a quick smoke test against the live API**

```bash
cd scripts && source venv/bin/activate && python3 - <<'EOF'
import os, sys
sys.path.insert(0, '.')
from adapters.sam_8a import SamEightAAdapter

adapter = SamEightAAdapter()
print("Fetching first page only (testing filter)...")

import requests, time
params = {
    "api_key": adapter._api_key,
    "sbaBusinessTypeDesc": "8(a) Business Development",
    "includeSections": "entityRegistration,coreData,assertions",
    "size": 10,
    "page": 0,
}
r = requests.get("https://api.sam.gov/entity-information/v3/entities", params=params, timeout=60)
r.raise_for_status()
data = r.json()
print(f"totalRecords: {data.get('totalRecords')}")
print(f"First entity name: {data['entityData'][0]['entityRegistration']['legalBusinessName']}")
print(f"First entity UEI: {data['entityData'][0]['entityRegistration']['ueiSAM']}")
print("Filter parameter works correctly.")
EOF
```

Expected: `totalRecords` is a number in the thousands (typically 5,000–9,000). If `totalRecords` is 0 or an error occurs, the `sbaBusinessTypeDesc` filter value may need adjustment — try `"8(a)"` or check the SAM.gov data dictionary for the correct description string.

- [ ] **Step 3: If the filter works, run the full pipeline**

```bash
cd scripts && source venv/bin/activate && python -m pipeline.run --snapshot-id 2026-Q2
```

This will re-run over the existing 2026-Q2 snapshot. Entity resolution will match existing NYC MWBE businesses by UEI if they overlap, and add new SAM.gov entries. The businesses table will have rows for both `nyc_mwbe` and `sam_8a` sources.

Expected: Console output showing nyc_mwbe + sam_8a adapters discovered, record count increases.

- [ ] **Step 4: Verify the output**

```bash
cd scripts && source venv/bin/activate && python3 - <<'EOF'
import sys; sys.path.insert(0, '.')
import duckdb
con = duckdb.connect('../data/bbrt.duckdb')
print("Records by source:")
for row in con.execute(
    "SELECT source_id, COUNT(*) FROM businesses WHERE snapshot_id='2026-Q2' GROUP BY source_id"
).fetchall():
    print(f"  {row[0]}: {row[1]}")
print("\nRecords by confidence:")
for row in con.execute(
    "SELECT confidence, COUNT(*) FROM businesses WHERE snapshot_id='2026-Q2' GROUP BY confidence"
).fetchall():
    print(f"  {row[0]}: {row[1]}")
con.close()
EOF
```

Expected: Two source_id rows (`nyc_mwbe` and `sam_8a`). Two confidence rows (`confirmed_black` from NYC, `mbe_unverified` from SAM.gov).

- [ ] **Step 5: Commit updated snapshot data**

```bash
git add data/businesses.csv data/bbrt.duckdb data/snapshots/
git commit -m "feat: 2026-Q2 snapshot updated with SAM.gov 8(a) businesses"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| `sam_8a.py` inherits `AdapterBase` | Task 1 |
| API key from `SAM_GOV_API_KEY` env var | Task 1 |
| `ValueError` if key missing | Task 1 (test + impl) |
| Paginate all results at `size=10` | Task 1 (test + impl) |
| UEI → `source_business_id` | Task 1 (test + impl) |
| FIELD_MAP covers all spec fields | Task 1 |
| Unmapped fields → `source_fields` | Task 1 (test + impl) |
| `CONFIDENCE = "mbe_unverified"` | Task 1 |
| `certification = "8(a)"` fixed value | Task 1 |
| `last_verified` = today | Task 1 |
| Missing optional fields → empty string | Task 1 (test + impl) |
| Live API filter verification | Task 2 |

**No placeholders.** All test code, implementation code, and commands are complete.

**Type consistency:** `fetch()` returns `list[dict]`, `parse()` accepts `list[dict]` and returns `list[dict]` — matches `AdapterBase` contract throughout.
