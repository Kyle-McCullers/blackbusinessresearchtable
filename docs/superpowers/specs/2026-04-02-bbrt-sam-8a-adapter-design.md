# BBRT Sub-project 2: SAM.gov 8(a) Adapter Design

## Goal

Add a national federal data source to the BBRT pipeline by building an adapter for the SAM.gov 8(a) Business Development Program. This adds ~5,000–9,000 nationally certified small disadvantaged businesses to the database, tagged at the `mbe_unverified` confidence tier.

## Background

The SBA 8(a) program certifies small businesses owned by "socially and economically disadvantaged" individuals. Eligible groups include Black Americans, Hispanic Americans, Native Americans, Asian Pacific Americans, and others. SAM.gov does not expose race/ethnicity as a queryable field — so 8(a) status cannot be used to confirm Black ownership. Records are tagged `mbe_unverified` to reflect this.

---

## Architecture

One new file: `scripts/adapters/sam_8a.py`. No changes to the pipeline infrastructure — the orchestrator auto-discovers it by scanning the `adapters/` directory.

**Existing infrastructure used:**
- `AdapterBase` — inherited; `map_record()` handles FIELD_MAP and `source_fields` overflow
- `BBRT_FIELDS` schema — unchanged
- `entity_resolver` — UEI as `source_business_id` gives strong match signal across quarterly runs
- GitHub Actions `SAM_GOV_API_KEY` secret — already wired in `quarterly_pipeline.yml`

---

## Data Access

**API:** SAM.gov Entity Management API v3
`https://api.sam.gov/entity-information/v3/entities`

**Authentication:** `api_key` query parameter (or `x-api-key` header). Read from `SAM_GOV_API_KEY` environment variable at adapter instantiation. If the variable is absent or empty, the adapter raises a `ValueError` immediately rather than failing mid-run.

**Filter:** `sbaBusinessTypeCode=A6` — the SBA program code for 8(a) Business Development. Only active entities are returned by default.

**Pagination:** The API returns up to 100 records per page. The response includes `totalRecords`. The adapter loops through all pages until all records are collected.

**Response format:** JSON. Each entity object contains nested structures for `entityRegistration`, `coreData`, `assertions`, and `repsAndCerts`.

---

## Field Mapping

| SAM.gov field path | BBRT field | Notes |
|---|---|---|
| `entityRegistration.legalBusinessName` | `business_name` | |
| `coreData.physicalAddress.addressLine1` | `address_street` | |
| `coreData.physicalAddress.city` | `address_city` | |
| `coreData.physicalAddress.stateOrProvinceCode` | `address_state` | 2-letter code |
| `coreData.physicalAddress.zipCode` | `address_zip` | |
| `coreData.entityInformation.entityURL` | `website` | |
| `assertions.goodsAndServices.primaryNaics` | `naics_code` | |
| `entityRegistration.ueiSAM` | `source_business_id` | Stable federal UEI |

**Unmapped fields** (stored in `source_fields`): CAGE code, entity status, registration expiration, SBA certification expiration date, HUBZone/WOSB/SDVOSB flags, points of contact, all NAICS codes beyond primary.

**Fixed values:**
- `SOURCE_ID = "sam_8a"`
- `SOURCE_NAME = "SAM.gov 8(a) Certified Businesses"`
- `PROGRAM = "8(a)"`
- `GEOGRAPHY = "National"`
- `CONFIDENCE = "mbe_unverified"`
- `last_verified` = set to the date `fetch()` runs (today's date as ISO string)
- `certification = "8(a)"`

---

## Implementation

### `fetch() -> list[dict]`

1. Read `SAM_GOV_API_KEY` from environment; raise `ValueError` if missing.
2. Build request params: `sbaBusinessTypeCode=A6`, `includeSections=entityRegistration,coreData,assertions`, `pageSize=100`, `page=0`.
3. Loop: POST/GET each page, collect entity dicts, increment page until `page * 100 >= totalRecords`.
4. Return flat list of raw entity dicts.

### `parse(raw: list[dict]) -> list[dict]`

For each entity dict:
1. Flatten nested SAM.gov structure into a flat source_row dict (dotted keys or extracted values).
2. Call `map_record(source_row)` to apply FIELD_MAP and collect unmapped fields into `source_fields`.
3. Set `source_business_id`, `certification`, `last_verified`.
4. Return list of standardized record dicts.

---

## Error Handling

- **Missing API key:** `ValueError` at adapter instantiation — caught by orchestrator, source marked as failed.
- **HTTP errors (4xx/5xx):** `raise_for_status()` — exception propagates to orchestrator, source marked as failed, other adapters continue.
- **Empty response:** Returns empty list without error.
- **Malformed entity:** Skip individual entity, emit `warnings.warn` with UEI if available.

---

## Testing

All tests use `unittest.mock.patch` — no live API calls.

| Test | What it verifies |
|---|---|
| `test_sam_adapter_metadata` | SOURCE_ID, CONFIDENCE, PROGRAM, GEOGRAPHY |
| `test_sam_adapter_raises_without_api_key` | `ValueError` when env var absent |
| `test_sam_adapter_paginates_all_pages` | Loops until `totalRecords` exhausted |
| `test_sam_adapter_maps_standard_fields` | FIELD_MAP applied correctly |
| `test_sam_adapter_sets_uei_as_source_business_id` | UEI → `source_business_id` |
| `test_sam_adapter_puts_extra_fields_in_source_fields` | Unmapped fields land in `source_fields` |
| `test_sam_adapter_handles_missing_optional_field` | Missing `entityURL` → empty string, no crash |

---

## Files

| File | Action |
|---|---|
| `scripts/adapters/sam_8a.py` | Create |
| `scripts/test_pipeline.py` | Modify — append SAM.gov adapter tests |

No other files change. `quarterly_pipeline.yml` already references `SAM_GOV_API_KEY`.

---

## Deployment Note

Before the next automated run (July 1, 2026), add `SAM_GOV_API_KEY` to GitHub Actions repository secrets at:
`Settings → Secrets and variables → Actions → New repository secret`
