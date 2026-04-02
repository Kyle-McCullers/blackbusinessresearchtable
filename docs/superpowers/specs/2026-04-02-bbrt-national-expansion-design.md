# BBRT National Expansion — Design Spec

**Date:** 2026-04-02
**Project:** blackbusinessresearchtable.com
**Scope:** Automated multi-source national pipeline + panel database

---

## Overview

Expand the Black Business Research Table from a single NYC MWBE snapshot into a
continuously updated, longitudinal panel database covering all 50 states and major
cities. The pipeline runs automatically on a quarterly schedule, ingests data from
state/local MWBE programs and federal certification databases, standardizes and
deduplicates records, and exports a fresh dataset to the public site.

The research value: a panel database that captures business entry and exit over time,
enabling survival analysis, entry rate tracking, and DiD designs around policy changes.

---

## Sub-Project Decomposition

This design is executed as four sequential sub-projects, each with its own plan:

| Sub-project | Scope |
|---|---|
| **1 — Pipeline infrastructure** | Adapter interface, DuckDB schema, entity resolver, Census geocoder, GitHub Actions cron, export layer |
| **2 — Federal adapters** | SAM.gov 8(a) and FHWA DBE/UCP adapters |
| **3 — State & city adapters** | All 50 states + major cities, batched by tier |
| **4 — Site V2** | State/city filter dropdowns, confidence tier badges, updated map behavior |

Sub-project 1 is built first — it defines the interfaces everything else depends on.
Sub-projects 2 and 3 run in parallel once Sub-project 1 is complete.
Sub-project 4 runs after Sub-project 3 has meaningful multi-state data.

**Initial run:** After Sub-projects 1–3 are complete, run the pipeline manually to
establish the `2026-Q2` baseline snapshot. The GitHub Actions cron takes over from
July 1, 2026.

---

## Architecture

### File Structure

```
scripts/
├── pipeline/
│   ├── run.py              ← orchestrator (called by GitHub Actions)
│   ├── adapter_base.py     ← abstract base class all adapters implement
│   ├── entity_resolver.py  ← matches businesses across snapshots
│   ├── db.py               ← DuckDB read/write layer
│   └── export.py           ← exports businesses.csv for the site
├── adapters/
│   ├── nyc_mwbe.py         ← existing source, migrated to new interface
│   ├── sam_8a.py           ← SBA 8(a) via SAM.gov API
│   ├── fhwa_dbe.py         ← federal DBE/UCP
│   ├── tx_hub.py           ← Texas HUB
│   ├── wa_omwbe.py         ← Washington OMWBE
│   └── ...                 ← one file per source
├── requirements.txt
└── test_*.py
data/
├── bbrt.duckdb             ← research panel database
├── businesses.csv          ← current snapshot for the public site
└── snapshots/
    └── YYYY-QN-summary.txt ← human-readable run report per quarter
.github/workflows/
└── quarterly_pipeline.yml  ← cron schedule + pipeline steps
```

### Adapter Interface

Every source adapter is a Python class that inherits from `AdapterBase`:

```python
class AdapterBase:
    SOURCE_ID   = ""   # e.g. "nyc_mwbe", "sam_8a", "tx_hub"
    SOURCE_NAME = ""   # human-readable label
    PROGRAM     = ""   # "MWBE" | "DBE" | "8(a)" | "CBE" | "HUB"
    GEOGRAPHY   = ""   # "NYC" | "TX" | "National" | etc.
    CONFIDENCE  = ""   # "confirmed_black" | "mbe_unverified"

    # Maps source column names → normalized BBRT field names.
    # Fields not listed here are stored in source_fields JSON.
    FIELD_MAP   = {}

    def fetch(self) -> bytes | dict:
        """Download file or call API. Return raw data."""

    def parse(self, raw) -> list[dict]:
        """Map source fields to BBRT schema. Return list of records."""

    def run(self) -> list[dict]:
        """fetch() + parse(). Called by the orchestrator."""
```

Adding a new state = one new file in `adapters/`. The orchestrator discovers
adapters automatically by scanning the directory.

---

## Data Source Catalog

### Confidence Tiers

- **confirmed_black:** Source has a specific Black / African American ethnicity field
  that was used to filter records. Every record is a confirmed Black-owned business.
- **mbe_unverified:** Source only identifies businesses as "Minority Business
  Enterprise" with no further ethnicity breakdown. Included for coverage but flagged.

### Tier 1 — API or clean download, ethnicity-specific (build first)

| Source ID | Program | Geography | Access Method |
|---|---|---|---|
| `nyc_mwbe` | MWBE | New York City | Excel download (existing) |
| `sam_8a` | SBA 8(a) | National | SAM.gov REST API, free key required |
| `wa_omwbe` | MWBE | Washington State | CSV download, African American field |
| `tx_hub` | HUB | Texas | Excel download, African American field |
| `dc_cbe` | CBE | Washington DC | Socrata open data API, ethnicity field |
| `md_mbe` | MBE | Maryland | Excel download, African American field |

### Tier 2 — Download available, ethnicity field present but less clean

| Source ID | Program | Geography | Notes |
|---|---|---|---|
| `va_swam` | SWAM | Virginia | CSV download, race codes |
| `il_bep` | BEP | Illinois | Excel download |
| `fl_osd` | MWBE | Florida | CSV download |
| `ga_mwbe` | MWBE | Georgia | Portal download |
| `nc_hub` | HUB | North Carolina | CSV download |
| `oh_edge` | MBE | Ohio | Download, verify ethnicity field |

### Tier 3 — MBE only, no ethnicity disaggregation (mbe_unverified)

Includes federal DBE/UCP directories and most remaining states and city programs
that do not break out ethnicity within the minority category. The full 50-state
inventory — confirming which tier each state falls into — is the first task of
Sub-project 3.

---

## Schema & Panel Data Model

### DuckDB Tables

**`sources`** — one row per adapter

| Field | Type | Description |
|---|---|---|
| `source_id` | string | e.g. `"nyc_mwbe"` |
| `source_name` | string | Human-readable label |
| `program` | string | Certification program type |
| `geography` | string | State, city, or "National" |
| `confidence` | string | `"confirmed_black"` or `"mbe_unverified"` |
| `first_ingested` | date | Date of first pipeline run for this source |

---

**`snapshots`** — one row per pipeline run

| Field | Type | Description |
|---|---|---|
| `snapshot_id` | string | e.g. `"2026-Q2"` |
| `run_date` | date | Date the pipeline ran |
| `records_added` | int | New businesses observed this quarter |
| `records_dropped` | int | Businesses not observed this quarter vs. prior |
| `sources_run` | string[] | List of source IDs included in this run |
| `sources_failed` | string[] | List of source IDs that errored |

---

**`businesses`** — the panel (one row per business × snapshot)

All existing BBRT V1 fields, plus:

| New Field | Type | Description |
|---|---|---|
| `snapshot_id` | string | Links to `snapshots` table |
| `source_id` | string | Which adapter produced this record |
| `source_business_id` | string | Unique ID from the source system (preferred for entity resolution) |
| `confidence` | string | `"confirmed_black"` or `"mbe_unverified"` |
| `source_fields` | JSON | All source columns not mapped to standard BBRT fields — nothing is discarded |

The `source_fields` JSON column captures every variable from the source database,
even those with no standard BBRT equivalent. When a field appears across many
sources, it is promoted to a standard column and the adapters updated.

---

**`business_registry`** — one row per unique real-world business (ever observed)

| Field | Type | Description |
|---|---|---|
| `business_id` | UUID | Stable identifier across all snapshots |
| `canonical_name` | string | Normalized name used for matching |
| `canonical_zip` | string | Normalized zip used for matching |
| `first_seen` | string | Snapshot ID of first observation |
| `last_seen` | string | Snapshot ID of most recent observation |
| `is_active` | bool | True if present in the most recent snapshot for its source |

---

**`field_catalog`** — one row per source field across all adapters

| Field | Type | Description |
|---|---|---|
| `source_id` | string | Which source this field comes from |
| `source_field_name` | string | Original column name in the source |
| `normalized_to` | string | BBRT standard field name, or `"source_fields"` |
| `coverage_pct` | float | % of records in this source that have a value |

The field catalog is the codebook for researchers using the dataset, and the
mechanism for identifying fields ready for promotion to standard columns.

---

### Entity Resolution

Business identity is matched across quarterly snapshots in priority order:

1. **Source-provided unique ID** (`source_business_id`) — most reliable. Used
   when the source program assigns its own stable identifier (e.g. NYC vendor ID).
2. **Normalized name + zip** — lowercase, strip punctuation and legal suffixes
   (LLC, Inc, Corp, Co), match on canonical_name + canonical_zip.
3. **New entity** — if no match found, assign a new UUID and add to
   `business_registry`.

Uncertain matches (name similarity > 80% but not exact, or zip changed between
snapshots) are logged to `data/snapshots/YYYY-QN-match-review.csv` each quarter
for manual inspection.

---

## GitHub Actions Automation

**Schedule:** `0 6 1 1,4,7,10 *` — 6am UTC on the first of January, April, July,
and October.

**Workflow steps:**

```
1.  Checkout repo
2.  Set up Python environment, install dependencies
3.  Run all adapters → collect raw records for this quarter
4.  Resolve entity identity → assign stable business_ids
5.  Write new snapshot to DuckDB (businesses + snapshots tables)
6.  Update business_registry (first_seen, last_seen, is_active)
7.  Geocode new records only → Census Geocoder batch API (free, no rate limit)
8.  Export businesses.csv for the website
9.  Write YYYY-QN-summary.txt run report
10. Commit changed files and push → GitHub Pages redeploys automatically
```

**Failure handling:** If a single adapter fails, the pipeline logs the error,
skips that source, and continues. The snapshot is written without that source's
records. GitHub Actions sends an email notification listing failed adapters and
error messages. A broken state adapter never blocks other sources from updating.

**Geocoding:** Census Geocoder batch API replaces Nominatim for new records.
It accepts up to 10,000 addresses per request, is free with no API key or rate
limit, and is more appropriate for U.S. address coverage. Only records with no
existing latitude/longitude are submitted — already-geocoded businesses are not
re-processed.

**GitHub Actions secrets:**
- `SAM_GOV_API_KEY` — required for the SBA 8(a) adapter
- Additional source API keys added as needed

**Storage:** `bbrt.duckdb` is tracked via Git LFS once it exceeds comfortable
git storage. This is the natural trigger to migrate to Approach C (cloud bucket
storage) in a future version.

---

## Site V2 Changes

**State and city filter dropdowns**

Two dropdowns added above the table and search bar, populated dynamically from
the loaded CSV:

```
[State ▾]  [City ▾]  [Search by name, city, or industry…]
```

Selecting a state filters both the map and the table simultaneously. The map
recenters and rezones to the selected state. Selecting a city narrows further.
"All States" resets both filters.

**Confidence tier indicator**

- Table: `Confidence` column in Expanded view — green `Confirmed` badge or
  gray `MBE — unverified` badge
- Map popups: one line below business name — `Confirmed Black-owned` or
  `MBE certified (ethnicity unverified)`
- Map legend below the map explaining the two tiers and marker colors
  (confirmed = green, unverified = gray)

**Coverage bar**

No changes required. The bar already computes dynamically from row count and
will update automatically as records grow. Denominator (160,000) is the research
target and remains fixed.

**No other structural changes.** The About section stat blocks, map, table,
search, column toggle, and dataset request form all work as-is. The site reads
`businesses.csv` — the export layer produces the same format regardless of how
many sources feed into it.

---

## Architecture Notes

### What is DuckDB?

DuckDB is an embedded analytical database — a single binary file that runs
entirely in-process with no server required. Think of it as a spreadsheet that
holds millions of rows and is queried with SQL. It is specifically designed for
research and data analysis workloads: aggregations, time-series queries, window
functions, joining large tables. It integrates directly with Python and R, and
exports to pandas dataframes in one line.

For BBRT: DuckDB is where the panel lives. A researcher can run
`SELECT * FROM businesses WHERE address_state = 'TX' AND snapshot_id BETWEEN '2026-Q2' AND '2028-Q1'`
and get a longitudinal Texas dataset in seconds. The `businesses.csv` the
public site reads is an export from DuckDB — it is not the database itself.

DuckDB is free and open source (MIT license). No paid tier, no row limits,
no expiration.

### What is a cron job?

A cron job is a task scheduled to run automatically at a fixed time, with no
manual triggering. The name comes from *chronos* (time). The schedule is written
in cron syntax: `0 6 1 1,4,7,10 *` means "at 6:00am on the first day of January,
April, July, and October." GitHub Actions supports cron natively — you write the
schedule once and it runs every quarter indefinitely.

### Where the pipeline is vulnerable

**1. Source format changes (highest risk)**
Every adapter hard-codes assumptions about its source: the download URL, the
column names, the file format. If a state program changes their Excel layout,
moves their download behind a login wall, or takes down the public URL, that
adapter breaks. Mitigation: the pipeline sends an email alert when any adapter
produces zero records or raises an error, so breakage is noticed within one
quarter.

**2. Entity resolution errors**
Fuzzy name + zip matching generates false positives (two different businesses
merged) and false negatives (the same business not recognized across snapshots).
This is measurement error in the panel. Mitigation: prefer source-provided unique
IDs wherever they exist; log uncertain matches to a review file each quarter.

**3. GitHub repo size**
DuckDB grows as the panel accumulates years of data. Git is not designed for
large binary files. Mitigation: use Git LFS for `bbrt.duckdb`; migrate to cloud
bucket storage (Approach C) when the file outgrows comfortable LFS storage.

**4. GitHub Actions job time limit**
GitHub Actions has a 6-hour maximum job duration. At scale, running all adapters
plus geocoding new records could approach this limit. Mitigation: geocode only
new records (Census Geocoder batch is fast), and split geocoding into a separate
workflow if needed.
