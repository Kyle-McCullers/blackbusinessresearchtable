# HANDOFF.md

Dated entries for resuming work across sessions. Most recent entry first.

---

## 2026-04-03 — Pipeline Infrastructure Complete + First 5 State Adapters

### Completed since last entry
- **Sub-project 1 (Pipeline Infrastructure)** fully merged to `main`:
  - `scripts/pipeline/` — adapter_base, db (DuckDB), entity_resolver, geocoder (Census batch API), export, run (orchestrator)
  - `scripts/test_pipeline.py` — 51+ tests passing
  - `.github/workflows/quarterly_pipeline.yml` — cron `0 6 1 1,4,7,10 *`
- **5 source adapters** built and merged to `main`:
  - `nyc_mwbe.py` — NYC MWBE, 3,775 `confirmed_black` businesses
  - `tx_hub.py` — Texas HUB, 4,074 `confirmed_black` businesses
  - `md_mbe.py` — Maryland MBE, 5,403 `confirmed_black` businesses
  - `ma_sdo.py` — Massachusetts SDO, 593 `confirmed_black` businesses
  - `al_ombe.py` — Alabama OMBE, 103 `confirmed_black` businesses
  - `sam_8a.py` — SAM.gov 8(a), built but **not yet in DB** (requires `SAM_GOV_API_KEY` secret)
- **2026-Q2 baseline snapshot** written to `data/bbrt.duckdb` — 13,948 total records, all `confirmed_black`
- `data/businesses.csv` exported (5.9MB)

### Currently in-flight / half-done
- `sam_8a` adapter exists in `scripts/adapters/` but was excluded from the 2026-Q2 run because the `SAM_GOV_API_KEY` GitHub Actions secret has not been configured. The adapter is complete and tested; it just needs the key.
- Sub-project 2 (national expansion) is underway. 5 of many planned state sources are done.
- Sub-project 3 (Site V2 — state/city filters, confidence badges) has not started. Blocked on having meaningful multi-state data.

### Single most important next action
**Add the next Tier 1 adapter.** Per the design spec (`docs/superpowers/specs/2026-04-02-bbrt-national-expansion-design.md`), the remaining Tier 1 sources are:
- `wa_omwbe` — Washington State OMWBE, CSV download, African American field
- `dc_cbe` — Washington DC CBE, Socrata open data API, ethnicity field

Start with `wa_omwbe` (CSV download is simpler than an API). Create `scripts/adapters/wa_omwbe.py`, inherit `AdapterBase`, add tests, run locally, then merge to `main`.

### Open questions / blockers
1. **SAM_GOV_API_KEY** — needs to be added as a GitHub Actions secret for `sam_8a` to run in the quarterly pipeline. Kyle needs to do this manually in GitHub repo settings.
2. **Tier classification** — the design spec notes that the full 50-state inventory (confirming which tier each state falls into) hasn't been done yet. Before diving into Tier 2, consider doing a quick audit of remaining state programs to confirm tier assignments.
3. **Site V2 trigger** — no explicit target for when to start Site V2. Reasonable threshold: 5+ states with `confirmed_black` data (currently at 4 states + NYC, so next 1-2 adapters may trigger this).

---

## 2026-04-02 — Project Kickoff

### Completed
- Initial V1 site built with NYC MWBE data (3,775 businesses)
- V1 design spec written (`docs/superpowers/specs/2026-04-02-black-business-table-design.md`)
- National expansion architecture designed (`docs/superpowers/specs/2026-04-02-bbrt-national-expansion-design.md`)
- Pipeline infrastructure plan written and executed

### Next action at time of entry
Build Sub-project 1 (pipeline infrastructure) — now complete.
