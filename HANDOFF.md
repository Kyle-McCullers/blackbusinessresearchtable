# HANDOFF.md

Dated entries for resuming work across sessions. Most recent entry first.

---

## 2026-06-13 (evening) — 5 queued adapters built + loaded → 16 states, 28,964 firms

Built, TDD-tested, and loaded the five queued confirmed_black adapters. **Public DB is now 28,964 confirmed_black businesses across 16 states + NYC** (snapshot 2026-Q2), up from 16,736 / 11 states.

New adapters (all manual-capture file readers, same pattern as `or_cobid`/`nv_dbe`):
- `va_swam` — Virginia SWaM/DBE, `Ethnicity == "Black or African American"` → **4,841**. xlsx with 3 repeated SWaM/MWAA/DBE column blocks; first-occurrence-wins reads block 1 (verified no Black firm is populated only in a later block).
- `nc_hub` — North Carolina HUB, `HUB == "Certified"` AND `HUBCategory == "Black"` → **3,615** (of 3,988 Black-category rows; 370 blank-status + 3 not-certified excluded). Normalizes spelled-out state names → 2-letter codes (~6% of firms are out-of-state HUB certs).
- `fl_mbe` — Florida OSD, 3 county-split xlsx files **pre-filtered** to African American + certified (no per-row ethnicity column; provenance is the export query) → **1,898** unique (2,003 rows − cross-county dups). Note: FL files carry NO zip column, so most FL firms geocode by city/state only or are table-only.
- `ca_mbe` — California DGS MBE, `Ethnicity == "Black American"` → **959** (961 rows − 2 dup names). April-2026 snapshot from the admin-data folder (CA live directory is offline). Parses the multi-line `Primary Address` cell best-effort.
- `ar_mwbe` — Arkansas M/WBE Registry, `VendorCategory in {"African American","African-American"}` → **915**.

Each adapter docstring records the exact ethnicity field + all distinct values verbatim (QC requirement). Tests: **135 pass** (115 + 20 new). DB backed up to `data/bbrt.duckdb.bak-316a71f` (local, gitignored/untracked) before the run.

**Pipeline run note:** CT and DE hit a transient Socrata **503** during the run and were **carried forward** (CT 970, DE 335 preserved — no loss). Because `write_businesses` uses `INSERT ... ON CONFLICT DO NOTHING` (no per-snapshot DELETE), a same-quarter re-run is **additive**: 6 TX firms that dropped from the source this cycle linger (4,074 vs 4,068 fetched) and existing firms keep their original geocodes. This was the *safest* path — a clean rebuild would have permanently dropped CT/DE while their APIs were 503ing. The 6 stale TX rows clear on the next clean quarterly snapshot (2026-Q3). 18,700/21,657 freshly-run records geocoded.

Remaining queued adapters from the prior entry are now DONE. Next up unchanged: Mapbox go-live (token push-protection), LA/city scrapers, NMSDC (UM membership check), RA/CRM survey pipeline.

---

## 2026-06-13 — Major expansion session (read this first)

### ⚠️ Environment — where everything lives now
- **Repo moved OFF iCloud** to **`~/Projects/blackbusinessresearchtable`** (the old `~/Desktop/Black Business Research Table` copy is STALE — iCloud was freezing git/venv. Always work in `~/Projects`.) A memory note records this.
- **Python venv: `~/.bbrt-venv`** (the in-repo `scripts/venv` is stale). Run Python as `~/.bbrt-venv/bin/python`. Deps: openpyxl, requests, pytest, duckdb, rapidfuzz.
- Manually-downloaded source files live in Dropbox: `~/University of Michigan Dropbox/Kyle McCullers/Projects and Proposals/Black Business Research Table/data/manual downloads/` (and historical originals in `.../Data/US State(s) Administrative Data/`).

### Current dataset (public, on GitHub)
- `data/bbrt.duckdb` + `data/businesses.csv` = **16,736 confirmed_black businesses, 11 states + NYC**, snapshot 2026-Q2. Sources: MD 5,403 · TX 4,074 · NYC 3,775 · CT 970 · IN 627 · MA 593 · SC 458 · DE 335 · OR 285 · NV 113 · AL 103. (`sam_8a` not loaded — SAM.gov entity API needs a system account, not just a data.gov key.)

### What was done this session
- **Phase 0 repairs:** fixed stale MD tests; built **carry-forward** so a source that doesn't run a cycle is never recorded as false business exits (`db.carry_forward_records`); fixed the Indiana adapter (openpyxl `reset_dimensions`) and loaded IN (627).
- **Phase 1:** full national source inventory → `scripts/sources_roadmap.yml` (every state/territory/city: built/buildable/blocked/no_data, ethnicity field + values).
- **Phase 2 (auto-fetch adapters):** CT, DE (Socrata), SC (xlsx URL), OR, NV (manual gob2g exports) — all built TDD, loaded.
- **Geocoder hardened** to survive network drops (returns {} instead of crashing the run).
- **Site V2** (live on `main`, Leaflet): US source-coverage map (outlines the 11 source-states), confidence dots/badges, state/city filters, direct CSV + codebook downloads, working Formspree form (`xqeolrkw`).
- **Mapbox GL migration:** DONE and **verified rendering with Kyle's token** (he previewed locally). Committed **locally on branch `feature/mapbox-gl` (NOT pushed)** — GitHub push-protection blocks the public Mapbox token. **Live site is still Leaflet (working).**
- **Minority sampling frame** (separate research DB): `data/mbe_frame.duckdb` — **6,732 minority firms (NY MWBE 6,114 + TN MBE 618), 6,729 with email**. GITIGNORED + LOCAL ONLY (privacy: contact info + future race data; never on the public repo/site). Reconstructable from the Dropbox manual-download files via `scripts/frame/frame.py`. This is the sampling frame for the identity-ascertainment study (public-disclosure match → email survey → RA phone follow-up).
- **Catalogs (on GitHub):** `docs/data-sources/source-catalog.csv` (78 rows — which states represented/not + per-directory detail), `nmsdc-affiliates.md`, `ethnicity-field-audit.md`. Roadmap: `scripts/sources_roadmap.yml`.
- **Standing policy (DECISIONS.md):** confirmed_black-only — do NOT load directories without a per-firm Black/ethnicity field (TN/UT/ID/NM/OK/OH/RI/MS DBE rosters have none → skipped).

### Files captured, NOT yet loaded (confirmed_black adapters QUEUED — build next)
In the Dropbox manual-downloads folder (+ CA in the admin-data folder). All have a verified ethnicity field:
- **North Carolina** — `North Carolina Vendor Details_2026-06-13.csv` — filter `HUB`=Certified AND `HUBCategory`=Black → ~3,988 (biggest add since MD).
- **Florida** — 3 files "...African American, certified..._A-G/H-M/N-Z Counties.xlsx" — pre-filtered to African American → ~2,003 (combine all 3).
- **California (historical)** — `.../US State(s) Administrative Data/California/Black_MBE_4.2.2026` — pre-filtered Ethnicity=Black American → 961 (CA live directory is offline; this Apr-2026 snapshot is loadable).
- **Arkansas** — `Arkansas women_minority_owned_business.csv` — filter `VendorCategory` in {"African American","African-American"} (two spellings) → 915.
- **Virginia** — `Virginia Directory Listing Export-...xlsx` — has an `Ethnicity` column but a MESSY export (repeated SWaM/MWAA column blocks) → needs careful parsing.

### Open threads / next actions (priority order)
1. **Build the 5 queued adapters** above (NC, FL, CA-hist, AR, VA) → ~16 states. They're file-based (like or_cobid/nv_dbe): glob the Dropbox folder, find header dynamically, filter ethnicity, dedup, TDD, load.
2. **Go live with Mapbox:** re-add Kyle's public Mapbox token to `js/main.js` line 7 on `feature/mapbox-gl`, then push — GitHub will give a one-time "allow this secret" bypass link (the token is a public client token; just **URL-restrict it** in Mapbox settings). Then merge `feature/mapbox-gl` → `main`. (Recommended permanent fix: load the token from a gitignored `js/config.js`.) The branch is local-only until then.
3. **LA City scraper** (free): `bca.lacity.gov/dbe-company/{id}` detail pages expose `ethnicity` — iterate IDs, parse, keep Black. Build with requests+BeautifulSoup.
4. **city-scrape-targets.md** — a background agent was generating this overnight (other large cities with LA-like scrapeable directories); review it (`docs/data-sources/`), may be untracked/uncommitted.
5. **NMSDC:** check whether **UM has an institutional NMSDC membership** (would unlock the 12,000+ ethnicity-tagged MBE Hub database free) — top lead. Else SRMSDC/Houston/EMSDC/PR via request.
6. **RA/CRM survey pipeline:** export a call sheet from `mbe_frame.duckdb`, draft the ≤5-min 3-Q script (identity + public disclosure) + sheet + re-import path + IRB protocol summary. (Ties to Kyle's dissertation on strategic identity disclosure.)
7. **Quarterly cron** runs 2026-07-01; carry-forward protects file-based sources; needs `SAM_GOV_API_KEY` secret for 8(a) (still 404s — system account needed).

### Backed up to GitHub vs LOCAL-ONLY (in case the computer is lost)
- **On GitHub (safe):** all code, `bbrt.duckdb` (16,736), `businesses.csv`, all docs/catalogs/roadmap, DECISIONS.md.
- **LOCAL ONLY:** `feature/mapbox-gl` branch (unpushed — token blocks it; the work is in local git), `data/mbe_frame.duckdb` (gitignored for privacy; reconstructable from Dropbox files), `~/.bbrt-venv` (rebuildable from requirements.txt). Manual-download files are in Dropbox (cloud-backed).
- A background research agent (city-scrape-targets) was still running at session end; its output doc may be uncommitted in `docs/data-sources/`.

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
