# HANDOFF.md

Dated entries for resuming work across sessions. Most recent entry first.

---

## 2026-06-15 — ▶ PHASE 2 KICKOFF (disclosure study) — READ THIS FIRST, START HERE

This is a cold-start brief for building **Phase 2** of the disclosure study in a fresh
session. It is intentionally over-detailed. The design is **approved** — build to the spec.

### 0. Orientation / environment (verify before doing anything)
- **Repo:** `~/Projects/blackbusinessresearchtable` (NOT the stale `~/Desktop` copy). Branch `main`.
- **Python:** `~/.bbrt-venv/bin/python` (deps: duckdb, openpyxl, requests, rapidfuzz, pytest; NOT pandas/yaml/bs4 — install if a step needs them, and add to `scripts/requirements.txt`).
- **Tests:** `cd scripts && ~/.bbrt-venv/bin/python -m pytest test_pipeline.py -q` → 148 passing now. Keep them green; add tests for everything new (this codebase is TDD).
- **DB:** `data/bbrt.duckdb` is **gitignored**, distributed via **GitHub Release `data-2026-Q2`** (asset `bbrt.duckdb`). It is NOT in the repo tree — if absent locally, `gh release download data-2026-Q2 --pattern bbrt.duckdb --dir data`. After a local load, **compact then re-upload**: compact via `COPY FROM DATABASE` into a fresh file (see batch-3 entry / the `gh release upload data-2026-Q2 data/bbrt.duckdb --clobber` step). The 100MB git limit is why it's on Releases — never `git add` the .duckdb.
- **Current data:** 36,928 businesses, 23 sources, 21 jurisdictions (20 states + DC), ALL currently `confidence='confirmed_black'`. Site live on Mapbox; businesses mapped to their base (address) state via `recordState()` in `js/main.js`.
- **gh CLI** is authenticated as Kyle-McCullers.

### 1. The approved design (build to this)
- **Spec:** `docs/superpowers/specs/2026-06-14-bbrt-disclosure-study-design.md` (APPROVED 2026-06-15). **Codebook:** `docs/codebook.md` (skeleton — fill as you build). **IRB stub:** `docs/irb-data-management-plan.md`.
- Read the spec fully before coding. The non-obvious, load-bearing decisions:
  - **Multi-basis identification** (a business can be >1 basis). **Circularity guardrail** (§4.3): the Google tag is BOTH a source AND the disclosure signal → the defensible disclosure rate is computed **only within the `certified` denominator**.
  - **Dual-basis display/filter guarantee** (§4.3): a firm that is certified AND self-identified must show both and **must still appear when filtering `is_certified=true`**. Never collapse bases to one mutually-exclusive label for filtering. Use `is_certified` (not the derived `identification`) as the certified filter.
  - **Public vs PRIVATE split** (§1.1, §5): disclosure DV, intersectional identity flags, and contact PII are PRIVATE — excluded from the public CSV export and the public site.

### 2. Files/inputs to locate at the start
- **Justin Frake's Google extract CSV** — in Kyle's **dissertation directory** (exact path NOT yet known; ASK KYLE or search `~/University of Michigan Dropbox/.../Dissertation*`). It's the interim input + the cross-check target (reproduce its **~14,000** Black-owned count).
- **UCSD source** (to own the acquisition): Google Local Review Data 2021, https://jiachengli1995.github.io/google/index.html#complete-data — per-state **gzipped JSON, one record/line**, business METADATA files. Fields: name, address, **gmap_id**, latitude, longitude, category, **MISC** (attr dict), url, etc.
- **CONFIRM the exact `MISC` key + value string** that carries the Black-owned attribute (e.g. a "Highlights"/"From the business" key containing "Identifies as Black-owned"). Do this on ONE state file first before scaling.

### 3. Phase-2 build steps (suggested order)

**(A) Multi-basis identification refactor** — `scripts/pipeline/db.py`, `adapter_base.py`, `run.py`, `export.py`, adapters, `js/main.js`, `index.html`, tests.
- Add columns: `is_certified`, `is_self_identified`, `is_media_identified` (BOOL), `identification` (VARCHAR derived primary = strongest: certified>self>media), `identification_sources` (VARCHAR JSON list of {source,url,date}), `identification_date`.
- Backfill existing rows: all current = `is_certified=true`, `identification='certified'`, `identification_sources` from `data_source`. Retire `confidence`/`confirmed_black` (the 22 confirmed_black adapters → `is_certified`). `mbe_unverified` stays OUT of the public DB (it belongs in the private `mbe_frame.duckdb` feeder).
- `export.py`: define PUBLIC vs PRIVATE column lists; the public `businesses.csv` must EXCLUDE all PRIVATE columns (§5). Add a separate full/private export for Kyle (CSV + Parquet; optional Stata `.dta`).
- Site: badges/filter use `is_certified`/`is_self_identified`/`is_media_identified` (currently the site keys off `confidence`/`confirmed_black` — update carefully; site is LIVE).
- Tests first, keep green.

**(B) Geographic enrichment** — extend `scripts/pipeline/geocoder.py`.
- The Census batch geocoder can return **geographies** (use the `geographies` benchmark/vintage endpoint) → capture `county_fips`, `census_tract`. Derive `census_region` + `census_division` from state. Add `congressional_district` + `cd_vintage` (118th/2020 baseline; CD is versioned — see §6 caveat). Store lat/long on every business (already geocoded; needed for matching).
- VERIFY what `geocoder.py` currently returns before extending.

**(C) UCSD acquisition** — NEW `scripts/disclosure/acquire_google_local.py`. **HEAVY — run via a background subagent.**
- Download per-state metadata from UCSD (all US states — decided), parse gzip-JSON, filter to Black-owned via the confirmed MISC key, extract → a `disclosers_google_2021` table (gmap_id, name, address, lat/long, category, the Black-owned flag, AND other identity attrs: women/veteran/LGBTQ → for intersectionality, PRIVATE). Version the script + write a data statement; cite UCSD (Zhang & Li; UCTopic / Personalized Showcases papers).
- Validate against Justin's CSV (~14k). Start with ONE state to confirm parsing, then scale.

**(D) Matching** — NEW `scripts/disclosure/match.py`. (Subagent or inline; reuse `entity_resolver.normalize_name`/`normalize_zip` + `rapidfuzz`.)
- Block by state + coarse geo; score on normalized name + address/zip + lat/long distance; classify `matched`/`ambiguous`/`no_profile`.
- Write PRIVATE disclosure fields: `google_gmap_id`, `google_match_status`, `google_match_score`, `google_match_date`, `discloses_black_google`, `disclosure_source`, `disclosure_observed_date`(2021-09), `disclosure_coded_by`('algorithm'), `disclosure_evidence_url`; intersectional `identity_women/veteran/lgbtq`, `google_misc`.
- **Ingest disclosers into BBRT (deduped):** disclosers matched to an existing certified firm → also set `is_self_identified=true` (dual-basis). Disclosers NOT in BBRT and not certified → ADD as `is_self_identified=true`, source "Google Maps Black-owned attribute (UCSD Google Local 2021)".
- **Compute the disclosure rate WITHIN the certified denominator** (exclude self-identified-via-Google to avoid circularity; exclude `no_profile`). Report match-quality stats.

**(E) Wrap-up:** fill `docs/codebook.md` (every column, public/private, the circularity + 2021 + CD-vintage notes); compact the DB; `gh release upload data-2026-Q2 data/bbrt.duckdb --clobber`; commit code + `businesses.csv` + docs; push. Update CLAUDE.md counts.

### 4. Decisions already locked (do NOT re-litigate)
Variable name `identification` (keep) · acquire ALL US states · exclude `no_profile` from the rate · Yelp deferred (Fusion API lacks the badge; await Justin's method — DO NOT scrape) · no RA yet (ship automated; RA validation is a later slow-burn) · multi-basis + dual-basis guarantee · public/private split · disclosure rate within `certified`.

### 5. Open questions to surface to Kyle (don't block on them)
Post-2021 discloser acquisition w/o ToS violation (UCSD is fixed 2021; Places API lacks the attribute) · whether to expose intersectional identities publicly (default PRIVATE) · mining UCSD reviews (future paper; a sample-review agent once data is local) · self/media source add-mechanisms to verify before ingesting (blackownedeverything.co = self-registration; 15% Pledge; myblackreceipt).

### 6. Context/process note
This design emerged over a long session (compacted once). Phase 2 is token-heavy (multi-GB acquisition, big match) — **run acquisition + matching via background subagent(s)** and keep the main thread for the refactor + reviewing results, OR just let it run and rely on git/Release as the durability backstop (work is safe regardless of compaction). Memory files `bbrt-disclosure-study` and `BBRT Pipeline Infrastructure Status` summarize all of this.

---

## 2026-06-14 (batch 3) — DC UCP (1,796) + base-state map coding → 23 sources, 36,928 firms

- **`dc_ucp`** — DC UCP DBE, **1,796 Black firms** (Ethnicity="Black"; new jurisdiction = DC). The "DC UCP exported empty" call earlier was WRONG — the file is a custom **Oracle APEX HTML** export (not B2Gnow): single `<table>` whose data `<tr>` are NOT closed, and the Address column packs street/city/state/zip/phone/email/website across `<br/>` in one cell. `scripts/adapters/dc_ucp.py` parses it by grouping `<td>` cells into 9-col rows and splitting the address on `<br/>`. Validates base-state coding hard: DC-UCP firms are based MD 769 / DC 554 / VA 126 / GA 55 / … — almost all outside DC.
- **Base-state map coding (Kyle's request):** `js/main.js` now maps/filters each business by the state it is *based in* (geocoded `address_state`), falling back to the program's state only when the address has no recognizable US state. New `recordState()` wraps the old `deriveSourceState()`. Dots were already at true lat/long; this fixes the state outline/filter attribution.
- DB compaction is now a required step after same-quarter reloads (bloats; `COPY FROM DATABASE` into a fresh file → 64MB→55MB here). **Storage decision still pending** (GitHub Releases vs LFS) — see below.

### Still open / discussed this session (not yet built)
- **FAA national DBE scrape** — NOT headlessly verifiable (portal needs a session; the search page exposes no detail-page structure). GATING SPOT-CHECK still needs Kyle: open one firm on faa.dbesystem.com, click its certification type, and report whether the detail page shows an ethnicity/race/disadvantaged-group field. If yes → a scrape could recover ethnicity for the no-ethnicity DBE directories (NJ/WA/TX-DOT/ID/DE-DOT/New Orleans/Cleveland) + untouched states.
- **DuckDB-in-git storage — RESOLVED (2026-06-14):** `bbrt.duckdb` is now gitignored and distributed via **GitHub Releases** (release `data-2026-Q2`; site links to `/releases/latest/download/bbrt.duckdb`). Quarterly workflow downloads the latest release DB before running and publishes a new dated release after. **After a LOCAL pipeline load, refresh the distributed copy:** `gh release upload data-2026-Q2 data/bbrt.duckdb --clobber` (or `gh release create data-<date> data/bbrt.duckdb --latest`). Only `businesses.csv` + snapshot summaries stay in git. (Old DB blobs remain in history — not purged, would need a risky force-push on the live Pages branch.)
- **Uncertified / self-identified lists** (BLM-era "best Black-owned businesses in <city>" listicles, directories like blackownedeverything.co, Instagram features): proposed a NEW confidence tier (`self_identified` / `media_identified`) kept separate from `confirmed_black`, so researchers can include/exclude. Captures consumer/retail firms that procurement directories miss. Instagram = low feasibility (API/ToS) — treat as lead source, not a scrape target. Plan not yet built; awaiting Kyle's tier-label decision.
- **Other federal agencies:** the productive federal vein is DOT DBE (done) + SBA 8(a) (blocked on SAM system account). Most federal sources don't publish per-firm RACE (privacy); Census ABS is aggregate-only. Federal is largely tapped.

---

## 2026-06-14 (batch 2) — 3 more B2Gnow tenants → 22 sources, 20 states, 35,131 firms

Added `chicago_mwbe` (1,811 — Illinois NEW), `baltimore_mwboo` (661 — Maryland density), `hawaii_dbe` (11 — Hawaii NEW). DB now **35,131 confirmed_black firms, 20 states**, 22 sources. Tests 145. Site SOURCE_CITY_STATE got Chicago→Illinois, Baltimore→Maryland.

**KEY LEARNING — B2Gnow ethnicity is per-tenant, not guaranteed.** The platform's ethnicity *search filter* does NOT always survive into the Excel/HTML *export*. From Kyle's full B2Gnow download pass (2026-06-14):
- EXPOSES ethnicity (built): Houston, Chicago, Baltimore, Hawaii, Pennsylvania; Atlanta via Black-specific cert code `AABE`.
- Does NOT expose ethnicity (downloaded but NOT loaded — fails confirmed_black-only): **Cleveland (OH), Delaware DOT, Idaho, New Jersey, New Orleans (LA), Texas DOT, Washington state, DC DDOT.** Files are in the manual-downloads folder labeled "(no ethnicities to select)"; their exports carry only DBE/SBE/MBE cert types, no Black breakdown, no AABE-style code. Colorado was offline; DC UCP exported empty; Chicago-Transit/FAA have no download button.

**OPEN LEAD — FAA national DBE directory as a scrape (Kyle's question).** `faa.dbesystem.com` lists **41,039 firms / 72,156 certifications** but the LIST page has no ethnicity (DBE ≠ Black; includes women + other groups), so it's unusable as-is. IF the per-firm detail page ("click the certification type") exposes the disadvantaged-group/ethnicity, scraping it could recover ethnicity for ALL the no-ethnicity DBE directories above at once (NJ, WA, TX-DOT, ID, DE-DOT, New Orleans, Cleveland, …) plus states never touched. Not yet verified — needs a spot-check of one detail page (session/POST, likely needs requests-with-cookies or Playwright). This is the single highest-leverage unbuilt source if detail pages carry race. Same pattern as the LA City scraper lead.

---

## 2026-06-14 (cont.) — B2Gnow adapter family → 19 sources, 18 states, 32,647 firms

Built the first 3 of a **B2Gnow adapter family** and loaded them. DB now **32,647 confirmed_black firms, 18 states + NYC** (added Georgia + Pennsylvania; Houston adds Texas density).

- `houston_obo` — Houston OBO MWBE, Ethnicity in {Black, Black American} → 2,224 firms (5,527 rows deduped).
- `pa_ucp_dbe` — Pennsylvania UCP DBE, Ethnicity = "Black American" → 592.
- `atlanta_aabe` — Atlanta OCC, Certification Type = "AABE" (no ethnicity column) → 866. Its `.xls` export is actually HTML — parsed by the shared base.

**Shared base: `scripts/pipeline/b2gnow_base.py`** (NOT in adapters/, so auto-discovery doesn't instantiate it). Handles both gob2g CSV (latin-1, preamble, dup City/State/Zip cols) and HTML-`.xls` (stdlib HTMLParser, no new dep); filters on an Ethnicity column or a Black cert code; dedups; maps fields. Adding another B2Gnow tenant = a ~15-line subclass setting SOURCE_ID/glob/filter/FIELD_MAP. Tests: **143 pass** (135 + 8). Site `js/main.js` SOURCE_CITY_STATE got Houston→Texas, Atlanta→Georgia (so the coverage map attributes city sources correctly).

**The vein:** most remaining public Black-tagged directories are B2Gnow (manual Excel export — NOT auto-fetchable). Verified inventory of next tenants in `docs/data-sources/dbe-b2gnow-sources.md`: Chicago, Baltimore (`baltimorecity.diversitycompliance.com`), Cleveland, CO/NJ/WA/ID state DBE, GA/TX/DC DOT DBE, FAA national. Workflow: Kyle exports the directory (filter Ethnicity=Black American → Download Results to Excel) to the manual-downloads folder; each new tenant is a quick subclass. **One manual export per NEW domain confirms the ethnicity column survives** — already confirmed for all 3 domains (csv on mwdbe/dbesystem, HTML on diversitycompliance).

**Policy recorded (DECISIONS.md):** NMSDC = members-only/licensed → never on the public site (private enrichment only). BBRT tracks businesses not cert legal status → DBE directories with an explicit Black field ARE captured despite the 2025 USDOT DBE rule flux (dated snapshots).

---

## 2026-06-14 — Mapbox GL is LIVE on main

Migrated the site map from Leaflet to **Mapbox GL JS (Light v11)** and shipped it to production (commit `0168121`). The three front-end files (`index.html`, `js/main.js`, `css/style.css`) were lifted from the old `feature/mapbox-gl` branch onto current `main` (the branch was stale — pre-dated the 5-adapter / 28,964-firm load — so it was NOT merged wholesale; only the 3 map files were applied, then the stale branch was deleted).

Token handling (the thing that had blocked go-live): Kyle created a **new URL-restricted public token** (`pk.…syliFAQ`, restricted to blackbusinessresearchtable.com + www + localhost:8000) — the old unrestricted default token is no longer in the code. GitHub push-protection still flags any Mapbox token; Kyle used the one-time "allow secret" unblock link, then the push succeeded. Going forward, refreshing the token = edit `js/main.js:7` and re-trigger the same unblock flow.

Map style is Light v11 (chosen over Dark/Standard/custom). Optional future polish: a branded monochrome Mapbox Studio style — swap one line (`js/main.js:96`).

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
