# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Project Overview

**Black Business Research Table** (`blackbusinessresearchtable.com`) — a longitudinal panel database of Black-owned businesses in the United States, built from public MWBE/certification records and open data sources. Designed for researchers, journalists, and policymakers.

Full original spec is in `research_table_claudecode_memo.md`. Architecture for the national pipeline expansion is in `docs/superpowers/specs/2026-04-02-bbrt-national-expansion-design.md`.

---

## Current State (as of 2026-04-03)

**Sub-project 1 (Pipeline Infrastructure): COMPLETE**
**Sub-project 2 (National Expansion — State/Federal Adapters): IN PROGRESS**

Database: `data/bbrt.duckdb` — 16,736 businesses across 2026-Q2 snapshot (11 states + NYC).

| Adapter | Source | Type | Count | Fetch |
|---|---|---|---|---|
| `md_mbe` | Maryland MBE | `confirmed_black` | 5,403 | file (manual) |
| `tx_hub` | Texas HUB | `confirmed_black` | 4,074 | auto (CSV URL) |
| `nyc_mwbe` | NYC MWBE | `confirmed_black` | 3,775 | file (manual) |
| `ct_das_smbe` | Connecticut DAS | `confirmed_black` | 970 | auto (Socrata) |
| `in_idoa` | Indiana IDOA | `confirmed_black` | 627 | file (manual) |
| `ma_sdo` | Massachusetts SDO | `confirmed_black` | 593 | file (manual) |
| `sc_smbcc` | South Carolina SMBCC | `confirmed_black` | 458 | auto (xlsx URL) |
| `de_osd` | Delaware OSD | `confirmed_black` | 335 | auto (Socrata) |
| `or_cobid` | Oregon COBID | `confirmed_black` | 285 | manual capture |
| `nv_dbe` | Nevada NDOT DBE | `confirmed_black` | 113 | manual capture |
| `al_ombe` | Alabama OMBE | `confirmed_black` | 103 | file (manual) |
| `sam_8a` | SAM.gov 8(a) | `mbe_unverified` | (not in DB — SAM.gov entity API returns 404 with a data.gov key; needs a SAM.gov system account) |

Manual-capture sources (`or_cobid`, `nv_dbe`) read CSV exports Kyle downloads to the
Dropbox `data/manual downloads` folder; the pipeline carries them forward on
quarters where no fresh file is provided.

`in_idoa` count is unique firms (deduplicated on Bidder ID); the source has 4,759
African-American commodity-code rows that collapse to 627 firms.

**Expansion is now roadmap-driven.** See `scripts/sources_roadmap.yml` for the full
national inventory (built / buildable / blocked / no_data). Per the 2026-06-10
decisions (`DECISIONS.md`), the big push prioritizes auto-fetch (API/URL) sources:
next up are `ct_das_smbe`, `de_osd` (Socrata), then `or_cobid`, `nv_dbe`, `sc_smbcc`
(direct Excel URLs). File-based sources (`md_mbe`, `ma_sdo`, `in_idoa`) refresh only
when a file is provided; the pipeline carries them forward otherwise.

---

## Architecture

### Tech Stack

- **Pipeline:** Python 3 + DuckDB — `scripts/pipeline/` + `scripts/adapters/`
- **Site:** Static HTML/CSS/JS — hostable on GitHub Pages or Netlify
- **Automation:** GitHub Actions cron (`0 6 1 1,4,7,10 *`) — quarterly pipeline runs
- **Dependencies:** `scripts/venv/` — activate with `source scripts/venv/bin/activate`

### Adding a New Adapter

1. Create `scripts/adapters/<source_id>.py` inheriting from `AdapterBase`
2. Implement `fetch()` and `parse()` methods
3. Set `SOURCE_ID`, `SOURCE_NAME`, `PROGRAM`, `GEOGRAPHY`, `CONFIDENCE`, `FIELD_MAP`
4. The orchestrator (`scripts/pipeline/run.py`) auto-discovers it
5. Add tests to `scripts/test_pipeline.py`

**Confidence tiers:**
- `confirmed_black` — source has an explicit Black/African American ethnicity field used to filter
- `mbe_unverified` — source only identifies "MBE" with no ethnicity breakdown

### File Structure

```
blackbusinessresearchtable/
├── index.html                          ← V1 site (functional)
├── css/style.css
├── js/main.js
├── data/
│   ├── bbrt.duckdb                     ← research panel database (tracked via Git LFS when large)
│   ├── businesses.csv                  ← current snapshot exported for the public site
│   └── snapshots/
│       └── YYYY-QN-summary.txt
├── scripts/
│   ├── pipeline/                       ← orchestrator, base class, DB, geocoder, export
│   ├── adapters/                       ← one file per data source
│   ├── test_pipeline.py
│   ├── requirements.txt
│   └── venv/                           ← local Python virtualenv (gitignored)
├── docs/
│   ├── data-sources/
│   │   └── mwbe-download-instructions.md
│   └── superpowers/
│       ├── specs/
│       │   ├── 2026-04-02-bbrt-national-expansion-design.md   ← master architecture doc
│       │   └── 2026-04-02-bbrt-sam-8a-adapter-design.md
│       └── plans/
│           ├── 2026-04-02-bbrt-pipeline-infrastructure.md
│           └── 2026-04-02-bbrt-sam-8a-adapter.md
├── .github/workflows/quarterly_pipeline.yml
├── CLAUDE.md
├── HANDOFF.md
└── DECISIONS.md
```

---

## Running the Pipeline Locally

```bash
cd scripts
source venv/bin/activate
python -m pipeline.run
```

The pipeline writes to `data/bbrt.duckdb` and exports `data/businesses.csv`.

---

## Site Design Constraints

- **Aesthetic reference:** opportunityinsights.org — clean, minimal, data-forward
- **Colors:** `#111111` (black), `#FFFFFF` (white), `#1B4332` (deep green) — accent TBD
- **Font:** Inter or IBM Plex Sans (Google Fonts)
- **No stock photos** — maps and data are the visuals
- Tone: academic credibility, not startup landing page
- **Site V2** (after multi-state data is in): add state/city filter dropdowns, confidence tier badges on map and table
