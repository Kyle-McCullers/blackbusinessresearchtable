# BBRT Comprehensive Expansion — Design Spec

**Date:** 2026-06-10
**Project:** blackbusinessresearchtable.com
**Scope:** Full national source coverage + working downloads + Site V2 + autonomous weekly expansion agent

---

## Overview

Take the Black Business Research Table from 5 states + NYC (~18,700 businesses once
Indiana loads) to the maximum feasible national coverage, make the dataset actually
downloadable from the site, redesign the homepage map as a US-wide coverage map, and
set up a weekly autonomous agent so expansion continues with zero day-to-day input
from Kyle.

## Decisions (made 2026-06-10)

1. **Downloads: "Both"** — instant direct download for the current-snapshot CSV +
   auto-generated codebook; a lightweight form gates the full longitudinal panel
   (DuckDB + all-snapshots CSV).
2. **Expansion: big push + weekly agent** — a comprehensive build-out now, then a
   scheduled cloud agent works the long tail weekly.
3. **Review gate: PRs for new sources, auto-merge for refreshes** — every new
   adapter arrives as a PR Kyle skims; quarterly refreshes of already-approved
   sources commit directly (existing pipeline behavior).

---

## Phase 0 — Repairs

| Item | Action | Owner |
|---|---|---|
| Indiana data not in DB | Re-run pipeline locally; verify ~18.7K records across 6 sources; export fresh `businesses.csv`; commit | Claude |
| `SAM_GOV_API_KEY` missing | Get a free key at https://api.data.gov/signup/ → GitHub repo → Settings → Secrets and variables → Actions → New repository secret named `SAM_GOV_API_KEY`. Also export it locally to load 8(a) data into the baseline | **Kyle (~10 min)** |
| Dead "Request Dataset" form | Replaced in Phase 3 | Claude |

Phase 0 also re-runs the full test suite as a health check before any new work.

---

## Phase 1 — Complete Source Inventory

**Goal:** enumerate every potential source once, so nothing is ever "undiscovered" —
only `built`, `buildable`, `blocked`, or `no_data`.

**Sweep targets:**
- All 50 states + DC
- 5 territories (PR, GU, VI, AS, MP)
- ~40 largest city/county certification programs (NYC done; Chicago, LA, Houston,
  Philadelphia, Atlanta, Cook County, etc.)
- Federal: SAM.gov 8(a) (built), SBA DSBS, FHWA DBE/UCP state directories

**Method:** parallel research agents (batched by region), each answering for its
assigned jurisdictions: does a public MWBE/DBE/SDB/HUB-type directory exist; URL;
format (CSV / Excel / Socrata or other API / portal / PDF); is there a Black /
African American ethnicity field (`confirmed_black`) or only "minority"
(`mbe_unverified`); is it directly fetchable or blocked (login wall, CAPTCHA,
records request, no public data). Findings verified against the live page, not
from memory.

**Outputs:**
1. `scripts/sources_roadmap.yml` — machine-readable, the single source of truth
   driving both the big push and the weekly agent:

```yaml
sources:
  - source_id: wa_omwbe
    name: Washington State OMWBE
    geography: WA
    level: state            # state | territory | city | county | federal
    program: MWBE
    access: csv_download    # csv_download | excel_download | api_socrata | api_rest | portal | pdf | records_request | none
    url: https://...
    ethnicity_field: true   # true → confirmed_black; false → mbe_unverified
    tier: 1
    status: buildable       # built | buildable | blocked | no_data
    status_reason: null     # required when blocked/no_data
    record_estimate: null
    last_checked: "2026-06-10"
```

2. `docs/data-sources/source-inventory.md` — human-readable companion with notes
   per jurisdiction, including dead ends and what unblocking would take.

**Status semantics:** `built` = adapter merged; `buildable` = fetchable now;
`blocked` = exists but not directly fetchable (reason required); `no_data` = no
public program/directory found. The weekly agent re-probes `blocked` entries
periodically — sources unblock when states redesign portals.

---

## Phase 2 — Adapter Big Push

Build every `buildable` source from the roadmap, in parallel batches (git
worktrees, subagent-driven). Each adapter follows the proven pattern:

1. Fetch the real source file; verify the ethnicity field against actual data
2. `scripts/adapters/<source_id>.py` inheriting `AdapterBase`, with `FIELD_MAP`,
   `CONFIDENCE` per the verified ethnicity field
3. Tests added to `scripts/test_pipeline.py`; full suite must pass
4. Run locally; record count sanity check (> 0, within order-of-magnitude of
   `record_estimate`)
5. **PR per source** with a standard summary: source name/URL, record count,
   confidence tier, 5 sample records, any caveats
6. Roadmap entry flipped to `built` in the same PR

PRs are batched so Kyle reviews several in one sitting. After each merged batch,
the pipeline re-runs locally and the site data refreshes.

Sources that fail during build (format surprises, dead URLs) get flipped to
`blocked` with the reason — never silently dropped.

**Realistic outcome:** ~15–25 working sources post-push; remainder explicitly
`blocked`/`no_data` with documented reasons.

---

## Phase 3 — Site V2: Downloads + US Coverage Map

### Downloads ("Both")

- **Direct download card:** buttons for `businesses.csv` (current snapshot) and an
  auto-generated `codebook.md`/`codebook.csv` built from the `field_catalog` table
  (variable name, description, source coverage). No gate. Citation block stays.
- **Full panel form:** replaces the dead form. Formspree (free tier) posts
  submissions (name, affiliation, intended use) to Kyle's email; on submit, the
  page reveals download links for the full longitudinal panel (`bbrt.duckdb` +
  all-snapshots CSV). Zero backend. **Kyle action: 5-min Formspree signup** —
  until then the form falls back to a `mailto:` link.
- Large files served from the repo (GitHub Pages/raw). If `bbrt.duckdb` outgrows
  comfortable git hosting, move to GitHub Releases assets (the quarterly workflow
  uploads them) — same links, no site change.

### US coverage map (homepage)

- **Default view: continental US**, with AK/HI/territories reachable (inset or zoom).
- **Dot map:** one dot per business — green `confirmed_black`, gray
  `mbe_unverified`. Canvas renderer / clustering for performance at 20K+ points.
- **Coverage layer:** US states GeoJSON; states present in the data get a light
  green outline + subtle tint; hover shows state name, business count, and source
  program(s). Uncovered states neutral. Legend: marker colors + covered/not-yet.
- Coverage layer derives from the loaded CSV at runtime — no manual updates; new
  states light up automatically when their data merges.

### Filters & badges (from the original Site V2 spec)

- State and city dropdowns above the table, filtering map + table together
- `Confidence` badge column in Expanded view; confidence line in map popups
- Per-state coverage stats in the About section (dynamic)

---

## Phase 4 — Weekly Autonomous Agent

A scheduled cloud agent (created via `/schedule`) runs **weekly**. It does not
require Kyle's machine to be on; it runs on Anthropic infrastructure against the
GitHub repo.

**Each run:**
1. Read `scripts/sources_roadmap.yml`
2. Pick the next `buildable` source (priority: confirmed_black > unverified;
   larger states first). If none, re-probe the oldest-checked `blocked` entries
   (max 3 per run) and update `last_checked`/`status_reason`
3. Build the adapter end-to-end (fetch real data, verify ethnicity field, tests,
   local pipeline run)
4. Open a **PR** with the standard summary; update the roadmap in the same PR
5. If nothing was built, update the pinned tracking issue
   ("BBRT expansion status") with what was probed and why nothing changed

**Notifications:** GitHub emails Kyle on every PR and issue update — no new
notification infrastructure. The PR description is the report.

**Stop condition:** when no `buildable` sources remain and all `blocked` entries
have been re-probed within 90 days, runs become cheap no-ops; Kyle can pause or
delete the routine anytime.

**Separation of duties:** the weekly agent adds *new* sources via PR; the existing
quarterly GitHub Actions cron refreshes *approved* sources and commits directly
(auto-merge for refreshes, per the review-gate decision).

---

## Testing & Verification

- Every adapter: unit tests on `parse()` with a real captured sample fixture
- Pipeline suite green before any PR opens
- Post-merge: full local pipeline run; record counts per source logged to the
  snapshot summary
- Site: manual verification that map dots, coverage outlines, filters, and both
  download paths work with the merged data

## Risks

| Risk | Mitigation |
|---|---|
| Source format/URL changes break adapters | Quarterly cron emails on failure; weekly agent doubles as repair crew |
| Misfiltered ethnicity field publishes wrong data | PR gate on all new sources; ethnicity field verified against the raw file, sample records in every PR |
| Repo size growth (DuckDB) | Git LFS, then GitHub Releases assets for data files |
| Weekly agent cost | Modest per-run; visible in usage; pausable anytime; becomes no-op when roadmap empties |
| Formspree free-tier limits (50/mo) | Fine at expected volume; direct download path is ungated regardless |

## Kyle's total ongoing involvement

1. One-time: add `SAM_GOV_API_KEY` secret (~10 min); Formspree signup (~5 min)
2. Recurring: skim adapter PRs when GitHub emails arrive (batched, ~2 min each)
3. Quarterly: read the snapshot summary email

Everything else is autonomous.
