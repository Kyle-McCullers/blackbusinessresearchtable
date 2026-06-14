# DBE / B2Gnow Public Source Inventory (2026-06-14)

Research sweep for additional **public, free, redistributable** directories with a
per-firm Black/African American field. Companion to `source-catalog.csv` and
`sources_roadmap.yml`.

## Key finding — B2Gnow is the dominant remaining vein

Most state DOT **DBE** directories and many city MWBE directories run on **B2Gnow**,
served under three interchangeable domains that share the identical
`FrontEnd/searchcertifieddirectory.asp` engine:

- `*.dbesystem.com` — state DOT / Unified Certification Program (UCP) DBE programs
- `*.mwdbe.com` — city MWBE programs
- `*.diversitycompliance.com` — city / agency programs

The engine exposes both an **`EthnicityID` filter** (federal DBE presumed-group
values; verbatim Black value = **"Black American"**, per 49 CFR Part 26) and
**Excel/CSV export** ("Download Results to Excel"). Workflow: filter
`EthnicityID = Black American` (or, for Atlanta, certification type
**"African American Business Enterprise (AABE)"**) → download → a Black-only
per-firm list.

**Fetch method = manual Excel export** for all of these (stateful ASP portal; not a
clean auto-CSV URL — POST scraping fails without session/viewstate). Same
manual-capture model as the existing `nc_hub` / `va_swam` / `or_cobid` adapters.

**Confidence:** the export-includes-ethnicity mechanism is HIGH-confidence (proven
from the California UCP page source: `EthnicityID` + `DownloadToXLS`/
`ExportResultsCSV`). Residual uncertainty: whether a *specific tenant* suppresses
the ethnicity column in its Excel template — confirm with **one manual export per
domain** before mass-building.

## ⚠️ Legal caveat — Oct 3, 2025 USDOT Interim Final Rule

Following litigation, USDOT issued an Interim Final Rule requiring reevaluation of
race-conscious DBE/ACDBE certifications; some directories now warn that
race-conscious certs are "not valid for contracting." The historical ethnicity
field still exists in exports, but directory freshness/maintenance may be
disrupted. For BBRT (a longitudinal research panel) this is acceptable as a **dated
snapshot** — but the confidence/date must be recorded, and these are best captured
sooner rather than later.

## Top actionable new sources (all live HTTP 200 unless noted; none in BBRT yet)

| # | Source | Endpoint | New state? | Ethnicity handle |
|---|---|---|---|---|
| 1 | Atlanta — Office of Contract Compliance | `atlanta.diversitycompliance.com` (302→canonical) | city | cert type "African American Business Enterprise (AABE)" — cleanest |
| 2 | Chicago — City M/W/DBE | `chicago.mwdbe.com` | city | EthnicityID = Black American |
| 3 | Houston — Office of Business Opportunity | `houston.mwdbe.com` | city | EthnicityID = Black American |
| 4 | Pennsylvania UCP DBE | `paucp.dbesystem.com` | ✅ PA | EthnicityID = Black American |
| 5 | Colorado UCP DBE/ACDBE | `coucp.dbesystem.com` | ✅ CO | EthnicityID = Black American |
| 6 | New Jersey UCP / NJDOT DBE | `njucp.dbesystem.com` | ✅ NJ | EthnicityID = Black American |
| 7 | Baltimore City MWBOO (current) | `baltimorecity.diversitycompliance.com` | city | EthnicityID = Black American (replaces dead Socrata us2p-bijb) |
| 8 | Washington — WSDOT OMWBE DBE | `wsdot.diversitycompliance.com` | ✅ WA | "Download Results to Excel" confirmed on page |
| 9 | Idaho ITD DBE | `itd.dbesystem.com` | ✅ ID | EthnicityID = Black American |
| 10 | Delaware DOT Civil Rights DBE | `deldotcivilrights.dbesystem.com` | (DE MWBE built; DOT DBE is a distinct firm set) | EthnicityID = Black American |

Also live & assessed: **FAA National DBE/ACDBE Directory** `faa.dbesystem.com`
(nationwide aggregate of all state-certified DBE/ACDBE — largest, slowest, overlaps
state lists), **Cleveland** `cleveland.diversitycompliance.com`, **CTA Chicago
Transit** `cta.dbesystem.com`, plus GA/TX/HI/DC DOT DBE (302 — need correct entry
URL/XID), New Orleans, Arizona ADOT.

No new clean **auto-CSV/API** source was found — the previously-built auto-fetch
sources (CT/DE Socrata, SC/NV/OR xlsx URLs) are the exceptions, not the rule.

## Checked — does NOT qualify (do not re-investigate)

- **Maryland Socrata `djj3-7sjc`** — non-tabular "link" record pointing at B2Gnow; no bulk download. (MD already built.)
- **Old Baltimore Socrata `us2p-bijb` / `79vq-8kmw`** — dead/redirect to ArcGIS legacy; superseded by the B2Gnow directory (#7).
- **Baltimore ArcGIS REST** (`opendata.baltimorecity.gov/egis/...`) — `499 Token Required`; not public.
- **Florida DOT DBE** `fdotxwp02.dot.state.fl.us` — timed out to non-browser clients; Excel ethnicity exposure unverified. (FL OSD MBE already built; this is the distinct DOT DBE set — needs a browser session to check.)
- **Tennessee TNUCP** `tdot.tn.gov/applications/dbedirect/` — JS single-page app; export/ethnicity not confirmable without a browser.
- **Columbus OH ODI** — no downloadable minority-business dataset with a Black field surfaced.
- **NMSDC / MMSDC / council Hubs** — members-only/licensed; excluded by policy.
- **Generic MBE/minority directories without a Black breakdown** — excluded.

## Recommended build model

A parameterized **`b2gnow` adapter family**: one base taking `(domain, tenant TN, XID)`,
documenting the manual "filter EthnicityID = Black American → Download Results to
Excel" step, then parsing the xlsx — analogous to the existing file-based adapters.
`confirmed_black` tier *iff* a one-time export confirms the ethnicity column
survives in that tenant's Excel template.

**Sequence:** (1) one manual test export per domain (`.dbesystem.com`,
`.mwdbe.com`, `.diversitycompliance.com`) to confirm ethnicity survives; (2) build
the shared parser; (3) prioritize Atlanta (explicit AABE), then Chicago/Houston,
then PA/CO/NJ/WA state DBE. Entity resolution dedups firms that also hold a state
MWBE cert already in BBRT.
