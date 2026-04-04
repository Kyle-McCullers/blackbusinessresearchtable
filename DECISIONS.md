# DECISIONS.md

Architecture and design decisions made during development. Append-only — do not edit prior entries.

---

| Date | Decision | Rationale | Implications |
|---|---|---|---|
| 2026-04-02 | Use DuckDB as the panel database instead of flat CSV | Need longitudinal panel with quarterly snapshots, entity resolution across time, and SQL query support for researchers. CSVs can't handle this at scale. | `bbrt.duckdb` is the source of truth; `businesses.csv` is an export layer for the public site only. |
| 2026-04-02 | Use Census Geocoder batch API instead of Nominatim/OSM | Free, no API key, no rate limit, up to 10,000 addresses per request, and specifically designed for U.S. addresses. Original V1 used Nominatim. | Only geocode new records each quarter — already-geocoded businesses are not re-processed. |
| 2026-04-02 | Adapter pattern with `AdapterBase` abstract class | Each state source has different formats/APIs; need to isolate breakage, enable independent development, and allow auto-discovery by the orchestrator. | Adding a new source = one new file in `scripts/adapters/`. Orchestrator requires no changes. |
| 2026-04-02 | Store all source fields in `source_fields` JSON column | Don't discard data — sources often have fields with no BBRT equivalent that may be valuable for researchers. Promotes fields to standard columns when they appear across many sources. | Keeps schema stable while preserving raw data. Field catalog table (`field_catalog`) documents coverage. |
| 2026-04-02 | Two confidence tiers: `confirmed_black` vs `mbe_unverified` | Some sources (SAM.gov 8(a)) only identify "MBE" with no ethnicity breakdown. Including them expands coverage but risks diluting the dataset if not clearly labeled. | All records must carry a `confidence` field. Site V2 will display this as a badge. Researchers can filter by tier. |
| 2026-04-02 | GitHub Actions quarterly cron at `0 6 1 1,4,7,10 *` | Pipeline needs to be fully automated with no manual triggering. GitHub Actions is free for public repos, integrates with git push, and sends failure notifications. | Next auto-run: 2026-07-01. Manual runs can be triggered via `workflow_dispatch`. |
| 2026-04-02 | Site V1 remains static HTML/JS (no framework, no build process) | Simplicity, zero hosting cost (GitHub Pages), no CI/CD complexity. The data is the product, not the UI framework. | Fine for current scale. Site V2 adds state/city dropdowns but stays static — reads `businesses.csv` dynamically via PapaParse. |
| 2026-04-03 | `sam_8a` adapter built but excluded from 2026-Q2 baseline | SAM_GOV_API_KEY not yet configured as a GitHub Actions secret. Adapter is correct and tested; it was a deployment blocker, not a code issue. | Must add `SAM_GOV_API_KEY` to GitHub repo secrets before next quarterly run (2026-07-01). |
