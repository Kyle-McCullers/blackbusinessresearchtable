# BBRT improvement backlog

Running list of things that would make the database better. Started 2026-10-25. Add freely; nothing here is scheduled.
Priority guess in brackets: [H] changes what the numbers mean, [M] adds coverage, [L] polish.

## 1. Counting and identity
- [H] One row per firm for the primary file; keep the longitudinal panel for history (Kyle, 2026-10-25). Needs a cross-source matching rule (proposed: normalized name + ZIP, near-matches to a review file, never auto-merged).
- [H] Within-source repeats still present: NYC ~144, DC ~129, FL ~63, Chicago ~31, TX ~21 (same name+ZIP). Decide per source: separate locations vs. duplicate rows.
- [H] Certification status for sources other than CT. Known: TX hub_status is D/I for every stored row; NC has blank-status rows; Atlanta and AL carry expiration dates; SAM has registration status. Same columns (`certification_status`, `first_certified`, `certification_expires`).
- [H] Owner-removal policy. Retention protects against sources dropping firms; it does not say what happens if an owner asks to be removed. Decide and write it down before the site is promoted.
- [M] Ownership change as the one real disqualifier (DECISIONS 2026-06-14): currently invisible.

## 2. Missing sources
- [M] States with no program here (30). The roadmap holds the inventory: `scripts/sources_roadmap.yml` (42 blocked, 6 buildable, 11 no-data) and `docs/data-sources/city-scrape-targets.md`.
- [M] Rebuild the Texas (CMBL/VetHUB) and South Carolina (HTML directory) adapters; both are carried forward and flagged.
- [M] SAM.gov public entity extract, business type `OY` (Black American Owned), see memory note on the SAM finding. National, annual renewal, self-selected.
- [M] More cities and counties, especially large metros with no state program (Philadelphia, Los Angeles, Detroit, etc.; check the city-scrape list first).
- [M] File-based sources refresh only when someone supplies a file (MD, NYC, MA, IN, AL, others). Put them on a schedule and show "last refreshed" on the site (already shown as "carried forward").
- [M] More B2Gnow tenants whose export carries ethnicity (see ethnicity-field-audit).

## 3. Sources that are not government programs
- [M] Directories of Black-owned businesses (Support Black Owned, National Black Guide, Official Black Wall Street, We Buy Black, Black-Owned Everything). Tier `self_identified` (decided 2026-06-14). Check each site's terms before collecting; the NMSDC decision (2026-06-13) is the precedent: only redistribute what is public and redistributable.
- [M] News and "best Black-owned in <city>" lists, tier `media_identified`. Keep the article URL and date as provenance.
- [M] Black chambers of commerce and local business associations; MBDA centers; HBCU entrepreneurship programs.
- [M] Grant and accelerator cohorts (founder funds, bank and corporate programs): dated, named lists with a clear identity basis.
- [M] Google Business Profile and Yelp "Black-owned" attributes (the Phase 2 disclosure work already reads Google).
- [L] Instagram and other social platforms as leads only, not as a collected source (terms).
- [M] Public suggestion form is live on the branch site (goes to the Formspree inbox); needs a review workflow: verify against a public source, then add as `self_identified` with provenance.

## 4. Black-owned professional practices (doctors, dentists, lawyers, accountants, therapists, architects)
- Where they already are: government certification lists include professional-services firms. In the current file, NAICS 54 (professional/scientific/technical) = 1,424 and NAICS 62 (health care) = 335 listings, but NAICS is filled for only 5,589 of 36,280 listings, so these undercount. Offices of physicians (6211) = 21, dentists (6212) = 1, legal (5411) = 64, accounting (5412) = 90.
- Existing directories (lead sources, all self-identified, uneven verification): Find A Black Doctor, Black-Owned Dental Practices, Black Doctors, Black Health Directory (blackhealth.org), Better Black News provider directory, Support Black Owned, National Black Guide, plus association directories (National Medical Association, National Dental Association; others such as NBA, NABA, NOMA, Association of Black Psychologists to check). Several are member-only or have no stated reuse terms.
- Verification that a practice exists and where: the public NPI registry (NPPES) and state licensing boards give provider, specialty and practice address. They do not give race, so identity still comes from the directory or the person. Pipeline idea: directory lead -> NPI / license match -> one practice record.
- Open design question: BBRT lists businesses, directories list practitioners. A Black physician employed by a health system is not a Black-owned business. Need a rule (owner-operated practice only? partners? group practices?) and a field for practice vs. practitioner.
- Denominator: Census Annual Business Survey and Nonemployer Statistics report firm counts by owner race and NAICS (aggregate). Use them to say how much of each profession this covers.

## 5. Location
- [H] 5,126 listings (14%) have no map location. Fall back to ZIP centroid, then county, so they at least count in the county map; keep a flag for how the location was derived.
- [M] Online-only, home-based and mobile businesses: add `location_type` (storefront / office / home-based / online-only / mobile) so they are not lost from maps and not read as "no data".
- [L] Census tract and per-capita layers (population denominators) for the map.

## 6. Contact, web and social
- [M] Populate the reserved social columns (`instagram_handle`, `facebook_url`, `tiktok_handle`, `yelp_url`, `google_maps_url`) from source fields and from links on each firm's own website. Decide: business accounts public, personal accounts never.
- [M] Website cleanup: some values are junk (e.g. a bare `https:`), some are emails (already blanked in the public file).
- [L] Verify websites still resolve; mark dead links.

## 7. Industry and quality
- [H] Industry is missing for most listings (NAICS present for about 15%). Classify from NAICS, program category, name and description; keep the method and a confidence value.
- [M] Name cleanup (leading quotes, symbols), address standardization, ZIP validation.
- [M] Totals-reconciliation tests per source and committed fixtures (TODO-ORGSCI-STAGE1 section C).
- [L] Source drift alarms beyond the "flagged" summary (row-count and schema checks per adapter).

## 8. Research and publication
- [M] Phase 2 disclosure branch: decide merge scope (see memory note on the disclosure study).
- [M] Archive each snapshot with a DOI (e.g. Zenodo) so citations point at a fixed file.
- [L] Accessibility audit of the new site; print and screenshot-friendly map export.
