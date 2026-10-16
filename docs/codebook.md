# BBRT Codebook (SKELETON)

Data dictionary for the Black Business Research Table. **Skeleton** — every column will be
fully specified during the Phase-2 build. `vis` = PUBLIC (in the public CSV) or PRIVATE
(excluded from public export). See the design spec:
`docs/superpowers/specs/2026-06-14-bbrt-disclosure-study-design.md`.

## Identity & denominator
| Column | Type | vis | Definition / values |
|---|---|---|---|
| business_id | str | PUBLIC | Stable entity id (entity-resolved across sources/snapshots) |
| business_name | str | PUBLIC | |
| is_certified | bool | PUBLIC | On a gov cert list with an explicit Black/African-American field |
| is_self_identified | bool | PUBLIC | Business identifies itself as Black-owned (Google attr; self-reg directories) |
| is_media_identified | bool | PUBLIC | Named Black-owned by a third party (listicle) |
| identification | enum | PUBLIC | Derived primary: `certified` > `self_identified` > `media_identified` |
| identification_sources | json | PUBLIC | List of {source, url, date} contributing each basis |

## Location & geography
| Column | Type | vis | Definition |
|---|---|---|---|
| address_street/city/state/zip | str | PUBLIC | Geocoded business address (mapped to where the business is BASED) |
| latitude, longitude | float | PUBLIC | From Census geocoder; used for matching |
| census_region, census_division | str | PUBLIC | Census 4 regions / 9 divisions |
| county_fips, census_tract | str | PUBLIC | Joinable to ACS/Decennial |
| congressional_district, cd_vintage | str,int | PUBLIC | CD + the redistricting vintage year (118th/2020 baseline) |

## Contact fields (PRIVATE, released on request)
| Column | Type | vis | Definition |
|---|---|---|---|
| owner_name | str | PRIVATE | Owner/contact name as listed by the certifying program |
| phone | str | PRIVATE | Business phone as listed by the certifying program |
| email | str | PRIVATE | Business email as listed by the certifying program |

These come from public certification directories, but those directories serve one firm at a
time behind a search form; a bulk CSV would be a ready-made contact list. They are withheld
from the public export (`PRIVATE_COLUMNS` in `scripts/pipeline/export.py`) and released to
verified researchers for research use on request: write to
data@blackbusinessresearchtable.com with your affiliation and purpose; response within 10
business days. `website` and social handles stay PUBLIC.

## Retention and `last_verified`
**A business, once recorded, is never removed.** Each quarterly run adds new firms and updates
existing ones; it never deletes. If a source stops listing a firm, returns nothing, or cannot
be fetched, the firm stays in every later snapshot with its last observed values.

`last_verified` is the date the record was last observed in its source. A row whose
`last_verified` is earlier than the snapshot date was **retained**, not re-observed that
quarter. Consequences for analysis: absence of a firm from a source's current list is not
recorded as an exit or decertification, and the panel measures first observation, not
survival. Do not infer firm closure from staleness. Each snapshot summary lists sources that
were flagged (failed, empty, or sharply shrunken) and how many records were retained.

## State codes
`address_state` in the public CSV is always a 2-letter USPS code (or blank when the source
value is missing or unrecognizable). The database keeps the raw source value.

## Disclosure layer (PRIVATE — not published)
| Column | Type | vis | Definition |
|---|---|---|---|
| google_gmap_id | str | PRIVATE | Matched Google Maps id (UCSD 2021) |
| google_match_status | enum | PRIVATE | matched / ambiguous / no_profile |
| google_match_score | float | PRIVATE | Fuzzy match confidence |
| discloses_black_google | bool/null | PRIVATE | DV: matched gmap_id carried the Black-owned attribute (2021) |
| disclosure_source/observed_date/coded_by/evidence_url | — | PRIVATE | Provenance of the disclosure measure |
| identity_women, identity_veteran, identity_lgbtq | bool | PRIVATE | Other Google identity attributes (intersectionality) |
| google_misc | json | PRIVATE | Raw UCSD MISC attribute dict |

## Key protocol notes (to expand)
- **Disclosure rate is computed within the `certified` denominator** (independent ground
  truth) to avoid the circularity that the Google tag is both a source and the signal.
- Disclosure snapshot = **Sept 2021**; `no_profile` firms are excluded from the rate.
- `mbe_unverified` minority data is NOT here — it lives in the private `mbe_frame.duckdb`.
