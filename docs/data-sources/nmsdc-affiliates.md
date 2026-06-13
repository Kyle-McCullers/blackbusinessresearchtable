# NMSDC Affiliate Councils: Data Source Inventory

## Overview

The National Minority Supplier Development Council (NMSDC) is the leading U.S. nonprofit that certifies and connects Minority Business Enterprises (MBEs) with corporate members. NMSDC operates through 23 regional affiliate councils that each conduct their own certification in defined geographic territories. Unlike generic "small business" programs, NMSDC certification requires ethnicity verification at the individual ownership level — the recognized categories are African-American/Black, Hispanic/Latino, Asian-Pacific, Asian-Indian, and Native American — making it one of the few major certification bodies that systematically tags businesses by specific ethnicity group. This makes Black-owned businesses identifiable within the dataset, which is the key value proposition for BBRT.

**Critical caveats:**

1. **The NMSDC Hub consolidation (2024–2025):** NMSDC has migrated all regional certification onto a single national platform called The NMSDC Hub (`thehub.nmsdc.org`). Most affiliate portals now redirect MBE applications there. The full national search database sits behind a members-only login. Regional affiliate sites that previously hosted their own directories are mostly pointing to the Hub or have deprecated their local search tools.

2. **Members-only national search:** The main NMSDC Hub MBE search tool (12,000+ certified businesses) is available only to NMSDC corporate members and MBEs with credentials. Corporate membership costs are not publicly disclosed but are typically in the range of several thousand dollars annually for large institutions. The national database does contain ethnicity fields and reportedly supports filtering by ethnicity type, but this is not publicly accessible.

3. **Deduplication with state MWBE rosters:** NMSDC-certified businesses often also hold state MWBE certification (e.g., a firm in Maryland may appear in both the CRMSDC directory and the Maryland MBE database already in BBRT). Do not double-count.

4. **Check-Mate:** NMSDC's corporate-member "Check-Mate" validation service returns only tax ID, company name, council, and certification expiration — no ethnicity field — and is members-only.

5. **Corporate Plus directory:** NMSDC publishes a small public-facing "Corporate Plus Member Directory" (`nmsdc.org/connect/corporate-plus-member-directory/`) listing a high-performing subset of MBEs. It is public, filterable by region and industry, but does **not** expose ethnicity. Useful for coverage checking but not for Black-specific extraction.

---

## Regional Affiliate Catalog

| Council | Region / States | Directory URL | Access type | Ethnicity exposed | Bulk export | How to obtain | Notes |
|---|---|---|---|---|---|---|---|
| **Western Regional MSDC (WRMSDC)** | Hawaii, Nevada, Northern California | `wrmsdc.org/certified-mbe/` | unknown | unknown | no | Scrape attempt or partnership request | Site loads but directory page content did not resolve clearly; certification page mentions NMSDC Central database. Contact: admin@wrmsdc.org |
| **Southern California MSDC (SCMSDC)** | Metro LA, Southern CA (excl. San Diego) | `scmsdc.org/mbe/` — login link to `thehub.nmsdc.org` | members_only | no (not on public pages) | no | Corporate/university membership | 1,300+ certified MBEs. All directory access routes to NMSDC Hub login. Public "ethnic minorities" page defines categories but no searchable directory. Contact: (213) 689-6960 |
| **Pacific Southwest MSDC (PSWMSDC)** | Arizona, Metro San Diego | `pswmsdc.org/mbe-opportunities/` | unknown | unknown | no | Partnership request | Arizona + San Diego County coverage. Page 404'd during research; likely routes to NMSDC Hub. Contact via Facebook/LinkedIn. |
| **Northwest Mountain MSDC (NWMMSDC)** | WA, AK, OR, MT, WY, ID, UT | `nwmmsdc.org/mbes/` | unknown | unknown | no | Partnership request | References NMSDC Central Vendor Management database. Does not appear to have an independent public directory. Contact: info@nwmmsdc.org |
| **Mountain Plains MSDC (MPMSDC)** | CO, KS, NE, Western MO | `mpmsdc.org/member-list/` — requires login | members_only | unknown | no | Corporate/university membership | Member list explicitly password-protected. ~500+ certified MBEs. Contact: mpmsdc.org |
| **Dallas/Fort Worth MSDC (DFWMSDC)** | Metro Dallas-Fort Worth, TX | `dfwmsdc.com/certification/` | unknown | unknown | no | Partnership request | Certification transitioned to NMSDC Hub Sept 2025. No public directory confirmed. Contact: 214-630-0747 |
| **Southwest MSDC (SMSDC)** | NM, OK, Southwestern TX | `smsdc.org/mbe-certification/` — routes to NMSDC Hub | members_only | unknown | no | Corporate/university membership | Routes entirely to `thehub.nmsdc.org`. No local public directory. Contact: (512) 386-8766 |
| **Houston MSDC (HMSDC)** | Houston metro, Beaumont, Corpus Christi, College Station, TX | `diversebusinessfinder.com/auth/login` | members_only | unknown | no | Corporate/university membership | Unique tool: "Diverse Business Finder" (not NMSDC Hub). Login required. ~919 area MBEs; Black businesses ~45% of that (est. 413). Certification transitioned to NMSDC Hub Sept 2025. Contact: info@hmsdc.org |
| **North Central MSDC (NCMSDC)** | MN, IA, WI, ND, SD | `northcentralmsdc.org/mbeprograms/` | unknown | unknown | no | Partnership request | References subscription services; no public directory confirmed. Contact: (612) 465-8881 |
| **Chicago MSDC (ChicagoMSDC)** | Metro Chicago, NW Indiana | `chicagomsdc.org/buydiverse/` | unknown | unknown | no | Partnership request | "Buy Diverse" page exists but login requirement not fully resolved. Certification now NMSDC Hub. Contact: info@chicagomsdc.org |
| **Southern Region MSDC (SRMSDC)** | AL, AR, LA, MS | `srmsdc.org` — tiered system (Registered/Listed/Certified) | public_searchable (partial) | unknown | unknown | Scrape attempt | Has both "Registered MBE" and "Listed MBE" pages at srmsdc.org — both returned 404 during research but are indexed by Google as of early 2023. Worth retry-scraping. Only NMSDC affiliate for AL, AR, LA, MS. Contact: info@srmsdc.org |
| **Mid-States MSDC** | IN (excl. NW), Central IL, Eastern MO | `midstatesmsdc.org` — SITE DOWN | none (site offline) | unknown | no | Wait/partnership request | Website offline for redesign as of research date. Previously covered ~500+ MBEs. Contact: info@midstatesmsdc.org |
| **TriState MSDC (TSMSDC)** | KY, TN, WV | `tsmsdc.net/mbe-certification/` — routes to NMSDC Hub | members_only | unknown | no | Corporate/university membership | Nashville-based, offices in Louisville and Charleston WV. NMSDC Hub is the database. Contact: info@tsmsdc.net |
| **Ohio MSDC (OMSDC)** | Ohio | `ohiomsdc.org/find-certified-mbes/` — routes to `nmsdc.org/nmsdc-central/` | members_only | unknown | no | Corporate/university membership | "Find Certified MBEs" page points to NMSDC Central (now NMSDC Hub). Login required. Contact: marketing@ohiomsdc.org |
| **Michigan MSDC (MMSDC)** | Michigan | `minoritysupplier.org` — routes to NMSDC Central ($500 access fee) | members_only | unknown | no | Corporate/university membership | NMSDC Central access requires completing a form + $500 regional usage fee for local corporate members. 1,200+ certified MBEs. MatchMaker365 also mentioned. Contact: (313) 873-3200 |
| **Georgia MSDC (GMSDC / Georgia Business Council)** | Georgia | `georgiacouncil.org/certification/` | unknown | unknown | no | Partnership request | Domain redirects from gmsdc.org to georgiacouncil.org. No public directory found. NMSDC Hub for active certs. Note: GA DOAS state MBE program restructured Jan 2024 (HB 128) — separate from NMSDC certs. Contact: info@georgiacouncil.org |
| **Florida State MSDC (FSMSDC)** | Florida | `fsmsdc.org/certified-mbe/` — 403 Forbidden | unknown | unknown | no | Partnership request | Certification fully migrated to NMSDC Hub July–Aug 2025. 403 on directory page during research. Contact via fsmsdc.org |
| **Puerto Rico MSDC (PRMSDC)** | Puerto Rico, US Virgin Islands | `prmsdc.org/mbe-showcase/` | public_searchable (partial) | unknown | unknown | Scrape attempt or partnership request | Has a public "MBE Showcase" and a portal at `portal.prmsdc.org`. Unclear if ethnicity is tagged per business. Also has PRMSDC HUB login. Worth investigating directly. Contact: +1-787-627-7272 |
| **Carolinas-Virginia MSDC (CVMSDC)** | NC, SC, Southern VA | `cvmsdc.org/mbe-services/` | members_only | unknown | no | Partnership request | References national MSDC supplier database for corporate buyers. No local public directory. HQ: Charlotte, NC. Contact: info@cvmsdc.org |
| **Capital Region MSDC (CRMSDC)** | DC, MD, Northern VA | `crmsdc.org/mbe-services/` — routes to NMSDC Hub | members_only | unknown | no | Corporate/university membership | References NMSDC Hub for directory access. Searchable profiles exist but require login. Overlaps with MD MBE program already in BBRT — dedup carefully. Contact: (301) 593-5860 |
| **Eastern MSDC (EMSDC)** | PA, Southern NJ, DE | `emsdc.org` — NMSDC Central (members-only) | members_only | yes (by certification standard, unclear if searchable) | unknown | Corporate/university membership | Explicitly offers "Corporate Directory with corporate buyer contact information" and "Diverse Yellow Pages" as MBE benefits. Certification criteria confirm ethnicity is tracked. Database via NMSDC Central (login required). Contact: info@emsdc.org |
| **New York & New Jersey MSDC (NYNJMSDC)** | New York, Northern NJ | `nynjmsdc.org/mbe-certification/` — routes to NMSDC Hub | members_only | unknown | no | Corporate/university membership | NMSDC has fully taken over certification from NYNJMSDC. All directory access through NMSDC Hub. Contact: 212-502-5663 |
| **Greater New England MSDC (GNEMSDC / NEBGC)** | CT, ME, MA, NH, RI, VT | `nebgc.org` (domain redirected from gnemsdc.org) | none | unknown | no | Not feasible at this time | As of May 2025, GNEMSDC ceased processing MBE certification — all cert/re-cert now handled directly by NMSDC national. Organization rebranded as New England Business Growth Collective (NEBGC). No MBE directory available. |

---

## NMSDC National Hub Summary

| Platform | URL | Access type | Ethnicity exposed | Bulk export | Notes |
|---|---|---|---|---|---|
| NMSDC Hub — MBE Search | `thehub.nmsdc.org` | members_only | yes (by cert standard; filter availability unconfirmed) | yes (for corporate members — "unlimited export") | 12,000+ MBEs. Primary national database. All affiliate certs now here. Ethnicity field exists in profiles but public filter unknown. |
| NMSDC Hub — Check-Mate | `thehub.nmsdc.org` | members_only | no | yes (matched list only) | Returns tax ID, company name, council, cert expiry only. No ethnicity. |
| NMSDC Corporate Plus Directory | `nmsdc.org/connect/corporate-plus-member-directory/` | public_searchable | no | unknown | ~small subset of high-capacity MBEs. Filters: region, industry, MBE type. No ethnicity filter. Public and potentially scrapeable but limited coverage and no ethnicity. |

---

## Top Opportunities

### Tier 1: Immediate targets (public or near-public with ethnicity potential)

1. **SRMSDC (Southern Region — AL, AR, LA, MS):** Has tiered public MBE pages (Registered MBE, Listed MBE) at srmsdc.org that were indexed publicly as of early 2023. Both pages returned 404 during this research sweep, but may be intermittently accessible or may require a direct contact/records request. This is the only NMSDC source covering AL, AR, and MS — states not in the BBRT pipeline. **Action:** Retry srspdc.org pages; if down, email info@srmsdc.org requesting a copy of the Registered/Listed MBE roster and ask whether ethnicity is included.

2. **Puerto Rico MSDC (PRMSDC — PR, USVI):** Has a public "MBE Showcase" page and a separate portal at portal.prmsdc.org. Ethnicity is part of NMSDC certification standard, and PRMSDC is a small-enough council that a direct records/partnership request has good odds of success. **Action:** Fetch `prmsdc.org/mbe-showcase/` and `portal.prmsdc.org` to assess what's exposed before making a records request.

3. **NMSDC Corporate Plus Directory (national):** At `nmsdc.org/connect/corporate-plus-member-directory/` — public, no login required, filterable by region and industry. Does not expose ethnicity, but could be scraped to identify a high-capability MBE universe for cross-referencing with other ethnicity sources (e.g., SBA 8(a)). Low priority unless you need a capability index, not an ethnicity-filtered list.

### Tier 2: Corporate/university membership or partnership (highest ROI if you get access)

4. **NMSDC Hub — Corporate Membership:** A single NMSDC corporate membership gives access to the full 12,000+ MBE national database with ethnicity fields and unlimited export. University of Michigan may already hold corporate membership (large research universities often join NMSDC). **Action: Check whether UM Ross, UM Procurement, or UM Office of Supplier Diversity holds an active NMSDC corporate membership.** If yes, this is the cleanest single source for a national Black-MBE extract. If no, the membership cost should be weighed against the research value — a university membership may be structured differently than a standard corporate membership.

5. **Houston MSDC (HMSDC — Houston area TX):** Unique platform (Diverse Business Finder) separate from the NMSDC Hub. ~919 MBEs in the Houston region, ~45% estimated to be Black-owned (~413 businesses). If a records/partnership request is made, Houston is a high-density Black business market worth prioritizing. **Action:** Contact info@hmsdc.org directly, describe the research mission, and request the Black/African-American certified MBE roster.

### Tier 3: Records/partnership request (requires relationship)

6. **CRMSDC (DC, MD, Northern VA):** Overlaps heavily with the Maryland MBE program already in BBRT (~5,400 MD confirmed_black records). CRMSDC adds DC and Northern VA coverage not in the current pipeline. **Action:** After exhausting NMSDC Hub access question, contact (301) 593-5860 for a research partnership.

7. **EMSDC (PA, Southern NJ, DE):** Explicitly provides a "Corporate Directory" and "Diverse Yellow Pages" as MBE benefits; ethnicity is tracked per certification standard. PA and DE are not currently in BBRT. **Action:** Contact info@emsdc.org for a research data request; frame as longitudinal panel data for academic research on minority business survival.

### Not feasible / low priority

- **GNEMSDC/NEBGC (New England):** Organization has ceased MBE certification as of May 2025 — no active directory.
- **Mid-States MSDC:** Website offline for redesign as of June 2026.
- **Most NMSDC Hub-only councils without independent directories (SCMSDC, NWMMSDC, MPMSDC, DFWMSDC, SMSDC, TSMSDC, OMSDC, MMSDC, FSMSDC, CVMSDC, NYNJMSDC):** All route to NMSDC Hub members-only. Access strategy is the same for all of them: NMSDC corporate/university membership or a direct national records request to NMSDC HQ (certification@nmsdc.org).

---

*Research conducted June 2026. Live pages verified via WebFetch where possible; some pages returned 404/403 or redirected. Access levels for NMSDC Hub-based councils are inferred from platform documentation — actual ethnicity filter availability in the Hub UI should be verified before purchasing membership.*
