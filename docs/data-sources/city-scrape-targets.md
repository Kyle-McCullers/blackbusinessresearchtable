# City-Level Minority Business Directory — Scrape Target Assessment

**Purpose:** Identify which large U.S. cities have a municipal DBE/MBE/WBE certification directory that can be scraped for Black/African American-owned businesses, using the same approach as the existing Los Angeles City adapter.

**Date assessed:** 2026-06-13

---

## The LA Pattern (Target Infrastructure)

Los Angeles' Bureau of Contract Administration at `https://bca.lacity.gov/dbe-mbe-wbe-directory` is the gold standard for our purposes. What makes it scrapeable:

1. **Server-rendered HTML** — pages return full content to `requests.get()`, no JavaScript execution needed
2. **Stable ID-based detail URLs** — each company has a page at `https://bca.lacity.gov/dbe-company/{id}` (also accessible as `?id={n}`)
3. **Ethnicity field on detail pages** — the per-company page explicitly states the owner's ethnicity (e.g., "African American"), not just a certification type
4. **No login required** — fully public, no session/CAPTCHA wall on individual detail pages
5. **Iterable** — IDs are sequential integers, so the full corpus can be crawled by iterating IDs

This combination lets us: iterate IDs → fetch each detail page → parse ethnicity → keep Black/African American firms. **The ethnicity field is the critical differentiator.** Many directories expose an "MBE" flag but not the specific race of the owner, making them useless for our purpose.

---

## Platform Taxonomy

| Platform | Notes | Ethnicity exposed? |
|---|---|---|
| **B2Gnow / mwdbe.com** | Most common. Search form returns HTML results, but per-firm URLs appear session-bound (POST results). Ethnicity filter exists on search form but ethnicity is NOT shown on public detail pages. Cannot iterate by ID. | Filter only, not on detail pages |
| **B2Gnow / gob2g.com** | Rebranded B2Gnow. Same behavior as mwdbe.com. JavaScript required for the interactive portions. | Same as above |
| **B2Gnow / diversitycompliance.com** | Another B2Gnow white-label. Charlotte, Baltimore, Milwaukee, Columbus, Cook County, Phoenix (SBE). Baltimore uniquely exposes an ethnicity filter with "Black American" option — but still session-bound results. | Filter only |
| **Socrata open data** | Best alternative to LA pattern. Boston, New Orleans, Detroit (ArcGIS). Downloadable CSV/API. No scraping needed. | Varies by dataset |
| **ArcGIS Hub** | Detroit uses this. Downloadable dataset. | Check field list |
| **Custom city site (LA-like)** | Rare. LA City BCA is the exemplar. | Yes (on detail pages) |
| **Salesforce Sites** | DC DSLBD. Server-rendered table of 1,970 CBEs. No ethnicity field. | No |
| **gob2g.com** | Kansas City, Miami-Dade, Tampa. Same as B2Gnow. | No |
| **California UCP (californiaucp.dbesystem.com)** | State-level, covers San Diego, Sacramento, San Jose via state program. Already assessed — no city-specific ethnicity field. | No |
| **None / state-only** | Cities that rely entirely on state programs (Oregon COBID for Portland, Washington OMWBE for Seattle). | Varies |

---

## Full Assessment Table

| City / Jurisdiction | Directory URL | Platform | Rendering | Detail pages? | Ethnicity exposed? | Scrape verdict | Est. firms | Notes |
|---|---|---|---|---|---|---|---|---|
| **Los Angeles (City)** | https://bca.lacity.gov/dbe-mbe-wbe-directory | Custom city site | Server-rendered HTML | Yes — `?id={n}` | **Yes — on detail page** | **LA-like — clean scrape** | ~2,500+ | Gold standard. IDs iterate sequentially. Already the model adapter. |
| **Houston** | https://houston.mwdbe.com/FrontEnd/searchcertifieddirectory.asp | B2Gnow / mwdbe.com | Server-rendered with JS enhancements | Unknown — results appear POST-bound | **Ethnicity filter on search form** (Asian/Black/Caucasian/Hispanic/Native American/Other) but unclear if on detail pages | Scrapeable but problematic — ethnicity on search form, not confirmed on stable detail pages | ~3,000+ MBE/WBE/SBE | Has explicit "Black" filter in ethnicity dropdown. If results expose stable per-firm URLs, this is high-value. The search form itself is server-rendered ASP. |
| **Chicago** | https://chicago.mwdbe.com/FrontEnd/SearchCertifiedDirectory.asp | B2Gnow / mwdbe.com | Server-rendered with JS enhancements | Unknown — results appear POST-bound | **Ethnicity filter on search form** (African American/Asian American/Caucasian/Hispanic-Latino/Native American/Other/N/A) | Scrapeable but problematic — same as Houston | Large (whole directory downloadable to Excel) | Excel bulk download may be best approach. Ethnicity confirmed in search filter. |
| **Philadelphia** | https://opendataphilly.org/datasets/oeo-registry-of-certified-minoritywomendisable-owned-business-enterprises/ | Socrata/Open Data Philly | Open data (CSV/API) | N/A — bulk dataset | **Unknown — field list not confirmed; dataset exists** | **Has open-data/API instead (no scrape needed)** — check for race field | ~4,000+ | OpenDataPhilly hosts the OEO Registry as a downloadable CSV. Must verify if race/ethnicity column is present. |
| **Phoenix** | https://phoenix.gob2g.com/FrontEnd/SearchCertifiedDirectory.asp | B2Gnow / gob2g.com | JS-required | Unknown | No ethnicity filter (SBE has no ethnic requirement; DBE is through AZ UCP at azdbe.azdot.gov) | No ethnicity exposed (not useful) | Unknown | SBE program = no race requirement. DBE is state-certified via AZ DOT. |
| **San Antonio** | N/A — relies on SCTRCA (South Central TX Regional Cert Agency) | External agency | — | — | Unknown | No city-specific directory found | — | City defers to SCTRCA for DBE and Bexar County for SBE/MBE. Bexar uses B2Gnow (bexar.sbdbe.com). No ethnicity filter confirmed. |
| **Dallas** | Defers to NCTRCA / DFW MBC | External agencies | — | — | Unknown | No city-specific directory found | — | City of Dallas accepts NCTRCA, DFW Minority Business Council, Women's Business Council-SW certifications. No city-run directory found. |
| **San Diego** | https://californiaucp.dbesystem.com/ | California UCP (state) | JS | — | No | No ethnicity exposed (not useful) | — | City uses California statewide UCP. Race not exposed in public directory. |
| **San Jose** | No city-specific directory found | — | — | — | — | No city directory identified | — | No dedicated city MBE directory. Falls under California state programs. |
| **Austin** | https://austintexas.mwdbe.com/FrontEnd/SearchCertifiedDirectory.asp | B2Gnow / mwdbe.com | Server-rendered with JS | Unknown | No ethnicity filter confirmed (search form does not show ethnicity filter) | No ethnicity exposed (not useful) | Unknown | SMBR certifies MBE/WBE/DBE but the directory search form does not expose ethnicity filter. Different from Houston/Chicago. |
| **Atlanta** | https://atlantaga.gob2g.com/ | B2Gnow / gob2g.com | JS-required | Unknown | Unknown — but cert categories include AABE (African American Business Enterprise) explicitly | Scrapeable but JS (headless) — if AABE searchable | Unknown | OCC certifies by race category (AABE, APABE, HABE, NABE, FBE). If the public vendor search allows filtering by AABE cert type, it may be extractable via headless browser. Worth investigating. |
| **Detroit** | https://data-detroitmi.hub.arcgis.com/datasets/detroit-business-certification-register/about | ArcGIS Hub open data | Open data (CSV/JSON/GeoJSON) | N/A — bulk dataset | **Unknown — field list not confirmed; ArcGIS fields must be checked** | **Has open-data/API instead (no scrape needed)** — check for race field | ~500–1,000 | ArcGIS Hub dataset. DBOP certifies MBE/WBE/DBE. Check if certification type encodes race or if a separate ethnicity field exists. |
| **Charlotte** | https://charlotte.diversitycompliance.com/ | B2Gnow / diversitycompliance.com | JS-required | Unknown | No (B2Gnow doesn't expose ethnicity on public detail pages) | No ethnicity exposed (not useful) | Unknown | Confirmed B2Gnow. Public vendor search does not expose race/ethnicity per firm. |
| **Columbus** | https://columbus.diversitycompliance.com/ → redirects to https://columbuscompliance.gob2g.com/ | B2Gnow / gob2g.com | JS-required | Unknown | No | No ethnicity exposed (not useful) | Unknown | Confirmed B2Gnow. JS-required. No ethnicity filter visible. |
| **San Francisco** | https://supplier.sfcitypartner.sfgov.org/pages/LBESearch/supplier-search.aspx | Custom ASP.NET | JS (403 on direct fetch) | Unknown | Unknown — LBE is location-based, not race-based | No ethnicity exposed (not useful) | ~1,400+ | LBE certification is based on SF business location, not race/ethnicity. Not useful for Black business identification. |
| **Seattle** | https://omwbe.diversitycompliance.com/FrontEnd/searchcertifieddirectory.asp (state OMWBE) | B2Gnow / diversitycompliance.com | Server-rendered + JS | Unknown | No ethnicity filter (certifications are MBE/WBE/MWBE/LGBTQBE without race breakdown) | No ethnicity exposed (not useful) | — | City uses Washington State OMWBE directory. No per-firm ethnicity. Seattle self-ID directory (web6.seattle.gov) does not expose race. |
| **Denver** | https://denver.mwdbe.com/FrontEnd/SearchCertifiedDirectory.asp?tn=denver | B2Gnow / mwdbe.com | Server-rendered + JS | Unknown | No ethnicity filter confirmed on search form (MWBE/SBE/EBE types only) | No ethnicity exposed (not useful) | Unknown | DSBO certifies MWBE, SBE, EBE, SBEC, DBE, ACDBE but directory search does not expose ethnicity filter. |
| **Boston** | https://data.boston.gov/dataset/certified-business-directory | Socrata / Analyze Boston | Open data (CSV/JSON/API/PowerBI) | N/A — bulk dataset | **Unknown — MBE flag present but individual race column not confirmed** | **Has open-data/API instead (no scrape needed)** — check for race field | ~938 certified firms (38% MBE) | Socrata platform. CSV downloadable. MBE certification status is in the data; verify if a race/ethnicity column distinguishes African American from other minorities. |
| **Baltimore** | https://baltimorecity.diversitycompliance.com/FrontEnd/searchcertifieddirectory.asp | B2Gnow / diversitycompliance.com | Server-rendered + JS | Unknown | **Yes — ethnicity filter includes "Black American"** as a search option | Scrapeable but JS (headless) — ethnicity filter confirmed | Unknown | Unusual: Baltimore's B2Gnow implementation explicitly exposes an ethnicity filter with "Black American" option. Results may still be session-bound. Headless browser approach may work to filter + extract. |
| **Memphis** | https://memphis.mwsbe.com/ | B2Gnow / mwsbe.com variant | Server-rendered + JS | Unknown | Unknown | No ethnicity exposed (not useful) — assumed B2Gnow pattern | Unknown | URL returns 404 on direct fetch. City uses mwsbe.com variant. B2Gnow pattern assumed. |
| **Washington DC** | https://dcdslbd.my.salesforce-sites.com/public | Salesforce Sites | Server-rendered table (1,970 CBEs) | Links appear JS-driven (href="#") | **No — CBE categories are economic/status designations, not racial** | No ethnicity exposed (not useful) | ~1,970 CBEs | Salesforce Sites portal. No ethnicity field. CBE certification does not break out by race. DCUCP (federal DBE) directory is separate. |
| **Cook County, IL** | https://cookcounty.diversitycompliance.com/ | B2Gnow / diversitycompliance.com | JS-required | Unknown | Unknown | No ethnicity exposed (not useful) | Unknown | Confirmed B2Gnow. Cook County and Chicago have reciprocal programs. Use Chicago directory for combined coverage. |
| **LA County** | https://iddweb.isd.lacounty.gov/DCA_eComplaint/SmallBusinessCertifications | Custom county site | Unknown | Unknown | Unknown — CBE is LA County's program (not race-specific) | Unknown — needs investigation | Unknown | LA County CBE program is separate from LA City BCA. CBE may not require race classification. Investigation needed. |
| **Miami-Dade County** | https://mdcsbd.gob2g.com/frontend/searchcertifieddirectory.asp | B2Gnow / gob2g.com | Server-rendered + JS | Unknown | **No ethnicity filter** — searches by certification type, commodity, tier only | No ethnicity exposed (not useful) | Unknown | Confirmed gob2g.com (B2Gnow). No ethnicity or race filter. reCAPTCHA required. |
| **Minneapolis** | No city-specific directory found | — | — | — | — | No city directory identified | — | City relies on CERT program (joint with St. Paul/Hennepin/Ramsey counties). CERT directory exists but hosted externally. No confirmed ethnicity field. |
| **Portland, OR** | https://www.oregon.gov/biz/programs/cobid/pages/default.aspx (state COBID) | Oregon state portal | — | — | Unknown | No city directory — state program only | — | Portland does not run its own certification. Uses Oregon COBID (state). COBID certifies MBE/WBE/ESB/DBE — check if MBE records expose race breakdown. |
| **Las Vegas** | No city-specific directory found | — | — | — | — | No city directory identified | — | Supplier Diversity Program exists but no public searchable directory with ethnicity found. |
| **Nashville** | https://memphis.mwsbe.com/ (Nashville uses similar system) | B2Gnow variant | — | — | Unknown | No ethnicity exposed (not useful) | — | Nashville Metro Government EBO program exists. Directory may be at nashville.mwdbe.com or similar. No confirmed URL. |
| **Jacksonville** | https://www.jacksonville.gov/departments/jacksonville-small-emerging-business.aspx | City website (JSEB program) | Unknown | Unknown | Unknown | No ethnicity exposed (not useful) — JSEB is size-based | Unknown | Jacksonville Small and Emerging Business (JSEB) is a size-based program, not race-based. Not useful for Black business identification. |
| **Kansas City** | https://kcmohrd.gob2g.com/ (redirected from kcmohrd.mwdbe.com) | B2Gnow / gob2g.com | JS-required | Unknown | Unknown — certifications are MBE/WBE/SLBE types | No ethnicity exposed (not useful) | Unknown | Confirmed B2Gnow. Public vendor search available. No ethnicity filter confirmed. |
| **Cleveland** | Likely Cuyahoga County: https://cuyahogacounty.diversitycompliance.com/ | B2Gnow / diversitycompliance.com | JS-required | Unknown | Unknown | No ethnicity exposed (not useful) | Unknown | No dedicated Cleveland city directory found. Cuyahoga County uses B2Gnow. |
| **Pittsburgh** | https://procurement.pittsburghpa.gov/beacon/ | Custom Beacon procurement system | Unknown | Unknown | Unknown | Unknown — needs investigation | Unknown | City working to publish comprehensive certified vendor list by end of Q2 2025. Beacon system may now have this. |
| **St. Louis** | https://www.flystl.com/civil-rights/business/business-diversity-development-1/ (airport DBE) | External / airport | — | — | Unknown | No city directory found | — | City M/WBE program recently resumed after pause. No public searchable directory confirmed. |
| **New Orleans** | https://data.nola.gov/Economy-and-Workforce/Certified-Disadvantaged-Business-Enterprise-DBE-Di/q42h-ptn2 | Socrata open data | Open data (CSV download confirmed) | N/A — bulk dataset | **No — no ethnicity/race field in the CSV** (columns: Company Name, DBA, Owner, Address, Phone, Email, Agency, Certification Type, Certified date, Capability, Service Type, Certifying Agency, Commodity Codes, Website) | No ethnicity exposed (not useful) — despite being open data | ~hundreds of SLDBEs | Socrata portal confirmed. CSV verified: 17 columns, **no ethnicity or race field**. Certification type is "SLDBE" (size-based), not race-coded. Data is also stale (last updated Nov 2019). |
| **Oakland** | https://apps.oaklandca.gov/contractcompliance/Contractors.aspx | Custom city ASP.NET | Unknown (404 on fetch) | Unknown | Unknown | Unknown — needs investigation | Unknown | Oakland LBE/SLBE is location-based, not race-based. City also has Contract Compliance division. |
| **Tampa** | https://tampa.gob2g.com/ | B2Gnow / gob2g.com | JS-required | Unknown | No ethnicity filter (WMBE and SLBE certifications) | No ethnicity exposed (not useful) | Unknown | Confirmed B2Gnow. WMBE certification doesn't break out by specific race. |
| **Milwaukee** | https://mke.diversitycompliance.com/FrontEnd/searchcertifieddirectory.asp | B2Gnow / diversitycompliance.com | Server-rendered + JS | Unknown | **No ethnicity filter** — only DBE/ACDBE and size-based certs | No ethnicity exposed (not useful) | Unknown | Milwaukee County directory. Cert types are DBE (federal) and size-based. No race breakdown. |
| **Tucson** | No city-specific directory found | — | — | — | — | No city directory identified | — | No dedicated Tucson MBE/DBE city directory found. Likely uses AZ UCP (state). |
| **Sacramento** | No city-specific directory found | — | — | — | — | No city directory identified | — | No sacramento.mwdbe.com or city-specific directory found. Likely uses California state programs (DGS/CUCP). |
| **Albuquerque** | https://www.cabq.gov/transit/our-department/disadvantaged-business-enterprise | City DBE program (transit only) | — | — | Unknown | No city directory found | — | ABQ RIDE has a DBE program for transit contracts. No comprehensive city MBE directory with ethnicity found. |
| **Fort Worth** | Defers to TxDOT/NCTRCA/DFW MBC | External agencies | — | — | Unknown | No city-specific directory found | — | Fort Worth relies on TxDOT DBE directory (state) and NCTRCA. No city-run directory. |

---

## Platform Frequency Summary

| Platform | Cities Using It |
|---|---|
| B2Gnow (mwdbe.com / gob2g.com / diversitycompliance.com) | Houston, Chicago, Austin, Denver, Kansas City, Baltimore, Charlotte, Columbus, Cook County, Miami-Dade, Tampa, Milwaukee, Atlanta, Cleveland (Cuyahoga), Phoenix (SBE), Memphis, Nashville |
| Open Data / Socrata / ArcGIS | Boston, New Orleans, Detroit, Philadelphia (OpenDataPhilly) |
| Custom city site (LA-like) | Los Angeles (City) |
| Salesforce Sites | Washington DC |
| State-program-only (no city directory) | San Diego (CA UCP), Portland (COBID), Seattle (WA OMWBE), San Jose, Sacramento, Tucson, Fort Worth, San Antonio (SCTRCA), Dallas (NCTRCA) |
| Not found / unclear | Las Vegas, Minneapolis, Nashville, Pittsburgh, St. Louis, Oakland, Albuquerque, Jacksonville (size-based) |

---

## Key Finding on B2Gnow Platforms

The B2Gnow/mwdbe.com/gob2g.com/diversitycompliance.com family is used by roughly **half of large U.S. cities**. The critical limitation: while many of these platforms have an ethnicity filter on the search form, the **results appear to be POST-session-bound** — they do not return stable per-firm URLs that can be iterated. Individual vendor profile pages (if they exist) are likely accessible only after a reCAPTCHA-gated POST request, making clean scraping difficult.

**Exception:** Houston and Chicago's mwdbe.com implementations have confirmed ethnicity filters (including "Black/African American") in the search form. If their result pages expose per-firm detail URLs, these could be scraped via headless browser. The Excel bulk-download option on both sites may be the more practical approach.

**Exception:** Baltimore's diversitycompliance.com instance confirms an ethnicity filter with "Black American" as an explicit option. This is unusual for the platform family.

---

## Ranked Best Scrape Targets

### Tier 1: LA-Like (Custom Site + Ethnicity on Detail Pages)

| Rank | City | Rationale |
|---|---|---|
| 1 | **Los Angeles (City)** | Already built. The exemplar. Server-rendered HTML, stable `?id={n}` URLs, ethnicity on detail page. |
| 2 | **LA County** | Separate from LA City BCA. CBE program at `iddweb.isd.lacounty.gov`. Needs investigation — could be another custom site. High priority given size. |
| 3 | **Pittsburgh** | Beacon system may now expose a comprehensive certified vendor list (goal stated for Q2 2025). Custom system, potentially scrapeable. Investigate. |
| 4 | **Oakland** | Custom ASP.NET at `apps.oaklandca.gov/contractcompliance/Contractors.aspx` — returned 404 but city is updating its BIMS system. May have ethnicity if it certifies by race category. |

### Tier 2: Scrapeable with Headless Browser (B2Gnow + Ethnicity Filter)

| Rank | City | Rationale |
|---|---|---|
| 5 | **Houston** | Confirmed "Black" option in ethnicity filter on search form. Server-rendered ASP. If results expose per-firm URLs, this yields ~hundreds of Black MBEs. Headless browser + reCAPTCHA solver OR bulk Excel download. Est. ~800–1,200 Black MBEs. |
| 6 | **Chicago** | Same platform, same "African American" ethnicity filter. Bulk download to Excel is the practical path. Large directory. Est. ~500–1,000 Black MBEs. |
| 7 | **Baltimore** | B2Gnow but confirmed "Black American" ethnicity filter — unusual. May allow headless filtering. Smaller market but high Black business concentration. |
| 8 | **Atlanta** | B2Gnow (gob2g.com) with explicit AABE (African American Business Enterprise) certification category. If AABE is filterable in the public vendor search, this is high-value — Atlanta is a major Black business hub. |

### Tier 3: Open Data / API (No Scraping Needed — Check Ethnicity Field)

| Rank | City | Rationale |
|---|---|---|
| 9 | **Philadelphia** | OpenDataPhilly hosts the OEO Registry as downloadable CSV. **Must verify if race/ethnicity column is present.** If yes, this is an easy API pull — no scraping required. |
| 10 | **Detroit** | ArcGIS Hub open data. DBOP certifies MBE/WBE/DBE. **Must verify if MBE certification type encodes race or if a separate race field exists.** If yes, easy download. |
| 11 | **Boston** | Socrata open data. ~938 firms, 38% MBE. **Must verify if an ethnicity/race column exists in the CSV beyond the "MBE" flag.** If race is not broken out, the MBE flag alone is insufficient (Maryland pattern). |

### Tier 4: No Ethnicity Exposed (Not Useful for BBRT)

The following use B2Gnow without ethnicity filters, or have size/location-based programs that don't classify by race:
- Charlotte, Columbus, Cook County, Miami-Dade, Tampa, Milwaukee, Denver, Austin, Kansas City, Phoenix (SBE), San Francisco (LBE), Seattle (OMWBE), Washington DC (CBE), New Orleans (SLDBE — confirmed no race field), Jacksonville (JSEB)

### Tier 5: No City Directory Found — State Programs Only

- San Diego, Portland, Seattle (city-level), San Jose, Sacramento, Tucson, Fort Worth, San Antonio, Dallas, Las Vegas, Minneapolis, Nashville, Albuquerque

---

## Recommended Next Steps

1. **Investigate Tier 3 cities first** (Philadelphia, Detroit, Boston) — open data means zero scraping complexity. Just verify the ethnicity field in the CSV schema.
2. **Explore Houston and Chicago bulk Excel downloads** from mwdbe.com — these require manual download but no scraping infrastructure changes.
3. **Build a headless-browser prototype for Atlanta** — AABE is an explicit certification category that may be filterable, yielding a major Black business hub.
4. **Investigate LA County** — given proximity to the existing LA City adapter and potentially similar infrastructure.
5. **Avoid building adapters for B2Gnow portals without ethnicity filters** — the 15+ cities in this category (Charlotte, Columbus, Denver, etc.) do not expose race and cannot yield confirmed_black records without additional data.

---

*Sources checked: live site fetches plus web searches as of 2026-06-13. Platform classifications based on direct page inspection where accessible, search results otherwise.*
