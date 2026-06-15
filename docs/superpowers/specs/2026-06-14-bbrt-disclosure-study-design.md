# BBRT as a Disclosure-Rate Research Instrument — Design Spec

**Status:** APPROVED (Kyle, 2026-06-15, after incorporating section-by-section review)
**Date:** 2026-06-14 · **Revised:** 2026-06-15
**Phase plan:** 3 → 2 → 1 (this doc = phase 3 design; then disclosure matching; then uncertified ingestion)

---

## 1. Purpose & research context

BBRT is being extended from a coverage database into a **research instrument for Kyle's
dissertation on strategic identity disclosure**. The question: **among businesses known to
be Black-owned, what fraction publicly disclose that identity, and what predicts
disclosure?** BBRT is the **denominator** (known Black-owned firms); an external **disclosure
signal** (the public "Black-owned" attribute on Google, later possibly Yelp) is the
**dependent variable**.

### 1.1 Public vs. private split (decided)
The database has two faces:
- **PUBLIC (blackbusinessresearchtable.com + downloads):** the denominator — business
  identity, location, geography, and the **identification tier(s)**. Downloadable as **CSV**
  (and Parquet/Stata — §8).
- **PRIVATE (Kyle-only, not published):** the **disclosure variable(s)** (which Kyle
  computes), the **intersectional identity flags** (women/veteran/LGBTQ from the Google
  data), and any contact info. These live in the same DuckDB file but in private columns
  excluded from the public CSV export.

The full main database **is** downloadable to CSV — that capability is a requirement (the
public export already produces `businesses.csv`; we add a private-columns-excluded export
rule so the public CSV never leaks the disclosure layer).

---

## 2. Core design decisions (summary)

1. Replace `confidence` with a **multi-basis identification** scheme (a business can be more
   than one): boolean `is_certified` / `is_self_identified` / `is_media_identified` + a
   derived primary `identification` + an `identification_sources` list (§3).
2. The disclosure DV is captured by **matching BBRT against external discloser lists**
   (Google "Black-owned" attribute; later Yelp) — that attribute is **not** in any public
   API (§4.1), so it must come from a dataset or human coding.
3. The Google discloser list is re-acquired from the **Google Local Review Data (2021),
   UCSD** at source — Kyle **owns the full acquisition cycle** (§4.2).
4. Matching is automated (fuzzy name + address + **lat/long geo distance**); RA coding is a
   long-horizon **validation** pass (§4.3).
5. **All variables/labels must be defined in the codebook** (`docs/codebook.md`) — this is a
   hard requirement for research-readiness (§7).

### 2.1 Prior work — Sharma, Frake & Watson (2025), *Marketing Science*
"Symbolic vs. Substantive Support: The Impact of BLM on Black-Owned Businesses"
(doi:10.1287/mksc.2023.0243). **What they did:** a difference-in-differences study around
George Floyd's murder using **Yelp's "Black-owned" badge** (launched 2020-06-18; measured as
of Nov 2020) as the Black-ownership marker — with a robustness measure from reviews
mentioning "Black-owned" — for **16,896 restaurants across 14 US cities**, plus **SafeGraph**
revenue/foot-traffic for substantive outcomes. Yelp seeded the badge via **myblackreceipt.com**.
Their focus is consumer *support response*; Kyle's is the *disclosure rate* — complementary,
not overlapping. **Their data + online appendices are at the article DOI** (INFORMS
"Supplemental Material" link): https://doi.org/10.1287/mksc.2023.0243 — worth pulling for
their Yelp-badge construction. NOTE: the file Justin Frake gave Kyle is the **Google/UCSD**
extract (§4.2), a different source than this Yelp paper.

---

## 3. The identification variable (multi-basis denominator stratifier)

Replaces the binary `confidence`. **A business can hold more than one basis** (e.g.,
certified by a state AND self-identified on Google). Captured as booleans plus provenance:

| Field | Meaning |
|---|---|
| `is_certified` | On a government certification list with an explicit Black/African-American field. The only confirmation accepted. |
| `is_self_identified` | The business identifies *itself* as Black-owned (Google "Black-owned" attribute; self-registration directories like blackownedeverything.co, 15% Pledge, myblackreceipt). |
| `is_media_identified` | A third party (journalist/blogger) named it Black-owned (BLM-era listicles). |
| `identification` | Derived **primary** label for simple stratification = the strongest basis present (certified > self > media). |
| `identification_sources` | JSON list of every source that contributed a basis (program/directory/article + url + date). |

This lets analyses compute disclosure rates **within and across bases** and answer overlap
questions ("of certified firms, how many also self-identify on Google?"). `confirmed_black`
is retired in favor of `is_certified` / `identification = certified`.

### 3.1 Routing of non-ethnicity MBE data (decided)
`mbe_unverified` (minority cert with no ethnicity breakdown) is **not** in the public panel.
It continues to route to the **private `mbe_frame.duckdb` sampling frame**, so the statewide
MBE downloads are not wasted — these become an RA-reviewable pool that can be promoted into
BBRT later (e.g., after RA ascertainment of Black ownership). Documented as a feeder, not a
tier.

### 3.2 Self-identified sources — provenance to verify per source
For each self/media source we record *how a business gets listed* (so the tier is honest):
- **blackownedeverything.co** (Zerina Akers): **self-registration** (owners submit) →
  `self_identified`. Caveat: the earliest entries grew from a curated Instagram feed
  (closer to `media_identified`) — flag the cohort if the site distinguishes them.
- **15% Pledge "Business Equity Community"** (1,200+, built with Google): listing process to
  confirm (contact info@15pp.org).
- **myblackreceipt.com** (Yelp's seed partner): consumer-uploaded receipts → owner presence;
  process to confirm.
- **BLM-era listicles**: third-party → `media_identified`.

**ACTION:** before ingesting any self/media source, document its add-mechanism in the
codebook; if owners must apply/register, it's `self_identified`, if a third party compiled
it, it's `media_identified`.

---

## 4. The disclosure layer (dependent variable — PRIVATE)

### 4.1 Why not the Google Places API
The "Black-owned" badge is a **Google Business Profile attribute the owner sets**; it shows
in Maps/Search "Highlights" but is **not returned by the public Places API** for third-party
businesses (confirmed; it lives in the owner-side Business Profile API). So disclosure is
captured by (a) matching against a dataset that recorded the attribute, or (b) human/RA
coding. We use (a) primary, (b) validation.

### 4.2 Disclosure data source — Google Local Review Data (2021), UCSD
- **Dataset:** Google Local Review Data (2021); Tianyang Zhang & Jiacheng Li (UC San Diego).
  ~4.96M US businesses, per-state, **gzipped JSON, one record per line**.
- **Metadata fields:** `name`, `address`, **`gmap_id`**, `latitude`, `longitude`, `category`,
  `avg_rating`, `num_of_reviews`, `price`, `hours`, **`MISC`** (attribute dict — incl. identity
  "Highlights"), `url`, `relative_results`, `state`.
- **Disclosure signal** = a Black-owned highlight string inside `MISC` (e.g. "Identifies as
  Black-owned"). **ACQUISITION TODO:** confirm the exact `MISC` key + value; reproduce Justin
  Frake's ~14k count as a validation check.
- **Provenance (decided):** re-acquire **all US states** directly from the UCSD repository and
  extract **all** businesses that identify as Black-owned, with a versioned extraction script
  + data statement. Justin's CSV is the interim input and cross-check target. Cite the UCSD
  authors (UCTopic / Personalized Showcases papers).
- **Snapshot:** September 2021 (cross-sectional).

### 4.3 The 14k Google disclosers are *also added to BBRT* (decided — with care)
The Black-owned-tagged Google businesses are **ingested into BBRT**, fully **deduplicated**
against existing rows (reuse the entity resolver). On match:
- **Already in BBRT (e.g., certified):** set `is_self_identified = true` *in addition to* its
  existing basis → the business now reads e.g. `certified` + `self_identified`. (This is why
  identification is multi-basis — §3.)
- **Not in BBRT and not certified:** **add** as `is_self_identified = true`, source = "Google
  Maps Black-owned attribute (UCSD Google Local 2021)".

⚠️ **Circularity guardrail (critical for the dissertation):** the Google tag serves *double
duty* — it both (a) **adds** self-identified businesses and (b) **is** the disclosure signal.
A business added *because* it had the tag trivially "discloses," so a disclosure rate computed
over self-identified-via-Google firms is ~100% by construction. **The clean, defensible
disclosure rate is computed within the `certified` denominator** (independent ground truth):
*of government-certified Black-owned firms, what share also set the Google Black-owned tag?*
This must be stated explicitly in the codebook and any paper; the disclosure flag records
which source established it so circular cases can be excluded from rate calculations.

### 4.4 Disclosure variable definitions (PRIVATE columns)
- `google_gmap_id`, `google_match_status` (`matched`|`ambiguous`|`no_profile`),
  `google_match_score`, `google_match_date`.
- **`discloses_black_google`** (DV): `true` if the matched gmap_id carried the Black-owned
  attribute (2021); `false` if matched without it; `null` if not measurable.
- Provenance: `disclosure_source`, `disclosure_observed_date` (2021-09), `disclosure_coded_by`
  (algorithm | RA), `disclosure_evidence_url`.
- `discloses_black_any` reserved for when Yelp/website channels join.

### 4.5 Other identity attributes — intersectionality (PRIVATE)
The UCSD `MISC` also carries **women-led**, **veteran-led**, **LGBTQ+**, etc. For every
matched/added business, capture these too: `identity_women`, `identity_veteran`,
`identity_lgbtq` (+ raw `google_misc` JSON). This enables intersectional analysis (Black ×
women-owned, Black × veteran-led, Black × women × veteran). Stored **private** for now;
public exposure decided later. (These fall outside BBRT's public Black-business scope but are
valuable in the same file.)

### 4.6 RA coding (decided — long-horizon validation, not a one-shot sample)
RA coding of Maps profiles is a **slow-burn over the whole matched list**, accumulating across
time (likely continuing into Kyle's first assistant-professor years), used to **validate and
correct** the automated match + disclosure flags rather than to capture them from scratch.
Match thresholds and a validation protocol are documented; RA capacity is currently unknown
(no RA on board yet), so v1 ships fully automated with RA validation layered in as capacity
allows.

### 4.7 Yelp (deferred)
Yelp has a Black-owned badge (Sharma et al. used it), but the **Fusion API does not appear to
expose it** (Yelp withholds many attributes; full access needs a Places Enterprise license).
**Decision:** defer Yelp until we learn Justin's acquisition method (ask him) or obtain a
compliant data path. No scraping.

---

## 5. Schema additions (`businesses` table)

**PUBLIC columns** (denominator + identification + geography):
- `is_certified`, `is_self_identified`, `is_media_identified` (bool), `identification` (derived
  primary), `identification_sources` (JSON list), `identification_date`
- **Geographic enrichment (§6):** `census_region`, `census_division`, `county_fips`,
  `census_tract`, `congressional_district`, `cd_vintage` (year)

**PRIVATE columns** (excluded from public CSV export):
- Disclosure: `google_gmap_id`, `google_match_status`, `google_match_score`,
  `google_match_date`, `discloses_black_google`, `discloses_black_any`, `disclosure_source`,
  `disclosure_observed_date`, `disclosure_coded_by`, `disclosure_evidence_url`
- Intersectional identity: `identity_women`, `identity_veteran`, `identity_lgbtq`,
  `google_misc` (raw)

The existing scaffold fields (`google_maps_url`, `yelp_url`, `discloses_*`) are reconciled
into the above during the phase-2 build (no duplicate/ambiguous columns).

---

## 6. Geographic enrichment (decided — adds census legibility)
BBRT addresses are geocoded via the **Census batch geocoder**, which already returns lat/long
**and** census geographies. We persist, per business:
- `census_region`, `census_division` (Census's 4 regions / 9 divisions)
- `county_fips`, `census_tract` (joinable to ACS/Decennial data — makes BBRT legible to census
  researchers)
- `congressional_district` + `cd_vintage`: store the **118th-Congress (2020-cycle)** district
  as the baseline. **Caveat (documented):** districts are redrawn ~each decade and some
  mid-decade court-ordered changes occur; we therefore stamp a vintage year and treat CD as a
  versioned attribute (a later vintage can be added without overwriting). This keeps the
  political-geography dimension available without claiming more precision than redistricting
  allows.

lat/long are derived from the address (geocoding) and stored on every business for precise
matching to the UCSD lat/long — confirming §4.3's geo-distance match step.

---

## 7. Codebook (`docs/codebook.md`) — hard requirement
Every column documented: name, type, allowed values, definition, source, public/private flag,
and (for the DV) the measurement protocol + the 2021 temporal caveat + the §4.3 circularity
guardrail. Geographic vintages documented. This is what makes the dataset citable/replicable
and is a precondition for "research-ready." Built alongside phase 2.

---

## 8. Research-readiness

### 8.1 Is DuckDB the right choice? (answering Kyle's question)
**DuckDB is an excellent *engine*** for building/querying the panel (fast, SQL, columnar,
free) and is increasingly used in data science — but social-science researchers mostly work
in **Stata, R, SAS/SPSS**, or plain CSV. So DuckDB should be the **internal build/storage**
layer, and we **distribute** in research-standard formats so no one is locked into it:
- **CSV** — universal (already produced).
- **Parquet** — efficient, native to R (`arrow`) and Python (`pandas`/`polars`).
- **Stata `.dta`** — via `pandas.to_stata` (optional, on request) for the dissertation's own
  analysis.
DuckDB itself reads/writes all of these, so the export is one script. Recommendation: **keep
DuckDB; publish CSV + Parquet; generate `.dta` for Kyle's analysis.** No need to switch
databases or pay for anything.

### 8.2 IRB (queue now)
A stub IRB data-management/protocol document is created at `docs/irb-data-management-plan.md`
so Kyle can begin the submission. The public records + the UCSD dataset are public/secondary
data; the **RA coding** and any **phone-ascertainment** are the human-subjects-adjacent
components needing the protocol. PII (contact info) stays private.

### 8.3 Limitations (documented)
2021-vs-2026 temporal gap; fuzzy-match error; Google-only disclosure channel for v1;
certification-list selection effects; CD redistricting; the §4.3 circularity (handled by
computing rates within `certified`).

---

## 9. Phased implementation plan

- **Phase 3 (done):** this spec (approved) + IRB stub + codebook skeleton.
- **Phase 2 (next):** multi-basis identification refactor; geographic-enrichment columns;
  `acquire_google_local.py` (own the acquisition, all states, reproduce ~14k); `match.py`
  (fuzzy name+address+geo); ingest the Google disclosers (dedup, multi-basis, intersectional
  capture); first disclosure rate **within the certified denominator**; public/private export
  split; codebook.
- **Phase 1 (after):** uncertified-list ingestion (self/media tiers — blackownedeverything.co,
  15% Pledge, myblackreceipt, listicles) to broaden the denominator; re-run matching.

---

## 10. Open questions

**Resolved (Kyle, 2026-06-15):**
1. Variable name `identification` — **keep.**
2. Acquisition scope — **re-acquire all US states** from UCSD; extract all Black-owned.
3. `no_profile` firms — **exclude** from the disclosure-rate denominator (until current-year
   disclosure is captured in 2026+).
4. Yelp — **wait**, pending Justin's method / a compliant path.
5. RA validation budget — **no RA yet**; v1 ships automated, RA validation layered in later.

**New / outstanding (to pursue):**
6. **Acquiring post-2021 Google disclosers without violating ToS** — hard. The Places API
   doesn't expose the attribute; the UCSD set is fixed at 2021. Options to weigh: future
   academic dataset releases, a Google data partnership, owner-side Business Profile API (only
   for owned listings), or RA/manual coding of a refreshed sample. Documented as an open
   data-acquisition problem; no ToS-violating scrape.
7. **Other identities (women/veteran/LGBTQ)** — captured from UCSD `MISC` into private columns
   for intersectional analysis (§4.5); public exposure TBD.
8. **Mining UCSD reviews** — once the data is acquired, sample reviews (a separate agent can
   do a small qualitative pass / topic modeling) to scope a possible future paper. Needs data
   in hand first.
9. **Geographic enrichment depth** — region/division/county/tract + CD-by-vintage are in scope
   (§6); decide later whether to add more (e.g., MSA, opportunity-zone flags).
