# BBRT as a Disclosure-Rate Research Instrument — Design Spec

**Status:** DRAFT for review (Kyle) · **Date:** 2026-06-14
**Author:** drafted with Claude; to be reviewed before any schema change
**Phase plan:** 3 → 2 → 1 (this doc = phase 3; then disclosure matching; then uncertified ingestion)

---

## 1. Purpose & research context

The Black Business Research Table (BBRT) is being extended from a coverage database
into a **research instrument for Kyle's dissertation on strategic identity disclosure**.

The study question: **among businesses known to be Black-owned, what fraction publicly
disclose that identity, and what predicts disclosure?** BBRT supplies the **denominator**
(a large, provenance-documented set of known Black-owned firms). A separate
**disclosure signal** — whether a firm sets the public "Black-owned" attribute on its
Google Business Profile — supplies the **dependent variable**.

Everything lives in the **main database** (`bbrt.duckdb`): one row per business, with
(a) an identification variable stratifying *how* we know it's Black-owned, and
(b) disclosure-measurement columns. The panel thus doubles as the denominator and the
analysis dataset.

---

## 2. Core design decisions (summary)

1. Replace the binary `confidence` tier with an **`identification`** variable:
   `certified` | `self_identified` | `media_identified`.
2. The disclosure DV is captured by **matching BBRT against an external list of
   disclosers** (firms that set the Google "Black-owned" attribute), NOT by reading the
   attribute from a public API (it is not exposed there — see §4.1).
3. The discloser list is derived from the **Google Local Review Data (2021), UCSD** —
   re-acquired from source so Kyle **owns the full acquisition cycle** (provenance).
4. Matching is automated (fuzzy name+address+geo); RA coding becomes **validation**, not
   primary capture. Yelp is a possible **second** disclosure channel (via its official
   API only — §4.2).
5. The temporal gap (2021 disclosure data vs 2026 certification data) is a **named
   limitation** with explicit handling (§4.4).

---

## 3. The identification variable (denominator stratifier)

Rename the existing `confidence` column (`confirmed_black` / `mbe_unverified`) to a
research-legible **`identification`** variable. One value per business, mutually exclusive,
in priority order (a business appearing in multiple tiers takes the strongest):

| Value | Meaning | Example sources |
|---|---|---|
| `certified` | On a government certification list with an explicit Black/African-American ethnicity field (the only confirmation accepted). Replaces `confirmed_black`. | State MWBE/DBE, city MWBE, HUB |
| `self_identified` | The business lists *itself* as Black-owned in a directory. | blackownedeverything.co |
| `media_identified` | A third party (journalist/blogger) named it Black-owned. | "Best Black-owned businesses in <city>" listicles |

Supporting columns: `identification_source` (the specific list/article/program),
`identification_source_url`, `identification_date`. These let analyses compute disclosure
rates **within each tier** (a core hypothesis: certified firms may disclose differently
than self/media-identified firms) and let reviewers audit provenance.

`mbe_unverified` (minority but no ethnicity breakdown) is retired from the public panel; it
remains only in the separate, private `mbe_frame.duckdb` sampling frame.

---

## 4. The disclosure layer (dependent variable)

### 4.1 Why not the Google Places API
The "Black-owned" badge is a **Google Business Profile attribute the owner sets**; it shows
on Maps/Search under "Highlights" but is **not returned by the public Places API** for
third-party businesses. So disclosure cannot be read live at scale via API. Two viable
paths: (a) match against an existing **dataset that captured the attribute**, or (b)
human/RA coding of each Maps profile. We use (a) as primary, (b) as validation.

### 4.2 Disclosure data source — Google Local Review Data (2021), UCSD
- **Dataset:** Google Local Review Data (2021); creators Tianyang Zhang & Jiacheng Li
  (UC San Diego). ~4.96M US businesses, per-state, **gzipped JSON, one record per line**.
- **Business metadata fields:** `name`, `address`, **`gmap_id`** (unique Google Maps id),
  `latitude`, `longitude`, `category`, `avg_rating`, `num_of_reviews`, `price`, `hours`,
  **`MISC`** (attribute dict: service options, accessibility, payment, **and identity
  "Highlights"**), `url`, `relative_results`, `state` (open/closed).
- **The disclosure signal lives in `MISC`.** Google's identity attribute appears as a
  highlight string (e.g. "Identifies as Black-owned" / "Black-owned"). **ACQUISITION TODO:**
  confirm the exact `MISC` key + value string carrying it (Justin Frake's extract found
  ~14,000 US businesses with the tag in 2021 — reproduce that filter from source).
- **Provenance goal:** Kyle owns the full cycle — download the per-state metadata files
  directly from the UCSD repository, parse, filter to the Black-owned subset, and version
  the extraction script + a data statement. Justin Frake's existing CSV is the interim
  input and the cross-check target (we should reproduce his ~14k count).
- **Snapshot:** September 2021, single time point (cross-sectional disclosure).

### 4.3 Matching methodology (BBRT ↔ Google Local)
BBRT rows have no `gmap_id`, so match on identity + location:
1. **Block** by state (and coarse geo) to keep comparisons tractable.
2. **Score** candidate pairs on normalized business name (reuse the pipeline's
   `entity_resolver.normalize_name` + `rapidfuzz`), normalized address/zip, and
   lat/long distance.
3. **Classify** each BBRT business: `matched` (high-confidence single match → adopt its
   `gmap_id`), `ambiguous` (multiple/low-confidence → queue for RA review), `no_profile`
   (no candidate → business had no 2021 Google presence).
4. **RA validation:** a sample of `matched` + all `ambiguous` are human-checked; record
   coder + date. Match thresholds are tuned against this validation set and documented.

Disclosure is then a property of the matched `gmap_id`: did that 2021 profile carry the
Black-owned attribute?

### 4.4 Disclosure variable definitions
- `google_gmap_id`, `google_match_status` (`matched`|`ambiguous`|`no_profile`),
  `google_match_score`, `google_match_date`.
- **`discloses_black_google`** (the DV): `true` if the matched gmap_id carried the
  Black-owned attribute in the 2021 data; `false` if matched but no attribute; `null` if
  `no_profile`/`ambiguous` (not measurable). Rollup `discloses_black_any` reserved for when
  Yelp/website channels are added.
- Provenance: `disclosure_source` ("Google Local 2021 / UCSD"), `disclosure_observed_date`
  (2021-09), `disclosure_coded_by` (algorithm vs RA), `disclosure_evidence_url`.

**Temporal-gap handling (named limitation):** the disclosure snapshot is 2021; BBRT
certifications are 2026. A firm can only have disclosed in 2021 if it existed/had a profile
then. Mitigations to document: (i) report disclosure rate on the subset with a 2021 Google
profile (`matched`), not the whole denominator; (ii) treat `no_profile` explicitly (could
be young firms or closed); (iii) note that re-acquiring a *current* Google snapshot is a
future extension (harder — the UCSD set is a fixed 2021 release). State this in the codebook
and any paper.

### 4.5 Yelp (future second channel)
Yelp also exposes a "Black-owned" attribute. The **only** ToS-safe route is the official
**Yelp Fusion API** (not scraping). OPEN: confirm whether Fusion's business endpoint returns
that attribute. If yes, it becomes `discloses_black_yelp` and strengthens
`discloses_black_any`. Deferred to a later phase.

---

## 5. Schema additions (`businesses` table)

Denominator stratifier (renames + adds):
- `identification` (was `confidence`): `certified|self_identified|media_identified`
- `identification_source`, `identification_source_url`, `identification_date`

Disclosure layer (new, nullable until measured):
- `google_gmap_id`, `google_match_status`, `google_match_score`, `google_match_date`
- `discloses_black_google`, `discloses_black_any`
- `disclosure_source`, `disclosure_observed_date`, `disclosure_coded_by`, `disclosure_evidence_url`

The existing scaffold fields (`google_maps_url`, `yelp_url`, `discloses_google_maps`,
`discloses_yelp`, `discloses_instagram`) are reconciled into the above (some renamed/retired)
during phase-2 build to avoid ambiguity.

---

## 6. Pipeline architecture

- `scripts/disclosure/acquire_google_local.py` — download per-state UCSD metadata, parse
  gzip-JSON, extract the Black-owned subset → a versioned `disclosers_google_2021` table
  (provenance-stamped). Reproduces Justin Frake's ~14k as a validation check.
- `scripts/disclosure/match.py` — match BBRT ↔ disclosers (blocking + fuzzy scoring), write
  `google_*` and `discloses_black_google` fields, emit an `ambiguous` queue.
- `scripts/disclosure/ra_sheet.py` — export an RA validation/coding sheet (business + Maps
  link) and re-import RA decisions.
- All write into `bbrt.duckdb`; disclosure fields are nullable so the panel works before
  matching is run.

---

## 7. Codebook (data dictionary) — to ship as `docs/codebook.md`
Every column documented: name, type, allowed values, definition, source, and (for the DV)
the exact measurement protocol + the 2021 temporal caveat. This is what makes the dataset
citable/replicable. Drafted alongside the phase-2 build.

---

## 8. Research-readiness

- **Provenance:** every business carries `identification_source(_url)`; the disclosure data
  is re-acquired from the UCSD source with a versioned extraction script + data statement —
  Kyle owns the full cycle, not a hand-me-down CSV.
- **Reproducibility:** acquisition + matching are scripted and seeded; match thresholds are
  documented and validated against an RA-checked sample.
- **IRB:** the data is public; the **RA coding** and any **phone-ascertainment** components
  are the parts that need Kyle's IRB protocol. Personally identifying contact info stays in
  the private frame DB, never on the public site.
- **Limitations (documented):** 2021 vs 2026 temporal gap; fuzzy-match error; Google-only
  disclosure channel until Yelp/website added; certification-list selection effects.

---

## 9. Phased implementation plan

- **Phase 3 (now):** this spec + the codebook skeleton; Kyle reviews and approves the schema.
- **Phase 2 (next):** rename `confidence` → `identification`; add disclosure columns; build
  `acquire_google_local.py` (own the acquisition cycle; reproduce ~14k) and `match.py`;
  produce a first disclosure rate with documented match quality.
- **Phase 1 (after):** uncertified-list ingestion (self/media tiers) to broaden the
  denominator; re-run matching.

---

## 10. Open questions / decisions for Kyle

1. **Column name:** `identification` for the tier variable — good, or prefer
   `id_basis` / `evidence_tier`?
2. **Acquisition scope:** re-acquire **all** US states from UCSD, or only states where BBRT
   currently has businesses (the 20 + DC) for v1?
3. **`no_profile` handling:** exclude from the disclosure-rate denominator, or report as a
   separate category?
4. **Yelp channel:** pursue now or after the Google channel is solid?
5. **Match-threshold review budget:** how much RA time is available to validate matches
   (drives how conservative the auto-match must be)?
