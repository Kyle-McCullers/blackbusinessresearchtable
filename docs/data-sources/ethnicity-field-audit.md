# Ethnicity Field Audit (QC)

The measurement-validity record for `confirmed_black` classification. One entry
per source: the exact ethnicity field name, **every distinct value observed in
that field, verbatim**, and which value(s) the adapter filters on as Black. This
is the codebook for what "confirmed_black" operationally means per source — use
it for quality-control review and the eventual data appendix.

A source is not classified `confirmed_black` until its real values are captured
here from actual data (not from program documentation).

Legend: ✅ verified against real data | ⚠️ contingent (needs a file/column check)

---

## Built sources

### `in_idoa` — Indiana IDOA Diversity Certified Businesses ✅
- **Field:** `Ethnic Group`
- **Distinct values (verbatim, with row counts 2026-06-10):**
  `AFA` (4,759), `CAU` (5,341), `HIS` (1,044), `AIN` (531), `APA` (407),
  `NAM` (189), `MRA` (157), `OTH` (72)
- **Black filter:** `AFA` (African American)
- **Note:** 4,759 AFA *rows* (one per UNSPSC commodity code) dedupe to **627
  unique firms** by Bidder ID. Codes are IDOA's; `AFA`=African American,
  `CAU`=Caucasian, `HIS`=Hispanic, `AIN`/`NAM`=American/Native American,
  `APA`=Asian Pacific American, `MRA`=multiracial(?), `OTH`=Other.

### `ct_das_smbe` — Connecticut DAS Supplier Diversity ✅
- **Field:** `class_description_detailed`
- **Distinct values (verbatim, 2026-06-10):**
  `Black American`, `Hispanic American`,
  `Asian Pacific American and Pacific Islander`, `Iberian Peninsula`,
  `American Indian`, `No minority race/ethnicity identified`
- **Black filter:** `Black American` (970 records of 7,836 total)

### `tx_hub` — Texas HUB ✅
- **Field:** `ELIGIBILITY CODE`
- **Black filter:** `BL` (Black American). Other codes include `AI`, `AS`,
  `HI`, `WO` (per Texas HUB schema). *(Full verbatim value enumeration to be
  backfilled on next refresh.)*

### `md_mbe` — Maryland MBE ✅
- **Field:** `Minority Status`
- **Black filter:** `African American`, `African American / Female`

### `ma_sdo`, `al_ombe`, `nyc_mwbe`
- Pre-existing `confirmed_black` adapters; ethnicity-field details to be
  backfilled into this audit on their next refresh. Each filters an explicit
  Black/African American ethnicity field (see the adapter source for the
  current filter values).

---

### `or_cobid` — Oregon COBID Certified Firms ✅ (manual capture)
- **Field:** `Ethnicity`
- **Distinct values (verbatim, 2026-06-11 full export):**
  `Caucasian (White)` (2,474), `Hispanic` (834), `African American (Black)` (624),
  `Asian Pacific` (466), `Native American (Indian)` (169),
  `Subcontinent Asian (Asian Indian)` (142), `Other` (63), `Unknown` (24)
- **Black filter:** `African American (Black)` — 624 cert-type rows → **285 unique firms**

### `nv_dbe` — Nevada NDOT DBE ✅ (manual capture)
- **Field:** `Ethnicity` (values are UPPERCASE in the export)
- **Distinct values (verbatim, 2026-06-11 full export):**
  `CAUCASIAN` (228), `BLACK AMERICAN` (227), `HISPANIC AMERICAN` (162),
  `ASIAN-PACIFIC AMERICAN` (54), `SUBCONTINENT ASIAN AMERICAN` (10),
  `NATIVE AMERICAN` (9), `OTHER MINORITY` (6)
- **Black filter:** `BLACK AMERICAN` (case-insensitive) — 227 rows → **113 unique firms**

---

## Buildable sources (values captured during inventory; confirm on build)

| Source | Field | Black value(s) | Verified |
|---|---|---|---|
| `de_osd` | `ddd_baa` (boolean) | `ddd_baa = YES` | ✅ Socrata |
| `sc_smbcc` | minority code (in cert #) | `01`, `02`, `05` | ⚠️ confirm column in .xlsx |
| `va_swam` | `ethnicity` | `African American` (presumed) | ⚠️ confirm from live record |
| `tn_godbe` | MBE minority group | `African American` | ⚠️ confirm export has the column |
| `baltimore_mwboo` | (unconfirmed) | `African American` | ⚠️ confirm published column |

See `scripts/sources_roadmap.yml` for full value lists and blocked-source
ethnicity fields (NC HUB `Black`, CO/CA/NV `Black American`, etc.).
