# IRB / Data-Management Plan — BBRT Disclosure Study (DRAFT STUB)

**Status:** stub to queue the IRB submission. Fill bracketed `[…]` fields before submitting.
**PI:** Kyle McCullers (University of Michigan, Ross School of Business)
**Related design:** `docs/superpowers/specs/2026-06-14-bbrt-disclosure-study-design.md`

---

## 1. Study summary
A study of **strategic identity disclosure** among Black-owned businesses: using the Black
Business Research Table (BBRT) as a denominator of known Black-owned firms, measure what
fraction publicly disclose Black ownership (e.g., the Google "Black-owned" attribute) and
what predicts disclosure.

## 2. Data sources & human-subjects determination
| Source | Type | Human subjects? | Notes |
|---|---|---|---|
| Government MWBE/DBE certification lists | Public records | No (public, business-level) | Denominator (`certified`) |
| Google Local Review Data 2021 (UCSD) | Public secondary dataset | No (public, de-identified business metadata) | Disclosure signal + self-identified |
| Self/media directories & listicles | Public | No | `self_identified` / `media_identified` |
| **RA coding of public Google Maps profiles** | Public observation | **Likely exempt** — public, business-level | Validation pass |
| **Phone ascertainment of owners** (future) | Contact with individuals | **YES — needs protocol** | Identity ascertainment; recruitment script + consent |

**Determination sought:** [Exempt / Not Regulated for the public-data + business-level
components; expedited/full for any owner contact]. Confirm with [UM IRB-HSBS].

## 3. Data classification & security
- **Public columns** (business identity, location, geography, identification tier) — publishable.
- **Private columns** (disclosure DV, intersectional identity flags, owner **contact info**) —
  stored in the gitignored `mbe_frame.duckdb` / private columns of `bbrt.duckdb`; never on the
  public site or public CSV export. Contact PII restricted to [storage location, access list].

## 4. The owner-contact / RA components (the parts needing review)
- RA coding protocol: [what RAs view, what they record, dated screenshots, no interaction].
- Phone ascertainment (if pursued): [recruitment, script, consent language, data handling,
  retention]. Ties to the `mbe_frame` sampling frame.

## 5. Risks & minimization
Business-level public data → minimal risk. Owner contact → standard minimal-risk survey
protections; no sensitive PII beyond what's needed; secure storage; [retention/destruction].

## 6. To complete before submission
- [ ] UM IRB determination category
- [ ] RA coding protocol document
- [ ] Phone-ascertainment protocol + consent script (if in scope)
- [ ] Data-security attestation (private-column storage + access)
- [ ] Citations/data-use terms for the UCSD dataset
