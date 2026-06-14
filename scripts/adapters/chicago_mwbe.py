"""
City of Chicago certified M/W/DBE directory adapter (B2Gnow family).

Source: City of Chicago directory on B2Gnow (`chicago.mwdbe.com`), exported
manually. Manual-capture source — see pipeline/b2gnow_base.py.

Filter: Ethnicity in {"African American", "African-American (Black)"} — Chicago
uses two distinct Black labels across its programs.
Confidence: confirmed_black — Ethnicity is an explicit, published per-firm field.

Distinct Ethnicity values observed (2026-06-14 full export, verbatim):
  "African American" (2,214), "Hispanic/Latino" (1,145), "Caucasian" (1,028),
  "Asian American" (521), "African-American (Black)" (463), "Hispanic American"
  (214), "Native American" (15), "Other" (10)
2,677 Black rows → deduplicated on company name + physical address.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.b2gnow_base import B2GnowAdapter


class ChicagoMwbeAdapter(B2GnowAdapter):
    SOURCE_ID   = "chicago_mwbe"
    SOURCE_NAME = "Chicago M/W/DBE"
    PROGRAM     = "MWBE"
    GEOGRAPHY   = "Chicago"

    FILE_GLOB     = "Chicago*Directory*.csv"
    FILTER_VALUES = frozenset({"african american", "african-american (black)"})
    DEFAULT_CERT  = "MWBE"

    FIELD_MAP = {
        "Company Name":      "business_name",
        "Physical Address":  "address_street",
        "City":              "address_city",
        "State":             "address_state",
        "Zip":               "address_zip",
        "Phone":             "phone",
        "Email":             "email",
        "Website":           "website",
        "Certification Type": "certification",
        "Capability":        "description",
    }
