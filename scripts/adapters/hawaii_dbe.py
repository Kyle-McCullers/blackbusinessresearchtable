"""
Hawaii DOT DBE directory adapter (B2Gnow family).

Source: Hawaii DOT DBE directory on B2Gnow (`hdot.dbesystem.com`), exported
manually to CSV. Manual-capture source — see pipeline/b2gnow_base.py.

Filter: Ethnicity == "Black American" (federal DBE presumed-group label).
Confidence: confirmed_black — Ethnicity is an explicit, published per-firm field.
(Per DECISIONS.md, BBRT tracks businesses, not certification legal status.)

Distinct Ethnicity values observed (2026-06-14 full export, verbatim):
  "Asian-Pacific American" (117), "Native American" (98), "Caucasian" (23),
  "Black American" (19), "Hispanic American" (15), "Subcontinent Asian American" (10)
Small directory; 19 Black rows → deduplicated on company name + physical address.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.b2gnow_base import B2GnowAdapter


class HawaiiDbeAdapter(B2GnowAdapter):
    SOURCE_ID   = "hawaii_dbe"
    SOURCE_NAME = "Hawaii DOT DBE"
    PROGRAM     = "DBE"
    GEOGRAPHY   = "Hawaii"

    FILE_GLOB    = "Hawaii*Directory*.csv"
    DEFAULT_CERT = "DBE"

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
