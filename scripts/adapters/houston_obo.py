"""
Houston Office of Business Opportunity (OBO) certified M/W/DBE directory adapter.

Source: City of Houston OBO directory on B2Gnow (`houston.mwdbe.com`), exported
manually to CSV ("Download Results to Excel"). Manual-capture source — see
pipeline/b2gnow_base.py for the shared platform handling.

Filter: Ethnicity in {"Black", "Black American"} (the city MWBE program labels
firms "Black"; the DBE side uses the federal "Black American" label).
Confidence: confirmed_black — Ethnicity is an explicit, published per-firm field.

Distinct Ethnicity values observed (2026-06-14 full export, verbatim):
  "Black" (5,505), "Hispanic" (2,949), "Caucasian" (1,276), "Asian" (1,171),
  "Other Ethnicity" (465), "Native American" (76), "Black American" (22),
  "Hispanic American" (16), "" (3)
5,527 Black rows collapse to ~2,224 unique firms (a firm appears once per
commodity/certification row; deduplicated on company name + physical address).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.b2gnow_base import B2GnowAdapter


class HoustonOboAdapter(B2GnowAdapter):
    SOURCE_ID   = "houston_obo"
    SOURCE_NAME = "Houston Office of Business Opportunity MWBE"
    PROGRAM     = "MWBE"
    GEOGRAPHY   = "Houston"

    FILE_GLOB    = "Houston*Directory*.csv"
    DEFAULT_CERT = "MWBE"

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
