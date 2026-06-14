"""
Baltimore City MWBOO certified directory adapter (B2Gnow family).

Source: Baltimore City Minority & Women's Business Opportunity Office directory on
B2Gnow (`baltimorecity.diversitycompliance.com`), exported manually. The
diversitycompliance.com export is an ".xls" that is actually HTML — handled by
the shared reader in pipeline/b2gnow_base.py. Manual-capture source. (Replaces the
dead Open Baltimore Socrata resource us2p-bijb.)

Filter: Ethnicity == "Black American".
Confidence: confirmed_black — Ethnicity is an explicit, published per-firm field.

Distinct Ethnicity values observed (2026-06-14 full export, verbatim):
  "Black American" (944), "Caucasian" (287), "Hispanic American" (171),
  "Asian-Pacific American" (143), "Subcontinent Asian-American" (17),
  "Native American" (7), "Other Minority" (3)
944 Black rows → deduplicated on company name + physical address.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.b2gnow_base import B2GnowAdapter


class BaltimoreMwbooAdapter(B2GnowAdapter):
    SOURCE_ID   = "baltimore_mwboo"
    SOURCE_NAME = "Baltimore MWBOO"
    PROGRAM     = "MWBE"
    GEOGRAPHY   = "Baltimore"

    FILE_GLOB    = "Baltimore*Directory*.xls"
    DEFAULT_CERT = "MBE"

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
