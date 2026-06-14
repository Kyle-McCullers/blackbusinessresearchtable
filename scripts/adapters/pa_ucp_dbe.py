"""
Pennsylvania Unified Certification Program (UCP) DBE directory adapter.

Source: PA UCP DBE directory on B2Gnow (`paucp.dbesystem.com`), exported manually
to CSV ("Download Results to Excel"). Manual-capture source — see
pipeline/b2gnow_base.py for the shared platform handling.

Filter: Ethnicity == "Black American" (federal DBE presumed-group label).
Confidence: confirmed_black — Ethnicity is an explicit, published per-firm field.
(Per DECISIONS.md, BBRT tracks businesses, not certification legal status; the
2025 USDOT DBE Interim Final Rule does not affect whether a listed firm is
Black-owned. This is a dated 2026-Q2 snapshot.)

Distinct Ethnicity values observed (2026-06-14 full export, verbatim):
  "Black American" (621), "Caucasian" (504), "Hispanic American" (95),
  "Subcontinent Asian American" (89), "Asian-Pacific American" (58),
  "Other" (31), "Native American" (10), "Other Minority" (8), "" (3)
621 Black rows → ~592 unique firms (deduplicated on company name + physical
address).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.b2gnow_base import B2GnowAdapter


class PaUcpDbeAdapter(B2GnowAdapter):
    SOURCE_ID   = "pa_ucp_dbe"
    SOURCE_NAME = "Pennsylvania UCP DBE"
    PROGRAM     = "DBE"
    GEOGRAPHY   = "Pennsylvania"

    FILE_GLOB    = "Pennsylvania*Directory*.csv"
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
