"""
Atlanta Office of Contract Compliance — African American Business Enterprise
(AABE) directory adapter.

Source: City of Atlanta certified-firm directory on B2Gnow
(`atlanta.diversitycompliance.com`), exported manually. The diversitycompliance.com
tenants export an ".xls" file that is actually an HTML <table> — handled by the
shared reader in pipeline/b2gnow_base.py. Manual-capture source.

This directory has NO ethnicity column; instead it carries a Black-specific
certification-type code, so the filter is on Certification Type == "AABE"
(African American Business Enterprise).
Confidence: confirmed_black — AABE is an explicit, published Black-owned
certification category.

Distinct Certification Type values observed (2026-06-14 full export, verbatim):
  "SBE" (1,293 — small business, non-ethnic), "AABE" (867), "FBE" (554 — female),
  "HABE" (108 — Hispanic), "APABE" (61 — Asian-Pacific), "NABE" (5 — Native American)
Records are deduplicated on company name + location.

Columns (HTML table header): Company Name, DBA Name, Owner First, Owner Last,
Location, Phone, Fax, Email, Website, Agency, Certification Type, Expiration,
Capability, Market Area, Supplier ID#, Commodity Codes. The address is a single
"Location" field ("City, ST"), parsed via ADDRESS_MODE = "location".
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.b2gnow_base import B2GnowAdapter


class AtlantaAabeAdapter(B2GnowAdapter):
    SOURCE_ID   = "atlanta_aabe"
    SOURCE_NAME = "Atlanta Office of Contract Compliance (AABE)"
    PROGRAM     = "MBE"
    GEOGRAPHY   = "Atlanta"

    FILE_GLOB     = "Atlanta*Directory*.xls"
    FILTER_FIELD  = "Certification Type"
    FILTER_VALUES = frozenset({"aabe"})
    ADDRESS_MODE  = "location"
    DEDUP_FIELDS  = ("Company Name", "Location")
    DEFAULT_CERT  = "AABE"

    FIELD_MAP = {
        "Company Name":       "business_name",
        "Phone":              "phone",
        "Email":              "email",
        "Website":            "website",
        "Certification Type": "certification",
        "Capability":         "description",
    }
