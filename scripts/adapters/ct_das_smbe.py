"""
Connecticut DAS Supplier Diversity (SBE/MBE) adapter.

Source: Connecticut Open Data (Socrata) — DAS certified Small Business
Enterprise / Minority Business Enterprise directory. Public CSV/JSON API,
no authentication.
Dataset: data.ct.gov resource y4ub-e4dd
Filter: class_description_detailed == "Black American"
Confidence: confirmed_black — race/ethnicity is an explicit, published per-firm
field (class_description_detailed), verified verbatim against the live API.

Observed distinct values of class_description_detailed (2026-06-10):
  "Black American", "Hispanic American",
  "Asian Pacific American and Pacific Islander", "Iberian Peninsula",
  "American Indian", "No minority race/ethnicity identified"

The dataset has ONE ROW PER CERTIFICATION TERM, not per firm: a firm that has renewed
three times appears three times (as of 2026-10-25: 1,001 Black American rows =
534 firms by the source's own vendorId = 521 distinct names; 322 rows are the firm's
current term, the other 679 are expired terms). parse() therefore emits one record per
firm, built from its most recent term, with certification_status ("current" if any term
is Certified, else "former"), first_certified, certification_expires, and every term kept
in source_fields["certification_terms"]. Expired firms stay in the record (DECISIONS
2026-06-14 and 2026-10-15): certification is evidence of a firm, not of its existence.

This is a fully auto-fetching source (no manual download), suitable for the
autonomous quarterly cron.
"""
import csv
import io
import sys
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

CT_CSV_URL = "https://data.ct.gov/resource/y4ub-e4dd.csv"
# Socrata caps unpaginated responses at 1,000 rows; raise the limit to pull the
# full directory (~7,800 rows) in one request.
FETCH_LIMIT = 50000
BLACK_VALUE = "Black American"
ETHNICITY_FIELD = "class_description_detailed"


class CtDasSmbeAdapter(AdapterBase):
    SOURCE_ID   = "ct_das_smbe"
    SOURCE_NAME = "Connecticut DAS Supplier Diversity"
    PROGRAM     = "MBE"
    GEOGRAPHY   = "Connecticut"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "vendorname":                            "business_name",
        "business_address1":                     "address_street",
        "townnamecrosswalk_standardized_town":   "address_city",
        "business_state":                        "address_state",
        "zip":                                   "address_zip",
        "gs_code":                               "naics_code",
        "goods_and_services":                    "industry",
        "product":                               "description",
    }

    def fetch(self) -> list[dict]:
        """Pull the CT directory and keep only Black American certified firms."""
        response = requests.get(CT_CSV_URL, params={"$limit": FETCH_LIMIT}, timeout=60)
        response.raise_for_status()
        reader = csv.DictReader(io.StringIO(response.text))
        return [
            row for row in reader
            if (row.get(ETHNICITY_FIELD) or "").strip() == BLACK_VALUE
        ]

    def parse(self, raw: list[dict]) -> list[dict]:
        """One record per firm (vendorId), built from the firm's most recent term."""
        firms: dict[str, list[dict]] = {}
        for source_row, key in zip(raw, firm_keys(raw)):
            firms.setdefault(key, []).append(source_row)

        records = []
        for key, terms in firms.items():
            # Newest term first; "Certified" outranks "Expired" on a tie.
            terms.sort(key=lambda t: (_day(t.get("active_date")),
                                      t.get("status") == "Certified"), reverse=True)
            latest = terms[0]
            record = self.map_record(latest)
            # Older terms often leave goods/services blank; fall back to the newest
            # term that has them.
            for field_src, field_dst in (("gs_code", "naics_code"),
                                         ("goods_and_services", "industry"),
                                         ("product", "description")):
                if not record[field_dst]:
                    record[field_dst] = next(
                        ((t.get(field_src) or "").strip() for t in terms
                         if (t.get(field_src) or "").strip()), "")
            record["source_business_id"] = key
            # certification_type is "MBE" or "SBE"; default MBE.
            record["certification"] = (
                (latest.get("certification_type") or "").strip() or "MBE"
            )
            any_current = any(t.get("status") == "Certified" for t in terms)
            record["certification_status"] = "current" if any_current else "former"
            starts = [d for d in (_day(t.get("active_date")) for t in terms) if d]
            ends = [d for d in (_day(t.get("expiration_date")) for t in terms) if d]
            record["first_certified"] = min(starts) if starts else ""
            record["certification_expires"] = max(ends) if ends else ""
            record["source_fields"]["certification_terms"] = [
                {"status": t.get("status", ""), "active": _day(t.get("active_date")),
                 "expires": _day(t.get("expiration_date")),
                 "type": t.get("new_certification", "")}
                for t in terms
            ]
            # The dataset ships authoritative coordinates as a WKT POINT; use them
            # so these records skip the geocoder entirely.
            lat, lon = _parse_point(latest.get("location", ""))
            if lat and lon:
                record["latitude"] = lat
                record["longitude"] = lon
            record["last_verified"] = str(date.today())
            records.append(record)
        return records


def _vendor_id(row: dict) -> str:
    """The vendorId in the company-detail link, or "" if the link is missing."""
    link = (row.get("link_to_company_detail_page") or "").strip()
    return parse_qs(urlparse(link).query).get("vendorId", [""])[0].upper()


def _identity(row: dict) -> tuple:
    return (" ".join((row.get("vendorname") or "").lower().split()),
            " ".join((row.get("business_address1") or "").lower().split()),
            (row.get("zip") or "").strip()[:5])


def firm_keys(rows: list[dict]) -> list[str]:
    """One stable key per firm, parallel to `rows`.

    A firm is the set of rows linked by the source's vendorId OR by an identical
    (name, street, zip). Both links are needed: a firm keeps one vendorId across its
    renewals, but the state sometimes re-registers the same firm under a second
    vendorId (same name, address and term dates), which a vendorId-only key would count
    as two firms. The key is the smallest vendorId in the group (name|zip if no row in
    the group has a vendorId).
    """
    parent: dict = {}

    def find(x):
        while parent.setdefault(x, x) != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        parent[find(a)] = find(b)

    for row in rows:
        node = ("row", _identity(row))
        find(node)
        vid = _vendor_id(row)
        if vid:
            union(node, ("vid", vid))
    members: dict = {}
    for row in rows:
        vid = _vendor_id(row)
        root = find(("vid", vid)) if vid else find(("row", _identity(row)))
        members.setdefault(root, set())
        if vid:
            members[root].add(vid)
    keys = []
    for row in rows:
        vid = _vendor_id(row)
        root = find(("vid", vid)) if vid else find(("row", _identity(row)))
        ids = members[root]
        if ids:
            keys.append(min(ids))
        else:
            ident = _identity(row)
            keys.append(f"{ident[0]}|{ident[2]}")
    return keys


def _day(value) -> str:
    """'2026-10-31T00:00:00.000' -> '2026-10-31'; blank stays blank."""
    return (value or "").strip()[:10]


def _parse_point(wkt: str) -> tuple[str, str]:
    """Parse 'POINT (lon lat)' → ('lat', 'lon'). Returns ('','') if unparseable."""
    if not wkt or "POINT" not in wkt.upper():
        return "", ""
    try:
        inner = wkt[wkt.index("(") + 1:wkt.index(")")].strip()
        lon_str, lat_str = inner.split()
        return lat_str, lon_str
    except (ValueError, IndexError):
        return "", ""
