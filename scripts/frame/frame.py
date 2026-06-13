"""
Minority-business sampling frame — a SEPARATE database from the public
confirmed_black dataset.

Holds minority/MWBE-certified firms whose specific ethnicity is NOT published
(e.g. NY State MWBE, Tennessee MBE). These are NOT Black-owned in the record —
they are a sampling frame for an identity-ascertainment study (public-disclosure
match → short survey → phone follow-up). Race/identity is filled in later, per
firm, into the survey-tracking columns; a firm only "graduates" into the public
confirmed_black dataset once its Black ownership is ascertained.

Stored at data/mbe_frame.duckdb (gitignored — contains contact info and, later,
race data tied to identifiable firms; never exported to the public site).
"""
import csv
import hashlib
from datetime import date
from pathlib import Path

import duckdb

FRAME_DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "mbe_frame.duckdb"

_DDL = """
CREATE TABLE IF NOT EXISTS mbe_frame (
    frame_id        VARCHAR PRIMARY KEY,   -- hash(source_id|name|zip|city)
    source_id       VARCHAR NOT NULL,      -- e.g. ny_mwbe, tn_godbe
    source_state    VARCHAR NOT NULL,
    business_name   VARCHAR,
    dba             VARCHAR,
    owner_name      VARCHAR,
    address_street  VARCHAR,
    address_city    VARCHAR,
    address_state   VARCHAR,
    address_zip     VARCHAR,
    phone           VARCHAR,
    email           VARCHAR,
    certification   VARCHAR,                -- M/W designation from the source
    captured_date   DATE,
    -- identity-ascertainment / survey tracking (filled in later) --
    ascertained_identity   VARCHAR DEFAULT '',  -- black | not_black | unknown | ...
    discloses_black_owned  VARCHAR DEFAULT '',  -- prominently | somewhat | no | ''
    ascertainment_method   VARCHAR DEFAULT '',  -- public_directory_match | survey_email | phone
    survey_status          VARCHAR DEFAULT 'not_contacted',
    notes                  VARCHAR DEFAULT ''
)
"""

# Flexible source-column → frame-field mapping (gob2g exports vary slightly).
_FIELD_CANDIDATES = {
    "business_name": ["Company Name", "Vendor Name", "Firm Name"],
    "dba":           ["DBA Name", "DBA"],
    "address_street": ["Physical Address", "Address", "Business Address"],
    "address_city":  ["City", "Business City"],
    "address_state": ["State", "Business State"],
    "address_zip":   ["Zip", "Zip Code", "Business Zip"],
    "phone":         ["Phone", "Phone Number", "Business Phone"],
    "email":         ["Email", "Email ID", "Business Email"],
    "certification": ["Certification Type", "Certification", "Class"],
}


def open_frame_db(db_path: Path = FRAME_DB_PATH) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(db_path))
    con.execute(_DDL)
    return con


def _frame_id(source_id, name, zipcode, city):
    raw = f"{source_id}|{(name or '').strip().lower()}|{(zipcode or '').strip()}|{(city or '').strip().lower()}"
    return hashlib.sha1(raw.encode("utf-8", "replace")).hexdigest()[:16]


def upsert_frame_records(con, records: list[dict]) -> int:
    """Insert frame records (idempotent on frame_id). Returns rows written."""
    written = 0
    for r in records:
        fid = _frame_id(r["source_id"], r.get("business_name"), r.get("address_zip"), r.get("address_city"))
        con.execute(
            """
            INSERT INTO mbe_frame
                (frame_id, source_id, source_state, business_name, dba, owner_name,
                 address_street, address_city, address_state, address_zip, phone,
                 email, certification, captured_date)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (frame_id) DO NOTHING
            """,
            [fid, r["source_id"], r["source_state"], r.get("business_name", ""),
             r.get("dba", ""), r.get("owner_name", ""), r.get("address_street", ""),
             r.get("address_city", ""), r.get("address_state", ""), r.get("address_zip", ""),
             r.get("phone", ""), r.get("email", ""), r.get("certification", ""), date.today()],
        )
        written += 1
    return written


def parse_gob2g_frame(file_path: Path, source_id: str, source_state: str,
                      minority_cert_types, encoding: str = "latin-1") -> list[dict]:
    """Parse a gob2g/dbesystem CSV export into minority-frame records.

    Skips the title preamble, locates the header row dynamically, and keeps only
    rows whose Certification Type is in `minority_cert_types` (the minority — not
    women-only — designations, e.g. {"MBE", "MWBE"}). Captures contact fields for
    later outreach. Dedups on company + zip + city.
    """
    with open(file_path, encoding=encoding, newline="") as f:
        rows = list(csv.reader(f))

    header_idx = next(
        (i for i, row in enumerate(rows)
         if any((c or "").strip() in ("Company Name", "Vendor Name", "Firm Name") for c in row)),
        None,
    )
    if header_idx is None:
        raise ValueError(f"No recognizable header row in {file_path}")
    header = [(c or "").strip() for c in rows[header_idx]]
    col = {}
    for i, name in enumerate(header):
        if name and name not in col:   # first occurrence (handles duplicate columns)
            col[name] = i

    def pick(row, field):
        for cand in _FIELD_CANDIDATES[field]:
            if cand in col and col[cand] < len(row):
                return row[col[cand]].strip()
        return ""

    cert_set = {c.upper() for c in minority_cert_types}
    owner_first_i = col.get("Owner First")
    owner_last_i = col.get("Owner Last")

    seen, out = set(), []
    for row in rows[header_idx + 1:]:
        cert = pick(row, "certification")
        if cert.upper() not in cert_set:
            continue
        name = pick(row, "business_name")
        zipc = pick(row, "address_zip").lstrip("\t").strip()
        city = pick(row, "address_city")
        key = (name.lower(), zipc, city.lower())
        if not name or key in seen:
            continue
        seen.add(key)
        first = row[owner_first_i].strip() if owner_first_i is not None and owner_first_i < len(row) else ""
        last = row[owner_last_i].strip() if owner_last_i is not None and owner_last_i < len(row) else ""
        out.append({
            "source_id": source_id, "source_state": source_state,
            "business_name": name, "dba": pick(row, "dba"),
            "owner_name": " ".join(filter(None, [first, last])),
            "address_street": pick(row, "address_street"), "address_city": city,
            "address_state": pick(row, "address_state"), "address_zip": zipc,
            "phone": pick(row, "phone"), "email": pick(row, "email"),
            "certification": cert,
        })
    return out
