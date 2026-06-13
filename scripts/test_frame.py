import csv
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from frame.frame import open_frame_db, upsert_frame_records, parse_gob2g_frame


def _make_gob2g(headers, rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Some Directory"]); w.writerow(["As of 2026-06-13"]); w.writerow([])
    w.writerow(["The information provided..."]); w.writerow([])
    w.writerow(headers)
    for r in rows:
        w.writerow(r)
    return buf.getvalue()


_HEADERS = ["Company Name", "DBA Name", "Owner First", "Owner Last",
            "Physical Address", "City", "State", "Zip", "Mailing Address",
            "City", "State", "Zip", "Phone", "Email", "Certification Type"]


def _row(name, cert, city="Nashville", zipc="\t37201", email="a@b.com"):
    return [name, "", "Jordan", "Smith", "100 Main St", city, "TN", zipc,
            "100 Main St", city, "TN", zipc, "615-555-0100", email, cert]


def _write(tmp_path, text):
    p = tmp_path / "Directory.csv"
    p.write_text(text, encoding="latin-1")
    return p


def test_frame_keeps_only_minority_cert_types(tmp_path):
    text = _make_gob2g(_HEADERS, [
        _row("Minority Co", "MBE"),
        _row("Women Co", "WBE"),
        _row("Small Co", "SBE"),
    ])
    recs = parse_gob2g_frame(_write(tmp_path, text), "tn_godbe", "Tennessee", {"MBE", "MWBE"})
    assert [r["business_name"] for r in recs] == ["Minority Co"]


def test_frame_captures_contact_fields_and_strips_zip_tab(tmp_path):
    text = _make_gob2g(_HEADERS, [_row("Minority Co", "MBE", email="owner@minco.com")])
    rec = parse_gob2g_frame(_write(tmp_path, text), "tn_godbe", "Tennessee", {"MBE"})[0]
    assert rec["owner_name"] == "Jordan Smith"
    assert rec["address_city"] == "Nashville"
    assert rec["address_zip"] == "37201"   # leading tab stripped
    assert rec["email"] == "owner@minco.com"
    assert rec["certification"] == "MBE"
    assert rec["source_state"] == "Tennessee"


def test_frame_dedups(tmp_path):
    text = _make_gob2g(_HEADERS, [_row("Minority Co", "MBE"), _row("Minority Co", "MBE")])
    recs = parse_gob2g_frame(_write(tmp_path, text), "tn_godbe", "Tennessee", {"MBE"})
    assert len(recs) == 1


def test_frame_db_upsert_idempotent(tmp_path):
    con = open_frame_db(tmp_path / "frame.duckdb")
    recs = [{"source_id": "tn_godbe", "source_state": "Tennessee",
             "business_name": "Minority Co", "address_zip": "37201",
             "address_city": "Nashville", "email": "a@b.com", "certification": "MBE"}]
    upsert_frame_records(con, recs)
    upsert_frame_records(con, recs)   # second time = no duplicate
    n = con.execute("SELECT COUNT(*) FROM mbe_frame").fetchone()[0]
    assert n == 1
    row = con.execute("SELECT survey_status, ascertained_identity FROM mbe_frame").fetchone()
    assert row == ("not_contacted", "")
    con.close()
