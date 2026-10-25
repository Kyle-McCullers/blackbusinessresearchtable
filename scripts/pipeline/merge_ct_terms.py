"""One-time cleanup: collapse Connecticut certification-term rows into one row per firm.

Before 2026-10-25 the ct_das_smbe adapter emitted one record per certification TERM, and
the entity resolver does not merge repeated rows within a single load, so a firm that had
renewed three times held three business_ids (1,002 ids for ~534 firms). The adapter now
emits one record per firm, keyed on the source's own vendorId. This tool brings the rows
ALREADY in a database in line with that.

No firm is removed (DECISIONS 2026-10-15): rows of one firm are merged into a single
business_id, every superseded id is written to `merged_records`, and rows that cannot be
tied to a firm in the live source are left exactly as they are.

Usage: python -m pipeline.merge_ct_terms DB.duckdb
"""
import re
import sys
from datetime import date
from pathlib import Path

from adapters.ct_das_smbe import CtDasSmbeAdapter, firm_keys
from pipeline.db import open_db

SOURCE_ID = "ct_das_smbe"
REASON = "ct certification-term duplicate (one row per firm)"


def _norm(value) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def _zip5(value) -> str:
    return re.sub(r"\D", "", value or "")[:5]


def _build_indexes(raw_rows):
    full, name_zip, name = {}, {}, {}
    for row, v in zip(raw_rows, firm_keys(raw_rows)):
        n = _norm(row.get("vendorname"))
        z = _zip5(row.get("zip"))
        full.setdefault((n, _norm(row.get("business_address1")), z), set()).add(v)
        name_zip.setdefault((n, z), set()).add(v)
        name.setdefault(n, set()).add(v)
    return full, name_zip, name


def _match(row, indexes):
    full, name_zip, name = indexes
    n, z = _norm(row["business_name"]), _zip5(row["address_zip"])
    for table, key in ((full, (n, _norm(row["address_street"]), z)),
                       (name_zip, (n, z)), (name, n)):
        hits = table.get(key)
        if hits and len(hits) == 1:
            return next(iter(hits))
    return None


def merge_ct_terms(con, raw_rows=None, today=None) -> dict:
    """Merge CT term rows in `con`. `raw_rows` defaults to a live fetch."""
    adapter = CtDasSmbeAdapter()
    if raw_rows is None:
        raw_rows = adapter.fetch()
    live = {r["source_business_id"]: r for r in adapter.parse(raw_rows)}
    indexes = _build_indexes(raw_rows)
    today = today or date.today()

    latest = con.execute(
        "SELECT MAX(snapshot_id) FROM businesses WHERE source_id = ?", [SOURCE_ID]
    ).fetchone()[0]
    if latest is None:
        return {"rows_before": 0, "rows_after": 0, "merged": 0, "unmatched": 0, "firms": 0}

    rows = con.execute(
        "SELECT business_id, snapshot_id, business_name, address_street, address_zip "
        "FROM businesses WHERE source_id = ?", [SOURCE_ID]
    ).fetchall()
    first_seen = dict(con.execute(
        "SELECT business_id, first_seen FROM business_registry WHERE source_id = ?",
        [SOURCE_ID]).fetchall())

    by_vendor, unmatched = {}, 0   # vendor -> [(business_id, snapshot_id)]
    for bid, snap, name, street, zipc in rows:
        v = _match({"business_name": name, "address_street": street, "address_zip": zipc}, indexes)
        if v is None:
            unmatched += 1
            continue
        by_vendor.setdefault(v, []).append((bid, snap))

    def order(bid):
        return (first_seen.get(bid) or "9999", bid)

    merged_ids = {}   # superseded id -> kept id
    plan = []         # (vendor, canonical, {snapshot: [ids]})
    for v, members in by_vendor.items():
        in_latest = [b for b, s in members if s == latest]
        canonical = min(in_latest or [b for b, _ in members], key=order)
        snaps = {}
        for b, s in members:
            snaps.setdefault(s, []).append(b)
        plan.append((v, canonical, snaps))

    rows_before = con.execute(
        "SELECT COUNT(*) FROM businesses WHERE source_id = ?", [SOURCE_ID]).fetchone()[0]
    con.execute("BEGIN")
    try:
        for v, canonical, snaps in plan:
            for snap, ids in snaps.items():
                # keep the canonical row if this snapshot has it, else the first id
                keep = canonical if canonical in ids else sorted(ids, key=order)[0]
                for b in ids:
                    if b != keep:
                        con.execute(
                            "DELETE FROM businesses WHERE business_id=? AND snapshot_id=? AND source_id=?",
                            [b, snap, SOURCE_ID])
                if keep != canonical:
                    con.execute(
                        "UPDATE businesses SET business_id=? WHERE business_id=? AND snapshot_id=? AND source_id=?",
                        [canonical, keep, snap, SOURCE_ID])
            for b in {b for ids in snaps.values() for b in ids} - {canonical}:
                merged_ids[b] = canonical
            rec = live[v]
            con.execute(
                "UPDATE businesses SET source_business_id=?, certification_status=?, "
                "first_certified=?, certification_expires=? "
                "WHERE business_id=? AND snapshot_id=? AND source_id=?",
                [v, rec["certification_status"], rec["first_certified"],
                 rec["certification_expires"], canonical, latest, SOURCE_ID])
            con.execute(
                "UPDATE business_registry SET source_business_id=? WHERE business_id=?",
                [v, canonical])
        for old, kept in merged_ids.items():
            con.execute("INSERT INTO merged_records VALUES (?,?,?,?,?)",
                        [old, kept, SOURCE_ID, REASON, today])
            con.execute("DELETE FROM business_registry WHERE business_id=?", [old])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    rows_after = con.execute(
        "SELECT COUNT(*) FROM businesses WHERE source_id = ?", [SOURCE_ID]).fetchone()[0]
    return {"rows_before": rows_before, "rows_after": rows_after,
            "merged": len(merged_ids), "unmatched": unmatched, "firms": len(by_vendor)}


if __name__ == "__main__":
    con = open_db(Path(sys.argv[1]))
    print(merge_ct_terms(con))
    con.execute("CHECKPOINT")
