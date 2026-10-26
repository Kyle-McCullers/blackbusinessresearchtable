"""One row per firm.

The database stores LISTINGS: one row per (program, business). The same firm is often listed by
several programs, or twice by one program. The public snapshot (`data/businesses.csv`) is one row per
FIRM; every listing stays in the database and in `data/listings.csv`, tied to its firm by `firm_map`.

Matching rule (Kyle, 2026-10-25): two listings are the same firm when their normalized name AND their
5-digit ZIP are equal. Similar-but-different names in the same ZIP are NOT merged; they are written to
`data/review/firm-near-matches.csv` for a person to look at. Listings with no ZIP (Florida, Atlanta and Oregon publish none) are matched on normalized name + city + state
instead, and disclosed as such in match_basis. Listings with no usable name, or no city and state either, stay as
their own firm (match_basis 'single'), so nothing is merged on thin evidence. A firm that changes its
name or ZIP will appear as two firms; that is a known limit, not an error.

firm_id is a hash of the match key, so it is stable across quarters as long as the name and ZIP are.
Firm rows carry no contact fields.
"""
import csv
import hashlib
import re
from collections import defaultdict
from pathlib import Path

from rapidfuzz import fuzz

from pipeline.entity_resolver import normalize_zip
from pipeline.export import EXPORT_COLUMNS, normalize_state, scrub_emails

_LEGAL_TOKENS = {"llc", "inc", "incorporated", "corp", "corporation", "ltd", "limited", "co", "company",
                 "pllc", "llp", "lp", "pc", "pa", "the"}
NEAR_MATCH_MIN_SCORE = 90

FIRM_COLUMNS = [
    "firm_id", "business_name", "year_founded",
    "address_street", "address_city", "address_state", "address_zip", "latitude", "longitude",
    "industry", "naics_code", "certification", "description", "website",
    "instagram_handle", "facebook_url", "tiktok_handle", "yelp_url", "google_maps_url",
    "discloses_google_maps", "discloses_yelp", "discloses_instagram",
    "confidence", "certification_status", "first_certified", "certification_expires", "last_verified",
    "n_listings", "n_sources", "sources", "programs", "match_basis",
]
LISTING_COLUMNS = ["firm_id", "business_id", "source_id"] + EXPORT_COLUMNS


def firm_name_key(name: str) -> str:
    """Lowercase, drop apostrophes and periods (so L.L.C. -> llc), remove legal-form words, keep the rest."""
    s = (name or "").lower().replace("&", " and ")
    s = re.sub(r"[.'’`]", "", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(t for t in s.split() if t not in _LEGAL_TOKENS)


def _firm_id(key: str) -> str:
    return "F" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def listing_rows(con, snapshot_id: str) -> list[dict]:
    """The snapshot's listings as clean public-safe dicts (no contact fields), with source_id/business_id."""
    cols = ", ".join(EXPORT_COLUMNS)
    out = []
    for r in con.execute(
            f"SELECT business_id, source_id, {cols} FROM businesses WHERE snapshot_id = ? "
            f"ORDER BY source_id, business_id", [snapshot_id]).fetchall():
        d = scrub_emails(dict(zip(["business_id", "source_id"] + EXPORT_COLUMNS, r)))
        d["address_state"] = normalize_state(d["address_state"])
        out.append({k: ("" if v is None else v) for k, v in d.items()})
    return out


def _rank(d: dict):
    """Higher is better: current status, then a map location, a website, an industry code, an explicit
    ethnicity field, then the most recent observation. Ties break on source_id so the result is deterministic."""
    status = {"current": 2, "": 1, "former": 0}.get(d.get("certification_status", ""), 1)
    return (status, bool(d["latitude"] and d["longitude"]), bool(d["website"]), bool(d["naics_code"]),
            d["confidence"] == "confirmed_black", d["last_verified"], d["source_id"])


def _first(listings, col):
    for d in listings:
        if d.get(col):
            return d[col]
    return ""


def _city_key(d: dict) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (d.get("address_city") or "").lower()).strip()


def group_listings(listings: list[dict]):
    """Return (groups, lbasis): groups maps a match key to its listings; lbasis maps (source_id, business_id)
    to how that listing was matched ('name+zip', 'name+city+state' or 'single').

    Pass 1: normalized name + 5-digit ZIP. Pass 2, ONLY for listings with no ZIP (Florida, Atlanta and
    Oregon publish none): normalized name + city + state, attached to a ZIP-matched firm when exactly one
    such firm exists, else grouped with other ZIP-less listings sharing the same name + city + state.
    """
    groups, lbasis = defaultdict(list), {}
    pending = []
    for d in listings:
        nk = firm_name_key(d["business_name"])
        zp = normalize_zip(d["address_zip"]) if d["address_zip"] else ""
        if nk and zp:
            groups[f"{nk}|{zp}"].append(d)
            lbasis[(d["source_id"], d["business_id"])] = "name+zip"
        else:
            pending.append((d, nk))
    idx = defaultdict(set)      # (name, city, state) -> ZIP-matched keys carrying it
    for key, members in groups.items():
        nk = key.split("|")[0]
        for m in members:
            c, st = _city_key(m), m["address_state"]
            if c and st:
                idx[(nk, c, st)].add(key)
    loose = defaultdict(list)
    for d, nk in pending:
        c, st = _city_key(d), d["address_state"]
        k2 = (nk, c, st)
        if nk and c and st:
            if len(idx.get(k2, ())) == 1:
                groups[next(iter(idx[k2]))].append(d)
                lbasis[(d["source_id"], d["business_id"])] = "name+city+state"
            else:
                loose[f"c|{nk}|{c}|{st}"].append(d)
        else:
            groups[f"single|{d['source_id']}|{d['business_id']}"].append(d)
            lbasis[(d["source_id"], d["business_id"])] = "single"
    for key, members in loose.items():
        groups[key].extend(members)
        for m in members:
            lbasis[(m["source_id"], m["business_id"])] = "name+city+state" if len(members) > 1 else "single"
    return groups, lbasis


def build_firms(listings: list[dict]):
    """-> (firms, firm_map_rows, near_matches)."""
    groups, lbasis = group_listings(listings)
    firms, fmap = [], []
    for key, members in groups.items():
        members = sorted(members, key=_rank, reverse=True)
        rep = members[0]
        fid = _firm_id(key)
        bases = sorted({lbasis[(m["source_id"], m["business_id"])] for m in members})
        statuses = [m["certification_status"] for m in members]
        if "current" in statuses:
            status = "current"
        elif all(s == "former" for s in statuses):
            status = "former"
        else:
            status = ""
        starts = [m["first_certified"] for m in members if m["first_certified"]]
        ends = [m["certification_expires"] for m in members if m["certification_expires"]]
        sources = sorted({m["source_id"] for m in members})
        programs = sorted({m["data_source"] for m in members if m["data_source"]})
        certs = sorted({m["certification"] for m in members if m["certification"]})
        firm = {c: _first(members, c) for c in FIRM_COLUMNS if c in EXPORT_COLUMNS}
        firm.update({
            "firm_id": fid, "business_name": rep["business_name"],
            "address_street": rep["address_street"], "address_city": rep["address_city"],
            "address_state": rep["address_state"], "address_zip": rep["address_zip"],
            "latitude": rep["latitude"], "longitude": rep["longitude"],
            "certification": "; ".join(certs),
            "confidence": "confirmed_black" if any(m["confidence"] == "confirmed_black" for m in members) else rep["confidence"],
            "certification_status": status,
            "first_certified": min(starts) if starts else "", "certification_expires": max(ends) if ends else "",
            "last_verified": max(m["last_verified"] for m in members),
            "n_listings": len(members), "n_sources": len(sources),
            "sources": ";".join(sources), "programs": "; ".join(programs), "match_basis": "; ".join(bases),
        })
        for flag in ("discloses_google_maps", "discloses_yelp", "discloses_instagram"):
            firm[flag] = _first(members, flag)
        firms.append(firm)
        for m in members:
            fmap.append({"firm_id": fid, "business_id": m["business_id"], "source_id": m["source_id"],
                         "match_basis": lbasis[(m["source_id"], m["business_id"])]})
    firms.sort(key=lambda f: (f["business_name"].lower(), f["firm_id"]))
    return firms, fmap, near_matches(firms)


def near_matches(firms: list[dict]) -> list[dict]:
    """Same-ZIP firms whose names are similar but not equal after normalization. Never merged."""
    by_zip = defaultdict(list)
    for f in firms:
        z = normalize_zip(f["address_zip"]) if f["address_zip"] else ""
        k = firm_name_key(f["business_name"])
        if z and k:
            by_zip[z].append((k, f))
    out = []
    for z, items in by_zip.items():
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                (ka, fa), (kb, fb) = items[i], items[j]
                if ka == kb:
                    continue
                score = fuzz.token_sort_ratio(ka, kb)
                if score >= NEAR_MATCH_MIN_SCORE:
                    out.append({"score": round(score), "zip": z,
                                "firm_id_a": fa["firm_id"], "name_a": fa["business_name"], "sources_a": fa["sources"],
                                "firm_id_b": fb["firm_id"], "name_b": fb["business_name"], "sources_b": fb["sources"]})
    out.sort(key=lambda r: (-r["score"], r["zip"], r["name_a"]))
    return out


def write_firm_tables(con, snapshot_id: str, firms: list[dict], fmap: list[dict]) -> None:
    """Replace this snapshot's rows in the `firms` and `firm_map` tables (created on first use)."""
    con.execute("CREATE TABLE IF NOT EXISTS firms (snapshot_id VARCHAR, " +
                ", ".join(f"{c} {'INTEGER' if c.startswith('n_') else 'VARCHAR'}" for c in FIRM_COLUMNS) + ")")
    con.execute("CREATE TABLE IF NOT EXISTS firm_map (snapshot_id VARCHAR, firm_id VARCHAR, business_id VARCHAR, "
                "source_id VARCHAR, match_basis VARCHAR)")
    con.execute("DELETE FROM firms WHERE snapshot_id = ?", [snapshot_id])
    con.execute("DELETE FROM firm_map WHERE snapshot_id = ?", [snapshot_id])
    # Bulk-load through a temporary CSV: DuckDB's row-at-a-time executemany takes minutes for 70k rows.
    import tempfile, os
    with tempfile.TemporaryDirectory() as td:
        fp, mp = os.path.join(td, "firms.csv"), os.path.join(td, "map.csv")
        _write_csv(Path(fp), ["snapshot_id"] + FIRM_COLUMNS, [dict(f, snapshot_id=snapshot_id) for f in firms])
        _write_csv(Path(mp), ["snapshot_id", "firm_id", "business_id", "source_id", "match_basis"],
                   [dict(m, snapshot_id=snapshot_id) for m in fmap])
        con.execute(f"INSERT INTO firms SELECT * FROM read_csv('{fp}', header=true, all_varchar=true, "
                    f"nullstr='NULL_NEVER_USED')")
        con.execute(f"INSERT INTO firm_map SELECT * FROM read_csv('{mp}', header=true, all_varchar=true, "
                    f"nullstr='NULL_NEVER_USED')")


def refresh_all_snapshots(con) -> dict:
    """Build firm tables for every snapshot in the database; return the latest snapshot's rows."""
    latest = None
    for (sid,) in con.execute("SELECT DISTINCT snapshot_id FROM businesses ORDER BY 1").fetchall():
        listings = listing_rows(con, sid)
        firms, fmap, near = build_firms(listings)
        write_firm_tables(con, sid, firms, fmap)
        latest = {"snapshot_id": sid, "listings": listings, "firms": firms, "fmap": fmap, "near": near}
    return latest or {"snapshot_id": None, "listings": [], "firms": [], "fmap": [], "near": []}


def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        w.writerows([[r.get(c, "") for c in columns] for r in rows])


def export_firm_files(latest: dict, data_dir: Path) -> None:
    """data/businesses.csv (one row per firm), data/listings.csv, data/review/firm-near-matches.csv."""
    fid_by_listing = {(m["source_id"], m["business_id"]): m["firm_id"] for m in latest["fmap"]}
    listings = [dict(d, firm_id=fid_by_listing[(d["source_id"], d["business_id"])]) for d in latest["listings"]]
    _write_csv(data_dir / "businesses.csv", FIRM_COLUMNS, latest["firms"])
    _write_csv(data_dir / "listings.csv", LISTING_COLUMNS, listings)
    _write_csv(data_dir / "review" / "firm-near-matches.csv",
               ["score", "zip", "firm_id_a", "name_a", "sources_a", "firm_id_b", "name_b", "sources_b"], latest["near"])
