"""Build the publishable copy of the research DB.

The working DB keeps everything the adapters ingest. The copy that is uploaded to the
GitHub Release must not carry contact data (DECISIONS 2026-09-23), so this writes a FRESH
file (not an in-place UPDATE: DuckDB can leave old values in freed blocks) with:
  - owner_name / phone / email set NULL
  - source_fields (the raw source row, which holds names, phones, emails) set NULL
  - email addresses cut from website / description / address_street
Usage: python -m pipeline.public_db SRC.duckdb DEST.duckdb
"""
import sys
from pathlib import Path

from pipeline.db import open_db
from pipeline.export import PRIVATE_COLUMNS

_EMAIL = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}"
_TABLES = ["sources", "snapshots", "business_registry", "field_catalog"]


def build_public_db(src: Path, dest: Path) -> None:
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest.unlink()
    con = open_db(dest)
    con.execute(f"ATTACH '{Path(src)}' AS s (READ_ONLY)")
    for t in _TABLES:
        if t == "snapshots":
            # Rows written before the carry-forward migration hold NULL here; the
            # fresh schema is NOT NULL DEFAULT '[]'.
            con.execute(
                "INSERT INTO snapshots SELECT snapshot_id, run_date, records_added, "
                "records_dropped, sources_run, sources_failed, "
                "COALESCE(sources_carried_forward, '[]') FROM s.snapshots"
            )
        else:
            con.execute(f"INSERT INTO {t} SELECT * FROM s.{t}")
    cols = [r[1] for r in con.execute("PRAGMA table_info('businesses')").fetchall()]
    exprs = []
    for c in cols:
        if c in PRIVATE_COLUMNS or c == "source_fields":
            exprs.append(f"NULL AS {c}")
        elif c == "website":
            exprs.append(
                f"CASE WHEN regexp_full_match(regexp_replace(trim(website), "
                f"'(?i)^(mailto:|https?://)', ''), '{_EMAIL}/?') THEN NULL ELSE website END AS {c}"
            )
        elif c in ("description", "address_street"):
            exprs.append(f"trim(regexp_replace({c}, '{_EMAIL}', '', 'g')) AS {c}")
        else:
            exprs.append(c)
    con.execute(
        f"INSERT INTO businesses SELECT {', '.join(exprs)} FROM s.businesses"
    )
    con.execute("DETACH s")
    con.execute("CHECKPOINT")
    con.close()


if __name__ == "__main__":
    build_public_db(Path(sys.argv[1]), Path(sys.argv[2]))
