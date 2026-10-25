import json
from datetime import date
from pathlib import Path

import duckdb

DB_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "bbrt.duckdb"

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS sources (
        source_id      VARCHAR PRIMARY KEY,
        source_name    VARCHAR NOT NULL,
        program        VARCHAR NOT NULL,
        geography      VARCHAR NOT NULL,
        confidence     VARCHAR NOT NULL,
        first_ingested DATE    NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS snapshots (
        snapshot_id     VARCHAR PRIMARY KEY,
        run_date        DATE    NOT NULL,
        records_added   INTEGER NOT NULL,
        records_dropped INTEGER NOT NULL,
        sources_run     VARCHAR NOT NULL,
        sources_failed  VARCHAR NOT NULL,
        sources_carried_forward VARCHAR NOT NULL DEFAULT '[]'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS businesses (
        business_id           VARCHAR NOT NULL,
        snapshot_id           VARCHAR NOT NULL,
        source_id             VARCHAR NOT NULL,
        source_business_id    VARCHAR,
        confidence            VARCHAR NOT NULL,
        business_name         VARCHAR,
        owner_name            VARCHAR,
        year_founded          VARCHAR,
        address_street        VARCHAR,
        address_city          VARCHAR,
        address_state         VARCHAR,
        address_zip           VARCHAR,
        latitude              VARCHAR,
        longitude             VARCHAR,
        industry              VARCHAR,
        naics_code            VARCHAR,
        certification         VARCHAR,
        description           VARCHAR,
        website               VARCHAR,
        phone                 VARCHAR,
        email                 VARCHAR,
        instagram_handle      VARCHAR,
        facebook_url          VARCHAR,
        tiktok_handle         VARCHAR,
        yelp_url              VARCHAR,
        google_maps_url       VARCHAR,
        discloses_google_maps VARCHAR,
        discloses_yelp        VARCHAR,
        discloses_instagram   VARCHAR,
        data_source           VARCHAR,
        last_verified         VARCHAR,
        source_fields         VARCHAR,
        certification_status  VARCHAR,
        first_certified       VARCHAR,
        certification_expires VARCHAR,
        PRIMARY KEY (business_id, snapshot_id, source_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS business_registry (
        business_id        VARCHAR PRIMARY KEY,
        canonical_name     VARCHAR NOT NULL,
        canonical_zip      VARCHAR NOT NULL,
        source_id          VARCHAR NOT NULL,
        source_business_id VARCHAR,
        first_seen         VARCHAR NOT NULL,
        last_seen          VARCHAR NOT NULL,
        is_active          BOOLEAN NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS merged_records (
        old_business_id  VARCHAR NOT NULL,
        kept_business_id VARCHAR NOT NULL,
        source_id        VARCHAR NOT NULL,
        reason           VARCHAR NOT NULL,
        merged_on        DATE    NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS field_catalog (
        source_id         VARCHAR NOT NULL,
        source_field_name VARCHAR NOT NULL,
        normalized_to     VARCHAR NOT NULL,
        coverage_pct      FLOAT   NOT NULL,
        PRIMARY KEY (source_id, source_field_name)
    )
    """,
]


def open_db(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Open (or create) the DuckDB database and ensure all tables exist."""
    con = duckdb.connect(str(db_path))
    for stmt in _DDL:
        con.execute(stmt)
    # Migration: add carried-forward tracking to pre-existing snapshots tables.
    # DuckDB rejects constraints on ALTER ADD COLUMN, so add a plain column here
    # (fresh DBs already get the NOT NULL DEFAULT version from CREATE TABLE above;
    # this only fires for DBs created before the column existed).
    con.execute(
        "ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS "
        "sources_carried_forward VARCHAR"
    )
    # Migration: certification-status columns (appended last so migrated and fresh
    # tables share one column order).
    for col in ("certification_status", "first_certified", "certification_expires"):
        con.execute(f"ALTER TABLE businesses ADD COLUMN IF NOT EXISTS {col} VARCHAR")
    return con


def upsert_source(con: duckdb.DuckDBPyConnection, adapter) -> None:
    """Insert source row if not already present."""
    con.execute(
        """
        INSERT INTO sources VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (source_id) DO NOTHING
        """,
        [adapter.SOURCE_ID, adapter.SOURCE_NAME, adapter.PROGRAM,
         adapter.GEOGRAPHY, adapter.CONFIDENCE, date.today()],
    )


def write_businesses(
    con: duckdb.DuckDBPyConnection,
    records: list[dict],
    snapshot_id: str,
) -> None:
    """Insert all records for a snapshot. Each record must have business_id set."""
    for r in records:
        con.execute(
            """
            INSERT INTO businesses VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            ) ON CONFLICT DO NOTHING
            """,
            [
                r.get("business_id", ""),
                snapshot_id,
                r.get("source_id", ""),
                r.get("source_business_id", ""),
                r.get("confidence", ""),
                r.get("business_name", ""),
                r.get("owner_name", ""),
                r.get("year_founded", ""),
                r.get("address_street", ""),
                r.get("address_city", ""),
                r.get("address_state", ""),
                r.get("address_zip", ""),
                r.get("latitude", ""),
                r.get("longitude", ""),
                r.get("industry", ""),
                r.get("naics_code", ""),
                r.get("certification", ""),
                r.get("description", ""),
                r.get("website", ""),
                r.get("phone", ""),
                r.get("email", ""),
                r.get("instagram_handle", ""),
                r.get("facebook_url", ""),
                r.get("tiktok_handle", ""),
                r.get("yelp_url", ""),
                r.get("google_maps_url", ""),
                r.get("discloses_google_maps", ""),
                r.get("discloses_yelp", ""),
                r.get("discloses_instagram", ""),
                r.get("data_source", ""),
                r.get("last_verified", ""),
                json.dumps(r.get("source_fields", {})),
                r.get("certification_status", ""),
                r.get("first_certified", ""),
                r.get("certification_expires", ""),
            ],
        )


# Column order matches the businesses table DDL above.
_BUSINESS_COLUMNS = [
    "business_id", "snapshot_id", "source_id", "source_business_id", "confidence",
    "business_name", "owner_name", "year_founded", "address_street", "address_city",
    "address_state", "address_zip", "latitude", "longitude", "industry", "naics_code",
    "certification", "description", "website", "phone", "email", "instagram_handle",
    "facebook_url", "tiktok_handle", "yelp_url", "google_maps_url",
    "discloses_google_maps", "discloses_yelp", "discloses_instagram", "data_source",
    "last_verified", "source_fields",
    "certification_status", "first_certified", "certification_expires",
]


def carry_forward_records(
    con: duckdb.DuckDBPyConnection,
    snapshot_id: str,
    observed_keys,
) -> list[dict]:
    """Return record dicts from the latest prior snapshot that were NOT observed
    in this run, so they stay in the record.

    RETENTION POLICY (DECISIONS 2026-10-15): once a business has been in the panel it
    stays in. Quarterly runs only add or update; a source dropping a firm, returning
    nothing, or failing outright never removes it. This is deliberate: a source can
    stop listing firms for reasons unrelated to the firms (program restructuring,
    politics, a changed export), and the panel must not inherit that.

    `observed_keys` is a set of (source_id, business_id) pairs seen this run. A prior
    row is carried unless its pair is in the set, so this covers a whole source that
    did not run, a source that ran but returned fewer firms, and a source that
    returned none. A carried row keeps its original `last_verified`, which is how a
    reader tells re-observed rows from retained ones. The caller stamps the new
    snapshot_id (via write_businesses) and must NOT bump the registry last_seen.
    """
    observed = set(observed_keys)
    # Latest snapshot at or before the current one. Using <= (not <>) means a
    # same-quarter re-run also carries from the rows already written this quarter.
    prior = con.execute(
        "SELECT MAX(snapshot_id) FROM businesses WHERE snapshot_id <= ?",
        [snapshot_id],
    ).fetchone()[0]
    if prior is None:
        return []
    cols = ", ".join(_BUSINESS_COLUMNS)
    rows = con.execute(
        f"SELECT {cols} FROM businesses WHERE snapshot_id = ?",
        [prior],
    ).fetchall()
    carried = []
    for row in rows:
        rec = dict(zip(_BUSINESS_COLUMNS, row))
        if (rec["source_id"], rec["business_id"]) in observed:
            continue
        try:
            rec["source_fields"] = json.loads(rec["source_fields"]) if rec["source_fields"] else {}
        except (TypeError, json.JSONDecodeError):
            rec["source_fields"] = {}
        rec["carried_forward"] = True
        carried.append(rec)
    return carried


def source_counts(con: duckdb.DuckDBPyConnection, snapshot_id: str) -> dict:
    """Records per source in the latest snapshot at or before `snapshot_id`."""
    prior = con.execute(
        "SELECT MAX(snapshot_id) FROM businesses WHERE snapshot_id <= ?", [snapshot_id]
    ).fetchone()[0]
    if prior is None:
        return {}
    return dict(con.execute(
        "SELECT source_id, COUNT(*) FROM businesses WHERE snapshot_id = ? GROUP BY 1",
        [prior],
    ).fetchall())


def write_snapshot_meta(
    con: duckdb.DuckDBPyConnection,
    snapshot_id: str,
    records_added: int,
    records_dropped: int,
    sources_run: list[str],
    sources_failed: list[str],
    sources_carried_forward: list[str] = None,
) -> None:
    con.execute(
        """
        INSERT INTO snapshots
            (snapshot_id, run_date, records_added, records_dropped,
             sources_run, sources_failed, sources_carried_forward)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (snapshot_id) DO NOTHING
        """,
        [snapshot_id, date.today(), records_added, records_dropped,
         json.dumps(sources_run), json.dumps(sources_failed),
         json.dumps(sources_carried_forward or [])],
    )


def get_registry(con: duckdb.DuckDBPyConnection) -> list[dict]:
    """Return all rows from business_registry as a list of dicts."""
    rows = con.execute(
        "SELECT business_id, canonical_name, canonical_zip, source_id, source_business_id, first_seen, last_seen, is_active FROM business_registry"
    ).fetchall()
    return [
        {
            "business_id": r[0], "canonical_name": r[1],
            "canonical_zip": r[2], "source_id": r[3],
            "source_business_id": r[4], "first_seen": r[5], "last_seen": r[6],
            "is_active": r[7],
        }
        for r in rows
    ]


def upsert_registry(
    con: duckdb.DuckDBPyConnection,
    snapshot_id: str,
    entries: list[dict],
) -> None:
    """
    Insert new registry entries or update last_seen for existing ones.
    entries: list of dicts with business_id, canonical_name, canonical_zip,
             source_id, source_business_id.
    """
    for e in entries:
        con.execute(
            """
            INSERT INTO business_registry
                (business_id, canonical_name, canonical_zip, source_id,
                 source_business_id, first_seen, last_seen, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
            ON CONFLICT (business_id) DO UPDATE SET
                last_seen = excluded.last_seen,
                is_active = TRUE
            """,
            [e["business_id"], e["canonical_name"], e["canonical_zip"],
             e["source_id"], e.get("source_business_id", ""),
             snapshot_id, snapshot_id],
        )
