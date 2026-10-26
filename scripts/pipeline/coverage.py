"""What each source covers, and the sources.csv the site reads.

COVERAGE is the one place that says which jurisdiction a source defines. It is separate
from where the listed firms are based: a firm certified by Maryland can be headquartered
in Virginia, so counting states that merely contain a listed firm overstates coverage.

scope:
  statewide       the state's own certification / supplier-diversity directory
  transportation  a state or district DOT disadvantaged-business (DBE) directory only
  city            a single city or county program
  federal         national program
Adding an adapter without an entry here fails test_every_adapter_declares_coverage.
"""
import csv
from pathlib import Path

COVERAGE = {
    "al_ombe":         ("statewide",      "AL", ""),
    "ar_mwbe":         ("statewide",      "AR", ""),
    "ca_mbe":          ("statewide",      "CA", ""),
    "ct_das_smbe":     ("statewide",      "CT", ""),
    "de_osd":          ("statewide",      "DE", ""),
    "fl_mbe":          ("statewide",      "FL", ""),
    "in_idoa":         ("statewide",      "IN", ""),
    "ma_sdo":          ("statewide",      "MA", ""),
    "md_mbe":          ("statewide",      "MD", ""),
    "nc_hub":          ("statewide",      "NC", ""),
    "or_cobid":        ("statewide",      "OR", ""),
    "sc_smbcc":        ("statewide",      "SC", ""),
    "tx_hub":          ("statewide",      "TX", ""),
    "va_swam":         ("statewide",      "VA", ""),
    "dc_ucp":          ("transportation", "DC", ""),
    "hawaii_dbe":      ("transportation", "HI", ""),
    "nv_dbe":          ("transportation", "NV", ""),
    "pa_ucp_dbe":      ("transportation", "PA", ""),
    "atlanta_aabe":    ("city",           "GA", "Atlanta"),
    "baltimore_mwboo": ("city",           "MD", "Baltimore"),
    "chicago_mwbe":    ("city",           "IL", "Chicago"),
    "houston_obo":     ("city",           "TX", "Houston"),
    "nyc_mwbe":        ("city",           "NY", "New York City"),
    "sam_8a":          ("federal",        "",   ""),
}

SOURCES_COLUMNS = [
    "source_id", "source_name", "program", "scope", "state", "place",
    "records", "former_records", "retained_records", "last_observed",
]


def export_sources_csv(con, output_path: Path, snapshot_id: str) -> None:
    """One row per registered source for the snapshot, with its coverage declaration.

    retained_records = rows carried forward from an earlier quarter and not re-observed
    this run (last_verified older than the snapshot's run date).
    """
    run_date = con.execute(
        "SELECT CAST(run_date AS VARCHAR) FROM snapshots WHERE snapshot_id = ?",
        [snapshot_id]).fetchone()
    run_date = run_date[0] if run_date else "9999-12-31"
    rows = con.execute(
        """
        SELECT s.source_id, s.source_name, s.program,
               COUNT(b.business_id),
               COUNT(*) FILTER (WHERE b.certification_status = 'former'),
               COUNT(*) FILTER (WHERE b.last_verified < ?),
               COALESCE(MAX(b.last_verified), '')
        FROM sources s
        LEFT JOIN businesses b ON b.source_id = s.source_id AND b.snapshot_id = ?
        GROUP BY s.source_id, s.source_name, s.program
        ORDER BY s.source_id
        """, [run_date, snapshot_id]).fetchall()
    out = []
    for sid, name, program, n, former, retained, last_obs in rows:
        scope, state, place = COVERAGE.get(sid, ("unknown", "", ""))
        out.append([sid, name, program, scope, state, place, n, former, retained, last_obs])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(SOURCES_COLUMNS)
        w.writerows(out)


def export_meta_json(con, output_path: Path, snapshot_id: str) -> dict:
    """Small facts the site shows (snapshot id, run date, counts) so they are never typed by hand."""
    import json
    run = con.execute("SELECT CAST(run_date AS VARCHAR) FROM snapshots WHERE snapshot_id = ?",
                      [snapshot_id]).fetchone()
    records, former = con.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE certification_status = 'former') "
        "FROM businesses WHERE snapshot_id = ?", [snapshot_id]).fetchone()
    meta = {"snapshot_id": snapshot_id, "run_date": run[0] if run else "",
            "records": records, "former": former, "headline": records - former}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(meta, indent=2) + "\n")
    return meta
