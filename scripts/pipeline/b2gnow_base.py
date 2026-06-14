"""
Shared base for B2Gnow / dbesystem / mwdbe / diversitycompliance directory
adapters.

Most state DOT DBE directories and many city MWBE directories run on the B2Gnow
platform, served under three interchangeable domains that share one export
engine:
  - *.dbesystem.com          (state DOT / UCP DBE programs)
  - *.mwdbe.com              (city MWBE programs)
  - *.diversitycompliance.com (city / agency programs)

Access is a manual Excel/CSV export ("Download Results to Excel") — the portal is
session/viewstate-bound and not cleanly auto-fetchable, so these are
manual-capture sources (a human downloads the full directory; the pipeline reads
whatever file is present and carries the source forward on quarters with no file).

Two export shapes are produced by the platform and both are handled here:
  1. CSV  — latin-1, a title/preamble precedes a header row located dynamically
            by a marker column; duplicate City/State/Zip columns (physical +
            mailing) collapse to the first occurrence (physical address).
  2. ".xls" that is actually HTML — a single <table> whose first <tr> is the
            header (e.g. the diversitycompliance.com tenants). Parsed with the
            stdlib HTML parser (no extra dependency).

A subclass sets the source identity, the file glob, the filter (which column and
which values mark a Black-owned firm), and the FIELD_MAP. Black firms are filtered
either on an explicit Ethnicity column ("Black" / "Black American") or, where the
directory has no ethnicity column, on a Black-specific certification-type code
(e.g. Atlanta's "AABE" = African American Business Enterprise).
"""
import csv
import os
from datetime import date
from glob import glob
from html.parser import HTMLParser
from pathlib import Path

from pipeline.adapter_base import AdapterBase

MANUAL_DIR = (
    Path.home() / "University of Michigan Dropbox" / "Kyle McCullers"
    / "Projects and Proposals" / "Black Business Research Table"
    / "data" / "manual downloads"
)

# Federal DBE presumed-group Black labels + common city-program variants, lowercased.
BLACK_ETHNICITY_VALUES = frozenset({"black", "black american"})


class B2GnowAdapter(AdapterBase):
    # ── subclass configuration ────────────────────────────────────────────────
    FILE_GLOB    = ""                       # glob within MANUAL_DIR (or SOURCE_DIR)
    SOURCE_DIR   = MANUAL_DIR               # override for non-default folders
    FILE_ENV     = ""                       # optional env-var override of the path
    FILTER_FIELD = "Ethnicity"             # column the Black filter is applied to
    FILTER_VALUES = BLACK_ETHNICITY_VALUES  # accepted values (compared lowercased)
    ADDRESS_MODE = "fields"                # "fields" (Physical Address/City/State/Zip)
                                            #   or "location" (single "Location" = "City, ST")
    DEDUP_FIELDS = ("Company Name", "Physical Address")
    DEFAULT_CERT = "DBE"
    CONFIDENCE   = "confirmed_black"

    def __init__(self, file_path: Path = None):
        path = file_path or (os.environ.get(self.FILE_ENV, "") if self.FILE_ENV else "")
        if path:
            self._file_path = Path(path)
        else:
            matches = sorted(glob(str(self.SOURCE_DIR / self.FILE_GLOB)))
            if not matches:
                raise FileNotFoundError(
                    f"{self.SOURCE_NAME} file not found. Save the directory export to "
                    f"'{self.SOURCE_DIR}' (matching '{self.FILE_GLOB}')"
                    + (f" or set {self.FILE_ENV}." if self.FILE_ENV else ".")
                )
            self._file_path = Path(matches[-1])  # newest by date-stamped name
        if not self._file_path.exists():
            raise FileNotFoundError(f"{self.SOURCE_NAME} file not found: {self._file_path}")

    def fetch(self) -> list[dict]:
        rows = read_b2gnow_rows(self._file_path)
        col, _, data = locate(rows, self.FILTER_FIELD, self._file_path)
        fi = col[self.FILTER_FIELD]
        dedup_idx = [col[f] for f in self.DEDUP_FIELDS if f in col]

        seen, out = set(), []
        for row in data:
            if len(row) <= fi or row[fi].strip().lower() not in self.FILTER_VALUES:
                continue
            rec = {name: (row[i] if i < len(row) else "") for name, i in col.items()}
            key = tuple((row[i].strip().lower() if i < len(row) else "") for i in dedup_idx)
            if key in seen:
                continue
            seen.add(key)
            out.append(rec)
        return out

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for sr in raw:
            rec = self.map_record(sr)
            first = (sr.get("Owner First") or "").strip()
            last = (sr.get("Owner Last") or "").strip()
            rec["owner_name"] = " ".join(filter(None, [first, last]))
            if self.ADDRESS_MODE == "location":
                loc = (sr.get("Location") or "").strip()
                if "," in loc:
                    city, st = loc.rsplit(",", 1)
                    rec["address_city"] = city.strip()
                    rec["address_state"] = st.strip()
                elif loc:
                    rec["address_city"] = loc
            if not rec.get("certification"):
                rec["certification"] = self.DEFAULT_CERT
            rec["last_verified"] = str(date.today())
            records.append(rec)
        return records


# ── shared readers / locators ─────────────────────────────────────────────────

class _TableExtractor(HTMLParser):
    """Collect the rows of the FIRST <table> as a list of lists of cell text."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows, self._row, self._cell = [], None, None
        self._in_table = False
        self._done = False

    def handle_starttag(self, tag, attrs):
        if self._done:
            return
        if tag == "table":
            self._in_table = True
        elif tag == "tr" and self._in_table:
            self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag):
        if self._done:
            return
        if tag in ("td", "th") and self._cell is not None:
            self._row.append("".join(self._cell).strip())
            self._cell = None
        elif tag == "tr" and self._row is not None:
            self.rows.append(self._row)
            self._row = None
        elif tag == "table" and self._in_table:
            self._in_table = False
            self._done = True   # first table only


def read_b2gnow_rows(path) -> list[list]:
    """Return rows (list of lists of strings) from a B2Gnow CSV or HTML('.xls') export."""
    head = open(path, "rb").read(256).lower()
    if b"<table" in head or b"<tr" in head or b"<strong" in head or b"<html" in head:
        parser = _TableExtractor()
        parser.feed(open(path, encoding="latin-1").read())
        return parser.rows
    with open(path, encoding="latin-1", newline="") as f:
        return list(csv.reader(f))


def locate(rows, marker, path):
    """Find the header row carrying `marker`; return (first-occurrence col map,
    marker index, data rows after the header). First occurrence wins, so duplicate
    columns (e.g. physical vs mailing City/State/Zip) resolve to the first."""
    header_idx = None
    for i, row in enumerate(rows):
        if any((c or "").strip() == marker for c in row):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError(f"No header row containing '{marker}' in {path}")
    header = rows[header_idx]
    col = {}
    for i, name in enumerate(header):
        name = (name or "").strip()
        if name and name not in col:
            col[name] = i
    return col, col[marker], rows[header_idx + 1:]
