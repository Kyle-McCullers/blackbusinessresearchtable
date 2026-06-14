"""
California certified MBE adapter (Black American), April 2026 snapshot.

Source: California Department of General Services / Cal eProcure certified-firm
directory, filtered to Ethnicity == "Black American" and exported to CSV
(`Black_MBE_4.2.2026`). The live CA directory is currently offline; this is a
loadable April-2026 snapshot. Manual-capture source (see or_cobid for the
pattern) — defaults to the historical admin-data folder, not the manual-downloads
folder, since this is a one-time snapshot.

Filter: Ethnicity == "Black American".
Confidence: confirmed_black — Ethnicity is an explicit, published per-firm field.

Distinct Ethnicity values observed (2026-04-02 snapshot — already pre-filtered):
  "Black American" (961)  [the export was pre-filtered to Black American]

File layout: a flat CSV with the header on the first row (no preamble), utf-8
with BOM. The "Primary Address" column packs street / "city zip" / "state county"
across embedded newlines, e.g. "1730 N. Wilton Place\\n Los Angeles 90028\\n CA 305\\n";
it is parsed best-effort into street/city/state/zip. Records are deduplicated on
vendor name.
"""
import csv
import os
import re
import sys
from datetime import date
from glob import glob
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DEFAULT_FILE_ENV = "CA_MBE_FILE"
SNAPSHOT_DIR = (
    Path.home() / "University of Michigan Dropbox" / "Kyle McCullers"
    / "Data" / "US State(s) Administrative Data" / "California"
)
FILE_GLOB = "Black_MBE*"
ENCODING = "utf-8-sig"
ETHNICITY_FIELD = "Ethnicity"
BLACK_VALUE = "Black American"
_ZIP_RE = re.compile(r"\b(\d{5})(?:-\d{4})?\b")


class CaMbeAdapter(AdapterBase):
    SOURCE_ID   = "ca_mbe"
    SOURCE_NAME = "California DGS Certified MBE (2026 snapshot)"
    PROGRAM     = "MBE"
    GEOGRAPHY   = "California"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "Vendor Name":      "business_name",
        "Contact":          "owner_name",
        "Contact Email":    "email",
        "Contact Phone":    "phone",
        "Website":          "website",
        "Business Activity": "description",
    }

    def __init__(self, file_path: Path = None):
        path = file_path or os.environ.get(DEFAULT_FILE_ENV, "")
        if path:
            self._file_path = Path(path)
        else:
            matches = sorted(glob(str(SNAPSHOT_DIR / FILE_GLOB)))
            if not matches:
                raise FileNotFoundError(
                    f"California MBE snapshot not found. Save it to '{SNAPSHOT_DIR}' "
                    f"(matching '{FILE_GLOB}') or set {DEFAULT_FILE_ENV}."
                )
            self._file_path = Path(matches[-1])
        if not self._file_path.exists():
            raise FileNotFoundError(f"California MBE snapshot not found: {self._file_path}")

    def fetch(self) -> list[dict]:
        with open(self._file_path, encoding=ENCODING, newline="") as f:
            rows = list(csv.reader(f))
        col, eth_i, data = _locate(rows, ETHNICITY_FIELD, self._file_path)

        seen, out = set(), []
        for row in data:
            if len(row) <= eth_i or row[eth_i].strip() != BLACK_VALUE:
                continue
            rec = {name: (row[i] if i < len(row) else "") for name, i in col.items()}
            key = rec.get("Vendor Name", "").strip().lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(rec)
        return out

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for sr in raw:
            rec = self.map_record(sr)
            street, city, state, zipc = _parse_address(sr.get("Primary Address", ""))
            rec["address_street"] = street
            rec["address_city"] = city
            rec["address_state"] = state
            rec["address_zip"] = zipc
            certs = (sr.get("Active Certifications") or "").replace("\r", " ").replace("\n", "; ").strip()
            rec["certification"] = certs or "MBE"
            rec["last_verified"] = str(date.today())
            records.append(rec)
        return records


def _parse_address(blob: str):
    """Best-effort parse of the multi-line Primary Address cell into
    (street, city, state, zip). Layout per line: street / 'city zip' / 'state ...'."""
    lines = [ln.strip() for ln in re.split(r"[\r\n]+", blob or "") if ln.strip()]
    street = lines[0] if lines else ""
    city = state = zipc = ""
    if len(lines) >= 2:
        m = _ZIP_RE.search(lines[1])
        if m:
            zipc = m.group(1)
            city = lines[1][:m.start()].strip()
        else:
            city = lines[1]
    if len(lines) >= 3:
        toks = lines[2].split()
        if toks:
            state = toks[0]
    return street, city, state, zipc


def _locate(rows, marker, path):
    """Find the header row carrying `marker`; return (first-occurrence col map,
    marker index, data rows after the header). Header names are stripped, so a
    trailing-space column like 'Vendor Name ' is keyed as 'Vendor Name'."""
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
