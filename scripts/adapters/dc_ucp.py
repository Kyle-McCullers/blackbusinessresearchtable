"""
Washington DC Unified Certification Program (UCP) DBE directory adapter.

Source: DC UCP DBE directory, exported manually to an ".xls" that is actually an
Oracle APEX HTML page (NOT the standard B2Gnow template — different schema and
markup, so this adapter does its own parsing rather than using b2gnow_base).
Manual-capture source.

Filter: Ethnicity == "Black".
Confidence: confirmed_black — Ethnicity is an explicit, published per-firm field.

Distinct Ethnicity values observed (2026-06-14 full export, verbatim):
  "Black" (1,810), "Other" (666), "Hispanic" (380), "Asian Pacific" (369),
  "" (247), "Subcontinent Asian" (242), "Native American" (43)

File quirks handled here:
  - A single <table> whose data <tr> rows are NOT closed (only the header has
    </tr>), so rows are recovered by grouping the <td>/<th> cells in document
    order into fixed groups of 9 columns.
  - The Address column packs street / "City, ST ZIP" / Phone: / Fax: / Email: /
    Website: across <br/> tags inside one cell; <br/> is converted to newlines
    before tag-stripping so the parts can be split back out.
Columns: Cert Type, Certificate Number, Company Name, Address, Contact Name,
Contact Title, Ethnicity, Description of services, Certification Agency.
Records are deduplicated on company name + certificate number.
"""
import html as _html
import os
import re
import sys
from datetime import date
from glob import glob
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DEFAULT_FILE_ENV = "DC_UCP_FILE"
MANUAL_DIR = (
    Path.home() / "University of Michigan Dropbox" / "Kyle McCullers"
    / "Projects and Proposals" / "Black Business Research Table"
    / "data" / "manual downloads"
)
FILE_GLOB = "Washington DC UCP*"
NCOL = 9
BLACK_VALUE = "black"
_CSZ_RE = re.compile(r"^(.*),\s*([A-Za-z]{2})\.?\s+(\d{5})")


class DcUcpAdapter(AdapterBase):
    SOURCE_ID   = "dc_ucp"
    SOURCE_NAME = "DC UCP DBE"
    PROGRAM     = "DBE"
    GEOGRAPHY   = "District of Columbia"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "Company Name":            "business_name",
        "Cert Type":               "certification",
        "Description of services": "description",
    }

    def __init__(self, file_path: Path = None):
        path = file_path or os.environ.get(DEFAULT_FILE_ENV, "")
        if path:
            self._file_path = Path(path)
        else:
            matches = sorted(glob(str(MANUAL_DIR / FILE_GLOB)))
            if not matches:
                raise FileNotFoundError(
                    f"DC UCP file not found. Save the directory export to "
                    f"'{MANUAL_DIR}' (matching '{FILE_GLOB}') or set {DEFAULT_FILE_ENV}."
                )
            self._file_path = Path(matches[-1])
        if not self._file_path.exists():
            raise FileNotFoundError(f"DC UCP file not found: {self._file_path}")

    def fetch(self) -> list[dict]:
        cells = _extract_cells(self._file_path)
        if len(cells) < NCOL:
            return []
        header = cells[:NCOL]
        col = {name: i for i, name in enumerate(header)}
        eth_i = col.get("Ethnicity")
        rows = [cells[i:i + NCOL] for i in range(NCOL, len(cells) - NCOL + 1, NCOL)]

        seen, out = set(), []
        for row in rows:
            if eth_i is None or len(row) <= eth_i:
                continue
            if row[eth_i].strip().lower() != BLACK_VALUE:
                continue
            rec = {name: row[i] for name, i in col.items() if i < len(row)}
            key = (rec.get("Company Name", "").strip().lower(),
                   rec.get("Certificate Number", "").strip())
            if key in seen:
                continue
            seen.add(key)
            out.append(rec)
        return out

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for sr in raw:
            rec = self.map_record(sr)
            street, city, state, zipc, phone, email, website = _parse_address(sr.get("Address", ""))
            rec["address_street"] = street
            rec["address_city"] = city
            rec["address_state"] = state
            rec["address_zip"] = zipc
            rec["phone"] = phone
            rec["email"] = email
            rec["website"] = website
            rec["owner_name"] = (sr.get("Contact Name") or "").strip()
            # Cert Type / description cells can pack multiple values across <br/>;
            # collapse the recovered newlines for clean CSV output.
            rec["certification"] = " / ".join(
                p.strip() for p in rec.get("certification", "").split("\n") if p.strip())
            rec["description"] = " ".join(rec.get("description", "").split())
            if not rec.get("certification"):
                rec["certification"] = "DBE"
            rec["last_verified"] = str(date.today())
            records.append(rec)
        return records


def _extract_cells(path) -> list:
    """Return every <td>/<th> cell's text in document order, with <br/> inside a
    cell converted to newlines (to preserve the packed Address column)."""
    raw = open(path, encoding="latin-1").read()
    cells = []
    for m in re.finditer(r"<t[dh][^>]*>(.*?)</t[dh]>", raw, re.I | re.S):
        text = re.sub(r"<br\s*/?>", "\n", m.group(1), flags=re.I)
        text = re.sub(r"<[^>]+>", "", text)          # strip any remaining tags
        cells.append(_html.unescape(text).strip())
    return cells


def _parse_address(blob: str):
    """Parse the <br/>-packed Address cell into
    (street, city, state, zip, phone, email, website)."""
    lines = [ln.strip() for ln in (blob or "").split("\n") if ln.strip()]
    street = city = state = zipc = phone = email = website = ""
    for i, ln in enumerate(lines):
        low = ln.lower()
        if low.startswith("phone:"):
            phone = ln.split(":", 1)[1].strip()
        elif low.startswith("fax:"):
            continue
        elif low.startswith("email:"):
            email = ln.split(":", 1)[1].strip()
        elif low.startswith("website:"):
            w = ln.split(":", 1)[1].strip()
            website = "" if w in ("", "http://", "https://") else w
        else:
            m = _CSZ_RE.match(ln)
            if m and not city:
                city, state, zipc = m.group(1).strip(), m.group(2).upper(), m.group(3)
            elif not street:
                street = ln
    return street, city, state, zipc, phone, email, website
