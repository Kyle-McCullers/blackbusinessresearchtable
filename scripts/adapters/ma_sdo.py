"""
Massachusetts SDO (Supplier Diversity Office) adapter.

Source: MA Supplier Diversity Office Certified Business Directory
File: Massachusetts MWBE_9.9.2025.xls (HTML disguised as XLS — government export format)
Filter: BusinessEthnicity_Description == "3-African American, Black"
        AND Is_MBE_Certified == "True"
Confidence: confirmed_black — race/ethnicity is an explicit SDO certification field.

The file is HTML inside an .xls wrapper. openpyxl and xlrd cannot read it;
it is parsed directly as HTML.

Updated files are available at:
https://www.diversitycertification.mass.gov/BusinessDirectory/BusinessDirectorySearch.aspx
Set Certification Type = "MBE", Ethnicity = "African American, Black" → Export to Excel.
Set MA_SDO_FILE env var to the downloaded path.
"""
import html
import os
import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.adapter_base import AdapterBase

DEFAULT_FILE_ENV = "MA_SDO_FILE"
ENCODING = "latin-1"
BLACK_ETHNICITY = "3-African American, Black"


class MaSdoAdapter(AdapterBase):
    SOURCE_ID   = "ma_sdo"
    SOURCE_NAME = "Massachusetts SDO MBE Directory"
    PROGRAM     = "MBE"
    GEOGRAPHY   = "Massachusetts"
    CONFIDENCE  = "confirmed_black"

    FIELD_MAP = {
        "Business_Name":          "business_name",
        "Business_AddressLine1":  "address_street",
        "Business_City":          "address_city",
        "Business_State":         "address_state",
        "Business_Zip5":          "address_zip",
        "Business_Phone":         "phone",
        "BusinessContact_Email":  "email",
        "Business_WebsiteURL":    "website",
        "SDO_Primary_NAICS_Code": "naics_code",
    }

    def __init__(self, file_path: Path = None):
        path = file_path or os.environ.get(DEFAULT_FILE_ENV, "")
        if not path:
            raise ValueError(
                f"{DEFAULT_FILE_ENV} environment variable is required. "
                "Download the MBE directory from "
                "https://www.diversitycertification.mass.gov/BusinessDirectory/BusinessDirectorySearch.aspx "
                "(set Ethnicity = 'African American, Black') and set the env var."
            )
        self._file_path = Path(path)
        if not self._file_path.exists():
            raise FileNotFoundError(f"Massachusetts SDO file not found: {self._file_path}")

    def fetch(self) -> list[dict]:
        """
        Parse the HTML-in-XLS export from the MA SDO portal.
        Returns one dict per MBE-certified Black-owned business.
        """
        content = self._file_path.read_text(encoding=ENCODING)

        # Extract column headers from the bold grey header row
        header_match = re.search(
            r'<tr[^>]*background-color:LightGrey[^>]*>(.*?)</tr>',
            content, re.DOTALL | re.IGNORECASE
        )
        if not header_match:
            return []
        headers = [re.sub(r'<[^>]+>', '', h).strip()
                   for h in re.findall(r'<td[^>]*>(.*?)</td>',
                                       header_match.group(1), re.IGNORECASE)]

        eth_idx = headers.index("BusinessEthnicity_Description")
        mbe_idx = headers.index("Is_MBE_Certified")

        # All <tr> blocks after the title row
        all_rows = re.findall(
            r'<tr(?![^>]*LightGrey)[^>]*>(.*?)</tr>',
            content, re.DOTALL | re.IGNORECASE
        )
        data_rows = all_rows[1:]  # first match is the page title row

        rows = []
        for raw_row in data_rows:
            cells = [html.unescape(re.sub(r'<[^>]+>', '', c)).strip()
                     for c in re.findall(r'<td[^>]*>(.*?)</td>',
                                         raw_row, re.IGNORECASE | re.DOTALL)]
            if len(cells) <= max(eth_idx, mbe_idx):
                continue
            if cells[eth_idx] != BLACK_ETHNICITY:
                continue
            if cells[mbe_idx].lower() != "true":
                continue
            # Replace non-breaking space placeholder
            row = {headers[i]: ("" if cells[i] == "\xa0" else cells[i])
                   for i in range(min(len(headers), len(cells)))}
            rows.append(row)

        return rows

    def parse(self, raw: list[dict]) -> list[dict]:
        records = []
        for source_row in raw:
            record = self.map_record(source_row)

            first = source_row.get("BusinessContact_FirstName", "").strip()
            last = source_row.get("BusinessContact_LastName", "").strip()
            record["owner_name"] = " ".join(filter(None, [first, last]))

            record["source_business_id"] = source_row.get("Business_ClientID", "").strip()
            record["certification"] = "MBE"
            record["last_verified"] = "2025-09-09"  # file date
            records.append(record)
        return records
