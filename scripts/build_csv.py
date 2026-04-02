import openpyxl
import csv
import time
import uuid
import re
import argparse
from pathlib import Path

import requests

SOURCE_FILE = (
    Path.home()
    / "University of Michigan Dropbox"
    / "Kyle McCullers"
    / "Data"
    / "US State(s) Administrative Data"
    / "New York"
    / "NYC MWBE_9.9.2025.xlsx"
)
OUTPUT_FILE = Path("data/businesses.csv")

XLSX_HEADER_ROW = 6  # 1-indexed; rows 1-5 are metadata

OUTPUT_COLUMNS = [
    "business_id", "business_name", "owner_name", "year_founded",
    "address_street", "address_city", "address_state", "address_zip",
    "latitude", "longitude", "industry", "naics_code", "certification",
    "description", "website", "phone", "email",
    "instagram_handle", "facebook_url", "tiktok_handle",
    "yelp_url", "google_maps_url",
    "discloses_google_maps", "discloses_yelp", "discloses_instagram",
    "data_source", "last_verified",
]

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "BlackBusinessResearchTable/1.0 (kylemcc@umich.edu)"}


def extract_year(value):
    """Return a 4-digit year string from a date object or string, or '' on failure."""
    if value is None:
        return ""
    if hasattr(value, "year"):
        return str(value.year)
    match = re.search(r"\b(19|20)\d{2}\b", str(value))
    return match.group() if match else ""


def load_and_filter(filepath=SOURCE_FILE):
    """Load xlsx, filter Ethnicity == 'Black', return list of dicts."""
    wb = openpyxl.load_workbook(filepath, read_only=True)
    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))
    wb.close()

    header = all_rows[XLSX_HEADER_ROW - 1]  # 0-indexed
    col = {name: i for i, name in enumerate(header) if name}

    records = []
    for row in all_rows[XLSX_HEADER_ROW:]:  # data starts after header
        if row[col["Ethnicity"]] != "Black":
            continue
        records.append({
            "business_name": row[col["Vendor Formal Name"]] or "",
            "owner_name": " ".join(
                filter(None, [row[col["First Name"]], row[col["Last Name"]]])
            ),
            "year_founded": extract_year(row[col["Date of Establishment"]]),
            "address_street": row[col["Address Line 1"]] or "",
            "address_city": row[col["City"]] or "",
            "address_state": row[col["State"]] or "",
            "address_zip": str(row[col["Zip"]] or "").strip(),
            "industry": row[col["NAICS Sector"]] or "",
            "naics_code": str(row[col["6 digit NAICS code"]] or "").strip(),
            "certification": (row[col["Certification"]] or "").strip(),
            "description": row[col["Business Description"]] or "",
            "website": row[col["Website"]] or "",
            "phone": row[col["Telephone"]] or "",
            "email": row[col["Email"]] or "",
        })
    return records


def geocode_address(street, city, state, zip_code):
    """Return (lat_str, lon_str) or ('', '') on any failure."""
    query = f"{street}, {city}, {state} {zip_code}"
    params = {"q": query, "format": "json", "limit": 1, "countrycodes": "us"}
    try:
        resp = requests.get(
            NOMINATIM_URL, params=params, headers=NOMINATIM_HEADERS, timeout=10
        )
        resp.raise_for_status()
        results = resp.json()
        if results:
            return str(results[0]["lat"]), str(results[0]["lon"])
    except Exception:
        pass
    return "", ""


def build_csv(source=SOURCE_FILE, output=OUTPUT_FILE, sample=None):
    """Main pipeline: load → filter → geocode → write CSV."""
    output.parent.mkdir(parents=True, exist_ok=True)
    records = load_and_filter(source)
    if sample is not None:
        records = records[:sample]

    print(f"Processing {len(records)} records → {output}")
    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for i, rec in enumerate(records):
            if i > 0:
                time.sleep(1)  # Nominatim rate limit: 1 req/sec
            lat, lon = geocode_address(
                rec["address_street"], rec["address_city"],
                rec["address_state"], rec["address_zip"]
            )
            row = {field: "" for field in OUTPUT_COLUMNS}
            row.update(rec)
            row["business_id"] = str(uuid.uuid4())[:8]
            row["latitude"] = lat
            row["longitude"] = lon
            row["data_source"] = "NYC MWBE Directory 2025"
            row["last_verified"] = "2025-09-09"
            writer.writerow(row)
            if (i + 1) % 100 == 0:
                print(f"  {i + 1}/{len(records)} geocoded...")

    print(f"Done. {len(records)} records written to {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build businesses.csv from NYC MWBE xlsx")
    parser.add_argument("--sample", type=int, default=None, help="Process only N records")
    parser.add_argument("--output", type=str, default=str(OUTPUT_FILE))
    args = parser.parse_args()
    build_csv(output=Path(args.output), sample=args.sample)
