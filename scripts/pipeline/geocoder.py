import csv
import io
import time
import warnings

import requests

CENSUS_URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
BATCH_SIZE = 10_000  # Census API limit per request


def batch_geocode(records: list[dict]) -> dict[str, tuple[float, float]]:
    """
    Geocode records that are missing latitude/longitude.

    Only submits records where latitude or longitude is blank.
    Records that already have both coordinates are silently skipped.

    Args:
        records: list of dicts with keys business_id, address_street,
                 address_city, address_state, address_zip, and optionally
                 latitude/longitude.

    Returns:
        dict mapping business_id -> (latitude, longitude) for
        successfully geocoded records.
    """
    to_geocode = [
        r for r in records
        if not (r.get("latitude", "").strip() and r.get("longitude", "").strip())
    ]
    if not to_geocode:
        return {}

    results: dict[str, tuple[float, float]] = {}

    for batch_start in range(0, len(to_geocode), BATCH_SIZE):
        batch = to_geocode[batch_start: batch_start + BATCH_SIZE]
        results.update(_geocode_batch(batch))

    return results


def _geocode_batch(records: list[dict]) -> dict[str, tuple[float, float]]:
    buf = io.StringIO()
    writer = csv.writer(buf)
    for r in records:
        biz_id = r.get("business_id", "").strip()
        if not biz_id:
            warnings.warn(f"Skipping record with missing business_id in geocoder: {r.get('business_name')!r}")
            continue
        writer.writerow([
            biz_id,
            r.get("address_street", ""),
            r.get("address_city", ""),
            r.get("address_state", ""),
            r.get("address_zip", ""),
        ])

    csv_payload = buf.getvalue()
    for attempt in range(3):
        response = requests.post(
            CENSUS_URL,
            data={"benchmark": "Public_AR_Current"},
            files={"addressFile": ("addresses.csv", csv_payload, "text/csv")},
            timeout=300,
        )
        if response.ok:
            break
        wait = 10 * (2 ** attempt)
        warnings.warn(f"Census Geocoder returned {response.status_code}; retrying in {wait}s (attempt {attempt + 1}/3)")
        time.sleep(wait)
    response.raise_for_status()

    results: dict[str, tuple[float, float]] = {}
    # Census batch geocoder response format (per Census API docs):
    # col 0: input record ID
    # col 1: input address
    # col 2: match status ("Match" | "No_Match" | "Tie")
    # col 3: match type ("Exact" | "Non_Exact")
    # col 4: matched address
    # col 5: coordinates as "lon,lat"
    # col 6: TIGER line ID
    # col 7: side of street
    reader = csv.reader(io.StringIO(response.text))
    for row in reader:
        if len(row) < 6:
            continue
        record_id = row[0].strip()
        match_status = row[2].strip().lower()
        coords = row[5].strip()
        if match_status == "match" and coords:
            try:
                lon_str, lat_str = coords.split(",")
                results[record_id] = (float(lat_str.strip()), float(lon_str.strip()))
            except (ValueError, AttributeError):
                continue

    return results
