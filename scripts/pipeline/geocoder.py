import csv
import io

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
        writer.writerow([
            r["business_id"],
            r.get("address_street", ""),
            r.get("address_city", ""),
            r.get("address_state", ""),
            r.get("address_zip", ""),
        ])

    response = requests.post(
        CENSUS_URL,
        data={"benchmark": "Public_AR_Current"},
        files={"addressFile": ("addresses.csv", buf.getvalue(), "text/csv")},
        timeout=300,
    )
    response.raise_for_status()

    results: dict[str, tuple[float, float]] = {}
    reader = csv.reader(io.StringIO(response.text))
    for row in reader:
        if len(row) < 6:
            continue
        record_id = row[0].strip()
        match_status = row[2].strip().lower()
        coords = row[5].strip() if len(row) > 5 else ""
        if match_status == "match" and coords:
            try:
                lon_str, lat_str = coords.split(",")
                results[record_id] = (float(lat_str.strip()), float(lon_str.strip()))
            except (ValueError, AttributeError):
                continue

    return results
