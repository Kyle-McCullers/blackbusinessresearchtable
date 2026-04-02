import pytest
import csv
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent))
from build_csv import load_and_filter, geocode_address, build_csv, extract_year

import openpyxl


@pytest.fixture
def sample_xlsx(tmp_path):
    """Minimal xlsx that mimics NYC MWBE format (5 metadata rows, header row 6, data from row 7)."""
    filepath = tmp_path / "test_mwbe.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    # Rows 1-5: metadata
    ws.append(["Export Date:", "09/09/2025"])
    ws.append(["Matching Records:", 3])
    ws.append(["Search Parameters"])
    ws.append(["codecategory", "both"])
    ws.append([None])
    # Row 6: header
    ws.append([
        "Account Number", "Vendor Formal Name", "Vendor DBA",
        "First Name", "Last Name", "Telephone", "Email",
        "Business Description", "Certification", "Certification Renewal Date",
        "Ethnicity", "Address Line 1", "Address Line 2", "City", "State", "Zip",
        "Mailing Address Line 1", "Mailing Address Line 2", "Mailing City",
        "Mailing State", "Mailing Zip", "Website", "Date of Establishment",
        "Aggregate Bonding Limit", "Signatory to Union Contract(s)",
        "6 digit NAICS code", "NAICS Sector", "NAICS Subsector", "NAICS Title",
        "Types of Construction Projects Performed", "NIGP codes",
        "Largest Value of Contract"
    ])
    # Row 7: Black business
    ws.append([
        "ACC001", "Test Black Business", "", "Jane", "Doe", "212-555-0001",
        "jane@test.com", "A test business.", "MBE", "01/01/2026",
        "Black", "123 Main St", "", "Brooklyn", "New York", "11201",
        "", "", "", "", "", "http://test.com", "2015",
        "", "", "561990", "Services", "Administrative Services", "Other Services",
        "", "", ""
    ])
    # Row 8: Hispanic business (should be filtered out)
    ws.append([
        "ACC002", "Test Hispanic Business", "", "John", "Smith", "212-555-0002",
        "", "Another business.", "MBE", "01/01/2026",
        "Hispanic", "456 Broadway", "", "Manhattan", "New York", "10013",
        "", "", "", "", "", "", "2018",
        "", "", "722511", "Accommodation and Food Services", "Food Services", "Restaurants",
        "", "", ""
    ])
    # Row 9: Black business with no year
    ws.append([
        "ACC003", "Another Black Business", "", "Bob", "Jones", "212-555-0003",
        "", "Yet another.", "WBE", "01/01/2026",
        "Black", "789 Atlantic Ave", "", "Bronx", "New York", "10451",
        "", "", "", "", "", "", None,
        "", "", "236220", "Construction", "Building Construction", "Commercial Building",
        "", "", ""
    ])
    wb.save(filepath)
    return filepath


def test_extract_year_from_year_string():
    assert extract_year("2015") == "2015"

def test_extract_year_from_date_string():
    assert extract_year("01/15/2018") == "2018"

def test_extract_year_from_none():
    assert extract_year(None) == ""

def test_extract_year_from_date_object():
    from datetime import date
    assert extract_year(date(2020, 6, 1)) == "2020"

def test_load_and_filter_returns_only_black(sample_xlsx):
    records = load_and_filter(sample_xlsx)
    assert len(records) == 2

def test_load_and_filter_excludes_non_black(sample_xlsx):
    records = load_and_filter(sample_xlsx)
    names = [r["business_name"] for r in records]
    assert "Test Hispanic Business" not in names

def test_load_and_filter_maps_columns(sample_xlsx):
    records = load_and_filter(sample_xlsx)
    rec = next(r for r in records if r["business_name"] == "Test Black Business")
    assert rec["owner_name"] == "Jane Doe"
    assert rec["address_street"] == "123 Main St"
    assert rec["address_city"] == "Brooklyn"
    assert rec["address_state"] == "New York"
    assert rec["address_zip"] == "11201"
    assert rec["industry"] == "Services"
    assert rec["naics_code"] == "561990"
    assert rec["certification"] == "MBE"
    assert rec["website"] == "http://test.com"
    assert rec["year_founded"] == "2015"
    assert rec["phone"] == "212-555-0001"
    assert rec["email"] == "jane@test.com"

def test_load_and_filter_handles_missing_year(sample_xlsx):
    records = load_and_filter(sample_xlsx)
    rec = next(r for r in records if r["business_name"] == "Another Black Business")
    assert rec["year_founded"] == ""


def test_geocode_address_returns_coords():
    mock_response = MagicMock()
    mock_response.json.return_value = [{"lat": "40.6892", "lon": "-74.0445"}]
    mock_response.raise_for_status.return_value = None
    with patch("requests.get", return_value=mock_response):
        lat, lon = geocode_address("123 Main St", "Brooklyn", "New York", "11201")
    assert lat == "40.6892"
    assert lon == "-74.0445"


def test_geocode_address_returns_empty_on_no_results():
    mock_response = MagicMock()
    mock_response.json.return_value = []
    mock_response.raise_for_status.return_value = None
    with patch("requests.get", return_value=mock_response):
        lat, lon = geocode_address("Fake St", "Nowhere", "NY", "00000")
    assert lat == ""
    assert lon == ""


def test_geocode_address_returns_empty_on_network_error():
    with patch("requests.get", side_effect=Exception("network error")):
        lat, lon = geocode_address("123 Main St", "Brooklyn", "NY", "11201")
    assert lat == ""
    assert lon == ""


def test_build_csv_creates_output_file(sample_xlsx, tmp_path):
    output = tmp_path / "out.csv"
    with patch("build_csv.geocode_address", return_value=("40.68", "-74.04")):
        with patch("time.sleep"):
            build_csv(source=sample_xlsx, output=output)
    assert output.exists()


def test_build_csv_output_has_correct_row_count(sample_xlsx, tmp_path):
    output = tmp_path / "out.csv"
    with patch("build_csv.geocode_address", return_value=("40.68", "-74.04")):
        with patch("time.sleep"):
            build_csv(source=sample_xlsx, output=output)
    with open(output) as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2  # 2 Black businesses in fixture


def test_build_csv_output_has_geocoords(sample_xlsx, tmp_path):
    output = tmp_path / "out.csv"
    with patch("build_csv.geocode_address", return_value=("40.68", "-74.04")):
        with patch("time.sleep"):
            build_csv(source=sample_xlsx, output=output)
    with open(output) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["latitude"] == "40.68"
    assert rows[0]["longitude"] == "-74.04"


def test_build_csv_output_has_metadata(sample_xlsx, tmp_path):
    output = tmp_path / "out.csv"
    with patch("build_csv.geocode_address", return_value=("40.68", "-74.04")):
        with patch("time.sleep"):
            build_csv(source=sample_xlsx, output=output)
    with open(output) as f:
        rows = list(csv.DictReader(f))
    assert rows[0]["data_source"] == "NYC MWBE Directory 2025"
    assert rows[0]["last_verified"] == "2025-09-09"
    assert len(rows[0]["business_id"]) == 8


def test_build_csv_sample_limits_records(sample_xlsx, tmp_path):
    output = tmp_path / "out.csv"
    with patch("build_csv.geocode_address", return_value=("40.68", "-74.04")):
        with patch("time.sleep"):
            build_csv(source=sample_xlsx, output=output, sample=1)
    with open(output) as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1
