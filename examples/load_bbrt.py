"""Load the public BBRT snapshot and reproduce the headline numbers.

Run from the repository root:  python examples/load_bbrt.py
Needs: pip install duckdb pandas
"""
import duckdb

con = duckdb.connect()
firms = con.execute("SELECT * FROM read_parquet('data/businesses.parquet')").df()

# Headline: currently certified, or from a program with no status field.
headline = firms[firms["certification_status"] != "former"]
print(f"{len(firms):,} listings; {len(headline):,} in the headline count")

# Listings by state where the business is based (blank = state not recorded).
print(headline.groupby("address_state").size().sort_values(ascending=False).head(10))

# Which program listed each business.
print(headline.groupby("data_source").size().sort_values(ascending=False).head(10))
