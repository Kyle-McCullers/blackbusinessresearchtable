"""Load the public BBRT snapshot and reproduce the headline numbers.

Run from the repository root:  python examples/load_bbrt.py
Needs: pip install duckdb pandas
"""
import duckdb

con = duckdb.connect()
firms = con.execute("SELECT * FROM read_parquet('data/businesses.parquet')").df()

# Headline: currently certified, or from a program with no status field.
headline = firms[firms["certification_status"] != "former"]
print(f"{len(firms):,} firms; {len(headline):,} in the headline count")
listings = con.execute("SELECT COUNT(*) FROM read_parquet('data/listings.parquet')").fetchone()[0]
print(f"{listings:,} program listings behind them")

# Listings by state where the business is based (blank = state not recorded).
print(headline.groupby("address_state").size().sort_values(ascending=False).head(10))

# Which programs list each firm.
print(headline.groupby("programs").size().sort_values(ascending=False).head(10))
