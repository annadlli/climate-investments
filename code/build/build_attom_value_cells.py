"""
Build ATTOM valuation cells for NFIP property-level merges.

Authors: Anna Li and Vendela Norman
Date: 2026-06-22

Description:
    Aggregates raw ATTOM property records to ZIP×policy-year and county×policy-year
    valuation cells. NFIP policies have no street address, so these cell files
    provide property-value measures merged onto the NFIP universe by cell.

    Policy-year (TAXYEARASSESSED) is used as the time dimension so that NFIP
    policy records can be matched to contemporaneous market values.
    TAXMARKETVALUEYEAR is empty in the data; TAXYEARASSESSED is used instead.

Inputs:  {data}/{state}/attom_{state}.parquet
Outputs: {data}/build/{state}_attom_value_zip_year.dta
         {data}/build/{state}_attom_value_county_year.dta
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


def quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--state",   required=True)
    p.add_argument("--data",    required=True)
    p.add_argument("--tmp",      default="/tmp")
    p.add_argument("--threads",  default=4, type=int)
    p.add_argument("--memory",   default="32GB")
    p.add_argument("--max-temp", default="800GB")
    return p.parse_args()


def main() -> None:
    args  = parse_args()
    state = args.state.lower()
    data  = Path(args.data)
    in_path  = data / state / f"attom_{state}.parquet"
    out_dir  = data / "build"
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote(args.tmp)}")
    con.execute(f"SET memory_limit={quote(args.memory)}")
    con.execute(f"SET max_temp_directory_size={quote(args.max_temp)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")

    print(f"Reading: {in_path}")
    con.execute(f"CREATE VIEW attom_raw AS SELECT * FROM read_parquet({quote(str(in_path))})")

    # One row per property × assessment year: valid zip, county, and assessed value
    con.execute("""
        CREATE OR REPLACE TEMP TABLE by_year AS
        SELECT
            regexp_extract(trim(cast(PROPERTYADDRESSZIP       AS varchar)), '^(\\d{5})', 1) AS zip_key,
            lpad(regexp_extract(trim(cast(SITUSSTATECOUNTYFIPS AS varchar)), '(\\d+)',   1), 5, '0') AS countycode,
            cast(TAXYEARASSESSED AS integer) AS policy_year,
            cast(coalesce(nullif(YEARBUILT, 0), nullif(YEARBUILTEFFECTIVE, 0)) AS integer) AS construction_year,
            ATTOMID,
            cast(TAXMARKETVALUETOTAL AS double) AS market_total
        FROM attom_raw
        WHERE cast(TAXYEARASSESSED AS integer) BETWEEN 1980 AND 2035
          AND cast(TAXMARKETVALUETOTAL AS double) > 0
    """)

    n = con.execute("SELECT count(*) FROM by_year").fetchone()[0]
    print(f"by_year rows: {n:,}")
    if n == 0:
        # Show what the raw columns look like to diagnose the problem
        print("WARNING: by_year is empty. Checking raw column availability:")
        print(con.execute("SELECT count(*) FROM attom_raw").fetchone()[0], "raw rows")
        print(con.execute("SELECT TAXMARKETVALUEYEAR, TAXMARKETVALUETOTAL FROM attom_raw LIMIT 5").df().to_string())
        raise ValueError("by_year is empty — check TAXMARKETVALUEYEAR / TAXMARKETVALUETOTAL column names and values")

    for tier, key, filt in [
        ("zip_year",    "zip_key,    policy_year", "zip_key    IS NOT NULL AND zip_key    != ''"),
        ("county_year", "countycode, policy_year", "countycode IS NOT NULL AND countycode != ''"),
    ]:
        df = con.execute(f"""
            SELECT {key},
                count(DISTINCT ATTOMID)    AS attom_n_properties,
                avg(market_total)          AS attom_mean_market_total,
                median(market_total)       AS attom_median_market_total
            FROM by_year
            WHERE {filt}
            GROUP BY {key}
        """).df()

        # Stata requires no missing strings
        for col in df.select_dtypes(include="str").columns:
            df[col] = df[col].fillna("").astype(str)

        out = out_dir / f"{state}_attom_value_{tier}.dta"
        df.to_stata(out, write_index=False, version=118)
        print(f"Saved {tier}: {out} ({len(df):,} cells)")


if __name__ == "__main__":
    main()
