"""
Build ATTOM valuation cells for NFIP property-level merges.

Authors: Anna Li 
Date: 2026-06-22

Description:
    Aggregates raw ATTOM property records to ZIP/year-built and county/year-built
    valuation cells. NFIP policies do not include street addresses, so these
    cell files provide property-value measures that can be merged onto the NFIP
    property universe without pretending there is a parcel-level match.

Notes / Sources:
    Input defaults to {data}/raw/attom_{state}.parquet.
    Outputs default to {data}/build/{state}_attom_value_{tier}.dta.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True, help="2-letter state abbreviation, e.g. TX")
    parser.add_argument(
        "--data",
        required=True,
        help="Dropbox data root. Input/output paths derive from this root.",
    )
    parser.add_argument(
        "--attom",
        default=None,
        help="Optional ATTOM parquet path. Defaults to {data}/raw/attom_{state}.parquet.",
    )
    parser.add_argument("--tmp", default="/tmp", help="DuckDB temporary directory.")
    parser.add_argument("--threads", default=4, type=int)
    parser.add_argument("--memory", default="32GB")
    parser.add_argument("--max-temp", default="200GB", help="DuckDB max temp spill size.")
    return parser.parse_args()


def clean_for_stata(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].fillna("").astype(str)
        if df[col].dtype == bool:
            df[col] = df[col].astype(np.int8)
    return df


def prepare_latest_property(con: duckdb.DuckDBPyConnection) -> None:
    query = f"""
        CREATE OR REPLACE TEMP TABLE latest_property AS
        WITH attom_clean AS (
            SELECT
                regexp_extract(trim(cast(PROPERTYADDRESSZIP AS varchar)), '^(\\d{{5}})', 1) AS zip_key,
                lpad(regexp_extract(trim(cast(SITUSSTATECOUNTYFIPS AS varchar)), '(\\d+)', 1), 5, '0') AS countycode,
                cast(coalesce(nullif(YEARBUILT, 0), nullif(YEARBUILTEFFECTIVE, 0)) AS integer) AS construction_year,
                floor(cast(coalesce(nullif(YEARBUILT, 0), nullif(YEARBUILTEFFECTIVE, 0)) AS integer) / 10) * 10
                    AS construction_decade,
                ATTOMID,
                cast(TAXASSESSEDVALUETOTAL AS double) AS assessed_total,
                cast(TAXASSESSEDVALUEIMPROVEMENTS AS double) AS assessed_improvements,
                cast(TAXMARKETVALUETOTAL AS double) AS market_total,
                cast(TAXYEARASSESSED AS integer) AS tax_year_assessed
            FROM attom_raw
        ),
        usable AS (
            SELECT *
            FROM attom_clean
            WHERE construction_year BETWEEN 1700 AND 2035
              AND (assessed_total > 0 OR market_total > 0)
        ),
        latest_property AS (
            SELECT *
            FROM usable
            QUALIFY row_number() OVER (
                PARTITION BY ATTOMID
                ORDER BY tax_year_assessed DESC NULLS LAST
            ) = 1
        )
        SELECT *
        FROM latest_property
    """
    con.execute(query)


def write_tier(con: duckdb.DuckDBPyConnection, tier: str, select_keys: str, group_keys: str, out_path: Path) -> None:
    query = f"""
        SELECT
            {select_keys},
            count(*) AS attom_n_records,
            count(DISTINCT ATTOMID) AS attom_n_properties,
            avg(assessed_total) AS attom_mean_assessed_total,
            median(assessed_total) AS attom_median_assessed_total,
            avg(assessed_improvements) AS attom_mean_assessed_improve,
            median(assessed_improvements) AS attom_median_assessed_improve,
            avg(market_total) AS attom_mean_market_total,
            median(market_total) AS attom_median_market_total,
            min(tax_year_assessed) AS attom_min_tax_year,
            max(tax_year_assessed) AS attom_max_tax_year
        FROM latest_property
        WHERE {group_keys}
        GROUP BY {select_keys}
    """
    df = con.execute(query).df()
    df = clean_for_stata(df)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_stata(out_path, write_index=False, version=118)
    print(f"Saved {tier}: {out_path} ({len(df):,} cells)")


def main() -> None:
    args = parse_args()
    state = args.state.lower()
    data = Path(args.data)
    in_path = Path(args.attom) if args.attom else data / "raw" / f"attom_{state}.parquet"
    out_dir = data / "build"

    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote_sql(args.tmp)}")
    con.execute(f"SET memory_limit={quote_sql(args.memory)}")
    con.execute(f"SET max_temp_directory_size={quote_sql(args.max_temp)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")

    print(f"Reading ATTOM parquet: {in_path}")
    con.execute(f"CREATE VIEW attom_raw AS SELECT * FROM read_parquet({quote_sql(str(in_path))})")
    print("Preparing latest ATTOM property records...")
    prepare_latest_property(con)

    write_tier(
        con,
        "zip_year",
        "zip_key, construction_year",
        "zip_key IS NOT NULL AND zip_key != ''",
        out_dir / f"{state}_attom_value_zip_year.dta",
    )
    write_tier(
        con,
        "zip_decade",
        "zip_key, construction_decade",
        "zip_key IS NOT NULL AND zip_key != '' AND construction_decade IS NOT NULL",
        out_dir / f"{state}_attom_value_zip_decade.dta",
    )
    write_tier(
        con,
        "county_year",
        "countycode, construction_year",
        "countycode IS NOT NULL AND countycode != ''",
        out_dir / f"{state}_attom_value_county_year.dta",
    )
    write_tier(
        con,
        "county_decade",
        "countycode, construction_decade",
        "countycode IS NOT NULL AND countycode != '' AND construction_decade IS NOT NULL",
        out_dir / f"{state}_attom_value_county_decade.dta",
    )


if __name__ == "__main__":
    main()
