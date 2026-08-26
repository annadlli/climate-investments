"""
Wagner-style tiered merge of ATTOM property values onto the NFIP universe.

Authors: Anna Li
Date: 2026-07-04

Description:
    NFIP policies carry no street address (lat/long are coarsened), so ATTOM
    property values cannot be joined 1:1. Following Wagner, matching is done by tiers: 
    Wagner tiers: 
    1	zipcode × year_built × flood_zone × year
    2	community × year_built × flood_zone × year
    3	community × flood_zone × year × 5-yr bin × post_FIRM
    4	community × flood_zone × year × decade × post_FIRM
    
    Tier order: (following Wagner generically
        1. zip      x construction-year   x policy-year
        2. zip      x construction-5-year x policy-year
        3. zip      x construction-decade x policy-year
        4. county   x construction-year   x policy-year
        5. county                         x policy-year

    ATTOM has no NFIP flood zone, so (unlike Wagner's house<->policy match) the
    flood-zone dimension is dropped; construction-year with a decade fallback is
    the fuzzy structure proxy. The result is a cell-level value join, not a
    parcel match -- property wealth is cell-level, not structure-level.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

# ATTOM value columns carried onto NFIP at every tier.
VALUE_COLS = ["attom_n_properties", "attom_mean_market_total", "attom_median_market_total"]


# Quote string values before interpolating them into DuckDB SQL commands.
def quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


# Parse the state, data root, and DuckDB resource settings from the command line.
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--state",   required=True)
    p.add_argument("--data",    required=True)
    p.add_argument("--tmp",      default="/tmp")
    p.add_argument("--threads",  default=4, type=int)
    p.add_argument("--memory",   default="32GB")
    p.add_argument("--max-temp", default="800GB")
    return p.parse_args()


# Open a DuckDB connection over the state's raw ATTOM parquet, tuned for large scans.
def open_attom(args: argparse.Namespace, state: str) -> duckdb.DuckDBPyConnection:
    # Resolve the raw ATTOM parquet under either layout:
    #   cluster/Torch: {data}/{state}/attom_{state}.parquet
    #   local:         {data}/raw/attom/attom_{state}.parquet
    candidates = [
        Path(args.data) / state / f"attom_{state}.parquet",
        Path(args.data) / "raw" / "attom" / f"attom_{state}.parquet",
    ]
    in_path = next((p for p in candidates if p.exists()), candidates[0])
    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote(args.tmp)}")
    con.execute(f"SET memory_limit={quote(args.memory)}")
    con.execute(f"SET max_temp_directory_size={quote(args.max_temp)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")

    print(f"Reading ATTOM: {in_path}")
    con.execute(f"CREATE VIEW attom_raw AS SELECT * FROM read_parquet({quote(str(in_path))})")

    # Property-year records: five-digit ZIP/county, tax year as the policy-year
    # dimension, YEARBUILT (effective-year fallback) as construction year.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE by_year AS
        SELECT
            regexp_extract(trim(cast(PROPERTYADDRESSZIP       AS varchar)), '^(\\d{5})', 1) AS zip_key,
            lpad(regexp_extract(trim(cast(SITUSSTATECOUNTYFIPS AS varchar)), '(\\d+)',   1), 5, '0') AS countycode,
            cast(TAXYEARASSESSED AS integer) AS year,
            cast(coalesce(nullif(YEARBUILT, 0), nullif(YEARBUILTEFFECTIVE, 0)) AS integer) AS construction_year,
            ATTOMID,
            cast(TAXMARKETVALUETOTAL AS double) AS market_total
        FROM attom_raw
        WHERE cast(TAXYEARASSESSED AS integer) BETWEEN 1980 AND 2035
          AND cast(TAXMARKETVALUETOTAL AS double) > 0
    """)
    # Construction decade (floor to 10) for the decade fallback tier.
    con.execute("ALTER TABLE by_year ADD COLUMN construction_decade INTEGER")
    con.execute("UPDATE by_year SET construction_decade = (construction_year / 10) * 10")
    return con


# Aggregate ATTOM property-years into one value-cell table for a given key.
def attom_cells(con: duckdb.DuckDBPyConnection, keys: list[str]) -> pd.DataFrame:
    key_sql = ", ".join(keys)
    notnull = " AND ".join(f"{k} IS NOT NULL" for k in keys) + \
        (" AND zip_key != ''" if "zip_key" in keys else "") + \
        (" AND countycode != ''" if "countycode" in keys else "")
    df = con.execute(f"""
        SELECT {key_sql},
            count(DISTINCT ATTOMID) AS attom_n_properties,
            avg(market_total)       AS attom_mean_market_total,
            median(market_total)    AS attom_median_market_total
        FROM by_year
        WHERE {notnull}
        GROUP BY {key_sql}
    """).df()
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str)
    return df


# Load the NFIP policy universe and normalize the geo/time merge keys to ATTOM's.
def load_nfip(data: Path, state: str) -> pd.DataFrame:
    nf = pd.read_stata(data / "clean" / "nfip_policies_state" / f"{state}.dta")
    nf["zip_key"] = nf["zipcode"].astype("string").str.extract(r"^(\d{5})", expand=False)
    nf["countycode"] = nf["countycode"].astype("string").str.extract(r"(\d+)", expand=False) \
        .str.zfill(5)
    nf = nf.rename(columns={"policy_year": "year"})
    nf["year"] = pd.to_numeric(nf["year"], errors="coerce").astype("Int64")
    nf["construction_year"] = pd.to_numeric(nf["construction_year"], errors="coerce").astype("Int64")
    nf["construction_decade"] = (nf["construction_year"] // 10) * 10
    return nf


# Fill ATTOM values on rows still unmatched, using one cell table joined on `keys`.
def apply_tier(nf: pd.DataFrame, cells: pd.DataFrame, keys: list[str], label: str) -> pd.DataFrame:
    todo = nf["attom_tier"].isna()
    if not todo.any():
        return nf
    # Join only the still-unmatched rows so coarser tiers never overwrite finer ones.
    joined = nf.loc[todo, keys].merge(cells, on=keys, how="left")
    joined.index = nf.index[todo]
    got = joined["attom_mean_market_total"].notna()
    idx = joined.index[got]
    for c in VALUE_COLS:
        nf.loc[idx, c] = joined.loc[idx, c].to_numpy()
    nf.loc[idx, "attom_tier"] = label
    print(f"  tier {label:<28} matched {int(got.sum()):>9,}  "
          f"(cumulative {int(nf['attom_tier'].notna().sum()):>9,} / {len(nf):,})")
    return nf


def main() -> None:
    args = parse_args()
    state = args.state.lower()
    data = Path(args.data)

    # --- ATTOM value cells at every Wagner tier grain ---
    con = open_attom(args, state)
    tiers = [
        (["zip_key", "construction_year", "year"],    "zip_construction_year"),
        (["zip_key", "construction_decade", "year"],  "zip_construction_decade"),
        (["zip_key", "year"],                         "zip_year"),
        (["countycode", "construction_year", "year"], "county_construction_year"),
        (["countycode", "year"],                      "county_year"),
    ]
    cell_tables = {label: attom_cells(con, keys) for keys, label in tiers}
    con.close()

    # --- NFIP universe + tiered waterfall ---
    nf = load_nfip(data, state)
    for c in VALUE_COLS:
        nf[c] = pd.NA
    nf["attom_tier"] = pd.Series(pd.NA, index=nf.index, dtype="object")

    print(f"NFIP policy-years ({state.upper()}): {len(nf):,}")
    for keys, label in tiers:
        nf = apply_tier(nf, cell_tables[label], keys, label)
    nf["attom_tier"] = nf["attom_tier"].fillna("unmatched")

    matched = (nf["attom_tier"] != "unmatched").sum()
    print(f"ATTOM value attached: {matched:,} / {len(nf):,} ({100*matched/len(nf):.1f}%)")

    # --- Save ---
    for c in VALUE_COLS:
        nf[c] = pd.to_numeric(nf[c], errors="coerce")
    nf = nf.drop(columns=["construction_decade"])
    out = data / "build" / f"{state}_nfip_attom_wagner.dta"
    nf.to_stata(out, write_index=False, version=118)
    print(f"Saved: {out} ({len(nf):,} rows)")


if __name__ == "__main__":
    main()
