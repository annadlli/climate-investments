"""
Authors: Anna Li
Date: 2026-08-16

Adds the Builty elevation flags onto the full ATTOM--NFHL property file.

The ATTOM--NFHL file is the master and every one of its rows survives. Builty
columns get filled in only where the ATTOMID matches, which is the same thing
as Stata's `merge 1:1 attomid, keep(1 3)` with python.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

# the columns we need off the Builty--ATTOM file, and what an empty one looks like
BUILTY_COLUMNS = {"ATTOMID": "string", "BUILTY_ID": "string",
                  "permit_year": "float64", "attom_match_tier": "string"}


def clean_id(series: pd.Series) -> pd.Series:
    # ATTOMIDs sometimes arrive as floats, so "12345.0" needs to become "12345"
    return series.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)


def parse_args() -> argparse.Namespace:
    # the ATTOM--NFHL master, the state's Builty links, and where the two outputs go
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--attom-nfhl", required=True)
    parser.add_argument("--builty-attom", help="State Builty--ATTOM file; omit if the state has none.")
    parser.add_argument("--out", required=True)
    parser.add_argument("--diagnostics", required=True)
    return parser.parse_args()


def load_permits(path: str | None) -> pd.DataFrame:
    # read the state's Builty--ATTOM links; states with no permits get an empty stand-in
    if path and Path(path).is_file():
        return pd.read_parquet(path)
    return pd.DataFrame({name: pd.Series(dtype=dtype) for name, dtype in BUILTY_COLUMNS.items()})


def collapse_to_properties(permits: pd.DataFrame) -> pd.DataFrame:
    # a property can carry several permits, so squash them down to one row per ATTOMID
    permits = permits.assign(attomid=clean_id(permits["ATTOMID"]))
    permits = permits.loc[permits["attomid"].notna()]
    permits["elevation_year"] = pd.to_numeric(permits["permit_year"], errors="coerce")
    builty = permits.groupby("attomid", as_index=False).agg(
        builty_elevation_year=("elevation_year", "min"),
        builty_n_properties=("BUILTY_ID", "nunique"),
        builty_attom_match_tier=("attom_match_tier", "first"),
    )
    # every ATTOMID that survives to here has at least one elevation permit on it
    builty["builty_elevated"] = 1
    return builty


def main() -> None:
    args = parse_args()

    # bring in the master file: one row per geocoded ATTOM property with its NFHL flood info
    attom = pd.read_parquet(args.attom_nfhl)
    attom["attomid"] = clean_id(attom["attomid"])

    # bring in the elevation permits and get them down to property level
    builty = collapse_to_properties(load_permits(args.builty_attom))

    # hang the Builty columns off the master; validate= is the guard that the join stays 1:1
    result = attom.merge(builty, on="attomid", how="left", validate="one_to_one")

    # properties with no permit are not elevated rather than unknown, so fill the blanks with zeros
    result["builty_elevated"] = result["builty_elevated"].fillna(0).astype("int8")
    result["builty_n_properties"] = result["builty_n_properties"].fillna(0).astype("int16")

    # mirror Stata's _merge codes so this reads the same as the rest of the pipeline
    result["builty_merge_status"] = result["builty_elevated"].map({0: 1, 1: 3}).astype("int8")

    # write the enriched universe out
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(out, index=False)

    # a one-line summary so we can see at a glance how many properties picked up a permit
    diagnostics = pd.DataFrame([{
        "attom_master_rows": len(attom),
        "attom_rows_after_merge": len(result),
        "builty_attomids": len(builty),
        "attom_rows_with_builty": int(result["builty_elevated"].sum()),
        # Builty ATTOMIDs absent from the master are dropped by the left join.
        # A non-zero count here means permits are being lost before assignment.
        "builty_attomids_unmatched": int(len(builty) - result["builty_elevated"].sum()),
        "unmatched_attom_rows_retained": int(result["builty_elevated"].eq(0).sum()),
        "merge_status_1_attom_only": int(result["builty_merge_status"].eq(1).sum()),
        "merge_status_3_attom_builty": int(result["builty_merge_status"].eq(3).sum()),
    }])
    Path(args.diagnostics).parent.mkdir(parents=True, exist_ok=True)
    diagnostics.to_csv(args.diagnostics, index=False)
    print(diagnostics.to_string(index=False))
    print(f"Saved {out}")


if __name__ == "__main__":
    main()
