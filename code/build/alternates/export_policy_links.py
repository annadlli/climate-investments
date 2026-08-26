"""
Export the policy-year NFIP-ATTOM assignments to Stata for the property collapse.

Authors: Anna Li
Date: 2026-08-14

Description:
    assign_attom_to_nfip.py writes wide parquet files carrying assignment keys,
    diagnostics, Builty flags, and ATTOM values. This exports the evidence and
    substantive fields needed to select a stable property link and carry that
    selected record into the analysis dataset.

    Input  : {data}/build/nfip_attom_policy_year_v2/{state}_nfip_attom_policy_year.parquet
    Output : {data}/build/nfip_attom_policy_year_v2/{state}_policy_links.dta
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


# Keep both the evidence needed for the consensus collapse and the substantive
# ATTOM/Builty fields that must travel with the ultimately selected assignment.
COLUMNS = [
    "state", "property_id", "policy_year", "nfip_attom_merge_status",
    "assigned_attomid", "match_tier", "match_tier_number", "match_cell_id",
    "assignment_method", "cell_singleton", "nfip_cell_n", "attom_cell_n",
    "nfip_property_n", "attom_value_year", "attom_value_lag",
    "builty_elevated", "builty_elevation_year", "builty_n_properties",
    "builty_merge_status", "builty_attom_match_tier",
    "attom_flood_zone_original", "attom_flood_zone_key",
    "attom_flood_risk_key", "attom_nfhl_flood_matched",
    "attom_nfhl_community_matched", "attom_property_use_std",
    "attom_market_value_total", "attom_market_value_land",
    "attom_market_value_improvements", "attom_assessed_value_total",
    "attom_assessed_value_improvements", "attom_previous_assessed_value",
    "attom_last_sale_price",
]

STRING_COLUMNS = [
    "state", "assigned_attomid", "match_tier", "match_cell_id",
    "assignment_method", "builty_attom_match_tier",
    "attom_flood_zone_original", "attom_flood_zone_key",
    "attom_flood_risk_key", "attom_property_use_std",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", required=True, help="Data root passed from master.do.")
    p.add_argument("--states", required=True, help="Space-separated state abbreviations.")
    p.add_argument("--links-dir", default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    data = Path(args.data)
    links = Path(args.links_dir) if args.links_dir else data / "build" / "nfip_attom_policy_year_v2"

    for state in args.states.split():
        st = state.lower()
        src = links / f"{st}_nfip_attom_policy_year.parquet"
        if not src.is_file():
            print(f"{st}: no {src.name}, skipping")
            continue

        # Check the schema before reading so a renamed column fails loudly.
        available = set(pq.ParquetFile(src).schema_arrow.names)
        missing = [c for c in COLUMNS if c not in available]
        if missing:
            raise KeyError(f"{src} missing {missing}; has {sorted(available)}")

        frame = pd.read_parquet(src, columns=COLUMNS)
        # Stata names are limited to 32 characters.
        frame = frame.rename(columns={
            "attom_assessed_value_improvements": "attom_assessed_improvements",
        })
        # Stata cannot store pandas NA in string columns.
        for column in STRING_COLUMNS:
            frame[column] = frame[column].fillna("").astype(str)
        # Booleans and nullable integers need a plain numeric dtype for .dta.
        for column in frame.columns.difference(STRING_COLUMNS):
            if frame[column].dtype == bool:
                frame[column] = frame[column].astype("int8")
            elif str(frame[column].dtype).startswith(("Int", "Float", "boolean")):
                frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float64")

        out = links / f"{st}_policy_links.dta"
        frame.to_stata(out, write_index=False, version=118)
        matched = int((frame["nfip_attom_merge_status"] == 3).sum())
        print(f"{st}: {len(frame):,} rows ({matched:,} matched) -> {out.name}")


if __name__ == "__main__":
    main()
