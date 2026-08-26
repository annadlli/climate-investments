"""Replace Builty fields in existing property-Wagner links.

Authors: Anna Li and Vendela Norman
Date: 2026-08-09

The ATTOM assignments and values remain fixed. Revised address-matched Builty
permits are collapsed by ATTOMID and replace the prior Builty event fields.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def normalize_attomid(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    normalized = series.astype(str).str.replace(r"\.0$", "", regex=True)
    return normalized.mask(numeric.notna(), numeric.round().astype("Int64").astype(str))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--links", required=True)
    parser.add_argument("--permits", required=True)
    parser.add_argument("--out", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    links = pd.read_stata(args.links, convert_categoricals=False)
    permits = pd.read_parquet(args.permits)
    matched = permits[
        permits["attom_match_tier"].ne("unmatched") & permits["ATTOMID"].notna()
    ].copy()
    matched["attomid"] = normalize_attomid(matched["ATTOMID"])
    events = (
        matched.groupby("attomid", as_index=False)
        .agg(
            builty_elevation_year=("permit_year", "min"),
            builty_n_permits=("permit_row_id", "size"),
            builty_attom_match_tier=("attom_match_tier", "min"),
        )
    )

    replaced = [
        "builty_elevated_wagner",
        "builty_elevation_year",
        "builty_n_permits",
        "builty_attom_match_tier",
    ]
    links = links.drop(columns=[column for column in replaced if column in links.columns])
    links["attomid"] = normalize_attomid(links["attomid"])
    links = links.merge(events, on="attomid", how="left", validate="one_to_one")
    links["builty_elevated_wagner"] = links["builty_n_permits"].notna().astype(int)
    links["builty_n_permits"] = links["builty_n_permits"].fillna(0)
    links["builty_attom_match_tier"] = links["builty_attom_match_tier"].fillna("")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    links.to_stata(out, write_index=False, version=118)
    print(
        f"Saved {out}: {len(links):,} links; "
        f"{int(links['builty_elevated_wagner'].sum()):,} Builty-matched ATTOM properties"
    )


if __name__ == "__main__":
    main()
