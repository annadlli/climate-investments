"""Authors: Anna Li
Date: 2026-08-12

Pull the NFHL/Wagner matches into the main analysis file and keep one
reference policy year per property.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    # Point it at the analysis file, the Wagner outputs, the Builty matches, and the state list.
    parser = argparse.ArgumentParser()
    parser.add_argument("--analysis", required=True)
    parser.add_argument("--wagner-dir", required=True)
    parser.add_argument(
        "--wagner-suffix",
        default="nfip_attom_wagner",
        help="State-file stem after the state prefix, without .parquet.",
    )
    parser.add_argument("--permits-dir", required=True)
    parser.add_argument("--states", nargs="+", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--diagnostics", required=True)
    return parser.parse_args()

# Work one state at a time: pick the reference year and attach the Builty info.
def property_links(
    state: str, wagner_dir: Path, permits_dir: Path, wagner_suffix: str
) -> pd.DataFrame:
    state_lower = state.lower()
    wagner = pd.read_parquet(wagner_dir / f"{state_lower}_{wagner_suffix}.parquet")
    wagner["property_id"] = pd.to_numeric(wagner["property_id"], errors="raise")
    wagner["policy_year"] = pd.to_numeric(wagner["policy_year"], errors="coerce")

    # Pick one year per property. If the property ever gets elevated, use the first
    # elevation year; otherwise, use the first policy year we see.
    wagner = wagner.sort_values(["property_id", "policy_year"])
    groups = wagner.groupby("property_id", sort=False)
    first_year = groups["policy_year"].transform("min")
    elevated = pd.to_numeric(wagner["elevated"], errors="coerce").fillna(0)
    first_elevated = elevated.groupby(wagner["property_id"]).transform("first")
    last_elevated = elevated.groupby(wagner["property_id"]).transform("last")
    elevation_year = (
        wagner["policy_year"]
        .where(elevated.eq(1))
        .groupby(wagner["property_id"])
        .transform("min")
    )
    transitioned = last_elevated.eq(1) & first_elevated.eq(0)
    reference_year = first_year.copy()
    reference_year.loc[transitioned] = elevation_year.loc[transitioned]
    links = wagner.loc[wagner["policy_year"].eq(reference_year)].copy()
    links = links.sort_values(["property_id", "policy_year"]).drop_duplicates(
        "property_id"
    )

    # Builty is stored at the permit level, so collapse it to one ATTOM link per
    # property and stick to the geocoded ATTOM file as the source of truth.
    permits = pd.read_parquet(
        permits_dir / f"{state_lower}_attom_permits_final.parquet"
    )
    permits["builty_attom_source"] = "geocoded_attom"

    permits = permits.loc[permits["ATTOMID"].notna()].copy()
    permits["matched_attomid"] = (
        permits["ATTOMID"].astype("string").str.replace(r"\.0$", "", regex=True)
    )
    permits["builty_elevation_year"] = pd.to_numeric(
        permits["PERMIT_YEAR"], errors="coerce"
    )
    builty = permits.groupby("matched_attomid", as_index=False).agg(
        builty_elevation_year=("builty_elevation_year", "min"),
        builty_n_permits=("BUILTY_ID", "nunique"),
        builty_attom_match_tier=("attom_match_tier", "first"),
        builty_attom_source=("builty_attom_source", "first"),
    )
    builty["builty_elevated_nfhl_wagner"] = 1
    links["matched_attomid"] = (
        links["matched_attomid"].astype("string").str.replace(r"\.0$", "", regex=True)
    )
    links = links.merge(builty, on="matched_attomid", how="left")
    links["builty_elevated_nfhl_wagner"] = (
        links["builty_elevated_nfhl_wagner"].fillna(0).astype("int8")
    )
    links["state"] = state.upper()

    keep = [
        "state",
        "property_id",
        "policy_year",
        "matched_attomid",
        "wagner_tier",
        "attom_value_year",
        "attom_value_lag",
        "attom_market_value_total",
        "attom_market_value_land",
        "attom_market_value_improvements",
        "attom_assessed_value_total",
        "attom_assessed_value_improvements",
        "attom_previous_assessed_value",
        "attom_last_sale_price",
        "builty_elevated_nfhl_wagner",
        "builty_elevation_year",
        "builty_n_permits",
        "builty_attom_match_tier",
        "builty_attom_source",
    ]
    return links[keep]


def main() -> None:
    # Build the state-level reference-year links first, then merge them into the
    # one-row-per-property analysis file.
    args = parse_args()
    states = [state.upper() for state in args.states]
    wagner_dir = Path(args.wagner_dir)
    permits_dir = Path(args.permits_dir)
    links = pd.concat(
        [
            property_links(state, wagner_dir, permits_dir, args.wagner_suffix)
            for state in states
        ],
        ignore_index=True,
    )
    links = links.rename(columns={"property_id": "property_id_state"})

    # Read the analysis file and rebuild the state-local to global property id
    # crosswalk. This keeps the state-specific ids lined up before we merge.
    analysis = pd.read_stata(args.analysis, convert_categoricals=False)
    analysis["state"] = analysis["state"].astype("string").str.upper().str.strip()
    analysis["property_id"] = pd.to_numeric(analysis["property_id"], errors="raise")
    crosswalk_parts = []
    for state in states:
        local_ids = sorted(
            links.loc[links["state"].eq(state), "property_id_state"].unique()
        )
        global_ids = sorted(
            analysis.loc[analysis["state"].eq(state), "property_id"].unique()
        )
        crosswalk_parts.append(
            pd.DataFrame(
                {
                    "state": state,
                    "property_id_state": local_ids,
                    "property_id": global_ids,
                }
            )
        )
    crosswalk = pd.concat(crosswalk_parts, ignore_index=True)
    links = links.merge(crosswalk, on=["state", "property_id_state"], how="left").drop(
        columns="property_id_state"
    )

    # Merge the ATTOM, NFHL/Wagner, and Builty fields into the final analysis file.
    # Missing matches stay missing instead of being dropped.
    result = analysis.merge(links, on=["state", "property_id"], how="left")
    result["nfhl_wagner_data_available"] = result["state"].isin(states).astype("int8")
    result.loc[~result["state"].isin(states), "builty_elevated_nfhl_wagner"] = pd.NA

    # Stata has a short variable-name limit, so rename the awkward ATTOM column and
    # convert strings to plain Python strings with missing values set to None.
    result = result.rename(
        columns={
            "attom_assessed_value_improvements": "attom_assessed_value_improvement",
        }
    )
    for column in result.select_dtypes(include=["string", "object"]).columns:
        result[column] = (
            result[column]
            .map(lambda value: None if pd.isna(value) else str(value))
            .astype("object")
        )

    # Quick coverage check: how many properties got a Wagner ATTOM match, and how
    # many have a Builty elevation match.
    diagnostics = []
    for state in states:
        state_rows = result.loc[result["state"].eq(state)]
        diagnostics.append(
            {
                "state": state,
                "analysis_properties": len(state_rows),
                "nfhl_wagner_matched": int(state_rows["matched_attomid"].notna().sum()),
                "builty_elevated_properties": int(
                    state_rows["builty_elevated_nfhl_wagner"].eq(1).sum()
                ),
                "nfhl_wagner_match_rate": float(
                    state_rows["matched_attomid"].notna().mean()
                ),
            }
        )

    # Save the final analysis file and a small state-by-state summary.
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    result.to_stata(out, write_index=False, version=118)
    pd.DataFrame(diagnostics).to_csv(args.diagnostics, index=False)
    print(pd.DataFrame(diagnostics).to_string(index=False))
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
