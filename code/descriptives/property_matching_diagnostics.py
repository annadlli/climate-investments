"""
Authors: Anna Li
Date: 2026-09-02

Creates diagnostics for the finalized Builty -> ATTOM -> NFIP property
pipeline from its saved construction outputs. Running diagnostics is separate
from building those outputs.

Example:
    python property_matching_diagnostics.py --data /path/to/Data \
        --output /path/to/output/descriptives/property_matching \
        --states AL CT DE FL GA LA ME MD MA MS NH NJ NY NC PA RI SC TX VT VA
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


BUILTY_TIER_ORDER = [
    "exact", "unique_no_suffix", "unique_compact", "unique_addr_county",
    "unique_nosuffix_county", "unique_compact_county", "unmatched",
]


def read_parquet(path: Path) -> pd.DataFrame:
    escaped = str(path).replace("'", "''")
    return duckdb.sql(f"SELECT * FROM read_parquet('{escaped}')").df()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--states", nargs="+", required=True)
    return parser.parse_args()


def coverage(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    # This is just the reusable summary table: group by a key, count rows, and
    # track how often the property value and block group are present.
    out = (frame.groupby(keys, dropna=False)
           .agg(n=("permit_row_id", "size"),
                n_prop_value=("has_prop_value", "sum"),
                n_blockgroup=("has_blockgroup", "sum")).reset_index())
    out["prop_value_rate"] = out["n_prop_value"] / out["n"]
    out["blockgroup_rate"] = out["n_blockgroup"] / out["n"]
    return out


def nfhl_diagnostics(frame: pd.DataFrame) -> pd.DataFrame:
    # Quick check on whether the ATTOM-NFHL side is looking sane: coordinates,
    # flood hits, community hits, and duplicate candidate counts.
    total = len(frame)
    valid = frame["longitude"].between(-180, 180) & frame["latitude"].between(-90, 90)
    counts = [
        ("ATTOM properties", total),
        ("valid accepted coordinates", int(valid.sum())),
        ("matched to flood polygon", int(frame["nfhl_flood_matched"].sum())),
        ("matched to community polygon", int(frame["nfhl_community_matched"].sum())),
        ("multiple flood candidates", int(frame["flood_candidate_count"].fillna(0).gt(1).sum())),
        ("multiple community candidates", int(frame["community_candidate_count"].fillna(0).gt(1).sum())),
    ]
    return pd.DataFrame([
        {"metric": metric, "count": count,
         "percent": round(100 * count / total, 2)}
        for metric, count in counts
    ])


def builty_attom_diagnostics(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # This is the Builty-to-ATTOM check: see how many permits get a property value
    # and a block group, and split that by the same match tier used in the real pipeline.
    flagged = frame.assign(
        has_prop_value=frame["prop_value"].notna(),
        has_blockgroup=frame["attom_has_blockgroup"].eq(1),
    )
    by_tier = coverage(flagged, ["attom_match_tier"])
    by_tier["tier_number"] = by_tier["attom_match_tier"].map(
        {tier: number for number, tier in enumerate(BUILTY_TIER_ORDER, start=1)})
    by_tier = by_tier.sort_values("tier_number").reset_index(drop=True)
    by_tier["permit_share"] = by_tier["n"] / len(frame)

    total = coverage(flagged.assign(_all=1), ["_all"]).drop(columns="_all")
    total.insert(0, "attom_match_tier", "TOTAL")
    total["tier_number"] = len(BUILTY_TIER_ORDER) + 1
    total["permit_share"] = 1.0
    summary = pd.concat([by_tier, total], ignore_index=True)
    detail = coverage(flagged, ["county_fips", "permit_year", "attom_match_tier"])
    return summary, detail


def enriched_diagnostics(attom: pd.DataFrame, permits: pd.DataFrame) -> pd.DataFrame:
    # This collapses the ATTOM + Builty merged file down to a few headline counts:
    # how many permits, how many ATTOM rows carry a Builty flag, and how the merge status is split.
    permit_attomids = (permits["ATTOMID"].astype("string").str.strip()
                       .str.replace(r"\.0$", "", regex=True).dropna().nunique())
    with_builty = int(attom["builty_elevated"].sum())
    return pd.DataFrame([{
        "attom_master_rows": len(attom),
        "attom_rows_after_merge": len(attom),
        "builty_attomids": permit_attomids,
        "attom_rows_with_builty": with_builty,
        "builty_attomids_unmatched": permit_attomids - with_builty,
        "unmatched_attom_rows_retained": int(attom["builty_elevated"].eq(0).sum()),
        "merge_status_1_attom_only": int(attom["builty_merge_status"].eq(1).sum()),
        "merge_status_3_attom_builty": int(attom["builty_merge_status"].eq(3).sum()),
    }])


def assignment_diagnostics(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # The matching step is a ladder. This summarizes how crowded each match cell was,
    # how many assignments happened at each tier, and whether the process was mostly singleton matches.
    matched = frame.loc[frame["nfip_attom_merge_status"].eq(3)].copy()
    cells = matched.drop_duplicates(["match_tier_number", "match_cell_id"])
    summary = (cells.groupby(["match_tier", "match_tier_number"], dropna=False)
               .agg(cells=("match_cell_id", "size"),
                    nfip_properties_in_cells=("nfip_cell_n", "sum"),
                    attom_properties_in_cells=("attom_cell_n", "sum"),
                    mean_attom_per_cell=("attom_cell_n", "mean"),
                    median_attom_per_cell=("attom_cell_n", "median"),
                    p90_attom_per_cell=("attom_cell_n", lambda values: values.quantile(.9)),
                    max_attom_per_cell=("attom_cell_n", "max"),
                    singleton_cells=("cell_singleton", "sum")).reset_index())
    assignments = (matched.groupby("match_tier_number").size()
                   .rename("assignments").reset_index())
    summary = summary.merge(assignments, on="match_tier_number", how="left")
    final_unmatched = int(frame["nfip_attom_merge_status"].eq(1).sum())
    later = summary.sort_values("match_tier_number", ascending=False)["assignments"].cumsum()
    summary = summary.sort_values("match_tier_number", ascending=False)
    summary["unmatched_after_tier"] = final_unmatched + later.shift(fill_value=0)
    summary["singleton_cell_share"] = summary["singleton_cells"] / summary["cells"]
    summary = summary.sort_values("match_tier_number").reset_index(drop=True)
    summary = summary.rename(columns={"match_tier_number": "tier_number"})

    cell_detail = (matched.groupby(
        ["match_tier", "match_cell_id", "nfip_cell_n", "attom_cell_n",
         "builty_attom_cell_n", "cell_singleton"], dropna=False)
        .size().rename("assignments").reset_index())
    return summary, cell_detail


def assignment_overview(frame: pd.DataFrame) -> pd.DataFrame:
    # One-line snapshot of the whole match: how many NFIP properties got an ATTOM
    # property, and whether the assigned ATTOM IDs stayed unique.
    matched = frame["nfip_attom_merge_status"].eq(3)
    assignments = int(matched.sum())
    distinct_attomids = frame.loc[matched, "assigned_attomid"].nunique()
    return pd.DataFrame([{
        "nfip_properties": len(frame),
        "attom_assignments": assignments,
        "attom_match_rate": assignments / len(frame),
        "distinct_assigned_attomids": distinct_attomids,
        "assigned_attomids_unique": assignments == distinct_attomids,
    }])


def main() -> None:
    # This is the top-level loop: read the pipeline outputs for each state, make a
    # few diagnostics tables, and dump them into the descriptives folder.
    args = parse_args()
    data = Path(args.data)
    output = Path(args.output)
    pipeline = data / "build" / "nfip_attom_pipeline_v2"

    for state in args.states:
        state = state.lower()
        nfhl = read_parquet(pipeline / "nfhl_matches" / f"{state}_attom_nfhl.parquet")
        permits = read_parquet(pipeline / "builty_attom" / f"{state}_attom_permits.parquet")
        enriched = read_parquet(
            pipeline / "attom_nfhl_builty" / f"{state}_attom_nfhl_builty.parquet")
        assignment = read_parquet(
            pipeline / "nfip_attom_property" / f"{state}_nfip_attom_property.parquet")

        nfhl_diagnostics(nfhl).to_csv(output / f"{state}_attom_nfhl.csv", index=False)
        tier, county = builty_attom_diagnostics(permits)
        tier.to_csv(output / f"{state}_builty_attom_by_tier.csv", index=False)
        county.to_csv(output / f"{state}_builty_attom_by_county.csv", index=False)
        enriched_diagnostics(enriched, permits).to_csv(
            output / f"{state}_attom_nfhl_builty.csv", index=False)
        tier, cells = assignment_diagnostics(assignment)
        assignment_overview(assignment).to_csv(
            output / f"{state}_assignment_overview.csv", index=False)
        tier.to_csv(output / f"{state}_assignment_by_tier.csv", index=False)
        cells.to_csv(output / f"{state}_assignment_cells.csv", index=False)

        print(f"Saved property-matching diagnostics for {state.upper()} to {output}")


if __name__ == "__main__":
    main()
