"""Compile property-level NFIP--ATTOM--Builty links into an analysis .dta."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--analysis", required=True)
    p.add_argument("--links-dir", required=True)
    p.add_argument("--permits-dir", help="Deprecated: Builty now travels in the ATTOM--NFHL links.")
    p.add_argument("--states", nargs="+", required=True)
    p.add_argument("--out-links", required=True)
    p.add_argument("--out-analysis", required=True)
    p.add_argument("--diagnostics", required=True)
    p.add_argument("--pattern", default="{state}_nfip_attom_property.parquet",
                   help="State link filename pattern inside --links-dir.")
    a = p.parse_args()

    links = []
    for state in [s.lower() for s in a.states]:
        link = pd.read_parquet(Path(a.links_dir) / a.pattern.format(state=state))
        link["state"] = state.upper()
        links.append(link)

    links = pd.concat(links, ignore_index=True)
    if links["property_id"].duplicated().any():
        raise ValueError("Compiled links are not unique on global property_id")
    # The property-level assigner names it assigned_attomid; older links use attomid.
    if "assigned_attomid" not in links and "attomid" in links:
        links = links.rename(columns={"attomid": "assigned_attomid"})
    links["assigned_attomid"] = (
        links["assigned_attomid"].astype("string").str.replace(r"\.0$", "", regex=True)
    )
    # One ATTOM house may not serve two NFIP properties.
    assigned = links.loc[links["assigned_attomid"].notna(), "assigned_attomid"]
    if assigned.duplicated().any():
        raise ValueError("assigned_attomid is not unique across the compiled states")
    links["builty_elevated"] = links["builty_elevated"].fillna(0).astype("int8")
    links["builty_n_properties"] = links["builty_n_properties"].fillna(0).astype("int16")

    analysis = pd.read_stata(a.analysis, convert_categoricals=False)
    analysis["state"] = analysis["state"].astype("string").str.upper().str.strip()
    analysis = analysis[analysis["state"].isin([s.upper() for s in a.states])].copy()
    before = len(analysis)
    result = analysis.merge(
        links.drop(columns=[c for c in ["state", "zipcode", "censusblockgroupfips",
                                        "construction_year", "ratedfloodzone", "postfirm",
                                        "policy_year_init", "zip_key", "blockgroup_key",
                                        "county_key", "community_key", "construction_5yr",
                                        "construction_decade", "flood_zone_key",
                                        "flood_risk_key", "nfip_flood_zone_original"]
                            if c in links]),
        on="property_id", how="left", validate="one_to_one"
    )
    if len(result) != before:
        raise ValueError("Final merge changed the analysis row count")

    diagnostics = result.groupby("state").agg(
        nfip_properties=("property_id", "size"),
        attom_matched=("assigned_attomid", lambda x: x.notna().sum()),
        builty_attom_nfip=("builty_elevated", lambda x: x.eq(1).sum()),
        with_attom_value=("attom_value_year", lambda x: x.notna().sum()),
    ).reset_index()
    diagnostics["attom_match_rate"] = diagnostics["attom_matched"] / diagnostics["nfip_properties"]
    diagnostics["builty_share_all_nfip"] = diagnostics["builty_attom_nfip"] / diagnostics["nfip_properties"]
    diagnostics["builty_share_attom"] = diagnostics["builty_attom_nfip"] / diagnostics["attom_matched"]

    # Stata variable names are limited to 32 characters. Use one explicit name
    # in both outputs instead of allowing pandas to truncate it implicitly.
    stata_names = {
        "attom_assessed_value_improvements": "attom_assessed_improvements",
    }
    links = links.rename(columns=stata_names)
    result = result.rename(columns=stata_names)

    # Stata strings cannot use pandas StringDtype reliably.
    for frame in [links, result]:
        for column in frame.select_dtypes(include=["string", "object"]).columns:
            frame[column] = frame[column].map(lambda x: None if pd.isna(x) else str(x)).astype(object)
    Path(a.out_links).parent.mkdir(parents=True, exist_ok=True)
    links.to_stata(a.out_links, write_index=False, version=118)
    result.to_stata(a.out_analysis, write_index=False, version=118)
    diagnostics.to_csv(a.diagnostics, index=False)
    print(diagnostics.to_string(index=False))
    print(f"Saved {a.out_links}")
    print(f"Saved {a.out_analysis}")


if __name__ == "__main__":
    main()
