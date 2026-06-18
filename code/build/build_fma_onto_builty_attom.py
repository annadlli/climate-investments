"""
Build FMA county-year measures onto Builty+ATTOM permits.

Authors: Anna Li and Vendela Norman
Date: 2026-06-17

Description:
    Aggregates FEMA FMA private-elevation projects to county-year and merges
    those county-year measures onto ATTOM-matched Builty elevation permits.

Notes / Sources:
    Permit input defaults to {data}/build/{state}_attom_permits_strict.parquet,
    produced by build_attom_onto_permits.py. FMA input defaults to
    {data}/clean/hma_projects.dta. Output defaults to
    {data}/build/{state}_attom_fma_permits_strict.parquet.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


STATE_FIPS = {
    "TX": "48",
    "VA": "51",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True, help="2-letter state abbreviation, e.g. TX")
    parser.add_argument(
        "--data",
        required=True,
        help="Dropbox data root. Input/output paths derive from this root.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    state = args.state.upper()
    state_lower = state.lower()
    if state not in STATE_FIPS:
        raise ValueError(f"Add state FIPS for {state} before running this state.")

    data = Path(args.data)
    permits_path = data / "build" / f"{state_lower}_attom_permits_strict.parquet"
    fma_path = data / "clean" / "hma_projects.dta"
    out_path = data / "build" / f"{state_lower}_attom_fma_permits_strict.parquet"
    diagnostics_path = out_path.with_name(f"{out_path.stem}_diagnostics.csv")

    print(f"State:        {state}")
    print(f"Builty+ATTOM: {permits_path}")
    print(f"FMA:          {fma_path}")
    print(f"Output:       {out_path}")

    print("\nLoading Builty+ATTOM permits...")
    permits = pd.read_parquet(permits_path)
    print(f"  Permits: {len(permits):,}")

    if "county_fips" not in permits.columns or "permit_year" not in permits.columns:
        raise KeyError("Expected county_fips and permit_year in Builty+ATTOM permits.")

    permits["county_fips"] = permits["county_fips"].astype(str).str.zfill(5)
    permits["permit_year"] = pd.to_numeric(permits["permit_year"], errors="coerce")

    print("\nLoading FMA projects...")
    fma = pd.read_stata(fma_path)
    print(f"  Raw HMA/FMA rows: {len(fma):,}")

    fma = fma[
        (pd.to_numeric(fma["statenumbercode"], errors="coerce") == int(STATE_FIPS[state]))
        & (fma["programarea"].astype(str).str.upper() == "FMA")
    ].copy()
    print(f"  {state} FMA rows: {len(fma):,}")

    project_type = fma["projecttype"].astype(str)
    fma = fma[project_type.str.contains(r"202\.[12]", regex=True, na=False)].copy()
    print(f"  Private-elevation rows: {len(fma):,}")

    funded_status = fma["status"].astype(str).str.upper().isin(["CLOSED", "OBLIGATED"])
    fma = fma[funded_status].copy()
    print(f"  Closed/obligated private-elevation rows: {len(fma):,}")

    fma["countycode"] = pd.to_numeric(fma["countycode"], errors="coerce")
    fma["year"] = pd.to_numeric(fma["programfy"], errors="coerce")
    fma = fma[(fma["countycode"] > 0) & fma["year"].notna()].copy()

    fma["county_fips"] = STATE_FIPS[state] + fma["countycode"].astype(int).astype(str).str.zfill(3)
    fma["year"] = fma["year"].astype(int)

    for col in [
        "projectamount",
        "federalshareobligated",
        "benefitcostratio",
        "netvaluebenefits",
        "numberofproperties",
        "numberoffinalproperties",
    ]:
        fma[col] = pd.to_numeric(fma[col], errors="coerce")

    fma["fma_project_row"] = 1
    fma_cy = (
        fma.groupby(["county_fips", "year"], dropna=False)
        .agg(
            fma_n_projects=("fma_project_row", "sum"),
            fma_project_amount=("projectamount", "sum"),
            fma_fed_obligated=("federalshareobligated", "sum"),
            fma_net_benefits=("netvaluebenefits", "sum"),
            fma_n_properties=("numberofproperties", "sum"),
            fma_n_final_properties=("numberoffinalproperties", "sum"),
            fma_avg_bcr=("benefitcostratio", "mean"),
        )
        .reset_index()
    )
    fma_cy["fma_any"] = (fma_cy["fma_n_projects"] > 0).astype(int)
    fma_cy["fma_log_fed_obligated"] = np.log(fma_cy["fma_fed_obligated"].fillna(0) + 1)
    print(f"  FMA county-year rows: {len(fma_cy):,}")

    print("\nMerging FMA county-year measures onto permits...")
    merged = permits.merge(
        fma_cy,
        left_on=["county_fips", "permit_year"],
        right_on=["county_fips", "year"],
        how="left",
    ).drop(columns=["year"], errors="ignore")

    fill_zero_cols = [
        "fma_n_projects",
        "fma_project_amount",
        "fma_fed_obligated",
        "fma_net_benefits",
        "fma_n_properties",
        "fma_n_final_properties",
        "fma_any",
        "fma_log_fed_obligated",
    ]
    for col in fill_zero_cols:
        merged[col] = merged[col].fillna(0)

    matched = int((merged["fma_any"] == 1).sum())
    print(f"  Permits in FMA county-years: {matched:,} / {len(merged):,}")

    diagnostics = (
        merged.assign(has_fma=merged["fma_any"] == 1)
        .groupby(["county_fips", "permit_year"], dropna=False)
        .agg(
            n_permits=("county_fips", "size"),
            n_permits_with_fma=("has_fma", "sum"),
            fma_n_projects=("fma_n_projects", "max"),
            fma_fed_obligated=("fma_fed_obligated", "max"),
        )
        .reset_index()
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(out_path, index=False)
    diagnostics.to_csv(diagnostics_path, index=False)

    print(f"\nSaved: {out_path}")
    print(f"Diagnostics: {diagnostics_path}")
    print(f"Shape: {merged.shape[0]:,} rows x {merged.shape[1]} columns")


if __name__ == "__main__":
    main()
