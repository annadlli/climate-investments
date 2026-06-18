#!/usr/bin/env python3
"""
Compare county coverage in Builty permits against FEMA HMGP projects.

Authors: Anna Li and Vendela Norman
Date: 2026-06-18

Description:
    Creates county-level coverage summaries and maps comparing raw Builty permit
    geography with FEMA HMGP project geography.

Notes / Sources:
    Pass the data root with --data. By default, this descriptives script compares
    raw Builty all-permit coverage against HMGP overall coverage and writes
    artifacts to output/raw_builty_hmgp_overall_coverage.

Outputs:
  - builty_hmgp_coverage_summary.csv
  - builty_hmgp_county_coverage_by_state.csv
  - hmgp_counties_missing_from_builty.csv
  - hmgp_county_years_missing_from_builty.csv
  - builty_hmgp_county_overlay_map.png
  - builty_hmgp_coverage.png
  - builty_hmgp_county_coverage_by_state.png
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd


DEFAULT_OUT_DIR = Path("output/raw_builty_hmgp_overall_coverage")
DEFAULT_COUNTY_GEOJSON = Path("output/reference/geojson_counties_fips.json")
HMA_FILENAME_CANDIDATES = (
    "hazard_mitigation_assistance_projects.csv",
    "HazardMitigationAssistanceProjects.csv",
    "HazardMitigationAssistanceProjects (1).csv",
)

STATE_FIPS = {
    "01": "AL",
    "02": "AK",
    "04": "AZ",
    "05": "AR",
    "06": "CA",
    "08": "CO",
    "09": "CT",
    "10": "DE",
    "11": "DC",
    "12": "FL",
    "13": "GA",
    "15": "HI",
    "16": "ID",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "20": "KS",
    "21": "KY",
    "22": "LA",
    "23": "ME",
    "24": "MD",
    "25": "MA",
    "26": "MI",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "30": "MT",
    "31": "NE",
    "32": "NV",
    "33": "NH",
    "34": "NJ",
    "35": "NM",
    "36": "NY",
    "37": "NC",
    "38": "ND",
    "39": "OH",
    "40": "OK",
    "41": "OR",
    "42": "PA",
    "44": "RI",
    "45": "SC",
    "46": "SD",
    "47": "TN",
    "48": "TX",
    "49": "UT",
    "50": "VT",
    "51": "VA",
    "53": "WA",
    "54": "WV",
    "55": "WI",
    "56": "WY",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare Builty permit geography to HMGP geography.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--data", required=True, help="Root data directory.")
    parser.add_argument(
        "--builty",
        default=None,
        help=(
            "Optional Builty parquet. Defaults to data/build/all_builty_elevations.parquet "
            "for filtered-elevation scope and data/raw/builty_all.parquet for raw scope."
        ),
    )
    parser.add_argument(
        "--hma",
        default=None,
        help=(
            "Optional FEMA HMA raw CSV. Defaults to the first existing HMA CSV "
            "candidate under {data}/raw."
        ),
    )
    parser.add_argument(
        "--out-dir",
        default=str(DEFAULT_OUT_DIR),
        help="Output directory for graph and CSVs.",
    )
    parser.add_argument(
        "--county-geojson",
        default=str(DEFAULT_COUNTY_GEOJSON),
        help="County GeoJSON with five-digit county FIPS IDs for map output.",
    )
    parser.add_argument(
        "--map-region",
        choices=["contiguous", "all"],
        default="contiguous",
        help="Map region to draw. Default excludes Alaska, Hawaii, and territories for readability.",
    )
    parser.add_argument(
        "--states",
        nargs="*",
        default=None,
        help="Optional state abbreviations to restrict the comparison, e.g. TX VA.",
    )
    parser.add_argument(
        "--hmgp-scope",
        choices=["private-elevation", "any-elevation", "overall"],
        default="overall",
        help=(
            "Which HMGP projects to compare against. Use private-elevation for "
            "project types containing 202.1 or 202.2."
        ),
    )
    parser.add_argument(
        "--builty-scope",
        choices=["filtered-elevation", "raw"],
        default="raw",
        help="Use final filtered elevation permits or the raw Builty all-permits file.",
    )
    return parser.parse_args()


def resolve_hma_path(data: Path, hma_arg: str | None) -> Path:
    if hma_arg:
        return Path(hma_arg)

    for filename in HMA_FILENAME_CANDIDATES:
        candidate = data / "raw" / filename
        if candidate.exists():
            return candidate

    candidates = ", ".join(str(data / "raw" / filename) for filename in HMA_FILENAME_CANDIDATES)
    raise FileNotFoundError(f"No HMA CSV found. Checked: {candidates}")


def normalize_fips(series: pd.Series, width: int) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.astype("Int64").astype(str).str.zfill(width).mask(numeric.isna())


def read_builty_locations(
    builty_path: Path, states: set[str] | None, builty_scope: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if builty_scope == "raw":
        return read_raw_builty_locations(builty_path, states)

    cols = ["STATE", "FIPS_STATE", "FIPS_COUNTY", "DATE_ISSUED"]
    if builty_scope == "filtered-elevation":
        cols.append("final_flag")
    builty = pd.read_parquet(builty_path, columns=cols)
    if builty_scope == "filtered-elevation":
        builty = builty[builty["final_flag"].eq(1)].copy()
    builty["state"] = builty["STATE"].astype(str).str.upper()
    builty["state_fips"] = normalize_fips(builty["FIPS_STATE"], 2)
    builty["county_code"] = normalize_fips(builty["FIPS_COUNTY"], 3)
    builty["county_fips"] = builty["state_fips"].fillna("") + builty["county_code"].fillna("")
    builty["permit_year"] = pd.to_datetime(builty["DATE_ISSUED"], errors="coerce").dt.year
    builty = builty.dropna(subset=["state", "county_fips"])
    if states:
        builty = builty[builty["state"].isin(states)].copy()

    counties = builty[["state", "county_fips"]].drop_duplicates()
    county_years = (
        builty.dropna(subset=["permit_year"])[["state", "county_fips", "permit_year"]]
        .assign(permit_year=lambda df: df["permit_year"].astype(int))
        .drop_duplicates()
    )
    return counties, county_years


def read_raw_builty_locations(builty_path: Path, states: set[str] | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    state_filter = ""
    if states:
        quoted_states = ", ".join(f"'{state}'" for state in sorted(states))
        state_filter = f"AND upper(STATE) IN ({quoted_states})"

    path = builty_path.as_posix().replace("'", "''")
    con = duckdb.connect()
    county_query = f"""
        SELECT DISTINCT
            upper(STATE) AS state,
            lpad(CAST(try_cast(FIPS_STATE AS INTEGER) AS VARCHAR), 2, '0') ||
                lpad(CAST(try_cast(FIPS_COUNTY AS INTEGER) AS VARCHAR), 3, '0') AS county_fips
        FROM read_parquet('{path}')
        WHERE FIPS_STATE IS NOT NULL
            AND FIPS_COUNTY IS NOT NULL
            {state_filter}
    """
    county_year_query = f"""
        SELECT DISTINCT
            upper(STATE) AS state,
            lpad(CAST(try_cast(FIPS_STATE AS INTEGER) AS VARCHAR), 2, '0') ||
                lpad(CAST(try_cast(FIPS_COUNTY AS INTEGER) AS VARCHAR), 3, '0') AS county_fips,
            EXTRACT(YEAR FROM try_cast(DATE_ISSUED AS DATE)) AS permit_year
        FROM read_parquet('{path}')
        WHERE FIPS_STATE IS NOT NULL
            AND FIPS_COUNTY IS NOT NULL
            AND try_cast(DATE_ISSUED AS DATE) IS NOT NULL
            {state_filter}
    """
    counties = con.execute(county_query).fetchdf().dropna(subset=["state", "county_fips"])
    county_years = con.execute(county_year_query).fetchdf().dropna(
        subset=["state", "county_fips", "permit_year"]
    )
    county_years["permit_year"] = county_years["permit_year"].astype(int)
    return counties, county_years


def filter_hmgp_scope(hmgp: pd.DataFrame, scope: str) -> pd.DataFrame:
    project_type = hmgp["projecttype"].astype(str)
    if scope == "overall":
        return hmgp.copy()
    if scope == "any-elevation":
        return hmgp[project_type.str.contains("elevat", case=False, na=False)].copy()
    private_elevation = project_type.str.contains(r"\b202\.1[A-Z]?\b|\b202\.2[A-Z]?\b", regex=True, na=False)
    return hmgp[private_elevation].copy()


def read_hmgp_locations(
    hma_path: Path, states: set[str] | None, scope: str
) -> tuple[pd.DataFrame, pd.DataFrame, int, int]:
    hma = pd.read_csv(hma_path, low_memory=False)
    hma.columns = [col.strip().lower() for col in hma.columns]

    hmgp = hma[hma["programarea"].astype(str).str.upper().eq("HMGP")].copy()
    hmgp = filter_hmgp_scope(hmgp, scope)
    hmgp_project_rows = len(hmgp)
    hmgp["state_fips"] = normalize_fips(hmgp["statenumbercode"], 2)
    hmgp["county_code"] = normalize_fips(hmgp["countycode"], 3)
    hmgp["state"] = hmgp["state_fips"].map(STATE_FIPS)
    hmgp["county_fips"] = hmgp["state_fips"].fillna("") + hmgp["county_code"].fillna("")
    hmgp["program_year"] = pd.to_numeric(hmgp["programfy"], errors="coerce").astype("Int64")

    if states:
        hmgp = hmgp[hmgp["state"].isin(states)].copy()
    statewide_rows = int((hmgp["county_code"] == "000").sum())
    hmgp = hmgp[
        hmgp["state"].notna()
        & hmgp["county_code"].notna()
        & hmgp["county_code"].ne("000")
    ].copy()

    counties = (
        hmgp.sort_values(["state", "county_fips", "county"])
        .groupby(["state", "county_fips"], as_index=False)
        .agg(county=("county", "first"))
    )
    county_years = (
        hmgp.dropna(subset=["program_year"])
        .assign(program_year=lambda df: df["program_year"].astype(int))
        .sort_values(["state", "county_fips", "program_year", "county"])
        .groupby(["state", "county_fips", "program_year"], as_index=False)
        .agg(county=("county", "first"))
    )
    return counties, county_years, statewide_rows, hmgp_project_rows


def coverage_summary(
    hmgp_counties: pd.DataFrame,
    hmgp_county_years: pd.DataFrame,
    builty_counties: pd.DataFrame,
    builty_county_years: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    county_matches = hmgp_counties.merge(
        builty_counties.assign(in_builty=1),
        on=["state", "county_fips"],
        how="left",
    )
    county_matches["covered_by_builty"] = county_matches["in_builty"].fillna(0).astype(int)
    county_matches = county_matches.drop(columns=["in_builty"])

    county_year_matches = hmgp_county_years.merge(
        builty_county_years.rename(columns={"permit_year": "program_year"}).assign(in_builty=1),
        on=["state", "county_fips", "program_year"],
        how="left",
    )
    county_year_matches["covered_by_builty"] = county_year_matches["in_builty"].fillna(0).astype(int)
    county_year_matches = county_year_matches.drop(columns=["in_builty"])

    rows = []
    for label, df in [
        ("HMGP counties", county_matches),
        ("HMGP county-years", county_year_matches),
    ]:
        total = len(df)
        covered = int(df["covered_by_builty"].sum())
        rows.append(
            {
                "unit": label,
                "hmgp_total": total,
                "covered_by_builty": covered,
                "missing_from_builty": total - covered,
                "coverage_rate": covered / total if total else pd.NA,
            }
        )
    return pd.DataFrame(rows), county_matches, county_year_matches


def state_coverage_summary(county_matches: pd.DataFrame) -> pd.DataFrame:
    state_summary = (
        county_matches.groupby("state", as_index=False)
        .agg(
            hmgp_counties=("county_fips", "nunique"),
            covered_by_builty=("covered_by_builty", "sum"),
        )
        .assign(
            missing_from_builty=lambda df: df["hmgp_counties"] - df["covered_by_builty"],
            coverage_rate=lambda df: df["covered_by_builty"] / df["hmgp_counties"],
        )
        .sort_values(["missing_from_builty", "hmgp_counties", "state"], ascending=[False, False, True])
    )
    return state_summary


def plot_coverage(summary: pd.DataFrame, out_path: Path, hmgp_scope: str, builty_scope: str) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(8.5, 5.5))

    x = range(len(summary))
    covered = summary["covered_by_builty"].astype(int)
    missing = summary["missing_from_builty"].astype(int)
    ax.bar(x, covered, label="Covered by Builty", color="#2F6B4F")
    ax.bar(x, missing, bottom=covered, label="Missing from Builty", color="#C95F45")

    ax.set_xticks(list(x))
    ax.set_xticklabels(summary["unit"])
    ax.set_ylabel("Count")
    hmgp_label = hmgp_scope.replace("-", " ").title()
    builty_label = builty_scope.replace("-", " ").title()
    ax.set_title(f"{builty_label} Builty Coverage of HMGP {hmgp_label} Locations")
    ax.legend(frameon=False)

    for idx, row in summary.iterrows():
        rate = row["coverage_rate"]
        label = "n/a" if pd.isna(rate) else f"{rate:.1%}"
        total = int(row["hmgp_total"])
        ax.text(idx, total, f" {label}", ha="center", va="bottom", fontsize=11)

    fig.tight_layout()
    fig.savefig(out_path, dpi=300)
    plt.close(fig)


def plot_state_coverage(
    state_summary: pd.DataFrame, out_path: Path, hmgp_scope: str, builty_scope: str
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plot_df = state_summary[state_summary["hmgp_counties"].gt(0)].copy()
    plot_df = plot_df.sort_values("hmgp_counties", ascending=True)

    plt.style.use("seaborn-v0_8-whitegrid")
    height = max(5.5, 0.28 * len(plot_df))
    fig, ax = plt.subplots(figsize=(9, height))
    y = range(len(plot_df))
    covered = plot_df["covered_by_builty"].astype(int)
    missing = plot_df["missing_from_builty"].astype(int)
    ax.barh(y, covered, label="Covered by Builty", color="#2F6B4F")
    ax.barh(y, missing, left=covered, label="Missing from Builty", color="#C95F45")
    ax.set_yticks(list(y))
    ax.set_yticklabels(plot_df["state"])
    ax.set_xlabel("HMGP counties")
    hmgp_label = hmgp_scope.replace("-", " ").title()
    builty_label = builty_scope.replace("-", " ").title()
    ax.set_title(f"{builty_label} Builty Coverage of HMGP {hmgp_label} Counties by State")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(out_path, dpi=300)
    plt.close(fig)


def plot_county_overlay_map(
    hmgp_counties: pd.DataFrame,
    builty_counties: pd.DataFrame,
    county_geojson: Path,
    out_path: Path,
    hmgp_scope: str,
    builty_scope: str,
    map_region: str,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt

    if not county_geojson.exists():
        raise FileNotFoundError(f"County GeoJSON not found: {county_geojson}")

    counties = gpd.read_file(county_geojson)
    counties["county_fips"] = counties["id"].astype(str).str.zfill(5)
    counties["state_fips"] = counties["county_fips"].str[:2]
    if map_region == "contiguous":
        counties = counties[~counties["state_fips"].isin({"02", "15", "60", "66", "69", "72", "78"})].copy()
    hmgp_set = set(hmgp_counties["county_fips"].astype(str))
    builty_set = set(builty_counties["county_fips"].astype(str))
    counties["in_hmgp"] = counties["county_fips"].isin(hmgp_set)
    counties["in_builty"] = counties["county_fips"].isin(builty_set)
    builty_label = builty_scope.replace("-", " ").title()
    builty_only_label = f"{builty_label} Builty only"
    counties["coverage_group"] = "Neither"
    counties.loc[counties["in_builty"] & ~counties["in_hmgp"], "coverage_group"] = builty_only_label
    counties.loc[~counties["in_builty"] & counties["in_hmgp"], "coverage_group"] = "HMGP only"
    counties.loc[counties["in_builty"] & counties["in_hmgp"], "coverage_group"] = "Both"

    colors = {
        "Neither": "#F1F1F1",
        builty_only_label: "#4C78A8",
        "HMGP only": "#E45756",
        "Both": "#5AA469",
    }

    fig, ax = plt.subplots(figsize=(14, 8.5))
    for group in ["Neither", builty_only_label, "HMGP only", "Both"]:
        counties[counties["coverage_group"].eq(group)].plot(
            ax=ax,
            color=colors[group],
            linewidth=0.05 if group == "Neither" else 0.12,
            edgecolor="white",
        )

    if map_region == "contiguous":
        ax.set_xlim(-125, -66)
        ax.set_ylim(24, 50)
    else:
        ax.set_xlim(-180, -64)
        ax.set_ylim(18, 72)
    ax.set_axis_off()
    hmgp_label = hmgp_scope.replace("-", " ").title()
    ax.set_title(f"{builty_label} Builty and HMGP {hmgp_label} County Coverage", fontsize=16, pad=12)
    legend_order = ["Both", "HMGP only", builty_only_label, "Neither"]
    handles = [mpatches.Patch(color=colors[group], label=group) for group in legend_order]
    ax.legend(handles=handles, loc="lower left", frameon=True, title="County status")
    fig.tight_layout()
    fig.savefig(out_path, dpi=300)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    data = Path(args.data)
    default_builty = (
        data / "raw/builty_all.parquet"
        if args.builty_scope == "raw"
        else data / "build/all_builty_elevations.parquet"
    )
    builty_path = Path(args.builty) if args.builty else default_builty
    hma_path = resolve_hma_path(data, args.hma)
    out_dir = Path(args.out_dir)
    county_geojson = Path(args.county_geojson)
    states = {state.upper() for state in args.states} if args.states else None

    if not builty_path.exists():
        raise FileNotFoundError(f"Builty parquet not found: {builty_path}")
    if not hma_path.exists():
        raise FileNotFoundError(f"HMA CSV not found: {hma_path}")

    out_dir.mkdir(parents=True, exist_ok=True)

    builty_counties, builty_county_years = read_builty_locations(builty_path, states, args.builty_scope)
    hmgp_counties, hmgp_county_years, statewide_rows, hmgp_project_rows = read_hmgp_locations(
        hma_path, states, args.hmgp_scope
    )
    summary, county_matches, county_year_matches = coverage_summary(
        hmgp_counties,
        hmgp_county_years,
        builty_counties,
        builty_county_years,
    )
    state_summary = state_coverage_summary(county_matches)

    summary_path = out_dir / "builty_hmgp_coverage_summary.csv"
    state_summary_path = out_dir / "builty_hmgp_county_coverage_by_state.csv"
    missing_counties_path = out_dir / "hmgp_counties_missing_from_builty.csv"
    missing_county_years_path = out_dir / "hmgp_county_years_missing_from_builty.csv"
    map_path = out_dir / "builty_hmgp_county_overlay_map.png"
    graph_path = out_dir / "builty_hmgp_coverage.png"
    state_graph_path = out_dir / "builty_hmgp_county_coverage_by_state.png"

    summary.to_csv(summary_path, index=False)
    state_summary.to_csv(state_summary_path, index=False)
    county_matches[county_matches["covered_by_builty"].eq(0)].sort_values(
        ["state", "county_fips", "county"]
    ).to_csv(missing_counties_path, index=False)
    county_year_matches[county_year_matches["covered_by_builty"].eq(0)].sort_values(
        ["state", "county_fips", "program_year", "county"]
    ).to_csv(missing_county_years_path, index=False)
    plot_county_overlay_map(
        hmgp_counties,
        builty_counties,
        county_geojson,
        map_path,
        args.hmgp_scope,
        args.builty_scope,
        args.map_region,
    )
    plot_coverage(summary, graph_path, args.hmgp_scope, args.builty_scope)
    plot_state_coverage(state_summary, state_graph_path, args.hmgp_scope, args.builty_scope)

    print(f"Builty scope:         {args.builty_scope}")
    print(f"Builty input:         {builty_path}")
    print(f"Builty counties:      {len(builty_counties):,}")
    print(f"Builty county-years:  {len(builty_county_years):,}")
    print(f"HMGP scope:           {args.hmgp_scope}")
    print(f"HMGP project rows:    {hmgp_project_rows:,}")
    print(f"HMGP counties:        {len(hmgp_counties):,}")
    print(f"HMGP county-years:    {len(hmgp_county_years):,}")
    print(f"HMGP statewide rows excluded from county coverage: {statewide_rows:,}")
    print()
    print(summary.to_string(index=False))
    print()
    print(f"Wrote summary:        {summary_path}")
    print(f"Wrote state summary:  {state_summary_path}")
    print(f"Wrote missing counties:      {missing_counties_path}")
    print(f"Wrote missing county-years:  {missing_county_years_path}")
    print(f"Wrote overlay map:    {map_path}")
    print(f"Wrote graph:          {graph_path}")
    print(f"Wrote state graph:    {state_graph_path}")


if __name__ == "__main__":
    main()
