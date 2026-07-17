"""
Create coverage descriptives figures.

Authors: Anna Li 
Original Date: 2026-05-02
Revised Date: 2026-07-17
Revision: Changed format and added the specification of FMA and the property level graphs. 
Description:
   Create coverage descriptions for Builty, FMA, and HMGP. FMA and HMGP are done for both project level and property level.

"""

import argparse
import json
import re
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd

STATE_FIPS = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY",
}

def parse_args():
    # Define the input files, output folder, and optional sample restrictions.
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--builty")
    parser.add_argument("--hma")
    parser.add_argument("--hma-mitigated")
    parser.add_argument("--out-dir", default="output/builty_fma_hmgp_coverage")
    parser.add_argument("--county-geojson", default="output/reference/geojson_counties_fips.json")
    parser.add_argument("--states", nargs="*")
    parser.add_argument("--builty-scope", choices=["raw", "filtered-elevation"], default="raw")
    parser.add_argument(
        "--project-scope",
        choices=["overall", "any-elevation", "private-elevation"],
        default="overall",
    )
    return parser.parse_args()


def normalize_fips(series, width):
    # Convert numeric FEMA codes to zero-padded strings used in county FIPS.
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.astype("Int64").astype(str).str.zfill(width).mask(numeric.isna())


def read_builty_dta(path, states, scope):
    # Read the elevation stata file in chunks and reduce each chunk to county counts.
    cols = ["STATE", "FIPS_STATE", "FIPS_COUNTY", "DATE_ISSUED"]
    if scope == "filtered-elevation":
        cols.append("final_flag")
    county_chunks = []
    year_chunks = []
    reader = pd.read_stata(
        path, columns=cols, convert_categoricals=False, chunksize=1_000_000
    )

    for chunk in reader:
        # Apply the pipeline's elevation flag when filtered coverage is requested.
        if scope == "filtered-elevation":
            chunk = chunk[chunk["final_flag"].eq(1)]

        # Standardize state and county identifiers, then apply the state sample.
        chunk["state"] = chunk["STATE"].astype(str).str.strip().str.upper()
        if states:
            chunk = chunk[chunk["state"].isin(states)]
        fips_state = chunk["FIPS_STATE"].astype(str).str.strip()
        fips_county = chunk["FIPS_COUNTY"].astype(str).str.strip()
        chunk = chunk.assign(
            county_fips=fips_state.str.zfill(2) + fips_county.str.zfill(3)
        )[fips_state.ne("") & fips_county.ne("")]
        # Store county totals and unique county-years for the final aggregation.
        county_chunks.append(chunk.groupby(["state", "county_fips"]).size())
        years = pd.to_numeric(chunk["DATE_ISSUED"].astype(str).str[:4], errors="coerce")
        year_chunks.append(
            chunk.assign(year=years)
            .dropna(subset=["year"])[["state", "county_fips", "year"]]
            .drop_duplicates()
        )
    # Combine the chunk-level results into one county and county-year table.
    counties = (
        pd.concat(county_chunks)
        .groupby(level=["state", "county_fips"]).sum()
        .reset_index(name="record_count")
    )
    county_years = pd.concat(year_chunks).drop_duplicates()
    return counties, county_years


def read_builty(path, states, scope):
    # Use pandas for Stata input and DuckDB for parquet input.
    if path.suffix == ".dta":
        return read_builty_dta(path, states, scope)

    state_filter = ""
    if states:
        state_filter = "AND upper(STATE) IN (" + ",".join(f"'{s}'" for s in states) + ")"

    scope_filter = "AND final_flag = 1" if scope == "filtered-elevation" else ""

    path = path.as_posix().replace("'", "''")
    con = duckdb.connect()
    counties = con.execute(
        f"""
        SELECT
            upper(STATE) AS state,
            lpad(CAST(try_cast(FIPS_STATE AS INTEGER) AS VARCHAR), 2, '0') ||
            lpad(CAST(try_cast(FIPS_COUNTY AS INTEGER) AS VARCHAR), 3, '0') AS county_fips,
            count(*) AS record_count
        FROM read_parquet('{path}')
        WHERE FIPS_STATE IS NOT NULL AND FIPS_COUNTY IS NOT NULL
        {state_filter} {scope_filter}
        GROUP BY 1, 2
        """
    ).fetchdf()
    county_years = con.execute(
        f"""
        SELECT DISTINCT
            upper(STATE) AS state,
            lpad(CAST(try_cast(FIPS_STATE AS INTEGER) AS VARCHAR), 2, '0') ||
            lpad(CAST(try_cast(FIPS_COUNTY AS INTEGER) AS VARCHAR), 3, '0') AS county_fips,
            EXTRACT(YEAR FROM try_cast(DATE_ISSUED AS DATE)) AS year
        FROM read_parquet('{path}')
        WHERE FIPS_STATE IS NOT NULL AND FIPS_COUNTY IS NOT NULL
          AND try_cast(DATE_ISSUED AS DATE) IS NOT NULL
        {state_filter} {scope_filter}
        """
    ).fetchdf()
    return counties, county_years


def apply_scope(projects, scope):
    # Keep all projects, all elevation projects, or private-home elevation codes.
    if scope == "overall":
        return projects
    project_type = projects["projecttype"].astype(str)
    if scope == "any-elevation":
        return projects[project_type.str.contains("elevat", case=False, na=False)]
    keep = project_type.str.contains(
        r"\b202\.1[A-Z]?\b|\b202\.2[A-Z]?\b", regex=True, na=False
    )
    return projects[keep]


def read_program(hma, program, states, scope):
    # Select one FEMA program and the requested project scope.
    projects = hma[hma["programarea"].astype(str).str.upper().eq(program)].copy()
    projects = apply_scope(projects, scope)
    # Build state abbreviations, county FIPS, and program years.
    projects["state_fips"] = normalize_fips(projects["statenumbercode"], 2)
    projects["county_code"] = normalize_fips(projects["countycode"], 3)
    projects["state"] = projects["state_fips"].map(STATE_FIPS)
    projects["county_fips"] = projects["state_fips"] + projects["county_code"]
    projects["year"] = pd.to_numeric(projects["programfy"], errors="coerce")
    # Remove statewide records and apply the optional state restriction.
    projects = projects[
        projects["county_code"].notna()
        & projects["county_code"].ne("000")
        & projects["state"].notna()
    ]
    if states:
        projects = projects[projects["state"].isin(states)]
    # Collapse the project records to county counts and unique county-years.
    counties = (
        projects.groupby(["state", "county_fips"], as_index=False)
        .size()
        .rename(columns={"size": "record_count"})
    )
    county_years = projects[["state", "county_fips", "year"]].dropna().drop_duplicates()
    return counties, county_years


def normalize_county_name(name):
    # Standardize FEMA county names to match names in the county GeoJSON.
    name = re.sub(r"\s*\(.*\)\s*$", "", str(name).strip().upper())
    for short, full in [("E. ", "EAST "), ("W. ", "WEST "), ("N. ", "NORTH "), ("S. ", "SOUTH ")]:
        if name.startswith(short):
            name = full + name[len(short):]
    return name


def county_name_crosswalk(geojson):
    # Map each standardized state-county name pair to its five-digit FIPS code.
    features = json.loads(Path(geojson).read_text())["features"]
    xwalk = {}
    for feature in features:
        props = feature["properties"]
        key = (props["STATE"], normalize_county_name(props["NAME"]))
        # On county/independent-city name ties (e.g. Richmond VA), prefer the county.
        if key not in xwalk or props["LSAD"] == "County":
            xwalk[key] = feature["id"]
    return xwalk


def read_mitigated(path, program, states, scope, xwalk):
    # Read the property-level FEMA file and select the requested program.
    props = pd.read_csv(path, low_memory=False)
    props.columns = props.columns.str.strip().str.lower()
    props = props[props["programarea"].astype(str).str.upper().eq(program)].copy()
    if scope != "overall":
        props = props[
            props["propertyaction"].astype(str).str.contains("elevation", case=False, na=False)
        ]
    # Convert state and county names to the geographic identifiers used in maps.
    props["state_fips"] = normalize_fips(props["statenumbercode"], 2)
    props["state"] = props["state_fips"].map(STATE_FIPS)
    props["county_fips"] = [
        xwalk.get((state_fips, normalize_county_name(county)))
        for state_fips, county in zip(props["state_fips"], props["county"])
    ]
    props["year"] = pd.to_numeric(props["programfy"], errors="coerce")
    props["numberofproperties"] = pd.to_numeric(
        props["numberofproperties"], errors="coerce"
    ).fillna(0)
    props = props[props["state"].notna() & props["county_fips"].notna()]
    if states:
        props = props[props["state"].isin(states)]
    # Sum reported properties by county and retain unique county-years.
    counties = props.groupby(["state", "county_fips"], as_index=False).agg(
        record_count=("numberofproperties", "sum")
    )
    counties = counties[counties["record_count"] > 0]
    county_years = props[["state", "county_fips", "year"]].dropna().drop_duplicates()
    return counties, county_years


def combine_counties(builty, fma, hmgp):
    # Outer-join the three sources and create one coverage flag per source.
    coverage = builty[["state", "county_fips"]].assign(in_builty=1)
    coverage = coverage.merge(
        fma[["state", "county_fips"]].assign(in_fma=1),
        on=["state", "county_fips"], how="outer",
    )
    coverage = coverage.merge(
        hmgp[["state", "county_fips"]].assign(in_hmgp=1),
        on=["state", "county_fips"], how="outer",
    )
    flags = ["in_builty", "in_fma", "in_hmgp"]
    coverage[flags] = coverage[flags].fillna(0).astype(int)
    return coverage.sort_values(["state", "county_fips"])


def make_summaries(coverage, builty_years, fma_years, hmgp_years):
    # Count covered counties and county-years for the source-level summary.
    source = pd.DataFrame(
        {
            "source": ["Builty", "FMA", "HMGP"],
            "counties": [coverage.in_builty.sum(), coverage.in_fma.sum(), coverage.in_hmgp.sum()],
            "county_years": [len(builty_years), len(fma_years), len(hmgp_years)],
        }
    )
    # Count covered counties separately within each state.
    state = (
        coverage.groupby("state", as_index=False)
        .agg(
            builty_counties=("in_builty", "sum"),
            fma_counties=("in_fma", "sum"),
            hmgp_counties=("in_hmgp", "sum"),
        )
        .sort_values("state")
    )
    return source, state


def plot_source(source, path, suptitle="Geographic Coverage by Data Source"):
    # Plot total county and county-year coverage for each source.
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    colors = ["#4C78A8", "#F2A541", "#E45756"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 5))
    for ax, column, title in [
        (axes[0], "counties", "Counties covered"),
        (axes[1], "county_years", "County-years covered"),
    ]:
        bars = ax.bar(source.source, source[column], color=colors)
        ax.bar_label(bars, labels=[f"{value:,.0f}" for value in source[column]], padding=3)
        ax.set_title(title)
        ax.set_ylabel("Count")
        ax.spines[["top", "right"]].set_visible(False)
    fig.suptitle(suptitle)
    fig.tight_layout()
    fig.savefig(path, dpi=300)
    plt.close(fig)


def plot_states(state, path, title="County Coverage by State and Data Source"):
    # Plot side-by-side county coverage counts within each state.
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    state = state.sort_values("state", ascending=False).reset_index(drop=True)
    y = np.arange(len(state))
    fig, ax = plt.subplots(figsize=(10, max(5.5, 0.32 * len(state))))
    width = 0.25
    ax.barh(y - width, state.builty_counties, height=width, label="Builty", color="#4C78A8")
    ax.barh(y, state.fma_counties, height=width, label="FMA", color="#F2A541")
    ax.barh(y + width, state.hmgp_counties, height=width, label="HMGP", color="#E45756")
    ax.set_yticks(y, state.state)
    ax.set_xlabel("Counties covered")
    ax.set_title(title)
    ax.legend(frameon=False)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=300)
    plt.close(fig)


def plot_dot_map(
    data,
    geojson,
    path,
    states,
    title,
    subtitle,
    legend_title,
    source_note,
    color="#2F7FB5",
):
    # Draw county centroids with marker area scaled to the source record count.
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    background = "#F7F5EF"
    land = "#F2EFE7"
    outline = "#918B80"
    navy = "#102A43"

    # Load the county geography and retain the requested contiguous-US sample.
    data = data[data["record_count"] > 0].copy()
    counties = gpd.read_file(geojson)
    counties["county_fips"] = counties["id"].astype(str).str.zfill(5)
    counties["state_fips"] = counties["county_fips"].str[:2]
    counties = counties[
        ~counties["state_fips"].isin({"02", "15", "60", "66", "69", "72", "78"})
    ].copy()
    if states:
        selected = {fips for fips, state in STATE_FIPS.items() if state in states}
        counties = counties[counties["state_fips"].isin(selected)].copy()

    # Join counts to county shapes and calculate representative county points.
    states_map = counties.dissolve(by="state_fips")
    points = counties[["county_fips", "geometry"]].merge(
        data[["county_fips", "record_count"]], on="county_fips", how="inner"
    )
    projected = points.to_crs(5070)
    points = gpd.GeoDataFrame(
        points.drop(columns="geometry"),
        geometry=projected.geometry.representative_point(),
        crs=5070,
    ).to_crs(4326)

    # Scale dot sizes relative to the largest county count.
    max_count = float(points["record_count"].max())
    points["marker_size"] = 16 + 340 * np.sqrt(points["record_count"] / max_count)

    # Draw the state background and scaled county dots.
    fig, ax = plt.subplots(figsize=(16, 10), facecolor=background)
    ax.set_facecolor(background)
    states_map.plot(ax=ax, color=land, edgecolor=outline, linewidth=0.65, zorder=1)
    ax.scatter(
        points.geometry.x,
        points.geometry.y,
        s=points["marker_size"],
        color=color,
        alpha=0.88,
        edgecolors="white",
        linewidths=0.35,
        zorder=3,
    )

    # Build a three-point size legend from the minimum, median, and maximum.
    legend_values = sorted(
        set(
            int(value)
            for value in [
                points["record_count"].min(),
                points["record_count"].median(),
                points["record_count"].max(),
            ]
        )
    )
    handles = [
        ax.scatter(
            [], [],
            s=16 + 340 * np.sqrt(value / max_count),
            color=color,
            alpha=0.88,
            edgecolors="white",
            linewidths=0.35,
            label=f"{value:,.0f}",
        )
        for value in legend_values
    ]
    ax.legend(
        handles=handles,
        title=legend_title,
        loc="lower left",
        frameon=True,
        facecolor="white",
        edgecolor="#D7D2C8",
        fontsize=9,
        title_fontsize=10,
    )

    # Add titles and source notes, then save the figure.
    if not states:
        ax.set_xlim(-125, -66)
        ax.set_ylim(24, 50)
    ax.set_axis_off()
    fig.suptitle(
        f"{title}\n{subtitle}",
        fontsize=20,
        fontweight="bold",
        color=navy,
        y=0.96,
        linespacing=1.35,
    )
    fig.text(
        0.5,
        0.025,
        source_note,
        ha="center",
        fontsize=9.5,
        color="#6B665E",
    )
    fig.subplots_adjust(left=0.025, right=0.99, top=0.88, bottom=0.075)
    fig.savefig(path, dpi=300, facecolor=background)
    plt.close(fig)


def plot_heatmap(
    data, geojson, path, states, title, subtitle, legend_title, source_note
):
    # Draw a county choropleth using a log scale for source record counts.
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import LogNorm
    from matplotlib.ticker import FuncFormatter, LogLocator

    background = "#F7F5EF"
    land = "#F2EFE7"
    outline = "#918B80"
    navy = "#102A43"

    # Load the county geography and retain the requested contiguous-US sample.
    counties = gpd.read_file(geojson)
    counties["county_fips"] = counties["id"].astype(str).str.zfill(5)
    counties["state_fips"] = counties["county_fips"].str[:2]
    counties = counties[
        ~counties["state_fips"].isin({"02", "15", "60", "66", "69", "72", "78"})
    ].copy()
    if states:
        selected = {fips for fips, state in STATE_FIPS.items() if state in states}
        counties = counties[counties["state_fips"].isin(selected)].copy()

    # Join county counts and define the color scale from positive observations.
    states_map = counties.dissolve(by="state_fips")
    counties = counties.merge(
        data[["county_fips", "record_count"]], on="county_fips", how="left"
    )
    positive = counties[counties["record_count"] > 0].copy()
    vmin = float(positive["record_count"].min())
    vmax = float(positive["record_count"].max())
    norm = LogNorm(vmin=vmin, vmax=vmax)

    # Draw the base counties, positive-count counties, and state borders.
    fig, ax = plt.subplots(figsize=(16, 10), facecolor=background)
    ax.set_facecolor(background)
    counties.plot(ax=ax, color=land, edgecolor="white", linewidth=0.08, zorder=1)
    positive.plot(
        ax=ax, column="record_count", cmap="OrRd", norm=norm,
        edgecolor="white", linewidth=0.08, zorder=2,
    )
    states_map.boundary.plot(ax=ax, color=outline, linewidth=0.65, zorder=3)

    # Add titles, colorbar, and source note, then save the figure.
    if not states:
        ax.set_xlim(-125, -66)
        ax.set_ylim(24, 50)
    ax.set_axis_off()
    fig.suptitle(
        f"{title}\n{subtitle}", fontsize=20, fontweight="bold", color=navy,
        y=0.96, linespacing=1.35,
    )
    colorbar_ax = fig.add_axes([0.23, 0.09, 0.54, 0.025])
    colorbar = fig.colorbar(
        plt.cm.ScalarMappable(norm=norm, cmap="OrRd"),
        cax=colorbar_ax, orientation="horizontal",
    )
    colorbar.locator = LogLocator(base=10)
    colorbar.formatter = FuncFormatter(lambda value, _: f"{value:,.0f}")
    colorbar.update_ticks()
    colorbar.set_label(legend_title, fontsize=10, color=navy)
    colorbar.ax.tick_params(labelsize=9, colors=navy)
    fig.text(0.5, 0.025, source_note, ha="center", fontsize=9.5, color="#6B665E")
    fig.subplots_adjust(left=0.025, right=0.99, top=0.88, bottom=0.15)
    fig.savefig(path, dpi=300, facecolor=background)
    plt.close(fig)


def plot_map(coverage, geojson, path, states, title="Builty, FMA, and HMGP County Coverage"):
    # Draw categorical county coverage for every combination of the three sources.
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt

    # Load county geography and merge the three source flags.
    counties = gpd.read_file(geojson)
    counties["county_fips"] = counties["id"].astype(str).str.zfill(5)
    counties["state_fips"] = counties.county_fips.str[:2]
    counties = counties[~counties.state_fips.isin({"02", "15", "60", "66", "69", "72", "78"})]
    if states:
        selected = {fips for fips, state in STATE_FIPS.items() if state in states}
        counties = counties[counties.state_fips.isin(selected)]
    counties = counties.merge(coverage.drop(columns="state"), on="county_fips", how="left")
    flags = ["in_builty", "in_fma", "in_hmgp"]
    counties[flags] = counties[flags].fillna(0).astype(int)

    # Convert the three binary flags into a readable overlap category.
    def group(row):
        names = [
            name for name, flag in
            [("Builty", "in_builty"), ("FMA", "in_fma"), ("HMGP", "in_hmgp")]
            if row[flag]
        ]
        return " + ".join(names) if names else "None"

    counties["group"] = counties.apply(group, axis=1)
    colors = {
        "None": "#F0F0F0", "Builty": "#4C78A8", "FMA": "#F2A541",
        "HMGP": "#E45756", "Builty + FMA": "#59A14F",
        "Builty + HMGP": "#B279A2", "FMA + HMGP": "#FF9DA7",
        "Builty + FMA + HMGP": "#2F4B7C",
    }
    order = [
        "Builty + FMA + HMGP", "Builty + FMA", "Builty + HMGP", "FMA + HMGP",
        "Builty", "FMA", "HMGP", "None",
    ]
    # Draw each overlap category and save the completed map.
    fig, ax = plt.subplots(figsize=(14, 8.5))
    for name in order[::-1]:
        subset = counties[counties.group.eq(name)]
        if subset.empty:
            continue
        subset.plot(ax=ax, color=colors[name], edgecolor="white", linewidth=0.08)
    if not states:
        ax.set_xlim(-125, -66)
        ax.set_ylim(24, 50)
    ax.set_axis_off()
    ax.set_title(title)
    present = set(counties.group)
    handles = [mpatches.Patch(color=colors[name], label=name) for name in order if name in present]
    ax.legend(handles=handles, loc="lower left", ncol=2, title="County status")
    fig.tight_layout()
    fig.savefig(path, dpi=300)
    plt.close(fig)


def main():
    # Resolve command-line paths and create the output directory.
    args = parse_args()
    data = Path(args.data)
    states = {state.upper() for state in args.states} if args.states else None
    # Set the default Builty and FEMA input files beneath the supplied data root.
    builty = Path(args.builty) if args.builty else data / "clean/all_builty_elevations.dta"
    hma = Path(args.hma) if args.hma else data / "raw/HazardMitigationAssistanceProjects.csv"
    hma_mitigated = (
        Path(args.hma_mitigated) if args.hma_mitigated
        else data / "raw/hma_mitigated_properties.csv"
    )
    geojson = Path(args.county_geojson)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Aggregate Builty permits to counties and county-years.
    builty_counties, builty_years = read_builty(builty, states, args.builty_scope)

    # Set 1: FEMA coverage from project-level HMA Projects (county codes).
    projects = pd.read_csv(hma, low_memory=False)
    projects.columns = projects.columns.str.strip().str.lower()
    fma_counties, fma_years = read_program(projects, "FMA", states, args.project_scope)
    hmgp_counties, hmgp_years = read_program(projects, "HMGP", states, args.project_scope)

    # Combine project-level coverage and create its summary charts and maps.
    coverage = combine_counties(builty_counties, fma_counties, hmgp_counties)
    source, state = make_summaries(coverage, builty_years, fma_years, hmgp_years)

    plot_source(source, out / "source_coverage.png")
    plot_states(state, out / "state_coverage.png")
    plot_map(coverage, geojson, out / "county_overlap_map.png", states)
    plot_dot_map(
        builty_counties,
        geojson,
        out / "builty_county_dotmap.png",
        states,
        "All Elevation Permits Across the United States",
        "County centroid dots scaled by elevation permit counts across all years",
        "Elevation permits",
        f"Source: {builty.name}. Map excludes Alaska and Hawaii.",
        color="#2F7FB5",
    )
    plot_dot_map(
        fma_counties,
        geojson,
        out / "fma_project_county_dotmap.png",
        states,
        "FMA Project Coverage Across the United States",
        "County centroid dots scaled by FMA project records across all years",
        "FMA project records",
        f"Source: {hma.name}. Map excludes Alaska and Hawaii.",
        color="#D9902F",
    )
    plot_dot_map(
        hmgp_counties,
        geojson,
        out / "hmgp_project_county_dotmap.png",
        states,
        "HMGP Project Coverage Across the United States",
        "County centroid dots scaled by HMGP project records across all years",
        "HMGP project records",
        f"Source: {hma.name}. Map excludes Alaska and Hawaii.",
        color="#C84C4C",
    )
    plot_heatmap(
        builty_counties, geojson, out / "builty_county_heatmap.png", states,
        "All Elevation Permits Across the United States",
        "County heatmap of elevation permit counts across all years",
        "Elevation permits",
        f"Source: {builty.name}. Map excludes Alaska and Hawaii.",
    )
    plot_heatmap(
        fma_counties, geojson, out / "fma_project_county_heatmap.png", states,
        "FMA Project Coverage Across the United States",
        "County heatmap of FMA project records across all years",
        "FMA project records",
        f"Source: {hma.name}. Map excludes Alaska and Hawaii.",
    )
    plot_heatmap(
        hmgp_counties, geojson, out / "hmgp_project_county_heatmap.png", states,
        "HMGP Project Coverage Across the United States",
        "County heatmap of HMGP project records across all years",
        "HMGP project records",
        f"Source: {hma.name}. Map excludes Alaska and Hawaii.",
    )

    # Set 2: FEMA coverage from property-level HMA Mitigated Properties
    # (county names mapped to FIPS; under-reports relative to Projects).
    xwalk = county_name_crosswalk(geojson)
    mit_fma_counties, mit_fma_years = read_mitigated(
        hma_mitigated, "FMA", states, args.project_scope, xwalk
    )
    mit_hmgp_counties, mit_hmgp_years = read_mitigated(
        hma_mitigated, "HMGP", states, args.project_scope, xwalk
    )

    # Combine property-level coverage and create its summary charts and maps.
    mit_coverage = combine_counties(builty_counties, mit_fma_counties, mit_hmgp_counties)
    mit_source, mit_state = make_summaries(
        mit_coverage, builty_years, mit_fma_years, mit_hmgp_years
    )

    plot_source(
        mit_source,
        out / "source_coverage_mitigated.png",
        suptitle="Geographic Coverage by Data Source (Mitigated Properties)",
    )
    plot_states(
        mit_state,
        out / "state_coverage_mitigated.png",
        title="County Coverage by State and Data Source (Mitigated Properties)",
    )
    plot_map(
        mit_coverage,
        geojson,
        out / "county_overlap_map_mitigated.png",
        states,
        title="Builty, FMA, and HMGP County Coverage (Mitigated Properties)",
    )
    plot_dot_map(
        mit_fma_counties,
        geojson,
        out / "fma_mitigated_county_dotmap.png",
        states,
        "FMA Mitigated-Property Coverage Across the United States",
        "County centroid dots scaled by the reported number of FMA properties",
        "FMA properties",
        f"Source: {hma_mitigated.name}. Map excludes Alaska and Hawaii.",
        color="#D9902F",
    )
    plot_dot_map(
        mit_hmgp_counties,
        geojson,
        out / "hmgp_mitigated_county_dotmap.png",
        states,
        "HMGP Mitigated-Property Coverage Across the United States",
        "County centroid dots scaled by the reported number of HMGP properties",
        "HMGP properties",
        f"Source: {hma_mitigated.name}. Map excludes Alaska and Hawaii.",
        color="#C84C4C",
    )
    plot_heatmap(
        mit_fma_counties, geojson, out / "fma_mitigated_county_heatmap.png", states,
        "FMA Mitigated-Property Coverage Across the United States",
        "County heatmap of the reported number of FMA properties across all years",
        "FMA properties",
        f"Source: {hma_mitigated.name}. Map excludes Alaska and Hawaii.",
    )
    plot_heatmap(
        mit_hmgp_counties, geojson, out / "hmgp_mitigated_county_heatmap.png", states,
        "HMGP Mitigated-Property Coverage Across the United States",
        "County heatmap of the reported number of HMGP properties across all years",
        "HMGP properties",
        f"Source: {hma_mitigated.name}. Map excludes Alaska and Hawaii.",
    )

if __name__ == "__main__":
    main()
