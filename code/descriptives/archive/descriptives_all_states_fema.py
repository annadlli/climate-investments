"""
descriptives_all_states_fema.py

National descriptives from:
  data/all_elevation_strict_filtered_fema.dta

This uses the all-state strict Builty elevation file with FEMA HMA and NPR
variables merged in. HMA variables are county-year values repeated on permit
rows, so funding/project totals are deduplicated to county-year before summing.
NPR variables are state-ZIP-year values, so NPR totals are deduplicated to
county-ZIP-year before county aggregation.
"""

from __future__ import annotations

import os
from pathlib import Path
import warnings

warnings.filterwarnings("ignore")

ROOT = Path("/Users/anna/Desktop/Research/climate-investments")
OUTDIR = ROOT / "output" / "descriptives" / "all_states_fema"
OUTDIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(OUTDIR / ".mplconfig"))

import geopandas as gpd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
from matplotlib.colors import LogNorm


ALL_FEMA_DTA = ROOT / "data" / "all_elevation_strict_filtered_fema.dta"
COUNTY_SHP = ROOT / "output/descriptives/counties_shp/cb_2022_us_county_5m.shp"

US_STATES_DC = {
    "AL", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
}

BG = "#f7f5ef"
EMPTY = "#efebe2"
EDGE = "#f8f6f0"
STATE_EDGE = "#6f665c"
INK = "#13293d"
BLUE = "#2166ac"
GREEN = "#1b7837"
ORANGE = "#b35806"
PURPLE = "#542788"


def money_label(x: float) -> str:
    if x >= 1e9:
        return f"${x / 1e9:.1f}B"
    if x >= 1e6:
        return f"${x / 1e6:.1f}M"
    if x >= 1e3:
        return f"${x / 1e3:.0f}k"
    return f"${x:.0f}"


def normalize_zip(series: pd.Series) -> pd.Series:
    z = series.fillna("").astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    z = z.str.slice(0, 5)
    z = z.where(z.ne("") & z.ne("."), "")
    z = z.mask(z.ne(""), z.str.zfill(5))
    return z.where(z.ne("00000"), "")


def load_data() -> pd.DataFrame:
    cols = [
        "STATE",
        "ZIPCODE",
        "fips_county",
        "event_year",
        "hma_n_projects_total",
        "hma_n_elev_total",
        "hma_n_buyout_total",
        "hma_project_amount",
        "hma_fed_obligated",
        "hma_fema_any",
        "hma_fema_elev",
        "npr_n_records",
        "npr_total_paid",
        "npr_fema_any",
    ]
    df = pd.read_stata(ALL_FEMA_DTA, columns=cols, convert_categoricals=False)
    df["STATE"] = df["STATE"].astype(str).str.strip().str.upper()
    df = df[df["STATE"].isin(US_STATES_DC)].copy()
    df["fips_county"] = df["fips_county"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(5)
    df["event_year"] = pd.to_numeric(df["event_year"], errors="coerce")
    df["zip_code"] = normalize_zip(df["ZIPCODE"])
    for col in [
        "hma_n_projects_total",
        "hma_n_elev_total",
        "hma_n_buyout_total",
        "hma_project_amount",
        "hma_fed_obligated",
        "hma_fema_any",
        "hma_fema_elev",
        "npr_n_records",
        "npr_total_paid",
        "npr_fema_any",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def build_county_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    permits = (
        df.groupby(["fips_county", "STATE"], as_index=False)
        .agg(n_elevation_permits=("fips_county", "size"))
        .rename(columns={"STATE": "state"})
    )

    county_year = (
        df[df["event_year"].notna()]
        .drop_duplicates(["fips_county", "event_year"])
        .groupby("fips_county", as_index=False)
        .agg(
            hma_n_projects_total=("hma_n_projects_total", "sum"),
            hma_n_elev_total=("hma_n_elev_total", "sum"),
            hma_n_buyout_total=("hma_n_buyout_total", "sum"),
            hma_project_amount=("hma_project_amount", "sum"),
            hma_fed_obligated=("hma_fed_obligated", "sum"),
            hma_active_years=("hma_fema_any", "sum"),
            hma_elev_active_years=("hma_fema_elev", "sum"),
        )
    )

    county_zip_year = (
        df[df["event_year"].notna() & df["zip_code"].ne("")]
        .drop_duplicates(["fips_county", "zip_code", "event_year"])
        .groupby("fips_county", as_index=False)
        .agg(
            npr_n_records=("npr_n_records", "sum"),
            npr_total_paid=("npr_total_paid", "sum"),
            npr_active_zipyears=("npr_fema_any", "sum"),
        )
    )

    county = permits.merge(county_year, on="fips_county", how="left")
    county = county.merge(county_zip_year, on="fips_county", how="left")
    numeric_cols = [c for c in county.columns if c not in {"fips_county", "state"}]
    county[numeric_cols] = county[numeric_cols].fillna(0)
    return county


def load_shapes(county: pd.DataFrame) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(COUNTY_SHP)
    gdf = gdf[gdf["STUSPS"].isin(US_STATES_DC)].copy()
    gdf["fips_county"] = gdf["GEOID"]
    gdf = gdf.merge(county, on="fips_county", how="left")
    fill_cols = [
        "n_elevation_permits",
        "hma_n_projects_total",
        "hma_n_elev_total",
        "hma_n_buyout_total",
        "hma_project_amount",
        "hma_fed_obligated",
        "hma_active_years",
        "hma_elev_active_years",
        "npr_n_records",
        "npr_total_paid",
        "npr_active_zipyears",
    ]
    for col in fill_cols:
        gdf[col] = gdf[col].fillna(0)
    gdf = gdf.to_crs(5070)
    centroids = gdf.geometry.centroid
    gdf["cx"] = centroids.x
    gdf["cy"] = centroids.y
    return gdf


def state_boundaries(gdf: gpd.GeoDataFrame) -> gpd.GeoSeries:
    return gdf.dissolve(by="STUSPS", as_index=False)[["STUSPS", "geometry"]].boundary


def bubble_sizes(values: pd.Series, max_size: float = 700, min_size: float = 18) -> pd.Series:
    clean = values.fillna(0).astype(float).clip(lower=0)
    if clean.max() <= 0:
        return pd.Series(np.repeat(min_size, len(clean)), index=values.index)
    return min_size + np.sqrt(clean / clean.max()) * (max_size - min_size)


def bubble_size_for_value(value: float, max_value: float, max_size: float = 700, min_size: float = 18) -> float:
    if max_value <= 0:
        return min_size
    return min_size + np.sqrt(max(value, 0) / max_value) * (max_size - min_size)


def annotate_top(ax, active: gpd.GeoDataFrame, value_col: str, is_money: bool = False, n: int = 12):
    for _, row in active.nlargest(n, value_col).iterrows():
        val = money_label(row[value_col]) if is_money else f"{int(row[value_col]):,}"
        ax.annotate(
            f"{row['NAME']}\n{val}",
            (row["cx"], row["cy"]),
            textcoords="offset points",
            xytext=(4, 4),
            fontsize=6,
            color="#222222",
            bbox=dict(boxstyle="round,pad=0.15", fc="white", alpha=0.68, lw=0),
        )


def draw_dotmap(
    gdf: gpd.GeoDataFrame,
    value_col: str,
    title: str,
    outname: str,
    color: str,
    legend_title: str,
    is_money: bool = False,
):
    fig, ax = plt.subplots(1, 1, figsize=(19, 12), facecolor=BG)
    fig.patch.set_facecolor(BG)
    boundaries = state_boundaries(gdf)
    gdf.plot(ax=ax, color=EMPTY, edgecolor=EDGE, linewidth=0.08)
    active = gdf[gdf[value_col] > 0].copy()
    if len(active):
        active.plot(ax=ax, color=EMPTY, edgecolor=EDGE, linewidth=0.08)
        ax.scatter(
            active["cx"],
            active["cy"],
            s=bubble_sizes(active[value_col]),
            color=color,
            alpha=0.72,
            edgecolors="white",
            linewidths=0.55,
            zorder=5,
        )
        annotate_top(ax, active, value_col, is_money=is_money)
        max_val = active[value_col].max()
        legend_vals = [max_val / 10, max_val / 3, max_val]
        for val in legend_vals:
            label = money_label(val) if is_money else f"{int(round(val)):,}"
            ax.scatter(
                [],
                [],
                s=bubble_size_for_value(val, max_val),
                color=color,
                alpha=0.72,
                edgecolors="white",
                linewidths=0.55,
                label=label,
            )
        ax.legend(title=legend_title, loc="lower left", fontsize=9, title_fontsize=10, framealpha=0.92)
    boundaries.plot(ax=ax, color=STATE_EDGE, linewidth=0.55, zorder=6)
    ax.set_title(title, fontsize=18, fontweight="bold", color=INK, pad=10)
    ax.set_axis_off()
    ax.set_facecolor(BG)
    plt.tight_layout()
    out = OUTDIR / outname
    plt.savefig(out, dpi=180, bbox_inches="tight")
    plt.close()
    print(f"Saved {out}")


def draw_hma_counts_vs_funding(gdf: gpd.GeoDataFrame):
    fig, axes = plt.subplots(1, 2, figsize=(22, 10), facecolor=BG)
    fig.patch.set_facecolor(BG)
    specs = [
        (axes[0], "hma_n_elev_total", "HMA Elevation Project Counts", BLUE, False, "Projects"),
        (axes[1], "hma_fed_obligated", "HMA Federal Obligated Funding", GREEN, True, "Federal dollars"),
    ]
    boundaries = state_boundaries(gdf)
    for ax, value_col, title, color, is_money, legend_title in specs:
        gdf.plot(ax=ax, color=EMPTY, edgecolor=EDGE, linewidth=0.08)
        active = gdf[gdf[value_col] > 0].copy()
        ax.scatter(
            active["cx"],
            active["cy"],
            s=bubble_sizes(active[value_col]),
            color=color,
            alpha=0.72,
            edgecolors="white",
            linewidths=0.55,
            zorder=5,
        )
        annotate_top(ax, active, value_col, is_money=is_money, n=10)
        boundaries.plot(ax=ax, color=STATE_EDGE, linewidth=0.55, zorder=6)
        ax.set_title(title, fontsize=15, fontweight="bold", color=INK)
        ax.set_axis_off()
        ax.set_facecolor(BG)
        max_val = active[value_col].max() if len(active) else 0
        if max_val > 0:
            for val in [max_val / 10, max_val / 3, max_val]:
                label = money_label(val) if is_money else f"{int(round(val)):,}"
                ax.scatter([], [], s=bubble_size_for_value(val, max_val), color=color, alpha=0.72, label=label)
            ax.legend(title=legend_title, loc="lower left", fontsize=8, title_fontsize=9, framealpha=0.92)
    plt.suptitle("All-State FEMA HMA: Project Counts vs Funding Amounts", fontsize=18, fontweight="bold", color=INK)
    plt.tight_layout()
    out = OUTDIR / "map_us_hma_counts_vs_funding_all_states.png"
    plt.savefig(out, dpi=180, bbox_inches="tight")
    plt.close()
    print(f"Saved {out}")


def draw_choropleth(gdf: gpd.GeoDataFrame, value_col: str, title: str, outname: str, cmap: str, is_money: bool = False):
    fig, ax = plt.subplots(1, 1, figsize=(19, 12), facecolor=BG)
    fig.patch.set_facecolor(BG)
    gdf.plot(ax=ax, color=EMPTY, edgecolor=EDGE, linewidth=0.08)
    active = gdf[gdf[value_col] > 0].copy()
    if len(active):
        norm = LogNorm(vmin=max(active[value_col].min(), 1), vmax=active[value_col].max())
        active.plot(ax=ax, column=value_col, cmap=cmap, norm=norm, edgecolor=EDGE, linewidth=0.08)
        sm = plt.cm.ScalarMappable(norm=norm, cmap=cmap)
        sm.set_array([])
        cbar = fig.colorbar(sm, ax=ax, orientation="horizontal", fraction=0.04, pad=0.02, shrink=0.82)
        cbar.set_label(title.split("\n")[0], fontsize=10, color=INK)
        if is_money:
            cbar.ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: money_label(x)))
        else:
            cbar.ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        cbar.ax.tick_params(labelsize=8, colors=INK)
    state_boundaries(gdf).plot(ax=ax, color=STATE_EDGE, linewidth=0.55, zorder=6)
    ax.set_title(title, fontsize=18, fontweight="bold", color=INK, pad=10)
    ax.set_axis_off()
    ax.set_facecolor(BG)
    plt.tight_layout()
    out = OUTDIR / outname
    plt.savefig(out, dpi=180, bbox_inches="tight")
    plt.close()
    print(f"Saved {out}")


def draw_year_bars(df: pd.DataFrame):
    permits = df.groupby("event_year", as_index=False).agg(n_elevation_permits=("fips_county", "size"))
    hma_cy = (
        df[df["event_year"].notna()]
        .drop_duplicates(["fips_county", "event_year"])
        .groupby("event_year", as_index=False)
        .agg(
            hma_n_elev_total=("hma_n_elev_total", "sum"),
            hma_fed_obligated=("hma_fed_obligated", "sum"),
        )
    )
    annual = permits.merge(hma_cy, on="event_year", how="left").fillna(0)
    annual = annual[(annual["event_year"] >= 2000) & (annual["event_year"] <= 2026)].copy()
    annual["event_year"] = annual["event_year"].astype(int)
    annual.to_csv(OUTDIR / "annual_all_states_fema_summary.csv", index=False)

    fig, axes = plt.subplots(3, 1, figsize=(13, 11), sharex=True, facecolor=BG)
    fig.patch.set_facecolor(BG)
    axes[0].bar(annual["event_year"], annual["n_elevation_permits"], color=PURPLE, alpha=0.78)
    axes[1].bar(annual["event_year"], annual["hma_n_elev_total"], color=BLUE, alpha=0.78)
    axes[2].bar(annual["event_year"], annual["hma_fed_obligated"], color=GREEN, alpha=0.78)
    axes[0].set_title("Builty Elevation Permit Counts", fontsize=12, fontweight="bold", color=INK)
    axes[1].set_title("HMA Elevation Project Counts", fontsize=12, fontweight="bold", color=INK)
    axes[2].set_title("HMA Federal Obligated Funding", fontsize=12, fontweight="bold", color=INK)
    axes[0].set_ylabel("Permits")
    axes[1].set_ylabel("Projects")
    axes[2].set_ylabel("Federal dollars")
    axes[2].yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: money_label(y)))
    for ax in axes:
        ax.grid(axis="y", color="#cccccc", alpha=0.45)
        ax.set_facecolor(BG)
        ax.spines[["top", "right"]].set_visible(False)
    axes[2].set_xlabel("Year")
    plt.suptitle("All-State Elevation Activity: Counts vs Funding", fontsize=15, fontweight="bold", color=INK)
    plt.tight_layout()
    out = OUTDIR / "bar_all_states_counts_vs_funding_by_year.png"
    plt.savefig(out, dpi=180, bbox_inches="tight")
    plt.close()
    print(f"Saved {out}")


def write_tables(df: pd.DataFrame, county: pd.DataFrame):
    state_summary = (
        county.groupby("state", as_index=False)
        .agg(
            counties_with_permits=("fips_county", "nunique"),
            n_elevation_permits=("n_elevation_permits", "sum"),
            hma_n_elev_total=("hma_n_elev_total", "sum"),
            hma_fed_obligated=("hma_fed_obligated", "sum"),
            hma_project_amount=("hma_project_amount", "sum"),
            npr_n_records=("npr_n_records", "sum"),
            npr_total_paid=("npr_total_paid", "sum"),
        )
        .sort_values("n_elevation_permits", ascending=False)
    )
    state_summary.to_csv(OUTDIR / "state_summary_all_states_fema.csv", index=False)
    county.sort_values("hma_fed_obligated", ascending=False).head(50).to_csv(
        OUTDIR / "top_counties_hma_funding_all_states.csv", index=False
    )
    county.sort_values("n_elevation_permits", ascending=False).head(50).to_csv(
        OUTDIR / "top_counties_elevation_permits_all_states.csv", index=False
    )


def main():
    df = load_data()
    county = build_county_aggregates(df)
    gdf = load_shapes(county)

    draw_dotmap(
        gdf,
        "n_elevation_permits",
        "All-State Builty Home Elevation Permits\nCounty bubble map",
        "map_us_elevation_permits_all_states_fema.png",
        PURPLE,
        "Permits",
    )
    draw_dotmap(
        gdf,
        "hma_n_elev_total",
        "All-State FEMA HMA Elevation Project Counts\nCounty-year totals deduplicated before county aggregation",
        "map_us_hma_elevation_project_counts_all_states.png",
        BLUE,
        "Projects",
    )
    draw_dotmap(
        gdf,
        "hma_fed_obligated",
        "All-State FEMA HMA Federal Obligated Funding\nCounty-year totals deduplicated before county aggregation",
        "map_us_hma_funding_all_states.png",
        GREEN,
        "Federal dollars",
        is_money=True,
    )
    draw_dotmap(
        gdf,
        "npr_n_records",
        "All-State FEMA NPR Record Counts\nCounty-ZIP-year totals deduplicated before county aggregation",
        "map_us_npr_records_all_states.png",
        ORANGE,
        "NPR records",
    )
    draw_choropleth(
        gdf,
        "hma_fed_obligated",
        "All-State FEMA HMA Federal Obligated Funding\nCounty heatmap",
        "heatmap_us_hma_funding_all_states.png",
        "YlGn",
        is_money=True,
    )
    draw_hma_counts_vs_funding(gdf)
    draw_year_bars(df)
    write_tables(df, county)


if __name__ == "__main__":
    main()
