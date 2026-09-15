"""
Authors: Anna Li
Date: 2026-09-15

ATTOM to Stata: the two files complete.do merges. Claude change 09-15.

  build/attom_links.dta   one row per NFIP property (state x property_id_state):
                          the assigned ATTOM ID, the match tier and the Builty
                          flags from the matcher (nfip_attom.py), all sample states
  build/attom_value.dta   one row per assigned ATTOM ID x policy year, market
                          value in 2023 dollars, panel years only

Replaces parquet_dta (links), attom_value_wide.py and attom_value_dta (values).
The value used to be one wide file per state over every ATTOM property in the
state (11-15M rows, 27 year columns), converted to Stata and merged onto the
panel one year at a time. Here the geocoded panel is filtered to the ATTOM
properties the matcher actually assigned (~6M across FL LA TX), kept long, and
written once, so complete.do merges it in one pass on (assigned_attomid,
policy_year). IDs are numeric so the merge keys are 4-byte integers, not strings.

Cleaning is unchanged from attom_value_wide.py: values <= 0 are missing (ATTOM
logs 0 where it holds no value), so are values below --min-value ($10,000
nominal, issue #26) and above --max-value ($100M); deflated with clean/cpi.dta.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

# what complete.do takes from the matcher, per NFIP property
LINK_COLUMNS = [
    "state", "property_id_state", "assigned_attomid", "match_tier_number",
    "builty_elevated", "builty_elevation_year", "builty_retrofit", "builty_new_construction",
    "builty_geo_backfilled", "builty_project_value", "builty_funding_type",
]
LINK_LABELS = {
    "property_id_state": "Property ID within state file",
    "assigned_attomid": "ATTOM property assigned by the matcher",
    "match_tier_number": "NFIP-ATTOM match tier (1 = block group x zone x year ... 15)",
    "builty_elevated": "Builty elevation permit on assigned ATTOM property",
    "builty_elevation_year": "Earliest Builty elevation-permit year",
    "builty_retrofit": "Builty permit elevates an existing structure",
    "builty_new_construction": "Builty permit is elevated new construction",
    "builty_geo_backfilled": "ATTOM coordinates/block group backfilled from Builty geocode",
    "builty_project_value": "Builty declared elevation project value (2023 $)",
    "builty_funding_type": "Funding source named in the Builty permit text",
}


def q(value: object) -> str:
    # sql-safe string literal
    return "'" + str(value).replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", required=True, help="Data root (from master.do).")
    p.add_argument("--states", required=True,
                   help="2-letter abbreviations, space- or comma-separated (master.do's `states`).")
    p.add_argument("--measure", default="market_value_total",
                   choices=["market_value_total", "assessed_value_total",
                            "market_value_improvements", "assessed_value_improvements"])
    p.add_argument("--year-min", type=int, default=2009, help="First panel year (nfip_hma_panel).")
    p.add_argument("--year-max", type=int, default=2025, help="Last panel year kept by complete.do.")
    p.add_argument("--min-value", type=float, default=1e4, help="Values below this (nominal $) are missing.")
    p.add_argument("--max-value", type=float, default=1e8, help="Values above this (nominal $) are missing.")
    p.add_argument("--memory", default="6GB", help="DuckDB memory cap; the geocoded panels are 6 GB each.")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--tmp", default=None, help="DuckDB spill directory (default: build/attom_stata_tmp).")
    return p.parse_args()


def stata_ints(frame: pd.DataFrame) -> pd.DataFrame:
    # pandas writes int32 as Stata long and int16 as int; a column with nulls has to
    # stay float so the missing values survive
    for column in frame.columns:
        series = frame[column]
        if pd.api.types.is_bool_dtype(series.dtype):
            frame[column] = series.astype("float64") if series.isna().any() else series.astype("int8")
        elif pd.api.types.is_integer_dtype(series.dtype) or pd.api.types.is_float_dtype(series.dtype):
            if series.isna().any():
                frame[column] = series.astype("float64")
            elif (series.dropna() == series.dropna().round()).all() and series.abs().max() < 2**31 - 2**5:
                frame[column] = series.astype("int32")
    return frame


def build_links(con: duckdb.DuckDBPyConnection, link_files: list[Path], out: Path) -> None:
    # one row per NFIP property, the assigned ATTOM ID as an integer
    files = "[" + ", ".join(q(f) for f in link_files) + "]"
    columns = ", ".join(
        "cast(assigned_attomid AS bigint) AS assigned_attomid" if c == "assigned_attomid"
        else "cast(property_id_state AS bigint) AS property_id_state" if c == "property_id_state"
        else c for c in LINK_COLUMNS)
    links = con.execute(f"""
        SELECT {columns}
        FROM read_parquet({files}, union_by_name=true)
        ORDER BY state, property_id_state
    """).df()
    n_dup = links.duplicated(["state", "property_id_state"]).sum()
    print(f"links: {len(links):,} NFIP properties, {links['assigned_attomid'].notna().sum():,} with an ATTOM "
          f"property, {n_dup:,} duplicate keys (must be 0)")
    links = stata_ints(links)
    links.to_stata(out, write_index=False, version=118, variable_labels=LINK_LABELS)
    print(f"Saved: {out}")


def build_values(con: duckdb.DuckDBPyConnection, states: list[str], link_files: list[Path],
                 geocoded_dir: Path, out: Path, measure: str, year_min: int, year_max: int,
                 min_value: float, max_value: float) -> None:
    # the geocoded panel is property x tax year; keep the assigned properties and
    # the panel years, deflate, and stack the states
    frames = []
    for state, link_file in zip(states, link_files):
        source = geocoded_dir / f"{state.lower()}_attom_geocoded.parquet"
        con.execute("DROP TABLE IF EXISTS ids")
        con.execute(f"""
            CREATE TEMP TABLE ids AS
            SELECT DISTINCT assigned_attomid AS attomid
            FROM read_parquet({q(link_file)}) WHERE assigned_attomid IS NOT NULL
        """)
        check = con.execute(f"""
            SELECT count(*) AS n_rows, count(DISTINCT (g.attomid, g.year)) AS n_keys,
                   sum((nominal IS NULL OR nominal <= 0)::int) missing_or_zero,
                   sum((nominal > 0 AND nominal < {min_value})::int) below_min,
                   sum((nominal > {max_value})::int) above_max
            FROM (SELECT g.attomid, g.year, cast(g.{measure} AS double) nominal
                  FROM read_parquet({q(source)}) g JOIN ids USING (attomid)
                  WHERE g.year BETWEEN {year_min} AND {year_max}) g
        """).fetchone()
        values = con.execute(f"""
            SELECT cast(g.attomid AS bigint) AS assigned_attomid,
                   cast(g.year AS integer) AS policy_year,
                   cast(cast(g.{measure} AS double) / c.cpi AS float) AS attom_value_2023
            FROM read_parquet({q(source)}) g
            JOIN ids USING (attomid)
            JOIN cpi c ON c.year = g.year
            WHERE g.year BETWEEN {year_min} AND {year_max}
              AND cast(g.{measure} AS double) BETWEEN {min_value} AND {max_value}
        """).df()
        print(f"{state}: {check[0]:,} assigned property-years {year_min}-{year_max} "
              f"({check[1]:,} distinct keys); dropped {check[2]:,} missing or zero, "
              f"{check[3]:,} below ${min_value:,.0f}, {check[4]:,} above ${max_value:,.0f}; "
              f"{len(values):,} kept")
        values["assigned_attomid"] = values["assigned_attomid"].astype("int32")
        values["policy_year"] = values["policy_year"].astype("int16")
        frames.append(values)

    values = pd.concat(frames, ignore_index=True)
    del frames
    values.sort_values(["assigned_attomid", "policy_year"], inplace=True, ignore_index=True)
    n_dup = values.duplicated(["assigned_attomid", "policy_year"]).sum()
    print(f"values: {len(values):,} rows, {values['assigned_attomid'].nunique():,} ATTOM properties, "
          f"{n_dup:,} duplicate keys (must be 0)")
    values.to_stata(out, write_index=False, version=118, variable_labels={
        "assigned_attomid": "ATTOM property assigned by the matcher",
        "policy_year": "Tax year of the ATTOM value (merged on policy year)",
        "attom_value_2023": f"ATTOM {measure.replace('_', ' ')} (2023 $)",
    })
    print(f"Saved: {out}")


def main() -> None:
    args = parse_args()
    data = Path(args.data)
    states = [s.strip().upper() for s in args.states.replace(",", " ").split() if s.strip()]
    pipeline = data / "build" / "nfip_attom_pipeline_v2"
    link_files = [pipeline / "nfip_attom_property" / f"{s.lower()}_nfip_attom_property.parquet" for s in states]
    tmp = Path(args.tmp) if args.tmp else data / "build" / "attom_stata_tmp"
    tmp.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit={q(args.memory)}")
    con.execute(f"SET threads={args.threads}")
    con.execute(f"SET temp_directory={q(tmp)}")
    con.execute("SET preserve_insertion_order=false")

    # CPI, base 2023, from the cleaner
    cpi = pd.read_stata(data / "clean" / "cpi.dta")[["year", "cpi"]]
    con.register("cpi_frame", cpi)
    con.execute('CREATE TABLE cpi AS SELECT cast("year" AS integer) AS "year", cast(cpi AS double) AS cpi FROM cpi_frame')

    build_links(con, link_files, data / "build" / "attom_links.dta")
    build_values(con, states, link_files, pipeline / "geocoded", data / "build" / "attom_value.dta",
                 args.measure, args.year_min, args.year_max, args.min_value, args.max_value)
    con.close()


if __name__ == "__main__":
    main()
