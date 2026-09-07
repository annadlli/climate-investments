"""
Author: Anna Li 
Date: 2026-09-06

ATTOM property value by tax year, one row per property, wide.

Reads the geocoded ATTOM panel (property x tax year) for each state and writes
one row per ATTOMID with the chosen value measure in every year, deflated to
2023 dollars

Cleaning: values <= 0 are missing (ATTOM logs 0 where it holds no value)
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


def q(value: object) -> str:
    """Quote a value for use as a DuckDB SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    """Parse command-line options for the state-level build."""
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", required=True, help="Data root (from master.do).")
    p.add_argument("--states", required=True,
                   help="2-letter abbreviations, space- or comma-separated (master.do's `states`).")
    p.add_argument("--measure", default="market_value_total",
                   choices=["market_value_total", "assessed_value_total",
                            "market_value_improvements", "assessed_value_improvements"])
    p.add_argument("--year-min", type=int, default=2000)
    p.add_argument("--year-max", type=int, default=2026)
    p.add_argument("--max-value", type=float, default=1e8,
                   help="Values above this (nominal $) are set to missing.")
    p.add_argument("--geocoded-dir", default=None,
                   help="Folder of {st}_attom_geocoded.parquet (default: build/nfip_attom_pipeline_v2/geocoded)")
    p.add_argument("--out-dir", default=None, help="Default: build/attom_value_wide")
    p.add_argument("--memory", default="16GB")
    p.add_argument("--threads", type=int, default=4)
    return p.parse_args()


def build_state(con: duckdb.DuckDBPyConnection, state: str, source: Path, out_dir: Path,
                measure: str, year_min: int, year_max: int, max_value: float) -> None:
    """Build and export the wide ATTOM value panel for one state."""
    years = list(range(year_min, year_max + 1))

    # Read the state panel, keep the requested years, and deflate usable values to 2023.
    con.execute("DROP TABLE IF EXISTS long")
    con.execute(f"""
        CREATE TABLE long AS
        WITH source AS (
            SELECT cast(attomid AS varchar) AS attomid,
                   cast(year AS integer) AS year,
                   cast({measure} AS double) AS nominal
            FROM read_parquet({q(source)})
        )
        SELECT attomid, year, nominal,
               CASE WHEN nominal > 0 AND nominal <= {max_value}
                    THEN nominal / c.cpi END AS real_value
        FROM source
        LEFT JOIN cpi c USING (year)
        WHERE year BETWEEN {year_min} AND {year_max}
          AND attomid IS NOT NULL
    """)

    # Summarize coverage and value quality before reshaping the data.
    diag = con.execute("""
        SELECT count(*) property_years,
               count(DISTINCT attomid) properties,
               sum((nominal IS NULL)::int) missing,
               sum((nominal = 0)::int) zero,
               sum((nominal < 0)::int) negative,
               sum((nominal > ?)::int) above_max,
               quantile_cont(real_value, 0.50) p50_real,
               quantile_cont(real_value, 0.99) p99_real,
               max(real_value) max_real
        FROM long
    """, [max_value]).df()
    by_year = con.execute("""
        SELECT year, count(*) property_years, sum((real_value IS NOT NULL)::int) with_value,
               quantile_cont(real_value, 0.5) p50_real
        FROM long GROUP BY year ORDER BY year
    """).df()

    # Pivot the cleaned values so each property has one column per year.
    value_cols = ", ".join(
        f"max(CASE WHEN year = {y} THEN real_value END) AS value_{y}" for y in years)
    con.execute("DROP TABLE IF EXISTS wide")
    con.execute(f"""
        CREATE TABLE wide AS
        SELECT attomid, {q(state)} AS state,
               count(real_value) AS n_value_years,
               min(CASE WHEN real_value IS NOT NULL THEN year END) AS value_year_first,
               max(CASE WHEN real_value IS NOT NULL THEN year END) AS value_year_last,
               {value_cols}
        FROM long GROUP BY attomid
    """)
    # Write the wide panel and diagnostic tables.
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_out = out_dir / f"{state.lower()}_attom_value_wide.parquet"
    con.execute(f"COPY (SELECT * FROM wide ORDER BY attomid) TO {q(parquet_out)} (FORMAT PARQUET, COMPRESSION ZSTD)")
    n = con.execute("SELECT count(*), sum((n_value_years > 0)::int) FROM wide").fetchone()
    print(f"{state}: {n[0]:,} properties, {n[1]:,} with at least one value; saved {parquet_out.name}")
    print(diag.to_string(index=False))
    diag.insert(0, "state", state)
    diag.to_csv(out_dir / f"{state.lower()}_attom_value_wide_diagnostics.csv", index=False)
    by_year.to_csv(out_dir / f"{state.lower()}_attom_value_wide_by_year.csv", index=False)


def main() -> None:
    """Configure DuckDB, load CPI data, and process each requested state."""
    args = parse_args()

    # Resolve input and output locations from the command-line options.
    data = Path(args.data)
    states = [s.strip().upper() for s in args.states.replace(",", " ").split() if s.strip()]
    geocoded_dir = Path(args.geocoded_dir) if args.geocoded_dir else data / "build" / "nfip_attom_pipeline_v2" / "geocoded"
    out_dir = Path(args.out_dir) if args.out_dir else data / "build" / "attom_value_wide"

    # Configure the DuckDB session for the panel build.
    con = duckdb.connect()
    con.execute(f"SET memory_limit={q(args.memory)}")
    con.execute(f"SET threads={args.threads}")
    con.execute(f"SET temp_directory={q(out_dir / 'duckdb_tmp')}")
    con.execute("SET preserve_insertion_order=false")

    # Register the CPI series used to convert nominal values to 2023 dollars.
    cpi = pd.read_stata(data / "clean" / "cpi.dta")[["year", "cpi"]]
    cpi["year"] = cpi["year"].astype(int)
    con.register("cpi_frame", cpi)
    con.execute("CREATE TABLE cpi AS SELECT cast(year AS integer) AS year, cast(cpi AS double) AS cpi FROM cpi_frame")

    # Build every requested state; a missing file now fails at the data read.
    for state in states:
        source = geocoded_dir / f"{state.lower()}_attom_geocoded.parquet"
        build_state(con, state, source, out_dir, args.measure, args.year_min, args.year_max,
                    args.max_value)
    # Release the DuckDB connection after all exports finish.
    con.close()


if __name__ == "__main__":
    # Run the command-line entry point when invoked as a script.
    main()
