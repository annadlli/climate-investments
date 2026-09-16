"""
Authors: Anna Li and Vendela Norman
Date: 2026-09-15

ATTOM market value in 2023 dollars for every NFIP-matched ATTOM property and
every year from --year-min to --year-max, for the merge onto the NFIP panel in
complete.do. value_own is the property's value in that tax year; value carries
the most recent earlier value into years without one, else the earliest later
one, and value_year says which year it came from.

Values <= 0 are missing (ATTOM logs 0 where it holds no value), as are values
below --min-value (placeholder assessments of a few hundred dollars) and above
--max-value.
"""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path

import duckdb
import pandas as pd


def q(value: object) -> str:
    """Quote a value for use as a DuckDB SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", required=True, help="Data root (from master.do).")
    p.add_argument("--states", required=True,
                   help="2-letter abbreviations, space- or comma-separated (master.do's `states`).")
    p.add_argument("--measure", default="market_value_total",
                   choices=["market_value_total", "assessed_value_total",
                            "market_value_improvements", "assessed_value_improvements"])
    p.add_argument("--year-min", type=int, default=2000)
    p.add_argument("--year-max", type=int, default=2026)
    p.add_argument("--min-value", type=float, default=1e4,
                   help="Values below this (nominal $) are set to missing.")
    p.add_argument("--max-value", type=float, default=1e8,
                   help="Values above this (nominal $) are set to missing.")
    p.add_argument("--geocoded-dir", default=None,
                   help="Folder of {st}_attom_geocoded.parquet (default: build/nfip_attom_pipeline_v2/geocoded)")
    p.add_argument("--links-dir", default=None,
                   help="Folder of {st}_nfip_attom_property.dta, the link files complete.do reads (default: build/nfip_attom_property)")
    p.add_argument("--out-dir", default=None, help="Default: build/attom_value")
    p.add_argument("--memory", default="16GB")
    p.add_argument("--threads", type=int, default=4)
    return p.parse_args()


def build_state(con: duckdb.DuckDBPyConnection, state: str, source: Path, links: Path,
                out_dir: Path, measure: str, year_min: int, year_max: int, min_value: float,
                max_value: float) -> None:
    """Deflate one state's values and fill every year for the NFIP-matched properties."""
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
               CASE WHEN nominal >= {min_value} AND nominal <= {max_value}
                    THEN nominal / c.cpi END AS value
        FROM source
        LEFT JOIN cpi c USING (year)
        WHERE year BETWEEN {year_min} AND {year_max}
          AND attomid IS NOT NULL
    """)

    # Coverage and value quality, to the log only
    diag = con.execute("""
        SELECT count(*) property_years,
               count(DISTINCT attomid) properties,
               sum((nominal IS NULL)::int) missing,
               sum((nominal = 0)::int) zero,
               sum((nominal < 0)::int) negative,
               sum((nominal > 0 AND nominal < ?)::int) below_min,
               sum((nominal > ?)::int) above_max,
               quantile_cont(value, 0.50) p50_real,
               quantile_cont(value, 0.99) p99_real,
               max(value) max_real
        FROM long
    """, [min_value, max_value]).df()
    by_year = con.execute("""
        SELECT year, count(*) property_years, sum((value IS NOT NULL)::int) with_value,
               quantile_cont(value, 0.5) p50_real
        FROM long GROUP BY year ORDER BY year
    """).df()

    # Matched properties x every year; carry the most recent value forward, else the earliest back
    ids = pd.read_stata(links, columns=["assigned_attomid"]).rename(columns={"assigned_attomid": "attomid"})
    ids = ids[ids["attomid"] != ""].drop_duplicates()
    con.register("ids", ids)
    con.execute("DROP TABLE IF EXISTS filled")
    con.execute(f"""
        CREATE TABLE filled AS
        WITH obs AS (
            SELECT attomid, year, value FROM long
            WHERE value IS NOT NULL AND attomid IN (SELECT attomid FROM ids)
        ),
        grid AS (
            SELECT i.attomid, t.year
            FROM (SELECT DISTINCT attomid FROM obs) i
            CROSS JOIN (SELECT unnest(range({year_min}, {year_max + 1})) AS year) t
        ),
        joined AS (
            SELECT g.attomid, g.year, o.value AS own_value,
                   CASE WHEN o.value IS NOT NULL THEN g.year END AS own_year
            FROM grid g LEFT JOIN obs o USING (attomid, year)
        )
        SELECT attomid, year, own_value AS value_own,
               coalesce(last_value(own_value IGNORE NULLS) OVER back,
                        first_value(own_value IGNORE NULLS) OVER fwd) AS value,
               coalesce(last_value(own_year IGNORE NULLS) OVER back,
                        first_value(own_year IGNORE NULLS) OVER fwd) AS value_year
        FROM joined
        WINDOW back AS (PARTITION BY attomid ORDER BY year ROWS UNBOUNDED PRECEDING),
               fwd  AS (PARTITION BY attomid ORDER BY year ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
    """)
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_out = out_dir / f"{state.lower()}_attom_value.parquet"
    con.execute(f"COPY (SELECT * FROM filled ORDER BY attomid, year) TO {q(parquet_out)} (FORMAT PARQUET, COMPRESSION ZSTD)")
    n = con.execute("""
        SELECT count(*), count(DISTINCT attomid), sum((value_year = year)::int),
               quantile_cont(abs(year - value_year), 0.5), max(abs(year - value_year))
        FROM filled
    """).fetchone()
    print(f"{state}: {n[1]:,} matched properties x {year_max - year_min + 1} years = {n[0]:,} rows; "
          f"{n[2]:,} own-year values, carried distance median {n[3]:.0f} max {n[4]}; saved {parquet_out.name}")
    print(diag.to_string(index=False))
    print(by_year.to_string(index=False))


def main() -> None:
    args = parse_args()
    data = Path(args.data)
    states = [s.strip().upper() for s in args.states.replace(",", " ").split() if s.strip()]
    geocoded_dir = Path(args.geocoded_dir) if args.geocoded_dir else data / "build" / "nfip_attom_pipeline_v2" / "geocoded"
    links_dir = Path(args.links_dir) if args.links_dir else data / "build" / "nfip_attom_property"
    out_dir = Path(args.out_dir) if args.out_dir else data / "build" / "attom_value"

    con = duckdb.connect()
    con.execute(f"SET memory_limit={q(args.memory)}")
    con.execute(f"SET threads={args.threads}")
    con.execute(f"SET temp_directory={q(Path(tempfile.gettempdir()) / 'attom_value')}")
    con.execute("SET preserve_insertion_order=false")

    # CPI series, base 2023, from clean_cpi.do
    cpi = pd.read_stata(data / "clean" / "cpi.dta")[["year", "cpi"]]
    cpi["year"] = cpi["year"].astype(int)
    con.register("cpi_frame", cpi)
    con.execute("CREATE TABLE cpi AS SELECT cast(year AS integer) AS year, cast(cpi AS double) AS cpi FROM cpi_frame")

    for state in states:
        build_state(con, state, geocoded_dir / f"{state.lower()}_attom_geocoded.parquet",
                    links_dir / f"{state.lower()}_nfip_attom_property.dta", out_dir,
                    args.measure, args.year_min, args.year_max, args.min_value, args.max_value)
    con.close()


if __name__ == "__main__":
    main()
