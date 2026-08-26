"""Apply successful retry geocodes to a separate ATTOM property-year copy."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def quote(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--geocoded", required=True)
    parser.add_argument("--fills", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--diagnostics", required=True)
    parser.add_argument("--tmp", required=True)
    parser.add_argument("--memory", default="80GB")
    args = parser.parse_args()

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.diagnostics).parent.mkdir(parents=True, exist_ok=True)
    Path(args.tmp).mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit={quote(args.memory)}")
    con.execute(f"SET temp_directory={quote(args.tmp)}")
    con.execute("SET preserve_insertion_order=false")

    # Fills contains only successful retry results and one row per ATTOMID.
    con.execute(f"""
        CREATE TEMP TABLE fills AS
        SELECT cast(attomid AS varchar) attomid,
               max(nullif(trim(cast(censusblockgroupfips AS varchar)), ''))
                   censusblockgroupfips,
               max(cast(longitude AS double)) longitude,
               max(cast(latitude AS double)) latitude
        FROM read_parquet({quote(args.fills)})
        WHERE longitude IS NOT NULL AND latitude IS NOT NULL
        GROUP BY 1
    """)

    before = con.execute(f"""
        SELECT count(DISTINCT cast(attomid AS varchar))
        FROM read_parquet({quote(args.geocoded)})
        WHERE longitude IS NOT NULL AND latitude IS NOT NULL
    """).fetchone()[0]
    recovered = con.execute("SELECT count(*) FROM fills").fetchone()[0]

    con.execute(f"""
        COPY (
            SELECT g.* EXCLUDE(censusblockgroupfips, geocode_match,
                               longitude, latitude),
                   coalesce(nullif(trim(cast(g.censusblockgroupfips AS varchar)), ''),
                            f.censusblockgroupfips, '') censusblockgroupfips,
                   CASE WHEN g.longitude IS NOT NULL AND g.latitude IS NOT NULL
                        THEN g.geocode_match
                        WHEN f.attomid IS NOT NULL THEN 'Match'
                        ELSE g.geocode_match END geocode_match,
                   coalesce(g.longitude, f.longitude) longitude,
                   coalesce(g.latitude, f.latitude) latitude
            FROM read_parquet({quote(args.geocoded)}) g
            LEFT JOIN fills f
              ON cast(g.attomid AS varchar)=f.attomid
        ) TO {quote(args.out)} (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    after = con.execute(f"""
        SELECT count(DISTINCT cast(attomid AS varchar))
        FROM read_parquet({quote(args.out)})
        WHERE longitude IS NOT NULL AND latitude IS NOT NULL
    """).fetchone()[0]
    total = con.execute(f"""
        SELECT count(DISTINCT cast(attomid AS varchar))
        FROM read_parquet({quote(args.out)})
    """).fetchone()[0]
    con.execute(f"""
        COPY (
            SELECT {total} attom_properties,
                   {before} geocoded_before,
                   {recovered} retry_fill_attomids,
                   {after} geocoded_after,
                   {after - before} net_recovered,
                   round(100.0*{before}/{total}, 2) geocoded_before_percent,
                   round(100.0*{after}/{total}, 2) geocoded_after_percent
        ) TO {quote(args.diagnostics)} (HEADER, DELIMITER ',')
    """)
    con.close()
    print(f"Saved {args.out}")
    print(f"Saved {args.diagnostics}")


if __name__ == "__main__":
    main()
