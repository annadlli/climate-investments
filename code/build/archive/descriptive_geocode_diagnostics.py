"""
Diagnostics for geocoded ATTOM block groups.

Authors: Anna Li
Date: 2026-07-13

Description:
    Post-run checks on the geocode pipeline output for one state. Prints:
      1. address-level match tabulation
      2. unmatched share by ZIP (worst offenders, min 200 addresses)
      3. a random sample of unmatched addresses
      4. properties-per-address for matched vs unmatched addresses
         (explains any gap between address- and property-level match rates)
    and writes {state}_geocoded_blockgroup_counts.csv (distinct block group x
    address count) - a small file to compare against NFIP's
    censusBlockGroupFips universe for the boundary-vintage check.

    Run where the geocode work dir lives (Torch):
        python descriptive_geocode_diagnostics.py --data <root> --state va
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def main() -> None:
    p = argparse.ArgumentParser(description="Diagnostics for geocoded ATTOM block groups.")
    p.add_argument("--state", required=True)
    p.add_argument("--data",  required=True)
    args  = p.parse_args()
    state = args.state.lower()
    data  = Path(args.data)
    base  = data / "build" / "attom_geocode" / f"{state}_addr"

    addr  = quote(str(base / "addresses.parquet"))
    res   = quote(str(base / "blockgroups_by_address.parquet"))
    xwalk = quote(str(base / "attomid_xwalk.parquet"))

    con = duckdb.connect()

    print(f"=== {state.upper()}: address-level match tabulation ===")
    print(con.execute(f"""
        SELECT match, count(*) AS n_addresses,
               round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
        FROM read_parquet({res})
        GROUP BY match ORDER BY n_addresses DESC
    """).df().to_string(index=False))

    print(f"\n=== {state.upper()}: unmatched share by ZIP (worst 25, min 200 addresses) ===")
    print(con.execute(f"""
        SELECT a.zip, a.city AS example_city,
               count(*) AS n_addresses,
               sum((r.match != 'Match')::int) AS n_unmatched,
               round(100.0 * sum((r.match != 'Match')::int) / count(*), 1) AS pct_unmatched
        FROM read_parquet({addr}) a
        JOIN read_parquet({res}) r USING (addrid)
        GROUP BY a.zip, a.city
        HAVING count(*) >= 200
        ORDER BY pct_unmatched DESC
        LIMIT 25
    """).df().to_string(index=False))

    print(f"\n=== {state.upper()}: 30 random unmatched addresses ===")
    print(con.execute(f"""
        SELECT a.street, a.city, a.zip
        FROM read_parquet({addr}) a
        JOIN read_parquet({res}) r USING (addrid)
        WHERE r.match = 'No_Match'
        USING SAMPLE 30
    """).df().to_string(index=False))

    print(f"\n=== {state.upper()}: properties per address, matched vs unmatched ===")
    print(con.execute(f"""
        WITH per_addr AS (
            SELECT addrid, count(*) AS n_props
            FROM read_parquet({xwalk})
            GROUP BY addrid
        )
        SELECT r.match,
               count(*)                    AS n_addresses,
               sum(p.n_props)              AS n_properties,
               round(avg(p.n_props), 2)    AS mean_props_per_addr,
               max(p.n_props)              AS max_props_per_addr
        FROM read_parquet({res}) r
        JOIN per_addr p USING (addrid)
        GROUP BY r.match ORDER BY n_addresses DESC
    """).df().to_string(index=False))

    print(f"\n=== {state.upper()}: unmatched addresses by (fixable?) cause ===")
    print(con.execute(f"""
        SELECT
            CASE
                WHEN NOT regexp_matches(a.street, '^\\d')
                    THEN '1 no house number (unfixable)'
                WHEN a.zip IN ('', '00000') AND (a.city = '' OR upper(a.city) = upper(a.state))
                    THEN '2 no usable city or zip (unfixable)'
                WHEN a.city = '' OR upper(a.city) = upper(a.state)
                    THEN '3 junk/blank city, zip ok (retryable)'
                WHEN a.zip IN ('', '00000')
                    THEN '4 missing zip, city ok (retryable)'
                ELSE '5 looks clean (typos / not in TIGER)'
            END AS cause,
            count(*) AS n_addresses,
            round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct_of_unmatched
        FROM read_parquet({addr}) a
        JOIN read_parquet({res}) r USING (addrid)
        WHERE r.match = 'No_Match'
        GROUP BY cause ORDER BY cause
    """).df().to_string(index=False))

    out = data / "build" / f"{state}_geocoded_blockgroup_counts.csv"
    con.execute(f"""
        COPY (
            SELECT censusblockgroupfips, count(*) AS n_addresses
            FROM read_parquet({res})
            WHERE censusblockgroupfips != ''
            GROUP BY censusblockgroupfips
            ORDER BY censusblockgroupfips
        ) TO {quote(str(out))} (HEADER)
    """)
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
