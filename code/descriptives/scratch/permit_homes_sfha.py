"""
Author: Anna Li
Date: 2026-10-02

Claude change 10-02: new scratch helper, run by hand before elevations_by_state.do. Counts
the retrofit permit homes that reached an ATTOM property record (step 3 of the matching)
and how many of them sit in a Special Flood Hazard Area per the NFHL join, by state, so
the state table can show the permit count on the SFHA footprint without a parquet reader
in Stata.

Reads   build/nfip_attom_pipeline_v2/attom_permits/{st}_attom_permits.parquet
Writes  ../output/tables/permit_homes_sfha.csv

Usage: python permit_homes_sfha.py --data <data root> --output <output folder> --states "FL LA TX"
"""
import argparse
from pathlib import Path

import duckdb
import pandas as pd


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--states", default="FL LA TX")
    a = p.parse_args()
    con = duckdb.connect()
    rows = []
    for st in a.states.replace(",", " ").split():
        f = Path(a.data) / "build" / "nfip_attom_pipeline_v2" / "attom_permits" / f"{st.lower()}_attom_permits.parquet"
        rows.append(con.execute(f"""
            SELECT '{st}' AS state_abbrev,
                   count(*) AS permit_homes_linked,
                   count(*) FILTER (WHERE nfhl_flood_matched) AS permit_homes_zone_known,
                   count(*) FILTER (WHERE upper(trim(cast(sfha_tf AS varchar))) = 'T') AS permit_homes_sfha,
                   count(*) FILTER (WHERE regexp_matches(upper(trim(cast(fld_zone AS varchar))), '^(A|V)')) AS permit_homes_zone_av
            FROM read_parquet('{f}')
            WHERE permit_elevated = 1 AND coalesce(permit_retrofit, 1) = 1
        """).df())
    out = pd.concat(rows, ignore_index=True)
    out.to_csv(Path(a.output) / "tables" / "permit_homes_sfha.csv", index=False)
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
