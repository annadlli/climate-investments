"""
Author: Anna Li
Date: 2026-09-07

Description: Adds a housing-stock benchmark to the Builty county-year coverage
    file: permits per 100 ATTOM single-family homes, and a strict coverage flag
    for county-years at or above --floor (default 1 per 100). The loose flag
    from clean_builty_coverage.py (any permit) stays as is.
"""

import argparse
from pathlib import Path

import duckdb
import pandas as pd

# the six single-family use codes the matcher uses, so both sides count the same homes
SF_USE_CODES = ("376", "380", "382", "383", "385", "386")


def main():
    p = argparse.ArgumentParser(description="Builty permits per 100 ATTOM single-family homes.")
    p.add_argument("--data", required=True, help="Data root (from master.do).")
    p.add_argument("--states", required=True, help="2-letter abbreviations (master.do's `states`).")
    p.add_argument("--floor", type=float, default=1.0, help="Permits per 100 homes to count as covered.")
    args = p.parse_args()

    states = [s.strip().upper() for s in args.states.replace(",", " ").split() if s.strip()]
    data = Path(args.data)
    geocoded = data / "build" / "nfip_attom_pipeline_v2" / "geocoded"
    codes = ", ".join(f"'{c}'" for c in SF_USE_CODES)

    # single-family homes per county, counted once each from the property x year panel
    con = duckdb.connect()
    frames = []
    for st in states:
        source = geocoded / f"{st.lower()}_attom_geocoded.parquet"
        if not source.exists():
            print(f"{st}: missing {source}, skipped")
            continue
        frames.append(con.execute(f"""
            SELECT '{st}' AS state, countycode, count(DISTINCT attomid) AS attom_n_sf
            FROM read_parquet('{source}')
            WHERE trim(cast(property_use_std AS varchar)) IN ({codes}) AND countycode IS NOT NULL
            GROUP BY 1, 2
        """).df())
    attom = pd.concat(frames, ignore_index=True)

    # rate and flag onto the county-year file; drop the columns first so reruns are clean
    path = data / "clean" / "builty_coverage_county.dta"
    county = pd.read_stata(path, convert_categoricals=False)
    county = county.drop(columns=[c for c in ("attom_n_sf", "builty_per_100", "builty_covered_strict") if c in county])
    county = county.merge(attom, on=["state", "countycode"], how="left")
    county["builty_per_100"] = 100 * county["builty_n_permits"] / county["attom_n_sf"]
    county["builty_covered_strict"] = (county["builty_per_100"] >= args.floor).astype("int8")

    # labels for every column, so the file stays labeled after the rewrite
    labels = {
        "state": "State", "countycode": "County FIPS", "year": "Permit year",
        "builty_n_permits": "Builty permits (all types)",
        "builty_n_localities": "Builty localities with permits",
        "builty_share_peak": "Permits as a share of the county's peak year",
        "builty_covered": "County-year has a Builty permit feed (>= 1 permit)",
        "attom_n_sf": "ATTOM single-family homes in county",
        "builty_per_100": "Builty permits per 100 ATTOM single-family homes",
        "builty_covered_strict": f"County-year covered: >= {args.floor:g} permits per 100 homes",
    }
    county.to_stata(path, write_index=False, variable_labels={k: v for k, v in labels.items() if k in county})

    # quick read on where the floor hits
    done = county[county.state.isin(states)]
    print(f"floor {args.floor:g} per 100: {int(done.builty_covered_strict.sum()):,} of {len(done):,} "
          f"county-years covered ({int(done.builty_covered.sum()):,} under the loose rule)")
    print(done[done.year.between(2010, 2024)].groupby("state")["builty_per_100"]
          .describe(percentiles=[.25, .5, .75]).round(1).to_string())


if __name__ == "__main__":
    main()
