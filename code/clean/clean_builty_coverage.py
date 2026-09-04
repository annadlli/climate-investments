"""
Author: Vendela Norman
Date: 2026-09-03

Description: Builds a Builty permit-coverage index from the raw permits parquet:
    permit counts (all permit types) by county x year and by ZIP x year, with a
    flag for county-years that report any permits (loose threshold; see TODO.md
    for the housing-stock benchmark and municipal matching). NFIP properties outside
    covered county-years cannot show a Builty elevation, so the flag restricts the
    analysis sample rather than treating them as not elevated.

Notes: County FIPS is near-complete except New York City, whose feed carries no
    county; boroughs are assigned from the ZIP prefix there. The three date fields
    are coalesced (issued, submitted, finaled) because some states populate only one.
    ZIP is missing for 13-89% of permits by state, so the ZIP index undercounts.

"""

import argparse
from pathlib import Path

import duckdb

MIN_PERMITS = 1   # county-year permit floor to count as covered (loose: any permit at all)
YEAR_MIN, YEAR_MAX = 1990, 2026

# NYC borough county FIPS from ZIP prefix
NYC_ZIP_TO_COUNTY = """
    CASE WHEN zip3 IN ('100','101','102') THEN '36061'
         WHEN zip3 = '104' THEN '36005'
         WHEN zip3 = '112' THEN '36047'
         WHEN zip3 IN ('110','111','113','114','115','116') THEN '36081'
         WHEN zip3 = '103' THEN '36085' END
"""


def main():
    p = argparse.ArgumentParser(description="Builty permit coverage by county-year and ZIP-year.")
    p.add_argument("--data", required=True, help="Data root with raw/ and clean/ (from master.do).")
    p.add_argument("--states", required=True, help="2-letter abbreviations (passed from master.do's `states`).")
    args = p.parse_args()

    states = [s.strip().upper() for s in args.states.replace(",", " ").split() if s.strip()]
    data = Path(args.data)
    src = data / "raw" / "builty_all.parquet"
    inlist = ", ".join(f"'{s}'" for s in states)

    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE permits AS
        SELECT STATE AS state,
               LOCALITY AS locality,
               lpad(nullif(trim(ZIPCODE), ''), 5, '0') AS zipcode,
               substr(lpad(nullif(trim(ZIPCODE), ''), 5, '0'), 1, 3) AS zip3,
               try_cast(substr(coalesce(nullif(DATE_ISSUED, ''), nullif(DATE_SUBMITTED, ''),
                                        nullif(DATE_FINALED, '')), 1, 4) AS INTEGER) AS year,
               CASE WHEN nullif(trim(FIPS_COUNTY), '') IS NOT NULL
                    THEN lpad(trim(FIPS_STATE), 2, '0') || lpad(trim(FIPS_COUNTY), 3, '0') END AS countycode
        FROM read_parquet('{src}')
        WHERE STATE IN ({inlist})
    """)
    con.execute(f"""
        UPDATE permits SET countycode = ({NYC_ZIP_TO_COUNTY})
        WHERE countycode IS NULL AND state = 'NY' AND zip3 IS NOT NULL
    """)
    con.execute(f"DELETE FROM permits WHERE year IS NULL OR year < {YEAR_MIN} OR year > {YEAR_MAX}")

    county = con.execute(f"""
        WITH c AS (
            SELECT state, countycode, year,
                   count(*) AS builty_n_permits,
                   count(DISTINCT locality) AS builty_n_localities
            FROM permits WHERE countycode IS NOT NULL
            GROUP BY 1, 2, 3
        )
        SELECT *,
               builty_n_permits / max(builty_n_permits) OVER (PARTITION BY countycode) AS builty_share_peak,
               CAST(builty_n_permits >= {MIN_PERMITS} AS INTEGER) AS builty_covered
        FROM c ORDER BY 1, 2, 3
    """).fetchdf()
    zipc = con.execute("""
        SELECT state, zipcode, year, count(*) AS builty_n_permits
        FROM permits WHERE zipcode IS NOT NULL
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
    """).fetchdf()

    for frame in (county, zipc):
        frame["year"] = frame["year"].astype("int16")
        frame["builty_n_permits"] = frame["builty_n_permits"].astype("int32")
    county["builty_n_localities"] = county["builty_n_localities"].astype("int16")
    county["builty_covered"] = county["builty_covered"].astype("int8")

    labels = {
        "state": "State", "countycode": "County FIPS", "zipcode": "ZIP code", "year": "Permit year",
        "builty_n_permits": "Builty permits (all types)",
        "builty_n_localities": "Builty localities with permits",
        "builty_covered": f"County-year has a Builty permit feed (>= {MIN_PERMITS} permits)",
        "builty_share_peak": "Permits as a share of the county's peak year",
    }
    county.to_stata(data / "clean" / "builty_coverage_county.dta", write_index=False,
                    variable_labels={k: v for k, v in labels.items() if k in county})
    zipc.to_stata(data / "clean" / "builty_coverage_zip.dta", write_index=False,
                  variable_labels={k: v for k, v in labels.items() if k in zipc})

    covered = county[county.builty_covered == 1]
    print(f"county-years: {len(county):,} ({len(covered):,} covered); "
          f"counties covered in any year: {covered.countycode.nunique():,}; "
          f"zip-years: {len(zipc):,}")
    print(covered.groupby("state").agg(counties=("countycode", "nunique"),
                                       yr_min=("year", "min"), yr_max=("year", "max")).to_string())


if __name__ == "__main__":
    main()
