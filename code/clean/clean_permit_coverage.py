"""
Authors: Vendela Norman and Anna Li
Date: 2026-10-02

Description: ATTOM permit coverage from the raw permits parquet: all-permit counts by
    county x year, ZIP x year and site-address city x year. The county file carries
    two flags: permit_covered (any permit in the county-year) and permit_covered_strict
    (at least --floor permits per 100 ATTOM single-family homes).
Source: raw/attom_permits/attom_permits_*.parquet (prepare/import_attom_permits.py)

Notes: The permit year is the issue date (EFFECTIVEDATE); 1900-01-01 and 1970-01-01 are
    ATTOM's placeholders for an unknown date and are dropped. ZIP is present for nearly
    every permit, so the ZIP index is close to complete, unlike Builty's.

Claude change 10-02: was clean_builty_coverage.py (archived), rewritten for the ATTOM
    permit file; variables renamed builty_* -> permit_*.
Claude change 10-04: reads every combined file in the folder; --states ALL; a state without a
    geocoded ATTOM panel gets missing attom_n_sf, permit_per_100 and permit_covered_strict.
"""

import argparse
from pathlib import Path

import duckdb
import pandas as pd

MIN_PERMITS = 1   # county-year permit floor to count as covered (loose: any permit at all)
YEAR_MIN, YEAR_MAX = 1990, 2026
SF_USE_CODES = ("376", "380", "382", "383", "385", "386")   # the single-family use codes the matcher uses
PLACEHOLDER_DATES = "(DATE '1900-01-01', DATE '1970-01-01')"
# a permit whose county code lies outside its own state is a geocoding slip (a few hundred
# rows); its county is set missing so county-year stays unique across states
STATE_FIPS = {"AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08", "CT": "09", "DE": "10",
              "DC": "11", "FL": "12", "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18", "IA": "19",
              "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
              "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34", "NM": "35",
              "NY": "36", "NC": "37", "ND": "38", "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
              "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
              "WV": "54", "WI": "55", "WY": "56"}


def main():
    p = argparse.ArgumentParser(description="ATTOM permit coverage by county-year, ZIP-year and city-year.")
    p.add_argument("--data", required=True, help="Data root with raw/ and clean/ (from master.do).")
    p.add_argument("--states", required=True, help="2-letter abbreviations (passed from master.do's `states`), or ALL.")
    p.add_argument("--raw", default=None, help="Permits parquet or glob (default: raw/attom_permits/attom_permits_*.parquet).")
    p.add_argument("--floor", type=float, default=1.0, help="Permits per 100 homes for permit_covered_strict.")
    p.add_argument("--memory", default="6GB")
    args = p.parse_args()

    states = [s.strip().upper() for s in args.states.replace(",", " ").split() if s.strip()]
    data = Path(args.data)
    src = Path(args.raw) if args.raw else data / "raw" / "attom_permits" / "attom_permits_*.parquet"
    if states == ["ALL"]:
        states = list(STATE_FIPS)
    inlist = ", ".join(f"'{s}'" for s in states)
    fips_case = "CASE PROPERTYADDRESSSTATE " + " ".join(f"WHEN '{s}' THEN '{STATE_FIPS[s]}'" for s in states) + " END"

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{args.memory}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"""
        CREATE TABLE permits AS
        SELECT PROPERTYADDRESSSTATE AS state,
               upper(nullif(trim(PROPERTYADDRESSCITY), '')) AS locality,
               substr(nullif(trim(PROPERTYADDRESSZIP), ''), 1, 5) AS zipcode,
               year(EFFECTIVEDATE) AS year,
               CASE WHEN substr(trim(STATECOUNTYFIPS), 1, 2) = {fips_case} THEN nullif(trim(STATECOUNTYFIPS), '') END AS countycode,
               ATTOMID IS NOT NULL AS has_attomid,
               coalesce(DESCRIPTION, '') <> '' AS has_description
        FROM read_parquet('{src}')
        WHERE PROPERTYADDRESSSTATE IN ({inlist})
          AND EFFECTIVEDATE NOT IN {PLACEHOLDER_DATES}
          AND year(EFFECTIVEDATE) BETWEEN {YEAR_MIN} AND {YEAR_MAX}
    """)

    county = con.execute(f"""
        WITH c AS (
            SELECT state, countycode, year,
                   count(*) AS permit_n_permits,
                   count(DISTINCT locality) AS permit_n_localities,
                   avg(has_attomid::integer) AS permit_share_attomid,
                   avg(has_description::integer) AS permit_share_description
            FROM permits WHERE countycode IS NOT NULL
            GROUP BY 1, 2, 3
        )
        SELECT *,
               permit_n_permits / max(permit_n_permits) OVER (PARTITION BY countycode) AS permit_share_peak,
               CAST(permit_n_permits >= {MIN_PERMITS} AS INTEGER) AS permit_covered
        FROM c ORDER BY 1, 2, 3
    """).fetchdf()
    # ATTOM single-family homes per county, so coverage can be judged against the housing stock
    geocoded = data / "build" / "nfip_attom_pipeline_v2" / "geocoded"
    codes = ", ".join(f"'{c}'" for c in SF_USE_CODES)
    with_panel = [st for st in states if (geocoded / f"{st.lower()}_attom_geocoded.parquet").exists()]
    print(f"ATTOM property panel found for {len(with_panel)} of {len(states)} states: {' '.join(with_panel)}")
    attom = pd.concat([con.execute(f"""
        SELECT '{st}' AS state, countycode, count(DISTINCT attomid) AS attom_n_sf
        FROM read_parquet('{geocoded / f"{st.lower()}_attom_geocoded.parquet"}')
        WHERE trim(cast(property_use_std AS varchar)) IN ({codes}) AND countycode IS NOT NULL
        GROUP BY 1, 2
    """).df() for st in with_panel] + [pd.DataFrame({"state": pd.Series(dtype=str), "countycode": pd.Series(dtype=str),
                                                     "attom_n_sf": pd.Series(dtype=float)})], ignore_index=True)
    county = county.merge(attom, on=["state", "countycode"], how="left")
    county["permit_per_100"] = 100 * county["permit_n_permits"] / county["attom_n_sf"]
    county["permit_covered_strict"] = (county["permit_per_100"] >= args.floor).astype(float)
    county.loc[county["attom_n_sf"].isna(), "permit_covered_strict"] = float("nan")

    zipc = con.execute("""
        SELECT state, zipcode, year, count(*) AS permit_n_permits
        FROM permits WHERE zipcode IS NOT NULL
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
    """).fetchdf()
    # city x year: the county is the modal county among the city's permits; year_first/year_last are its span
    locality = con.execute("""
        WITH modal AS (
            SELECT state, locality, arg_max(countycode, n) AS countycode
            FROM (SELECT state, locality, countycode, count(*) AS n
                  FROM permits WHERE countycode IS NOT NULL AND locality IS NOT NULL GROUP BY 1, 2, 3)
            GROUP BY 1, 2
        ), ly AS (
            SELECT state, locality, year, count(*) AS permit_n_permits
            FROM permits WHERE locality IS NOT NULL GROUP BY 1, 2, 3
        )
        SELECT ly.state, ly.locality, ly.year, ly.permit_n_permits, modal.countycode,
               min(ly.year) OVER (PARTITION BY ly.state, ly.locality) AS year_first,
               max(ly.year) OVER (PARTITION BY ly.state, ly.locality) AS year_last
        FROM ly LEFT JOIN modal USING (state, locality)
        ORDER BY 1, 2, 3
    """).fetchdf()

    for frame in (county, zipc, locality):
        frame["year"] = frame["year"].astype("int16")
        frame["permit_n_permits"] = frame["permit_n_permits"].astype("int32")
    for column in ("year_first", "year_last"):
        locality[column] = locality[column].astype("int16")
    county["permit_n_localities"] = county["permit_n_localities"].astype("int16")
    county["permit_covered"] = county["permit_covered"].astype("int8")

    labels = {
        "state": "State", "countycode": "County FIPS", "zipcode": "ZIP code", "year": "Permit year (issue date)",
        "permit_n_permits": "ATTOM permits (all types)",
        "permit_n_localities": "Cities with permits",
        "permit_share_attomid": "Share of permits with an ATTOMID",
        "permit_share_description": "Share of permits with a work description",
        "permit_covered": f"County-year has an ATTOM permit feed (>= {MIN_PERMITS} permits)",
        "permit_share_peak": "Permits as a share of the county's peak year",
        "attom_n_sf": "ATTOM single-family homes in county",
        "permit_per_100": "ATTOM permits per 100 ATTOM single-family homes",
        "permit_covered_strict": f"County-year has >= {args.floor:g} ATTOM permits per 100 homes",
        "locality": "Site address city (ATTOM PropertyAddressCity)",
        "year_first": "First year the city reports permits",
        "year_last": "Last year the city reports permits",
    }
    county.to_stata(data / "clean" / "permit_coverage_county.dta", write_index=False,
                    variable_labels={k: v for k, v in labels.items() if k in county})
    zipc.to_stata(data / "clean" / "permit_coverage_zip.dta", write_index=False,
                  variable_labels={k: v for k, v in labels.items() if k in zipc})
    locality.to_stata(data / "clean" / "permit_coverage_locality.dta", write_index=False, version=118,
                      variable_labels={k: v for k, v in labels.items() if k in locality})

    covered = county[county.permit_covered == 1]
    print(f"county-years: {len(county):,} ({len(covered):,} covered, "
          f"{int(county.permit_covered_strict.sum()):,} strictly); "
          f"counties covered in any year: {covered.countycode.nunique():,}; "
          f"zip-years: {len(zipc):,}; city-years: {len(locality):,} "
          f"({locality.groupby('state').locality.nunique().sum():,} cities)")
    print(covered.groupby("state").agg(counties=("countycode", "nunique"),
                                       yr_min=("year", "min"), yr_max=("year", "max")).to_string())


if __name__ == "__main__":
    main()
