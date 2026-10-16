"""
Authors: Anna Li
Date: 2026-10-02
Description: The ATTOM version of the builty permit extract script. Identifying elevations, along with appending status history information 
to identifies actually done elevations.
"""

import argparse
from pathlib import Path

import duckdb

# Candidate net -- recall, not precision. Anything an elevation permit might say.
CANDIDATE_PATTERNS = [
    r"elevat", r"rais(e|ed|ing)", r"lift(ed|ing)", r"jack(ed|ing) up", r"flood", r"fema",
    r"base flood elevation", r"finished floor elevation", r"freeboard", r"floodplain",
    r"\bbfe\b", r"\bffe\b", r"\bnfip\b", r"\bicc\b", r"flood plain", r"flood zone",
    r"hazard mitigation", r"substantial damage", r"substantially damaged",
    r"substantial improvement", r"substantially improved", r"storm surge",
    r"out of (the )?floodplain",
]

# ATTOM fills an unknown issue date with one of these
PLACEHOLDER_DATES = "(DATE '1900-01-01', DATE '1970-01-01')"

US_STATES = ("AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH "
             "NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY").split()


def main():
    p = argparse.ArgumentParser(description="Extract per-state ATTOM elevation-candidate permits.")
    p.add_argument("--data", required=True)
    p.add_argument("--states", required=True)
    p.add_argument("--memory", default="6GB")
    args = p.parse_args()

    states = [s.strip().upper() for s in args.states.replace(",", " ").split() if s.strip()]
    data = Path(args.data)
    src = data / "raw" / "attom_permits" / "attom_permits_*.parquet"
    status_src = data / "raw" / "attom_permits" / "attom_permit_status_*.parquet"
    have_status = any((data / "raw" / "attom_permits").glob("attom_permit_status_*.parquet"))
    #classifiers: added as placeholder for now, but will use in future for better selection
    # classifier_src = data / "raw" / "attom_permits" / "attom_permit_classifiers_*.parquet"
    # have_classifiers = any((data / "raw" / "attom_permits").glob("attom_permit_classifiers_*.parquet"))
    out_dir = data / "clean" / "permits_raw"
    out_dir.mkdir(parents=True, exist_ok=True)  
    inlist = ", ".join(f"'{s}'" for s in states)
    net = "|".join(CANDIDATE_PATTERNS)

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{args.memory}'")
    con.execute("SET preserve_insertion_order = false")
    if states == ["ALL"]:
        present = [r[0] for r in con.execute(f"SELECT DISTINCT PROPERTYADDRESSSTATE FROM read_parquet('{src}')").fetchall()]
        states = [s for s in US_STATES if s in present]
        inlist = ", ".join(f"'{s}'" for s in states)
    print(f"Scanning {src.name} for {len(states)} states ...", flush=True)

    # To match our original Builty pipeline structure, the permit's "Type - SubType" goes on line 1 of DESCRIPTION and the work text on line 2
    con.execute(f"""
        CREATE TABLE permits AS
        SELECT cast(BUILDINGPERMITID AS varchar) AS PERMIT_ID,
               cast(ATTOMID AS varchar) AS ATTOMID,
               PERMITNUMBER AS PERMIT_NUMBER,
               upper(COUNTYNAME) AS COUNTY,
               substr(STATECOUNTYFIPS, 1, 2) AS FIPS_STATE,
               substr(STATECOUNTYFIPS, 3, 3) AS FIPS_COUNTY,
               upper(PROPERTYADDRESSCITY) AS LOCALITY,
               PROPERTYADDRESSSTATE AS STATE,
               PROPERTYADDRESSZIP AS ZIPCODE,
               upper(PROPERTYADDRESSFULL) AS STREET_ADDRESS,
               CASE WHEN EFFECTIVEDATE NOT IN {PLACEHOLDER_DATES}
                    THEN strftime(EFFECTIVEDATE, '%Y-%m-%d') END AS DATE_ISSUED,
               STATUS, TYPE AS RECORD_TYPE,
               concat_ws(' - ', nullif(trim(TYPE), ''), nullif(trim(SUBTYPE), ''))
                   || chr(10) || coalesce(DESCRIPTION, '') AS DESCRIPTION,
               JOBVALUE AS PROJECT_VALUE, FEES AS TOTAL_FEES
        FROM read_parquet('{src}')
        WHERE PROPERTYADDRESSSTATE IN ({inlist})
          AND regexp_matches(lower(concat_ws(' ', TYPE, SUBTYPE, DESCRIPTION)), '{net}');
    """)

    # Status history: one summary row per candidate permit
    if have_status:
        con.execute(f"""
            CREATE TABLE status_summary AS
            WITH s AS (
                SELECT cast(BUILDINGPERMITID AS varchar) AS PERMIT_ID,
                       BUILDINGPERMITSTATUS AS st,
                       CASE WHEN EFFECTIVEDATE NOT IN {PLACEHOLDER_DATES} THEN EFFECTIVEDATE END AS d
                FROM read_parquet('{status_src}', union_by_name = true)
                WHERE cast(BUILDINGPERMITID AS varchar) IN (SELECT PERMIT_ID FROM permits)
            )
            SELECT PERMIT_ID,
                   strftime(min(d) FILTER (WHERE st IN ('FILED', 'APPLIED')), '%Y-%m-%d')  AS DATE_FILED,
                   strftime(min(d) FILTER (WHERE st = 'APPROVED'), '%Y-%m-%d')             AS DATE_APPROVED,
                   strftime(min(d) FILTER (WHERE st = 'ISSUED'), '%Y-%m-%d')               AS DATE_ISSUED_STATUS,
                   strftime(min(d) FILTER (WHERE st IN ('FINAL', 'COMPLETE')), '%Y-%m-%d') AS DATE_FINALED,

                   strftime(CASE WHEN max(d) FILTER (WHERE st = 'REINSTATED')
                                      >= max(d) FILTER (WHERE st IN ('EXPIRED', 'CANCELED', 'WITHDRAWN',
                                                                     'REVOKED', 'REJECTED'))
                                 THEN NULL
                                 ELSE max(d) FILTER (WHERE st IN ('EXPIRED', 'CANCELED', 'WITHDRAWN',
                                                                  'REVOKED', 'REJECTED')) END,
                            '%Y-%m-%d')                                                        AS DATE_EXPIRED,

                   arg_max(st, d) FILTER (WHERE d IS NOT NULL)                                AS STATUS_LAST,
                   strftime(max(d), '%Y-%m-%d')                                               AS STATUS_LAST_DATE,
                   count(*)                                                                   AS N_STATUS,
                   count(*) FILTER (WHERE st IN ('FINAL', 'COMPLETE'))                        AS N_FINAL_ROWS,
                   count(*) FILTER (WHERE st IN ('EXPIRED', 'CANCELED', 'WITHDRAWN',
                                                 'REVOKED', 'REJECTED'))                       AS N_KILL_ROWS,
                   count(*) FILTER (WHERE st = 'REINSTATED')                                  AS N_REINSTATED_ROWS
            FROM s GROUP BY 1;
        """)
        print(con.execute("""
            SELECT p.STATE AS state, count(*) AS candidates,
                   count(s.PERMIT_ID) AS with_status_history,
                   count(s.DATE_FINALED) AS finaled, count(s.DATE_EXPIRED) AS expired,
                   count(CASE WHEN s.N_FINAL_ROWS > 0 THEN 1 END) AS finaled_any_date 
            FROM permits p LEFT JOIN status_summary s USING (PERMIT_ID) GROUP BY 1 ORDER BY 2 DESC
        """).df().to_string(index=False), flush=True)
    else:
        print("no raw/attom_permits/attom_permit_status_*.parquet: status-history columns written empty", flush=True)
        con.execute("""
            CREATE TABLE status_summary AS
            SELECT PERMIT_ID, NULL::varchar AS DATE_FILED, NULL::varchar AS DATE_APPROVED,
                   NULL::varchar AS DATE_ISSUED_STATUS, NULL::varchar AS DATE_FINALED,
                   NULL::varchar AS DATE_EXPIRED, NULL::varchar AS STATUS_LAST,
                   NULL::varchar AS STATUS_LAST_DATE, NULL::bigint AS N_STATUS,
                   NULL::bigint AS N_FINAL_ROWS, NULL::bigint AS N_KILL_ROWS, NULL::bigint AS N_REINSTATED_ROWS
            FROM permits WHERE false;
        """)

    print(con.execute(f"""
        SELECT PROPERTYADDRESSSTATE AS state, count(*) AS permits, count(ATTOMID) AS with_attomid,
               count(*) FILTER (WHERE DESCRIPTION <> '') AS with_description,
               count(DISTINCT STATECOUNTYFIPS) AS counties
        FROM read_parquet('{src}') WHERE PROPERTYADDRESSSTATE IN ({inlist}) GROUP BY 1 ORDER BY 2 DESC
    """).df().to_string(index=False), flush=True)
    for st in states:
        out = out_dir / f"{st.lower()}.csv"
        con.execute(f"""COPY (SELECT p.*, s.DATE_FILED, s.DATE_APPROVED, s.DATE_ISSUED_STATUS, s.DATE_FINALED,
                               s.DATE_EXPIRED, s.STATUS_LAST, s.STATUS_LAST_DATE, s.N_STATUS,
                               s.N_FINAL_ROWS, s.N_KILL_ROWS, s.N_REINSTATED_ROWS
                        FROM permits p LEFT JOIN status_summary s USING (PERMIT_ID)
                        WHERE p.STATE = '{st}' ORDER BY p.PERMIT_ID)
                        TO '{out}' (FORMAT CSV, HEADER);""")
        n, n_id = con.execute(f"""SELECT count(*), count(ATTOMID) FROM permits WHERE STATE = '{st}'""").fetchone()
        print(f"  {st}: {n:,} candidate permits, {n_id:,} with an ATTOMID", flush=True)


if __name__ == "__main__":
    main()
