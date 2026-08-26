"""
Export the ATTOM candidate universe and value panel to Stata.

Authors: Anna Li
Date: 2026-08-14

Description:
    Bridge between the parquet side of the build and the Stata assignment. All
    the heavy reduction (raw ATTOM -> property-year panel) already happened in
    build_attom_geocoded.py; this only reshapes the geocoded panel plus the
    NFHL/Builty enrichment into two small .dta files per state:

        {state}_attom_candidates.dta   one row per ATTOMID: cell keys, zone,
                                       post-FIRM status, Builty flags
        {state}_attom_values.dta       ATTOMID x tax year: the value columns,
                                       for the as-of pick after assignment

    Splitting identity from values keeps the candidate file small enough that
    Stata can hold it in a frame for the whole tier waterfall.

    Input  : {data}/build/{state}_attom_geocoded.parquet
             {data}/build/attom_nfhl_builty/{state}_attom_nfhl_builty.parquet
    Output : {data}/build/attom_stata/{state}_attom_candidates.dta
             {data}/build/attom_stata/{state}_attom_values.dta
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


VALUE_COLUMNS = [
    "market_value_total", "market_value_land", "market_value_improvements",
    "assessed_value_total", "assessed_value_improvements",
    "previous_assessed_value", "last_sale_price",
]

HIGH_RISK = ("A", "AE", "AH", "AO", "V", "VE")
LOW_RISK = ("B", "C", "D", "X", "XE")


def q(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_list(values: tuple[str, ...]) -> str:
    return ",".join(q(v) for v in values)


ZONE_SQL = """
    CASE WHEN regexp_matches(upper(replace(trim(cast(e.fld_zone AS varchar)),' ','')),'^A[0-9]{1,2}$') THEN 'AE'
         WHEN regexp_matches(upper(replace(trim(cast(e.fld_zone AS varchar)),' ','')),'^V[0-9]{1,2}$') THEN 'VE'
         ELSE nullif(upper(replace(trim(cast(e.fld_zone AS varchar)),' ','')),'') END
"""


def use_code_filter(use_codes: str) -> str:
    """See assign_attom_to_nfip_property.py; 'blank' admits unreported codes."""
    if use_codes.strip().lower() == "all":
        return "TRUE"
    tokens = [t.strip() for t in use_codes.split(",") if t.strip()]
    allow_blank = any(t.lower() == "blank" for t in tokens)
    codes = [t for t in tokens if t.lower() != "blank"]
    clauses = []
    if codes:
        clauses.append(f"trim(cast(property_use_std AS varchar)) IN ({sql_list(tuple(codes))})")
    if allow_blank:
        clauses.append("coalesce(trim(cast(property_use_std AS varchar)),'')=''")
    return "(" + " OR ".join(clauses) + ")" if clauses else "TRUE"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state", required=True)
    p.add_argument("--attom", required=True)
    p.add_argument("--attom-nfhl-builty", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--use-codes", default="376,380,382,383,385,386,blank",
                   help="ATTOM SFR property type (patio home, residential general, "
                        "row house, rural residence, single family residence, "
                        "townhouse); 'blank' admits unreported codes, 'all' disables.")
    p.add_argument("--tmp", default="/tmp")
    p.add_argument("--memory", default="32GB")
    p.add_argument("--threads", type=int, default=4)
    return p.parse_args()


def to_stata(frame: pd.DataFrame, path: Path) -> None:
    for column in frame.columns:
        if frame[column].dtype == object or str(frame[column].dtype) == "string":
            frame[column] = frame[column].fillna("").astype(str)
        elif frame[column].dtype == bool:
            frame[column] = frame[column].astype("int8")
        elif str(frame[column].dtype).startswith(("Int", "boolean")):
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float64")
    frame.to_stata(path, write_index=False, version=118)


def main() -> None:
    a = parse_args()
    state = a.state.lower()
    out_dir = Path(a.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    Path(a.tmp).mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory={q(a.tmp)}")
    con.execute(f"SET memory_limit={q(a.memory)}")
    con.execute(f"SET threads={a.threads}")
    con.execute("SET preserve_insertion_order=false")

    use_filter = use_code_filter(a.use_codes)
    candidates = con.execute(f"""
        WITH p AS (
          SELECT cast(attomid AS varchar) attomid,
                 max(cast(construction_year AS integer)) construction_year,
                 max(nullif(trim(cast(zip AS varchar)),'')) zip_key,
                 max(nullif(trim(cast(censusblockgroupfips AS varchar)),'')) blockgroup_key
          FROM read_parquet({q(a.attom)})
          WHERE {use_filter}
          GROUP BY 1
        ), e AS (SELECT * FROM read_parquet({q(a.attom_nfhl_builty)}))
        SELECT p.attomid, p.construction_year,
               (p.construction_year//5)*5  construction_5yr,
               (p.construction_year//10)*10 construction_decade,
               coalesce(p.zip_key,'') zip_key,
               coalesce(p.blockgroup_key,'') blockgroup_key,
               coalesce(nullif(upper(trim(cast(e.nfip_community_id AS varchar))),''),'') community_key,
               coalesce(upper(replace(trim(cast(e.fld_zone AS varchar)),' ','')),'') attom_flood_zone_original,
               coalesce(({ZONE_SQL}),'') flood_zone_key,
               coalesce(CASE WHEN ({ZONE_SQL}) IN ({sql_list(HIGH_RISK)}) THEN 'high_risk'
                             WHEN ({ZONE_SQL}) IN ({sql_list(LOW_RISK)})  THEN 'low_risk' END,'') flood_risk_key,
               CASE WHEN e.initial_firm_year IS NOT NULL THEN cast(
                    p.construction_year >= CASE WHEN cast(e.initial_firm_year AS integer)<1975
                                                THEN 1975 ELSE cast(e.initial_firm_year AS integer)+1 END
                    AS integer) END postfirm_key,
               cast(e.nfhl_flood_matched AS integer) attom_nfhl_flood_matched,
               cast(e.nfhl_community_matched AS integer) attom_nfhl_community_matched,
               coalesce(cast(e.builty_elevated AS integer),0) builty_elevated,
               cast(e.builty_elevation_year AS integer) builty_elevation_year,
               coalesce(cast(e.builty_n_properties AS integer),0) builty_n_properties,
               cast(e.builty_merge_status AS integer) builty_merge_status,
               coalesce(cast(e.builty_attom_match_tier AS varchar),'') builty_attom_match_tier
        FROM p INNER JOIN e USING(attomid)
        WHERE p.construction_year BETWEEN 1700 AND 2027
    """).df()

    values = con.execute(f"""
        SELECT cast(attomid AS varchar) AS attomid, cast(year AS integer) AS year,
               {','.join(f'max(cast({c} AS double)) AS {c}' for c in VALUE_COLUMNS)}
        FROM read_parquet({q(a.attom)})
        WHERE {use_filter} AND cast(year AS integer) BETWEEN 1980 AND 2035
        GROUP BY 1,2
    """).df()
    # Values are only needed for properties that can actually be assigned.
    values = values[values["attomid"].isin(set(candidates["attomid"]))].copy()
    con.close()

    cand_path = out_dir / f"{state}_attom_candidates.dta"
    val_path = out_dir / f"{state}_attom_values.dta"
    to_stata(candidates, cand_path)
    to_stata(values, val_path)
    print(f"{state.upper()}: {len(candidates):,} candidates (use_codes={a.use_codes}) -> {cand_path.name}")
    print(f"{state.upper()}: {len(values):,} value rows -> {val_path.name}")


if __name__ == "__main__":
    main()
