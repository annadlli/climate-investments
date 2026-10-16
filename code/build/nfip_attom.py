"""
Authors: Anna Li
Original Date: 2026-08-14
Revised Date: 2026-09-21

Pairs each NFIP-insured property with one ATTOM property, so the insurance
records pick up a property value and an elevation-permit flag. Use NFHL zones and NFIP ratings to get flood zones
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

# ATTOM value fields carried onto the NFIP property
VALUE_COLUMNS = [
    "market_value_total", "market_value_land", "market_value_improvements",
    "assessed_value_total", "assessed_value_improvements",
    "previous_assessed_value", "last_sale_price",
]
# High vs low flood risk, following Wagner classification, where SFHA zones (A/V family) are high, the rest low.
# Unknown codes staymissing rather than being called low risk. 
# A99 and AR (levee zones) exist in NFIP but ot really in the NFHL, so they count as high risk and can only match on risk, not zone.
HIGH_RISK = ("AE", "AH", "AO", "VE", "A99", "AR")
LOW_RISK = ("D", "X", "XE")

# match ladder, tightest first. each tier is (cell keys, name).
# gives up precision one piece at a time: coarser geography, zone -> risk,
# exact year -> 5yr window, then no flood info at all
#total of 15
TIERS = [
    (["blockgroup_key", "flood_zone_key", "construction_year"], "1_bg_zone_exact_year"),
    (["blockgroup_key", "flood_risk_key", "construction_year"], "2_bg_risk_exact_year"),
    (["blockgroup_key", "flood_zone_key", "construction_5yr", "postfirm_key"], "3_bg_zone_5yr_postfirm"),
    (["blockgroup_key", "flood_risk_key", "construction_5yr", "postfirm_key"], "4_bg_risk_5yr_postfirm"),
    (["zip_key", "flood_zone_key", "construction_year"], "5_zip_zone_exact_year"),
    (["zip_key", "flood_risk_key", "construction_5yr", "postfirm_key"], "6_zip_risk_5yr_postfirm"),
    (["community_key", "flood_zone_key", "construction_year"], "7_community_zone_exact_year"),
    (["community_key", "flood_risk_key", "construction_5yr", "postfirm_key"], "8_community_risk_5yr_postfirm"),
    (["county_key", "flood_zone_key", "construction_year"], "9_county_zone_exact_year"),
    (["county_key", "flood_risk_key", "construction_5yr", "postfirm_key"], "10_county_risk_5yr_postfirm"),
    (["blockgroup_key", "construction_year"], "11_bg_exact_year"),
    (["zip_key", "construction_year"], "12_zip_exact_year"),
    (["county_key", "construction_year"], "13_county_exact_year"),
    (["county_key", "construction_5yr"], "14_county_5yr"),
]
# last resort, added after LA -- construction year mostly missing there
TIER_15 = (["blockgroup_key", "flood_zone_key"], "15_bg_zone_no_construction")

# NFIP homes first insured before this year carry 2010-geography block groups, later ones 2020
VINTAGE_SWITCH_YEAR = 2021 

# columns a match fills in, and their types
ASSIGNMENT_COLUMNS = {
    "nfip_attom_merge_status": "integer default 1", "match_tier": "varchar",
    "match_tier_number": "integer", "match_cell_id": "varchar",
    "assigned_attomid": "varchar", "assignment_method": "varchar",
    "nfip_cell_rank": "integer", "attom_cell_rank": "integer",
    "nfip_cell_n": "integer", "attom_cell_n": "integer",
    "permit_attom_cell_n": "integer", "cell_singleton": "integer",
    "attom_flood_zone_original": "varchar", "attom_flood_zone_key": "varchar",
    "attom_flood_risk_key": "varchar", "attom_nfhl_flood_matched": "boolean",
    "attom_property_use_std": "varchar",
    "attom_nfhl_community_matched": "boolean", "permit_elevated": "integer",
    "permit_elevation_year": "integer", "permit_n_properties": "integer",
    "permit_merge_status": "integer", "permit_attom_match_tier": "varchar",
    # 09-06: retrofit / new-construction flags and the geocode-backfill flag from attom_permits.py
    "permit_retrofit": "integer", "permit_new_construction": "integer",
    "permit_geo_backfilled": "integer",
    "permit_built_at_permit": "integer",   # 09-07: permit year within a year of ATTOM year built
    "permit_completed": "integer", "permit_year_issued": "integer", "permit_status": "varchar",
    # 09-12: builty cost and funding source
    "permit_project_value": "double", "permit_funding_type": "integer",
    "blockgroup_vintage_used": "varchar",   # 09-21: which ATTOM block-group vintage the cell used
    "nfip_alive_at_permit": "integer",      # 09-22: 1 = insured in the permit year, 0 = not, NULL = no permit house in the cell
    "attom_value_year": "integer", "attom_value_lag": "integer",
    **{f"attom_{c}": "double" for c in VALUE_COLUMNS},
}

# same tidy-up as zone(), in SQL for the ATTOM side.
# goes in as a value, so single braces are fine in the f-strings
_Z = "upper(replace(trim(cast(fld_zone AS varchar)),' ',''))"
ZONE_SQL = f"""
    CASE WHEN regexp_matches({_Z}, '^(AH|AO)B$') THEN substr({_Z}, 1, 2)
         WHEN regexp_matches({_Z}, '^(A|AE|A(0?[1-9]|[12][0-9]|30))$') THEN 'AE'
         WHEN regexp_matches({_Z}, '^(V|VE|V(0?[1-9]|[12][0-9]|30))$') THEN 'VE'
         WHEN {_Z} IN ('B', 'C') THEN 'X'
         ELSE nullif({_Z}, '') END
"""


def q(value: object) -> str:
    # sql-safe string
    return "'" + str(value).replace("'", "''") + "'"


def sql_list(values: tuple[str, ...]) -> str:
    return ",".join(q(v) for v in values)


def norm(series: pd.Series, width: int | None = None) -> pd.Series:
    # tidy a key: trim, upper, drop trailing ".0", pad numeric codes to width
    value = series.astype("string").str.strip().str.upper().str.replace(r"\.0$", "", regex=True)
    value = value.replace({"": pd.NA, "<NA>": pd.NA, "NAN": pd.NA, "NONE": pd.NA})
    if width:
        numeric = value.str.fullmatch(r"\d+").fillna(False)
        value.loc[numeric] = value.loc[numeric].str.zfill(width)
    return value


def to_year(series: pd.Series, label: str) -> pd.Series:
    # year key -> number. bad values become NA and drop the property out of every tier keyed on it, so say how many
    value = pd.to_numeric(series, errors="coerce")
    return value


def zone(series: pd.Series) -> pd.Series:
# Put NFIP policy codes and NFHL map codes on one key 
    out = norm(series).str.replace(" ", "", regex=False)
    out = out.str.replace(r"^(AH|AO)B$", r"\1", regex=True)
    # numbered zones run 1-30 (A99 is its own zone, so no bare \d{1,2})
    out.loc[out.str.fullmatch(r"A|AE|A(0?[1-9]|[12][0-9]|30)").fillna(False)] = "AE"
    out.loc[out.str.fullmatch(r"V|VE|V(0?[1-9]|[12][0-9]|30)").fillna(False)] = "VE"
    out.loc[out.isin(["B", "C"])] = "X"
    return out


def risk(series: pd.Series) -> pd.Series:
    # high/low buckets on the aligned key; map gaps (AREANOTINCLUDED, missing) stay missing
    z = zone(series)
    out = pd.Series(pd.NA, index=z.index, dtype="string")
    out.loc[z.isin(HIGH_RISK)] = "high_risk"
    out.loc[z.isin(LOW_RISK)] = "low_risk"
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state", required=True)
    p.add_argument("--properties", required=True,
                   help="clean/nfip_policies_property.dta (first-policy-year snapshot, all states)")
    p.add_argument("--attom", required=True, help="{state}_attom_geocoded.parquet")
    p.add_argument("--attom-nfhl-permits", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--tmp", required=True)
    p.add_argument("--use-codes", default="376,380,382,383,385,386",
                   help="ATTOM property_use_std codes eligible as candidates. "
                        "Default is the six agreed single-family codes; use 'all' to disable.")
    p.add_argument("--add-tier-15", action="store_true",
                   help="Add the block group x flood zone fallback that ignores construction "
                        "year, and let year-less ATTOM properties into the pool to reach it.")
    p.add_argument("--memory", default="64GB")
    p.add_argument("--threads", type=int, default=4)
    return p.parse_args()


def load_properties(path: str, state: str) -> pd.DataFrame: 
    # one row per NFIP property as of its first policy year, built by
    # prep_nfip_policies.do (Section 2). All states in one file; filter to this one.
    columns = ["property_id", "property_id_state", "state", "construction_year",
               "policy_year_init", "zipcode", "censusblockgroupfips", "countycode",
               "nfipratedcommunitynumber", "flood_zone", "post_firm"]
    frame = pd.read_stata(path, convert_categoricals=False, columns=columns)
    frame = frame.loc[frame["state"].astype(str).str.upper().eq(state.upper())].copy()
    frame["matching_policy_year"] = frame["policy_year_init"]

    # one row per property, or nothing downstream is 1:1
    if frame["property_id"].duplicated().any():
        raise ValueError(f"{state}: property_id is not unique in the NFIP property base")

    # every tier key, same shape as the ATTOM side
    frame["zip_key"] = norm(frame["zipcode"], 5)
    frame["county_key"] = norm(frame["countycode"], 5)
    frame["blockgroup_key"] = norm(frame["censusblockgroupfips"], 12)
    # 09-21: which ATTOM vintage to try first (see module docstring)
    init = pd.to_numeric(frame["policy_year_init"], errors="coerce")
    frame["blockgroup_vintage_pref"] = pd.Series(
        ["2010" if (y == y and y < VINTAGE_SWITCH_YEAR) else "2020" for y in init], index=frame.index, dtype="string")
    frame["community_key"] = norm(frame["nfipratedcommunitynumber"], 6)
    frame["construction_year"] = to_year(frame["construction_year"], "construction_year").astype("Int64")
    frame["construction_5yr"] = (frame["construction_year"] // 5) * 5
    frame["construction_decade"] = (frame["construction_year"] // 10) * 10
    frame["nfip_flood_zone_original"] = frame["flood_zone"].astype("string")
    frame["flood_zone_key"] = zone(frame["flood_zone"])
    frame["flood_risk_key"] = risk(frame["flood_zone"])
    frame["postfirm_key"] = to_year(frame["post_firm"], "post_firm").astype("Int64")

    # anchor year for the value lookup
    frame["reference_year"] = to_year(
        frame["matching_policy_year"], "matching_policy_year"
    ).astype("Int64")

    # last policy year, for the permit-aware ranking: nfip_policy_years.parquet sits beside the properties file
    ly = pd.read_parquet(Path(path).with_name("nfip_policy_years.parquet"))
    ly = ly.loc[ly["state"].str.upper() == state.upper(), ["property_id_state", "policy_year_last"]]
    ly["property_id_state"] = pd.to_numeric(ly["property_id_state"], errors="coerce")
    frame["property_id_state"] = pd.to_numeric(frame["property_id_state"], errors="coerce")
    frame = frame.merge(ly.drop_duplicates("property_id_state"), on="property_id_state", how="left")
    frame["policy_year_last"] = frame["policy_year_last"].astype("Int64")
    print(f"{state}: last policy year known for {frame['policy_year_last'].notna().mean():.1%} of properties")
    frame["policy_year_init_num"] = to_year(frame["policy_year_init"], "policy_year_init").astype("Int64")
    return frame


def use_code_filter(use_codes: str) -> str:
    # single-family only. 'all' turns it off; 'blank' also lets through rows with
    # no code -- those look like thin-record houses (median 3bed/1bath), not the
    # 0-bed 0-bath lots and shops the other codes cover
    if use_codes.strip().lower() == "all":
        return "TRUE"
    tokens = [t.strip() for t in use_codes.split(",") if t.strip()]
    codes = [t for t in tokens if t.lower() != "blank"]
    clauses = []
    if codes:
        clauses.append(f"trim(cast(property_use_std AS varchar)) IN ({sql_list(tuple(codes))})")
    if any(t.lower() == "blank" for t in tokens):
        clauses.append("coalesce(trim(cast(property_use_std AS varchar)),'')=''")
    return "(" + " OR ".join(clauses) + ")" if clauses else "TRUE"


def build_attom(con: duckdb.DuckDBPyConnection, attom: str, enriched: str,
                use_codes: str, allow_missing_construction: bool) -> None:
    # eligible ATTOM pool: one row each, with NFHL flood zone + elevation flag
    #09-06add in flags carried from attom_permits.py, and fall back to the permit block group when ATTOM's own geocode fails
    year_filter = "TRUE" if allow_missing_construction else "p.construction_year BETWEEN 1700 AND 2027"
    con.execute(f"""
        CREATE TABLE attom AS
        WITH p AS (
          -- geocoded file is property x year, so flatten to properties
          SELECT cast(attomid AS varchar) attomid,
                 max(cast(construction_year AS integer)) construction_year,
                 max(nullif(trim(cast(property_use_std AS varchar)),'')) property_use_std,
                 max(nullif(trim(cast(zip AS varchar)),'')) zip_key,
                 max(nullif(trim(cast(countycode AS varchar)),'')) county_key,
                 max(nullif(trim(cast(censusblockgroupfips AS varchar)),'')) blockgroup_key,
                 max(nullif(trim(cast(censusblockgroupfips2010 AS varchar)),'')) blockgroup_key_2010  
          FROM read_parquet({q(attom)})
          WHERE {use_code_filter(use_codes)}
          GROUP BY 1
        ), e AS (SELECT * FROM read_parquet({q(enriched)}))
        SELECT p.* EXCLUDE(blockgroup_key),
          coalesce(p.blockgroup_key, nullif(trim(cast(e.permit_blockgroup AS varchar)),'')) blockgroup_key,
          cast(e.permit_retrofit AS integer) permit_retrofit,
          cast(e.permit_new_construction AS integer) permit_new_construction,
          cast(e.permit_built_at_permit AS integer) permit_built_at_permit,
          cast(e.permit_project_value AS double) permit_project_value,
          cast(e.permit_funding_type AS integer) permit_funding_type,
          cast(e.permit_completed AS integer) permit_completed,        
          cast(e.permit_year_issued AS integer) permit_year_issued,    
          cast(e.permit_status AS varchar) permit_status,              
          coalesce(cast(e.coords_backfilled_permit AS integer),0) permit_geo_backfilled,
          -- pad community number to 6 digits to match NFIP
          CASE WHEN regexp_matches(trim(cast(e.nfip_community_id AS varchar)), '^[0-9]+(\\.0)?$')
                 THEN lpad(regexp_extract(trim(cast(e.nfip_community_id AS varchar)), '^(\\d+)', 1), 6, '0')
               ELSE nullif(upper(trim(cast(e.nfip_community_id AS varchar))),'') END community_key,
          upper(replace(trim(cast(e.fld_zone AS varchar)),' ','')) attom_flood_zone_original,
          ({ZONE_SQL}) flood_zone_key,
          CASE WHEN ({ZONE_SQL}) IN ({sql_list(HIGH_RISK)}) THEN 'high_risk'
               WHEN ({ZONE_SQL}) IN ({sql_list(LOW_RISK)}) THEN 'low_risk' END flood_risk_key,
          (p.construction_year//5)*5 construction_5yr,
          (p.construction_year//10)*10 construction_decade,
          -- post-FIRM = built after the town's first map; 1975 floor if that map
          -- predates the program
          CASE WHEN e.initial_firm_year IS NOT NULL THEN cast(
               p.construction_year >= CASE WHEN cast(e.initial_firm_year AS integer)<1975
                                           THEN 1975 ELSE cast(e.initial_firm_year AS integer)+1 END
               AS integer) END postfirm_key,
          cast(e.nfhl_flood_matched AS boolean) nfhl_flood_matched,
          cast(e.nfhl_community_matched AS boolean) nfhl_community_matched,
          coalesce(cast(e.permit_elevated AS integer),0) permit_elevated,
          cast(e.permit_elevation_year AS integer) permit_elevation_year,
          coalesce(cast(e.permit_n_properties AS integer),0) permit_n_properties,
          cast(e.permit_merge_status AS integer) permit_merge_status,
          cast(e.permit_attom_match_tier AS varchar) permit_attom_match_tier,
          false assigned
        FROM p INNER JOIN e USING(attomid)
        WHERE {year_filter}
    """)


def apply_tier(con: duckdb.DuckDBPyConnection, keys: list[str], label: str, tier: int,
               bg_vintage: str | None = None, nfip_pref: str | None = None) -> int:
    # 09-21: a block-group tier runs once per (NFIP preferred vintage, ATTOM vintage) pair.
    # bg_vintage picks which ATTOM code stands in for blockgroup_key; nfip_pref restricts
    # the NFIP side to homes that prefer a given vintage. Both None = the plain tier.
    ks = ", ".join(keys)
    valid = " AND ".join(f"{k} IS NOT NULL" for k in keys)
    attom_src = ("(SELECT * EXCLUDE(blockgroup_key), blockgroup_key_2010 AS blockgroup_key FROM attom)"
                 if bg_vintage == "2010" else "attom")
    nfip_where = f" AND blockgroup_vintage_pref = {q(nfip_pref)}" if nfip_pref else ""

    # cell sizes on both sides, before this tier takes anything.
    # inner join keeps only cells with an unmatched NFIP property and a free ATTOM one
    con.execute("DROP TABLE IF EXISTS cell_stats")
    con.execute(f"""
        CREATE TEMP TABLE cell_stats AS
        WITH n AS (
          SELECT {ks}, count(*) nfip_cell_n FROM nfip
          WHERE nfip_attom_merge_status=1 AND {valid}{nfip_where} GROUP BY {ks}
        ), a AS (
          SELECT {ks}, count(*) attom_cell_n, sum((permit_elevated=1)::integer) permit_attom_cell_n,
                 -- 09-22: the cell's permit year (earliest, if several permit houses)
                 min(CASE WHEN permit_elevated=1 THEN permit_elevation_year END) permit_year
          FROM {attom_src} WHERE assigned=false AND {valid} GROUP BY {ks}
        )
        SELECT n.*, a.attom_cell_n, a.permit_attom_cell_n, a.permit_year,
               {q(label)}||'|'||md5(concat_ws('|',{ks})) match_cell_id,
               (n.nfip_cell_n=1 AND a.attom_cell_n=1)::integer cell_singleton
        FROM n INNER JOIN a USING({ks})
    """)

    # inside a cell, rank both sides and pair by position. order is a hash of
    # cell id + property id: reproducible, unrelated to sort order, different
    # salt per side. Permit houses ranked first so they win their slot
    con.execute("DROP TABLE IF EXISTS tier_hits")
    con.execute(f"""
        CREATE TEMP TABLE tier_hits AS
        WITH nr AS (
          -- 09-22: in a cell with a permit house, homes insured in the permit year rank first
          -- (they pair with the permit house, which ranks first on the ATTOM side), then homes
          -- built by the permit year, then the hash order
          SELECT n.property_id, {ks}, s.match_cell_id, s.nfip_cell_n, s.attom_cell_n,
                 s.permit_attom_cell_n, s.cell_singleton,
                 CASE WHEN s.permit_year IS NULL THEN NULL
                      WHEN n.policy_year_init_num <= s.permit_year
                       AND coalesce(n.policy_year_last, 9999) >= s.permit_year THEN 1 ELSE 0 END alive_at_permit,
                 row_number() over(partition by {ks}
                   order by CASE WHEN s.permit_year IS NOT NULL AND n.policy_year_init_num <= s.permit_year
                                  AND coalesce(n.policy_year_last, 9999) >= s.permit_year THEN 0 ELSE 1 END,
                            CASE WHEN s.permit_year IS NOT NULL AND n.construction_year <= s.permit_year THEN 0 ELSE 1 END,
                            md5(s.match_cell_id||'|nfip|fixed_seed|'||cast(n.property_id as varchar))) cell_rank
          FROM nfip n INNER JOIN cell_stats s USING({ks})
          WHERE n.nfip_attom_merge_status=1 AND {valid}{nfip_where}
        ), ar AS (
          SELECT a.* EXCLUDE({ks}), {ks},
                 a.flood_zone_key assigned_flood_zone_key,
                 a.flood_risk_key assigned_flood_risk_key,
                 row_number() over(partition by {ks}
                   order by a.permit_elevated desc,
                            md5(s.match_cell_id||'|attom|fixed_seed|'||a.attomid)) cell_rank
          FROM {attom_src} a INNER JOIN cell_stats s USING({ks})
          WHERE a.assigned=false AND {valid}
        )
        SELECT nr.property_id, nr.match_cell_id, nr.nfip_cell_n, nr.attom_cell_n,
               nr.permit_attom_cell_n, nr.cell_singleton, nr.alive_at_permit,
               nr.cell_rank nfip_cell_rank, ar.cell_rank attom_cell_rank,
               ar.* EXCLUDE({ks}, cell_rank)
        FROM nr INNER JOIN ar USING({ks}, cell_rank)
    """)

    # copy the winner's details onto the NFIP property
    assignments = [
        "nfip_attom_merge_status=3", f"match_tier={q(label)}", f"match_tier_number={tier}",
        "match_cell_id=h.match_cell_id", "assigned_attomid=h.attomid",
        "nfip_cell_rank=h.nfip_cell_rank", "attom_cell_rank=h.attom_cell_rank",
        "nfip_cell_n=h.nfip_cell_n", "attom_cell_n=h.attom_cell_n",
        "permit_attom_cell_n=h.permit_attom_cell_n", "cell_singleton=h.cell_singleton",
        "assignment_method=case when h.cell_singleton=1 then 'singleton' else 'deterministic_hash_rank' end",
        "attom_flood_zone_original=h.attom_flood_zone_original",
        "attom_flood_zone_key=h.assigned_flood_zone_key",
        "attom_flood_risk_key=h.assigned_flood_risk_key",
        "attom_property_use_std=h.property_use_std",
        "attom_nfhl_flood_matched=h.nfhl_flood_matched",
        "attom_nfhl_community_matched=h.nfhl_community_matched",
        "permit_elevated=h.permit_elevated", "permit_elevation_year=h.permit_elevation_year",
        "permit_n_properties=h.permit_n_properties", "permit_merge_status=h.permit_merge_status",
        "permit_attom_match_tier=h.permit_attom_match_tier",
        # 09-06: the three new flags ride onto the NFIP property with the rest
        "permit_retrofit=h.permit_retrofit", "permit_new_construction=h.permit_new_construction",
        "permit_geo_backfilled=h.permit_geo_backfilled",
        "permit_built_at_permit=h.permit_built_at_permit",
        "permit_completed=h.permit_completed", "permit_year_issued=h.permit_year_issued",
        "permit_status=h.permit_status",
        "permit_project_value=h.permit_project_value", "permit_funding_type=h.permit_funding_type",
        f"blockgroup_vintage_used={q(bg_vintage) if bg_vintage else 'NULL'}",
        "nfip_alive_at_permit=h.alive_at_permit",
    ]
    con.execute(f"UPDATE nfip n SET {','.join(assignments)} FROM tier_hits h WHERE n.property_id=h.property_id")

    # mark those ATTOMIDs used, so later tiers can't reuse them
    con.execute("UPDATE attom a SET assigned=true FROM tier_hits h WHERE a.attomid=h.attomid")

    # Report progress; diagnostic tables are generated separately.
    assigned = con.execute("SELECT count(*) FROM tier_hits").fetchone()[0]
    print(f"{label:<38} {'pref ' + nfip_pref + ' on ATTOM ' + bg_vintage if bg_vintage else '':<28} {assigned:>10,}")
    return assigned


def attach_values(con: duckdb.DuckDBPyConnection, attom: str) -> None:
    # value each property: latest ATTOM assessment at or before its matched
    # policy year. unmatched come back blank
    # 09-15:  panel is read once, cut to the  assigned IDs, joined, and a window keeps the latest eligible year: same rows,
    # but it streams under a small memory cap instead of holding the panel
    con.execute(f"""
        CREATE TABLE final AS
        WITH v AS (
          SELECT cast(attomid AS varchar) attomid, cast(year AS integer) value_year,
                 {','.join(VALUE_COLUMNS)}
          FROM read_parquet({q(attom)})
          WHERE cast(year AS integer) BETWEEN 1980 AND 2035
            AND cast(attomid AS varchar) IN (SELECT assigned_attomid FROM nfip
                                             WHERE assigned_attomid IS NOT NULL)
        ), pick AS (
          SELECT n.property_id, v.value_year, {','.join('v.'+c for c in VALUE_COLUMNS)},
                 row_number() OVER (PARTITION BY n.property_id ORDER BY v.value_year DESC) rk
          FROM nfip n JOIN v ON v.attomid = n.assigned_attomid
          WHERE v.value_year <= coalesce(n.permit_elevation_year, n.reference_year)
        )
        SELECT n.* EXCLUDE(attom_value_year, attom_value_lag,
                           {','.join('attom_'+c for c in VALUE_COLUMNS)}),
               coalesce(n.permit_elevation_year, n.reference_year) value_reference_year,
               p.value_year attom_value_year,
               coalesce(n.permit_elevation_year, n.reference_year) - p.value_year attom_value_lag,
               {','.join('p.'+c+' attom_'+c for c in VALUE_COLUMNS)}
        FROM nfip n LEFT JOIN pick p ON p.property_id = n.property_id AND p.rk = 1
    """)


def main() -> None:
    args = parse_args()
    state = args.state.upper()

    # NFIP properties for this state
    properties = load_properties(args.properties, state)
    print(f"{state}: block-group vintage preference: {properties['blockgroup_vintage_pref'].value_counts().to_dict()}")
    print(f"{state}: {len(properties):,} NFIP properties")

    con = duckdb.connect()
    con.execute(f"SET temp_directory={q(args.tmp)}")
    con.execute(f"SET memory_limit={q(args.memory)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")

    # everything starts unmatched, columns empty
    con.register("property_frame", properties)
    con.execute("CREATE TABLE nfip AS SELECT * FROM property_frame")
    for name, dtype in ASSIGNMENT_COLUMNS.items():
        con.execute(f"ALTER TABLE nfip ADD COLUMN {name} {dtype}")

    # the ATTOM pool to match against
    build_attom(con, args.attom, args.attom_nfhl_permits, args.use_codes,
                allow_missing_construction=args.add_tier_15)
    print(f"{state}: {con.execute('SELECT count(*) FROM attom').fetchone()[0]:,} "
          f"ATTOM candidates (use_codes={args.use_codes})")

    # down the ladder; each tier only sees leftovers
    tiers = TIERS + [TIER_15] if args.add_tier_15 else list(TIERS)
    for number, (keys, label) in enumerate(tiers, start=1):
        if "blockgroup_key" in keys:
            # preferred vintage for each side first, then the other vintage as fallback
            total = 0
            for nfip_pref, bg_vintage in (("2010", "2010"), ("2020", "2020"), ("2010", "2020"), ("2020", "2010")):
                total += apply_tier(con, keys, label, number, bg_vintage=bg_vintage, nfip_pref=nfip_pref)
            print(f"{label:<38} {total:>10,}")
        else:
            apply_tier(con, keys, label, number)

    attach_values(con, args.attom)

    out = Path(args.out)
    con.execute(f"COPY (SELECT * FROM final ORDER BY property_id) TO {q(out)} (FORMAT PARQUET, COMPRESSION ZSTD)")

    con.close()
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
