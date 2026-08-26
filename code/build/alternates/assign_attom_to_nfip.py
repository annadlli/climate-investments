"""Assign enriched ATTOM properties to NFIP policy records within matching cells.

NFIP is the master: every source policy row is retained. ATTOM candidates already
carry historical NFHL and Builty attributes. Builty does not affect cell
eligibility, but Builty-linked candidates are prioritized within a cell before
the deterministic hash tie-break, matching the coverage-oriented V2 rule.
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
BUILTY_COLUMNS = [
    "builty_elevated", "builty_elevation_year", "builty_n_properties",
    "builty_attom_match_tier", "builty_merge_status",
]

SFR_CODES = ("376", "380", "382", "383", "385", "386")

# Wagner-style policy-year cells. Policy year is part of every tier; tier 15
# deliberately omits construction year to retain properties in states where
# ATTOM's assessor feed does not report it.
TIERS = [
    (["policy_year", "blockgroup_key", "flood_zone_key", "construction_year"], "1_bg_zone_exact_year"),
    (["policy_year", "blockgroup_key", "flood_risk_key", "construction_year"], "2_bg_risk_exact_year"),
    (["policy_year", "blockgroup_key", "flood_zone_key", "construction_5yr", "postfirm_key"], "3_bg_zone_5yr_postfirm"),
    (["policy_year", "blockgroup_key", "flood_risk_key", "construction_5yr", "postfirm_key"], "4_bg_risk_5yr_postfirm"),
    (["policy_year", "zip_key", "flood_zone_key", "construction_year"], "5_zip_zone_exact_year"),
    (["policy_year", "zip_key", "flood_risk_key", "construction_5yr", "postfirm_key"], "6_zip_risk_5yr_postfirm"),
    (["policy_year", "community_key", "flood_zone_key", "construction_year"], "7_community_zone_exact_year"),
    (["policy_year", "community_key", "flood_risk_key", "construction_5yr", "postfirm_key"], "8_community_risk_5yr_postfirm"),
    (["policy_year", "county_key", "flood_zone_key", "construction_year"], "9_county_zone_exact_year"),
    (["policy_year", "county_key", "flood_risk_key", "construction_5yr", "postfirm_key"], "10_county_risk_5yr_postfirm"),
    (["policy_year", "blockgroup_key", "construction_year"], "11_bg_exact_year"),
    (["policy_year", "zip_key", "construction_year"], "12_zip_exact_year"),
    (["policy_year", "county_key", "construction_year"], "13_county_exact_year"),
    (["policy_year", "county_key", "construction_5yr"], "14_county_5yr"),
    (["policy_year", "blockgroup_key", "flood_zone_key"], "15_bg_zone_no_construction"),
]


def q(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def use_code_filter(specification: str) -> str:
    if specification.strip().lower() == "all":
        return "TRUE"
    codes = tuple(code.strip() for code in specification.split(",") if code.strip())
    return "trim(cast(property_use_std as varchar)) in (" + ",".join(q(code) for code in codes) + ")"


def policy_cell_condition(n_alias: str = "n", p_alias: str = "p") -> str:
    """Whether an ATTOM property can share any current tier with an NFIP row."""
    alternatives = []
    for keys, _ in TIERS:
        match_keys = [key for key in keys if key != "policy_year"]
        alternatives.append("(" + " and ".join(
            f"{n_alias}.{key}={p_alias}.{key}" for key in match_keys
        ) + ")")
    # Wagner excludes a known house from years before it was built. Missing
    # construction year is allowed only through tier 15's key list.
    age = (f"({p_alias}.construction_year is null or "
           f"{p_alias}.construction_year<={n_alias}.policy_year)")
    return age + " and (" + " or ".join(alternatives) + ")"


def norm(series: pd.Series, width: int | None = None) -> pd.Series:
    out = series.astype("string").str.strip().str.upper().str.replace(r"\.0$", "", regex=True)
    out = out.replace({"": pd.NA, "<NA>": pd.NA, "NAN": pd.NA, "NONE": pd.NA})
    if width:
        numeric = out.str.fullmatch(r"\d+").fillna(False)
        out.loc[numeric] = out.loc[numeric].str.zfill(width)
    return out


def zone(series: pd.Series) -> pd.Series:
    out = norm(series).str.replace(" ", "", regex=False)
    out.loc[out.str.fullmatch(r"A\d{1,2}").fillna(False)] = "AE"
    out.loc[out.str.fullmatch(r"V\d{1,2}").fillna(False)] = "VE"
    return out


def risk(series: pd.Series) -> pd.Series:
    z = zone(series)
    out = pd.Series(pd.NA, index=z.index, dtype="string")
    out.loc[z.isin(["A", "AE", "AH", "AO", "V", "VE"])] = "high_risk"
    out.loc[z.isin(["B", "C", "D", "X", "XE"])] = "low_risk"
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state", required=True)
    p.add_argument("--nfip", required=True)
    p.add_argument("--attom", required=True)
    p.add_argument("--attom-nfhl-builty", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--cell-diagnostics", required=True)
    p.add_argument("--tier-diagnostics", required=True)
    p.add_argument("--tmp", required=True)
    p.add_argument("--memory", default="64GB")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--use-codes", default=",".join(SFR_CODES),
                   help="Comma-separated ATTOM property-use codes; use 'all' to disable.")
    return p.parse_args()


def load_nfip(path: str, state: str) -> pd.DataFrame:
    frame = pd.read_stata(path, convert_categoricals=False)
    frame.columns = [str(c).lower() for c in frame.columns]
    # Clean state panels call FEMA's post-FIRM construction indicator
    # ``postfirm``. Accept the raw FEMA name as an explicit alias, but never
    # manufacture this matching variable when neither field is present.
    if "postfirm" not in frame and "postfirmconstructionindicator" in frame:
        frame = frame.rename(columns={"postfirmconstructionindicator": "postfirm"})
    required = {
        "property_id", "policy_year", "construction_year", "postfirm",
        "censusblockgroupfips", "zipcode", "countycode", "nfipratedcommunitynumber",
        "ratedfloodzone",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(
            f"NFIP input {path} is missing {missing}. "
            "Use clean/nfip_policies_state/{state}.dta created by "
            "clean_nfip_policies.do; the property-level collapsed file is not "
            "the policy-year input required by this assignment. Available "
            f"columns: {sorted(frame.columns)}"
        )
    frame.insert(0, "_nfip_rowid", range(1, len(frame) + 1))
    frame["property_policy_id"] = (
        state.upper() + "_" + frame["_nfip_rowid"].astype(str)
    )
    frame["state"] = state.upper()
    frame["policy_year"] = pd.to_numeric(frame["policy_year"], errors="coerce").astype("Int64")
    frame["construction_year"] = pd.to_numeric(frame["construction_year"], errors="coerce").astype("Int64")
    frame["construction_5yr"] = (frame["construction_year"] // 5) * 5
    frame["construction_decade"] = (frame["construction_year"] // 10) * 10
    frame["postfirm_key"] = pd.to_numeric(frame["postfirm"], errors="coerce").astype("Int64")
    frame["blockgroup_key"] = norm(frame["censusblockgroupfips"], 12)
    frame["zip_key"] = norm(frame["zipcode"], 5)
    frame["county_key"] = norm(frame["countycode"], 5)
    frame["community_key"] = norm(frame["nfipratedcommunitynumber"], 6)
    frame["nfip_flood_zone_original"] = frame["ratedfloodzone"].astype("string")
    frame["flood_zone_key"] = zone(frame["ratedfloodzone"])
    frame["flood_risk_key"] = risk(frame["ratedfloodzone"])
    return frame


def apply_tier(con: duckdb.DuckDBPyConnection, keys: list[str], label: str, tier: int) -> dict:
    ks = ", ".join(keys)
    valid = " AND ".join(f"{k} IS NOT NULL" for k in keys)
    con.execute("DROP TABLE IF EXISTS cell_stats")
    con.execute(f"""
        CREATE TEMP TABLE cell_stats AS
        WITH n AS (
          SELECT {ks}, count(*) nfip_cell_n, count(distinct property_id) nfip_property_n
          FROM nfip WHERE nfip_attom_merge_status=1 AND {valid} GROUP BY {ks}
        ), a AS (
          SELECT {ks}, count(*) attom_cell_n,
                 sum(cast(builty_elevated=1 as integer)) builty_attom_cell_n
          FROM housing WHERE assigned=false AND {valid} GROUP BY {ks}
        )
        SELECT {ks}, n.nfip_cell_n, n.nfip_property_n, a.attom_cell_n,
               a.builty_attom_cell_n,
               least(n.nfip_cell_n,a.attom_cell_n) cell_capacity,
               n.nfip_cell_n*a.attom_cell_n cell_candidate_pairs,
               cast(n.nfip_cell_n=1 and a.attom_cell_n=1 as integer) cell_singleton,
               cast(a.attom_cell_n=1 as integer) cell_unique_attom,
               greatest(n.nfip_cell_n-a.attom_cell_n,0) cell_excess_nfip,
               greatest(a.attom_cell_n-n.nfip_cell_n,0) cell_excess_attom,
               md5(concat_ws('|',{q(label)},{','.join(f'cast({k} as varchar)' for k in keys)})) match_cell_id
        FROM n INNER JOIN a USING ({ks})
    """)
    diag = con.execute(
        f"SELECT {q(label)} match_tier, {tier} tier_number, * FROM cell_stats"
    ).fetchdf()
    con.execute("DROP TABLE IF EXISTS tier_hits")
    con.execute(f"""
        CREATE TEMP TABLE tier_hits AS
        WITH nr AS (
          SELECT n._nfip_rowid,{ks},s.* EXCLUDE({ks}),
                 row_number() over(partition by {ks}
                   order by md5(s.match_cell_id||'|nfip|fixed_seed|'||cast(n.property_policy_id as varchar))) cell_rank
          FROM nfip n INNER JOIN cell_stats s USING({ks})
          WHERE n.nfip_attom_merge_status=1 AND {valid}
        ), ar AS (
          SELECT a.* EXCLUDE({ks}),{ks},
                 a.policy_year assigned_policy_year,
                 a.flood_zone_key assigned_flood_zone_key,
                 a.flood_risk_key assigned_flood_risk_key,
                 row_number() over(partition by {ks}
                   order by a.builty_elevated desc,
                            md5(s.match_cell_id||'|attom|fixed_seed|'||a.attomid)) cell_rank
          FROM housing a INNER JOIN cell_stats s USING({ks})
          WHERE a.assigned=false AND {valid}
        )
        SELECT nr._nfip_rowid,nr.match_cell_id,nr.nfip_cell_n,nr.attom_cell_n,
               nr.nfip_property_n,nr.builty_attom_cell_n,nr.cell_singleton,
               nr.cell_rank nfip_cell_rank,ar.cell_rank attom_cell_rank,
               ar.* EXCLUDE({ks},cell_rank)
        FROM nr INNER JOIN ar USING({ks},cell_rank)
    """)
    assignments = [
        "nfip_attom_merge_status=3", f"match_tier={q(label)}", f"match_tier_number={tier}",
        "match_cell_id=h.match_cell_id", "assigned_attomid=h.attomid",
        "nfip_cell_rank=h.nfip_cell_rank", "attom_cell_rank=h.attom_cell_rank",
        "nfip_cell_n=h.nfip_cell_n", "attom_cell_n=h.attom_cell_n",
        "nfip_property_n=h.nfip_property_n", "builty_attom_cell_n=h.builty_attom_cell_n",
        "cell_singleton=h.cell_singleton",
        "assignment_method=case when h.cell_singleton=1 then 'singleton' else 'deterministic_hash_rank' end",
        "attom_flood_zone_original=h.attom_flood_zone_original",
        "attom_flood_zone_key=h.assigned_flood_zone_key", "attom_flood_risk_key=h.assigned_flood_risk_key",
        "attom_nfhl_flood_matched=h.nfhl_flood_matched",
        "attom_nfhl_community_matched=h.nfhl_community_matched",
        "builty_elevated=h.builty_elevated", "builty_elevation_year=h.builty_elevation_year",
        "builty_n_properties=h.builty_n_properties", "builty_merge_status=h.builty_merge_status",
        "builty_attom_match_tier=h.builty_attom_match_tier",
        "attom_value_year=h.attom_value_year", "attom_value_lag=h.attom_value_lag",
    ] + [f"attom_{c}=h.{c}" for c in VALUE_COLUMNS]
    con.execute(f"UPDATE nfip n SET {','.join(assignments)} FROM tier_hits h WHERE n._nfip_rowid=h._nfip_rowid")
    con.execute("UPDATE housing a SET assigned=true FROM tier_hits h WHERE a.attomid=h.attomid AND a.policy_year=h.assigned_policy_year")
    assigned = con.execute("SELECT count(*) FROM tier_hits").fetchone()[0]
    unmatched = con.execute("SELECT count(*) FROM nfip WHERE nfip_attom_merge_status=1").fetchone()[0]
    return {"cells": diag, "summary": {"match_tier": label, "tier_number": tier,
            "assignments": assigned, "unmatched_after_tier": unmatched}}


def main() -> None:
    a = parse_args(); state = a.state.lower()
    nfip = load_nfip(a.nfip, state)
    con = duckdb.connect(); Path(a.tmp).mkdir(parents=True, exist_ok=True)
    con.execute(f"set temp_directory={q(a.tmp)}"); con.execute(f"set memory_limit={q(a.memory)}")
    con.execute(f"set threads={a.threads}"); con.execute("set preserve_insertion_order=false")
    con.register("nfip_frame", nfip); con.execute("create table nfip as select * from nfip_frame")
    additions = {
      "nfip_attom_merge_status":"integer default 1", "match_tier":"varchar", "match_tier_number":"integer",
      "match_cell_id":"varchar", "assigned_attomid":"varchar", "assignment_method":"varchar",
      "nfip_cell_rank":"integer", "attom_cell_rank":"integer", "nfip_cell_n":"integer",
      "attom_cell_n":"integer", "nfip_property_n":"integer", "builty_attom_cell_n":"integer",
      "cell_singleton":"integer", "attom_flood_zone_original":"varchar", "attom_flood_zone_key":"varchar",
      "attom_flood_risk_key":"varchar", "attom_nfhl_flood_matched":"boolean",
      "attom_nfhl_community_matched":"boolean", "builty_elevated":"integer",
      "builty_elevation_year":"integer", "builty_n_properties":"integer", "builty_merge_status":"integer",
      "builty_attom_match_tier":"varchar", "attom_value_year":"integer", "attom_value_lag":"integer",
    }
    additions.update({f"attom_{c}":"double" for c in VALUE_COLUMNS})
    for name,typ in additions.items(): con.execute(f"alter table nfip add column {name} {typ}")
    attom=q(a.attom); enriched=q(a.attom_nfhl_builty)
    candidate_filter = use_code_filter(a.use_codes)
    any_cell = policy_cell_condition("n", "p")
    con.execute(f"""
      create table properties as
      with p as (
        select cast(attomid as varchar) attomid,
          max(nullif(trim(cast(zip as varchar)),'')) zip_key,
          max(nullif(trim(cast(countycode as varchar)),'')) county_key,
          max(nullif(trim(cast(censusblockgroupfips as varchar)),'')) blockgroup_key,
          max(cast(construction_year as integer)) construction_year,
          max(nullif(trim(cast(property_use_std as varchar)),'')) property_use_std
        from read_parquet({attom}) where {candidate_filter} group by 1
      ), e as (select * from read_parquet({enriched}))
      select p.*,
        case when regexp_matches(trim(cast(e.nfip_community_id as varchar)), '^[0-9]+(\\.0)?$')
             then lpad(regexp_extract(trim(cast(e.nfip_community_id as varchar)), '^(\\d+)', 1),6,'0')
             else nullif(upper(trim(cast(e.nfip_community_id as varchar))),'') end community_key,
        upper(replace(trim(cast(e.fld_zone as varchar)),' ','')) attom_flood_zone_original,
        case when regexp_matches(upper(replace(trim(cast(e.fld_zone as varchar)),' ','')),'^A[0-9]{{1,2}}$') then 'AE'
             when regexp_matches(upper(replace(trim(cast(e.fld_zone as varchar)),' ','')),'^V[0-9]{{1,2}}$') then 'VE'
             else nullif(upper(replace(trim(cast(e.fld_zone as varchar)),' ','')),'') end flood_zone_key,
        case when (case when regexp_matches(upper(replace(trim(cast(e.fld_zone as varchar)),' ','')),'^A[0-9]{{1,2}}$') then 'AE'
                             when regexp_matches(upper(replace(trim(cast(e.fld_zone as varchar)),' ','')),'^V[0-9]{{1,2}}$') then 'VE'
                             else nullif(upper(replace(trim(cast(e.fld_zone as varchar)),' ','')),'') end)
                       in ('A','AE','AH','AO','V','VE') then 'high_risk'
             when (case when regexp_matches(upper(replace(trim(cast(e.fld_zone as varchar)),' ','')),'^A[0-9]{{1,2}}$') then 'AE'
                             when regexp_matches(upper(replace(trim(cast(e.fld_zone as varchar)),' ','')),'^V[0-9]{{1,2}}$') then 'VE'
                             else nullif(upper(replace(trim(cast(e.fld_zone as varchar)),' ','')),'') end)
                       in ('B','C','D','X','XE') then 'low_risk' end flood_risk_key,
        (p.construction_year//5)*5 construction_5yr,(p.construction_year//10)*10 construction_decade,
        case when e.initial_firm_year is not null then cast(p.construction_year>=case when e.initial_firm_year<1975 then 1975 else e.initial_firm_year+1 end as integer) end postfirm_key,
        cast(e.nfhl_flood_matched as boolean) nfhl_flood_matched,
        cast(e.nfhl_community_matched as boolean) nfhl_community_matched,
        coalesce(cast(e.builty_elevated as integer),0) builty_elevated,
        cast(e.builty_elevation_year as integer) builty_elevation_year,
        coalesce(cast(e.builty_n_properties as integer),0) builty_n_properties,
        cast(e.builty_merge_status as integer) builty_merge_status,
        cast(e.builty_attom_match_tier as varchar) builty_attom_match_tier
      from p inner join e using(attomid)
    """)
    con.execute(f"""
      create table tax_values as select cast(attomid as varchar) attomid,cast(year as integer) tax_year,
        {','.join(f'max(cast({c} as double)) {c}' for c in VALUE_COLUMNS)}
      from read_parquet({attom}) where {candidate_filter} group by 1,2
    """)
    # Drop ATTOM properties that cannot enter any matching cell in any NFIP
    # policy year. This avoids expanding the full state ATTOM universe across
    # every year before the tier logic runs.
    con.execute(f"""
      create table eligible_properties as
      select p.* from properties p where exists (
        select 1 from nfip n where {any_cell}
      )
    """)
    con.execute(f"""
      create table housing as select p.*,y.policy_year,v.tax_year attom_value_year,
        y.policy_year-v.tax_year attom_value_lag,{','.join('v.'+c for c in VALUE_COLUMNS)},false assigned
      from eligible_properties p
      inner join (select distinct policy_year from nfip where policy_year is not null) y
        on exists (select 1 from nfip n where n.policy_year=y.policy_year and {any_cell})
      left join lateral (select * from tax_values v where v.attomid=p.attomid and v.tax_year<=y.policy_year order by tax_year desc limit 1) v on true
    """)
    cells=[]; summaries=[]
    for i,(keys,label) in enumerate(TIERS,1):
        result=apply_tier(con,keys,label,i); cells.append(result["cells"]); summaries.append(result["summary"])
        print(label,result["summary"]["assignments"],flush=True)
    out=Path(a.out); out.parent.mkdir(parents=True,exist_ok=True)
    con.execute(f"copy (select * exclude(_nfip_rowid) from nfip order by _nfip_rowid) to {q(out)} (format parquet,compression zstd)")
    Path(a.cell_diagnostics).parent.mkdir(parents=True,exist_ok=True)
    pd.concat(cells,ignore_index=True).to_csv(a.cell_diagnostics,index=False)
    tier_frame = pd.DataFrame(summaries)
    tier_frame["duckdb_version"] = duckdb.__version__
    tier_frame["ranking_method"] = "salted_md5"
    gaps = con.execute("""
      select count(*) nfip_rows,
        sum(cast(blockgroup_key is null as integer)) missing_blockgroup,
        sum(cast(zip_key is null as integer)) missing_zip,
        sum(cast(community_key is null as integer)) missing_community,
        sum(cast(construction_year is null as integer)) missing_construction_year,
        sum(cast(postfirm_key is null as integer)) missing_postfirm,
        sum(cast(community_key is null and postfirm_key is null as integer)) missing_community_and_postfirm,
        sum(cast(blockgroup_key is null and zip_key is null and community_key is null as integer)) missing_all_match_geographies
      from nfip
    """).fetchdf()
    for column in gaps.columns:
        tier_frame[column] = gaps.iloc[0][column]
    tier_frame.to_csv(a.tier_diagnostics,index=False)
    con.close()


if __name__ == "__main__":
    main()
