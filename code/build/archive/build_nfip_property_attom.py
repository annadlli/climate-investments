"""Assign canonical NFIP properties to ATTOM properties and write final links.

The input is clean/nfip_policies_property.dta (one row per inferred NFIP
property, with the same global property_id used by analysis.dta). Community is
recovered from the state policy file using the original property-defining keys.
Assignments are deterministic and one-to-one across successively broader cells.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


VALUE_COLUMNS = ["market_value_total", "market_value_land", "market_value_improvements",
                 "assessed_value_total", "assessed_value_improvements",
                 "previous_assessed_value", "last_sale_price"]


def q(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def norm(series: pd.Series, width: int) -> pd.Series:
    value = series.astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
    numeric = value.str.fullmatch(r"\d+").fillna(False)
    value.loc[numeric] = value.loc[numeric].str.zfill(width)
    return value.replace({"": pd.NA, "<NA>": pd.NA})


def risk(series: pd.Series) -> pd.Series:
    zone = series.astype("string").str.upper().str.replace(" ", "", regex=False)
    zone = zone.mask(zone.str.fullmatch(r"A\d{1,2}").fillna(False), "AE")
    zone = zone.mask(zone.str.fullmatch(r"V\d{1,2}").fillna(False), "VE")
    out = pd.Series(pd.NA, index=series.index, dtype="string")
    out.loc[zone.isin(["A", "AE", "AH", "AO", "V", "VE"])] = "high_risk"
    out.loc[zone.isin(["B", "C", "D", "X", "XE"])] = "low_risk"
    return out


def args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--properties", required=True)
    p.add_argument("--state-policies", required=True)
    p.add_argument("--attom", required=True)
    p.add_argument("--nfhl", required=True)
    p.add_argument("--state", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--diagnostics", required=True)
    p.add_argument("--tmp", required=True)
    p.add_argument("--memory", default="24GB")
    p.add_argument("--threads", type=int, default=4)
    return p.parse_args()


def recover_community(properties: pd.DataFrame, policy_path: Path) -> pd.DataFrame:
    policy = pd.read_stata(policy_path, convert_categoricals=False,
                           columns=["property_id", "policy_year", "nfipratedcommunitynumber"])
    latest = (policy.sort_values(["property_id", "policy_year"])
              .drop_duplicates("property_id", keep="last"))
    # Within a state, canonical property_id values retain the state-file order.
    # Validate the rank crosswalk with the exact property count before using it.
    local_ids = sorted(latest["property_id"].unique())
    global_ids = sorted(properties["property_id"].unique())
    if len(local_ids) != len(global_ids):
        raise ValueError(f"Property count mismatch: state={len(local_ids):,}, canonical={len(global_ids):,}")
    crosswalk = pd.DataFrame({"property_id_local": local_ids, "property_id": global_ids})
    latest = latest.rename(columns={"property_id": "property_id_local"}).merge(
        crosswalk, on="property_id_local", validate="one_to_one"
    )
    latest["community_key"] = norm(latest["nfipratedcommunitynumber"], 6)
    return properties.merge(latest[["property_id", "community_key"]], on="property_id",
                            how="left", validate="one_to_one")


def apply(con: duckdb.DuckDBPyConnection, keys: list[str], label: str) -> tuple[int, dict]:
    ks = ", ".join(keys); valid = " AND ".join(f"{x} IS NOT NULL" for x in keys)
    diag = con.execute(f"""
        WITH n AS (SELECT {ks}, count(*) np FROM nfip WHERE attomid IS NULL AND {valid} GROUP BY {ks}),
             a AS (SELECT {ks}, count(*) na FROM attom WHERE assigned=false AND {valid} GROUP BY {ks}),
             c AS (SELECT n.np,a.na FROM n INNER JOIN a USING ({ks}))
        SELECT count(*),sum(np),sum(na),avg(na),median(na),quantile_cont(na,.9),max(na),
               sum((na=1)::integer) FROM c
    """).fetchone()
    con.execute("DROP TABLE IF EXISTS hits")
    con.execute(f"""
        CREATE TEMP TABLE hits AS
        WITH n AS (SELECT property_id,{ks},row_number() over(partition by {ks} order by property_id) r
                   FROM nfip WHERE attomid IS NULL AND {valid}),
             a AS (SELECT attomid,{ks},
                          row_number() over(partition by {ks} order by attomid) r
                   FROM attom WHERE assigned=false AND {valid})
        SELECT n.property_id,a.* EXCLUDE({ks},r) FROM n INNER JOIN a USING({ks},r)
    """)
    count = con.execute("select count(*) from hits").fetchone()[0]
    sets = [f"match_tier={q(label)}", "attomid=h.attomid",
            "builty_elevated=h.builty_elevated",
            "builty_elevation_year=h.builty_elevation_year",
            "builty_n_properties=h.builty_n_properties"]
    con.execute(f"UPDATE nfip n SET {','.join(sets)} FROM hits h WHERE n.property_id=h.property_id")
    con.execute("UPDATE attom a SET assigned=true FROM hits h WHERE a.attomid=h.attomid")
    names = ["cells", "nfip_properties_in_cells", "attom_properties_in_cells", "mean_properties_per_cell",
             "median_properties_per_cell", "p90_properties_per_cell", "maximum_properties_per_cell", "singleton_cells"]
    row = {"tier": label, **dict(zip(names, diag)), "assignments": count}
    row["singleton_cell_share"] = row["singleton_cells"] / row["cells"] if row["cells"] else pd.NA
    row["unmatched_after_tier"] = con.execute("select count(*) from nfip where attomid is null").fetchone()[0]
    print(f"{label:<40} {count:>10,}")
    return count, row


def main() -> None:
    a = args(); state = a.state.upper(); st = state.lower()
    columns = ["property_id", "state", "zipcode", "censusblockgroupfips", "construction_year",
               "ratedfloodzone", "postfirm", "policy_year_init"]
    properties = pd.read_stata(a.properties, convert_categoricals=False, columns=columns)
    properties = properties[properties["state"].astype(str).str.upper().eq(state)].copy()
    properties = recover_community(properties, Path(a.state_policies))
    properties["zip_key"] = norm(properties["zipcode"], 5)
    properties["blockgroup_key"] = norm(properties["censusblockgroupfips"], 12)
    properties["construction_year"] = pd.to_numeric(properties["construction_year"], errors="coerce").astype("Int64")
    properties["reference_year"] = pd.to_numeric(properties["policy_year_init"], errors="coerce").astype("Int64")
    properties["flood_risk_key"] = risk(properties["ratedfloodzone"])
    properties["postfirm_key"] = pd.to_numeric(properties["postfirm"], errors="coerce").astype("Int64")
    properties["construction_5yr"] = (properties["construction_year"] // 5) * 5
    properties["construction_decade"] = (properties["construction_year"] // 10) * 10

    con = duckdb.connect(); Path(a.tmp).mkdir(parents=True, exist_ok=True)
    con.execute(f"set temp_directory={q(a.tmp)}"); con.execute(f"set memory_limit={q(a.memory)}")
    con.execute(f"set threads={a.threads}"); con.register("property_frame", properties)
    con.execute("create table nfip as select * from property_frame")
    con.execute("alter table nfip add column match_tier varchar"); con.execute("alter table nfip add column attomid varchar")
    con.execute("alter table nfip add column attom_value_year integer"); con.execute("alter table nfip add column attom_value_lag integer")
    con.execute("alter table nfip add column builty_elevated integer default 0")
    con.execute("alter table nfip add column builty_elevation_year integer")
    con.execute("alter table nfip add column builty_n_properties integer default 0")
    for x in VALUE_COLUMNS: con.execute(f"alter table nfip add column attom_{x} double")

    con.execute(f"""
        CREATE TABLE attom AS
        WITH p AS (
          SELECT cast(attomid as varchar) attomid,max(cast(construction_year as integer)) construction_year,
                 max(nullif(trim(cast(zip as varchar)),'')) zip_key,
                 max(nullif(trim(cast(censusblockgroupfips as varchar)),'')) blockgroup_key
          FROM read_parquet({q(a.attom)}) GROUP BY 1
        ), z AS (
          SELECT cast(attomid as varchar) attomid,upper(trim(cast(nfip_community_id as varchar))) community_key,
                 CASE WHEN upper(replace(trim(cast(fld_zone as varchar)),' ','')) IN ('A','AE','AH','AO','V','VE')
                      OR regexp_matches(upper(replace(trim(cast(fld_zone as varchar)),' ','')),'^[AV][0-9]{{1,2}}$')
                      THEN 'high_risk' ELSE 'low_risk' END flood_risk_key,
                 cast(initial_firm_year as integer) firm_year,
                 coalesce(cast(builty_elevated as integer),0) builty_elevated,
                 cast(builty_elevation_year as integer) builty_elevation_year,
                 coalesce(cast(builty_n_properties as integer),0) builty_n_properties
          FROM read_parquet({q(a.nfhl)}) WHERE nfhl_flood_matched AND nfhl_community_matched
        ), base AS (
          SELECT p.*,nullif(z.community_key,'') community_key,z.flood_risk_key,
                 z.builty_elevated,z.builty_elevation_year,z.builty_n_properties,
                 (p.construction_year//5)*5 construction_5yr,(p.construction_year//10)*10 construction_decade,
                 cast(p.construction_year>=case when z.firm_year<1975 then 1975 else z.firm_year+1 end as integer) postfirm_key
          FROM p INNER JOIN z USING(attomid) WHERE p.construction_year between 1700 and 2027
        )
        SELECT b.*,false assigned FROM base b
    """)
    tiers = [
      (["blockgroup_key","construction_year","flood_risk_key"],"0_blockgroup_exact_year_risk"),
      (["zip_key","construction_year","flood_risk_key"],"1_zip_exact_year_risk"),
      (["community_key","construction_year","flood_risk_key"],"2_community_exact_year_risk"),
      (["community_key","construction_5yr","flood_risk_key","postfirm_key"],"3_community_5yr_risk_postfirm"),
      (["community_key","construction_decade","flood_risk_key","postfirm_key"],"4_community_decade_risk_postfirm")]
    rows=[]
    for keys,label in tiers: rows.append(apply(con,keys,label)[1])
    con.execute(f"""
      CREATE TABLE final AS SELECT n.* EXCLUDE(attom_value_year,attom_value_lag,{','.join('attom_'+x for x in VALUE_COLUMNS)}),
        v.value_year attom_value_year,n.reference_year-v.value_year attom_value_lag,
        {','.join('v.'+x+' attom_'+x for x in VALUE_COLUMNS)}
      FROM nfip n LEFT JOIN LATERAL (
        SELECT cast(year as integer) value_year,{','.join(VALUE_COLUMNS)} FROM read_parquet({q(a.attom)}) v
        WHERE cast(v.attomid as varchar)=n.attomid AND cast(v.year as integer)<=n.reference_year
        ORDER BY cast(v.year as integer) DESC LIMIT 1) v ON true
    """)
    out=Path(a.out); out.parent.mkdir(parents=True,exist_ok=True)
    con.execute(f"copy (select * from final order by property_id) to {q(out)} (format parquet,compression zstd)")
    pd.DataFrame(rows).to_csv(a.diagnostics,index=False); con.close()
    print(f"Saved {out}"); print(f"Saved {a.diagnostics}")


if __name__ == "__main__": main()
