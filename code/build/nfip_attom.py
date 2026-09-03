"""
Authors: Anna Li
Original Date: 2026-08-14
Revised Date: 2026-08-16

Pairs each NFIP-insured property with one ATTOM property, so the insurance
records pick up a property value and a Builty elevation flag.

NFIP is redacted: no street address, and coordinates rounded to about a tenth
of a degree. So no real address match -- follows Wagner instead, matching
inside a cell of properties that look alike. The pairing
is one-to-one and deterministic, which means the result merges straight onto
the property-level analysis file.

Matching happens once, at each property's first observed policy year. The
crosswalk that comes out has no time dimension, so a property-year panel gets
rebuilt afterwards by joining ATTOM values on (attomid, year).

Note: this is not cumulative. Cumulative was done only for diagnostics. 
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

# Wagner's high/low split. anything on neither list stays missing, not low risk
HIGH_RISK = ("A", "AE", "AH", "AO", "V", "VE")
LOW_RISK = ("B", "C", "D", "X", "XE")

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

# columns a match fills in, and their types
ASSIGNMENT_COLUMNS = {
    "nfip_attom_merge_status": "integer default 1", "match_tier": "varchar",
    "match_tier_number": "integer", "match_cell_id": "varchar",
    "assigned_attomid": "varchar", "assignment_method": "varchar",
    "nfip_cell_rank": "integer", "attom_cell_rank": "integer",
    "nfip_cell_n": "integer", "attom_cell_n": "integer",
    "builty_attom_cell_n": "integer", "cell_singleton": "integer",
    "attom_flood_zone_original": "varchar", "attom_flood_zone_key": "varchar",
    "attom_flood_risk_key": "varchar", "attom_nfhl_flood_matched": "boolean",
    "attom_property_use_std": "varchar",
    "attom_nfhl_community_matched": "boolean", "builty_elevated": "integer",
    "builty_elevation_year": "integer", "builty_n_properties": "integer",
    "builty_merge_status": "integer", "builty_attom_match_tier": "varchar",
    "attom_value_year": "integer", "attom_value_lag": "integer",
    **{f"attom_{c}": "double" for c in VALUE_COLUMNS},
}

# same tidy-up as zone(), in SQL for the ATTOM side.
# goes in as a value, so single braces are fine in the f-strings
ZONE_SQL = """
    CASE WHEN regexp_matches(upper(replace(trim(cast(fld_zone AS varchar)),' ','')),'^A[0-9]{1,2}$') THEN 'AE'
         WHEN regexp_matches(upper(replace(trim(cast(fld_zone AS varchar)),' ','')),'^V[0-9]{1,2}$') THEN 'VE'
         ELSE nullif(upper(replace(trim(cast(fld_zone AS varchar)),' ','')),'') END
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
    # old maps number the zones (A7, V12); newer ones just say AE and VE
    out = norm(series).str.replace(" ", "", regex=False)
    out.loc[out.str.fullmatch(r"A\d{1,2}").fillna(False)] = "AE"
    out.loc[out.str.fullmatch(r"V\d{1,2}").fillna(False)] = "VE"
    return out


def risk(series: pd.Series) -> pd.Series:
    # high/low buckets; oddities like AR stay missing
    z = zone(series)
    out = pd.Series(pd.NA, index=z.index, dtype="string")
    out.loc[z.isin(HIGH_RISK)] = "high_risk"
    out.loc[z.isin(LOW_RISK)] = "low_risk"
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state", required=True)
    p.add_argument("--properties", required=True, help="clean/nfip_policies_property.dta")
    p.add_argument("--state-policies", required=True,
                   help="Policy panel fallback for property files that predate retained *_init keys.")
    p.add_argument("--attom", required=True, help="{state}_attom_geocoded.parquet")
    p.add_argument("--attom-nfhl-builty", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--tmp", required=True)
    p.add_argument("--use-codes", default="376,380,382,383,385,386",
                   help="ATTOM property_use_std codes eligible as candidates. "
                        "Default is the six agreed single-family codes; use 'all' to disable.")
    p.add_argument("--add-tier-15", action="store_true",
                   help="Add the block group x flood zone fallback that ignores construction "
                        "year, and let year-less ATTOM properties into the pool to reach it.")
    p.add_argument("--snapshot-mode", choices=["initial", "nfhl"], default="initial",
                   help="Use the initial NFIP policy or the policy closest to the NFHL vintage.")
    p.add_argument("--nfhl-snapshot-year", type=int,
                   help="Required with --snapshot-mode nfhl.")
    p.add_argument("--memory", default="64GB")
    p.add_argument("--threads", type=int, default=4)
    return p.parse_args()


def initial_policy_snapshot(frame: pd.DataFrame, state_policies: str) -> pd.DataFrame:
    # older property files lack the *_init columns -- rebuild from each
    # property's first policy year
    columns = ["property_id", "policy_year", "zipcode", "censusblockgroupfips",
               "flood_zone", "post_firm", "countycode", "nfipratedcommunitynumber"]
    policy = pd.read_stata(state_policies, convert_categoricals=False, columns=columns)
    initial = (policy.sort_values(["property_id", "policy_year"])
               .drop_duplicates("property_id", keep="first")
               .rename(columns={"property_id": "property_id_state",
                                "policy_year": "policy_year_init_check"}))

    # property file has global IDs, policy panel has state ones.
    # both sort the same way, so pair by position
    base = ["property_id", "state", "construction_year", "policy_year_init"]
    if "property_id_state" not in frame:
        frame = frame.merge(
            pd.DataFrame({"property_id_state": sorted(initial["property_id_state"].unique()),
                          "property_id": sorted(frame["property_id"].unique())}),
            on="property_id", validate="one_to_one")
    frame = frame[base + ["property_id_state"]].merge(
        initial, on="property_id_state", how="left", validate="one_to_one")
    return frame.drop(columns="policy_year_init_check")


def load_properties(path: str, state_policies: str, state: str,
                    snapshot_mode: str = "initial",
                    nfhl_snapshot_year: int | None = None) -> pd.DataFrame:
    # one row per NFIP property, as at its first policy year
    if snapshot_mode == "nfhl":
        if nfhl_snapshot_year is None:
            raise ValueError("--nfhl-snapshot-year is required for NFHL snapshot matching")
        columns = ["property_id", "policy_year", "construction_year", "zipcode",
                   "censusblockgroupfips", "flood_zone", "post_firm",
                   "countycode", "nfipratedcommunitynumber"]
        frame = pd.read_stata(state_policies, convert_categoricals=False, columns=columns)
        frame.columns = [str(c).lower() for c in frame.columns]
        frame["policy_year"] = pd.to_numeric(frame["policy_year"], errors="coerce")
        frame["policy_year_init"] = frame.groupby("property_id")["policy_year"].transform("min")
        frame["snapshot_year_gap"] = (frame["policy_year"] - nfhl_snapshot_year).abs()
        # earlier year wins an equal-distance tie
        frame = (frame.sort_values(["property_id", "snapshot_year_gap", "policy_year"])
                 .drop_duplicates("property_id", keep="first")
                 .rename(columns={"property_id": "property_id_state",
                                  "policy_year": "matching_policy_year"}))
        frame["state"] = state.upper()
        frame["property_id"] = (
            state.upper() + "_" + frame["property_id_state"].astype("string")
            .str.replace(r"\.0$", "", regex=True)
        )
        frame["nfhl_snapshot_year"] = nfhl_snapshot_year
    else:
        frame = pd.read_stata(path, convert_categoricals=False)
        frame.columns = [str(c).lower() for c in frame.columns]
        frame = frame.loc[frame["state"].astype(str).str.upper().eq(state.upper())].copy()

        # prefer the *_init snapshot from prep_nfip_policies.do
        base = ["property_id", "state", "construction_year", "policy_year_init"]
        init_map = {"zipcode_init": "zipcode",
                    "censusblockgroupfips_init": "censusblockgroupfips",
                    "flood_zone_init": "flood_zone",
                    "post_firm_init": "post_firm",
                    "countycode_init": "countycode",
                    "nfipratedcommunitynumber_init": "nfipratedcommunitynumber"}
        if all(c in frame.columns for c in init_map):
            keep = base + (["property_id_state"] if "property_id_state" in frame else []) + list(init_map)
            frame = frame[keep].rename(columns=init_map)
        else:
            frame = initial_policy_snapshot(frame, state_policies)
        frame["matching_policy_year"] = frame["policy_year_init"]
        frame["nfhl_snapshot_year"] = pd.NA
        frame["snapshot_year_gap"] = pd.NA

    # one row per property, or nothing downstream is 1:1
    if frame["property_id"].duplicated().any():
        raise ValueError(f"{state}: property_id is not unique in the NFIP property base")

    # every tier key, same shape as the ATTOM side
    frame["zip_key"] = norm(frame["zipcode"], 5)
    frame["county_key"] = norm(frame["countycode"], 5)
    frame["blockgroup_key"] = norm(frame["censusblockgroupfips"], 12)
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
                 max(nullif(trim(cast(censusblockgroupfips AS varchar)),'')) blockgroup_key
          FROM read_parquet({q(attom)})
          WHERE {use_code_filter(use_codes)}
          GROUP BY 1
        ), e AS (SELECT * FROM read_parquet({q(enriched)}))
        SELECT p.*,
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
          coalesce(cast(e.builty_elevated AS integer),0) builty_elevated,
          cast(e.builty_elevation_year AS integer) builty_elevation_year,
          coalesce(cast(e.builty_n_properties AS integer),0) builty_n_properties,
          cast(e.builty_merge_status AS integer) builty_merge_status,
          cast(e.builty_attom_match_tier AS varchar) builty_attom_match_tier,
          false assigned
        FROM p INNER JOIN e USING(attomid)
        WHERE {year_filter}
    """)


def apply_tier(con: duckdb.DuckDBPyConnection, keys: list[str], label: str, tier: int) -> None:
    ks = ", ".join(keys)
    valid = " AND ".join(f"{k} IS NOT NULL" for k in keys)

    # cell sizes on both sides, before this tier takes anything.
    # inner join keeps only cells with an unmatched NFIP property and a free ATTOM one
    con.execute("DROP TABLE IF EXISTS cell_stats")
    con.execute(f"""
        CREATE TEMP TABLE cell_stats AS
        WITH n AS (
          SELECT {ks}, count(*) nfip_cell_n FROM nfip
          WHERE nfip_attom_merge_status=1 AND {valid} GROUP BY {ks}
        ), a AS (
          SELECT {ks}, count(*) attom_cell_n, sum((builty_elevated=1)::integer) builty_attom_cell_n
          FROM attom WHERE assigned=false AND {valid} GROUP BY {ks}
        )
        SELECT n.*, a.attom_cell_n, a.builty_attom_cell_n,
               {q(label)}||'|'||md5(concat_ws('|',{ks})) match_cell_id,
               (n.nfip_cell_n=1 AND a.attom_cell_n=1)::integer cell_singleton
        FROM n INNER JOIN a USING({ks})
    """)

    # inside a cell, rank both sides and pair by position. order is a hash of
    # cell id + property id: reproducible, unrelated to sort order, different
    # salt per side. Builty houses ranked first so they win their slot
    con.execute("DROP TABLE IF EXISTS tier_hits")
    con.execute(f"""
        CREATE TEMP TABLE tier_hits AS
        WITH nr AS (
          SELECT n.property_id, {ks}, s.match_cell_id, s.nfip_cell_n, s.attom_cell_n,
                 s.builty_attom_cell_n, s.cell_singleton,
                 row_number() over(partition by {ks}
                   order by md5(s.match_cell_id||'|nfip|fixed_seed|'||cast(n.property_id as varchar))) cell_rank
          FROM nfip n INNER JOIN cell_stats s USING({ks})
          WHERE n.nfip_attom_merge_status=1 AND {valid}
        ), ar AS (
          SELECT a.* EXCLUDE({ks}), {ks},
                 a.flood_zone_key assigned_flood_zone_key,
                 a.flood_risk_key assigned_flood_risk_key,
                 row_number() over(partition by {ks}
                   order by a.builty_elevated desc,
                            md5(s.match_cell_id||'|attom|fixed_seed|'||a.attomid)) cell_rank
          FROM attom a INNER JOIN cell_stats s USING({ks})
          WHERE a.assigned=false AND {valid}
        )
        SELECT nr.property_id, nr.match_cell_id, nr.nfip_cell_n, nr.attom_cell_n,
               nr.builty_attom_cell_n, nr.cell_singleton,
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
        "builty_attom_cell_n=h.builty_attom_cell_n", "cell_singleton=h.cell_singleton",
        "assignment_method=case when h.cell_singleton=1 then 'singleton' else 'deterministic_hash_rank' end",
        "attom_flood_zone_original=h.attom_flood_zone_original",
        "attom_flood_zone_key=h.assigned_flood_zone_key",
        "attom_flood_risk_key=h.assigned_flood_risk_key",
        "attom_property_use_std=h.property_use_std",
        "attom_nfhl_flood_matched=h.nfhl_flood_matched",
        "attom_nfhl_community_matched=h.nfhl_community_matched",
        "builty_elevated=h.builty_elevated", "builty_elevation_year=h.builty_elevation_year",
        "builty_n_properties=h.builty_n_properties", "builty_merge_status=h.builty_merge_status",
        "builty_attom_match_tier=h.builty_attom_match_tier",
    ]
    con.execute(f"UPDATE nfip n SET {','.join(assignments)} FROM tier_hits h WHERE n.property_id=h.property_id")

    # mark those ATTOMIDs used, so later tiers can't reuse them
    con.execute("UPDATE attom a SET assigned=true FROM tier_hits h WHERE a.attomid=h.attomid")

    # Report progress; diagnostic tables are generated separately.
    assigned = con.execute("SELECT count(*) FROM tier_hits").fetchone()[0]
    print(f"{label:<38} {assigned:>10,}")


def attach_values(con: duckdb.DuckDBPyConnection, attom: str) -> None:
    # value each property: latest ATTOM assessment at or before its matched
    # policy year. unmatched come back blank
    con.execute(f"""
        CREATE TABLE final AS
        SELECT n.* EXCLUDE(attom_value_year, attom_value_lag,
                           {','.join('attom_'+c for c in VALUE_COLUMNS)}),
               coalesce(n.builty_elevation_year, n.reference_year) value_reference_year,
               v.value_year attom_value_year,
               coalesce(n.builty_elevation_year, n.reference_year) - v.value_year attom_value_lag,
               {','.join('v.'+c+' attom_'+c for c in VALUE_COLUMNS)}
        FROM nfip n LEFT JOIN LATERAL (
          SELECT cast(year AS integer) value_year, {','.join(VALUE_COLUMNS)}
          FROM read_parquet({q(attom)}) v
          WHERE cast(v.attomid AS varchar)=n.assigned_attomid
            AND cast(v.year AS integer) BETWEEN 1980 AND 2035
            AND cast(v.year AS integer)<=coalesce(n.builty_elevation_year, n.reference_year)
          ORDER BY cast(v.year AS integer) DESC LIMIT 1
        ) v ON true
    """)


def main() -> None:
    args = parse_args()
    state = args.state.upper()

    # NFIP properties for this state
    properties = load_properties(
        args.properties, args.state_policies, state,
        snapshot_mode=args.snapshot_mode,
        nfhl_snapshot_year=args.nfhl_snapshot_year,
    )
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
    build_attom(con, args.attom, args.attom_nfhl_builty, args.use_codes,
                allow_missing_construction=args.add_tier_15)
    print(f"{state}: {con.execute('SELECT count(*) FROM attom').fetchone()[0]:,} "
          f"ATTOM candidates (use_codes={args.use_codes})")

    # down the ladder; each tier only sees leftovers
    tiers = TIERS + [TIER_15] if args.add_tier_15 else list(TIERS)
    for number, (keys, label) in enumerate(tiers, start=1):
        apply_tier(con, keys, label, number)

    attach_values(con, args.attom)

    out = Path(args.out)
    con.execute(f"COPY (SELECT * FROM final ORDER BY property_id) TO {q(out)} (FORMAT PARQUET, COMPRESSION ZSTD)")

    con.close()
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
