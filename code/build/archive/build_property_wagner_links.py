"""
Build stable Wagner-style NFIP-property links to individual ATTOM properties.

Authors: Anna Li
Date: 2026-08-04

NFIP has no address.  This script creates one row per NFIP property and pairs it
one-to-one with one ATTOM property by deterministic sequence number within a
tiered geography/construction-year cell.  Once an ATTOMID is assigned, ATTOM
values are selected as of the NFIP reference year and address-matched Builty
permits attach exactly through that same ATTOMID.

This is an imputed Wagner-style property link, not an observed parcel match.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


TIERS = [
    ("blockgroup_construction_year", ["bg_key", "construction_year"]),
    ("blockgroup_construction_5yr", ["bg_key", "construction_5yr"]),
    ("blockgroup_construction_decade", ["bg_key", "construction_decade"]),
    ("blockgroup", ["bg_key"]),
    ("zip_construction_decade", ["zip_key", "construction_decade"]),
    ("zip", ["zip_key"]),
    ("county_construction_decade", ["county_key", "construction_decade"]),
    ("county", ["county_key"]),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="One-to-one Wagner NFIP--ATTOM property linkage.")
    p.add_argument("--state", required=True)
    p.add_argument("--data", required=True)
    p.add_argument("--nfip", default=None)
    p.add_argument("--attom", default=None)
    p.add_argument("--builty", default=None)
    p.add_argument("--out", default=None)
    p.add_argument("--diagnostics", default=None)
    p.add_argument("--bg-vintage", choices=["current", "2010"], default="current")
    p.add_argument("--use-codes", default="385")
    p.add_argument("--no-dta", action="store_true")
    p.add_argument("--tmp", default="/tmp")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--memory", default="64GB")
    p.add_argument("--max-temp", default="800GB")
    return p.parse_args()


def q(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def digits(col: str) -> str:
    return f"regexp_replace(cast({col} AS varchar), '[^0-9]', '', 'g')"


def padded(col: str, width: int) -> str:
    d = digits(col)
    return f"CASE WHEN length({d}) BETWEEN {width-1} AND {width} THEN lpad({d},{width},'0') ELSE '' END"


def configure(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> None:
    Path(args.tmp).mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory={q(args.tmp)}")
    con.execute(f"SET memory_limit={q(args.memory)}")
    con.execute(f"SET max_temp_directory_size={q(args.max_temp)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")


def load_nfip(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    raw = pd.read_stata(path, convert_categoricals=False)
    required = {"property_id", "policy_year", "construction_year", "censusblockgroupfips", "zipcode", "countycode"}
    missing = required.difference(raw.columns)
    if missing:
        raise KeyError(f"NFIP input missing {sorted(missing)}")
    con.register("nfip_raw", raw)
    con.execute(f"""
        CREATE TABLE nfip_panel AS
        SELECT *,
               {padded('censusblockgroupfips',12)} bg_key,
               {padded('zipcode',5)} zip_key,
               {padded('countycode',5)} county_key
        FROM nfip_raw
    """)
    con.unregister("nfip_raw")
    # Stable property row. Reference values at the observed elevation year where
    # possible; otherwise at the first policy year.
    con.execute("""
        CREATE TABLE nfip_property AS
        WITH timing AS (
          SELECT property_id,
                 min(policy_year) first_policy_year,
                 min(policy_year) FILTER (WHERE elevated = 1) first_elevated_year
          FROM nfip_panel GROUP BY property_id
        ), picked AS (
          SELECT n.*, t.first_policy_year, t.first_elevated_year,
                 coalesce(t.first_elevated_year, t.first_policy_year) reference_year,
                 row_number() OVER (PARTITION BY n.property_id ORDER BY
                   abs(n.policy_year-coalesce(t.first_elevated_year,t.first_policy_year)), n.policy_year) rn
          FROM nfip_panel n JOIN timing t USING (property_id)
        )
        SELECT row_number() OVER () _id, property_id, reference_year,
               bg_key, zip_key, county_key, construction_year,
               (construction_year // 5)*5 construction_5yr,
               (construction_year // 10)*10 construction_decade
        FROM picked WHERE rn=1
    """)
    return con.execute("SELECT count(*) FROM nfip_property").fetchone()[0]


def load_attom(con: duckdb.DuckDBPyConnection, path: Path, vintage: str, use_codes: str) -> int:
    bg = "censusblockgroupfips2010" if vintage == "2010" else "censusblockgroupfips"
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({q(path)})").fetchall()}
    if bg not in cols:
        raise KeyError(f"ATTOM input has no {bg}; choose another --bg-vintage")
    if use_codes.lower() == "all":
        use_filter = "TRUE"
    else:
        codes = ",".join(q(x.strip()) for x in use_codes.split(",") if x.strip())
        use_filter = f"trim(coalesce(property_use_std,'')) IN ({codes})"
    con.execute(f"""
        CREATE TABLE attom_panel AS
        SELECT cast(attomid AS varchar) attomid, year,
               {padded(bg,12)} bg_key, {padded('zip',5)} zip_key,
               {padded('countycode',5)} county_key, construction_year,
               (construction_year // 5)*5 construction_5yr,
               (construction_year // 10)*10 construction_decade,
               market_value_total, market_value_land, market_value_improvements,
               assessed_value_total, assessed_value_improvements,
               previous_assessed_value, property_use_std, area_building,
               area_lot_acres, bedrooms, baths, last_sale_price, last_sale_date,
               longitude, latitude, geocode_match, property_address_full
        FROM read_parquet({q(path)})
        WHERE year BETWEEN 1980 AND 2035 AND {use_filter}
    """)
    # Use the most recent nonempty geography/construction record as the stable
    # identity keys. Values remain year-specific and attach later as-of.
    con.execute("""
        CREATE TABLE attom_property AS
        SELECT attomid,
               arg_max(bg_key, year) bg_key, arg_max(zip_key, year) zip_key,
               arg_max(county_key, year) county_key,
               arg_max(construction_year, year) construction_year,
               (arg_max(construction_year, year) // 5)*5 construction_5yr,
               (arg_max(construction_year, year) // 10)*10 construction_decade
        FROM attom_panel GROUP BY attomid
    """)
    return con.execute("SELECT count(*) FROM attom_property").fetchone()[0]


def apply_tier(con: duckdb.DuckDBPyConnection, label: str, keys: list[str]) -> int:
    parts = ",".join(keys)
    join = " AND ".join(f"n.{k}=a.{k}" for k in keys)
    valid_n = " AND ".join(f"n.{k} IS NOT NULL" + (f" AND n.{k}!=''" if k.endswith("_key") else "") for k in keys)
    valid_a = " AND ".join(f"a.{k} IS NOT NULL" + (f" AND a.{k}!=''" if k.endswith("_key") else "") for k in keys)
    con.execute(f"""
        INSERT INTO links
        WITH nr AS (
          SELECT n._id, row_number() OVER (PARTITION BY {parts} ORDER BY cast(property_id AS varchar), _id) seq
          FROM nfip_property n WHERE {valid_n}
            AND NOT EXISTS (SELECT 1 FROM links l WHERE l._id=n._id)
        ), ar AS (
          SELECT a.attomid, {parts}, row_number() OVER (PARTITION BY {parts} ORDER BY attomid) seq
          FROM attom_property a WHERE {valid_a}
            AND NOT EXISTS (SELECT 1 FROM links l WHERE l.attomid=a.attomid)
        )
        SELECT nr._id, a.attomid, {q(label)} FROM nr
        JOIN nfip_property n USING (_id)
        JOIN ar a ON {join} AND nr.seq=a.seq
    """)
    return con.execute(f"SELECT count(*) FROM links WHERE wagner_tier={q(label)}").fetchone()[0]


def main() -> None:
    args = parse_args(); state = args.state.lower(); data = Path(args.data)
    nfip = Path(args.nfip) if args.nfip else data/"clean"/"nfip_policies_state"/f"{state}.dta"
    attom = Path(args.attom) if args.attom else data/"build"/f"{state}_attom_geocoded.parquet"
    builty = Path(args.builty) if args.builty else data/"build"/f"{state}_attom_permits.parquet"
    out = Path(args.out) if args.out else data/"build"/f"{state}_property_wagner_links.parquet"
    diag = Path(args.diagnostics) if args.diagnostics else out.with_name(f"{state}_property_wagner_tiers.csv")
    out.parent.mkdir(parents=True,exist_ok=True)
    con=duckdb.connect(); configure(con,args)
    nn=load_nfip(con,nfip); na=load_attom(con,attom,args.bg_vintage,args.use_codes)
    print(f"{state.upper()}: {nn:,} NFIP properties; {na:,} ATTOM properties")
    con.execute("CREATE TABLE links(_id BIGINT, attomid VARCHAR, wagner_tier VARCHAR)")
    for label,keys in TIERS:
        hit=apply_tier(con,label,keys); total=con.execute("SELECT count(*) FROM links").fetchone()[0]
        print(f"  {label:<34} {hit:>9,} (cumulative {total:,}/{nn:,})")
    # Select the assigned property's closest prior ATTOM assessment, otherwise
    # its earliest later assessment.
    con.execute("""
        CREATE TABLE linked_values AS
        SELECT n.property_id, n.reference_year, l.attomid, coalesce(l.wagner_tier,'unmatched') wagner_tier,
               a.* EXCLUDE(attomid),
               CASE WHEN a.year<=n.reference_year THEN 'prior' ELSE 'post' END attom_value_asof
        FROM nfip_property n JOIN links l USING (_id) JOIN attom_panel a USING(attomid)
        QUALIFY row_number() OVER(PARTITION BY n._id ORDER BY (a.year<=n.reference_year) DESC,
          CASE WHEN a.year<=n.reference_year THEN -a.year ELSE a.year END)=1
    """)
    # Address-matched Builty permits attach exactly through the assigned ATTOMID.
    builty_cols = (
        {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet({q(builty)})").fetchall()}
        if builty.exists() else set()
    )
    if {"ATTOMID", "permit_year", "attom_match_tier"}.issubset(builty_cols):
        con.execute(f"""
            CREATE TABLE builty_event AS
            SELECT cast(ATTOMID AS varchar) attomid, min(permit_year) builty_elevation_year,
                   count(*) builty_n_permits,
                   min(attom_match_tier) builty_attom_match_tier
            FROM read_parquet({q(builty)})
            WHERE attom_match_tier!='unmatched' AND ATTOMID IS NOT NULL
            GROUP BY cast(ATTOMID AS varchar)
        """)
        builty_join="LEFT JOIN builty_event b USING(attomid)"
        builty_available = 1
    else:
        print(f"NOTE: no usable Builty--ATTOM rows at {builty}; Builty fields will be empty")
        con.execute("CREATE TABLE builty_event(attomid VARCHAR,builty_elevation_year INTEGER,builty_n_permits BIGINT,builty_attom_match_tier VARCHAR)")
        builty_join="LEFT JOIN builty_event b USING(attomid)"
        builty_available = 0
    con.execute(f"""
        CREATE TABLE final AS SELECT {q(state.upper())} state, v.*,
          {builty_available}::INTEGER builty_data_available,
          CASE WHEN {builty_available}=1 THEN (b.attomid IS NOT NULL)::INTEGER END builty_elevated_wagner,
          b.builty_elevation_year,
          CASE WHEN {builty_available}=1 THEN coalesce(b.builty_n_permits,0) END builty_n_permits,
          b.builty_attom_match_tier
        FROM linked_values v {builty_join}
    """)
    con.execute(f"COPY final TO {q(out)} (FORMAT PARQUET,COMPRESSION ZSTD)")
    d=con.execute("SELECT wagner_tier,count(*) n FROM final GROUP BY 1 ORDER BY n DESC").df()
    n_linked = int(d["n"].sum())
    if n_linked < nn:
        d = pd.concat([d, pd.DataFrame([{"wagner_tier": "unmatched", "n": nn - n_linked}])], ignore_index=True)
    d["share"] = d["n"] / nn
    d.to_csv(diag,index=False)
    print(d.to_string(index=False)); print(f"Saved: {out}\nSaved: {diag}")
    if not args.no_dta:
        frame=con.execute("SELECT * FROM final").df()
        for c in frame.select_dtypes(include=['object']).columns: frame[c]=frame[c].fillna('').astype(str)
        frame.to_stata(out.with_suffix('.dta'),write_index=False,version=118)
        print(f"Saved: {out.with_suffix('.dta')}")
    con.close()


if __name__ == "__main__": main()
