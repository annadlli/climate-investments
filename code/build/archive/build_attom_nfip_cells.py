"""
Build ATTOM cells at the finest geography NFIP exposes.

Authors: Anna Li
Date: 2026-07-08

Description:
    NFIP policy files do not contain street addresses, so ATTOM cannot be joined
    to NFIP by parcel/address. After geocoding ATTOM addresses to Census block
    groups with geocode_attom_blockgroups.py, this script aggregates ATTOM
    assessor records to the closest NFIP-observable cells:

        census block group x construction-year/bin x tax-year

    Those cells can then be merged onto NFIP policy/property rows by
    censusblockgroupfips, construction_year, and policy_year (as-of tax year).
    This supports property-level NFIP panels in the sense that every NFIP
    property_id can receive ATTOM cell values, but it is still a cell-level
    value merge because NFIP has no exact address.

    Input  : {data}/{state}/attom_{state}.parquet   (cluster) or
             {data}/raw/attom/attom_{state}.parquet or {data}/attom_{state}.parquet
             {data}/build/{state}_attom_blockgroups.parquet or .dta
    Output : {data}/build/{state}_attom_nfip_blockgroup_{grain}.dta
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


GRAINS = {
    "constr_year":   ["censusblockgroupfips", "construction_year"],
    "constr_5yr":    ["censusblockgroupfips", "construction_5yr"],
    "constr_decade": ["censusblockgroupfips", "construction_decade"],
    "year":          ["censusblockgroupfips"],
}


def quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Aggregate geocoded ATTOM records to NFIP-observable block-group cells."
    )
    p.add_argument("--state", required=True)
    p.add_argument("--data", required=True)
    p.add_argument(
        "--blockgroups",
        default=None,
        help="Optional geocoded ATTOM block-group crosswalk path for pilot runs.",
    )
    p.add_argument(
        "--output-tag",
        default=None,
        help="Optional output prefix tag. Defaults to the state abbreviation.",
    )
    p.add_argument("--tmp", default="/tmp")
    p.add_argument("--threads", default=4, type=int)
    p.add_argument("--memory", default="32GB")
    p.add_argument("--max-temp", default="800GB")
    return p.parse_args()


def resolve_parquet(data: Path, state: str) -> Path:
    candidates = [
        data / state / f"attom_{state}.parquet",
        data / "raw" / "attom" / f"attom_{state}.parquet",
        data / f"attom_{state}.parquet",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(f"No ATTOM parquet for {state} under {data}")


def resolve_blockgroups(data: Path, state: str) -> Path:
    candidates = [
        data / "build" / f"{state}_attom_blockgroups.parquet",
        data / "build" / f"{state}_attom_blockgroups.dta",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(
        f"No geocoded ATTOM block-group crosswalk for {state}; "
        "run geocode_attom_blockgroups.py first."
    )


def load_blockgroups(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        bg = pd.read_parquet(path)
    elif path.suffix.lower() == ".dta":
        bg = pd.read_stata(path, preserve_dtypes=False)
    else:
        raise ValueError(f"Unsupported block-group crosswalk format: {path}")

    required = {"attomid", "censusblockgroupfips"}
    missing = required.difference(bg.columns)
    if missing:
        raise KeyError(f"Block-group crosswalk missing columns: {sorted(missing)}")

    bg = bg[["attomid", "censusblockgroupfips"]].copy()
    bg["attomid"] = bg["attomid"].astype("string")
    bg["censusblockgroupfips"] = (
        bg["censusblockgroupfips"].astype("string").fillna("").str.strip()
    )
    bg = bg[bg["censusblockgroupfips"].str.len() == 12]
    return bg.drop_duplicates("attomid")


def main() -> None:
    args = parse_args()
    state = args.state.lower()
    data = Path(args.data)
    attom_path = resolve_parquet(data, state)
    bg_path = Path(args.blockgroups) if args.blockgroups else resolve_blockgroups(data, state)
    output_tag = (args.output_tag or state).lower()
    out_dir = data / "build"
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"Reading ATTOM: {attom_path}")
    print(f"Reading geocoded block groups: {bg_path}")
    blockgroups = load_blockgroups(bg_path)
    print(f"Geocoded ATTOM properties with valid block group: {len(blockgroups):,}")

    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote(args.tmp)}")
    con.execute(f"SET memory_limit={quote(args.memory)}")
    con.execute(f"SET max_temp_directory_size={quote(args.max_temp)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")
    con.register("attom_bg", blockgroups)
    con.execute(f"CREATE VIEW attom_raw AS SELECT * FROM read_parquet({quote(str(attom_path))})")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE by_year AS
        SELECT
            bg.censusblockgroupfips,
            regexp_extract(trim(cast(a.PROPERTYADDRESSZIP AS varchar)), '^(\\d{5})', 1) AS zip_key,
            lpad(regexp_extract(trim(cast(a.SITUSSTATECOUNTYFIPS AS varchar)), '(\\d+)', 1), 5, '0') AS countycode,
            cast(a.TAXYEARASSESSED AS integer) AS year,
            CASE WHEN cast(coalesce(nullif(a.YEARBUILT, 0), nullif(a.YEARBUILTEFFECTIVE, 0)) AS integer)
                      BETWEEN 1700 AND 2027
                 THEN cast(coalesce(nullif(a.YEARBUILT, 0), nullif(a.YEARBUILTEFFECTIVE, 0)) AS integer)
            END AS construction_year,
            cast(a.ATTOMID AS varchar) AS attomid,
            cast(a.TAXMARKETVALUETOTAL AS double) AS market_total
        FROM attom_raw AS a
        INNER JOIN attom_bg AS bg
            ON cast(a.ATTOMID AS varchar) = bg.attomid
        WHERE cast(a.TAXYEARASSESSED AS integer) BETWEEN 1980 AND 2035
          AND cast(a.TAXMARKETVALUETOTAL AS double) > 0
          AND bg.censusblockgroupfips != ''
    """)
    con.execute("ALTER TABLE by_year ADD COLUMN construction_5yr INTEGER")
    con.execute("ALTER TABLE by_year ADD COLUMN construction_decade INTEGER")
    con.execute("UPDATE by_year SET construction_5yr    = (construction_year // 5)  * 5")
    con.execute("UPDATE by_year SET construction_decade = (construction_year // 10) * 10")

    n = con.execute("SELECT count(*) FROM by_year").fetchone()[0]
    print(f"ATTOM property-year rows available for NFIP cells: {n:,}")

    for grain, keys in GRAINS.items():
        gk = keys + ["year"]
        key_sql = ", ".join(gk)
        notnull = " AND ".join(f"{k} IS NOT NULL" for k in gk)
        df = con.execute(f"""
            SELECT {key_sql},
                count(DISTINCT attomid) AS attom_n_properties,
                avg(market_total)       AS attom_mean_market_total,
                median(market_total)    AS attom_median_market_total
            FROM by_year
            WHERE {notnull}
              AND censusblockgroupfips != ''
            GROUP BY {key_sql}
        """).df()
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].fillna("").astype(str)
        out = out_dir / f"{output_tag}_attom_nfip_blockgroup_{grain}.dta"
        df.to_stata(out, write_index=False, version=118)
        print(f"Saved blockgroup_{grain:13} {len(df):>9,} cells -> {out.name}")

    con.close()


if __name__ == "__main__":
    main()
