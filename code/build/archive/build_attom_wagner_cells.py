"""
Build Wagner-grain ATTOM value cells for the NFIP<->ATTOM merge.

Authors: Anna Li
Date: 2026-07-06

Description:
    Aggregates the raw ATTOM parquet to small per-cell value files so the actual
    NFIP<->ATTOM merge (build_wagnermerge.do) can run in Stata on tiny inputs --
    the 41GB parquet never has to leave the cluster / be local.

    Eight grains are written, matching the tiers in build_wagnermerge.do:
        zip / county  x  {construction-year, construction-5yr, construction-decade}  x  tax-year
        zip / county  x  tax-year                                     (pure-geography backstop)

    Tax year (TAXYEARASSESSED) is the time dimension; the merge matches it as-of
    the NFIP policy year. Construction year (YEARBUILT, effective-year fallback)
    is guarded to the plausible range NFIP uses (1700-2027) so junk vintages do
    not pollute the 5-yr/decade cell means.

    Input  : {data}/{state}/attom_{state}.parquet   (cluster) or
             {data}/raw/attom/attom_{state}.parquet  (local)
    Output : {data}/build/{state}_attom_wagner_{grain}.dta  (8 files, a few MB each)
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


# Grain -> cell key columns. Column names match the NFIP side in build_wagnermerge.do.
GRAINS = {
    "zip_constr_year":      ["zip_key", "construction_year"],
    "zip_constr_5yr":       ["zip_key", "construction_5yr"],
    "zip_constr_decade":    ["zip_key", "construction_decade"],
    "county_constr_year":   ["countycode", "construction_year"],
    "county_constr_5yr":    ["countycode", "construction_5yr"],
    "county_constr_decade": ["countycode", "construction_decade"],
    "zip_year":             ["zip_key"],
    "county_year":          ["countycode"],
}


# Quote string values before interpolating them into DuckDB SQL commands.
def quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--state",   required=True)
    p.add_argument("--data",    required=True)
    p.add_argument("--tmp",      default="/tmp")
    p.add_argument("--threads",  default=4, type=int)
    p.add_argument("--memory",   default="32GB")
    p.add_argument("--max-temp", default="800GB")
    return p.parse_args()


def resolve_parquet(data: Path, state: str) -> Path:
    # Cluster layout first ({data}/{state}/...), then local ({data}/raw/attom/...).
    candidates = [
        data / state / f"attom_{state}.parquet",
        data / "raw" / "attom" / f"attom_{state}.parquet",
    ]
    return next((p for p in candidates if p.exists()), candidates[0])


def main() -> None:
    args = parse_args()
    state = args.state.lower()
    data = Path(args.data)
    in_path = resolve_parquet(data, state)
    out_dir = data / "build"
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote(args.tmp)}")
    con.execute(f"SET memory_limit={quote(args.memory)}")
    con.execute(f"SET max_temp_directory_size={quote(args.max_temp)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")

    print(f"Reading ATTOM: {in_path}")
    con.execute(f"CREATE VIEW attom_raw AS SELECT * FROM read_parquet({quote(str(in_path))})")

    # Property-year records: five-digit ZIP/county, tax year, guarded construction year.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE by_year AS
        SELECT
            regexp_extract(trim(cast(PROPERTYADDRESSZIP       AS varchar)), '^(\\d{5})', 1) AS zip_key,
            lpad(regexp_extract(trim(cast(SITUSSTATECOUNTYFIPS AS varchar)), '(\\d+)',   1), 5, '0') AS countycode,
            cast(TAXYEARASSESSED AS integer) AS year,
            CASE WHEN cast(coalesce(nullif(YEARBUILT, 0), nullif(YEARBUILTEFFECTIVE, 0)) AS integer)
                      BETWEEN 1700 AND 2027
                 THEN cast(coalesce(nullif(YEARBUILT, 0), nullif(YEARBUILTEFFECTIVE, 0)) AS integer)
            END AS construction_year,
            ATTOMID,
            cast(TAXMARKETVALUETOTAL AS double) AS market_total
        FROM attom_raw
        WHERE cast(TAXYEARASSESSED AS integer) BETWEEN 1980 AND 2035
          AND cast(TAXMARKETVALUETOTAL AS double) > 0
    """)
    # Coarser construction bins (floor division; DuckDB / is true division).
    con.execute("ALTER TABLE by_year ADD COLUMN construction_5yr INTEGER")
    con.execute("ALTER TABLE by_year ADD COLUMN construction_decade INTEGER")
    con.execute("UPDATE by_year SET construction_5yr    = (construction_year // 5)  * 5")
    con.execute("UPDATE by_year SET construction_decade = (construction_year // 10) * 10")

    n = con.execute("SELECT count(*) FROM by_year").fetchone()[0]
    print(f"by_year rows: {n:,}")

    # One small value-cell file per grain. Every grain groups by its keys + tax year.
    for grain, keys in GRAINS.items():
        gk = keys + ["year"]
        key_sql = ", ".join(gk)
        notnull = " AND ".join(f"{k} IS NOT NULL" for k in gk)
        if "zip_key" in keys:
            notnull += " AND zip_key != ''"
        if "countycode" in keys:
            notnull += " AND countycode != ''"
        df = con.execute(f"""
            SELECT {key_sql},
                count(DISTINCT ATTOMID) AS attom_n_properties,
                avg(market_total)       AS attom_mean_market_total,
                median(market_total)    AS attom_median_market_total
            FROM by_year
            WHERE {notnull}
            GROUP BY {key_sql}
        """).df()
        for c in df.select_dtypes(include=["object"]).columns:
            df[c] = df[c].fillna("").astype(str)
        out = out_dir / f"{state}_attom_wagner_{grain}.dta"
        df.to_stata(out, write_index=False, version=118)
        print(f"Saved {grain:22} {len(df):>9,} cells -> {out.name}")

    con.close()


if __name__ == "__main__":
    main()
