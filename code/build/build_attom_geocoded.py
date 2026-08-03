"""
Build the property-level geocoded ATTOM panel: characteristics + block groups.

Authors: Anna Li
Original Date: 2026-07-15
Revised Date: 2026-08-03 (merged the block-group fan-out and ATTOM join steps)

Description:
    Step 2 in the ATTOM block-group geocode pipeline (geocode_attom.py is
    step 1)

      A. Merge the addrid -> block group link from the geocode step
         back onto the ATTOMID -> addrid crosswalk, so every property carries
         the block group of its (shared) address. Saves
         {state}_attom_blockgroups.dta/.parquet (one row per ATTOMID) - the
         input that build_attom_nfip_cells.py aggregates to NFIP merge cells.

      B. Join. Joins that block-group link back onto the raw ATTOM parquet by
         ATTOMID and saves a property x tax-year panel: address, assessed/market
         values, and property characteristics from ATTOM, plus the block group,
         coordinates, and match flag. Every property is kept (left join);
         unmatched ones carry an empty block-group code.

"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


def quote(s: str) -> str:
    # sql-safe strings
    return "'" + s.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    # state, data, sample, duckdb info
    p = argparse.ArgumentParser(description="Fan block groups to ATTOMIDs and join onto ATTOM.")
    p.add_argument("--state",  required=True)
    p.add_argument("--data",   required=True, help="Data root (from master.do).")
    p.add_argument("--sample", default=0, type=int, help="Pilot: use the sample outputs of this size.")
    p.add_argument("--memory", default="24GB", help="DuckDB memory cap; keep below the SLURM allocation.")
    p.add_argument("--tmp",    default=None, help="DuckDB spill directory (defaults inside the work dir).")
    return p.parse_args()


def resolve_parquet(data: Path, state: str) -> Path:
    # find the attom parquet for this state
    for c in [data / state / f"attom_{state}.parquet",
              data / "raw" / "attom" / f"attom_{state}.parquet",
              data / f"attom_{state}.parquet"]:
        if c.exists():
            return c
    return data / state / f"attom_{state}.parquet"


# ---------------------------------------------------------------------------
# phase a: fan block groups to attomids
# ---------------------------------------------------------------------------
def fan_blockgroups(work: Path, out_base: Path) -> None:
    # geocode file + attom crosswalk
    xwalk_file = work / "attomid_xwalk.parquet"
    link_file  = work / "blockgroups_by_address.parquet"

    xwalk = pd.read_parquet(xwalk_file)
    link  = pd.read_parquet(link_file)

    # attach bg to each matched attomid
    out_df = xwalk.merge(link, on="addrid", how="left")
    out_df = out_df.drop(columns=["addrid"])

    # save the fan-out file
    out_df.to_parquet(f"{out_base}.parquet", index=False)
    out_df.to_stata(f"{out_base}.dta", write_index=False, version=118)


# ---------------------------------------------------------------------------
# phase b: join block groups onto attom records
# ---------------------------------------------------------------------------
def join_attom(parquet: Path, link_file: Path, out: Path, sample: int, memory: str, tmp: Path) -> None:
    # duckdb for the big join
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit={quote(memory)}")
    con.execute(f"SET temp_directory={quote(str(tmp))}")
    con.execute("SET preserve_insertion_order=false")

    # full run keeps all rows; sample version is lighter
    join_type = "INNER JOIN" if sample > 0 else "LEFT JOIN"

    con.execute(f"""
        COPY (
            WITH per_year AS (
                -- collapse duplicate assessor rows
                SELECT
                    cast(ATTOMID AS varchar)                        AS attomid,
                    cast(TAXYEARASSESSED AS integer)                AS year,
                    max(trim(cast(PROPERTYADDRESSFULL AS varchar)))  AS property_address_full,
                    max(cast(TAXMARKETVALUETOTAL       AS double))  AS market_value_total,
                    max(cast(TAXMARKETVALUELAND        AS double))  AS market_value_land,
                    max(cast(TAXMARKETVALUEIMPROVEMENTS AS double)) AS market_value_improvements,
                    max(cast(TAXASSESSEDVALUETOTAL     AS double))  AS assessed_value_total,
                    max(cast(TAXASSESSEDVALUEIMPROVEMENTS AS double)) AS assessed_value_improvements,
                    max(cast(PREVIOUSASSESSEDVALUE     AS double))  AS previous_assessed_value,
                    max(CASE WHEN cast(coalesce(nullif(YEARBUILT, 0), nullif(YEARBUILTEFFECTIVE, 0)) AS integer)
                                  BETWEEN 1700 AND 2027
                             THEN cast(coalesce(nullif(YEARBUILT, 0), nullif(YEARBUILTEFFECTIVE, 0)) AS integer)
                        END)                                        AS construction_year,
                    max(trim(cast(PROPERTYUSESTANDARDIZED AS varchar))) AS property_use_std,
                    max(cast(AREABUILDING  AS double))              AS area_building,
                    max(cast(AREALOTACRES  AS double))              AS area_lot_acres,
                    max(cast(BEDROOMSCOUNT AS double))              AS bedrooms,
                    max(cast(BATHCOUNT     AS double))              AS baths,
                    max(cast(DEEDLASTSALEPRICE AS double))          AS last_sale_price,
                    max(trim(cast(DEEDLASTSALEDATE AS varchar)))    AS last_sale_date,
                    max(regexp_extract(trim(cast(PROPERTYADDRESSZIP AS varchar)), '^(\\d{{5}})', 1)) AS zip,
                    max(lpad(regexp_extract(trim(cast(SITUSSTATECOUNTYFIPS AS varchar)), '(\\d+)', 1), 5, '0')) AS countycode
                FROM read_parquet({quote(str(parquet))})
                WHERE TAXYEARASSESSED IS NOT NULL
                GROUP BY 1, 2
            )
            SELECT
                p.*,
                coalesce(b.censusblockgroupfips, '') AS censusblockgroupfips,
                coalesce(b.match, '')                AS geocode_match,
                b.longitude, b.latitude
            FROM per_year p
            {join_type} read_parquet({quote(str(link_file))}) b USING (attomid)
        ) TO {quote(str(out))} (FORMAT parquet)
    """)

    # final panel for downstream work
    con.close()


def main() -> None:
    # entry point for the attom blockgroup build
    args    = parse_args()
    state   = args.state.lower()
    data    = Path(args.data)
    parquet = resolve_parquet(data, state)

    tag  = f"{state}_sample{args.sample}" if args.sample > 0 else state
    work = data / "build" / "attom_geocode" / f"{tag}_addr"

    # build the fan-out file
    blockgroups_base = data / "build" / f"{tag}_attom_blockgroups"
    fan_blockgroups(work, blockgroups_base)

    # attach block groups to the attom panel
    tmp = Path(args.tmp) if args.tmp else work / "duckdb_tmp"
    join_attom(parquet, Path(f"{blockgroups_base}.parquet"),
               data / "build" / f"{tag}_attom_geocoded.parquet",
               args.sample, args.memory, tmp)


if __name__ == "__main__":
    main()
