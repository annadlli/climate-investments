"""
Authors: Anna Li
Original Date: 2026-07-15
Revised Date: 2026-08-16

Builds the geocoded ATTOM panel: one row per property per tax year, carrying
the property's characteristics, its values, and the census block group that
geocode_attom.py found for its address.

1.spread the address-level block groups back out to properties, since many properties share an address. 
2. Map that onto the raw ATTOM records and write the panel the rest of the pipeline reads.
Properties the geocoder could not place are kept with a blank block group.

Revised all paths to refer to the Dropbox data structure.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

# pull the leading digits out of the raw ATTOM zip field, whatever type it arrived as
ZIP_DIGITS = "regexp_extract(trim(cast(PROPERTYADDRESSZIP AS varchar)), '^(\\d+)', 1)"

# turn those digits into a real 5-digit zip: blank out the junk values, trim zip+4
# down to 5, and pad the New England zips whose leading zero got lost somewhere upstream (error caught by Claude)
ZIP_CLEAN = f"""
    CASE WHEN {ZIP_DIGITS} IN ('', '0', '00000') THEN NULL
         WHEN length({ZIP_DIGITS}) >= 5 THEN substr({ZIP_DIGITS}, 1, 5)
         ELSE lpad({ZIP_DIGITS}, 5, '0') END
"""

# ATTOM records the build year in two places and uses 0 for "we don't know"
YEAR_BUILT = "cast(coalesce(nullif(YEARBUILT, 0), nullif(YEARBUILTEFFECTIVE, 0)) AS integer)"


def quote(s: str) -> str:
    # escape single quotes so a path or setting can be dropped straight into SQL
    return "'" + s.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    # which state to build, where the data lives, and how much room DuckDB gets
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state",  required=True)
    p.add_argument("--data",   required=True, help="Data root (from master.do).")
    p.add_argument("--sample", default=0, type=int, help="Pilot: use the sample outputs of this size.")
    p.add_argument("--memory", default="24GB", help="DuckDB memory cap; keep below the SLURM allocation.")
    p.add_argument("--tmp",    default=None, help="DuckDB spill directory (defaults inside the work dir).")
    p.add_argument("--out",    default=None, help="Output geocoded ATTOM parquet path.")
    p.add_argument("--blockgroups-out", default=None,
                   help="Output basename for the ATTOMID/block-group crosswalk ")
    return p.parse_args()


def resolve_parquet(data: Path, state: str) -> Path:
    # One fixed location. The old version searched three candidate paths and, if
    # none existed, returned the first anyway -- so a missing file surfaced later
    # as an opaque DuckDB read error instead of here.
    parquet = data / "raw" / "attom" / f"attom_{state}.parquet"
    if not parquet.exists():
        raise FileNotFoundError(f"No ATTOM parquet for {state}: {parquet}")
    return parquet


def fan_blockgroups(work: Path, out_base: Path) -> None:
    # geocode_attom.py gave us block groups per unique address; the crosswalk says
    # which properties sit at which address, so join them to get block groups per property
    xwalk = pd.read_parquet(work / "attomid_xwalk.parquet")
    link = pd.read_parquet(work / "blockgroups_by_address.parquet")

    properties = xwalk.merge(link, on="addrid", how="left").drop(columns=["addrid"])

    # Parquet feeds the Python pipeline. Stata is only for final files, not now due to data size
    properties.to_parquet(f"{out_base}.parquet", index=False)


def join_attom(parquet: Path, link_file: Path, out: Path, sample: int, memory: str, tmp: Path) -> None:
    # this join runs over tens of millions of rows, so hand it to DuckDB on disk
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit={quote(memory)}")
    con.execute(f"SET temp_directory={quote(str(tmp))}")
    con.execute("SET preserve_insertion_order=false")

    # a full run keeps every property; a pilot run only wants the sampled ones (aka the 5 states)
    join_type = "INNER JOIN" if sample > 0 else "LEFT JOIN"

    con.execute(f"""
        COPY (
            WITH per_year AS (
                -- ATTOM ships several assessor rows per property-year, so take the
                -- best value of each field and collapse down to one row per year
                SELECT
                    cast(ATTOMID AS varchar)                        AS attomid,
                    cast(TAXYEARASSESSED AS integer)                AS year,
                    max(trim(cast(PROPERTYADDRESSFULL AS varchar))) AS property_address_full,
                    max(cast(TAXMARKETVALUETOTAL        AS double)) AS market_value_total,
                    max(cast(TAXMARKETVALUELAND         AS double)) AS market_value_land,
                    max(cast(TAXMARKETVALUEIMPROVEMENTS AS double)) AS market_value_improvements,
                    max(cast(TAXASSESSEDVALUETOTAL      AS double)) AS assessed_value_total,
                    max(cast(TAXASSESSEDVALUEIMPROVEMENTS AS double)) AS assessed_value_improvements,
                    -- treat a wild build year as missing rather than letting it through
                    max(CASE WHEN {YEAR_BUILT} BETWEEN 1700 AND 2027
                             THEN {YEAR_BUILT} END)                 AS construction_year,
                    max(trim(cast(PROPERTYUSESTANDARDIZED AS varchar))) AS property_use_std,
                    max(cast(AREABUILDING  AS double))              AS area_building,
                    max(cast(AREALOTACRES  AS double))              AS area_lot_acres,
                    max(cast(BEDROOMSCOUNT AS double))              AS bedrooms,
                    max(cast(BATHCOUNT     AS double))              AS baths,
                    -- the DEEDLASTSALE fields are empty in our licensed extract
                    -- (0 nonnull of 43.8M LA rows), so use the assessor ones instead
                    max(cast(ASSESSORLASTSALEAMOUNT AS double))     AS last_sale_price,
                    max(trim(cast(ASSESSORLASTSALEDATE AS varchar))) AS last_sale_date,
                    max({ZIP_CLEAN})                                AS zip,
                    max(lpad(regexp_extract(trim(cast(SITUSSTATECOUNTYFIPS AS varchar)),
                                            '(\\d+)', 1), 5, '0'))  AS countycode
                FROM read_parquet({quote(str(parquet))})
                WHERE TAXYEARASSESSED IS NOT NULL
                GROUP BY 1, 2
            ), with_prior AS (
                -- PREVIOUSASSESSEDVALUE is empty in our extract, so build it ourselves
                -- from the previous year we actually observe for the same property.
                -- Stepping over observed years means a skipped assessment carries
                -- forward instead of coming back missing.
                SELECT p.*,
                       lag(assessed_value_total) OVER (PARTITION BY attomid ORDER BY year)
                           AS previous_assessed_value,
                       lag(year) OVER (PARTITION BY attomid ORDER BY year)
                           AS previous_assessed_year
                FROM per_year p
                WHERE year BETWEEN 1980 AND 2035
            )
            -- finally attach the block group and coordinates to every property-year
            SELECT p.*,
                   coalesce(b.censusblockgroupfips, '') AS censusblockgroupfips,
                   coalesce(b.match, '') AS geocode_match,
                   b.longitude, b.latitude
            FROM with_prior p
            {join_type} read_parquet({quote(str(link_file))}) b USING (attomid)
        ) TO {quote(str(out))} (FORMAT parquet)
    """)
    con.close()


def main() -> None:
    args = parse_args()
    state = args.state.lower()
    data = Path(args.data)
    parquet = resolve_parquet(data, state)

    # pilot runs write to their own tagged files so they cannot clobber a real build
    tag = f"{state}_sample{args.sample}" if args.sample > 0 else state
    work = data / "build" / "attom_geocode" / f"{tag}_addr"

    # step one: block groups per property
    blockgroups_base = (Path(args.blockgroups_out) if args.blockgroups_out else
                        data / "build" / f"{tag}_attom_blockgroups")
    blockgroups_base.parent.mkdir(parents=True, exist_ok=True)
    fan_blockgroups(work, blockgroups_base)

    # step two: the property x tax-year panel everything downstream reads
    output = Path(args.out) if args.out else data / "build" / f"{tag}_attom_geocoded.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(args.tmp) if args.tmp else work / "duckdb_tmp"
    join_attom(parquet, Path(f"{blockgroups_base}.parquet"), output, args.sample, args.memory, tmp)


if __name__ == "__main__":
    main()
