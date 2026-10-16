"""
Authors: Anna Li
Date: 2026-09-13

Claude change 09-13: new script (issue #26, Louisiana construction year).

Louisiana only. Two parts plus a comparison; run all, or one with --check / --match / --compare.

Run: python nfip_attom_la_noyear.py --data <root> --attom-nfhl-builty <la_attom_builty.parquet>
     [--baseline <la_nfip_attom_property.parquet>] [--check | --match | --compare]

1. Check. Where the ATTOM year built is missing in Louisiana: single-family
   ATTOM properties without a year by parish, against the NFIP properties and
   the Builty-elevated houses in the same parish. Says whether the gap sits in
   the parishes that carry the grants and permits (Jefferson, St. Tammany,
   Terrebonne, ...) or is spread evenly.

2. Match. The proposed ladder fix: the no-year block group x flood zone tier
   (tier 15 in nfip_attom.py) runs right after the last tier that still keys on
   a flood zone (tier 10), ahead of the four tiers that drop the zone (11-14).
   An ATTOM house with no year but the right block group and zone then beats a
   county-wide match on year. Tier numbers keep their nfip_attom.py meaning, so
   match_tier_number is comparable across the two ladders. Output goes to a
   separate file; nothing on the production path changes. With --baseline, the
   baseline link file is compared: assignments that changed, and Builty
   retrofits by tier under each ladder.

Reuses nfip_attom.py's functions; the inputs are the same as step 4 of
slurm/run_property_matching.sh.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from nfip_attom import (ASSIGNMENT_COLUMNS, TIER_15, TIERS, apply_tier, attach_values,  # noqa: E402
                        build_attom, load_properties, q)

SF_USE_CODES = "376,380,382,383,385,386"
STATE = "LA"

# the proposed ladder: zone-keyed tiers, then the no-year block group x zone tier, then the rest
LADDER = [(keys, label, number) for number, (keys, label) in enumerate(TIERS, start=1)]
LADDER = LADDER[:10] + [(TIER_15[0], TIER_15[1], 15)] + LADDER[10:]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data", required=True, help="Data root (from master.do).")
    p.add_argument("--attom-nfhl-builty", required=True,
                   help="la_attom_builty.parquet from step 3 (carries builty_elevated per ATTOM property)")
    p.add_argument("--out", default=None,
                   help="Output link file (default: build/nfip_attom_pipeline_v2/nfip_attom_property/la_nfip_attom_property_noyear.parquet)")
    p.add_argument("--baseline", default=None,
                   help="Baseline la_nfip_attom_property.parquet to compare against (optional)")
    p.add_argument("--check", action="store_true", help="Run only the missing-year check")
    p.add_argument("--match", action="store_true", help="Run only the reordered match")
    p.add_argument("--compare", action="store_true", help="Run only the baseline comparison on an existing --out")
    p.add_argument("--tmp", default=None)
    p.add_argument("--memory", default="8GB")
    p.add_argument("--threads", type=int, default=4)
    return p.parse_args()


def check(con: duckdb.DuckDBPyConnection, attom: str, enriched: str, properties: str, xwalk: str) -> None:
    # single-family ATTOM houses with and without a year, by parish
    codes = ",".join(q(c) for c in SF_USE_CODES.split(","))
    attom_by_parish = con.execute(f"""
        WITH p AS (
          SELECT cast(attomid AS varchar) attomid,
                 max(cast(construction_year AS integer)) construction_year,
                 max(nullif(trim(cast(countycode AS varchar)),'')) countycode
          FROM read_parquet({q(attom)})
          WHERE trim(cast(property_use_std AS varchar)) IN ({codes})
          GROUP BY 1
        ), e AS (
          SELECT cast(attomid AS varchar) attomid, builty_elevated, builty_retrofit
          FROM read_parquet({q(enriched)})
        )
        SELECT p.countycode,
               count(*) attom_sf,
               sum((p.construction_year IS NULL)::int) attom_sf_no_year,
               sum(coalesce(e.builty_retrofit, 0)) builty_retrofit,
               sum(((e.builty_retrofit = 1) AND p.construction_year IS NULL)::int) builty_retrofit_no_year
        FROM p LEFT JOIN e USING (attomid)
        GROUP BY 1
    """).df()

    # NFIP properties per parish, from the first-policy-year snapshot
    nfip = load_properties(properties, STATE)
    nfip_by_parish = (nfip.groupby("county_key").size().rename("nfip_properties")
                      .reset_index().rename(columns={"county_key": "countycode"}))

    # parish names
    names = pd.read_stata(xwalk)
    names = names[names["state"] == "Louisiana"].copy()
    names["countycode"] = "22" + names["countycode"].astype(int).astype(str).str.zfill(3)
    names = names[["countycode", "county"]]

    table = (attom_by_parish.merge(nfip_by_parish, on="countycode", how="outer")
             .merge(names, on="countycode", how="left").fillna({"county": "?"}))
    for col in ["attom_sf", "attom_sf_no_year", "builty_retrofit", "builty_retrofit_no_year", "nfip_properties"]:
        table[col] = table[col].fillna(0).astype(int)
    table["share_no_year"] = table["attom_sf_no_year"] / table["attom_sf"].where(table["attom_sf"] > 0)
    table["share_retrofit_no_year"] = (table["builty_retrofit_no_year"]
                                       / table["builty_retrofit"].where(table["builty_retrofit"] > 0))
    table = table.sort_values("nfip_properties", ascending=False)

    total = table[["attom_sf", "attom_sf_no_year", "builty_retrofit", "builty_retrofit_no_year", "nfip_properties"]].sum()
    print(f"\n{STATE}: {total['attom_sf_no_year']:,} of {total['attom_sf']:,} single-family ATTOM properties "
          f"have no year built ({total['attom_sf_no_year'] / total['attom_sf']:.1%}); "
          f"{total['builty_retrofit_no_year']:,} of {total['builty_retrofit']:,} Builty retrofit houses "
          f"({total['builty_retrofit_no_year'] / max(total['builty_retrofit'], 1):.1%})")
    print("\nBy parish, parishes with the most NFIP properties first (share = no year / single-family):")
    show = table[["county", "nfip_properties", "attom_sf", "share_no_year", "builty_retrofit", "share_retrofit_no_year"]]
    with pd.option_context("display.max_rows", 100, "display.width", 140, "display.float_format", "{:.2f}".format):
        print(show.head(25).to_string(index=False))

    # concentration: share of the state's missing years in the parishes with the most NFIP properties
    top = table.head(5)
    print(f"\nTop 5 parishes by NFIP properties hold {top['nfip_properties'].sum() / total['nfip_properties']:.0%} of NFIP "
          f"properties, {top['attom_sf_no_year'].sum() / total['attom_sf_no_year']:.0%} of the missing years and "
          f"{top['builty_retrofit'].sum() / max(total['builty_retrofit'], 1):.0%} of the Builty retrofits")


def match(con: duckdb.DuckDBPyConnection, attom: str, enriched: str, properties: str, out: Path) -> None:
    nfip = load_properties(properties, STATE)
    print(f"\n{STATE}: {len(nfip):,} NFIP properties")
    con.register("property_frame", nfip)
    con.execute("CREATE TABLE nfip AS SELECT * FROM property_frame")
    for name, dtype in ASSIGNMENT_COLUMNS.items():
        con.execute(f"ALTER TABLE nfip ADD COLUMN {name} {dtype}")

    # year-less ATTOM houses stay in the pool, as with --add-tier-15
    build_attom(con, attom, enriched, SF_USE_CODES, allow_missing_construction=True)
    print(f"{STATE}: {con.execute('SELECT count(*) FROM attom').fetchone()[0]:,} ATTOM candidates")

    print("\nReordered ladder (tier numbers as in nfip_attom.py):")
    for keys, label, number in LADDER:
        apply_tier(con, keys, label, number)

    attach_values(con, attom)
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT * FROM final ORDER BY property_id) TO {q(out)} (FORMAT PARQUET, COMPRESSION ZSTD)")
    print(f"Saved: {out}")


def compare(con: duckdb.DuckDBPyConnection, out: Path, baseline: Path) -> None:
    # same NFIP properties, two ladders: who moved, and where the Builty retrofits land
    print("\nBuilty retrofits on the panel by tier, baseline vs reordered:")
    # each ladder counted on its own tier column, then lined up by tier
    print(con.execute(f"""
        WITH b AS (SELECT match_tier_number tier, count(*) matched_baseline,
                          sum(coalesce(builty_retrofit, 0)) retrofits_baseline
                   FROM read_parquet({q(baseline)}) WHERE match_tier_number IS NOT NULL GROUP BY 1),
             r AS (SELECT match_tier_number tier, count(*) matched_reordered,
                          sum(coalesce(builty_retrofit, 0)) retrofits_reordered
                   FROM read_parquet({q(out)}) WHERE match_tier_number IS NOT NULL GROUP BY 1)
        SELECT tier, matched_baseline, matched_reordered, retrofits_baseline, retrofits_reordered
        FROM b FULL JOIN r USING (tier) ORDER BY tier
    """).df().to_string(index=False))
    moved = con.execute(f"""
        SELECT count(*) n, sum((b.assigned_attomid IS DISTINCT FROM r.assigned_attomid)::int) changed,
               sum((b.assigned_attomid IS NULL AND r.assigned_attomid IS NOT NULL)::int) newly_matched,
               sum((b.assigned_attomid IS NOT NULL AND r.assigned_attomid IS NULL)::int) newly_unmatched,
               sum(coalesce(b.builty_retrofit, 0)) retrofits_baseline, sum(coalesce(r.builty_retrofit, 0)) retrofits_reordered
        FROM read_parquet({q(baseline)}) b JOIN read_parquet({q(out)}) r USING (property_id)
    """).df()
    print("\nAssignments:")
    print(moved.to_string(index=False))


def main() -> None:
    args = parse_args()
    data = Path(args.data)
    pipe = data / "build" / "nfip_attom_pipeline_v2"
    attom = str(pipe / "geocoded" / "la_attom_geocoded.parquet")
    properties = str(data / "clean" / "nfip_policies_property.dta")
    xwalk = str(data / "clean" / "crosswalks" / "county_xwalk.dta")
    out = Path(args.out) if args.out else pipe / "nfip_attom_property" / "la_nfip_attom_property_noyear.parquet"
    only = args.check or args.match or args.compare
    run_check = args.check or not only
    run_match = args.match or not only
    run_compare = (args.compare or not only) and args.baseline is not None

    con = duckdb.connect()
    con.execute(f"SET temp_directory={q(args.tmp or (pipe / 'tmp' / 'la_noyear'))}")
    con.execute(f"SET memory_limit={q(args.memory)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")

    if run_check:
        check(con, attom, args.attom_nfhl_builty, properties, xwalk)
    if run_match:
        match(con, attom, args.attom_nfhl_builty, properties, out)
    if run_compare:
        compare(con, out, Path(args.baseline))
    con.close()


if __name__ == "__main__":
    main()
