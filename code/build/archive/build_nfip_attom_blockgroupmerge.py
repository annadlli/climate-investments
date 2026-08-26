"""
Merge block-group ATTOM cells onto NFIP policy/property rows.

Authors: Anna Li
Date: 2026-07-08

Description:
    Takes the NFIP policy-year file from clean_nfip_policies.do and attaches
    ATTOM values from build_attom_nfip_cells.py. Matching is tiered from finest
    to coarsest:

        1. census block group x construction year
        2. census block group x construction 5-year bin
        3. census block group x construction decade
        4. census block group

    Within each tier, ATTOM tax year is matched as-of the NFIP policy year: the
    latest available ATTOM assessment year less than or equal to policy_year.

    Output is still an NFIP policy/property file. Each NFIP property_id can carry
    ATTOM values, but the values are block-group cells, not exact parcel matches.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


VALUE_COLS = [
    "attom_n_properties",
    "attom_mean_market_total",
    "attom_median_market_total",
]

TIERS = [
    ("constr_year", ["censusblockgroupfips", "construction_year"], "blockgroup_construction_year"),
    ("constr_5yr", ["censusblockgroupfips", "construction_5yr"], "blockgroup_construction_5yr"),
    ("constr_decade", ["censusblockgroupfips", "construction_decade"], "blockgroup_construction_decade"),
    ("year", ["censusblockgroupfips"], "blockgroup_year"),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Attach ATTOM block-group cells to cleaned NFIP policy rows."
    )
    p.add_argument("--state", required=True)
    p.add_argument("--data", required=True)
    return p.parse_args()


def normalize_string_key(series: pd.Series, width: int | None = None) -> pd.Series:
    out = series.astype("string").fillna("").str.strip()
    out = out.mask(out.isin([".", "<NA>", "nan", "NaN"]), "")
    if width:
        out = out.str.extract(r"(\d+)", expand=False).fillna("").str.zfill(width)
    return out


def load_nfip(data: Path, state: str) -> pd.DataFrame:
    path = data / "clean" / "nfip_policies_state" / f"{state}.dta"
    nf = pd.read_stata(path, preserve_dtypes=False)
    nf.insert(0, "_id", range(1, len(nf) + 1))

    nf["censusblockgroupfips"] = normalize_string_key(nf["censusblockgroupfips"])
    nf.loc[nf["censusblockgroupfips"].str.len() != 12, "censusblockgroupfips"] = ""
    nf["policy_year"] = pd.to_numeric(nf["policy_year"], errors="coerce")
    nf["construction_year"] = pd.to_numeric(nf["construction_year"], errors="coerce")
    nf["construction_5yr"] = (nf["construction_year"] // 5) * 5
    nf["construction_decade"] = (nf["construction_year"] // 10) * 10

    for col in VALUE_COLS:
        nf[col] = pd.NA
    nf["attom_year"] = pd.NA
    nf["attom_tier"] = ""
    return nf


def load_cells(data: Path, state: str, grain: str) -> pd.DataFrame:
    path = data / "build" / f"{state}_attom_nfip_blockgroup_{grain}.dta"
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}; run build_attom_nfip_cells.py for {state} first."
        )
    cells = pd.read_stata(path, preserve_dtypes=False)
    cells["censusblockgroupfips"] = normalize_string_key(cells["censusblockgroupfips"])
    cells["year"] = pd.to_numeric(cells["year"], errors="coerce")
    for col in VALUE_COLS:
        cells[col] = pd.to_numeric(cells[col], errors="coerce")
    return cells


def apply_tier(nf: pd.DataFrame, cells: pd.DataFrame, keys: list[str], label: str) -> pd.DataFrame:
    eligible = nf[nf["attom_tier"].eq("")].copy()
    eligible = eligible[eligible["policy_year"].notna()]
    for key in keys:
        if pd.api.types.is_string_dtype(eligible[key]):
            eligible = eligible[eligible[key].ne("")]
        else:
            eligible = eligible[eligible[key].notna()]

    if eligible.empty:
        return nf

    con = duckdb.connect()
    con.register("eligible", eligible[["_id", "policy_year"] + keys])
    con.register("cells", cells[keys + ["year"] + VALUE_COLS])
    key_join = " AND ".join(f"e.{key} = c.{key}" for key in keys)
    hits = con.execute(f"""
        SELECT
            e._id,
            c.year AS attom_year,
            c.attom_n_properties,
            c.attom_mean_market_total,
            c.attom_median_market_total
        FROM eligible AS e
        INNER JOIN cells AS c
            ON {key_join}
           AND c.year <= e.policy_year
        QUALIFY row_number() OVER (
            PARTITION BY e._id
            ORDER BY c.year DESC
        ) = 1
    """).df()
    con.close()

    if hits.empty:
        print(f"  tier {label:<32} matched         0")
        return nf

    idx = nf.index[nf["_id"].isin(hits["_id"])]
    hits = hits.set_index("_id")
    ids = nf.loc[idx, "_id"]
    for col in VALUE_COLS + ["attom_year"]:
        nf.loc[idx, col] = hits.loc[ids, col].to_numpy()
    nf.loc[idx, "attom_tier"] = label

    matched = nf["attom_tier"].ne("").sum()
    print(f"  tier {label:<32} matched {len(hits):>9,}  "
          f"(cumulative {matched:>9,} / {len(nf):,})")
    return nf


def main() -> None:
    args = parse_args()
    state = args.state.lower()
    data = Path(args.data)

    nf = load_nfip(data, state)
    print(f"NFIP policy-years ({state.upper()}): {len(nf):,}")

    for grain, keys, label in TIERS:
        cells = load_cells(data, state, grain)
        nf = apply_tier(nf, cells, keys, label)

    nf["attom_tier"] = nf["attom_tier"].replace("", "unmatched")
    matched = nf["attom_tier"].ne("unmatched").sum()
    print(f"ATTOM block-group value attached: {matched:,} / {len(nf):,} "
          f"({matched / max(len(nf), 1):.1%})")

    out = data / "build" / f"{state}_nfip_attom_blockgroup.dta"
    nf = nf.drop(columns=["_id", "construction_5yr", "construction_decade"])
    for col in VALUE_COLS + ["attom_year"]:
        nf[col] = pd.to_numeric(nf[col], errors="coerce")
    nf.to_stata(out, write_index=False, version=118)
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()
