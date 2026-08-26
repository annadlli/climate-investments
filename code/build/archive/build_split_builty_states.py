"""
Split the national Builty elevation file into one parquet per state.

Authors: Anna Li and Vendela Norman
Original Date: 2026-06-17
Revised Date: 2026-08-03

Description:
    Writes one per-state elevation file for the ATTOM merge, into the single
    canonical location build_attom_onto_permits.py reads:
    builty-states/builty_elevations_{state}.dta. There is one elevation
    tier - the national input is already filtered, so no loose/strict variants
    are produced. Accepts either the filtered parquet or the Stata dta; a dta
    is filtered to final_flag == 1 when that column is present. The output
    extension follows --filename-pattern (.dta or .parquet).

    Only needed when regenerating the per-state files from the national one;
    if the per-state .dta files are supplied directly, skip this step.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


# Command-line inputs; the national file and outputs derive from the data root.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--input", default=None)
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--states", nargs="+", default=["TX", "VA"])
    parser.add_argument("--filename-pattern", default="builty_elevations_{state_lower}.dta")
    parser.add_argument("--tmp", default="/tmp")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory", default="32GB")
    return parser.parse_args()


def main() -> None:
    # Resolve paths and configure DuckDB.
    args = parse_args()
    data = Path(args.data)
    input_path = Path(args.input) if args.input else data / "build" / "all_builty_elevations.parquet"
    out_dir = Path(args.out_dir) if args.out_dir else data / "builty-states"
    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote_sql(args.tmp)}")
    con.execute(f"SET threads={int(args.threads)}")
    con.execute(f"SET memory_limit={quote_sql(args.memory)}")
    con.execute("SET preserve_insertion_order=false")

    # A dta input is read through pandas and filtered to final elevations; a
    # parquet is scanned directly.
    if input_path.suffix == ".dta":
        frame = pd.read_stata(input_path, convert_categoricals=False)
        # The national elevation dta is already filtered and may not carry
        # final_flag; only re-filter when the column is present.
        if "final_flag" in frame.columns:
            frame = frame[frame["final_flag"].eq(1)]
        con.register("permits_input", frame)
        input_rel = "permits_input"
    else:
        input_rel = f"read_parquet({quote_sql(str(input_path))})"

    # Write one file per requested state, in the format the pattern asks for.
    for state in [s.upper() for s in args.states]:
        out_path = out_dir / args.filename_pattern.format(state=state, state_lower=state.lower())
        out_path.parent.mkdir(parents=True, exist_ok=True)
        select = f"SELECT * FROM {input_rel} WHERE upper(STATE) = {quote_sql(state)}"
        if out_path.suffix.lower() == ".dta":
            con.execute(select).df().to_stata(out_path, write_index=False, version=118)
        else:
            con.execute(f"COPY ({select}) TO {quote_sql(str(out_path))} (FORMAT PARQUET)")

    con.close()


if __name__ == "__main__":
    main()
