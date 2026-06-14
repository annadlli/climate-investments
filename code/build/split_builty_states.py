"""
Authors: Anna Li and Vendela Norman
Date: 2026-06-14
Description: Split strict-filtered Builty elevation permits into per-state parquet files.
Notes / Sources: Reads data/clean/builty_strict.parquet (output of clean/clean_builty_strict.py).
Example:
    python split_builty_states.py \
      --input data/clean/builty_strict.parquet \
      --out-dir data/build \
      --states TX VA
"""

import argparse
from pathlib import Path

import duckdb
import pandas as pd


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/clean/builty_strict.parquet")
    parser.add_argument("--out-dir", default="data/build") 
    parser.add_argument("--states", nargs="+", default=["TX", "VA"]) #change as needed
    parser.add_argument(
        "--filename-pattern",
        default="{state_lower}_flood_elevation_strict.parquet",
        help="Available fields: {state}, {state_lower}.",
    )
    parser.add_argument("--diagnostics", default=None, help="Optional CSV with state row counts.")
    parser.add_argument("--tmp", default="/tmp", help="DuckDB temp directory.")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory", default="32GB")
    args = parser.parse_args()

    con = duckdb.connect()
    con.execute(f"SET temp_directory={quote_sql(args.tmp)}")
    con.execute(f"SET threads={args.threads}")
    con.execute(f"SET memory_limit={quote_sql(args.memory)}")
    con.execute("SET preserve_insertion_order=false")

    path_sql = quote_sql(args.input)
    out_dir = Path(args.out_dir)

    rows = []
    for state in [s.upper() for s in args.states]:
        state_lower = state.lower()
        filename = args.filename_pattern.replace("{state}", state).replace("{state_lower}", state_lower)
        out_path = out_dir / state_lower / filename
        out_path.parent.mkdir(parents=True, exist_ok=True)

        n = con.execute(
            f"SELECT count(*) FROM read_parquet({path_sql}) WHERE upper(STATE) = {quote_sql(state)}"
        ).fetchone()[0]

        print(f"Writing {state} ({n:,} rows): {out_path}")
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet({path_sql})
                WHERE upper(STATE) = {quote_sql(state)}
            ) TO {quote_sql(str(out_path))} (FORMAT PARQUET)
            """
        )
        rows.append({"state": state, "output": str(out_path), "rows": n})

    con.close()

    diagnostics_path = Path(args.diagnostics) if args.diagnostics else out_dir / "builty_state_split_diagnostics.csv"
    pd.DataFrame(rows).to_csv(diagnostics_path, index=False)
    print(f"Diagnostics: {diagnostics_path}")


if __name__ == "__main__":
    main()
