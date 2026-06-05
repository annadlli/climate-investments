"""
Split a filtered Builty elevation parquet into state-level parquet files.

Example:
    python split_builty_states.py \
      --input data/clean/all_elevation.parquet \
      --out-dir data/build \
      --states TX VA
"""

import argparse
from pathlib import Path

import duckdb
import pandas as pd


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def project_root() -> Path:
    candidates = [Path.cwd(), *Path(__file__).resolve().parents]
    for candidate in candidates:
        if (candidate / "data").exists() and (candidate / "torch").exists():
            return candidate
    return Path.cwd()


def main() -> None:
    root = project_root()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default=str(root / "data" / "clean" / "all_elevation.parquet"),
        help="Strict filtered Builty elevation parquet.",
    )
    parser.add_argument(
        "--out-dir",
        default=str(root / "data" / "build"),
        help="Directory that will receive state subfolders, e.g. data/build/tx and data/build/va.",
    )
    parser.add_argument("--states", nargs="+", default=["TX", "VA"])
    parser.add_argument(
        "--filename-pattern",
        default="{state_lower}_flood_elevation_strict.parquet",
        help="Output filename pattern. Available fields: state, state_lower.",
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

    input_path = str(Path(args.input))
    out_dir = Path(args.out_dir)
    path_sql = quote_sql(input_path)

    rows = []
    for state in [s.upper() for s in args.states]:
        state_lower = state.lower()
        filename = args.filename_pattern.format(state=state, state_lower=state_lower)
        out_path = out_dir / state_lower / filename
        out_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"Writing {state}: {out_path}")
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet({path_sql})
                WHERE upper(STATE) = {quote_sql(state)}
            ) TO {quote_sql(str(out_path))} (FORMAT PARQUET)
            """
        )
        n = con.execute(
            f"SELECT count(*) FROM read_parquet({quote_sql(str(out_path))})"
        ).fetchone()[0]
        rows.append({"state": state, "output": str(out_path), "rows": n})
        print(f"  rows: {n:,}")

    diagnostics_path = Path(args.diagnostics) if args.diagnostics else out_dir / "builty_state_split_diagnostics.csv"
    pd.DataFrame(rows).to_csv(diagnostics_path, index=False)
    print(f"Diagnostics: {diagnostics_path}")


if __name__ == "__main__":
    main()
