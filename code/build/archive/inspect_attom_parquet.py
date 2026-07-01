"""
Quick inspection of raw ATTOM parquet file.

Usage:
    python inspect_attom_parquet.py --parquet /path/to/attom_tx.parquet
"""

import argparse
import duckdb


def quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--parquet", required=True)
    p.add_argument("--threads", default=4, type=int)
    p.add_argument("--memory",  default="32GB")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    con = duckdb.connect()
    con.execute(f"SET memory_limit={quote(args.memory)}")
    con.execute(f"SET threads={args.threads}")
    con.execute(f"CREATE VIEW raw AS SELECT * FROM read_parquet({quote(args.parquet)})")

    print("=" * 60)
    print("ROW COUNT")
    print("=" * 60)
    n = con.execute("SELECT count(*) FROM raw").fetchone()[0]
    print(f"{n:,} rows\n")

    print("=" * 60)
    print("COLUMNS AND TYPES")
    print("=" * 60)
    print(con.execute("DESCRIBE raw").df().to_string(index=False))
    print()

    print("=" * 60)
    print("KEY VALUE FIELDS — non-null and >0 counts")
    print("=" * 60)
    value_cols = [
        "TAXASSESSEDVALUETOTAL",
        "TAXMARKETVALUETOTAL",
        "TAXYEARASSESSED",
        "TAXMARKETVALUEYEAR",
        "TAXFISCALYEAR",
        "YEARBUILT",
        "YEARBUILTEFFECTIVE",
    ]
    for col in value_cols:
        try:
            row = con.execute(f"""
                SELECT
                    count(*) FILTER (WHERE {col} IS NOT NULL)                   AS n_nonnull,
                    count(*) FILTER (WHERE cast({col} AS double) > 0)           AS n_positive,
                    min(cast({col} AS double))                                   AS min_val,
                    max(cast({col} AS double))                                   AS max_val
                FROM raw
            """).fetchone()
            print(f"{col:30s}  nonnull={row[0]:>10,}  positive={row[1]:>10,}  min={row[2]}  max={row[3]}")
        except Exception as e:
            print(f"{col:30s}  ERROR: {e}")

    print()
    print("=" * 60)
    print("SAMPLE ROWS (5)")
    print("=" * 60)
    print(con.execute("SELECT * FROM raw LIMIT 5").df().T.to_string())


if __name__ == "__main__":
    main()
