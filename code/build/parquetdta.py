import argparse
from pathlib import Path

import duckdb
import pandas as pd
import numpy as np


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True, help="State abbreviation, e.g. TX or VA")
    args = parser.parse_args()

    state = args.state.lower()

    root = Path("/Users/anna/Desktop/Research/climate-investments")
    in_path = root / "data" / "build" / state / f"{state}_attom_permits_strict.parquet"
    out_path = root / "data" / "build" / f"{state}_attom_builty.dta"

    con = duckdb.connect()

    print(f"Reading parquet: {in_path}")
    df = con.execute(f"SELECT * FROM '{in_path}'").df()
    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")

    for col in df.columns:
        if df[col].dtype == object:
            if df[col].isna().all():
                df[col] = pd.array([""] * len(df), dtype=pd.StringDtype())
            else:
                df[col] = df[col].astype(str).replace("nan", "").replace("None", "")

    for col in df.columns:
        if df[col].dtype == bool:
            df[col] = df[col].astype(np.int8)

    print(f"Saving to dta: {out_path}")
    df.to_stata(out_path, write_index=False, version=118)

    print(f"Done. Output: {out_path}")


if __name__ == "__main__":
    main()
