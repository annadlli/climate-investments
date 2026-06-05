import duckdb
import pandas as pd
import numpy as np

con = duckdb.connect()

print("Reading parquet...")
df = con.execute("SELECT * FROM '/scratch/adl9602/tx/flood_elevation.parquet'").df()
print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")

# Fix object columns
for col in df.columns:
    if df[col].dtype == object:
        # Check if all values are None/NaN
        if df[col].isna().all():
            # Replace with empty string column
            df[col] = pd.array([''] * len(df), dtype=pd.StringDtype())
        else:
            df[col] = df[col].astype(str).replace('nan', '').replace('None', '')

# Fix boolean columns
for col in df.columns:
    if df[col].dtype == bool:
        df[col] = df[col].astype(np.int8)

print("Saving to dta...")
df.to_stata(
    '/scratch/adl9602/tx/flood_elevation.dta',
    write_index=False,
    version=118
)
print("Done. Output: /scratch/adl9602/tx/flood_elevation.dta")