import os
import glob
import duckdb

con = duckdb.connect()

for state_name in ["tx"]:  # "la", "va", "mo", "nc"
    input_dir = os.path.join("data", state_name)
    files = glob.glob(os.path.join(input_dir, "*.parquet"))
    
    if not files:
        print(f"No parquet files found for {state_name}, skipping")
        continue
    
    print(f"Appending {len(files)} files for {state_name}...")
    
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet({files})
        ) TO 'data/{state_name}.parquet' (FORMAT PARQUET)
    """)
    
    n = con.execute(f"SELECT count(*) FROM 'data/{state_name}.parquet'").fetchone()[0]
    print(f"  {state_name}.parquet: {n:,} rows")

print("\nDone. Output: data/tx.parquet")