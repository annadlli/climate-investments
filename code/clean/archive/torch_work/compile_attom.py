import os
import glob
import duckdb

con = duckdb.connect()
con.execute("SET temp_directory='/scratch/adl9602/tmp'")
con.execute("SET memory_limit='200GB'")
con.execute("SET threads=4")

for state_name in ["va"]:  # "tx", "la", "mo", "nc"
    # ATTOM snappy parquet files — named like assessor-history_0_0_0.snappy.parquet
    input_dir = f"/scratch/adl9602/tx/data/{state_name}/attom"
    files = sorted(glob.glob(os.path.join(input_dir, "*.snappy.parquet")))

    if not files:
        print(f"No snappy.parquet files found in {input_dir}, skipping")
        continue

    print(f"Compiling {len(files)} ATTOM files for {state_name}...")
    for f in files:
        print(f"  {os.path.basename(f)}")

    # Write quoted list for DuckDB glob
    files_sql = "[" + ", ".join(f"'{f}'" for f in files) + "]"

    out = f"/scratch/adl9602/tx/data/{state_name}/attom_{state_name}.parquet"
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet({files_sql})
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    n = con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0]
    print(f"  Done: {out}  ({n:,} rows)")

print("\nDone.")
