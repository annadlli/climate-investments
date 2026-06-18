import os
import glob
import duckdb

con = duckdb.connect()

base_dir = "/scratch/adl9602/tx/data"

# Use the most recent run folder
run_dirs = sorted(
    [d for d in glob.glob(os.path.join(base_dir, "run_*")) if os.path.isdir(d)]
)
if not run_dirs:
    raise RuntimeError("No run folders found.")
run_dir = run_dirs[-1]
print(f"Using run: {run_dir}")

# -------------------------
# AS and CTOK → data/state/<STATE>/<source>.parquet
# -------------------------
for source in ["as", "ctok"]:
    src_files = glob.glob(os.path.join(run_dir, source, "*.parquet"))
    if not src_files:
        print(f"\nNo files found for {source}, skipping.")
        continue

    states = con.execute(
        f"SELECT DISTINCT STATE FROM read_parquet({src_files}) ORDER BY STATE"
    ).fetchall()
    states = [s[0] for s in states]
    print(f"\n{source}: {len(src_files)} files, {len(states)} states: {states}")

    for state in states:
        state_dir = os.path.join(base_dir, "state", state)
        os.makedirs(state_dir, exist_ok=True)
        out_file = os.path.join(state_dir, f"{source}.parquet")

        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet({src_files})
                WHERE STATE = '{state}'
            ) TO '{out_file}' (FORMAT PARQUET)
        """)
        n = con.execute(f"SELECT count(*) FROM '{out_file}'").fetchone()[0]
        print(f"  {state}/{source}.parquet: {n:,} rows")

print("\nDone.")
