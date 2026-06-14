import os
import glob
import duckdb

con = duckdb.connect()
con.execute("SET temp_directory='/scratch/adl9602/tmp'")
con.execute("SET memory_limit='200GB'")
con.execute("SET threads=4")

# Per-state config: map state -> list of input directories
STATE_DIRS = {
    "tx": [
        "/scratch/adl9602/tx/data/tx/attomtx",
        "/scratch/adl9602/tx/data/tx/attomtx2",
    ],
    "va": [
        "/scratch/adl9602/tx/data/va/attom",
    ],
    # "la": [...],
    # "nc": [...],
}

STATE_OUT = {
    "tx": "/scratch/adl9602/tx/data/tx/attom_tx.parquet",
    "va": "/scratch/adl9602/tx/data/va/attom_va.parquet",
}

for state_name in ["tx"]:  # switch to "va" etc. as needed
    input_dirs = STATE_DIRS[state_name]
    out = STATE_OUT[state_name]

    # Collect files from all input directories
    all_files = []
    for d in input_dirs:
        found = sorted(glob.glob(os.path.join(d, "*.snappy.parquet")))
        print(f"  {d}: {len(found)} files")
        all_files.extend(found)

    if not all_files:
        print(f"No snappy.parquet files found for {state_name}, skipping")
        continue

    print(f"\nFound {len(all_files)} total snappy files for {state_name}, checking integrity...")

    good, bad = [], []
    for f in all_files:
        try:
            with open(f, "rb") as fh:
                header = fh.read(4)
                fh.seek(-4, 2)
                footer = fh.read(4)
            if header == b"PAR1" and footer == b"PAR1":
                good.append(f)
            else:
                bad.append(f)
        except Exception as e:
            bad.append(f)

    if bad:
        print(f"  WARNING: {len(bad)} truncated/corrupt files skipped:")
        for f in bad:
            print(f"    {os.path.dirname(f).split('/')[-1]}/{os.path.basename(f)}")
    print(f"  Compiling {len(good)} valid files...")

    if not good:
        print(f"  No valid files to compile for {state_name}, skipping")
        continue

    files_sql = "[" + ", ".join(f"'{f}'" for f in good) + "]"

    os.makedirs(os.path.dirname(out), exist_ok=True)
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet({files_sql})
        ) TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)

    n = con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0]
    print(f"\n  Done: {out}  ({n:,} rows)")

print("\nDone.")
