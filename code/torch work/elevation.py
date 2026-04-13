# import duckdb

# con = duckdb.connect()

# total = con.execute("SELECT count(*) FROM '/scratch/adl9602/tx/merged.parquet'").fetchone()[0]
# print(f"Total obs in dataset: {total:,}")

# con.execute("""
#     COPY (
#         SELECT *
#         FROM '/scratch/adl9602/tx/merged.parquet'
#         WHERE lower(DESCRIPTION) LIKE '%flood%'
#            OR lower(DESCRIPTION) LIKE '%elevat%'
#            OR lower(DESCRIPTION) LIKE '%fema%'
#            OR lower(DESCRIPTION) LIKE '%mitigation%'
#            OR lower(DESCRIPTION) LIKE '%floodplain%'
#            OR lower(DESCRIPTION) LIKE '%levee%'
#     ) TO '/scratch/adl9602/tx/flood_elevation.parquet' (FORMAT PARQUET)
# """)

# n = con.execute("SELECT count(*) FROM '/scratch/adl9602/tx/flood_elevation.parquet'").fetchone()[0]
# print(f"Flood/elevation obs: {n:,}")
# print(f"Share of total: {n/total*100:.3f}%")
import os
import glob
import duckdb

con = duckdb.connect()

state_dir = "/scratch/adl9602/tx/data/state"

for state in sorted(os.listdir(state_dir)):
    state_path = os.path.join(state_dir, state)
    if not os.path.isdir(state_path):
        continue

    input_files = glob.glob(os.path.join(state_path, "*.parquet"))
    # skip flood_elevation output file if re-running
    input_files = [f for f in input_files if "flood_elevation" not in os.path.basename(f)]

    if not input_files:
        print(f"{state}: no parquet files, skipping")
        continue

    out_file = os.path.join(state_path, "flood_elevation.parquet")

    total = con.execute(f"SELECT count(*) FROM read_parquet({input_files})").fetchone()[0]
    print(f"{state}: {total:,} total obs")

    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet({input_files})
            WHERE lower(DESCRIPTION) LIKE '%flood%'
               OR lower(DESCRIPTION) LIKE '%elevat%'
               OR lower(DESCRIPTION) LIKE '%fema%'
               OR lower(DESCRIPTION) LIKE '%mitigation%'
               OR lower(DESCRIPTION) LIKE '%floodplain%'
               OR lower(DESCRIPTION) LIKE '%levee%'
        ) TO '{out_file}' (FORMAT PARQUET)
    """)

    n = con.execute(f"SELECT count(*) FROM '{out_file}'").fetchone()[0]
    print(f"  flood_elevation.parquet: {n:,} rows ({n/total*100:.3f}%)")

print("\nDone.")
