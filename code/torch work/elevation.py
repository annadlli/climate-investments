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
import duckdb

con = duckdb.connect()

total = con.execute("SELECT count(*) FROM '/scratch/adl9602/tx/data/tx.parquet'").fetchone()[0]
print(f"Total obs in dataset: {total:,}")

con.execute("""
    COPY (
        SELECT *
        FROM '/scratch/adl9602/tx/data/tx.parquet'
        WHERE lower(DESCRIPTION) LIKE '%flood%'
           OR lower(DESCRIPTION) LIKE '%elevat%'
           OR lower(DESCRIPTION) LIKE '%fema%'
           OR lower(DESCRIPTION) LIKE '%mitigation%'
           OR lower(DESCRIPTION) LIKE '%floodplain%'
           OR lower(DESCRIPTION) LIKE '%levee%'
    ) TO '/scratch/adl9602/tx/flood_elevation.parquet' (FORMAT PARQUET)
""")

n = con.execute("SELECT count(*) FROM '/scratch/adl9602/tx/flood_elevation.parquet'").fetchone()[0]
print(f"Flood/elevation obs: {n:,}")
print(f"Share of total: {n/total*100:.3f}%")