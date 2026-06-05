import duckdb
import os
import glob

con = duckdb.connect()
con.execute("SET temp_directory='/scratch/adl9602/tmp'")
con.execute("SET memory_limit='200GB'")
con.execute("SET threads=4")
con.execute("SET preserve_insertion_order=false")

ADDR_CLEAN_COMBINED = """
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    regexp_replace(
    regexp_replace(
    regexp_replace(
        replace(
        replace(
            lower(trim(STREET)),
        ',', ''), '.', ''),
    '\\s+', ' ', 'g'),
    '\\s+city of.*$', '', 'g'),
    '\\s+etj.*$', '', 'g'),
    ' street', ' st'),
    ' avenue', ' ave'),
    ' boulevard', ' blvd'),
    ' drive', ' dr'),
    ' court', ' ct'),
    ' place', ' pl'),
    ' lane', ' ln'),
    ' road', ' rd'),
    ' circle', ' cir'),
    ' highway', ' hwy'),
    ' parkway', ' pkwy'),
    ' terrace', ' ter'),
    ' trail', ' trl'),
    ' north', ' n'),
    ' south', ' s'),
    ' east', ' e'),
    ' west', ' w'),
    ' apartment', ' apt'),
    ' suite', ' ste'),
    ' unit', ' apt'),
    ' #', ' apt ')
"""

ADDR_CLEAN_ADDRESS = """
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    replace(
    regexp_replace(
        replace(
        replace(
            lower(trim(PROPERTYADDRESSFULL)),
        ',', ''), '.', ''),
    '\\s+', ' ', 'g'),
    ' street', ' st'),
    ' avenue', ' ave'),
    ' boulevard', ' blvd'),
    ' drive', ' dr'),
    ' court', ' ct'),
    ' place', ' pl'),
    ' lane', ' ln'),
    ' road', ' rd'),
    ' circle', ' cir'),
    ' highway', ' hwy'),
    ' parkway', ' pkwy'),
    ' terrace', ' ter'),
    ' trail', ' trl'),
    ' north', ' n'),
    ' south', ' s'),
    ' east', ' e'),
    ' west', ' w'),
    ' apartment', ' apt'),
    ' suite', ' ste'),
    ' unit', ' apt'),
    ' #', ' apt ')
"""

ZIP_CLEAN_COMBINED = """
    regexp_extract(
        replace(upper(trim(ZIPCODE)), ' ', ''),
    '^(\\d{5})', 1)
"""

ZIP_CLEAN_ADDRESS = """
    regexp_extract(
        replace(upper(trim(PROPERTYADDRESSZIP)), ' ', ''),
    '^(\\d{5})', 1)
"""

# Create cleaned deduplicated view of combined once
con.execute(f"""
    CREATE VIEW combined_clean AS
    SELECT *,
        {ADDR_CLEAN_COMBINED} AS addr_clean,
        {ZIP_CLEAN_COMBINED}  AS zip_clean
    FROM '/scratch/adl9602/tx/combined.parquet'
    WHERE STREET  IS NOT NULL AND trim(STREET)  != ''
      AND ZIPCODE IS NOT NULL AND trim(ZIPCODE) != ''
""")

con.execute("""
    CREATE VIEW combined_dedup AS
    SELECT DISTINCT ON (addr_clean, zip_clean) *
    FROM combined_clean
""")

combined_cols = set(r[0] for r in con.execute("DESCRIBE combined_clean").fetchall())

os.makedirs('/scratch/adl9602/tmp/chunks', exist_ok=True)

# Both folders together
files = sorted(
    glob.glob('/scratch/adl9602/tx/attomtx/*.snappy.parquet') +
    glob.glob('/scratch/adl9602/tx/attomtx2/*.snappy.parquet')
)
print(f"Found {len(files)} address files to process")

matched_chunks = []
total_matched = 0

for i, fpath in enumerate(files):
    fname = os.path.basename(fpath)
    folder = os.path.basename(os.path.dirname(fpath))
    print(f"[{i+1}/{len(files)}] {folder}/{fname}...")

    try:
        file_cols = set(r[0] for r in con.execute(
            f"DESCRIBE SELECT * FROM '{fpath}' LIMIT 1"
        ).fetchall())
    except Exception as e:
        print(f"  Skipping: {e}")
        continue

    if 'PROPERTYADDRESSFULL' not in file_cols or 'PROPERTYADDRESSZIP' not in file_cols:
        print(f"  Skipping: missing address columns")
        continue

    overlapping      = combined_cols & file_cols
    new_address_cols = [
        c for c in file_cols
        if c not in overlapping and c not in ("addr_clean", "zip_clean")
    ]

    if not new_address_cols:
        print(f"  Skipping: no new columns to bring in")
        continue

    address_select = ", ".join(f'a."{c}"' for c in new_address_cols)
    chunk_path = f"/scratch/adl9602/tmp/chunks/{folder}_chunk_{i:03d}.parquet"

    try:
        con.execute(f"""
            COPY (
                SELECT
                    c.addr_clean,
                    c.zip_clean,
                    c.* EXCLUDE (addr_clean, zip_clean),
                    {address_select}
                FROM combined_dedup c
                INNER JOIN (
                    SELECT DISTINCT ON (addr_clean, zip_clean) *,
                        {ADDR_CLEAN_ADDRESS} AS addr_clean,
                        {ZIP_CLEAN_ADDRESS}  AS zip_clean
                    FROM '{fpath}'
                    WHERE PROPERTYADDRESSFULL IS NOT NULL AND trim(PROPERTYADDRESSFULL) != ''
                      AND PROPERTYADDRESSZIP  IS NOT NULL AND trim(PROPERTYADDRESSZIP)  != ''
                ) a
                ON c.addr_clean = a.addr_clean
                AND c.zip_clean = a.zip_clean
            ) TO '{chunk_path}' (FORMAT PARQUET)
        """)
        n = con.execute(f"SELECT count(*) FROM '{chunk_path}'").fetchone()[0]
        total_matched += n
        matched_chunks.append(chunk_path)
        print(f"  Matched {n:,} rows (running total: {total_matched:,})")
    except Exception as e:
        print(f"  Error: {e}")
        continue

if not matched_chunks:
    raise RuntimeError("No chunks produced — check address column names")

print(f"\nCombining {len(matched_chunks)} chunks...")
con.execute(f"""
    COPY (
        SELECT * FROM read_parquet({matched_chunks})
    ) TO '/scratch/adl9602/tx/merged.parquet' (FORMAT PARQUET)
""")

total = con.execute("SELECT count(*) FROM '/scratch/adl9602/tx/merged.parquet'").fetchone()[0]
print(f"\nDone. Total matched rows: {total:,}")
print("Output: /scratch/adl9602/tx/merged_loop.parquet")