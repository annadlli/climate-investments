"""
Authors: Anna Li
Date: 2026-10-04

Description: Downloads the ATTOM building-permit files from Dewey, one folder per
    endpoint in the manifest, and combines each endpoint's monthly files into one
    parquet, raw/attom_permits/attom_permits_<name>.parquet.
Source: Dewey ATTOM building permits; the endpoints are licensed URLs and live in a
    private manifest (anna_private/attom_permits_endpoints.csv: name, endpoint, table, notes)
"""

import argparse
import re  # column detection for the status tables
import os
import shutil
from pathlib import Path

import duckdb
import pandas as pd


def main():
    p = argparse.ArgumentParser(description="Download and combine the ATTOM permit files from Dewey.")
    p.add_argument("--data", required=True, help="Data root with raw/ (from master.do).")
    p.add_argument("--manifest", required=True, help="CSV with columns name, endpoint.")
    p.add_argument("--api-key", default=os.environ.get("DEWEY_API_KEY"),
                   help="Dewey API key (default: the DEWEY_API_KEY environment variable).")
    p.add_argument("--memory", default="6GB")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--tmp", default=None, help="DuckDB spill directory (default: raw/attom_permits/duckdb_tmp).")
    p.add_argument("--keep-monthly", action="store_true",
                   help="Keep the monthly Dewey files after combining (default: delete them).")
    args = p.parse_args()

    out = Path(args.data) / "raw" / "attom_permits"
    tmp = Path(args.tmp) if args.tmp else out / "duckdb_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    manifest = pd.read_csv(args.manifest)
    if "table" not in manifest.columns:
        manifest["table"] = "permit"
    manifest["table"] = manifest["table"].fillna("permit").str.strip().str.lower()
    unknown = sorted(set(manifest["table"]) - {"permit", "status"})
    if unknown:
        raise ValueError(f"manifest column `table` must be permit or status, got {unknown}")

    for name, endpoint, table in zip(manifest["name"], manifest["endpoint"], manifest["table"]):
        combined = out / (f"attom_permits_{name}.parquet" if table == "permit"
                          else f"attom_permit_status_{name}.parquet")
        if combined.exists():
            print(f"{name}: {combined.name} exists, skipped", flush=True)
            continue

        # download; files already in the folder are skipped, so an interrupted run resumes
        import deweydatapy as ddp
        monthly = out / "dewey" / name
        monthly.mkdir(parents=True, exist_ok=True)
        files = ddp.get_file_list(args.api_key, endpoint, print_info=True)
        print(f"{name}: downloading {len(files)} files to {monthly}", flush=True)
        ddp.download_files(files, str(monthly), skip_exists=True)
        n_local = sum(1 for f in monthly.iterdir() if f.suffix == ".parquet")
        # a short download would silently drop months of permits from every count downstream
        if n_local < len(files):
            raise RuntimeError(f"{name}: {n_local} of {len(files)} files on disk. Rerun to resume.")

        # combine: DuckDB streams the monthly files, so the table never has to fit in memory.
        # The id and dollar columns arrive as 38-digit decimals, cast to integer and double
        con = duckdb.connect()
        con.execute(f"SET memory_limit = '{args.memory}'")
        con.execute(f"SET threads = {args.threads}")
        con.execute(f"SET temp_directory = '{tmp}'")
        con.execute("SET preserve_insertion_order = false")
        part = combined.with_suffix(".parquet.part")
        if table == "permit":
            select = """
                SELECT * EXCLUDE (ATTOMID, BUILDINGPERMITID, FEES, JOBVALUE),
                       CAST(ATTOMID AS BIGINT) AS ATTOMID,
                       CAST(BUILDINGPERMITID AS BIGINT) AS BUILDINGPERMITID,
                       CAST(FEES AS DOUBLE) AS FEES,
                       CAST(JOBVALUE AS DOUBLE) AS JOBVALUE"""
        else:
            # the status files do not name the status column BUILDINGPERMITSTATUS,
            # so the three columns are found in the files' own schema by pattern (output names unchanged)
            cols = [r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{monthly}/*.parquet', union_by_name = true)").fetchall()]
            print(f"{name}: status file columns {cols}", flush=True)

            def find(patterns, what):
                for pat in patterns:
                    hits = [c for c in cols if re.fullmatch(pat, c, flags=re.I)]
                    if hits:
                        return hits[0]
                raise RuntimeError(f"{name}: no {what} column among {cols}; add its name to the patterns")
            id_col = find([r"BUILDINGPERMITID", r".*PERMIT.*ID", r".*PERMITNUMBER"], "permit id")
            st_col = find([r"BUILDINGPERMITSTATUS", r"(?!.*DATE).*PERMIT.*STATUS.*", r"(?!.*DATE).*STATUS.*"], "status")
            dt_col = find([r"EFFECTIVEDATE", r".*EFFECTIVE.*DATE.*", r".*STATUS.*DATE.*",
                           r"(?!.*(PUBLICATION|DBCREATE|DBUPDATE)).*DATE.*"], "effective date")
            print(f"{name}: using id = {id_col}, status = {st_col}, date = {dt_col}", flush=True)
            select = f"""
                SELECT CAST("{id_col}" AS BIGINT) AS BUILDINGPERMITID,
                       upper(trim(CAST("{st_col}" AS VARCHAR))) AS BUILDINGPERMITSTATUS,
                       CAST("{dt_col}" AS DATE) AS EFFECTIVEDATE"""
        con.execute(f"""
            COPY (
                {select}
                FROM read_parquet('{monthly}/*.parquet', union_by_name = true)
            ) TO '{part}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        part.rename(combined)
        n = con.execute(f"SELECT count(*) FROM read_parquet('{combined}')").fetchone()[0]
        unit = "permits" if table == "permit" else "status rows"
        print(f"{name}: {n:,} {unit} -> {combined} ({combined.stat().st_size / 1e9:.1f} GB)", flush=True)
        if table == "status":
            print(con.execute(f"""SELECT BUILDINGPERMITSTATUS AS status, count(*) AS rows_
                                    FROM read_parquet('{combined}') GROUP BY 1 ORDER BY 2 DESC""")
                  .df().to_string(index=False), flush=True)
        con.close()
        if not args.keep_monthly:
            shutil.rmtree(monthly)


if __name__ == "__main__":
    main()
