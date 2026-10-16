"""
Authors: Claude (for Anna Li)
Date: 2026-10-15
Claude change 10-15 (new file). Diagnostic for the ATTOM permit status history, run by hand on
the cluster login node (the combined status tables live only there):

    /scratch/adl9602/venvs/py311/bin/python status_history_check.py --data /scratch/adl9602/tx/data

Why: on the 10-15 download, 92% of elevation permits with a FINAL/COMPLETE date are finaled in
their issue year, and for 40% of permits whose snapshot STATUS says "complete" the history's
last stage is APPLIED or ISSUED. Both fit completion rows whose EFFECTIVEDATE is a placeholder
that extract_attom_permits.py nulls, which drops them from DATE_FINALED and STATUS_LAST.
This script counts status rows by stage and by date class (null, placeholder, out of range,
valid) for every permit and for the elevation candidates, and prints how many candidates
have a completion row at all versus a completion row with a valid date. Nothing is written.
"""
import argparse, glob
import duckdb

PLACEHOLDERS = ("1900-01-01", "1901-01-01", "1970-01-01", "1979-12-31", "1980-01-01", "1999-12-31",
                "2000-01-01", "2099-12-31", "9999-12-31", "0001-01-01")

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--data", required=True); ap.add_argument("--memory", default="32GB")
    a = ap.parse_args()
    status = f"{a.data}/raw/attom_permits/attom_permit_status_*.parquet"
    cands = sorted(glob.glob(f"{a.data}/clean/permits_raw/*.csv"))
    if not glob.glob(status):
        raise SystemExit(f"no status parquet under {a.data}/raw/attom_permits")
    con = duckdb.connect(); con.execute(f"SET memory_limit='{a.memory}'")
    ph = ", ".join(f"DATE '{p}'" for p in PLACEHOLDERS)
    con.execute(f"""
        CREATE VIEW s AS
        SELECT BUILDINGPERMITID AS id, BUILDINGPERMITSTATUS AS st, EFFECTIVEDATE AS d,
               CASE WHEN EFFECTIVEDATE IS NULL THEN 'null'
                    WHEN EFFECTIVEDATE IN ({ph}) THEN 'placeholder'
                    WHEN year(EFFECTIVEDATE) < 1980 OR year(EFFECTIVEDATE) > 2026 THEN 'out_of_range'
                    ELSE 'valid' END AS dclass
        FROM read_parquet('{status}', union_by_name = true)""")
    print("=== all status rows: stage x date class");
    print(con.execute("SELECT st, dclass, count(*) n FROM s GROUP BY 1,2 ORDER BY 1,2").df().pivot(index="st", columns="dclass", values="n").fillna(0).astype(int).to_string())
    print("\n=== most common raw EFFECTIVEDATE values (top 15)")
    print(con.execute("SELECT d, count(*) n FROM s GROUP BY 1 ORDER BY 2 DESC LIMIT 15").df().to_string(index=False))
    print("\n=== rows per permit"); print(con.execute("SELECT quantile_cont(n, [0.5, 0.9, 0.99]) FROM (SELECT id, count(*) n FROM s GROUP BY 1)").fetchone())
    if cands:
        files = "', '".join(cands)
        con.execute(f"""CREATE VIEW c AS SELECT DISTINCT cast(PERMIT_ID AS varchar) AS id, STATE, STATUS AS snapshot
                        FROM read_csv_auto(['{files}'], union_by_name = true, all_varchar = true)""")
        print("\n=== elevation candidates (permits_raw): completion rows in the history, by state")
        print(con.execute("""
            SELECT c.STATE,
                   count(*) AS candidates,
                   count(DISTINCT CASE WHEN s.id IS NOT NULL THEN c.id END) AS with_history,
                   count(DISTINCT CASE WHEN s.st IN ('FINAL','COMPLETE') THEN c.id END) AS completion_row_any_date,
                   count(DISTINCT CASE WHEN s.st IN ('FINAL','COMPLETE') AND s.dclass = 'valid' THEN c.id END) AS completion_row_valid_date,
                   count(DISTINCT CASE WHEN lower(trim(c.snapshot)) IN ('complete','final','completed','finaled') THEN c.id END) AS snapshot_says_complete,
                   count(DISTINCT CASE WHEN lower(trim(c.snapshot)) IN ('complete','final','completed','finaled')
                                        AND s.st IN ('FINAL','COMPLETE') THEN c.id END) AS snapshot_complete_and_row
            FROM c LEFT JOIN s ON cast(s.id AS varchar) = c.id GROUP BY 1 ORDER BY 2 DESC""").df().to_string(index=False))
        print("\n=== candidates: date class of their completion rows")
        print(con.execute("""SELECT dclass, count(*) n FROM s WHERE st IN ('FINAL','COMPLETE')
                             AND cast(id AS varchar) IN (SELECT id FROM c) GROUP BY 1 ORDER BY 2 DESC""").df().to_string(index=False))
        print("\n=== candidates: do all of a permit's rows share one date? (share of permits)")
        print(con.execute("""SELECT avg(CASE WHEN nd = 1 THEN 1 ELSE 0 END) one_date, avg(CASE WHEN nd >= 3 THEN 1 ELSE 0 END) three_plus
                             FROM (SELECT id, count(DISTINCT d) nd, count(*) n FROM s WHERE cast(id AS varchar) IN (SELECT id FROM c) GROUP BY 1)
                             """).fetchone())

if __name__ == "__main__":
    main()
