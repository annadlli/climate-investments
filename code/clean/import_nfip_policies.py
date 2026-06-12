"""Extract NFIP policy records for selected states from the raw ~29GB OpenFEMA CSV.

One duckdb pass (memory-safe) filters to --states and writes one CSV per state for
clean_nfip_policies.do to import and clean. Read as strings so leading zeros in
zip/county/tract/community survive. Kept broad -- trim columns in the .do, not here.
"""

import argparse
from pathlib import Path

import duckdb

# Wagner's Atlantic + Gulf Coast states (default scope)
WAGNER_STATES = "AL CT DE FL GA LA ME MD MA MS NH NJ NY NC PA RI SC TX VT VA"
DROP = ["reportedCity", "mapPanelNumber", "mapPanelSuffix", "femaRegion"]

def main():
    p = argparse.ArgumentParser(description="Extract per-state NFIP policy records.")
    p.add_argument("--data", required=True, help="Data root with raw/ and clean/ (from master.do).")
    p.add_argument("--states", default=WAGNER_STATES, help="2-letter abbreviations (default: Wagner's states)")
    args = p.parse_args()

    states = [s.strip().upper() for s in args.states.replace(",", " ").split() if s.strip()]
    data = Path(args.data)
    src = data / "raw" / "FimaNfipPoliciesV2.csv"
    out_dir = data / "clean" / "nfip_policies_raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    excl = ", ".join(f'"{c}"' for c in DROP)
    inlist = ", ".join(f"'{s}'" for s in states)

    con = duckdb.connect()
    print(f"Scanning {src.name} for {len(states)} states ...", flush=True)
    con.execute(f"""
        CREATE TABLE pol AS
        SELECT * EXCLUDE ({excl})
        FROM read_csv_auto('{src}', all_varchar=true, header=true)
        WHERE upper("propertyState") IN ({inlist});
    """)
    for st in states:
        out = out_dir / f"{st.lower()}.csv"
        con.execute(f"""COPY (SELECT * FROM pol WHERE upper("propertyState") = '{st}')
                        TO '{out}' (FORMAT CSV, HEADER);""")
        n = con.execute(f"""SELECT count(*) FROM pol WHERE upper("propertyState") = '{st}'""").fetchone()[0]
        print(f"  {st}: {n:,} rows", flush=True)


if __name__ == "__main__":
    main()
