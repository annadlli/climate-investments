"""
Author: Vendela Norman
Date: 2026-07-23

Description: Splits the raw Builty permits parquet into per-state extracts, keeping
    only permits whose DESCRIPTION plausibly refers to a flood elevation. Writes one
    CSV per state into clean/builty_raw/.

"""

import argparse
from pathlib import Path

import duckdb

# Dropped: no analytic value and 47% of the row. CONTACTS alone is 36% and holds
# contractor names/phones (PII we have no use for).
DROP = ["CONTACTS", "ATTRIBUTES", "PROJECTS"]

# Candidate net -- recall, not precision. Anything an elevation permit might say.
CANDIDATE_PATTERNS = [
    r"elevat",
    r"rais(e|ed|ing)",
    r"lift(ed|ing)",
    r"jack(ed|ing) up",
    r"flood",
    r"fema",
    r"base flood elevation",
    r"finished floor elevation",
    r"freeboard",
    r"floodplain",
    r"\bbfe\b",
    r"\bffe\b",
    r"\bnfip\b",
    r"\bicc\b",
    r"flood plain",
    r"flood zone",
    r"hazard mitigation",
    r"substantial damage",
    r"substantially damaged",
    r"substantial improvement",
    r"substantially improved",
    r"storm surge",
    r"out of (the )?floodplain",
]


def main():
    p = argparse.ArgumentParser(description="Extract per-state Builty elevation-candidate permits.")
    p.add_argument("--data", required=True, help="Data root with raw/ and clean/ (from master.do).")
    p.add_argument("--states", required=True, help="2-letter abbreviations, space- or comma-separated (passed from master.do's `states`).")
    args = p.parse_args()

    states = [s.strip().upper() for s in args.states.replace(",", " ").split() if s.strip()]
    data = Path(args.data)
    src = data / "raw" / "builty_all.parquet"
    out_dir = data / "clean" / "builty_raw"
    out_dir.mkdir(parents=True, exist_ok=True)
    inlist = ", ".join(f"'{s}'" for s in states)
    excl = ", ".join(f'"{c}"' for c in DROP)
    net = "|".join(CANDIDATE_PATTERNS)

    con = duckdb.connect()
    print(f"Scanning {src.name} for {len(states)} states ...", flush=True)
    con.execute(f"""
        CREATE TABLE permits AS
        SELECT * EXCLUDE ({excl})
        FROM read_parquet('{src}')
        WHERE upper("STATE") IN ({inlist})
          AND "DESCRIPTION" IS NOT NULL
          AND regexp_matches(lower("DESCRIPTION"), '{net}');
    """)
    for st in states:
        out = out_dir / f"{st.lower()}.csv"
        con.execute(f"""COPY (SELECT * FROM permits WHERE upper("STATE") = '{st}')
                        TO '{out}' (FORMAT CSV, HEADER);""")
        n = con.execute(f"""SELECT count(*) FROM permits WHERE upper("STATE") = '{st}'""").fetchone()[0]
        print(f"  {st}: {n:,} candidate permits", flush=True)


if __name__ == "__main__":
    main()
