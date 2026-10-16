"""
Authors: Anna Li
Date: 2026-09-14

Claude change 09-14: new test script (Builty -> ATTOM address match, stage 1).

Two looser rungs under the six exact tiers of attom_builty.py, run by hand on a
state's step-3 permit file to see what they would recover. Nothing on the
production path changes; the script prints the rates and writes a review
listing of every new pair so the false positives can be read off by eye.

  Rung 7, loose key: house number + first word of the street name (leading
  direction skipped, ordinal endings stripped) + ZIP, accepted only when
  exactly one ATTOM parcel in the ZIP carries that key.

  Rung 8, fuzzy: block on house number + ZIP, then Jaro-Winkler on the street
  string alone (unit, lot, building and trailing town removed). Accepted when
  the best candidate scores at least --jw-threshold and beats the runner-up
  by --jw-margin, or when the ATTOM street is a prefix of the permit street
  (the permit only adds a lot number or a town).

Run: python attom_builty_fuzzy.py --state LA \\
        --permits <build/nfip_attom_pipeline_v2/attom_builty/la_attom_permits.parquet> \\
        --attom <raw/attom/attom_la.parquet or geocoded/la_attom_geocoded.parquet> \\
        --out-dir <folder for the review csv>
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb
import pandas as pd

# attom_builty.py's abbreviation list plus the forms the unmatched permits used
ABBREVIATIONS = [
    ("street", "st"), ("avenue", "ave"), ("boulevard", "blvd"), ("drive", "dr"),
    ("court", "ct"), ("place", "pl"), ("lane", "ln"), ("road", "rd"), ("circle", "cir"),
    ("highway", "hwy"), ("parkway", "pkwy"), ("terrace", "ter"), ("terr", "ter"),
    ("trail", "trl"), ("tr", "trl"), ("square", "sq"), ("cove", "cv"), ("crossing", "xing"),
    ("alley", "aly"), ("point", "pt"), ("north", "n"), ("south", "s"), ("east", "e"),
    ("west", "w"), ("bl", "blvd"), ("av", "ave"), ("mount", "mt"),
    ("apartment", "apt"), ("suite", "ste"), ("unit", "apt"),
]
ORDINAL_WORDS = {"first": "1", "second": "2", "third": "3", "fourth": "4", "fifth": "5",
                 "sixth": "6", "seventh": "7", "eighth": "8", "ninth": "9", "tenth": "10"}
DIRECTIONS = r"(?:n|s|e|w|ne|nw|se|sw)"
STATE_TAIL = r"\b(?:la|fl|tx|louisiana|florida|texas)\b\s*$"
# unit, lot, building tails and the local codes some jurisdictions append (La Porte "clp")
TAIL = r"\s+(?:apt|unit|ste|lot|bldg|building|#|ub clp|clp|parcel).*$"


def clean(text: str) -> str:
    # same spirit as attom_builty.py's cleaning, on either side, plus the fixes above
    s = str(text or "").lower()
    s = re.sub(r"[,.()@]", " ", s)
    s = re.sub(r"\b(\d{5})(?:-\d{4})?\b\s*$", " ", s)          # trailing zip
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(STATE_TAIL, "", s).strip()
    for long, short in ABBREVIATIONS:
        s = re.sub(rf"\b{long}\b", short, s)
    for word, num in ORDINAL_WORDS.items():
        s = re.sub(rf"\b{word}\b", num, s)
    s = re.sub(r"\b(\d+)(?:st|nd|rd|th)\b", r"\1", s)            # 28th -> 28
    s = s.replace("#", " apt ")
    return re.sub(r"\s+", " ", s).strip()


def loose_key(addr: str) -> tuple[str | None, str | None]:
    # house number and the first word of the street name
    m = re.match(rf"^(\d+[a-z]?)\s+(?:{DIRECTIONS}\s+)?([a-z0-9]+)", addr)
    return (m.group(1), m.group(2)) if m else (None, None)


def street_string(addr: str) -> str:
    # what the fuzzy rung compares: the street without house number, tails or town
    s = re.sub(r"^\d+[a-z]?\s+", "", addr)
    s = re.sub(TAIL, "", s)
    s = re.sub(r"\s+\d+\s*$", "", s)                              # bare trailing lot number
    return s.strip()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state", required=True)
    p.add_argument("--permits", required=True, help="{st}_attom_permits.parquet from step 3")
    p.add_argument("--attom", required=True, help="raw attom_{st}.parquet or {st}_attom_geocoded.parquet")
    p.add_argument("--out-dir", required=True, help="folder for the review listing")
    p.add_argument("--jw-threshold", type=float, default=0.90)
    p.add_argument("--jw-margin", type=float, default=0.05)
    p.add_argument("--memory", default="8GB")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--tmp", default=None)
    return p.parse_args()


def attom_columns(con: duckdb.DuckDBPyConnection, path: str) -> tuple[str, str]:
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
    if "PROPERTYADDRESSFULL" in cols:
        return "PROPERTYADDRESSFULL", "PROPERTYADDRESSZIP"
    return "property_address_full", "zip"


def main() -> None:
    args = parse_args()
    state = args.state.upper()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{args.memory}'")
    con.execute(f"SET threads={args.threads}")
    if args.tmp:
        con.execute(f"SET temp_directory='{args.tmp}'")

    # permits the exact tiers left behind. A street field holding only the house
    # number means the rest slid into LOCALITY, so glue them back together
    permits = pd.read_parquet(args.permits)
    n_all = len(permits)
    n_exact = int((permits["attom_match_tier"] != "unmatched").sum())
    u = permits.loc[permits["attom_match_tier"] == "unmatched",
                    ["permit_row_id", "STREET_ADDRESS", "LOCALITY", "zip_clean", "addr_clean"]].copy()
    raw = u["STREET_ADDRESS"].fillna("").astype(str)
    shifted = raw.str.strip().str.fullmatch(r"\d+")
    raw = raw.where(~shifted, raw.str.strip() + " " + u["LOCALITY"].fillna("").astype(str))
    u["addr"] = raw.map(clean)
    u[["hn", "tok"]] = u["addr"].apply(lambda a: pd.Series(loose_key(a)))
    u["street"] = u["addr"].map(street_string)
    u["zip"] = u["zip_clean"].fillna("").astype(str).str.extract(r"^(\d{5})")[0]
    u = u[u["hn"].notna() & u["zip"].notna()]
    con.register("u", u[["permit_row_id", "addr", "hn", "tok", "street", "zip"]])

    # ATTOM parcels in those ZIPs, cleaned the same way, one row per parcel
    addr_col, zip_col = attom_columns(con, args.attom)
    con.execute(f"""
        CREATE TABLE a AS
        SELECT DISTINCT cast(attomid AS varchar) attomid,
               lower(trim("{addr_col}")) attom_address,
               regexp_extract(cast("{zip_col}" AS varchar), '^(\\d{{5}})', 1) zip
        FROM read_parquet('{args.attom}')
        WHERE regexp_extract(cast("{zip_col}" AS varchar), '^(\\d{{5}})', 1) IN (SELECT DISTINCT zip FROM u)
          AND "{addr_col}" IS NOT NULL
    """)
    a = con.execute("SELECT * FROM a").df()
    a["addr"] = a["attom_address"].map(clean)
    a[["hn", "tok"]] = a["addr"].apply(lambda s: pd.Series(loose_key(s)))
    a["street"] = a["addr"].map(street_string)
    a = a[a["hn"].notna()]
    con.register("attom", a[["attomid", "attom_address", "addr", "hn", "tok", "street", "zip"]])

    # rung 7: loose key, unique in the ZIP
    loose = con.execute("""
        WITH c AS (
          SELECT u.permit_row_id, u.addr permit_addr, count(DISTINCT t.attomid) n,
                 min(t.attomid) attomid, min(t.attom_address) attom_address
          FROM u JOIN attom t ON t.zip = u.zip AND t.hn = u.hn AND t.tok = u.tok
          GROUP BY 1, 2)
        SELECT permit_row_id, permit_addr, attomid, attom_address, 'loose_key' tier, 1.0 score FROM c WHERE n = 1
    """).df()
    con.register("loose", loose[["permit_row_id"]])

    # rung 8: block on house number + ZIP, Jaro-Winkler on the street string;
    # a prefix hit (ATTOM street is the start of the permit street) counts as 1
    fuzzy = con.execute(f"""
        WITH cand AS (
          SELECT u.permit_row_id, u.addr permit_addr, u.street permit_street,
                 t.attomid, t.attom_address, t.street attom_street,
                 CASE WHEN length(t.street) >= 4 AND starts_with(u.street, t.street) THEN 1.0
                      ELSE jaro_winkler_similarity(u.street, t.street) END score
          FROM u JOIN attom t ON t.zip = u.zip AND t.hn = u.hn
          WHERE u.permit_row_id NOT IN (SELECT permit_row_id FROM loose)
        ), ranked AS (
          SELECT *, row_number() OVER (PARTITION BY permit_row_id ORDER BY score DESC, attomid) rk,
                 count(*) OVER (PARTITION BY permit_row_id) n_cand,
                 max(score) OVER (PARTITION BY permit_row_id) best
          FROM cand
        ), second AS (
          SELECT permit_row_id, max(score) runner_up FROM ranked WHERE rk = 2 GROUP BY 1
        )
        SELECT r.permit_row_id, r.permit_addr, r.attomid, r.attom_address, 'jaro_winkler' tier,
               round(r.score, 3) score, r.n_cand, coalesce(s.runner_up, 0) runner_up, r.permit_street, r.attom_street
        FROM ranked r LEFT JOIN second s USING (permit_row_id)
        WHERE r.rk = 1 AND r.score >= {args.jw_threshold}
          AND (s.runner_up IS NULL OR r.score - s.runner_up >= {args.jw_margin})
    """).df()

    # rates and the review listing
    n_loose, n_fuzzy = len(loose), len(fuzzy)
    print(f"\n{state}: {n_all:,} permits")
    print(f"  exact tiers        {n_exact:>6,}  {n_exact / n_all:6.1%}")
    print(f"  + loose key        {n_loose:>6,}  -> {(n_exact + n_loose) / n_all:6.1%}")
    print(f"  + Jaro-Winkler     {n_fuzzy:>6,}  -> {(n_exact + n_loose + n_fuzzy) / n_all:6.1%}")
    print(f"  still unmatched    {n_all - n_exact - n_loose - n_fuzzy:>6,}")

    review = pd.concat([loose, fuzzy], ignore_index=True).sort_values(["tier", "score", "permit_addr"])
    path = out_dir / f"{state.lower()}_fuzzy_review.csv"
    review.to_csv(path, index=False)
    print(f"  review listing: {path}")
    pd.set_option("display.width", 200)
    pd.set_option("display.max_colwidth", 45)
    print("\nJaro-Winkler pairs, lowest scores first (the ones to read):")
    print(fuzzy.sort_values("score")[["permit_addr", "attom_address", "score", "runner_up", "n_cand"]].head(25).to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
