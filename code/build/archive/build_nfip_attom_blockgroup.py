"""Authors: Anna Li and Vendela Norman
Date: 2026-08-10

Match NFIP policies to ATTOM homes using a block-group-first extension of
Wagner's sequential one-to-one cell assignment. Keep every policy, including
unmatched policies, and report property multiplicity within each active tier.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd


SINGLE_FAMILY_CODE = "385"
VALUE_COLUMNS = [
    "market_value_total",
    "market_value_land",
    "market_value_improvements",
    "assessed_value_total",
    "assessed_value_improvements",
    "previous_assessed_value",
    "last_sale_price",
]


def quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    # Small CLI wrapper for the state-level NFIP match job.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data", required=True, help="Data root passed from master.do."
    )
    parser.add_argument("--state", required=True, help="Two-letter state abbreviation.")
    parser.add_argument(
        "--attom", help="Override the geocoded ATTOM property-year parquet."
    )
    parser.add_argument("--nfhl", help="Override the ATTOMID-level NFHL match parquet.")
    parser.add_argument("--nfip", help="Override the cleaned NFIP state .dta file.")
    parser.add_argument("--out", help="Override the output parquet.")
    parser.add_argument("--tmp", help="DuckDB spill directory.")
    parser.add_argument("--memory", default="24GB")
    parser.add_argument("--threads", default=4, type=int)
    parser.add_argument(
        "--priority-attomids",
        help="Optional Builty-permit parquet; matched ATTOMIDs sort first only for sensitivity analysis.",
    )
    parser.add_argument("--cell-diagnostics", help="CSV with property counts per active tier cell.")
    return parser.parse_args()


def normalize_identifier(series: pd.Series, width: int | None = None) -> pd.Series:
    # Clean up IDs so ZIPs, communities, and zone labels line up across files.
    values = series.astype("string").str.strip().str.upper()
    values = values.replace({"<NA>": pd.NA, "NAN": pd.NA, "NONE": pd.NA, "": pd.NA})
    if width:
        numeric = values.str.fullmatch(r"\d+").fillna(False)
        values.loc[numeric] = values.loc[numeric].str.zfill(width)
    return values


def normalize_zone(series: pd.Series) -> pd.Series:
    # Turn AE/A1 and VE/V1 type labels into the same flood-zone buckets 
    zone = normalize_identifier(series).str.replace(" ", "", regex=False)
    numbered_a = zone.str.fullmatch(r"A\d{1,2}").fillna(False)
    numbered_v = zone.str.fullmatch(r"V\d{1,2}").fillna(False)
    zone.loc[numbered_a] = "AE"
    zone.loc[numbered_v] = "VE"
    return zone


def flood_risk_category(series: pd.Series) -> pd.Series:
    # High-risk vs low-risk split for the Wagner cell definitions.
    zone = normalize_zone(series)
    out = pd.Series(pd.NA, index=zone.index, dtype="string")
    out.loc[zone.isin(["A", "AE", "AH", "AO", "V", "VE"])] = "high_risk"
    out.loc[zone.isin(["D", "C", "B", "X", "XE"])] = "low_risk"
    return out


def resolve_inputs(
    args: argparse.Namespace, data: Path, state: str
) -> tuple[Path, Path, Path, Path]:
    # Point to the right ATTOM, NFHL, and NFIP files for each state run.
    attom = (
        Path(args.attom)
        if args.attom
        else data / "build" / "geocoded" / f"{state}_attom_geocoded.parquet"
    )
    nfhl = (
        Path(args.nfhl)
        if args.nfhl
        else data / "build" / "nfhl_matches" / f"{state}_attom_nfhl.parquet"
    )
    nfip = (
        Path(args.nfip)
        if args.nfip
        else data / "clean" / "nfip_policies_state" / f"{state}.dta"
    )
    out = (
        Path(args.out)
        if args.out
        else data / "build" / "nfhl_matches" / f"{state}_nfip_attom_wagner.parquet"
    )
    return attom, nfhl, nfip, out


def load_nfip(path: Path, state: str) -> pd.DataFrame:
    frame = pd.read_stata(path, convert_categoricals=False)
    frame.columns = [str(column).lower() for column in frame.columns]
    # Build the keys we need for the Wagner matching cells.
    frame.insert(0, "_nfip_rowid", range(1, len(frame) + 1))
    frame["state"] = state.upper()
    frame["zip_key"] = normalize_identifier(frame["zipcode"], 5)
    frame["blockgroup_key"] = normalize_identifier(frame["censusblockgroupfips"], 12)
    frame["community_key"] = normalize_identifier(frame["nfipratedcommunitynumber"], 6)
    frame["flood_zone_key"] = normalize_zone(frame["ratedfloodzone"])
    frame["flood_risk_key"] = flood_risk_category(frame["ratedfloodzone"])
    frame["construction_year"] = pd.to_numeric(
        frame["construction_year"], errors="coerce"
    ).astype("Int64")
    frame["policy_year"] = pd.to_numeric(frame["policy_year"], errors="coerce").astype(
        "Int64"
    )
    frame["postfirm_key"] = pd.to_numeric(frame["postfirm"], errors="coerce").astype(
        "Int64"
    )
    frame["construction_5yr"] = ((frame["construction_year"] // 5) * 5).where(
        frame["construction_year"].between(1920, 2017)
    )
    frame["construction_decade"] = ((frame["construction_year"] // 10) * 10).where(
        frame["construction_year"].between(1920, 2019)
    )
    return frame


def create_tables(
    con: duckdb.DuckDBPyConnection,
    nfip: pd.DataFrame,
    attom_path: Path,
    nfhl_path: Path,
    priority_ids: pd.DataFrame,
) -> None:
    # Load the NFIP panel and set up the fields that will hold each assignment.
    con.register("nfip_frame", nfip)
    con.execute("CREATE TABLE nfip AS SELECT * FROM nfip_frame")
    con.unregister("nfip_frame")
    con.execute("ALTER TABLE nfip ADD COLUMN wagner_tier VARCHAR")
    con.execute("ALTER TABLE nfip ADD COLUMN matched_attomid VARCHAR")
    con.execute("ALTER TABLE nfip ADD COLUMN attom_value_year INTEGER")
    con.execute("ALTER TABLE nfip ADD COLUMN attom_value_lag INTEGER")
    for column in VALUE_COLUMNS:
        con.execute(f"ALTER TABLE nfip ADD COLUMN attom_{column} DOUBLE")
    con.register("priority_frame", priority_ids)
    con.execute(
        "CREATE TEMP TABLE priority_ids AS SELECT DISTINCT cast(attomid AS varchar) attomid FROM priority_frame"
    )
    con.unregister("priority_frame")

    attom = quote(str(attom_path))
    nfhl = quote(str(nfhl_path))

    # Keep a single ATTOM value record per property-year, then merge in the NFHL
    # flood and community information needed for cell matching.
    con.execute(
        f"""
        CREATE TABLE tax_values AS
        SELECT
            cast(attomid AS varchar) AS attomid,
            cast(year AS integer) AS tax_year,
            max(cast(market_value_total AS double)) AS market_value_total,
            max(cast(market_value_land AS double)) AS market_value_land,
            max(cast(market_value_improvements AS double)) AS market_value_improvements,
            max(cast(assessed_value_total AS double)) AS assessed_value_total,
            max(cast(assessed_value_improvements AS double)) AS assessed_value_improvements,
            max(cast(previous_assessed_value AS double)) AS previous_assessed_value,
            max(cast(last_sale_price AS double)) AS last_sale_price
        FROM read_parquet({attom})
        WHERE trim(cast(property_use_std AS varchar)) = {quote(SINGLE_FAMILY_CODE)}
          AND cast(year AS integer) BETWEEN 1900 AND 2100
        GROUP BY 1, 2
    """
    )

    con.execute(
        f"""
        CREATE TEMP TABLE candidate_properties AS
        WITH a AS (
            SELECT
                cast(attomid AS varchar) AS attomid,
                max(nullif(trim(cast(zip AS varchar)), '')) AS zip_raw,
                max(nullif(trim(cast(censusblockgroupfips AS varchar)), '')) AS blockgroup_raw,
                max(cast(construction_year AS integer)) AS construction_year
            FROM read_parquet({attom})
            WHERE trim(cast(property_use_std AS varchar)) = {quote(SINGLE_FAMILY_CODE)}
            GROUP BY 1
        ), n AS (
            SELECT
                cast(attomid AS varchar) AS attomid,
                upper(trim(cast(nfip_community_id AS varchar))) AS community_key,
                CASE
                    WHEN regexp_matches(upper(replace(trim(cast(fld_zone AS varchar)), ' ', '')), '^A[0-9]{{1,2}}$') THEN 'AE'
                    WHEN regexp_matches(upper(replace(trim(cast(fld_zone AS varchar)), ' ', '')), '^V[0-9]{{1,2}}$') THEN 'VE'
                    ELSE upper(replace(trim(cast(fld_zone AS varchar)), ' ', ''))
                END AS flood_zone_key,
                cast(initial_firm_year AS integer) AS initial_firm_year,
                cast(nfhl_flood_matched AS boolean) AS nfhl_flood_matched,
                cast(nfhl_community_matched AS boolean) AS nfhl_community_matched
            FROM read_parquet({nfhl})
        )
        SELECT
            a.attomid,
            CASE
                WHEN regexp_matches(a.zip_raw, '^\\d{{5}}') THEN regexp_extract(a.zip_raw, '^(\\d{{5}})', 1)
            END AS zip_key,
            CASE WHEN regexp_matches(a.blockgroup_raw, '^\\d{{12}}$')
                 THEN a.blockgroup_raw END AS blockgroup_key,
            nullif(n.community_key, '') AS community_key,
            nullif(n.flood_zone_key, '') AS flood_zone_key,
            CASE WHEN n.flood_zone_key IN ('A','AE','AH','AO','V','VE') THEN 'high_risk'
                 WHEN n.flood_zone_key IN ('D','C','B','X','XE') THEN 'low_risk' END AS flood_risk_key,
            a.construction_year,
            CASE WHEN a.construction_year BETWEEN 1920 AND 2017
                 THEN (a.construction_year // 5) * 5 END AS construction_5yr,
            CASE WHEN a.construction_year BETWEEN 1920 AND 2019
                 THEN (a.construction_year // 10) * 10 END AS construction_decade,
            n.initial_firm_year,
            CASE WHEN n.initial_firm_year IS NOT NULL THEN cast(
                 a.construction_year >= CASE WHEN n.initial_firm_year < 1975
                                             THEN 1975 ELSE n.initial_firm_year + 1 END
                 AS integer) END AS postfirm_key,
            cast(pr.attomid IS NOT NULL AS integer) AS builty_priority
        FROM a INNER JOIN n USING (attomid)
        LEFT JOIN priority_ids pr USING (attomid)
        WHERE a.construction_year BETWEEN 1700 AND 2027
          AND n.nfhl_flood_matched
          AND n.nfhl_community_matched
          AND n.flood_zone_key IN ('A','AE','AH','AO','V','VE')
    """
    )

    # Restrict to houses that actually have a plausible match in at least one cell.
    con.execute(
        """
        CREATE TABLE properties AS
        SELECT p.*
        FROM candidate_properties p
        WHERE EXISTS (
            SELECT 1 FROM nfip n
            WHERE n.flood_risk_key = p.flood_risk_key
              AND (
                  (n.blockgroup_key = p.blockgroup_key AND n.construction_year = p.construction_year)
                  OR (n.zip_key = p.zip_key AND n.construction_year = p.construction_year)
                  OR (n.community_key = p.community_key AND n.construction_year = p.construction_year)
                  OR (n.community_key = p.community_key AND n.construction_5yr = p.construction_5yr
                      AND n.postfirm_key = p.postfirm_key)
                  OR (n.community_key = p.community_key AND n.construction_decade = p.construction_decade
                      AND n.postfirm_key = p.postfirm_key)
              )
        )
    """
    )

    # Trim the tax history to the houses that can plausibly be used in the match.
    con.execute(
        """
        CREATE TABLE candidate_tax_values AS
        SELECT t.* FROM tax_values t INNER JOIN properties p USING (attomid)
    """
    )
    con.execute("DROP TABLE tax_values")
    con.execute("ALTER TABLE candidate_tax_values RENAME TO tax_values")
    print(
        "ATTOM static Wagner candidates: "
        f"{con.execute('SELECT count(*) FROM properties').fetchone()[0]:,}",
        flush=True,
    )

    # Expand each house across policy years and attach the closest earlier tax value.
    con.execute(
        """
        CREATE TABLE housing AS
        WITH bounds AS (
            SELECT min(policy_year)::integer AS first_year,
                   max(policy_year)::integer AS last_year
            FROM nfip WHERE policy_year IS NOT NULL
        ), years AS (
            SELECT unnest(generate_series(first_year, last_year))::integer AS policy_year
            FROM bounds
        )
        SELECT
            p.*, y.policy_year,
            v.tax_year AS attom_value_year,
            y.policy_year - v.tax_year AS attom_value_lag,
            v.market_value_total,
            v.market_value_land,
            v.market_value_improvements,
            v.assessed_value_total,
            v.assessed_value_improvements,
            v.previous_assessed_value,
            v.last_sale_price,
            false AS assigned
        FROM properties p
        CROSS JOIN years y
        JOIN LATERAL (
            SELECT * FROM tax_values v
            WHERE v.attomid = p.attomid AND v.tax_year <= y.policy_year
            ORDER BY v.tax_year DESC LIMIT 1
        ) v ON true
        WHERE p.construction_year <= y.policy_year
          AND EXISTS (
              SELECT 1 FROM nfip n
              WHERE n.policy_year = y.policy_year
                AND n.flood_risk_key = p.flood_risk_key
                AND (
                    (n.blockgroup_key = p.blockgroup_key AND n.construction_year = p.construction_year)
                    OR (n.zip_key = p.zip_key AND n.construction_year = p.construction_year)
                    OR (n.community_key = p.community_key AND n.construction_year = p.construction_year)
                    OR (n.community_key = p.community_key AND n.construction_5yr = p.construction_5yr
                        AND n.postfirm_key = p.postfirm_key)
                    OR (n.community_key = p.community_key AND n.construction_decade = p.construction_decade
                        AND n.postfirm_key = p.postfirm_key)
                )
          )
    """
    )


def apply_tier(con: duckdb.DuckDBPyConnection, keys: list[str], label: str) -> int:
    # Rank policies and houses inside each cell, then pair them one-to-one.
    key_sql = ", ".join(keys)
    nonmissing = " AND ".join(f"{key} IS NOT NULL" for key in keys)
    con.execute("DROP TABLE IF EXISTS tier_hits")
    con.execute(
        f"""
        CREATE TEMP TABLE tier_hits AS
        WITH policy_ranked AS (
            SELECT _nfip_rowid, {key_sql},
                   row_number() OVER (PARTITION BY {key_sql} ORDER BY _nfip_rowid) AS cell_rank
            FROM nfip
            WHERE wagner_tier IS NULL AND {nonmissing}
        ), house_ranked AS (
            SELECT attomid, attom_value_year, attom_value_lag,
                   {', '.join(VALUE_COLUMNS)}, {key_sql},
                   row_number() OVER (
                       PARTITION BY {key_sql} ORDER BY attomid
                   ) AS cell_rank
            FROM housing
            WHERE NOT assigned AND {nonmissing}
        )
        SELECT p._nfip_rowid, h.* EXCLUDE ({key_sql}, cell_rank)
        FROM policy_ranked p
        INNER JOIN house_ranked h USING ({key_sql}, cell_rank)
    """
    )
    count = con.execute("SELECT count(*) FROM tier_hits").fetchone()[0]
    # Save the matched ATTOM property and value onto the policy record.
    assignments = [
        f"wagner_tier = {quote(label)}",
        "matched_attomid = h.attomid",
        "attom_value_year = h.attom_value_year",
        "attom_value_lag = h.attom_value_lag",
    ] + [f"attom_{column} = h.{column}" for column in VALUE_COLUMNS]
    con.execute(
        f"""
        UPDATE nfip AS n SET {', '.join(assignments)}
        FROM tier_hits h WHERE n._nfip_rowid = h._nfip_rowid
    """
    )
    con.execute(
        """
        UPDATE housing AS x SET assigned = true
        FROM tier_hits h
        WHERE x.attomid = h.attomid AND x.policy_year = (
            SELECT policy_year FROM nfip n WHERE n._nfip_rowid = h._nfip_rowid
        )
    """
    )
    print(f"  {label:<35} {count:,}")
    return int(count)


def cell_diagnostics(con: duckdb.DuckDBPyConnection, keys: list[str], label: str) -> dict:
    """Measure property multiplicity before assigning the current tier."""
    key_sql = ", ".join(keys)
    nonmissing = " AND ".join(f"{key} IS NOT NULL" for key in keys)
    row = con.execute(f"""
        WITH policy_cells AS (
            SELECT {key_sql}, count(*) n_policies
            FROM nfip WHERE wagner_tier IS NULL AND {nonmissing}
            GROUP BY {key_sql}
        ), property_cells AS (
            SELECT {key_sql}, count(*) n_properties
            FROM housing WHERE NOT assigned AND {nonmissing}
            GROUP BY {key_sql}
        ), active AS (
            SELECT p.n_policies, h.n_properties
            FROM policy_cells p INNER JOIN property_cells h USING ({key_sql})
        )
        SELECT count(*), sum(n_policies), sum(n_properties), avg(n_properties),
               median(n_properties), quantile_cont(n_properties, .9), max(n_properties),
               sum((n_properties=1)::integer)
        FROM active
    """).fetchone()
    names = ["cells", "policies_in_active_cells", "properties_in_active_cells",
             "mean_properties_per_cell", "median_properties_per_cell",
             "p90_properties_per_cell", "maximum_properties_per_cell", "singleton_cells"]
    result = {"tier": label, **dict(zip(names, row))}
    result["singleton_cell_share"] = (
        result["singleton_cells"] / result["cells"] if result["cells"] else pd.NA
    )
    return result


def main() -> None:
    # Resolve the file paths and the optional priority list for the run.
    args = parse_args()
    data = Path(args.data)
    state = args.state.lower()
    attom_path, nfhl_path, nfip_path, out = resolve_inputs(args, data, state)
    nfip = load_nfip(nfip_path, state)
    if args.priority_attomids:
        priority = pd.read_parquet(args.priority_attomids, columns=["ATTOMID"])
        priority = priority.loc[priority["ATTOMID"].notna(), ["ATTOMID"]].copy()
        priority["attomid"] = (
            priority["ATTOMID"].astype("string").str.replace(r"\.0$", "", regex=True)
        )
        priority_ids = priority[["attomid"]].drop_duplicates()
    else:
        priority_ids = pd.DataFrame({"attomid": pd.Series(dtype="string")})

    # Set up DuckDB for the state-level build.
    con = duckdb.connect()
    con.execute(f"SET memory_limit={quote(args.memory)}")
    con.execute(f"SET threads={args.threads}")
    con.execute("SET preserve_insertion_order=false")
    if args.tmp:
        tmp = Path(args.tmp)
        tmp.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory={quote(str(tmp))}")

    # Build the candidate tables and step through the four Wagner tiers in order.
    print(f"NFIP policies: {len(nfip):,}")
    create_tables(con, nfip, attom_path, nfhl_path, priority_ids)
    print(
        f"ATTOM single-family properties with NFHL: {con.execute('SELECT count(*) FROM properties').fetchone()[0]:,}"
    )
    print(
        f"ATTOM property-policy-year candidates: {con.execute('SELECT count(*) FROM housing').fetchone()[0]:,}"
    )

    tiers = [
        (
            ["blockgroup_key", "construction_year", "flood_risk_key", "policy_year"],
            "0_blockgroup_exact_year_risk",
        ),
        (
            ["zip_key", "construction_year", "flood_risk_key", "policy_year"],
            "1_zip_exact_year_risk",
        ),
        (
            ["community_key", "construction_year", "flood_risk_key", "policy_year"],
            "2_community_exact_year_risk",
        ),
        (
            [
                "community_key",
                "construction_5yr",
                "flood_risk_key",
                "policy_year",
                "postfirm_key",
            ],
            "3_community_5yr_risk_postfirm",
        ),
        (
            [
                "community_key",
                "construction_decade",
                "flood_risk_key",
                "policy_year",
                "postfirm_key",
            ],
            "4_community_decade_risk_postfirm",
        ),
    ]
    tier_counts = []
    cell_rows = []
    for keys, label in tiers:
        row = cell_diagnostics(con, keys, label)
        count = apply_tier(con, keys, label)
        row["assignments"] = count
        row["unmatched_policies_after_tier"] = con.execute(
            "SELECT count(*) FROM nfip WHERE wagner_tier IS NULL"
        ).fetchone()[0]
        cell_rows.append(row)
        tier_counts.append((label, count))

    # Save the final NFIP panel and a compact match summary.
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT * EXCLUDE (_nfip_rowid) FROM nfip ORDER BY _nfip_rowid) TO {quote(str(out))} (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    total = con.execute("SELECT count(*) FROM nfip").fetchone()[0]
    matched = con.execute(
        "SELECT count(*) FROM nfip WHERE wagner_tier IS NOT NULL"
    ).fetchone()[0]
    lag = con.execute(
        "SELECT avg(attom_value_lag), median(attom_value_lag), max(attom_value_lag) FROM nfip WHERE wagner_tier IS NOT NULL"
    ).fetchone()
    diagnostics = pd.DataFrame(
        [
            {"metric": "NFIP policies", "count": total, "percent": 100.0},
            {
                "metric": "matched",
                "count": matched,
                "percent": round(100 * matched / total, 2),
            },
        ]
        + [
            {"metric": label, "count": count, "percent": round(100 * count / total, 2)}
            for label, count in tier_counts
        ]
        + [
            {"metric": "mean ATTOM value lag", "count": lag[0], "percent": pd.NA},
            {"metric": "median ATTOM value lag", "count": lag[1], "percent": pd.NA},
            {"metric": "maximum ATTOM value lag", "count": lag[2], "percent": pd.NA},
        ]
    )
    diagnostics_path = out.with_name(f"{out.stem}_diagnostics.csv")
    diagnostics.to_csv(diagnostics_path, index=False)
    cell_path = Path(args.cell_diagnostics) if args.cell_diagnostics else out.with_name(
        f"{out.stem}_cell_diagnostics.csv"
    )
    pd.DataFrame(cell_rows).to_csv(cell_path, index=False)
    con.close()
    print(diagnostics.to_string(index=False))
    print(f"Saved: {out}")
    print(f"Saved: {diagnostics_path}")
    print(f"Saved: {cell_path}")


if __name__ == "__main__":
    main()
