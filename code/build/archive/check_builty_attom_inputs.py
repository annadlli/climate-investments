"""
Inspect Builty and ATTOM inputs for the ATTOM matching step.

Authors: Anna Li and Vendela Norman
Date: 2026-06-17

Description:
    Reads a few rows from the state Builty elevation file and ATTOM file to
    verify county FIPS formats and whether Builty has JOB_VALUE or
    PROJECT_VALUE before running build_attom_onto_permits.py.

Notes / Sources:
    Inputs are {data}/build/{state}_flood_elevation_strict.parquet and
    {data}/raw/attom_{state}.parquet.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


STATE_FIPS_MAP = {
    "TX": "48",
    "VA": "51",
    "FL": "12",
    "LA": "22",
    "NC": "37",
    "SC": "45",
    "GA": "13",
    "AL": "01",
    "MS": "28",
    "TN": "47",
}


def quote_sql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def county_clean_sql(col: str, state_fips: str) -> str:
    digits = f"regexp_extract(trim(\"{col}\"), '(\\d+)', 1)"
    return f"""
        CASE
            WHEN length({digits}) = 5 THEN {digits}
            WHEN length({digits}) = 4 AND starts_with({digits}, {quote_sql(state_fips)})
                THEN {quote_sql(state_fips)} || lpad(substr({digits}, 3), 3, '0')
            WHEN length({digits}) = 3 THEN {quote_sql(state_fips)} || {digits}
            ELSE {digits}
        END
    """


def existing_columns(con: duckdb.DuckDBPyConnection, path: Path) -> list[str]:
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({quote_sql(str(path))}) LIMIT 1"
    ).fetchall()
    return [row[0] for row in rows]


def print_df(title: str, df) -> None:
    print(f"\n{title}")
    if df.empty:
        print("  <no rows>")
    else:
        print(df.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True, help="2-letter state abbreviation, e.g. TX")
    parser.add_argument(
        "--data",
        required=True,
        help="Dropbox data root. Input paths derive from this root.",
    )
    parser.add_argument("--n", type=int, default=5, help="Rows to print from each input.")
    args = parser.parse_args()

    state = args.state.upper()
    state_lower = state.lower()
    state_fips = STATE_FIPS_MAP.get(state, "")
    data = Path(args.data)
    builty_path = data / "build" / f"{state_lower}_flood_elevation_strict.parquet"
    attom_path = data / "raw" / f"attom_{state_lower}.parquet"

    con = duckdb.connect()
    print(f"State: {state}")
    print(f"Builty: {builty_path}")
    print(f"ATTOM:  {attom_path}")

    if not builty_path.exists():
        print("\nBuilty file not found. Run build_split_builty_states.py first.")
    else:
        builty_cols = existing_columns(con, builty_path)
        value_cols = [col for col in ["JOB_VALUE", "PROJECT_VALUE"] if col in builty_cols]
        has_county_fips = "COUNTY_FIPS" in builty_cols
        has_split_fips = {"FIPS_STATE", "FIPS_COUNTY"}.issubset(builty_cols)
        print("\nBuilty columns:")
        print(f"  COUNTY_FIPS present: {has_county_fips}")
        print(f"  FIPS_STATE/FIPS_COUNTY present: {has_split_fips}")
        print(f"  Value columns present: {value_cols if value_cols else '<none>'}")

        if value_cols:
            counts_sql = ", ".join(
                f"count(\"{col}\") AS nonmissing_{col.lower()}" for col in value_cols
            )
            counts = con.execute(
                f"SELECT {counts_sql} FROM read_parquet({quote_sql(str(builty_path))})"
            ).df()
            print_df("Builty value nonmissing counts", counts)

        sample_cols = [
            col
            for col in [
                "COUNTY_FIPS",
                "FIPS_STATE",
                "FIPS_COUNTY",
                "STATE",
                "PERMIT_DATE",
                "STREET",
                "ZIPCODE",
            ]
            + value_cols
            if col in builty_cols
        ]
        sample_sql = ", ".join(f'"{col}"' for col in sample_cols)
        if has_county_fips:
            county_expr = (
                f"{quote_sql(state_fips)} || "
                "right(lpad(CAST(\"COUNTY_FIPS\" AS VARCHAR), 5, '0'), 3)"
            )
        elif has_split_fips:
            county_expr = (
                "lpad(regexp_replace(CAST(\"FIPS_STATE\" AS VARCHAR), '\\\\.0$', ''), 2, '0') || "
                "lpad(regexp_replace(CAST(\"FIPS_COUNTY\" AS VARCHAR), '\\\\.0$', ''), 3, '0')"
            )
        else:
            county_expr = "NULL"
        sample = con.execute(
            f"""
            SELECT
                {sample_sql},
                {county_expr} AS normalized_county_fips
            FROM read_parquet({quote_sql(str(builty_path))})
            LIMIT {int(args.n)}
            """
        ).df()
        print_df("Builty sample", sample)

    if not attom_path.exists():
        print("\nATTOM file not found.")
    else:
        attom_cols = existing_columns(con, attom_path)
        county_col = next(
            (
                col
                for col in ["SITUSSTATECOUNTYFIPS", "PROPERTYCOUNTYFIPS", "COUNTYFIPS", "FIPS"]
                if col in attom_cols
            ),
            None,
        )
        value_cols = [
            col
            for col in [
                "TAXASSESSEDVALUETOTAL",
                "PREVIOUSASSESSEDVALUE",
                "TAXYEARASSESSED",
                "TAXMARKETVALUETOTAL",
            ]
            if col in attom_cols
        ]
        zip_cols = [col for col in ["SITUSZIP", "PROPERTYADDRESSZIP"] if col in attom_cols]
        print("\nATTOM columns:")
        print(f"  County FIPS column: {county_col if county_col else '<none>'}")
        print(f"  ZIP columns present: {zip_cols if zip_cols else '<none>'}")
        print(f"  Value/year columns present: {value_cols if value_cols else '<none>'}")

        select_cols = [col for col in [county_col, *zip_cols, *value_cols] if col]
        select_sql = ", ".join(f'"{col}"' for col in select_cols)
        normalized = (
            f", {county_clean_sql(county_col, state_fips)} AS normalized_county_fips"
            if county_col
            else ""
        )
        sample = con.execute(
            f"""
            SELECT {select_sql}{normalized}
            FROM read_parquet({quote_sql(str(attom_path))})
            LIMIT {int(args.n)}
            """
        ).df()
        print_df("ATTOM sample", sample)

    con.close()


if __name__ == "__main__":
    main()
