"""
Author: Anna Li
Date: 2026-09-30

Claude change 09-30: test script, run by hand (not in master.do). Reads the ATTOM
building-permit sample (five counties; Duval, FL is the only one in FL LA TX), writes
the county's permits as one .dta, and reshapes the elevation candidates into the
Builty raw layout so the unmodified clean/clean_builty.do screen runs on them from an
alternate data root.

ATTOM -> Builty layout:
    DESCRIPTION     "Type - SubType" on line 1 (clean_builty.do reads line 1 as the
                    permit subtype), the permit's Description on line 2
    PROPERTY_TYPE   from ATTOM's classifier tags: Commercial if tagged COMMERCIAL and
                    not RESIDENTIAL, Residential if the reverse, else empty
    STREET_ADDRESS  PropertyAddressFull, upper case
    DATE_ISSUED     EffectiveDate (1900-01-01 and 1970-01-01 are placeholders -> empty)
    DATE_SUBMITTED  first FILED / APPLIED date in the status table
    DATE_FINALED    first FINAL / COMPLETE date in the status table
    PROJECT_VALUE   JobValue;  TOTAL_FEES  Fees;  BUILTY_ID  BuildingPermitID
Extra columns ride along: ATTOMID, PERMIT_NUMBER, CLASSIFIERS, and the assessor's
property use and year built for the permit's ATTOMID (from the geocoded ATTOM panel).

Writes  <out>/attom_permits_<fips>.dta                 (every permit in the county; owner
                                                        mailing block and names dropped)
        <out>/root/clean/builty_raw/<st>.csv           (elevation candidates, Builty layout)
        <out>/root/clean/cpi.dta                       (copy, for clean_builty.do Section 2)

Usage: python attom_permits_sample.py --data <data root> --out <folder> [--fips 12031]
Then:  stata-mp -b do code/clean/clean_builty.do <out>/root FL
"""
import argparse, shutil, sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "prepare"))
from extract_attom_permits import CANDIDATE_PATTERNS   # the same recall net as the pipeline extract

SAMPLE = "raw/ATTOM sample permits/sample/PROPERTY_DATA_SAMPLE_PARQUET_{}_0001.Parquet"
SF_USE_CODES = ("376", "380", "382", "383", "385", "386")   # as in clean_builty_coverage.py
PLACEHOLDER_DATES = "(DATE '1900-01-01', DATE '1970-01-01')"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data", required=True, help="Data root with raw/, clean/ and build/.")
    p.add_argument("--out", required=True, help="Output folder (local, not Dropbox).")
    p.add_argument("--fips", default="12031", help="State-county FIPS to keep (default Duval, FL).")
    a = p.parse_args()

    data, out = Path(a.data), Path(a.out)
    permit, classifier, status = (data / SAMPLE.format(t) for t in
                                  ("BUILDINGPERMIT", "BUILDINGPERMITCLASSIFIERS", "BUILDINGPERMITSTATUS"))
    (out / "root" / "clean" / "builty_raw").mkdir(parents=True, exist_ok=True)
    (out / "root" / "clean" / "builty_states").mkdir(exist_ok=True)
    shutil.copy(data / "clean" / "cpi.dta", out / "root" / "clean" / "cpi.dta")

    con = duckdb.connect()
    con.execute("SET memory_limit = '6GB'")

    print(f"{permit.name}: {permit.stat().st_size / 1e6:,.0f} MB")
    print(con.execute(f"""
        SELECT PropertyAddressState AS state, StateCountyFIPS AS fips, CountyName AS county,
               count(*) AS permits, count(ATTOMID) AS with_attomid,
               min(year(EffectiveDate)) FILTER (WHERE EffectiveDate NOT IN {PLACEHOLDER_DATES}) AS year_min,
               max(year(EffectiveDate)) AS year_max
        FROM read_parquet('{permit}') GROUP BY ALL HAVING count(*) >= 1000 ORDER BY permits DESC
    """).df().to_string(index=False))

    # One row per permit: permit fields, classifier tags, status dates, assessor use and year built
    state = con.execute(f"""SELECT PropertyAddressState FROM read_parquet('{permit}')
                            WHERE StateCountyFIPS = '{a.fips}' GROUP BY 1 ORDER BY count(*) DESC LIMIT 1""").fetchone()[0]
    geocoded = data / "build" / "nfip_attom_pipeline_v2" / "geocoded" / f"{state.lower()}_attom_geocoded.parquet"
    con.execute(f"""
        CREATE TABLE permits AS
        WITH c AS (
            SELECT BuildingPermitID,
                   string_agg(BuildingPermitClassifiers, '; ' ORDER BY BuildingPermitClassifiers) AS classifiers,
                   bool_or(BuildingPermitClassifiers = 'RESIDENTIAL') AS tag_residential,
                   bool_or(BuildingPermitClassifiers IN ('COMMERCIAL', 'COMMERCIAL RENOVATION')) AS tag_commercial
            FROM read_parquet('{classifier}') GROUP BY 1
        ), s AS (
            SELECT BuildingPermitID,
                   min(EffectiveDate) FILTER (WHERE BuildingPermitStatus IN ('FILED', 'APPLIED')
                                              AND EffectiveDate NOT IN {PLACEHOLDER_DATES}) AS date_submitted,
                   min(EffectiveDate) FILTER (WHERE BuildingPermitStatus IN ('FINAL', 'COMPLETE')
                                              AND EffectiveDate NOT IN {PLACEHOLDER_DATES}) AS date_finaled
            FROM read_parquet('{status}') GROUP BY 1
        ), g AS (
            SELECT try_cast(attomid AS BIGINT) AS ATTOMID,
                   arg_max(nullif(trim(property_use_std), ''), year) AS property_use_std,
                   max(construction_year) AS construction_year
            FROM read_parquet('{geocoded}') WHERE countycode = '{a.fips}' GROUP BY 1
        )
        SELECT b.BuildingPermitID AS building_permit_id, b.ATTOMID AS attomid,
               b.CountyName AS county_name, b.StateCountyFIPS AS fips,
               b.PropertyAddressFull AS address_full,
               b.PropertyAddressAddressHouseNumber AS house_number,
               b.PropertyAddressAddressStreetDirection AS street_direction,
               b.PropertyAddressAddressStreetName AS street_name,
               b.PropertyAddressAddressStreetSuffix AS street_suffix,
               b.PropertyAddressAddressStreetPostDirection AS street_post_direction,
               b.PropertyAddressAddressUnitPrefix AS unit_prefix, b.PropertyAddressAddressUnit AS unit,
               b.PropertyAddressCity AS city, b.PropertyAddressState AS state,
               b.PropertyAddressZIP AS zip, b.PropertyAddressZIP4 AS zip4,
               CASE WHEN b.EffectiveDate NOT IN {PLACEHOLDER_DATES} THEN b.EffectiveDate END AS date_issued,
               s.date_submitted, s.date_finaled,
               b.PermitNumber AS permit_number, b.Status AS status, b.Type AS permit_type,
               b.SubType AS permit_subtype, b.ProjectName AS project_name,
               b.JobValue AS job_value, b.Fees AS fees, b.Description AS description,
               c.classifiers, coalesce(c.tag_residential, false) AS tag_residential,
               coalesce(c.tag_commercial, false) AS tag_commercial,
               g.property_use_std, g.construction_year, b.PublicationDate AS publication_date
        FROM read_parquet('{permit}') b
        LEFT JOIN c USING (BuildingPermitID) LEFT JOIN s USING (BuildingPermitID) LEFT JOIN g USING (ATTOMID)
        WHERE b.StateCountyFIPS = '{a.fips}' AND b.PropertyAddressState = '{state}'
    """)

    # Elevation candidates in the Builty raw layout
    net = "|".join(CANDIDATE_PATTERNS)
    csv = out / "root" / "clean" / "builty_raw" / f"{state.lower()}.csv"
    codes = ", ".join(f"'{c}'" for c in SF_USE_CODES)
    con.execute(f"""
        COPY (
            SELECT NULL AS APN, building_permit_id AS BUILTY_ID, NULL AS CBSA, upper(county_name) AS COUNTY,
                   strftime(date_finaled, '%Y-%m-%d') AS DATE_FINALED,
                   strftime(date_issued, '%Y-%m-%d') AS DATE_ISSUED,
                   strftime(date_submitted, '%Y-%m-%d') AS DATE_SUBMITTED,
                   concat_ws(' - ', nullif(trim(permit_type), ''), nullif(trim(permit_subtype), ''))
                       || chr(10) || coalesce(description, '') AS DESCRIPTION,
                   NULL AS FIPS_CBSA, substr(fips, 3, 3) AS FIPS_COUNTY, substr(fips, 1, 2) AS FIPS_STATE,
                   upper(city) AS LOCALITY, job_value AS PROJECT_VALUE,
                   CASE WHEN tag_commercial AND NOT tag_residential THEN 'Commercial'
                        WHEN tag_residential AND NOT tag_commercial THEN 'Residential' END AS PROPERTY_TYPE,
                   permit_type AS RECORD_TYPE, state AS STATE, status AS STATUS,
                   upper(address_full) AS STREET_ADDRESS, fees AS TOTAL_FEES, NULL AS WORK_TYPES, zip AS ZIPCODE,
                   attomid AS ATTOMID, permit_number AS PERMIT_NUMBER, classifiers AS CLASSIFIERS,
                   property_use_std AS ATTOM_USE, CAST(property_use_std IN ({codes}) AS INTEGER) AS ATTOM_SF,
                   construction_year AS ATTOM_YEAR_BUILT
            FROM permits
            WHERE regexp_matches(lower(concat_ws(' ', permit_type, permit_subtype, description)), '{net}')
        ) TO '{csv}' (FORMAT CSV, HEADER)
    """)
    n, n_cand = (con.execute(q).fetchone()[0] for q in
                 ("SELECT count(*) FROM permits", f"SELECT count(*) FROM read_csv('{csv}', all_varchar = true)"))
    print(f"{a.fips}: {n:,} permits, {n_cand:,} elevation candidates -> {csv}")

    # County permits as .dta: the long, mostly short or empty text fields as strL (description
    # averages 34 characters with a 1,000 maximum; as fixed-width strings the file is 3.1 GB)
    df = con.execute("SELECT * FROM permits ORDER BY building_permit_id").df()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].fillna("")
    for col in ("tag_residential", "tag_commercial"):
        df[col] = df[col].astype("int8")
    for col in ("attomid", "job_value", "fees", "construction_year"):
        df[col] = df[col].astype("float64")
    dates = ["date_issued", "date_submitted", "date_finaled", "publication_date"]
    labels = {
        "building_permit_id": "ATTOM building permit ID", "attomid": "ATTOM property ID (missing if unmatched)",
        "fips": "State-county FIPS", "address_full": "Site address", "date_issued": "EffectiveDate (issue date)",
        "date_submitted": "First FILED/APPLIED date (status table)",
        "date_finaled": "First FINAL/COMPLETE date (status table)", "permit_type": "Permit type (jurisdiction)",
        "permit_subtype": "Permit subtype (jurisdiction)", "job_value": "Declared job value (nominal $)",
        "fees": "Permit fees (nominal $)", "description": "Work description",
        "classifiers": "ATTOM classifier tags", "tag_residential": "Classifier tag RESIDENTIAL",
        "tag_commercial": "Classifier tag COMMERCIAL or COMMERCIAL RENOVATION",
        "property_use_std": "Assessor property use (latest year, geocoded panel)",
        "construction_year": "Assessor year built (geocoded panel)",
    }
    dta = out / f"attom_permits_{a.fips}.dta"
    df.to_stata(dta, write_index=False, version=118,
                convert_strl=["description", "classifiers", "project_name"],
                convert_dates={c: "td" for c in dates}, variable_labels=labels,
                data_label=f"ATTOM building permits, county {a.fips}")
    print(f"{len(df):,} rows x {len(df.columns)} columns -> {dta} ({dta.stat().st_size / 1e6:,.0f} MB)")


if __name__ == "__main__":
    main()
