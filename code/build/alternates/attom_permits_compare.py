"""
Author: Anna Li
Date: 2026-09-30

Claude change 09-30: test script, run by hand (not in master.do). Compares elevations
found in Builty with elevations found in the ATTOM building-permit sample for the same
county (Duval, FL), both screened by clean/clean_builty.do: Builty from the pipeline
files, ATTOM from the alternate root written by attom_permits_sample.py.

The labels below are a hand read of all 160 ATTOM survivors and all 32 Builty survivors
on 2026-09-30. "House raised" = an existing house lifted onto a new or higher foundation,
including trade permits that reconnect utilities after the lift.

Writes  ../output/tables/attom_permits_vs_builty_duval.xlsx
        (sheets funnel, by_year, by_locality, attom_survivors, builty_survivors)
        <out>/attom_elevations_<fips>.dta   (the ATTOM permits that survive the screen, one row per
                                             permit, with the hand label; the county's raw permits are
                                             <out>/attom_permits_<fips>.dta from attom_permits_sample.py)

Usage: python attom_permits_compare.py --data <data root> --out <folder> --output <output folder>
"""
import argparse
from pathlib import Path

import duckdb
import pandas as pd

FIPS = "12031"
RAISED = {266482154, 972729874, 263654480, 272908291, 48760321, 48405750, 47964814, 958603597,
          201876380, 992791072, 173014645, 992832545, 992896412, 992896118, 970036250, 974523169,
          973578929, 1166130791, 170144322, 280988716, 49449136, 268682779, 306077162, 253674515,
          997631431, 250001639, 250160410, 1154817669, 1141730151, 258038435}
AMBIGUOUS = {265329263, 1169306532, 1129665704}   # lifted for a repair, or not clearly a house
NEW_BUILD = {255199871, 992789475, 954622624, 1173276538, 1134409204, 1134409205, 1142497809,
             1158903075, 1143461386}              # new house built elevated
BUILTY_RAISED_ADDRESS = "1908 N 1ST ST"           # the other 31 Builty survivors are on Raising Hill Dr

# Raise-the-house wording, tuned on these 160 rows (so its hit rate here is in-sample)
NOUN = r"(?:house|home|residence|dwelling|structure|building)"
VERB = r"(?:\brais(?:e|ed|ing)\b|\blift(?:ed|ing)?\b|jack(?:ed|ing)? ?up|\bel[ae]vat(?:e|ed|ing)\b)"
NOT_HOUSE = (r"slab|service|riser|mast\b|tree|carport|deck|roof|elevated (?:single|home|house)"
             r"|(?:single.family|story) [^.;]{0,20}elevated|new construction|fire alarm|sprinkler|\bvua\b"
             r"|code enforcement|lock down")


def year_of(df: pd.DataFrame) -> pd.Series:
    # clean_builty.do's permit year: earliest of issued, finaled, submitted
    cols = [pd.to_numeric(df[c].str[:4], errors="coerce") for c in ("date_issued", "date_finaled", "date_submitted")]
    return pd.concat(cols, axis=1).min(axis=1)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--data", required=True, help="Data root (Dropbox): Builty pipeline files.")
    p.add_argument("--out", required=True, help="Folder written by attom_permits_sample.py.")
    p.add_argument("--output", required=True, help="Output folder with tables/.")
    a = p.parse_args()
    data, alt = Path(a.data), Path(a.out) / "root" / "clean"
    con = duckdb.connect()

    # Screened permits and properties, both sources
    builty = pd.read_stata(data / "clean" / "builty_states" / "builty_elevations_fl.dta")
    builty = builty[builty.fips_county == FIPS[2:]].copy()
    builty["label"] = "not elevation"
    builty.loc[builty.street_address == BUILTY_RAISED_ADDRESS, "label"] = "house raised"
    builty_prop = pd.read_stata(data / "clean" / "builty_elevations.dta")
    builty_prop = builty_prop[(builty_prop.state == "FL") & (builty_prop.fips_county == FIPS[2:])]

    attom = pd.read_stata(alt / "builty_states" / "builty_elevations_fl.dta")
    attom_prop = pd.read_stata(alt / "builty_elevations.dta")
    ids = attom.builty_id.astype(int)
    attom["label"] = "not elevation"
    for name, group in (("house raised", RAISED), ("ambiguous", AMBIGUOUS), ("elevated new build", NEW_BUILD)):
        attom.loc[ids.isin(group), "label"] = name
    d = attom.description
    attom["raise_house_wording"] = ((d.str.contains(VERB + r"[^.;]{0,25}" + NOUN)
                                     | d.str.contains(NOUN + r"[^.;]{0,25}" + VERB)
                                     | d.str.contains("raising and new foundation"))
                                    & ~d.str.contains(NOT_HOUSE)).astype(int)
    attom["subtype_new_building"] = attom.permit_subtype.str.contains("new building").astype(int)
    for df in (builty, attom):
        df["year"] = year_of(df)

    # All permits by year and locality, both sources
    raw = data / "raw" / "ATTOM sample permits" / "sample" / "PROPERTY_DATA_SAMPLE_PARQUET_BUILDINGPERMIT_0001.Parquet"
    attom_all = con.execute(f"""
        SELECT upper(PropertyAddressCity) AS locality, year(EffectiveDate) AS year, count(*) AS attom_permits,
               count(*) FILTER (WHERE Description <> '') AS attom_with_text
        FROM read_parquet('{raw}')
        WHERE StateCountyFIPS = '{FIPS}' AND PropertyAddressState = 'FL' GROUP BY ALL
    """).df()
    builty_all = pd.read_stata(data / "clean" / "builty_coverage_locality.dta")
    builty_all = (builty_all[(builty_all.state == "FL") & (builty_all.countycode == FIPS)]
                  .rename(columns={"builty_n_permits": "builty_permits"})[["locality", "year", "builty_permits"]])
    cand_builty = con.execute(f"""SELECT count(*) FROM read_csv('{data / "clean" / "builty_raw" / "fl.csv"}',
                                  all_varchar = true) WHERE FIPS_COUNTY = '{FIPS[2:]}'""").fetchone()[0]
    cand_attom = con.execute(f"""SELECT count(*) FROM read_csv('{alt / "builty_raw" / "fl.csv"}',
                                 all_varchar = true)""").fetchone()[0]

    window = lambda df: df[df.year.between(1990, 2026)]
    raised_b, raised_a = builty[builty.label == "house raised"], attom[attom.label == "house raised"]
    funnel = pd.DataFrame([
        ("All permits, 1990-2026", window(builty_all).builty_permits.sum(), window(attom_all).attom_permits.sum()),
        ("  with a work description", None, window(attom_all).attom_with_text.sum()),
        ("Elevation candidates (extract_builty.py net)", cand_builty, cand_attom),
        ("Survive the clean_builty.do screen: permits", len(builty), len(attom)),
        ("  addresses (clean_builty.do collapse)", len(builty_prop), len(attom_prop)),
        ("  flagged retrofit (addresses)", builty_prop.retrofit.sum(), attom_prop.retrofit.sum()),
        ("  flagged new construction (addresses)", builty_prop.new_construction.sum(), attom_prop.new_construction.sum()),
        ("  jurisdiction subtype 'new building' (permits)", None, attom.subtype_new_building.sum()),
        ("Hand read: house raised, permits", len(raised_b), len(raised_a)),
        ("  house raised, addresses", raised_b.street_address.nunique(), raised_a.street_address.nunique()),
        ("  house raised, single-family per assessor (addresses)", None,
         raised_a[raised_a.attom_sf == "1"].street_address.nunique()),
        ("  ambiguous, permits", 0, (attom.label == "ambiguous").sum()),
        ("  elevated new build, permits", 0, (attom.label == "elevated new build").sum()),
        ("  not an elevation, permits", (builty.label == "not elevation").sum(), (attom.label == "not elevation").sum()),
        ("Raise-the-house wording: permits", None, attom.raise_house_wording.sum()),
        ("  of which hand-read house raised", None, attom[attom.label == "house raised"].raise_house_wording.sum()),
    ], columns=["stage", "builty", "attom"])

    by_year = (window(attom_all).groupby("year").attom_permits.sum().to_frame()
               .join(window(builty_all).groupby("year").builty_permits.sum(), how="outer")
               .join(builty.groupby("year").size().rename("builty_screened"))
               .join(raised_b.groupby("year").size().rename("builty_house_raised"))
               .join(attom.groupby("year").size().rename("attom_screened"))
               .join(raised_a.groupby("year").size().rename("attom_house_raised"))
               .fillna(0).astype(int).reset_index().astype({"year": int}))
    by_locality = (window(attom_all).groupby("locality").attom_permits.sum().to_frame()
                   .join(window(builty_all).groupby("locality").builty_permits.sum(), how="outer")
                   .join(builty.groupby("locality").size().rename("builty_screened"))
                   .join(raised_b.groupby("locality").size().rename("builty_house_raised"))
                   .join(attom.groupby("locality").size().rename("attom_screened"))
                   .join(raised_a.groupby("locality").size().rename("attom_house_raised"))
                   .fillna(0).astype(int).sort_values("attom_permits", ascending=False).reset_index())

    keep_a = ["builty_id", "attomid", "permit_number", "street_address", "locality", "year", "label",
              "raise_house_wording", "retrofit", "new_construction", "attom_sf", "attom_year_built",
              "project_value", "permit_subtype", "description"]
    keep_b = ["builty_id", "street_address", "locality", "year", "label", "retrofit", "new_construction",
              "project_value", "permit_subtype", "description"]
    xlsx = Path(a.output) / "tables" / "attom_permits_vs_builty_duval.xlsx"
    with pd.ExcelWriter(xlsx) as w:
        funnel.to_excel(w, sheet_name="funnel", index=False)
        by_year.to_excel(w, sheet_name="by_year", index=False)
        by_locality.to_excel(w, sheet_name="by_locality", index=False)
        attom.sort_values(["label", "year"])[keep_a].to_excel(w, sheet_name="attom_survivors", index=False)
        builty.sort_values(["label", "year"])[keep_b].to_excel(w, sheet_name="builty_survivors", index=False)

    # Claude change 10-02: the screened ATTOM permits as their own .dta, hand label included
    elev = attom.sort_values(["label", "year", "street_address"])[keep_a].copy()
    for col in ("builty_id", "attomid", "year", "attom_sf", "attom_year_built", "project_value"):
        elev[col] = pd.to_numeric(elev[col], errors="coerce")
    elev = elev.rename(columns={"builty_id": "building_permit_id", "attom_sf": "attom_single_family",
                                "project_value": "job_value", "retrofit": "screen_retrofit",
                                "new_construction": "screen_new_construction"})
    elev.to_stata(Path(a.out) / f"attom_elevations_{FIPS}.dta", write_index=False, version=118,
                  convert_strl=["description"], data_label=f"ATTOM permits passing the Builty elevation screen, county {FIPS}",
                  variable_labels={
                      "building_permit_id": "ATTOM building permit ID", "attomid": "ATTOM property ID",
                      "year": "Permit year (clean_builty.do rule)", "label": "Hand read 2026-09-30",
                      "raise_house_wording": "Raise/lift/elevate verb next to a house noun",
                      "screen_retrofit": "clean_builty.do retrofit flag", "screen_new_construction": "clean_builty.do new-construction flag",
                      "attom_single_family": "Assessor single-family use code", "attom_year_built": "Assessor year built",
                      "job_value": "Declared job value (2023 $, clean_builty.do)", "permit_subtype": "Type - SubType (jurisdiction)",
                      "description": "Work description"})

    pd.set_option("display.width", 200)
    print(funnel.to_string(index=False))
    print(by_locality.to_string(index=False))
    print(by_year.to_string(index=False))
    print(pd.crosstab(attom.label, attom.raise_house_wording))


if __name__ == "__main__":
    main()
