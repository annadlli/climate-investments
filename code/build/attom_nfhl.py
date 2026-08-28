"""
Authors: Anna Li
Original Date: 2026-08-12
Revised Date: 2026-08-16

Works out which FEMA flood zone and which NFIP community each ATTOM property
sits in, using Wagner's historical NFHL maps.

The coordinates come from the Census geocoder (geocode_attom.py), not from
ATTOM, which ships no usable ones. Every property stays in the output whether
or not it landed inside a polygon, so nothing silently disappears here.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd
import pyogrio

# the NFHL downloads are filed by state FIPS, so we need the code for each state
STATE_FIPS = {
    "al": "01", "ct": "09", "de": "10", "fl": "12", "ga": "13",
    "la": "22", "me": "23", "md": "24", "ma": "25", "ms": "28",
    "nh": "33", "nj": "34", "ny": "36", "nc": "37", "pa": "42",
    "ri": "44", "sc": "45", "tx": "48", "vt": "50", "va": "51",
}

# the fields we want off each NFHL layer
FLOOD_FIELDS = ["FLD_ZONE", "ZONE_SUBTY", "SFHA_TF", "STATIC_BFE", "DEPTH", "DFIRM_ID"]
COMMUNITY_FIELDS = ["CID", "COMM_NO", "COM_NFO_ID", "POL_NAME1", "CO_FIPS", "ST_FIPS", "DFIRM_ID"]


def parse_args() -> argparse.Namespace:
    # where the data lives, which state, and which NFHL download to read
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, help="Two-letter state abbreviation.")
    parser.add_argument("--nfhl", required=True,
                        help="An NFHL .gdb/.gpkg, or a folder of state-FIPS subfolders "
                             "such as nfhl/50/NFHL_50_20161207.gdb.")
    parser.add_argument("--points", required=True,
                        help="ATTOMID-level geocode parquet from step 1.")
    parser.add_argument("--out", required=True, help="Output parquet path.")
    return parser.parse_args()


def resolve_nfhl(path: Path, state: str) -> Path:
    # either we were handed the map file directly, or we go find it under the state's folder
    if path.suffix.lower() in {".gdb", ".gpkg"}:
        return path
    return sorted((path / STATE_FIPS[state]).glob("*.gdb"))[0]


def find_layer(dataset: Path, expected: str) -> str | None:
    # layer names vary a bit between NFHL vintages, so match on letters and digits
    # only, and fall back to a suffix match before giving up
    def simplify(name: str) -> str:
        return re.sub(r"[^A-Z0-9]", "", str(name).upper())

    layers = [str(row[0]) for row in pyogrio.list_layers(dataset)]
    target = simplify(expected)
    exact = [layer for layer in layers if simplify(layer) == target]
    suffix = [layer for layer in layers if simplify(layer).endswith(target)]
    candidates = exact or suffix
    return candidates[0] if len(candidates) == 1 else None


def resolve_points(points: str, state: str) -> Path:
    # The geocoded panel from step 1 is the only source of coordinates here as ATTOM doesn't have good coordinates (mostly missing). run_matching.sh owns the layout.
    path = Path(points)
    if not path.exists():
        raise FileNotFoundError(f"No geocoded ATTOM panel for {state}: {path}")
    return path


def load_points(points_path: str, state: str) -> pd.DataFrame:
    # read the geocoded properties. The file may be a property x year panel, so
    # take one coordinate row per property, preferring one that actually has coordinates
    path = resolve_points(points_path, state)
    con = duckdb.connect()
    schema = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    columns = {str(row[0]).lower(): str(row[0]) for row in schema}

    keep = ["attomid", "longitude", "latitude"]
    keep += [c for c in ("match", "geocode_match", "match_type") if c in columns]
    select = ", ".join(f'"{columns[c]}" AS "{c}"' for c in keep)
    points = con.execute(f"""
        SELECT * EXCLUDE (row_num) FROM (
            SELECT {select},
                   row_number() OVER (PARTITION BY cast("{columns['attomid']}" AS varchar)
                                      ORDER BY "{columns['longitude']}" IS NULL,
                                               "{columns['latitude']}" IS NULL) AS row_num
            FROM read_parquet(?)
        ) WHERE row_num = 1
    """, [str(path)]).fetchdf()
    con.close()

    points["attomid"] = points["attomid"].astype(str)
    points["longitude"] = pd.to_numeric(points["longitude"], errors="coerce")
    points["latitude"] = pd.to_numeric(points["latitude"], errors="coerce")
    return points.sort_values("attomid")


def spatial_attributes(points: gpd.GeoDataFrame, polygons: gpd.GeoDataFrame,
                       wanted: list[str], prefix: str) -> pd.DataFrame:
    # keep only the polygon fields desired, renamed to lowercase
    by_upper = {str(column).upper(): str(column) for column in polygons.columns}
    found = {name: by_upper[name] for name in wanted if name in by_upper}
    polygons = polygons[list(found.values()) + [polygons.geometry.name]].copy()
    polygons = polygons.rename(columns={src: name.lower() for name, src in found.items()})

    # measure each polygon in an equal-area projection so the areas are comparable
    polygons["polygon_area"] = polygons.to_crs("EPSG:5070").geometry.area

    # drop each property into the polygons it falls inside
    joined = gpd.sjoin(points.to_crs(polygons.crs), polygons, how="left", predicate="within")

    # note how many polygons a property hit, then keep the smallest one
    joined[f"{prefix}_candidate_count"] = (
        joined.groupby("attomid")["index_right"].transform(lambda hits: hits.notna().sum()))
    joined = joined.sort_values(["attomid", "polygon_area", "index_right"],
                                na_position="last").drop_duplicates("attomid", keep="first")

    columns = ["attomid", f"{prefix}_candidate_count"] + [name.lower() for name in found]
    return joined[columns].reset_index(drop=True)


def attach_firm_dates(community: pd.DataFrame, dataset: Path) -> pd.DataFrame:
    # the community lookup table carries the date each town got its first flood map,
    # which is what decides whether a house counts as pre- or post-FIRM later on
    layer = find_layer(dataset, "L_COMM_INFO") if "com_nfo_id" in community else None
    if layer is None:
        community["initial_firm_year"] = pd.NA
        community["firm_info_layer_found"] = False
        return community

    info = pyogrio.read_dataframe(dataset, layer=layer,
                                  columns=["COM_NFO_ID", "IN_FRM_DAT", "IN_NFIP_DT"])
    info.columns = [str(column).lower() for column in info.columns]

    # prefer the FIRM date, fall back to the date the town joined NFIP, and ignore
    # anything outside the years the program has existed
    firm = pd.to_datetime(info["in_frm_dat"], errors="coerce", utc=True).dt.year
    nfip = pd.to_datetime(info["in_nfip_dt"], errors="coerce", utc=True).dt.year
    info["initial_firm_year"] = firm.where(firm.between(1968, 2027), nfip)
    info.loc[~info["initial_firm_year"].between(1968, 2027), "initial_firm_year"] = pd.NA

    # drop lookup rows with no key, otherwise an unmatched property could pick up
    # a date from a completely unrelated record
    info = info.loc[info["com_nfo_id"].notna(), ["com_nfo_id", "initial_firm_year"]]
    info = info.drop_duplicates("com_nfo_id")

    community = community.merge(info, on="com_nfo_id", how="left", validate="many_to_one")
    community["firm_info_layer_found"] = True
    return community


def normalize_community_id(series: pd.Series) -> pd.Series:
    # NFIP community numbers are six digits; ATTOM's side sometimes loses the
    # leading zero or arrives as a float, so put it back into the same shape
    values = series.str.strip().str.replace(r"\.0$", "", regex=True)
    numeric = values.str.fullmatch(r"\d+").fillna(False)
    values.loc[numeric] = values.loc[numeric].str.zfill(6)
    return values.replace("", pd.NA)


def main() -> None:
    args = parse_args()
    state = args.state.lower()
    nfhl = resolve_nfhl(Path(args.nfhl), state)
    print(f"NFHL database: {nfhl}")

    # load the geocoded properties and work out which ones we can actually place:
    # they need real coordinates and a genuine geocoder match, not a guess
    points = load_points(args.points, state)
    valid = points["longitude"].between(-180, 180) & points["latitude"].between(-90, 90)
    match_column = "geocode_match" if "geocode_match" in points else "match"
    valid &= points[match_column].eq("Match")

    # turn those into map points the spatial join can use
    placeable = gpd.GeoDataFrame(
        points.loc[valid, ["attomid"]].copy(),
        geometry=gpd.points_from_xy(points.loc[valid, "longitude"], points.loc[valid, "latitude"]),
        crs="EPSG:4326")

    # read the two NFHL layers once each: flood hazard areas and political boundaries
    flood_layer = find_layer(nfhl, "S_FLD_HAZ_AR")
    community_layer = find_layer(nfhl, "S_POL_AR")
    if flood_layer is None or community_layer is None:
        available = [str(row[0]) for row in pyogrio.list_layers(nfhl)]
        raise ValueError(
            f"Required NFHL layers not found in {nfhl}: "
            f"flood={flood_layer}, community={community_layer}; "
            f"available={available}"
        )
    print(f"Flood layer: {flood_layer}")
    print(f"Community layer: {community_layer}")
    flood_polygons = gpd.read_file(nfhl, layer=flood_layer, engine="pyogrio")
    community_polygons = gpd.read_file(nfhl, layer=community_layer, engine="pyogrio")

    # do the two spatial joins. Both layers have a DFIRM_ID, so rename them apart
    flood = spatial_attributes(placeable, flood_polygons, FLOOD_FIELDS, "flood")
    flood = flood.rename(columns={"dfirm_id": "flood_dfirm_id"})
    community = spatial_attributes(placeable, community_polygons, COMMUNITY_FIELDS, "community")
    community = community.rename(columns={"dfirm_id": "community_dfirm_id"})
    community = attach_firm_dates(community, nfhl)

    # put the flood and community results back onto every property, including the
    # ones we could not place, which simply come back blank
    result = points.merge(flood, on="attomid", how="left", validate="one_to_one")
    result = result.merge(community, on="attomid", how="left", validate="one_to_one")

    # FEMA writes -9999 where a base flood elevation or depth is not published
    for column in ("static_bfe", "depth"):
        result.loc[result[column] <= -9990, column] = pd.NA

    # flag what each property managed to match
    result["nfhl_flood_matched"] = result["flood_candidate_count"].fillna(0).gt(0)
    result["nfhl_community_matched"] = result["community_candidate_count"].fillna(0).gt(0)

    # CID is the community number we want; COMM_NO is the older name for it
    community_id = "cid" if "cid" in result else "comm_no"
    result["nfip_community_id"] = normalize_community_id(result[community_id].fillna("").astype(str))

    # write the property-level flood file
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(out, index=False)

    # summarise how the join went so coverage problems are visible straight away
    total = len(result)
    counts = [
        ("ATTOM properties", total),
        ("valid accepted coordinates", int(valid.sum())),
        ("matched to flood polygon", int(result["nfhl_flood_matched"].sum())),
        ("matched to community polygon", int(result["nfhl_community_matched"].sum())),
        ("multiple flood candidates", int(result["flood_candidate_count"].fillna(0).gt(1).sum())),
        ("multiple community candidates", int(result["community_candidate_count"].fillna(0).gt(1).sum())),
    ]
    diagnostics = pd.DataFrame([
        {"metric": metric, "count": count, "percent": round(100 * count / total, 2) if total else 0.0}
        for metric, count in counts])
    diagnostics_path = out.with_name(f"{out.stem}_diagnostics.csv")
    diagnostics.to_csv(diagnostics_path, index=False)
    print(diagnostics.to_string(index=False))
    print(f"Saved: {out}")
    print(f"Saved: {diagnostics_path}")


if __name__ == "__main__":
    main()
