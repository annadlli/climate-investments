"""
Authors: Anna Li
Date: 2026-08-12

Tie NFHL flood info to ATTOM homes, using Wagner's 2018 NFHL files.
 Use the geocoded ATTOM points, join them to FEMA flood and community polygons, and keep the property row even if the match
is missing.
This uses census pulled latitude and longitude coordinates
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import geopandas as gpd
import duckdb
import pandas as pd
import pyogrio


def parse_args() -> argparse.Namespace:
    # this script expects the data root, state code, NFHL file, and the point layer.
    parser = argparse.ArgumentParser(
        description="Spatially join geocoded ATTOM properties to FEMA NFHL polygons."
    )
    parser.add_argument(
        "--data", required=True, help="Data root passed from master.do."
    )
    parser.add_argument("--state", required=True, help="Two-letter state abbreviation.")
    parser.add_argument(
        "--nfhl",
        required=True,
        help=(
            "NFHL .gdb/.gpkg, or a root containing state-FIPS directories "
            "such as nfhl/50/NFHL_50_20161207.gdb."
        ),
    )
    parser.add_argument(
        "--points",
        help="Optional ATTOMID-level geocode parquet; defaults to the pipeline output.",
    )
    parser.add_argument("--out", help="Optional output parquet path.")
    parser.add_argument(
        "--flood-layer", help="Override the detected S_FLD_HAZ_AR layer."
    )
    parser.add_argument(
        "--community-layer", help="Override the detected S_POL_AR layer."
    )
    return parser.parse_args()


def normalized_name(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", value.upper())


STATE_FIPS = {
    "al": "01",
    "ct": "09",
    "de": "10",
    "fl": "12",
    "ga": "13",
    "la": "22",
    "me": "23",
    "md": "24",
    "ma": "25",
    "ms": "28",
    "nh": "33",
    "nj": "34",
    "ny": "36",
    "nc": "37",
    "pa": "42",
    "ri": "44",
    "sc": "45",
    "tx": "48",
    "vt": "50",
    "va": "51",
}


def resolve_nfhl(path: Path, state: str) -> Path:
    # look for the NFHL map gdb
    if path.suffix.lower() in {".gdb", ".gpkg"}:
        return path
    if state not in STATE_FIPS:
        raise ValueError(f"No FIPS mapping configured for state {state!r}.")

    fips = STATE_FIPS[state]
    candidates = sorted((path / fips).glob("*.gdb"))
    if len(candidates) != 1:
        raise FileNotFoundError(
            f"Expected exactly one .gdb under {path / fips}; found: {candidates}"
        )
    return candidates[0]


def find_layer(dataset: Path, override: str | None, expected: str) -> str:
    # Pull the layer names from the NFHL file and match the one wanted, detecting by name.
    layers = [str(row[0]) for row in pyogrio.list_layers(dataset)]
    if override:
        if override not in layers:
            raise ValueError(
                f"Layer {override!r} not found. Available layers: {layers}"
            )
        return override

    target = normalized_name(expected)
    exact = [layer for layer in layers if normalized_name(layer) == target]
    suffix = [layer for layer in layers if normalized_name(layer).endswith(target)]
    candidates = exact or suffix
    if len(candidates) != 1:
        raise ValueError(
            f"Could not uniquely detect {expected}. Candidates: {candidates}; "
            f"available layers: {layers}. Supply the corresponding --*-layer argument."
        )
    return candidates[0]


def attach_firm_dates(community: pd.DataFrame, dataset: Path) -> pd.DataFrame:
    # FEMA community records can carry the initial NFIP / FIRMs date. Attach it to each community polygon match.
    if "com_nfo_id" not in community:
        community["initial_firm_year"] = pd.NA
        return community

    layer = find_layer(dataset, None, "L_COMM_INFO")
    info = pyogrio.read_dataframe(
        dataset,
        layer=layer,
        columns=["COM_NFO_ID", "IN_FRM_DAT", "IN_NFIP_DT"],
    )
    info.columns = [str(column).lower() for column in info.columns]
    firm = pd.to_datetime(info["in_frm_dat"], errors="coerce", utc=True).dt.year
    nfip = pd.to_datetime(info["in_nfip_dt"], errors="coerce", utc=True).dt.year
    info["initial_firm_year"] = firm.where(firm.between(1968, 2027), nfip)
    info.loc[~info["initial_firm_year"].between(1968, 2027), "initial_firm_year"] = (
        pd.NA
    )
    # Exclude missing COM_NFO_ID rows so an unmatched spatial point cannot inherit a date from an unrelated missing-key lookup record.
    info = info.loc[info["com_nfo_id"].notna(), ["com_nfo_id", "initial_firm_year"]]
    info = info.drop_duplicates("com_nfo_id")
    return community.merge(info, on="com_nfo_id", how="left", validate="many_to_one")


def resolve_points(data: Path, state: str, override: str | None) -> Path:
    # Find the geocoded ATTOM file, that will be matched to NFHL polygons.
    if override:
        path = Path(override)
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    candidates = [
        data / "build" / f"{state}_attom_blockgroups.parquet",
        data / "build" / f"{state}_attom_geocoded.parquet",
        data / "build" / "geocoded" / f"{state}_attom_geocoded.parquet",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(
        "No geocoded ATTOM output found. Expected one of: "
        + ", ".join(str(path) for path in candidates)
    )


def load_points(data: Path, state: str, override: str | None) -> pd.DataFrame:
    # Read the ATTOM point file, keep the most useful fields, and collapse repeated property rows down to one coordinate row per ATTOMID.
    path = resolve_points(data, state, override)
    con = duckdb.connect()
    schema = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
    ).fetchall()
    source_columns = {str(row[0]).lower(): str(row[0]) for row in schema}
    required = {"attomid", "longitude", "latitude"}
    missing = required - set(source_columns)
    if missing:
        raise ValueError(f"{path} is missing required columns: {sorted(missing)}")

    #  keep one coordinate record per ATTOMID.
    keep = ["attomid", "longitude", "latitude"]
    for column in ("match", "geocode_match", "match_type"):
        if column in source_columns:
            keep.append(column)
    select = ", ".join(f'"{source_columns[column]}" AS "{column}"' for column in keep)
    points = con.execute(
        f"""
        SELECT * EXCLUDE (_row_number)
        FROM (
            SELECT {select},
                   row_number() OVER (
                       PARTITION BY cast("{source_columns['attomid']}" AS varchar)
                       ORDER BY "{source_columns['longitude']}" IS NULL,
                                "{source_columns['latitude']}" IS NULL
                   ) AS _row_number
            FROM read_parquet(?)
        )
        WHERE _row_number = 1
        """,
        [str(path)],
    ).fetchdf()
    con.close()
    points["attomid"] = points["attomid"].astype(str)
    points["longitude"] = pd.to_numeric(points["longitude"], errors="coerce")
    points["latitude"] = pd.to_numeric(points["latitude"], errors="coerce")
    points = points.sort_values("attomid")
    return points


def select_fields(
    frame: gpd.GeoDataFrame, wanted: list[str]
) -> tuple[gpd.GeoDataFrame, dict[str, str]]:
    # Select variables and lowercase it
    by_upper = {str(column).upper(): str(column) for column in frame.columns}
    found = {name: by_upper[name] for name in wanted if name in by_upper}
    selected = frame[list(found.values()) + [frame.geometry.name]].copy()
    selected = selected.rename(
        columns={source: target.lower() for target, source in found.items()}
    )
    return selected, found


def spatial_attributes(
    point_frame: gpd.GeoDataFrame,
    polygons: gpd.GeoDataFrame,
    attributes: list[str],
    prefix: str,
) -> pd.DataFrame:
    # Spatial join: take ATTOM points, match them to flood/community polygons, and keep the most specific polygon when a point hits more
    polygons, found = select_fields(polygons, attributes)
    if not found:
        raise ValueError(
            f"The {prefix} layer contains none of the expected fields: {attributes}"
        )
    if polygons.crs is None:
        raise ValueError(f"The {prefix} layer has no coordinate reference system.")

    # If a point lands in more than one polygon, take the smaller one.
    polygons["_polygon_area"] = polygons.to_crs("EPSG:5070").geometry.area
    projected_points = point_frame.to_crs(polygons.crs)
    joined = gpd.sjoin(projected_points, polygons, how="left", predicate="within")
    joined[f"{prefix}_candidate_count"] = joined.groupby("attomid")[
        "index_right"
    ].transform(lambda values: values.notna().sum())
    joined = joined.sort_values(
        ["attomid", "_polygon_area", "index_right"], na_position="last"
    ).drop_duplicates("attomid", keep="first")

    attribute_columns = [name.lower() for name in found]
    return joined[
        ["attomid", f"{prefix}_candidate_count"] + attribute_columns
    ].reset_index(drop=True)


def diagnostic_row(metric: str, count: int, denominator: int) -> dict[str, object]:
    # keeps the counts and percent shares in one easy-to-read place.
    return {
        "metric": metric,
        "count": int(count),
        "percent": round(100 * count / denominator, 2) if denominator else 0.0,
    }

# Main flow: load points, clean up coordinates, match to flood/community  polygons, attach outputs, and save a diagnostics file.
def main() -> None:
    args = parse_args()
    data = Path(args.data)
    state = args.state.lower()
    nfhl_root = Path(args.nfhl)
    if not nfhl_root.exists():
        raise FileNotFoundError(nfhl_root)
    nfhl = resolve_nfhl(nfhl_root, state)
    print(f"NFHL database: {nfhl}")

    # Keep only rows with valid latitude/longitude and a geocode match when that field exists.
    points = load_points(data, state, args.points)
    valid = points["longitude"].between(-180, 180) & points["latitude"].between(-90, 90)
    if "geocode_match" in points:
        valid &= points["geocode_match"].eq("Match")
    elif "match" in points:
        valid &= points["match"].eq("Match")

    # Keep only the ATTOM rows with usable Census geocoder coordinates.
    spatial = gpd.GeoDataFrame(
        points.loc[valid, ["attomid"]].copy(),
        geometry=gpd.points_from_xy(
            points.loc[valid, "longitude"], points.loc[valid, "latitude"]
        ),
        crs="EPSG:4326",
    )

    # Read the flood and community layers once for a state.
    flood_layer = find_layer(nfhl, args.flood_layer, "S_FLD_HAZ_AR")
    community_layer = find_layer(nfhl, args.community_layer, "S_POL_AR")
    print(f"Flood layer: {flood_layer}")
    print(f"Community layer: {community_layer}")

    flood_polygons = gpd.read_file(nfhl, layer=flood_layer, engine="pyogrio")
    community_polygons = gpd.read_file(nfhl, layer=community_layer, engine="pyogrio")

    flood = spatial_attributes(
        spatial,
        flood_polygons,
        ["FLD_ZONE", "ZONE_SUBTY", "SFHA_TF", "STATIC_BFE", "DEPTH", "DFIRM_ID"],
        "flood",
    )
    community = spatial_attributes(
        spatial,
        community_polygons,
        ["CID", "COMM_NO", "COM_NFO_ID", "POL_NAME1", "CO_FIPS", "ST_FIPS", "DFIRM_ID"],
        "community",
    )
    community = attach_firm_dates(community, nfhl)

    # Attach the flood and community results back to the full ATTOM file
    # flag  whether each property matched a flood polygon, a community polygon,
    # or neither.
    result = points.merge(flood, on="attomid", how="left", validate="one_to_one")
    result = result.merge(community, on="attomid", how="left", validate="one_to_one")
    for column in ("static_bfe", "depth"):
        if column in result:
            result.loc[result[column] <= -9990, column] = pd.NA
    result["nfhl_flood_matched"] = result["flood_candidate_count"].fillna(0).gt(0)
    result["nfhl_community_matched"] = (
        result["community_candidate_count"].fillna(0).gt(0)
    )

    if "cid" in result:
        result["nfip_community_id"] = result["cid"].fillna("").astype(str)
    elif "comm_no" in result:
        result["nfip_community_id"] = result["comm_no"].fillna("").astype(str)

    out = (
        Path(args.out)
        if args.out
        else data / "build" / "nfhl_matches" / f"{state}_attom_nfhl.parquet"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(out, index=False)

    n = len(result)
    diagnostics = pd.DataFrame(
        [
            diagnostic_row("ATTOM properties", n, n),
            diagnostic_row("valid accepted coordinates", int(valid.sum()), n),
            diagnostic_row(
                "matched to flood polygon", int(result["nfhl_flood_matched"].sum()), n
            ),
            diagnostic_row(
                "matched to community polygon",
                int(result["nfhl_community_matched"].sum()),
                n,
            ),
            diagnostic_row(
                "multiple flood candidates",
                int(result["flood_candidate_count"].fillna(0).gt(1).sum()),
                n,
            ),
            diagnostic_row(
                "multiple community candidates",
                int(result["community_candidate_count"].fillna(0).gt(1).sum()),
                n,
            ),
        ]
    )
    diagnostics_path = out.with_name(f"{out.stem}_diagnostics.csv")
    diagnostics.to_csv(diagnostics_path, index=False)
    print(diagnostics.to_string(index=False))
    print(f"Saved: {out}")
    print(f"Saved: {diagnostics_path}")


if __name__ == "__main__":
    main()
