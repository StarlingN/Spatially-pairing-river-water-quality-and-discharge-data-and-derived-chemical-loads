# -*- coding: utf-8 -*-
"""
Join WSC POI points (CSV) to nearest stream polylines (shapefile),
append stream attributes (prefixed as 'poi_v1_0_WSC_stream_...') and compute geodesic distance (meters)
from each point to the nearest stream.

- Streams the CSV in chunks.
- Preserves ALL original CSV fields exactly as text.
- Appends poi_v1_0_WSC_stream_* columns + final 'poi_v1_0_WSC_stream_distance' column.
- Uses GeoPandas spatial index and sjoin_nearest for candidate nearest search.
- Computes geodesic distance via pyproj.Geod after snapping to nearest point on the line.
- No direct GDAL usage (no 'osgeo'); relies on geopandas/shapely/pyproj only.
- Progress bars via tqdm.
"""

from __future__ import annotations

import os
import sys
import csv
import logging
from pathlib import Path
from typing import List

import pandas as pd
import geopandas as gpd
from shapely import make_valid
from shapely.geometry import LineString, MultiLineString
from shapely.ops import nearest_points
from tqdm import tqdm
from pyproj import Transformer, Geod

# ----------------------------
# ---- CONFIGURATION ----------
# ----------------------------

# INPUTS
# NOTE: Now points to the File Geodatabase (.gdb) instead of a .shp
SHAPEFILE_PATH = r"E:\publications\clawave_1\gis\finalcat_info_riv_v1_0_clipped.gdb"
# The feature class name inside the GDB:
STREAM_LAYER_NAME = "finalcat_info_riv_v1_0_clipped"

INPUT_CSV_PATH = r"E:\publications\clawave_1\poi_v1_0_WSC\process_5_catchments_added\poi_v1_0_WSC_clipped_renamed+catchments.csv"

# OUTPUT
OUTPUT_CSV_PATH = r"E:\publications\clawave_1\poi_v1_0_WSC\process_6_streams_added\poi_v1_0_WSC_clipped_renamed+catchments+streams.csv"

# Performance / memory
CHUNK_SIZE = 100_000                 # adjust to your RAM/CPU
SHOW_SAMPLE_LOG_EVERY_N_CHUNKS = 1

# Geometry / CRS
WGS84 = "EPSG:4326"
# Canada-wide projected CRS in meters (good for nearest + meter distances):
PROJ_CRS = "EPSG:3978"               # NAD83 / Canada Atlas Lambert (meters)

# CSV coordinate columns (UPDATED)
CSV_LAT_COL = "poi_v1_0_WSC_Lat"
CSV_LON_COL = "poi_v1_0_WSC_Lon"

# ----------------------------
# ---- COLUMN ORDERS ----------
# ----------------------------

# Input CSV columns in desired order (UPDATED)
CSV_COLUMNS_ORDER: List[str] = [
    "poi_v1_0_WSC_Id",
    "poi_v1_0_WSC_SubId",
    "poi_v1_0_WSC_Obs_NM",
    "poi_v1_0_WSC_DA_Obs",
    "poi_v1_0_WSC_DrainArea",
    "poi_v1_0_WSC_DA_Diff",
    "poi_v1_0_WSC_SRC_obs",
    "poi_v1_0_WSC_Use_region",
    "poi_v1_0_WSC_Gauge_nm",
    "poi_v1_0_WSC_data_tp",
    "poi_v1_0_WSC_Lat",
    "poi_v1_0_WSC_Lon",
    "poi_v1_0_WSC_SSDA_NM",
    "poi_v1_0_WSC_SDA_NM",
    "poi_v1_0_WSC_Notes",
    "poi_v1_0_WSC_ifdelete",
    "poi_v1_0_WSC_catchment_SubId",
    "poi_v1_0_WSC_catchment_DowSubId",
    "poi_v1_0_WSC_catchment_RivSlope",
    "poi_v1_0_WSC_catchment_RivLength",
    "poi_v1_0_WSC_catchment_BasSlope",
    "poi_v1_0_WSC_catchment_BasAspect",
    "poi_v1_0_WSC_catchment_BasArea",
    "poi_v1_0_WSC_catchment_BkfWidth",
    "poi_v1_0_WSC_catchment_BkfDepth",
    "poi_v1_0_WSC_catchment_Lake_Cat",
    "poi_v1_0_WSC_catchment_HyLakeId",
    "poi_v1_0_WSC_catchment_LakeVol",
    "poi_v1_0_WSC_catchment_LakeDepth",
    "poi_v1_0_WSC_catchment_LakeArea",
    "poi_v1_0_WSC_catchment_Laketype",
    "poi_v1_0_WSC_catchment_Has_POI",
    "poi_v1_0_WSC_catchment_MeanElev",
    "poi_v1_0_WSC_catchment_FloodP_n",
    "poi_v1_0_WSC_catchment_Q_Mean",
    "poi_v1_0_WSC_catchment_Ch_n",
    "poi_v1_0_WSC_catchment_DrainArea",
    "poi_v1_0_WSC_catchment_Strahler",
    "poi_v1_0_WSC_catchment_Seg_ID",
    "poi_v1_0_WSC_catchment_Seg_order",
    "poi_v1_0_WSC_catchment_Max_DEM",
    "poi_v1_0_WSC_catchment_Min_DEM",
    "poi_v1_0_WSC_catchment_Obs_NM",
    "poi_v1_0_WSC_catchment_SRC_obs",
    "poi_v1_0_WSC_catchment_centroid_x",
    "poi_v1_0_WSC_catchment_centroid_y",
    "poi_v1_0_WSC_catchment_DA_Chn_L",
    "poi_v1_0_WSC_catchment_DA_Slope",
    "poi_v1_0_WSC_catchment_DA_Chn_Slp",
    "poi_v1_0_WSC_catchment_outletLat",
    "poi_v1_0_WSC_catchment_outletLng",
    "poi_v1_0_WSC_catchment_k",
    "poi_v1_0_WSC_catchment_c",
    "poi_v1_0_WSC_catchment_SSDA_NM",
    "poi_v1_0_WSC_catchment_SDA_NM",
    "poi_v1_0_WSC_catchment_Shape_Leng",
    "poi_v1_0_WSC_catchment_Shape_Area",
]

# Stream shapefile fields as they exist in the shapefile (UNCHANGED)
STREAM_INPUT_COLUMNS: List[str] = [
    "SubId","DowSubId","RivSlope","RivLength","BasSlope","BasAspect","BasArea",
    "BkfWidth","BkfDepth","Lake_Cat","HyLakeId","LakeVol","LakeDepth","LakeArea","Laketype",
    "Has_POI","MeanElev","FloodP_n","Q_Mean","Ch_n","DrainArea","Strahler",
    "Seg_ID","Seg_order","Max_DEM","Min_DEM","Obs_NM","SRC_obs","centroid_x","centroid_y",
    "DA_Chn_L","DA_Slope","DA_Chn_Slp","outletLat","outletLng","k","c","SSDA_NM","SDA_NM","Shape_Leng"
]

# Stream output prefix and distance column (UPDATED)
STREAM_PREFIX = "poi_v1_0_WSC_stream_"
STREAM_OUTPUT_COLUMNS = [f"{STREAM_PREFIX}{c}" for c in STREAM_INPUT_COLUMNS]
STREAM_DISTANCE_COL = "poi_v1_0_WSC_stream_distance"

ALL_OUTPUT_COLUMNS_ORDER = CSV_COLUMNS_ORDER + STREAM_OUTPUT_COLUMNS + [STREAM_DISTANCE_COL]

# ----------------------------
# ---- LOGGING SETUP ---------
# ----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("join_poi_to_nearest_streams")

# ----------------------------
# ---- HELPERS ---------------
# ----------------------------

def ensure_crs4326(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        logger.warning("Shapefile has no CRS. Assuming EPSG:4326; adjust if incorrect.")
        gdf = gdf.set_crs(WGS84)
    elif str(gdf.crs).upper().replace(":", "") != WGS84.replace(":", ""):
        logger.info(f"Reprojecting shapefile from {gdf.crs} to {WGS84} ...")
        gdf = gdf.to_crs(WGS84)
    return gdf

def validate_and_fix_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    invalid_count = (~gdf.geometry.is_valid).sum()
    if invalid_count:
        logger.warning(f"Found {invalid_count} invalid geometries; repairing with make_valid() ...")
        gdf = gdf.set_geometry(gdf.geometry.map(make_valid))
        still_invalid = (~gdf.geometry.is_valid).sum()
        if still_invalid:
            logger.warning(f"{still_invalid} geometries remain invalid after repair.")
    empty_count = gdf.geometry.is_empty.sum()
    if empty_count:
        logger.warning(f"Found {empty_count} empty geometries; they will not match any points.")
    return gdf

def load_streams(dataset_path: str, layer_name: str | None = None) -> gpd.GeoDataFrame:
    logger.info(f"Loading streams from: {dataset_path}" + (f" [layer={layer_name}]" if dataset_path.lower().endswith('.gdb') else ""))
    if dataset_path.lower().endswith(".gdb"):
        if not layer_name:
            raise ValueError("A 'layer_name' must be provided when reading from a File Geodatabase (.gdb).")
        streams_wgs84 = gpd.read_file(dataset_path, layer=layer_name)
    else:
        streams_wgs84 = gpd.read_file(dataset_path)

    streams_wgs84 = ensure_crs4326(streams_wgs84)
    streams_wgs84 = validate_and_fix_geometries(streams_wgs84)

    missing = [c for c in STREAM_INPUT_COLUMNS if c not in streams_wgs84.columns]
    if missing:
        logger.warning("Missing expected stream fields in shapefile: " + ", ".join(missing))
    keep = [c for c in STREAM_INPUT_COLUMNS if c in streams_wgs84.columns]
    rename_map = {c: f"{STREAM_PREFIX}{c}" for c in keep}
    streams_wgs84 = streams_wgs84[keep + ["geometry"]].rename(columns=rename_map)

    streams_proj = streams_wgs84.to_crs(PROJ_CRS)
    _ = streams_proj.sindex
    logger.info(f"Streams loaded: {len(streams_proj):,} features | CRS={streams_proj.crs}")
    return streams_proj

def csv_dtype_all_strings(columns: List[str]) -> dict[str, str]:
    return {col: "string" for col in columns}

def write_chunk(df_chunk_out: pd.DataFrame, output_path: str, header: bool) -> None:
    df_chunk_out.to_csv(
        output_path,
        mode="w" if header else "a",
        header=header,
        index=False,
        encoding="utf-8",
        na_rep="",
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n"
    )

def nearest_point_on_line_proj(point_geom, line_geom):
    try:
        if isinstance(line_geom, (LineString, MultiLineString)):
            d_along = line_geom.project(point_geom)
            return line_geom.interpolate(d_along)
        return nearest_points(point_geom, line_geom)[1]
    except Exception:
        return nearest_points(point_geom, line_geom)[1]

def main():
    logger.info(f"Scanning CSV header: {INPUT_CSV_PATH}")
    header_df = pd.read_csv(INPUT_CSV_PATH, nrows=0, dtype="string")
    csv_cols_in_file = list(header_df.columns)

    missing_csv_cols = [c for c in CSV_COLUMNS_ORDER if c not in csv_cols_in_file]
    if missing_csv_cols:
        logger.error(
            "The input CSV is missing required columns:\n  " + ", ".join(missing_csv_cols)
        )
        sys.exit(1)

    dtype_map = csv_dtype_all_strings(csv_cols_in_file)

    streams_proj = load_streams(SHAPEFILE_PATH, layer_name=STREAM_LAYER_NAME)
    streams_wgs84_bounds = streams_proj.to_crs(WGS84).total_bounds
    minx, miny, maxx, maxy = streams_wgs84_bounds
    logger.info(f"Streams WGS84 bounds: minx={minx:.6f}, miny={miny:.6f}, maxx={maxx:.6f}, maxy={maxy:.6f}")

    to_proj = Transformer.from_crs(WGS84, PROJ_CRS, always_xy=True)
    to_wgs84 = Transformer.from_crs(PROJ_CRS, WGS84, always_xy=True)
    geod = Geod(ellps="WGS84")

    Path(OUTPUT_CSV_PATH).parent.mkdir(parents=True, exist_ok=True)
    if os.path.exists(OUTPUT_CSV_PATH):
        logger.warning(f"Output file exists and will be overwritten: {OUTPUT_CSV_PATH}")
        os.remove(OUTPUT_CSV_PATH)

    first_chunk = True
    total_rows = 0
    total_valid_pts = 0
    total_matched = 0

    logger.info(f"Processing CSV in chunks of {CHUNK_SIZE:,} rows ...")
    chunk_iter = pd.read_csv(
        INPUT_CSV_PATH,
        dtype=dtype_map,
        chunksize=CHUNK_SIZE,
        low_memory=False,
        keep_default_na=False
    )

    with tqdm(desc="Nearest streams", unit="rows", unit_scale=True) as pbar:
        for i, df_chunk in enumerate(chunk_iter, start=1):
            n = len(df_chunk)
            total_rows += n

            base_out = df_chunk.reindex(columns=CSV_COLUMNS_ORDER).copy()

            lat = pd.to_numeric(df_chunk[CSV_LAT_COL], errors="coerce")
            lon = pd.to_numeric(df_chunk[CSV_LON_COL], errors="coerce")

            in_bbox = (
                lat.notna() & lon.notna() &
                (lat >= miny) & (lat <= maxy) &
                (lon >= minx) & (lon <= maxx)
            )
            idx_valid = df_chunk.index[in_bbox]
            total_valid_pts += int(in_bbox.sum())

            stream_cols_all = STREAM_OUTPUT_COLUMNS + [STREAM_DISTANCE_COL]
            # Use object dtype to preserve mixed types from shapefile attributes
            attrs_full = pd.DataFrame(index=df_chunk.index, columns=stream_cols_all, dtype="object")

            if len(idx_valid) > 0:
                gpoints = gpd.GeoDataFrame(
                    df_chunk.loc[idx_valid, []].copy(),
                    geometry=gpd.points_from_xy(lon.loc[idx_valid], lat.loc[idx_valid]),
                    crs=WGS84
                )
                gpoints_proj = gpoints.to_crs(PROJ_CRS)

                existing_stream_cols = [c for c in STREAM_OUTPUT_COLUMNS if c in streams_proj.columns]

                # ---------- NEAREST JOIN (planar) ----------
                joined = gpd.sjoin_nearest(
                    gpoints_proj,
                    streams_proj[existing_stream_cols + ["geometry"]],
                    how="left",
                    distance_col="_planar_m"
                )

                # Collapse any tie-duplicates so there's max 1 match per point
                if joined.index.duplicated().any():
                    joined = joined[~joined.index.duplicated(keep="first")]

                # Ensure 1:1 alignment and row order equals idx_valid
                joined = joined.reindex(gpoints_proj.index)

                # ---------- Build attributes & distances ----------
                if not joined.empty:
                    has_match = joined["index_right"].notna()

                    # Compute geodesic distance only for matched rows
                    geodesic_dist = pd.Series(index=joined.index, dtype="float64")

                    matched_idx = joined.index[has_match]
                    for jdx in tqdm(matched_idx, desc=f"  chunk {i}: geodesic distance", leave=False):
                        try:
                            pt_proj = joined.at[jdx, "geometry"]
                            idx_right = joined.at[jdx, "index_right"]
                            if pd.isna(idx_right):
                                geodesic_dist.at[jdx] = float("nan")
                                continue
                            line_proj = streams_proj.geometry.loc[idx_right]
                            nearest_proj = nearest_point_on_line_proj(pt_proj, line_proj)
                            nx, ny = to_wgs84.transform(nearest_proj.x, nearest_proj.y)
                            px = lon.loc[jdx]
                            py = lat.loc[jdx]
                            _, _, dist_m = geod.inv(px, py, nx, ny)
                            geodesic_dist.at[jdx] = dist_m
                        except Exception:
                            geodesic_dist.at[jdx] = float("nan")

                    attrs_slice = joined[existing_stream_cols].copy()
                    attrs_slice[STREAM_DISTANCE_COL] = geodesic_dist

                    # Ensure all expected stream_* columns exist
                    for col in STREAM_OUTPUT_COLUMNS:
                        if col not in attrs_slice.columns:
                            attrs_slice[col] = pd.NA

                    # Put columns in the exact requested order
                    attrs_slice = attrs_slice.reindex(columns=stream_cols_all)

                    # Assignment is now safe because attrs_slice matches idx_valid length
                    attrs_full.loc[idx_valid, stream_cols_all] = attrs_slice.values

                    total_matched += int(has_match.sum())
                else:
                    logger.info(f"Chunk {i}: no matches produced by sjoin_nearest()")
            else:
                logger.info(f"Chunk {i}: no valid points within streams bbox.")

            chunk_out = pd.concat([base_out, attrs_full.reindex(columns=stream_cols_all)], axis=1)
            chunk_out = chunk_out.reindex(columns=ALL_OUTPUT_COLUMNS_ORDER)

            write_chunk(chunk_out, OUTPUT_CSV_PATH, header=first_chunk)
            first_chunk = False

            if i % SHOW_SAMPLE_LOG_EVERY_N_CHUNKS == 0:
                logger.info(
                    f"Chunk {i:>4}: rows={n:,} | valid_pts_in_bbox={int(in_bbox.sum()):,} | "
                    f"matched_to_stream={int((attrs_full[STREAM_DISTANCE_COL].notna()).sum()):,} | "
                    f"written='{os.path.basename(OUTPUT_CSV_PATH)}'"
                )
            pbar.update(n)

    logger.info("Done.")
    logger.info(f"Total rows processed: {total_rows:,}")
    logger.info(f"Total valid points within streams bbox: {total_valid_pts:,}")
    logger.info(f"Total rows matched to a stream: {total_matched:,}")
    logger.info(f"Output: {OUTPUT_CSV_PATH}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.getLogger().error("Interrupted by user. Partial output may exist.")
        sys.exit(2)
    except Exception as exc:
        logging.getLogger().exception(f"Fatal error: {exc}")
        sys.exit(1)
