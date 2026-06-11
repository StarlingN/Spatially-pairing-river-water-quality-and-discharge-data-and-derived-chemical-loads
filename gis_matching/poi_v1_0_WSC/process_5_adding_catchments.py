# -*- coding: utf-8 -*-
"""
Spatially join WSC POI points (CSV) to CLRH catchment polygons (shapefile),
appending catchment attributes to each row, with shapefile-derived columns
written *prefixed* as "poi_v1_0_WSC_catchment_<name>" in a single pass.

- Streams the CSV in chunks to keep memory reasonable.
- Preserves ALL original CSV fields exactly as text (no type coercion).
- Catchment attributes are appended with 'poi_v1_0_WSC_catchment_' prefix; blanks if no containing polygon.
- Uses 'intersects' (covers boundary points).
- Includes robust error checks and detailed tqdm progress.
"""

from __future__ import annotations

import os
import sys
import csv
import logging
from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely import make_valid
from tqdm import tqdm

# ----------------------------
# ---- CONFIGURATION ----------
# ----------------------------

# INPUTS
# NOTE: Now points to the File Geodatabase (.gdb) instead of a .shp
SHAPEFILE_PATH = r"E:\publications\clawave_1\gis\finalcat_info_v1_0_clipped.gdb"
# The feature class name inside the GDB:
CATCHMENT_LAYER_NAME = "finalcat_info_v1_0_clipped"

INPUT_CSV_PATH = r"E:\publications\clawave_1\poi_v1_0_WSC\process_4_renamed\poi_v1_0_WSC_clipped_renamed.csv"

# OUTPUT
OUTPUT_CSV_PATH = r"E:\publications\clawave_1\poi_v1_0_WSC\process_5_catchments_added\poi_v1_0_WSC_clipped_renamed+catchments.csv"

# Performance / memory
CHUNK_SIZE = 100_000
SHOW_SAMPLE_LOG_EVERY_N_CHUNKS = 1

# Geometry / CRS
EXPECTED_CRS = "EPSG:4326"  # WGS84

# Point-in-polygon predicate:
SJOIN_PREDICATE = "intersects"

# Coordinate columns in the input CSV  (UPDATED)
CSV_LAT_COL = "poi_v1_0_WSC_Lat"
CSV_LON_COL = "poi_v1_0_WSC_Lon"

# Input CSV columns in desired order (UPDATED)
CSV_COLUMNS_ORDER = [
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
]

# Shapefile fields (as they exist in the shapefile) — unchanged list
CATCHMENT_INPUT_COLUMNS = [
    "SubId","DowSubId","RivSlope","RivLength","BasSlope","BasAspect","BasArea",
    "BkfWidth","BkfDepth","Lake_Cat","HyLakeId","LakeVol","LakeDepth","LakeArea","Laketype",
    "Has_POI","MeanElev","FloodP_n","Q_Mean","Ch_n","DrainArea","Strahler",
    "Seg_ID","Seg_order","Max_DEM","Min_DEM","Obs_NM","SRC_obs","centroid_x","centroid_y",
    "DA_Chn_L","DA_Slope","DA_Chn_Slp","outletLat","outletLng","k","c","SSDA_NM","SDA_NM",
    "Shape_Leng","Shape_Area"
]

# Prefix configuration (UPDATED)
CATCHMENT_PREFIX = "poi_v1_0_WSC_catchment_"
def prefixed(col: str) -> str:
    return col if col.startswith(CATCHMENT_PREFIX) else f"{CATCHMENT_PREFIX}{col}"

# Final output catchment columns (desired order, all prefixed)
CATCHMENT_OUTPUT_COLUMNS = [prefixed(c) for c in CATCHMENT_INPUT_COLUMNS]

ALL_OUTPUT_COLUMNS_ORDER = CSV_COLUMNS_ORDER + CATCHMENT_OUTPUT_COLUMNS

# ----------------------------
# ---- LOGGING SETUP ---------
# ----------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("join_poi_to_catchments_prefixed")

# ----------------------------
# ---- HELPERS ---------------
# ----------------------------

def ensure_crs4326(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf.crs is None:
        logger.warning("Shapefile has no CRS. Assuming EPSG:4326; adjust if incorrect.")
        gdf = gdf.set_crs(EXPECTED_CRS)
    elif str(gdf.crs).upper().replace(":", "") != EXPECTED_CRS.replace(":", ""):
        logger.info(f"Reprojecting shapefile from {gdf.crs} to {EXPECTED_CRS} ...")
        gdf = gdf.to_crs(EXPECTED_CRS)
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

def load_catchments(dataset_path: str, layer_name: str | None = None) -> gpd.GeoDataFrame:
    """
    Load catchments from either a shapefile or a File Geodatabase feature class,
    ensure CRS/validity, keep only needed columns,
    and RENAME those columns to have 'poi_v1_0_WSC_catchment_' prefix for output.
    """
    logger.info(f"Loading catchments from: {dataset_path}" + (f" [layer={layer_name}]" if dataset_path.lower().endswith('.gdb') else ""))
    if dataset_path.lower().endswith(".gdb"):
        if not layer_name:
            raise ValueError("A 'layer_name' must be provided when reading from a File Geodatabase (.gdb).")
        catch = gpd.read_file(dataset_path, layer=layer_name)
    else:
        catch = gpd.read_file(dataset_path)

    catch = ensure_crs4326(catch)
    catch = validate_and_fix_geometries(catch)

    # Track missing original (unprefixed) fields
    missing_cols = [c for c in CATCHMENT_INPUT_COLUMNS if c not in catch.columns]
    if missing_cols:
        logger.warning(
            "The following expected catchment columns are missing from the shapefile and will be blank in the output: "
            + ", ".join(missing_cols)
        )

    # Keep only shapefile columns that actually exist
    keep_input = [c for c in CATCHMENT_INPUT_COLUMNS if c in catch.columns]

    # Build rename map to prefixed names (avoid double-prefixing if already present)
    rename_map = {c: prefixed(c) for c in keep_input if not c.startswith(CATCHMENT_PREFIX)}
    # If the shapefile already has some 'poi_v1_0_WSC_catchment_' fields, carry them through as-is.
    keep_prefixed_existing = [c for c in catch.columns if c.startswith(CATCHMENT_PREFIX)]
    # Construct final keep set (prefixed + existing prefixed) + geometry
    catch = catch[keep_input + keep_prefixed_existing + ["geometry"]].rename(columns=rename_map)

    # Build (and cache) spatial index
    _ = catch.sindex
    logger.info(f"Catchments loaded: {len(catch):,} polygons. CRS={catch.crs}.")
    return catch

def csv_dtype_all_strings(columns: list[str]) -> dict[str, str]:
    return {col: "string" for col in columns}

def spatial_join_points_to_polys(
    points_gdf: gpd.GeoDataFrame,
    catchments_gdf: gpd.GeoDataFrame,
    expected_output_cols: list[str],
    predicate: str = SJOIN_PREDICATE
) -> pd.DataFrame:
    """
    Spatially join points to polygons. Returns ONLY the (prefixed) catchment columns,
    indexed by the original point index for alignment.
    """
    if points_gdf.empty:
        return pd.DataFrame(index=points_gdf.index, columns=expected_output_cols)

    poly_cols_out = [c for c in expected_output_cols if c in catchments_gdf.columns]
    polys_min = catchments_gdf[poly_cols_out + ["geometry"]]

    joined = gpd.sjoin(points_gdf[["geometry"]], polys_min, how="left", predicate=predicate)

    # Keep only the polygon attribute columns
    out = joined.drop(columns=[c for c in joined.columns if c not in poly_cols_out], errors="ignore")
    out = out.reindex(points_gdf.index)

    # If multiple polys intersect a point, keep the first
    if out.index.duplicated().any():
        out = out[~out.index.duplicated(keep="first")]

    # Reindex to include all expected columns in order
    out = out.reindex(columns=expected_output_cols)
    return out

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

def main():
    # --- Verify input CSV header and prepare dtype mapping -----------------------
    logger.info(f"Scanning CSV header: {INPUT_CSV_PATH}")
    header_df = pd.read_csv(INPUT_CSV_PATH, nrows=0)
    csv_cols_in_file = list(header_df.columns)

    missing_csv_cols = [c for c in CSV_COLUMNS_ORDER if c not in csv_cols_in_file]
    if missing_csv_cols:
        logger.error(
            "The input CSV is missing the following required columns:\n  " +
            ", ".join(missing_csv_cols)
        )
        sys.exit(1)

    dtype_map = csv_dtype_all_strings(csv_cols_in_file)

    # --- Load catchments once ----------------------------------------------------
    catchments = load_catchments(SHAPEFILE_PATH, layer_name=CATCHMENT_LAYER_NAME)
    minx, miny, maxx, maxy = catchments.total_bounds
    logger.info(f"Catchment bounds (lon/lat): minx={minx:.6f}, miny={miny:.6f}, maxx={maxx:.6f}, maxy={maxy:.6f}")

    # Ensure output dir; overwrite if exists
    Path(OUTPUT_CSV_PATH).parent.mkdir(parents=True, exist_ok=True)
    if os.path.exists(OUTPUT_CSV_PATH):
        logger.warning(f"Output file already exists and will be overwritten: {OUTPUT_CSV_PATH}")
        os.remove(OUTPUT_CSV_PATH)

    first_chunk = True
    total_rows = 0
    total_valid_points = 0
    total_matched = 0

    logger.info(f"Processing CSV in chunks of {CHUNK_SIZE:,} rows ...")
    chunk_iter = pd.read_csv(
        INPUT_CSV_PATH,
        dtype=dtype_map,
        chunksize=CHUNK_SIZE,
        low_memory=False,
        keep_default_na=False
    )

    with tqdm(desc="Joining points to catchments", unit="rows", unit_scale=True) as pbar:
        for i, df_chunk in enumerate(chunk_iter, start=1):
            this_rows = len(df_chunk)
            total_rows += this_rows

            # Convert coords to numeric for masks only (do not mutate original text)
            lat = pd.to_numeric(df_chunk[CSV_LAT_COL], errors="coerce")
            lon = pd.to_numeric(df_chunk[CSV_LON_COL], errors="coerce")

            in_bbox_mask = (
                lat.notna() & lon.notna() &
                (lat >= miny) & (lat <= maxy) &
                (lon >= minx) & (lon <= maxx)
            )
            valid_pts = int(in_bbox_mask.sum())
            total_valid_points += valid_pts

            # Build points GeoDataFrame with the SAME index as df_chunk where valid/bbox-filtered
            gpoints = gpd.GeoDataFrame(
                df_chunk.loc[in_bbox_mask, []].copy(),
                geometry=gpd.points_from_xy(lon.loc[in_bbox_mask], lat.loc[in_bbox_mask]),
                crs=EXPECTED_CRS
            )

            # Spatial join -> prefixed attribute columns
            try:
                attrs = spatial_join_points_to_polys(
                    gpoints,
                    catchments,
                    expected_output_cols=CATCHMENT_OUTPUT_COLUMNS,
                    predicate=SJOIN_PREDICATE
                )
            except Exception as e:
                logger.exception(f"Spatial join failed on chunk {i}: {e}")
                attrs = pd.DataFrame(index=gpoints.index, columns=CATCHMENT_OUTPUT_COLUMNS, dtype="float64")

            matches_mask = attrs.notna().any(axis=1) if not attrs.empty else pd.Series(dtype=bool)
            chunk_matched = int(matches_mask.sum())
            total_matched += chunk_matched

            # Build the output chunk: original CSV columns (in order) + prefixed catchment attributes
            chunk_out = df_chunk.reindex(columns=CSV_COLUMNS_ORDER).copy()

            # Allocate full attribute frame for all rows (default NaN -> blank in CSV)
            attrs_full = pd.DataFrame(index=df_chunk.index, columns=CATCHMENT_OUTPUT_COLUMNS, dtype="float64")
            if not attrs.empty:
                attrs_full.loc[attrs.index, :] = attrs.values

            chunk_out = pd.concat([chunk_out, attrs_full], axis=1)

            # Write out
            write_chunk(chunk_out, OUTPUT_CSV_PATH, header=first_chunk)
            first_chunk = False

            if i % SHOW_SAMPLE_LOG_EVERY_N_CHUNKS == 0:
                logger.info(
                    f"Chunk {i:>4}: rows={this_rows:,} | valid_pts_in_bbox={valid_pts:,} | "
                    f"matched={chunk_matched:,} | written='{os.path.basename(OUTPUT_CSV_PATH)}'"
                )
            pbar.update(this_rows)

    logger.info("Done.")
    logger.info(f"Total rows processed: {total_rows:,}")
    logger.info(f"Total valid points within catchment bbox: {total_valid_points:,}")
    logger.info(f"Total rows matched to a polygon: {total_matched:,}")
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
