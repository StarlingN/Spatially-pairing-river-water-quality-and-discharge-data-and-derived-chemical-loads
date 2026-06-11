# -*- coding: utf-8 -*-
"""
Match hydrological stations (POI) to DataStream water-quality records.

Output = copy of DataStream rows (unchanged) + appended POI columns,
but only for rows that find a single POI match satisfying:
(1.1 OR 1.2) AND 2 AND 3 AND 4

Where:
1.1) All datastream_catchment_* == corresponding poi_v1_0_WSC_catchment_*
OR
1.2) datastream catchment is a direct neighbor (touches) of the POI catchment
    (neighboring relationships built once from the shapefile).

2) All datastream_stream_* == corresponding poi_v1_0_WSC_stream_*
3) datastream_stream_distance < DIST_THRESHOLD and poi_v1_0_WSC_stream_distance < DIST_THRESHOLD
4) geodesic distance between (datastream_wgs84_lat,long) and (poi_v1_0_WSC_Lat,Lon)
   < 2 * DIST_THRESHOLD  (the factor 2 is enforced via 'geo_thresh' computed from DIST_THRESHOLD)

Notes:
- Uses pandas/geopandas/shapely/pyproj (no GDAL).
- Streams DataStream CSV in chunks; writes only matched rows.
- Compares numbers/text robustly via canonicalized signatures.
- Preserves DataStream fields exactly; POI fields appended at the end.
"""

from __future__ import annotations

import os
import sys
import csv
import math
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Set

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import make_valid
from tqdm import tqdm
from pyproj import Geod

# =============================================================================
# --------------------------- CONFIGURATION -----------------------------------
# =============================================================================

# INPUTS
DATASTREAM_CSV = r"E:\publications\clawave_1\datastream\process_6_streams_added\observations_2000_2025_all_clipped_unified_crs_renamed+catchments+streams.csv"
POI_CSV        = r"E:\publications\clawave_1\poi_v1_0_WSC\process_6_streams_added\poi_v1_0_WSC_clipped_renamed+catchments+streams.csv"
CATCHMENT_SHP  = r"E:\publications\clawave_1\gis\finalcat_info_v1_0_clipped.gdb"
CATCHMENT_LAYER = "finalcat_info_v1_0_clipped"

# OUTPUT
OUTPUT_CSV     = r"E:\publications\clawave_1\datastream\process_7_datastream_with_poi_v1_0_WSC_matched\observations_2000_2025_all_clipped_unified_crs_renamed+catchments+streams_matchups.csv"

# Performance
CHUNK_SIZE = 100_000  # rows per chunk for DataStream
LOG_EVERY_N_CHUNKS = 1

# Distance threshold (Condition 3). Condition 4 uses 2 * DIST_THRESHOLD.
DIST_THRESHOLD = 1000.0  # meters

# CRS / Geodesy
WGS84 = "EPSG:4326"
GEOD = Geod(ellps="WGS84")

# Cache adjacency so subsequent runs skip the heavy sjoin step
NEIGHBOR_CACHE = Path(OUTPUT_CSV).with_suffix(".neighbors.pkl")

# =============================================================================
# ----------------------------- COLUMN DEFINITIONS ----------------------------
# =============================================================================

# DataStream (46) base columns (verbatim order)
DS_BASE_COLS: List[str] = [
    "datastream_Id","datastream_DOI","datastream_DatasetName","datastream_MonitoringLocationID","datastream_MonitoringLocationName",
    "datastream_MonitoringLocationLatitude","datastream_MonitoringLocationLongitude",
    "datastream_MonitoringLocationHorizontalCoordinateReferenceSystem",
    "datastream_MonitoringLocationHorizontalAccuracyMeasure","datastream_MonitoringLocationHorizontalAccuracyUnit",
    "datastream_MonitoringLocationVerticalMeasure","datastream_MonitoringLocationVerticalUnit",
    "datastream_MonitoringLocationType","datastream_ActivityType","datastream_ActivityMediaName",
    "datastream_ActivityStartDate","datastream_ActivityStartTime","datastream_ActivityStartTimeZone",
    "datastream_ActivityEndDate","datastream_ActivityEndTime","datastream_ActivityEndTimeZone",
    "datastream_ActivityDepthHeightMeasure","datastream_ActivityDepthHeightUnit","datastream_SampleCollectionEquipmentName",
    "datastream_CharacteristicName","datastream_MethodSpeciation","datastream_ResultSampleFraction","datastream_ResultValue","datastream_ResultUnit",
    "datastream_ResultValueType","datastream_ResultDetectionCondition","datastream_ResultDetectionQuantitationLimitMeasure",
    "datastream_ResultDetectionQuantitationLimitUnit","datastream_ResultDetectionQuantitationLimitType",
    "datastream_ResultStatusID","datastream_ResultComment","datastream_ResultAnalyticalMethodID",
    "datastream_ResultAnalyticalMethodContext","datastream_ResultAnalyticalMethodName",
    "datastream_AnalysisStartDate","datastream_AnalysisStartTime","datastream_AnalysisStartTimeZone",
    "datastream_LaboratoryName","datastream_LaboratorySampleID","datastream_wgs84_lat","datastream_wgs84_long"
]

# Catchment fields (prefix-stripped base names)
CATCHMENT_FIELDS: List[str] = [
    "SubId","DowSubId","RivSlope","RivLength","BasSlope","BasAspect","BasArea",
    "BkfWidth","BkfDepth","Lake_Cat","HyLakeId","LakeVol","LakeDepth","LakeArea","Laketype",
    "Has_POI","MeanElev","FloodP_n","Q_Mean","Ch_n","DrainArea","Strahler",
    "Seg_ID","Seg_order","Max_DEM","Min_DEM","Obs_NM","SRC_obs","centroid_x","centroid_y",
    "DA_Chn_L","DA_Slope","DA_Chn_Slp","outletLat","outletLng","k","c","SSDA_NM","SDA_NM",
    "Shape_Leng","Shape_Area"
]

# Stream fields (prefix-stripped base names) – distance kept separate
STREAM_FIELDS: List[str] = [
    "SubId","DowSubId","RivSlope","RivLength","BasSlope","BasAspect","BasArea",
    "BkfWidth","BkfDepth","Lake_Cat","HyLakeId","LakeVol","LakeDepth","LakeArea","Laketype",
    "Has_POI","MeanElev","FloodP_n","Q_Mean","Ch_n","DrainArea","Strahler",
    "Seg_ID","Seg_order","Max_DEM","Min_DEM","Obs_NM","SRC_obs","centroid_x","centroid_y",
    "DA_Chn_L","DA_Slope","DA_Chn_Slp","outletLat","outletLng","k","c","SSDA_NM","SDA_NM","Shape_Leng"
]
DS_STREAM_DIST = "datastream_stream_distance"
POI_STREAM_DIST = "poi_v1_0_WSC_stream_distance"

# Full DataStream columns = base + catchment + stream + distance
DS_CATCH_COLS = [f"datastream_catchment_{c}" for c in CATCHMENT_FIELDS]
DS_STREAM_COLS = [f"datastream_stream_{c}" for c in STREAM_FIELDS]
DATASTREAM_COLUMNS = DS_BASE_COLS + DS_CATCH_COLS + DS_STREAM_COLS + [DS_STREAM_DIST]

# POI station base columns (16)
POI_BASE_COLS: List[str] = [
    "poi_v1_0_WSC_Id","poi_v1_0_WSC_SubId","poi_v1_0_WSC_Obs_NM","poi_v1_0_WSC_DA_Obs","poi_v1_0_WSC_DrainArea","poi_v1_0_WSC_DA_Diff",
    "poi_v1_0_WSC_SRC_obs","poi_v1_0_WSC_Use_region","poi_v1_0_WSC_Gauge_nm","poi_v1_0_WSC_data_tp",
    "poi_v1_0_WSC_Lat","poi_v1_0_WSC_Lon","poi_v1_0_WSC_SSDA_NM","poi_v1_0_WSC_SDA_NM","poi_v1_0_WSC_Notes","poi_v1_0_WSC_ifdelete"
]
POI_CATCH_COLS = [f"poi_v1_0_WSC_catchment_{c}" for c in CATCHMENT_FIELDS]
POI_STREAM_COLS = [f"poi_v1_0_WSC_stream_{c}" for c in STREAM_FIELDS]

# Columns to append to output (exact order requested)
POI_APPEND_COLS = POI_BASE_COLS + POI_CATCH_COLS + POI_STREAM_COLS + [POI_STREAM_DIST]

# Final output columns order
ALL_OUTPUT_COLS = DATASTREAM_COLUMNS + POI_APPEND_COLS

# Convenience column names
DS_LAT = "datastream_wgs84_lat"
DS_LON = "datastream_wgs84_long"
POI_LAT = "poi_v1_0_WSC_Lat"
POI_LON = "poi_v1_0_WSC_Lon"
DS_CATCH_SUBID = "datastream_catchment_SubId"
POI_CATCH_SUBID = "poi_v1_0_WSC_catchment_SubId"

# =============================================================================
# ----------------------------- LOGGING ---------------------------------------
# =============================================================================

# FIX: proper logging placeholders
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("datastream_poi_matcher")

# =============================================================================
# ----------------------------- HELPERS ---------------------------------------
# =============================================================================

def ensure_dirs():
    Path(OUTPUT_CSV).parent.mkdir(parents=True, exist_ok=True)

def canonicalize_series(s: pd.Series) -> pd.Series:
    """
    Return canonical string representation for robust equality:
    - Numeric values → general format with 12 significant digits.
    - Text → stripped, lowercased.
    - Missing → empty string.
    """
    s2 = s.copy()
    # Convert to string early to avoid dtype issues
    out = s2.astype("string").fillna("").str.strip().str.lower()
    # Compute numeric mask and format numerics in-place
    num = pd.to_numeric(s2, errors="coerce")
    mask = num.notna()
    if mask.any():
        out.loc[mask] = num.loc[mask].map(lambda v: f"{float(v):.12g}")
    return out.fillna("")

def build_signature(df: pd.DataFrame, cols: List[str]) -> pd.Series:
    """
    Build row-wise signature string by concatenating canonicalized
    values of the given columns with a '|' separator.
    """
    if not cols:
        return pd.Series([""], index=df.index, dtype="string")
    can = pd.DataFrame({c: canonicalize_series(df[c]) for c in cols})
    sig = can.agg("|".join, axis=1).astype("string")
    return sig

def geodesic_distance_m(lat1, lon1, lat2, lon2) -> float:
    try:
        _, _, dist_m = GEOD.inv(float(lon1), float(lat1), float(lon2), float(lat2))
        return float(dist_m)
    except Exception:
        return math.inf

def load_poi_and_build_indices() -> Tuple[pd.DataFrame, Dict[str, int], Dict[Tuple[str, str], int], pd.Series, pd.Series, pd.Series]:
    """
    Load POI CSV (dtype=str), verify columns, and build:
      - map_catch_stream: (catch_sig||stream_sig) -> best poi idx
      - map_sub_stream: (SubId, stream_sig) -> best poi idx
    'Best' = minimal poi_v1_0_WSC_stream_distance; ties → first.
    """
    logger.info("Loading POI stations: %s", POI_CSV)
    poi_df = pd.read_csv(POI_CSV, dtype="string", keep_default_na=False, low_memory=False)
    missing = [c for c in (POI_BASE_COLS + POI_CATCH_COLS + POI_STREAM_COLS + [POI_STREAM_DIST]) if c not in poi_df.columns]
    if missing:
        raise KeyError(f"POI CSV missing required columns: {', '.join(missing)}")

    poi_catch_sig = build_signature(poi_df, POI_CATCH_COLS)
    poi_stream_sig = build_signature(poi_df, POI_STREAM_COLS)

    poi_lat = pd.to_numeric(poi_df[POI_LAT], errors="coerce")
    poi_lon = pd.to_numeric(poi_df[POI_LON], errors="coerce")
    poi_stream_dist = pd.to_numeric(poi_df[POI_STREAM_DIST], errors="coerce")

    logger.info("Indexing POI by (catchment signature, stream signature)...")
    key_catch_stream = (poi_catch_sig + "||" + poi_stream_sig).astype("string")
    aux = pd.DataFrame({
        "_key": key_catch_stream,
        "_idx": np.arange(len(poi_df)),
        "_dist": poi_stream_dist
    })
    aux["_dist_filled"] = aux["_dist"].fillna(np.inf)
    aux_sorted = aux.sort_values(by=["_key", "_dist_filled", "_idx"], kind="mergesort")
    best_catch_stream = aux_sorted.drop_duplicates(subset="_key", keep="first")
    map_catch_stream = dict(zip(best_catch_stream["_key"], best_catch_stream["_idx"]))

    logger.info("Indexing POI by (catchment SubId, stream signature)...")
    poi_subid = canonicalize_series(poi_df[POI_CATCH_SUBID])
    key_sub_stream = list(zip(poi_subid.tolist(), poi_stream_sig.tolist()))
    aux2 = pd.DataFrame({
        "_key_sub": key_sub_stream,
        "_idx": np.arange(len(poi_df)),
        "_dist": poi_stream_dist
    })
    aux2["_dist_filled"] = aux2["_dist"].fillna(np.inf)
    aux2_sorted = aux2.sort_values(by=["_key_sub", "_dist_filled", "_idx"], kind="mergesort")
    best_sub_stream = aux2_sorted.drop_duplicates(subset="_key_sub", keep="first")
    map_sub_stream = dict(zip(best_sub_stream["_key_sub"], best_sub_stream["_idx"]))

    return poi_df, map_catch_stream, map_sub_stream, poi_lat, poi_lon, poi_stream_dist

def load_catchments_and_neighbors() -> Dict[str, Set[str]]:
    """
    Load catchment polygons and compute adjacency (touching) sets by SubId.
    Returns dict: subid(str) -> set(neighbor_subid str).
    Safe across GeoPandas versions; avoids index_left/right and supports predicate/op.
    """
    # Try to load a cached neighbor map
    try:
        if NEIGHBOR_CACHE.exists():
            logger.info("Loading cached catchment adjacency: %s", NEIGHBOR_CACHE)
            import pickle
            with open(NEIGHBOR_CACHE, "rb") as f:
                neighbors = pickle.load(f)
            if isinstance(neighbors, dict) and neighbors:
                logger.info("Cached adjacency loaded (%s entries).", f"{len(neighbors):,}")
                return neighbors
            else:
                logger.warning("Neighbor cache found but invalid/empty; rebuilding…")
    except Exception as e:
        logger.warning("Failed to load neighbor cache (%s); rebuilding…", e)

    logger.info("Loading catchment polygons: %s", CATCHMENT_SHP)
    # --- Only change here: support FileGDB feature class ---
    if CATCHMENT_SHP.lower().endswith(".gdb"):
        gdf = gpd.read_file(CATCHMENT_SHP, layer=CATCHMENT_LAYER)[["SubId", "geometry"]].copy()
    else:
        gdf = gpd.read_file(CATCHMENT_SHP)[["SubId", "geometry"]].copy()
    # --------------------------------------------------------

    # Ensure CRS and geometry validity
    if gdf.crs is None or str(gdf.crs).upper().replace(":", "") != WGS84.replace(":", ""):
        logger.info("Reprojecting catchments to %s for adjacency…", WGS84)
        gdf = gdf.to_crs(WGS84)

    invalid_count = (~gdf.geometry.is_valid).sum()
    if invalid_count:
        logger.warning("Found %s invalid geometries; repairing with make_valid()…", f"{invalid_count:,}")
        gdf = gdf.set_geometry(gdf.geometry.map(make_valid))

    # Canonicalize SubId for stable keys
    gdf["SubId"] = canonicalize_series(gdf["SubId"])

    logger.info("Building adjacency (touches) graph via spatial join… (this can be heavy once)")
    left = gdf[["SubId", "geometry"]].copy()
    right = gdf[["SubId", "geometry"]].copy()

    # GeoPandas compatibility: predicate vs op
    try:
        sjoin = gpd.sjoin(left, right, how="inner", predicate="touches", lsuffix="L", rsuffix="R")
    except TypeError:
        # Older GeoPandas uses `op=`
        sjoin = gpd.sjoin(left, right, how="inner", op="touches", lsuffix="L", rsuffix="R")

    # Determine SubId columns from join result
    # Prefer explicit suffix names, then fallbacks
    if "SubId_L" in sjoin.columns and "SubId_R" in sjoin.columns:
        sub_left_col, sub_right_col = "SubId_L", "SubId_R"
    elif "SubId_left" in sjoin.columns and "SubId_right" in sjoin.columns:
        sub_left_col, sub_right_col = "SubId_left", "SubId_right"
    else:
        if "SubId_right" in sjoin.columns and "SubId" in sjoin.columns:
            sub_left_col, sub_right_col = "SubId", "SubId_right"
        elif "SubId_left" in sjoin.columns and "SubId" in sjoin.columns:
            sub_left_col, sub_right_col = "SubId_left", "SubId"
        else:
            raise KeyError("Could not find left/right SubId columns after spatial join.")

    # Remove self pairs
    sjoin = sjoin[sjoin[sub_left_col] != sjoin[sub_right_col]]

    # Build neighbor sets
    neighbors: Dict[str, Set[str]] = {}
    for subid_left, grp in sjoin.groupby(sub_left_col):
        neighbors[str(subid_left)] = set(grp[sub_right_col].astype(str).tolist())
    # Ensure all polygons appear
    for sid in gdf["SubId"]:
        neighbors.setdefault(str(sid), set())

    logger.info("Adjacency graph built for %s catchments.", f"{len(neighbors):,}")

    # Save cache
    try:
        import pickle
        with open(NEIGHBOR_CACHE, "wb") as f:
            pickle.dump(neighbors, f, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info("Cached adjacency to %s", NEIGHBOR_CACHE)
    except Exception as e:
        logger.warning("Could not cache adjacency: %s", e)

    return neighbors

def write_header(output_path: str):
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(ALL_OUTPUT_COLS)

def append_rows(df_out: pd.DataFrame, output_path: str):
    df_out.to_csv(
        output_path,
        mode="a",
        header=False,  # header already written
        index=False,
        encoding="utf-8",
        na_rep="",
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n"
    )

# =============================================================================
# ----------------------------- MAIN PIPELINE ---------------------------------
# =============================================================================

def main():
    Path(OUTPUT_CSV).parent.mkdir(parents=True, exist_ok=True)

    # -------- Load POI + indices --------
    poi_df, map_catch_stream, map_sub_stream, poi_lat, poi_lon, poi_stream_dist = load_poi_and_build_indices()

    # -------- Build/load catchment adjacency graph --------
    neighbors_by_subid = load_catchments_and_neighbors()

    # -------- Prepare output header --------
    if os.path.exists(OUTPUT_CSV):
        logger.warning("Output already exists and will be overwritten: %s", OUTPUT_CSV)
        os.remove(OUTPUT_CSV)
    write_header(OUTPUT_CSV)

    # -------- Verify DataStream columns exist --------
    logger.info("Scanning DataStream CSV header: %s", DATASTREAM_CSV)
    header_df = pd.read_csv(DATASTREAM_CSV, nrows=0, dtype="string")
    ds_missing = [c for c in DATASTREAM_COLUMNS if c not in header_df.columns]
    if ds_missing:
        raise KeyError("DataStream CSV missing required columns:\n  " + ", ".join(ds_missing))

    # -------- Stream DataStream CSV in chunks --------
    total_in = 0
    total_written = 0
    total_matched = 0
    geo_thresh = 2.0 * DIST_THRESHOLD

    logger.info("Processing DataStream in chunks of %,d rows ...", CHUNK_SIZE)
    ds_iter = pd.read_csv(
        DATASTREAM_CSV,
        dtype="string",
        chunksize=CHUNK_SIZE,
        low_memory=False,
        keep_default_na=False
    )

    with tqdm(desc="Matching POI to DataStream", unit="rows", unit_scale=True) as pbar:
        for ci, df in enumerate(ds_iter, start=1):
            n = len(df)
            total_in += n

            # Base output frame: start with exact DataStream columns in order
            base_out = df.reindex(columns=DATASTREAM_COLUMNS).copy()

            # Prepare an empty frame for POI columns (object dtype)
            poi_block = pd.DataFrame(index=df.index, columns=POI_APPEND_COLS, dtype="object")

            # Quick pre-filter for Condition 3 on DataStream side
            ds_dist = pd.to_numeric(df[DS_STREAM_DIST], errors="coerce")
            ds_ok_dist = ds_dist.notna() & (ds_dist < DIST_THRESHOLD)

            # Build canonical signatures for primary equality join ((1.1 AND 2) together)
            ds_catch_sig = build_signature(df, DS_CATCH_COLS)
            ds_stream_sig = build_signature(df, DS_STREAM_COLS)
            key_primary = (ds_catch_sig + "||" + ds_stream_sig).astype("string")

            # Map to best POI index by exact catchment+stream signature
            idx_primary = key_primary.map(map_catch_stream).astype("float")  # float to allow NaN
            primary_mask = idx_primary.notna() & ds_ok_dist

            matched_primary = pd.Series(False, index=df.index)

            if primary_mask.any():
                idx_list = idx_primary.loc[primary_mask].astype(int).tolist()
                # POI distance filter (Condition 3)
                poi_dist_arr = poi_stream_dist.iloc[idx_list].values
                poi_ok_dist = poi_dist_arr < DIST_THRESHOLD

                # Geodesic distance (Condition 4)
                ds_lat = pd.to_numeric(df.loc[primary_mask, DS_LAT], errors="coerce").values
                ds_lon = pd.to_numeric(df.loc[primary_mask, DS_LON], errors="coerce").values
                poi_lat_arr = poi_lat.iloc[idx_list].values
                poi_lon_arr = poi_lon.iloc[idx_list].values

                with np.errstate(all="ignore"):
                    _, _, geo_dists = GEOD.inv(ds_lon, ds_lat, poi_lon_arr, poi_lat_arr)
                geo_ok = geo_dists < geo_thresh

                ok = poi_ok_dist & geo_ok & ~np.isnan(geo_dists)
                ok_idx_positions = np.where(ok)[0]

                if ok_idx_positions.size > 0:
                    accepted_rows = idx_primary.loc[primary_mask].iloc[ok_idx_positions].index
                    accepted_poi_idx = [idx_list[k] for k in ok_idx_positions]

                    for col in POI_APPEND_COLS:
                        poi_block.loc[accepted_rows, col] = poi_df.loc[accepted_poi_idx, col].values

                    matched_primary.loc[accepted_rows] = True

            # Remaining rows to try adjacency (1.2)
            remaining_mask = (~matched_primary) & ds_ok_dist

            ds_subid_rem = canonicalize_series(df.loc[remaining_mask, DS_CATCH_SUBID])
            ds_stream_sig_rem = ds_stream_sig.loc[remaining_mask]
            ds_lat_rem = pd.to_numeric(df.loc[remaining_mask, DS_LAT], errors="coerce")
            ds_lon_rem = pd.to_numeric(df.loc[remaining_mask, DS_LON], errors="coerce")

            matched_adj = pd.Series(False, index=df.index)

            if remaining_mask.any():
                rem_index = ds_subid_rem.index.tolist()
                for ridx in tqdm(rem_index, desc=f"  chunk {ci}: adjacency scan", leave=False):
                    subid = ds_subid_rem.at[ridx]
                    stream_sig = ds_stream_sig_rem.at[ridx]
                    if not subid or not stream_sig:
                        continue

                    neighbor_set = neighbors_by_subid.get(subid, set())
                    if not neighbor_set:
                        continue

                    candidates: List[int] = []
                    for nsub in neighbor_set:
                        key = (nsub, stream_sig)
                        cand_idx = map_sub_stream.get(key)
                        if cand_idx is not None:
                            candidates.append(cand_idx)
                    if not candidates:
                        continue

                    # Condition 3 on POI distance
                    cand_dist = poi_stream_dist.iloc[candidates].values
                    mask3 = cand_dist < DIST_THRESHOLD
                    if not mask3.any():
                        continue

                    # Condition 4 (geodesic distance)
                    lat1 = ds_lat_rem.at[ridx]
                    lon1 = ds_lon_rem.at[ridx]
                    if pd.isna(lat1) or pd.isna(lon1):
                        continue
                    lat2 = poi_lat.iloc[candidates].values
                    lon2 = poi_lon.iloc[candidates].values
                    with np.errstate(all="ignore"):
                        _, _, geo = GEOD.inv(np.full_like(lat2, lon1, dtype=float), np.full_like(lat2, lat1, dtype=float),
                                            lon2.astype(float), lat2.astype(float))
                    mask4 = geo < geo_thresh
                    ok_all = mask3 & mask4 & ~np.isnan(geo)
                    if not ok_all.any():
                        continue

                    ok_idx = np.where(ok_all)[0]
                    # Pick smallest stream distance; tie → first
                    best_local = ok_idx[np.argmin(cand_dist[ok_idx])]
                    best_poi = int(candidates[best_local])

                    for col in POI_APPEND_COLS:
                        poi_block.at[ridx, col] = poi_df.at[best_poi, col]
                    matched_adj.at[ridx] = True

            # Rows to write = matched_primary OR matched_adj
            final_mask = matched_primary | matched_adj
            num_matched = int(final_mask.sum())

            if num_matched > 0:
                out_chunk = pd.concat([base_out.loc[final_mask], poi_block.loc[final_mask]], axis=1)
                out_chunk = out_chunk.reindex(columns=ALL_OUTPUT_COLS)
                append_rows(out_chunk, OUTPUT_CSV)
                total_written += len(out_chunk)
                total_matched += num_matched

            if ci % LOG_EVERY_N_CHUNKS == 0:
                logger.info(
                    "Chunk %4d: in=%,d | matched_primary=%,d | matched_adj=%,d | written_cumulative=%,d",
                    ci, n, int(matched_primary.sum()), int(matched_adj.sum()), total_written
                )

            pbar.update(n)

    logger.info("Done.")
    logger.info("Total rows read:    %,d", total_in)
    logger.info("Total rows matched: %,d", total_matched)
    logger.info("Total rows written: %,d", total_written)
    logger.info("Output: %s", OUTPUT_CSV)


# =============================================================================
# --------------------------------- RUN ---------------------------------------
# =============================================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.error("Interrupted by user. Partial output may exist.")
        sys.exit(2)
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        sys.exit(1)
