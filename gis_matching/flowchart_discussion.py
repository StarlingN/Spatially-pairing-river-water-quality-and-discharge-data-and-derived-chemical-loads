#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
  CLAWAVE-1  |  DataStream → WRTDS Pipeline  |  Comprehensive Report Script
=============================================================================
Reads every major CSV produced by the pipeline and prints a richly structured
summary of record counts, station/location tallies, parameter breakdowns, and
data-quality metrics at every text-documented processing step.

Run:
    python pipeline_report.py

Requirements:
    pandas, tqdm  (pip install pandas tqdm)
=============================================================================
"""

from __future__ import annotations

import os
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
from tqdm import tqdm

# ─────────────────────────────────────────────────────────────────────────────
#  FILE PATHS  (edit the root if your data lives somewhere else)
# ─────────────────────────────────────────────────────────────────────────────
ROOT = Path(r"E:\publications\clawave_1\data")

# ── DataStream main pipeline ──────────────────────────────────────────────────
P0_RAW        = ROOT / "datastream/process_0_downloaded/observations_2000_2025_all.csv"
P1_CLIPPED    = ROOT / "datastream/process_1_clipped/observations_2000_2025_all_clipped.csv"
P4_MERGED     = ROOT / "datastream/process_4_merged_unified_crs+renamed/observations_2000_2025_all_clipped_unified_crs_renamed.csv"

# ── Spatial matching outputs (process 7) ──────────────────────────────────────
P7_MATCHED    = ROOT / "datastream/process_7_datastream_with_poi_v1_0_WSC_matched/observations_2000_2025_all_clipped_unified_crs_renamed+catchments+streams_matchups_fixed.csv"
P7_MATCHUPS   = ROOT / "datastream/process_7_datastream_with_poi_v1_0_WSC_matched/canada_datastream_hydat_matchups.csv"

# ── RHBN-enriched matchups (process 9) ────────────────────────────────────────
P9_RHBN       = ROOT / "datastream/process_9_added_rhbn/canada_datastream_hydat_matchups+RHBN.csv"

# ── Per-parameter CSVs (process 10 — cleaned, pre-QA/QC) ─────────────────────
P10_DIR       = ROOT / "datastream/process_10_cleaned"

# ── Per-parameter CSVs (process 11 — final WRTDS-ready) ──────────────────────
P11_DIR       = ROOT / "datastream/process_11_cleaned"

# ── WSC POI reference layers ─────────────────────────────────────────────────
POI_RAW       = ROOT / "poi_v1_0_WSC/process_1_arcgis_clipped/poi_v1_0_WSC_clipped.csv"
POI_STREAMS   = ROOT / "poi_v1_0_WSC/process_6_streams_added/poi_v1_0_WSC_clipped_renamed+catchments+streams.csv"

# ── Chunk size for large-file streaming ──────────────────────────────────────
CHUNK_ROWS    = 300_000

# ─────────────────────────────────────────────────────────────────────────────
#  PRETTY-PRINT HELPERS
# ─────────────────────────────────────────────────────────────────────────────
SEP_MAJOR = "=" * 78
SEP_MINOR = "-" * 78
SEP_THIN  = "·" * 78


def hdr(title: str, char: str = "="):
    width = 78
    bar = char * width
    print(f"\n{bar}")
    print(f"  {title}")
    print(bar)


def sub(title: str):
    print(f"\n  ── {title}")
    print(f"  {SEP_THIN}")


def kv(label: str, value, indent: int = 4):
    pad = " " * indent
    print(f"{pad}{label:<52} {value}")


def kvf(label: str, value: int | float, fmt: str = ",d", indent: int = 4):
    pad = " " * indent
    val_str = format(value, fmt) if isinstance(value, (int, float)) else str(value)
    print(f"{pad}{label:<52} {val_str}")


def section_done(t0: float):
    elapsed = time.time() - t0
    print(f"\n  ✓  Section completed in {elapsed:.1f}s")


def missing_file_warning(path: Path) -> bool:
    if not path.exists():
        print(f"  ⚠  FILE NOT FOUND – skipping: {path}")
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
#  LOW-LEVEL I/O HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def fast_row_count(path: Path) -> int:
    """Fast newline-count row estimate (minus header). Works well for standard CSVs."""
    n = 0
    with open(path, "rb") as f:
        while True:
            buf = f.read(8 * 1024 * 1024)
            if not buf:
                break
            n += buf.count(b"\n")
    return max(0, n - 1)


def file_size_mb(path: Path) -> float:
    return path.stat().st_size / (1024 ** 2)


def chunked_reader(path: Path, usecols=None, dtype=None, desc: str = ""):
    """Yield pandas chunks with a tqdm wrapper."""
    kwargs = dict(
        chunksize=CHUNK_ROWS,
        low_memory=False,
        dtype=dtype or {},
        keep_default_na=False,
    )
    if usecols:
        kwargs["usecols"] = usecols
    total_est = fast_row_count(path)
    reader = pd.read_csv(path, **kwargs)
    with tqdm(total=total_est, unit="rows", unit_scale=True,
               desc=f"  reading {desc or path.name[:40]}", leave=False) as pbar:
        for chunk in reader:
            pbar.update(len(chunk))
            yield chunk


def count_rows_and_unique(path: Path, unique_cols: list[str],
                           round_cols: list[str] | None = None,
                           round_dp: int = 6,
                           desc: str = "") -> dict:
    """
    Stream a CSV and return:
      - total_rows
      - unique_<colname> sets for each requested column
      - unique_latlon set (if 'lat' and 'lon' in round_cols)
    """
    result = {"total_rows": 0}
    sets: dict[str, set] = {c: set() for c in unique_cols}
    coord_pairs: set = set()
    lat_col = lon_col = None
    if round_cols and len(round_cols) == 2:
        lat_col, lon_col = round_cols[0], round_cols[1]

    for chunk in chunked_reader(path, usecols=None, desc=desc):
        result["total_rows"] += len(chunk)
        for c in unique_cols:
            if c in chunk.columns:
                sets[c].update(chunk[c].dropna().astype(str).unique())
        if lat_col and lon_col and lat_col in chunk.columns and lon_col in chunk.columns:
            lat = pd.to_numeric(chunk[lat_col], errors="coerce").round(round_dp)
            lon = pd.to_numeric(chunk[lon_col], errors="coerce").round(round_dp)
            mask = lat.notna() & lon.notna()
            if mask.any():
                coord_pairs.update(zip(lat[mask].to_numpy(), lon[mask].to_numpy()))

    for c, s in sets.items():
        result[f"unique_{c}"] = s
    if round_cols:
        result["unique_latlon"] = coord_pairs
    return result


def read_small_csv(path: Path, dtype: dict | None = None) -> pd.DataFrame | None:
    """Read a small CSV fully into memory; return None if file missing."""
    if missing_file_warning(path):
        return None
    return pd.read_csv(path, dtype=dtype or {}, low_memory=False, keep_default_na=False)


def aggregate_param_dir(directory: Path,
                         stn_col: str = "StationNum",
                         val_col: str = "ResultValue",
                         date_col: str = "ActivityStartDate") -> dict:
    """
    Scan all *.csv files in a directory (excluding subdirs).
    Returns per-parameter stats and global totals.
    """
    result = {
        "params_found": [],
        "param_rows": {},
        "param_stations": {},
        "param_date_range": {},
        "total_rows": 0,
        "all_stations": set(),
    }
    csvs = sorted([p for p in directory.glob("*.csv") if p.is_file()])
    if not csvs:
        return result

    for csv_path in tqdm(csvs, desc=f"  scanning {directory.name}", unit="file", leave=False):
        param_name = csv_path.stem
        df = pd.read_csv(
            csv_path,
            dtype={stn_col: "string"},
            low_memory=False,
            keep_default_na=False,
        )
        n = len(df)
        result["total_rows"] += n
        result["params_found"].append(param_name)
        result["param_rows"][param_name] = n

        # Unique stations
        if stn_col in df.columns:
            stns = set(df[stn_col].dropna().str.strip().unique())
            result["param_stations"][param_name] = stns
            result["all_stations"].update(stns)
        else:
            result["param_stations"][param_name] = set()

        # Date range
        if date_col in df.columns:
            dates = pd.to_datetime(df[date_col], errors="coerce")
            d_min = dates.min()
            d_max = dates.max()
            result["param_date_range"][param_name] = (
                d_min.strftime("%Y-%m-%d") if pd.notna(d_min) else "N/A",
                d_max.strftime("%Y-%m-%d") if pd.notna(d_max) else "N/A",
            )
        else:
            result["param_date_range"][param_name] = ("N/A", "N/A")

    result["params_found"].sort()
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 0  —  RAW DOWNLOAD  (process_0)
# ─────────────────────────────────────────────────────────────────────────────

def report_step0():
    hdr("STEP 0  |  RAW DataStream API Download  (process_0_downloaded)")
    t0 = time.time()
    if missing_file_warning(P0_RAW):
        print("  Skipping Step 0.\n")
        return

    sub("File information")
    kvf("File size (MB)", file_size_mb(P0_RAW), ".1f")
    kvf("Fast row estimate (excl. header)", fast_row_count(P0_RAW))

    sub("Streaming full file to get exact row count + basic stats")
    total_rows = 0
    unique_doi  = set()
    unique_mlid = set()

    for chunk in chunked_reader(P0_RAW, desc="process_0_raw"):
        total_rows += len(chunk)
        if "DOI" in chunk.columns:
            unique_doi.update(chunk["DOI"].dropna().astype(str).unique())
        if "MonitoringLocationID" in chunk.columns:
            unique_mlid.update(chunk["MonitoringLocationID"].dropna().astype(str).unique())

    sub("Record counts")
    kvf("Total raw records downloaded",        total_rows)
    kvf("Unique DataStream DOIs (datasets)",    len(unique_doi))
    kvf("Unique MonitoringLocationIDs",         len(unique_mlid))

    sub("Notes")
    print("    All records are 'River/Stream' MonitoringLocationType,")
    print("    'Surface Water' ActivityMediaName, spanning 2000–2025.")
    section_done(t0)


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 1  —  CLIPPED TO CANADIAN BOUNDARY  (process_1)
# ─────────────────────────────────────────────────────────────────────────────

def report_step1():
    hdr("STEP 1  |  Geographic Clip to Canadian Boundary  (process_1_clipped)")
    t0 = time.time()
    if missing_file_warning(P1_CLIPPED):
        print("  Skipping Step 1.\n")
        return

    sub("File information")
    kvf("File size (MB)", file_size_mb(P1_CLIPPED), ".1f")

    total_rows = 0
    unique_mlid = set()
    lat_col = "MonitoringLocationLatitude"
    lon_col = "MonitoringLocationLongitude"
    unique_latlon: set = set()

    for chunk in chunked_reader(P1_CLIPPED, desc="process_1_clipped"):
        total_rows += len(chunk)
        if "MonitoringLocationID" in chunk.columns:
            unique_mlid.update(chunk["MonitoringLocationID"].dropna().astype(str).unique())
        if lat_col in chunk.columns and lon_col in chunk.columns:
            lat = pd.to_numeric(chunk[lat_col], errors="coerce").round(6)
            lon = pd.to_numeric(chunk[lon_col], errors="coerce").round(6)
            mask = lat.notna() & lon.notna()
            if mask.any():
                unique_latlon.update(zip(lat[mask].to_numpy(), lon[mask].to_numpy()))

    sub("Record counts after spatial clipping")
    kvf("Total records within Canadian boundary",     total_rows)
    kvf("Unique MonitoringLocationIDs",               len(unique_mlid))
    kvf("Unique (lat, lon) coordinate pairs (6 dp)",  len(unique_latlon))

    sub("Bbox used for clip")
    print("    Lat  41.676569 – 83.137545  |  Lon  −141.002002 – −52.619458")
    section_done(t0)


# ─────────────────────────────────────────────────────────────────────────────
#  STEP 2-3  —  CRS HARMONISATION NOTE (no file count change documented)
# ─────────────────────────────────────────────────────────────────────────────

def report_step2_3():
    hdr("STEPS 2–3  |  CRS Split  →  ArcGIS Coordinate Harmonisation  (process_2 / process_3)")
    print()
    print("  No record-count change occurs at this stage.")
    print("  The dataset is split by CRS label (NAD27 / NAD83 / WGS84 / UNKWN),")
    print("  reprojected to WGS84 decimal-degree coordinates (4 d.p.) using")
    print("  ArcGIS Pro v2.9, then reassembled in process_4.")
    print()
    print("  CRS categories in raw dataset: NAD27, NAD83, WGS84, UNKWN")
    print("  Target CRS after harmonisation: WGS84 (EPSG:4326)")


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 4  —  MERGED + RENAMED  (process_4)
# ─────────────────────────────────────────────────────────────────────────────

def report_step4():
    hdr("STEP 4  |  Merged & WGS84-Unified Dataset  (process_4_merged)")
    t0 = time.time()
    if missing_file_warning(P4_MERGED):
        print("  Skipping Step 4.\n")
        return

    sub("File information")
    kvf("File size (MB)", file_size_mb(P4_MERGED), ".1f")

    total_rows = 0
    lat_col = "datastream_wgs84_lat"
    lon_col = "datastream_wgs84_long"
    unique_latlon: set = set()

    for chunk in chunked_reader(P4_MERGED, desc="process_4_merged"):
        total_rows += len(chunk)
        if lat_col in chunk.columns and lon_col in chunk.columns:
            lat = pd.to_numeric(chunk[lat_col], errors="coerce").round(6)
            lon = pd.to_numeric(chunk[lon_col], errors="coerce").round(6)
            mask = lat.notna() & lon.notna()
            if mask.any():
                unique_latlon.update(zip(lat[mask].to_numpy(), lon[mask].to_numpy()))

    sub("Record counts — all CRS groups reunified")
    kvf("Total records (should match Step 1)",          total_rows)
    kvf("Unique (wgs84_lat, wgs84_long) pairs (6 dp)",  len(unique_latlon))

    sub("Column naming")
    print("    All columns now carry the 'datastream_' prefix.")
    print("    Coordinate columns: datastream_wgs84_lat, datastream_wgs84_long (4 d.p.)")
    section_done(t0)


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 5-6  —  CATCHMENT + STREAM JOINING (note only)
# ─────────────────────────────────────────────────────────────────────────────

def report_step5_6():
    hdr("STEPS 5–6  |  Spatial Join: Catchments & Streams  (process_5 / process_6)")
    print()
    print("  CLRH (Canadian Lake and River Hydrofabric v1) attributes appended to")
    print("  every DataStream observation via point-in-polygon (catchment) and")
    print("  nearest-segment (stream) spatial joins.")
    print()
    print("  Catchment layer : finalcat_info_v1_0_clipped.gdb")
    print("  Stream layer    : finalcat_info_riv_v1_0_clipped")
    print()
    print("  Predicate used  : intersects  (covers boundary points)")
    print("  No records are dropped at this stage; NaN is written where no")
    print("  polygon/segment intersects the observation point.")
    print()
    print("  Output columns added (prefix 'datastream_catchment_' / 'datastream_stream_'):")
    print("    SubId, DowSubId, RivSlope, RivLength, BasArea, DrainArea, Strahler,")
    print("    Obs_NM, SRC_obs, outletLat, outletLng, centroid_x/y, …  (41 fields each)")


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 7  —  SPATIAL PAIRING WITH WSC POI / HYDAT  (process_7)
# ─────────────────────────────────────────────────────────────────────────────

def report_step7_poi():
    """WSC POI reference file statistics."""
    hdr("REFERENCE  |  WSC POI Layer  (poi_v1_0_WSC)")
    t0 = time.time()
    if missing_file_warning(POI_RAW):
        return

    df = read_small_csv(POI_RAW)
    if df is None:
        return

    sub("WSC POI clipped to Canadian domain")
    kvf("Total WSC POI records (HYDAT gauge-catchments)", len(df))

    obs_col = "Obs_NM" if "Obs_NM" in df.columns else None
    if obs_col:
        kvf("Unique HYDAT station IDs (Obs_NM)", df[obs_col].nunique())
    section_done(t0)


def report_step7_matched():
    """Full matched observation file."""
    hdr("STEP 7a  |  Spatial Pairing Output — Full Matched Records  (process_7 matchups_fixed)")
    t0 = time.time()
    if missing_file_warning(P7_MATCHED):
        print("  Skipping.\n")
        return

    sub("File information")
    kvf("File size (MB)", file_size_mb(P7_MATCHED), ".1f")

    total_rows = 0
    unique_hydat: set = set()
    unique_latlon: set = set()
    unique_ds_locid: set = set()
    date_min = date_max = None
    dist_ds_vals = []   # sample for stream-distance stats (every Nth chunk)
    char_counter: Counter = Counter()
    SAMPLE_EVERY = 3   # collect distance stats every N chunks to save RAM

    lat_col = "datastream_wgs84_lat"
    lon_col = "datastream_wgs84_long"
    stn_col = "poi_v1_0_WSC_Obs_NM"
    locid_col = "datastream_MonitoringLocationID"
    dist_col = "datastream_stream_distance"
    char_col = "datastream_CharacteristicName"
    date_col = "datastream_ActivityStartDate"

    chunk_idx = 0
    for chunk in chunked_reader(P7_MATCHED, desc="process_7_matched_fixed"):
        total_rows += len(chunk)
        chunk_idx += 1

        if stn_col in chunk.columns:
            unique_hydat.update(chunk[stn_col].dropna().astype(str).str.strip().unique())
        if locid_col in chunk.columns:
            unique_ds_locid.update(chunk[locid_col].dropna().astype(str).unique())
        if lat_col in chunk.columns and lon_col in chunk.columns:
            lat = pd.to_numeric(chunk[lat_col], errors="coerce").round(6)
            lon = pd.to_numeric(chunk[lon_col], errors="coerce").round(6)
            mask = lat.notna() & lon.notna()
            if mask.any():
                unique_latlon.update(zip(lat[mask].to_numpy(), lon[mask].to_numpy()))
        if date_col in chunk.columns:
            dates = pd.to_datetime(chunk[date_col], errors="coerce")
            cmin = dates.min()
            cmax = dates.max()
            if pd.notna(cmin):
                date_min = cmin if date_min is None else min(date_min, cmin)
            if pd.notna(cmax):
                date_max = cmax if date_max is None else max(date_max, cmax)
        if char_col in chunk.columns:
            char_counter.update(chunk[char_col].dropna().astype(str).tolist())
        if dist_col in chunk.columns and chunk_idx % SAMPLE_EVERY == 0:
            dist_vals = pd.to_numeric(chunk[dist_col], errors="coerce").dropna()
            dist_ds_vals.extend(dist_vals.sample(min(500, len(dist_vals)), random_state=42).tolist())

    # Remove empty/placeholder station strings
    unique_hydat.discard("")
    unique_hydat.discard("nan")
    unique_hydat.discard("<NA>")
    unique_ds_locid.discard("")

    sub("Record & station counts after spatial pairing")
    kvf("Total matched observation records",              total_rows)
    kvf("Unique HYDAT station IDs matched (Obs_NM)",     len(unique_hydat))
    kvf("Unique DataStream monitoring locations",         len(unique_latlon))
    kvf("Unique DataStream MonitoringLocationIDs",        len(unique_ds_locid))

    sub("Temporal coverage of matched records")
    kvf("Earliest ActivityStartDate", date_min.strftime("%Y-%m-%d") if date_min else "N/A")
    kvf("Latest   ActivityStartDate", date_max.strftime("%Y-%m-%d") if date_max else "N/A")

    sub("Matching algorithm thresholds applied")
    print("    • Max DataStream–stream distance  : 1,000 m")
    print("    • Max POI–stream distance         : 1,000 m")
    print("    • Max inter-station geodesic dist : 2,000 m")
    print("    • Topo condition: same OR adjacent CLRH catchment + same stream segment")

    sub("Stream-distance statistics (DataStream observations → matched stream segment)")
    if dist_ds_vals:
        import statistics
        kvf("Sample size for distance stats", len(dist_ds_vals))
        kvf("Min  dist to stream (m)", f"{min(dist_ds_vals):.2f}")
        kvf("Max  dist to stream (m)", f"{max(dist_ds_vals):.2f}")
        kvf("Mean dist to stream (m)", f"{statistics.mean(dist_ds_vals):.2f}")
        kvf("Median dist to stream (m)", f"{statistics.median(dist_ds_vals):.2f}")

    sub("Top-20 water quality parameters in matched dataset (CharacteristicName)")
    top20 = char_counter.most_common(20)
    for rank, (param, cnt) in enumerate(top20, 1):
        print(f"    {rank:>2}. {param:<52} {cnt:>10,}")
    kvf("Total distinct CharacteristicName values", len(char_counter))

    section_done(t0)


def report_step7_matchups():
    """Unique location–station lookup table."""
    hdr("STEP 7b  |  Unique Location–Station Lookup Table  (canada_datastream_hydat_matchups.csv)")
    t0 = time.time()
    if missing_file_warning(P7_MATCHUPS):
        return

    df = read_small_csv(P7_MATCHUPS, dtype={"StationNum": "string"})
    if df is None:
        return

    sub("File structure")
    kvf("Total rows (unique lat/lon/CRS + StationNum combos)", len(df))
    kv("Columns", list(df.columns))

    sub("Station-level stats")
    if "StationNum" in df.columns:
        unique_stns = df["StationNum"].dropna().str.strip()
        unique_stns = unique_stns[unique_stns != ""]
        kvf("Unique HYDAT stations (StationNum)",              unique_stns.nunique())

        # How many DataStream locations per HYDAT station?
        stn_counts = df.groupby("StationNum").size()
        kvf("HYDAT stations with exactly 1 DataStream location",
            int((stn_counts == 1).sum()))
        kvf("HYDAT stations with >1 DataStream locations",
            int((stn_counts > 1).sum()))
        kvf("Max DataStream locations for one HYDAT station",  int(stn_counts.max()))
        kvf("Mean DataStream locations per HYDAT station",
            f"{stn_counts.mean():.2f}")

    sub("Coordinate coverage")
    for col in ["MonitoringLocationLatitude", "MonitoringLocationLongitude"]:
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            kvf(f"{col} — range",
                f"{vals.min():.4f}  to  {vals.max():.4f}")

    section_done(t0)


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 9  —  RHBN INTEGRATION
# ─────────────────────────────────────────────────────────────────────────────

def report_step9():
    hdr("STEP 9  |  RHBN Integration  (process_9_added_rhbn)")
    t0 = time.time()
    if missing_file_warning(P9_RHBN):
        return

    df = read_small_csv(P9_RHBN, dtype={"StationNum": "string"})
    if df is None:
        return

    sub("RHBN flag column: rhbn_listed  (1 = RHBN member, 0 = not)")
    if "rhbn_listed" in df.columns:
        df["rhbn_listed"] = pd.to_numeric(df["rhbn_listed"], errors="coerce").fillna(0).astype(int)
        val_counts = df["rhbn_listed"].value_counts().sort_index()
        kvf("Rows with rhbn_listed = 1  (RHBN stations)",   int(val_counts.get(1, 0)))
        kvf("Rows with rhbn_listed = 0  (non-RHBN)",        int(val_counts.get(0, 0)))

    sub("After filtering to RHBN-only stations")
    if "StationNum" in df.columns and "rhbn_listed" in df.columns:
        rhbn_df = df[df["rhbn_listed"] == 1].copy()
        rhbn_df["StationNum"] = rhbn_df["StationNum"].astype(str).str.strip()
        rhbn_stns = set(rhbn_df["StationNum"].unique()) - {"", "nan", "<NA>"}
        kvf("Unique RHBN HYDAT stations in matched dataset",  len(rhbn_stns))
        kvf("Unique DataStream locations linked to RHBN stns", len(rhbn_df))

        sub("RHBN sub-designation note")
        print("    Both RHBN-N (natural) and RHBN-U (urbanizing) sub-designations retained.")
        print("    RHBN registry total: 1,075 stations Canada-wide.")
        print("    Stations in our matched dataset that appear in RHBN: shown above.")

    section_done(t0)


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 8 / 10  —  PARAMETER FILTERING + COLUMN CLEANING  (process_10)
# ─────────────────────────────────────────────────────────────────────────────

def report_step10():
    hdr("STEP 10  |  Parameter Filtering + Column Cleaning  (process_10_cleaned)")
    t0 = time.time()
    if not P10_DIR.exists():
        print(f"  ⚠  Directory not found: {P10_DIR}")
        return

    stats = aggregate_param_dir(P10_DIR, stn_col="StationNum",
                                 val_col="ResultValue",
                                 date_col="ActivityStartDate")

    sub("15 target parameters selected (CharacteristicName)")
    print("    Total Phosphorus, Organic Carbon, Total Nitrogen, Inorganic Nitrogen,")
    print("    Nitrite, Orthophosphate, Kjeldahl Nitrogen, Ammonia, Nitrate,")
    print("    Chloride, Magnesium, Total Hardness, Temperature (water), pH,")
    print("    Alkalinity (total)")

    sub("Record & station counts across all 15 parameter files")
    kvf("Total records (all parameters combined)",       stats["total_rows"])
    kvf("Unique HYDAT stations across all parameters",  len(stats["all_stations"]))
    kvf("Number of parameter files found",              len(stats["params_found"]))

    sub("Per-parameter row counts and unique station counts")
    print(f"    {'Parameter':<50} {'Rows':>10}   {'Stations':>9}   {'Date range'}")
    print(f"    {'-'*50} {'-'*10}   {'-'*9}   {'-'*22}")
    for p in sorted(stats["params_found"]):
        n_rows = stats["param_rows"].get(p, 0)
        n_stns = len(stats["param_stations"].get(p, set()))
        d_range = stats["param_date_range"].get(p, ("N/A", "N/A"))
        print(f"    {p:<50} {n_rows:>10,}   {n_stns:>9,}   {d_range[0]}  –  {d_range[1]}")

    sub("Cleaning operations applied at this step")
    print("    • Retained only 'datastream_*' columns (dropped catchment/stream attributes)")
    print("    • Stripped 'datastream_' prefix from all column names")
    print("    • Left-joined StationNum and rhbn_listed from RHBN matchups table")
    print("      (join keys: MonitoringLocationLatitude, Longitude, CRS)")
    print("    • rhbn_listed = 0 for any observation not matched in the RHBN table")

    section_done(t0)


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION 11  —  TEMPORAL QA/QC (WRTDS-READY)  (process_11)
# ─────────────────────────────────────────────────────────────────────────────

def report_step11():
    hdr("STEP 11  |  Temporal QA/QC — WRTDS-Ready Dataset  (process_11_cleaned)")
    t0 = time.time()
    if not P11_DIR.exists():
        print(f"  ⚠  Directory not found: {P11_DIR}")
        return

    stats = aggregate_param_dir(P11_DIR, stn_col="StationNum",
                                 val_col="ResultValue",
                                 date_col="ActivityStartDate")

    sub("Temporal QA/QC criteria applied")
    print("    1. RHBN-only stations (rhbn_listed == 1)")
    print("    2. ≥ 4 consecutive valid years per station-parameter pair")
    print("    3. ≥ 10 discrete measurements per valid year")
    print("    4. ≥ 8 consecutive months with data within each valid year")

    sub("Record & station counts — post-QA/QC")
    kvf("Total WRTDS-ready records (all parameters)", stats["total_rows"])
    kvf("Unique HYDAT stations retained",            len(stats["all_stations"]))
    kvf("Number of parameter files retained",        len(stats["params_found"]))

    # Count viable parameter files (>0 records)
    viable_params = [p for p in stats["params_found"] if stats["param_rows"].get(p, 0) > 0]
    kvf("Parameters with at least 1 valid station",  len(viable_params))

    sub("Per-parameter breakdown — viable parameters only")
    print(f"    {'Parameter':<50} {'Rows':>10}   {'Stations':>9}   {'Date range'}")
    print(f"    {'-'*50} {'-'*10}   {'-'*9}   {'-'*22}")
    for p in sorted(viable_params):
        n_rows = stats["param_rows"].get(p, 0)
        n_stns = len(stats["param_stations"].get(p, set()))
        d_range = stats["param_date_range"].get(p, ("N/A", "N/A"))
        print(f"    {p:<50} {n_rows:>10,}   {n_stns:>9,}   {d_range[0]}  –  {d_range[1]}")

    sub("Station listing (unique HYDAT IDs in final dataset)")
    all_stns = sorted(stats["all_stations"])
    print(f"    Total: {len(all_stns)}")
    line = ""
    for s in all_stns:
        candidate = (line + "  " + s).strip()
        if len(candidate) > 72:
            print(f"    {line}")
            line = s
        else:
            line = candidate
    if line:
        print(f"    {line}")

    sub("Station × parameter matrix  (which parameters are viable per station)")
    stn_params: dict[str, list] = defaultdict(list)
    for p in viable_params:
        for stn in stats["param_stations"].get(p, set()):
            stn_params[stn].append(p)

    print(f"    {'Station':<14} {'#Params':>7}   Parameters")
    print(f"    {'-'*14} {'-'*7}   {'-'*48}")
    for stn in sorted(stn_params.keys()):
        plist = sorted(stn_params[stn])
        # Truncate display if many
        pstr = ", ".join(plist[:6])
        if len(plist) > 6:
            pstr += f", … (+{len(plist)-6} more)"
        print(f"    {stn:<14} {len(plist):>7}   {pstr}")

    sub("Split-by-station files")
    split_dir = P11_DIR / "split_by_station"
    if split_dir.exists():
        split_csvs = list(split_dir.glob("*.csv"))
        kvf("Split-by-station CSV files generated", len(split_csvs))
        # Count unique stations from filenames
        stns_from_filenames = set()
        for f in split_csvs:
            # filename pattern: <param>_<StationNum>.csv
            parts = f.stem.rsplit("_", 1)
            if len(parts) == 2:
                stns_from_filenames.add(parts[1])
        kvf("Unique stations in split-by-station directory", len(stns_from_filenames))
    else:
        print("    (split_by_station subdirectory not found)")

    section_done(t0)


# ─────────────────────────────────────────────────────────────────────────────
#  SECTION FINAL  —  END-TO-END FUNNEL SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

def report_funnel_summary():
    hdr("PIPELINE FUNNEL SUMMARY  |  Records at Each Documented Step")
    print()
    rows = [
        ("Step 0",  "Raw DataStream API download (2000–2025)",           "35,230,617",  "N/A"),
        ("Step 1",  "Clipped to Canadian terrestrial boundary",           "35,202,575",  "−27,042"),
        ("Steps 2–4","CRS harmonisation → WGS84 (no record loss)",        "35,202,575",  "0"),
        ("Steps 5–6","CLRH catchment & stream attribute join",             "35,202,575",  "0"),
        ("Step 7",  "Spatial pairing with HYDAT WSC POI",                 "4,709,305+",  "−30.5 M+"),
        ("Step 9",  "RHBN filter (200 stations / 298 DS locations)",       "subset",      "–"),
        ("Step 10", "Parameter filter: 15 WQ parameters selected",         "per-param",   "–"),
        ("Step 11", "Temporal QA/QC (WRTDS-ready)",                       "~39,690",     "–"),
    ]
    print(f"  {'Step':<12} {'Description':<48} {'Records':>12}  {'Change':>10}")
    print(f"  {'-'*12} {'-'*48} {'-'*12}  {'-'*10}")
    for step, desc, recs, change in rows:
        print(f"  {step:<12} {desc:<48} {recs:>12}  {change:>10}")

    print()
    print("  ┌─────────────────────────────────────────────────────────────────────────┐")
    print("  │  Spatial pairing retained ~13.4 % of all records,                      │")
    print("  │  linking them to 860 unique HYDAT gauges and                            │")
    print("  │  1,392 unique DataStream monitoring locations.                          │")
    print("  │                                                                         │")
    print("  │  Subsequent RHBN + temporal QA/QC further narrows the pool to          │")
    print("  │  ~17 RHBN stations across 13 water-quality parameters,                 │")
    print("  │  providing robust multi-decadal records suitable for WRTDS             │")
    print("  │  load estimation.                                                       │")
    print("  └─────────────────────────────────────────────────────────────────────────┘")


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    overall_start = time.time()

    print()
    print(SEP_MAJOR)
    print("  CLAWAVE-1  |  DataStream → WRTDS  |  Pipeline Diagnostic Report")
    print(f"  Generated: {pd.Timestamp.now().strftime('%Y-%m-%d  %H:%M:%S')}")
    print(SEP_MAJOR)

    # ── Run each section ──────────────────────────────────────────────────────
    try:
        report_step0()
    except Exception as e:
        print(f"  [ERROR] Step 0 failed: {e}")

    try:
        report_step1()
    except Exception as e:
        print(f"  [ERROR] Step 1 failed: {e}")

    report_step2_3()

    try:
        report_step4()
    except Exception as e:
        print(f"  [ERROR] Step 4 failed: {e}")

    report_step5_6()

    try:
        report_step7_poi()
    except Exception as e:
        print(f"  [ERROR] POI section failed: {e}")

    try:
        report_step7_matched()
    except Exception as e:
        print(f"  [ERROR] Step 7a failed: {e}")

    try:
        report_step7_matchups()
    except Exception as e:
        print(f"  [ERROR] Step 7b failed: {e}")

    try:
        report_step9()
    except Exception as e:
        print(f"  [ERROR] Step 9 failed: {e}")

    try:
        report_step10()
    except Exception as e:
        print(f"  [ERROR] Step 10 failed: {e}")

    try:
        report_step11()
    except Exception as e:
        print(f"  [ERROR] Step 11 failed: {e}")

    report_funnel_summary()

    total_elapsed = time.time() - overall_start
    print()
    print(SEP_MAJOR)
    print(f"  Report complete.  Total runtime: {total_elapsed:.1f} s")
    print(SEP_MAJOR)
    print()


if __name__ == "__main__":
    main()