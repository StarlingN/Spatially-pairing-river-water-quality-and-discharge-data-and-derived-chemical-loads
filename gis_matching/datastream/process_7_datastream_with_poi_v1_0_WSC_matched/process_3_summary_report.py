#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Comprehensive column report (chunked) for:
r"E:\publications\clawave_1\data\datastream\process_7_datastream_with_poi_v1_0_WSC_matched\observations_2000_2025_all_clipped_unified_crs_renamed+catchments+streams_matchups_fixed.csv"

What it does:
1) Prints UNIQUE values for:
   - datastream_MonitoringLocationType
   - datastream_ActivityMediaName
   - poi_v1_0_WSC_Obs_NM

2) Prints MIN/MAX for:
   - datastream_ActivityStartDate (as date)
   - datastream_ActivityEndDate   (as date)
   - datastream_stream_distance   (numeric)
   - poi_v1_0_WSC_stream_distance (numeric)

3) Writes value-counts table for:
   - datastream_CharacteristicName
   to "<same_folder>/datastream_CharacteristicName_value_counts.csv"

4) Prints the count of UNIQUE (lat, lon) pairs:
   - datastream_wgs84_lat
   - datastream_wgs84_long
   (using numeric comparison rounded to 6 decimals)
"""

import os
import sys
import argparse
import textwrap
from collections import Counter

import pandas as pd
import numpy as np


# ---------------------- CONFIG ----------------------
# Adjust if needed
CHUNKSIZE = 100_000
COORD_ROUND_DECIMALS = 6
# Set to None to print ALL unique values; or an integer to limit console output.
UNIQUE_PRINT_LIMIT = None  # e.g., 200
# ---------------------------------------------------


def print_header(title: str):
    line = "=" * 80
    sub = "-" * 80
    print(f"\n{line}\n{title}\n{sub}")


def print_subheader(title: str):
    print(f"\n{title}\n" + "-" * len(title))


def clean_str_values(values):
    """Drop None/empty/placeholder strings and return a sorted list of cleaned strings."""
    cleaned = []
    for v in values:
        if v is None:
            continue
        s = str(v).strip()
        if s == "" or s.lower() in {"nan", "none", "null"}:
            continue
        cleaned.append(s)
    # Natural-ish sort (case-insensitive)
    return sorted(cleaned, key=lambda x: (x.lower(), x))


def summarize_unique(name, values, limit=UNIQUE_PRINT_LIMIT, width=100):
    vals = clean_str_values(values)
    count = len(vals)
    print_subheader(f'Unique values for "{name}"  (count: {count:,})')
    if count == 0:
        print("No non-empty values found.")
        return
    to_show = vals if (limit is None or limit >= count) else vals[:limit]
    block = ", ".join(to_show)
    print(textwrap.fill(block, width=width))
    if limit is not None and count > limit:
        print(f"... [{count - limit:,} more not shown]")


def main(csv_path: str):
    # Columns by task
    unique_cols = [
        "datastream_MonitoringLocationType",
        "datastream_ActivityMediaName",
        "poi_v1_0_WSC_Obs_NM",
    ]

    date_range_cols = [
        "datastream_ActivityStartDate",
        "datastream_ActivityEndDate",
    ]

    numeric_range_cols = [
        "datastream_stream_distance",
        "poi_v1_0_WSC_stream_distance",
    ]

    value_counts_col = "datastream_CharacteristicName"

    lat_col = "datastream_wgs84_lat"
    lon_col = "datastream_wgs84_long"

    # Deduplicate + union of all needed columns
    needed_cols = sorted(
        set(unique_cols + date_range_cols + numeric_range_cols + [value_counts_col, lat_col, lon_col])
    )

    # Initialize aggregators
    uniques = {c: set() for c in unique_cols}
    date_min = {c: None for c in date_range_cols}
    date_max = {c: None for c in date_range_cols}
    num_min = {c: np.inf for c in numeric_range_cols}
    num_max = {c: -np.inf for c in numeric_range_cols}
    vc_counter = Counter()
    unique_coords = set()
    total_rows = 0

    # Prepare output path for temp CSV
    folder = os.path.dirname(os.path.abspath(csv_path))
    vc_out = os.path.join(folder, "datastream_CharacteristicName_value_counts.csv")

    # Validate columns exist (quick read of header)
    try:
        header_only = pd.read_csv(csv_path, nrows=0)
        existing_cols = set(header_only.columns)
    except Exception as e:
        print(f"ERROR: Unable to read CSV header.\n{e}")
        sys.exit(1)

    missing = [c for c in needed_cols if c not in existing_cols]
    if missing:
        print_header("Column Check")
        print("The following required columns are MISSING in the CSV:")
        for c in missing:
            print(f" - {c}")
        print("\nPlease verify the file and column names.")
        sys.exit(1)

    print_header("Starting Report")
    print(f"File: {csv_path}")
    print(f"Output (value-counts CSV): {vc_out}")
    print(f"Chunksize: {CHUNKSIZE:,}")
    print(f"Only reading {len(needed_cols)} columns out of the full table to save memory.")
    print("Working...")

    # Stream through the file in chunks (read selected columns as strings; convert per-need)
    try:
        for chunk in pd.read_csv(
            csv_path,
            usecols=needed_cols,
            chunksize=CHUNKSIZE,
            dtype="string",         # keep raw text; convert precisely as needed
            low_memory=True,
        ):
            # Track row count
            total_rows += len(chunk)

            # 1) Unique values (string-based)
            for c in unique_cols:
                # set update with raw, drop NAs later in printing
                uniques[c].update(chunk[c].dropna().tolist())

            # 2) Date ranges
            for c in date_range_cols:
                s = pd.to_datetime(chunk[c], errors="coerce", utc=False)
                if not s.empty:
                    cmin = s.min(skipna=True)
                    cmax = s.max(skipna=True)
                    if pd.notna(cmin):
                        date_min[c] = cmin if date_min[c] is None or cmin < date_min[c] else date_min[c]
                    if pd.notna(cmax):
                        date_max[c] = cmax if date_max[c] is None or cmax > date_max[c] else date_max[c]

            # 2) Numeric ranges
            for c in numeric_range_cols:
                s = pd.to_numeric(chunk[c], errors="coerce")
                if s.notna().any():
                    cmin = s.min(skipna=True)
                    cmax = s.max(skipna=True)
                    if pd.notna(cmin) and cmin < num_min[c]:
                        num_min[c] = float(cmin)
                    if pd.notna(cmax) and cmax > num_max[c]:
                        num_max[c] = float(cmax)

            # 3) Value counts (string-based)
            vc_series = chunk[value_counts_col].dropna()
            if not vc_series.empty:
                vc_counter.update(vc_series.value_counts(dropna=False).to_dict())

            # 4) Unique lat/lon pairs (numeric-based, rounded)
            lat = pd.to_numeric(chunk[lat_col], errors="coerce")
            lon = pd.to_numeric(chunk[lon_col], errors="coerce")
            mask = lat.notna() & lon.notna()
            if mask.any():
                lat_r = lat[mask].round(COORD_ROUND_DECIMALS)
                lon_r = lon[mask].round(COORD_ROUND_DECIMALS)
                unique_coords.update(zip(lat_r.to_numpy(), lon_r.to_numpy()))

    except KeyboardInterrupt:
        print("\nInterrupted by user. Printing partial results gathered so far...\n")
    except Exception as e:
        print(f"\nERROR while processing:\n{e}\nPrinting partial results gathered so far...\n")

    # ---------------------- PRINT REPORT ----------------------
    print_header("Report")

    # Basic stats
    print_subheader("Overview")
    print(f"Rows processed: {total_rows:,}")
    print(f"Columns analyzed: {len(needed_cols)}")
    print(f"Temp value-counts CSV (instruction 3):\n  {vc_out}")

    # 1) Unique values
    for c in unique_cols:
        summarize_unique(c, uniques[c])

    # 2) Ranges
    print_subheader("Ranges (Min / Max)")
    # Dates
    for c in date_range_cols:
        dmin = date_min[c]
        dmax = date_max[c]
        dmin_s = dmin.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(dmin) and dmin is not None else "N/A"
        dmax_s = dmax.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(dmax) and dmax is not None else "N/A"
        print(f'- {c}:')
        print(f'    min: {dmin_s}')
        print(f'    max: {dmax_s}')

    # Numerics
    for c in numeric_range_cols:
        vmin = None if (np.isinf(num_min[c]) or pd.isna(num_min[c])) else num_min[c]
        vmax = None if (np.isinf(num_max[c]) or pd.isna(num_max[c])) else num_max[c]
        vmin_s = f"{vmin:.6f}" if vmin is not None else "N/A"
        vmax_s = f"{vmax:.6f}" if vmax is not None else "N/A"
        print(f'- {c}:')
        print(f'    min: {vmin_s}')
        print(f'    max: {vmax_s}')

    # 3) Save value-counts CSV
    print_subheader(f'Value Counts Table → "{os.path.basename(vc_out)}"')
    if vc_counter:
        vc_df = (
            pd.DataFrame(
                {
                    value_counts_col: list(vc_counter.keys()),
                    "count": list(vc_counter.values()),
                }
            )
            .sort_values("count", ascending=False)
            .reset_index(drop=True)
        )
        try:
            vc_df.to_csv(vc_out, index=False, encoding="utf-8")
            print(f"Saved {len(vc_df):,} rows to:\n  {vc_out}")
        except Exception as e:
            print(f"ERROR saving value-counts CSV:\n{e}")
    else:
        print("No values found to save.")

    # 4) Unique coordinate pairs
    print_subheader("Unique Locations (lat, lon)")
    print(f"Rounding used: {COORD_ROUND_DECIMALS} decimals")
    print(f"Unique (lat, lon) pairs: {len(unique_coords):,}")

    print("\nDone.\n")


if __name__ == "__main__":
    default_path = r"E:\publications\clawave_1\data\datastream\process_7_datastream_with_poi_v1_0_WSC_matched\observations_2000_2025_all_clipped_unified_crs_renamed+catchments+streams_matchups_fixed.csv"
    parser = argparse.ArgumentParser(description="Print a comprehensive report on selected columns from a large CSV.")
    parser.add_argument("csv_path", nargs="?", default=default_path, help="Path to the CSV file.")
    args = parser.parse_args()
    main(args.csv_path)
