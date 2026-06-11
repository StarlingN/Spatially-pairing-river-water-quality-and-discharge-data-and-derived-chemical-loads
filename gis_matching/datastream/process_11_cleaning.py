# -*- coding: utf-8 -*-
"""
Clean CSVs for WRTDS-ready periods (grouping by StationNum) with easy configuration,
and also export per-station split CSVs.

Reads each CSV from:
  E:\publications\clawave_1\data\datastream\process_10_cleaned

Writes filtered CSVs (same filenames & column order) to:
  E:\publications\clawave_1\data\datastream\process_11_cleaned

Additionally writes station-split CSVs to:
  E:\publications\clawave_1\data\datastream\process_11_cleaned\split_by_station
  using the original filename with "_<StationNum>" appended before the .csv suffix.

Pipeline per file:
  0) DROP rows with blank ResultValue ('' or NA)  <-- done first
  1) [Optional] Drop rows with rhbn_listed == 0  (configurable)
  2) Group by StationNum
  3) Keep only rows inside VALID PERIODS determined by:
       - VALID YEAR: >= MIN_MEASUREMENTS_PER_YEAR measurements AND
                     >= MIN_CONSECUTIVE_MONTHS_IN_YEAR consecutive months with data
                     (set either threshold to 0 to disable that rule)
       - VALID PERIOD: runs of >= MIN_CONSECUTIVE_VALID_YEARS_IN_PERIOD consecutive valid years
         (1 -> allow single-year periods; 0 -> accept all valid years)
  4) Sort by StationNum asc, ActivityStartDate asc
  5) If final result is empty, DO NOT write any outputs
  6) If not empty, write:
       - main combined CSV to OUTPUT_DIR
       - per-station CSVs to OUTPUT_DIR / "split_by_station", one file per StationNum

Notes:
- StationNum is treated as the unique-location key.
- Unparseable dates or missing StationNum rows are dropped (cannot evaluate).
- Original columns are preserved; helper columns are removed before saving.
"""

from pathlib import Path
from tqdm import tqdm
import pandas as pd
import numpy as np
import re

# ======================= CONFIGURATION (EDIT HERE) =======================
INPUT_DIR  = Path(r"E:\publications\clawave_1\data\datastream\process_10_cleaned")
OUTPUT_DIR = Path(r"E:\publications\clawave_1\data\datastream\process_11_cleaned")

# Toggle: drop rows where rhbn_listed == 0
DROP_RHBN_ZERO = True   # set False to keep rows regardless of RHBN flag

# Thresholds for "valid year" and "valid period"
MIN_MEASUREMENTS_PER_YEAR = 10      # set 0 to disable count requirement
MIN_CONSECUTIVE_MONTHS_IN_YEAR = 8  # set 0 to disable consecutive-months requirement
MIN_CONSECUTIVE_VALID_YEARS_IN_PERIOD = 4  # 1 -> single-year periods allowed; 0 -> accept all valid years

# Export per-station split files as well?
SPLIT_BY_STATION = True
SPLIT_SUBDIR_NAME = "split_by_station"  # subfolder under OUTPUT_DIR

# Progress bars
SHOW_PROGRESS = True
# ===================== END CONFIGURATION (EDIT HERE) =====================

# Column names
COL_DATE = "ActivityStartDate"
COL_STN  = "StationNum"
COL_RHBN = "rhbn_listed"
COL_VAL  = "ResultValue"

def _assert_required_columns(df, file_name):
    needed = {COL_DATE, COL_STN, COL_VAL}
    if DROP_RHBN_ZERO:
        needed.add(COL_RHBN)
    missing = needed.difference(df.columns)
    if missing:
        raise KeyError(f"{file_name}: Missing required columns: {sorted(missing)}")

def _longest_consecutive_run(sorted_ints):
    """Return the longest run length of consecutive integers in a sorted list (duplicates allowed)."""
    if not sorted_ints:
        return 0
    longest = 1
    current = 1
    for i in range(1, len(sorted_ints)):
        if sorted_ints[i] == sorted_ints[i-1] + 1:
            current += 1
            if current > longest:
                longest = current
        elif sorted_ints[i] == sorted_ints[i-1]:
            continue
        else:
            current = 1
    return longest

def _consecutive_year_runs(sorted_years):
    """Given sorted distinct years, return [(start,end), ...] of consecutive runs."""
    if not sorted_years:
        return []
    runs = []
    start = prev = sorted_years[0]
    for y in sorted_years[1:]:
        if y == prev + 1:
            prev = y
        else:
            runs.append((start, prev))
            start = prev = y
    runs.append((start, prev))
    return runs

def _year_is_valid(grp, yr, years, months, min_meas, min_consec_months):
    """Check if a given year satisfies configured validity rules."""
    mask_y = (years == yr)
    n_records = int(mask_y.sum())

    if min_meas > 0 and n_records < min_meas:
        return False

    if min_consec_months > 0:
        months_present = sorted(set(months[mask_y].dropna().astype(int).tolist()))
        if not months_present:
            return False
        if _longest_consecutive_run(months_present) < min_consec_months:
            return False

    return True

def _valid_years_for_station(grp, dt_col="_dt"):
    """For one StationNum group, return set of valid years under configured thresholds."""
    years  = grp[dt_col].dt.year
    months = grp[dt_col].dt.month
    candidate_years = sorted(int(y) for y in years.dropna().unique())

    valid = set()
    for yr in candidate_years:
        if _year_is_valid(
            grp, yr, years, months,
            MIN_MEASUREMENTS_PER_YEAR,
            MIN_CONSECUTIVE_MONTHS_IN_YEAR
        ):
            valid.add(yr)
    return valid

def _periods_from_valid_years(valid_years):
    """
    Convert valid years into acceptable periods based on MIN_CONSECUTIVE_VALID_YEARS_IN_PERIOD:
      - >=2: runs of that many consecutive years or more.
      - ==1: each valid year is its own (start=end=year).
      - <=0: accept all valid years individually (same as 1).
    """
    if not valid_years:
        return []

    min_years = MIN_CONSECUTIVE_VALID_YEARS_IN_PERIOD
    yrs = sorted(valid_years)

    if min_years <= 1:
        return [(y, y) for y in yrs]

    runs = _consecutive_year_runs(yrs)
    return [(a, b) for (a, b) in runs if (b - a + 1) >= min_years]

def _mask_rows_in_periods(grp, dt_col, periods):
    """Boolean mask for rows whose year lies in any [start,end] period."""
    if not periods:
        return pd.Series(False, index=grp.index)
    years = grp[dt_col].dt.year
    mask = pd.Series(False, index=grp.index)
    for (a, b) in periods:
        mask |= years.between(a, b, inclusive="both")
    return mask

def _sanitize_for_filename(value: str) -> str:
    """
    Sanitize StationNum for safe filenames:
    - Keep letters, digits, underscore, dash
    - Replace other chars with '_'
    - Collapse repeated '_' and strip leading/trailing '_'
    """
    if value is None:
        return "NA"
    s = str(value)
    s = re.sub(r"[^A-Za-z0-9_\-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "NA"

def _write_split_by_station(df_final: pd.DataFrame, in_path: Path, out_dir: Path, original_cols):
    """
    Write per-station CSVs under OUTPUT_DIR / SPLIT_SUBDIR_NAME.
    Uses same column order as the combined output and the same base filename,
    with "_<StationNum>" appended before the extension.
    """
    split_dir = out_dir / SPLIT_SUBDIR_NAME
    split_dir.mkdir(parents=True, exist_ok=True)

    # Ensure we only write columns that exist (original order)
    cols = [c for c in original_cols if c in df_final.columns]

    # Iterate stations in appearance order (df is already sorted)
    for stn, grp in df_final.groupby(COL_STN, sort=False, dropna=False):
        stn_str = "" if pd.isna(stn) else str(stn)
        safe_stn = _sanitize_for_filename(stn_str)
        out_name = f"{in_path.stem}_{safe_stn}{in_path.suffix}"
        out_path = split_dir / out_name

        # grp is already ordered as in df_final, which is StationNum + date ascending
        grp[cols].to_csv(out_path, index=False)

def process_one_file(in_path: Path, out_dir: Path):
    """
    Returns:
        'written' if an output file was created (and per-station files if enabled),
        'skipped_empty' if the final result is empty (no file written).
    """
    # Read; preserve StationNum leading zeros
    df = pd.read_csv(in_path, dtype={COL_STN: "string"}, low_memory=False)

    # Remember original column order
    original_cols = list(df.columns)

    _assert_required_columns(df, in_path.name)

    # (0) Drop rows with blank ResultValue ('' or NA) — done FIRST
    rv_str = df[COL_VAL].astype("string")
    mask_has_value = rv_str.str.strip().notna() & (rv_str.str.strip() != "")
    df = df[mask_has_value].copy()
    if df.empty:
        return "skipped_empty"

    # (1) Optionally drop rows where rhbn_listed == 0
    if DROP_RHBN_ZERO:
        df[COL_RHBN] = pd.to_numeric(df[COL_RHBN], errors="coerce").fillna(0).astype(int)
        df = df[df[COL_RHBN] == 1].copy()
        if df.empty:
            return "skipped_empty"

    # Normalize StationNum and drop rows without a StationNum
    df[COL_STN] = df[COL_STN].astype("string").str.strip()
    df = df.dropna(subset=[COL_STN]).copy()
    if df.empty:
        return "skipped_empty"

    # Parse/validate date
    df["_dt"] = pd.to_datetime(df[COL_DATE], errors="coerce")
    df = df.dropna(subset=["_dt"]).copy()
    if df.empty:
        return "skipped_empty"

    # Build mask: keep rows belonging to any valid period per StationNum
    keep_mask = pd.Series(False, index=df.index)

    groups = df.groupby(COL_STN, dropna=False).groups
    iterator = groups.items()
    if SHOW_PROGRESS:
        iterator = tqdm(iterator, total=len(groups), desc=f"  by StationNum in {in_path.name}", unit="stn")

    for stn, idx in iterator:
        grp = df.loc[idx]

        valid_years = _valid_years_for_station(grp, dt_col="_dt")
        if not valid_years:
            continue

        periods = _periods_from_valid_years(valid_years)
        if not periods:
            continue

        keep_mask.loc[idx] = _mask_rows_in_periods(grp, "_dt", periods)

    df = df[keep_mask].copy()
    if df.empty:
        return "skipped_empty"

    # Sort: StationNum, then ActivityStartDate
    df["_station_sort"] = df[COL_STN].astype("string").fillna("")
    df = df.sort_values(by=["_station_sort", "_dt"], ascending=[True, True], kind="mergesort")

    # Drop helpers and restore exact original column order
    df_out = df.drop(columns=["_station_sort", "_dt"], errors="ignore")
    df_out = df_out[[c for c in original_cols if c in df_out.columns]]

    # Write main combined file
    out_dir.mkdir(parents=True, exist_ok=True)
    combined_path = out_dir / in_path.name
    df_out.to_csv(combined_path, index=False)

    # Write per-station split files (optional)
    if SPLIT_BY_STATION:
        _write_split_by_station(df_out, in_path, out_dir, original_cols)

    return "written"

def main():
    if not INPUT_DIR.exists():
        raise FileNotFoundError(f"Input directory not found: {INPUT_DIR}")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(INPUT_DIR.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {INPUT_DIR}")

    written = skipped = failed = 0

    outer_iter = files
    if SHOW_PROGRESS:
        outer_iter = tqdm(files, desc="Filtering valid periods (by StationNum)", unit="file")

    for f in outer_iter:
        try:
            status = process_one_file(f, OUTPUT_DIR)
            if status == "written":
                written += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"[ERROR] {f.name}: {e}")
            failed += 1

    print(f"\nDone. Written: {written}, Skipped (empty): {skipped}, Failed: {failed}")
    print(f"Combined outputs in: {OUTPUT_DIR}")
    if SPLIT_BY_STATION:
        print(f"Per-station outputs in: {OUTPUT_DIR / SPLIT_SUBDIR_NAME}")

if __name__ == "__main__":
    main()
