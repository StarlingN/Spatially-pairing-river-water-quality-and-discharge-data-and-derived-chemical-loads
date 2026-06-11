# -*- coding: utf-8 -*-
"""
Batch clean & join:
- Keep only 'datastream_' columns (excluding datastream_catchment_*, datastream_stream_*).
- Strip the 'datastream_' prefix from column names.
- Join StationNum and rhbn_listed from the RHBN lookup on:
    MonitoringLocationLatitude, MonitoringLocationLongitude,
    MonitoringLocationHorizontalCoordinateReferenceSystem
- Save outputs with original filenames into the target folder.
"""

from pathlib import Path
from tqdm import tqdm
import pandas as pd
import numpy as np

# -------------------- Config --------------------
INPUT_DIR  = Path(r"E:\publications\clawave_1\data\datastream\process_8_filtered_to_parameters")
OUTPUT_DIR = Path(r"E:\publications\clawave_1\data\datastream\process_10_cleaned")
LOOKUP_CSV = Path(r"E:\publications\clawave_1\data\datastream\process_9_added_rhbn\canada_datastream_hydat_matchups+RHBN.csv")

# Round lat/long on both sides to avoid tiny float mismatches (set to None to disable)
ROUND_DECIMALS = 6

# The three join keys (after renaming/normalization)
KEY_LAT  = "MonitoringLocationLatitude"
KEY_LON  = "MonitoringLocationLongitude"
KEY_CRS  = "MonitoringLocationHorizontalCoordinateReferenceSystem"
JOIN_KEYS = [KEY_LAT, KEY_LON, KEY_CRS]

# Columns to carry over from the lookup
LOOKUP_COLS_NEEDED = ["StationNum", "rhbn_listed"]

# ------------------------------------------------

def normalize_key_columns(df, prefix="datastream_"):
    """
    Ensure the dataframe has the three join keys with the exact names:
    MonitoringLocationLatitude, MonitoringLocationLongitude,
    MonitoringLocationHorizontalCoordinateReferenceSystem

    The input may have them either as 'datastream_<Name>' or already without prefix.
    We create/overwrite the non-prefixed names for consistency.
    """
    # Map from non-prefixed to possible prefixed names
    candidates = {
        KEY_LAT: f"{prefix}MonitoringLocationLatitude",
        KEY_LON: f"{prefix}MonitoringLocationLongitude",
        KEY_CRS: f"{prefix}MonitoringLocationHorizontalCoordinateReferenceSystem",
    }

    for base, pref in candidates.items():
        if base in df.columns:
            # already present, leave as is
            continue
        elif pref in df.columns:
            df[base] = df[pref]
        else:
            raise KeyError(f"Expected key column not found: '{base}' or '{pref}'")

    # Type cleanup: lat/lon numeric, CRS string & stripped
    df[KEY_LAT] = pd.to_numeric(df[KEY_LAT], errors="coerce")
    df[KEY_LON] = pd.to_numeric(df[KEY_LON], errors="coerce")
    df[KEY_CRS] = df[KEY_CRS].astype("string").str.strip()

    if ROUND_DECIMALS is not None:
        df[KEY_LAT] = df[KEY_LAT].round(ROUND_DECIMALS)
        df[KEY_LON] = df[KEY_LON].round(ROUND_DECIMALS)

    return df


def build_lookup(lookup_csv: Path) -> pd.DataFrame:
    """
    Load the RHBN lookup, normalize key columns, and return a deduplicated mapping
    with columns: JOIN_KEYS + LOOKUP_COLS_NEEDED
    """
    if not lookup_csv.exists():
        raise FileNotFoundError(f"Lookup CSV not found: {lookup_csv}")

    # Read; keep memory-friendly defaults
    lu = pd.read_csv(lookup_csv, dtype={"StationNum": "string"}, low_memory=False)

    # Normalize keys (handles both prefixed and non-prefixed cases)
    lu = normalize_key_columns(lu)

    # Ensure expected lookup columns exist
    for col in LOOKUP_COLS_NEEDED:
        if col not in lu.columns:
            raise KeyError(f"'{col}' column not found in lookup: {lookup_csv}")

    # Prefer rows with rhbn_listed == 1 if duplicates exist on the same key
    lu["__sort_pref__"] = lu["rhbn_listed"].fillna(0).astype(int)

    # Keep only needed columns
    lu = lu[JOIN_KEYS + LOOKUP_COLS_NEEDED + ["__sort_pref__"]]

    # Deduplicate by keys, keeping the row where rhbn_listed is 1 if present
    lu = lu.sort_values("__sort_pref__", ascending=False)
    lu = lu.drop_duplicates(subset=JOIN_KEYS, keep="first").drop(columns="__sort_pref__")

    # Clean final types
    lu["rhbn_listed"] = lu["rhbn_listed"].fillna(0).astype(int)
    # Keep StationNum as string; missing remains <NA>
    lu["StationNum"] = lu["StationNum"].astype("string")

    return lu


def process_one_csv(in_path: Path, lookup_df: pd.DataFrame) -> Path:
    """
    Process a single input CSV:
    - Keep only datastream_* (excluding datastream_catchment_*, datastream_stream_*)
    - Strip 'datastream_' prefix
    - Normalize join keys
    - Left-merge StationNum & rhbn_listed
    - Save to OUTPUT_DIR with same filename
    """
    df = pd.read_csv(in_path, low_memory=False)

    # Keep only 'datastream_' columns, excluding catchment & stream blocks
    keep_cols = [
        c for c in df.columns
        if c.startswith("datastream_")
        and not c.startswith("datastream_catchment_")
        and not c.startswith("datastream_stream_")
    ]

    if not keep_cols:
        raise ValueError(f"No 'datastream_' columns found in {in_path.name}")

    df = df[keep_cols].copy()

    # Rename: strip the 'datastream_' prefix
    rename_map = {c: c.replace("datastream_", "", 1) for c in df.columns}
    df = df.rename(columns=rename_map)

    # Normalize keys (now columns should be the unprefixed names)
    df = normalize_key_columns(df, prefix="datastream_")  # prefix not used here but harmless

    # Merge
    merged = df.merge(lookup_df, how="left", on=JOIN_KEYS, validate="m:1")

    # For rows with no match, set rhbn_listed=0 (explicitly)
    merged["rhbn_listed"] = merged["rhbn_listed"].fillna(0).astype(int)
    # StationNum left as string; will be <NA> if not matched

    # Ensure output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / in_path.name
    merged.to_csv(out_path, index=False)
    return out_path


def main():
    lookup_df = build_lookup(LOOKUP_CSV)

    csv_files = sorted(INPUT_DIR.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {INPUT_DIR}")

    successes, failures = 0, 0
    for csv_path in tqdm(csv_files, desc="Processing CSV files", unit="file"):
        try:
            out_path = process_one_csv(csv_path, lookup_df)
            successes += 1
        except Exception as e:
            failures += 1
            print(f"[ERROR] {csv_path.name}: {e}")

    print(f"\nDone. Success: {successes}, Failed: {failures}")
    print(f"Outputs written to: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
