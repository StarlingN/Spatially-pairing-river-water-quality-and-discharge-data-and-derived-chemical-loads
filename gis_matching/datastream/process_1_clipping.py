# clip_by_bbox.py
# Filters a huge CSV by lat/lon range, preserving all other fields unchanged.
# Usage: python clip_by_bbox.py

import os
import math
import pandas as pd
from tqdm import tqdm

# --------- INPUTS ---------
IN_PATH  = r"E:\publications\clawave_1\data\datastream\process_0_downloaded\observations_2000_2025_all.csv"
OUT_PATH = r"E:\publications\clawave_1\data\datastream\process_1_clipped\observations_2000_2025_all_clipped.csv"

COL_LAT = "MonitoringLocationLatitude"
COL_LON = "MonitoringLocationLongitude"

LAT_MIN, LAT_MAX = 41.676569, 83.137545
LON_MIN, LON_MAX = -141.002002, -52.619458

# Tune for your RAM; smaller chunks reduce memory, larger chunks improve speed.
CHUNK_ROWS = 200_000

def main():
    if not os.path.exists(IN_PATH):
        raise FileNotFoundError(f"CSV not found: {IN_PATH}")

    # Remove old output if present
    if os.path.exists(OUT_PATH):
        os.remove(OUT_PATH)

    total_size = os.path.getsize(IN_PATH)
    wrote_header = False
    kept = dropped = 0

    # Open input handle so we can track bytes consumed for tqdm
    with open(IN_PATH, "rb") as f_in, tqdm(
        total=total_size, unit="B", unit_scale=True, desc="Clipping CSV"
    ) as pbar:

        # Read *all columns* as raw text to avoid changing anything on write.
        # keep_default_na=False + na_filter=False preserves literal text like "NA", "", etc.
        reader = pd.read_csv(
            f_in,
            chunksize=CHUNK_ROWS,
            dtype=str,
            engine="c",
            on_bad_lines="skip",
            keep_default_na=False,
            na_filter=False
        )

        for chunk in reader:
            # Ensure the needed columns exist
            if COL_LAT not in chunk.columns or COL_LON not in chunk.columns:
                raise KeyError(f"Required columns not found: {COL_LAT}, {COL_LON}")

            # Compute numeric mask (coerce invalids to NaN -> False in comparisons)
            lat = pd.to_numeric(chunk[COL_LAT], errors="coerce")
            lon = pd.to_numeric(chunk[COL_LON], errors="coerce")

            mask = (
                (lat >= LAT_MIN) & (lat <= LAT_MAX) &
                (lon >= LON_MIN) & (lon <= LON_MAX)
            )

            filtered = chunk[mask]

            kept += len(filtered)
            dropped += len(chunk) - len(filtered)

            # Write filtered rows *exactly as read* (strings preserved)
            if not filtered.empty:
                filtered.to_csv(
                    OUT_PATH,
                    mode="a" if wrote_header else "w",
                    index=False,
                    header=not wrote_header,
                    lineterminator="\n",
                )
                wrote_header = True

            # Update progress bar based on bytes consumed by pandas from the same file handle
            pbar.update(max(0, f_in.tell() - pbar.n))

    print("\nDone.")
    print(f"Kept rows   : {kept:,}")
    print(f"Dropped rows: {dropped:,}")
    print(f"Output file : {OUT_PATH}")

if __name__ == "__main__":
    main()
