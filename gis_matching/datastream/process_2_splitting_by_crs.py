# split_by_crs.py
# Streams a large CSV and writes four files by CRS category (NAD27/NAD83/UNKWN/WGS84)
# without altering any other fields.
#
# Usage: python split_by_crs.py

import os
import pandas as pd
from tqdm import tqdm

IN_PATH = r"E:\publications\clawave_1\data\datastream\process_1_clipped\observations_2000_2025_all_clipped.csv"

OUT_PATHS = {
    "NAD27": r"E:\publications\clawave_1\data\datastream\process_2_split_by_crs\observations_2000_2025_all_clipped_NAD27.csv",
    "NAD83": r"E:\publications\clawave_1\data\datastream\process_2_split_by_crs\observations_2000_2025_all_clipped_NAD83.csv",
    "UNKWN": r"E:\publications\clawave_1\data\datastream\process_2_split_by_crs\observations_2000_2025_all_clipped_UNKWN.csv",
    "WGS84": r"E:\publications\clawave_1\data\datastream\process_2_split_by_crs\observations_2000_2025_all_clipped_WGS84.csv",
}

CRS_COL = "MonitoringLocationHorizontalCoordinateReferenceSystem"
CHUNK_ROWS = 200_000  # increase for more speed if you have RAM headroom

def main():
    if not os.path.exists(IN_PATH):
        raise FileNotFoundError(f"CSV not found: {IN_PATH}")

    # Remove existing outputs to avoid duplicate headers/appends from prior runs
    for p in OUT_PATHS.values():
        if os.path.exists(p):
            os.remove(p)

    wrote_header = {k: False for k in OUT_PATHS}
    counts = {k: 0 for k in OUT_PATHS}
    other_count = 0

    total_size = os.path.getsize(IN_PATH)

    # Open input handle so tqdm can track bytes read via f.tell()
    with open(IN_PATH, "rb") as f_in, tqdm(
        total=total_size, unit="B", unit_scale=True, desc="Splitting by CRS"
    ) as pbar:

        # Read ALL columns as strings to preserve them exactly (no NA coercion / formatting changes)
        reader = pd.read_csv(
            f_in,
            chunksize=CHUNK_ROWS,
            dtype=str,
            engine="c",
            on_bad_lines="skip",
            keep_default_na=False,
            na_filter=False,
        )

        for chunk in reader:
            if CRS_COL not in chunk.columns:
                raise KeyError(f"Required column not found: {CRS_COL}")

            # Normalize only for comparison (preserve original values when writing)
            crs_norm = chunk[CRS_COL].astype(str).str.strip()

            # Write each desired group
            for crs_value, out_path in OUT_PATHS.items():
                mask = crs_norm.eq(crs_value)
                if mask.any():
                    part = chunk.loc[mask]
                    part.to_csv(
                        out_path,
                        mode="a" if wrote_header[crs_value] else "w",
                        index=False,
                        header=not wrote_header[crs_value],
                        lineterminator="\n",
                    )
                    wrote_header[crs_value] = True
                    counts[crs_value] += int(mask.sum())

            # Count any unexpected values (should be zero given your note)
            other_count += int(~crs_norm.isin(OUT_PATHS.keys()).sum())

            # Update progress bar based on file bytes consumed
            pbar.update(max(0, f_in.tell() - pbar.n))

    print("\nDone.")
    for k, p in OUT_PATHS.items():
        print(f"{k:>6}: {counts[k]:,} rows -> {p}")
    if other_count:
        print(f"OTHER: {other_count:,} rows encountered with unexpected CRS (not written)")

if __name__ == "__main__":
    main()
