# concat_clipped_updated.py
# Concatenate four CSVs with identical schema into one CSV, keeping only ONE header.
# Force "wgs84_lat" and "wgs84_long" to be plain numbers with EXACTLY 4 decimal places.
# Streams in chunks for large files and shows a tqdm progress bar.
# Usage: python concat_clipped_updated.py

import os
import re
import csv  # <-- added for writing a custom header row
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from tqdm import tqdm

INPUTS = [
    r"E:\publications\clawave_1\data\datastream\process_3_arcgis_unified_coordinates\observations_2000_2025_all_clipped_NAD27_updated.csv",
    r"E:\publications\clawave_1\data\datastream\process_3_arcgis_unified_coordinates\observations_2000_2025_all_clipped_NAD83_updated.csv",
    r"E:\publications\clawave_1\data\datastream\process_3_arcgis_unified_coordinates\observations_2000_2025_all_clipped_WGS84_updated.csv",
    r"E:\publications\clawave_1\data\datastream\process_3_arcgis_unified_coordinates\observations_2000_2025_all_clipped_UNKWN_updated.csv",
]

OUTPUT = r"E:\publications\clawave_1\data\datastream\process_4_merged_unified_crs+renamed\observations_2000_2025_all_clipped_unified_crs_renamed.csv"

WGS_LAT_COL = "wgs84_lat"
WGS_LON_COL = "wgs84_long"

# Prefix to add to output header names
HEADER_PREFIX = "datastream_"

# Tune for your RAM; larger -> faster
CHUNK_ROWS = 200_000

# Regex that matches simple decimal numbers (with optional sign and decimals)
NUM_RE = re.compile(r"^[\+\-]?(?:\d+(?:\.\d*)?|\.\d+)$")

def _header_text(path: str) -> str:
    with open(path, "rb") as f:
        hb = f.readline()
    return hb.decode("utf-8-sig", errors="replace").rstrip("\r\n")

def _format_fixed4_series(s: pd.Series) -> pd.Series:
    """
    Return a string series with EXACTLY 4 decimals for any numeric-looking value.
    Non-numeric entries are left unchanged.
    No apostrophes, no extra characters.
    """
    s = s.astype(str)

    # First pass: try fast numeric conversion
    stripped = s.str.strip()
    num = pd.to_numeric(stripped, errors="coerce")
    is_num = num.notna()

    # Handle tiny negative zeros -> 0.0000
    num.loc[is_num & (num.abs() < 0.00005)] = 0.0

    # Format all recognized numerics to exactly 4 decimals
    s.loc[is_num] = num.loc[is_num].map(lambda x: f"{x:.4f}")

    # Second pass: regex fallback
    remaining = ~is_num
    looks_numeric = stripped.str.match(NUM_RE, na=False) & remaining

    if looks_numeric.any():
        def _fmt(v: str) -> str:
            txt = v.strip()
            try:
                d = Decimal(txt)
                if abs(d) < Decimal("0.00005"):
                    d = Decimal("0")
                d = d.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                return f"{d:.4f}"
            except Exception:
                return v

        s.loc[looks_numeric] = s.loc[looks_numeric].map(_fmt)

    return s

def main():
    # Basic checks
    for p in INPUTS:
        if not os.path.exists(p):
            raise FileNotFoundError(f"Missing input file: {p}")

    # Verify headers match across all inputs
    ref_header = _header_text(INPUTS[0])
    for p in INPUTS[1:]:
        if _header_text(p) != ref_header:
            raise ValueError(f"Header mismatch detected in: {p}")

    # Prepare output
    if os.path.exists(OUTPUT):
        os.remove(OUTPUT)
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)

    total_bytes = sum(os.path.getsize(p) for p in INPUTS)
    wrote_header = False
    columns = None  # captured from the first chunk of the first file
    prefixed_columns = None  # computed once from `columns`

    with tqdm(total=total_bytes, unit="B", unit_scale=True, desc="Concatenating & formatting") as pbar:
        for i, src in enumerate(INPUTS):
            with open(src, "rb") as f_in:
                # progress tracking per file
                last_pos = f_in.tell()

                # For files after the first, skip header line manually
                if i > 0:
                    skipped = f_in.readline()
                    pbar.update(len(skipped))
                    last_pos = f_in.tell()
                    reader = pd.read_csv(
                        f_in,
                        chunksize=CHUNK_ROWS,
                        dtype=str,
                        engine="c",
                        on_bad_lines="skip",
                        keep_default_na=False,
                        na_filter=False,
                        header=None,
                        names=columns,  # set after first chunk
                    )
                else:
                    # First file: let pandas read header
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
                    # Capture column order once
                    if columns is None:
                        columns = list(chunk.columns)
                        if WGS_LAT_COL not in columns or WGS_LON_COL not in columns:
                            raise KeyError(
                                f"Required columns not found: '{WGS_LAT_COL}', '{WGS_LON_COL}'"
                            )
                        # Build once: header to be written with prefix added to every column name
                        prefixed_columns = [HEADER_PREFIX + c for c in columns]

                        # Write the single (prefixed) header row now
                        if not wrote_header:
                            with open(OUTPUT, "a", newline="", encoding="utf-8") as f_out:
                                writer = csv.writer(f_out, lineterminator="\n")
                                writer.writerow(prefixed_columns)
                            wrote_header = True

                    # Enforce EXACT 4-decimal formatting for lat/lon
                    chunk[WGS_LAT_COL] = _format_fixed4_series(chunk[WGS_LAT_COL])
                    chunk[WGS_LON_COL] = _format_fixed4_series(chunk[WGS_LON_COL])

                    # Write out data rows only (no header; we already wrote a custom one)
                    chunk.to_csv(
                        OUTPUT,
                        mode="a",
                        index=False,
                        header=False,  # <-- important: use our custom header
                        lineterminator="\n",
                    )

                    # Update progress bar correctly
                    cur = f_in.tell()
                    if cur > last_pos:
                        pbar.update(cur - last_pos)
                        last_pos = cur

    print("Done.")
    print(f"Output file: {OUTPUT}")

if __name__ == "__main__":
    main()
