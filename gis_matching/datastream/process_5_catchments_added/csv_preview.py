
from __future__ import annotations

import csv
import io
import os
from pathlib import Path

# You need tqdm installed: pip install tqdm
from tqdm import tqdm

# ============================================================
# USER PARAMETERS (EDIT THESE THREE LINES ONLY)
# ============================================================
INPUT_CSV_PATH = r"E:\publications\clawave_1\data\datastream\process_5_catchments_added\observations_2000_2025_all_clipped_unified_crs_renamed+catchments.csv"
OUTPUT_DIR     = r"E:\publications\clawave_1\data\datastream\process_5_catchments_added"  # no trailing backslash is safest
N_ROWS         = 1000  # number of data rows to keep in the preview
# ============================================================


# ------------------------------------------------------------
# Public API
# ------------------------------------------------------------

def create_csv_preview(input_csv_path: str, output_dir: str, n_rows: int = 1000) -> str:
    """
    Create a CSV "preview" that preserves all columns but limits rows.
    - Streams input; suitable for very large files.
    - Detects common encodings (UTF-8/UTF-8-SIG/UTF-16/UTF-32, cp1252/latin-1 fallback).
    - Sniffs CSV dialect (delimiter/quoting) and writes with the same settings.
    - Always includes the header + first `n_rows` data rows.
    - Automatically appends "_preview" to the input filename and saves in `output_dir`.
    - Shows a tqdm progress bar over rows written, with byte progress in the postfix.
    """
    input_path = Path(input_csv_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    # Build output path: <output_dir>/<original_name>_preview<.csv>
    output_directory = Path(output_dir)
    output_directory.mkdir(parents=True, exist_ok=True)
    stem = input_path.stem  # "file" for "file.csv"
    suffix = "".join(input_path.suffixes) or ".csv"  # handle multi-suffixes, default to .csv
    preview_name = f"{stem}_preview{suffix}"
    output_path = output_directory / preview_name

    # Increase field size limit for very wide CSVs
    try:
        csv.field_size_limit(10**9)
    except OverflowError:
        csv.field_size_limit(2_147_483_647)

    # Encoding detection (BOM-aware + reasonable fallbacks)
    enc = _detect_encoding(input_path)

    # Sniff dialect from a sample; fallback to excel (comma-delimited)
    dialect = _sniff_dialect(input_path, enc)

    # File size for byte progress
    try:
        file_size = os.path.getsize(input_path)
    except OSError:
        file_size = 0

    limit = int(n_rows)
    rows_written = 0

    # Open underlying binary stream to track bytes read accurately
    raw = open(input_path, "rb")
    try:
        buffered = io.BufferedReader(raw)
        text = io.TextIOWrapper(buffered, encoding=enc, errors="replace", newline="")

        with text, open(output_path, "w", encoding="utf-8", newline="") as fout:
            reader = csv.reader(text, dialect)
            writer = csv.writer(fout, dialect)

            # Header
            header = next(reader, None)
            if header is not None:
                writer.writerow(header)

            # Progress bar over rows written (data rows only)
            with tqdm(total=limit, unit="row", desc="Writing preview", leave=True) as pbar:
                for i, row in enumerate(reader, start=1):
                    writer.writerow(row)
                    rows_written += 1
                    pbar.update(1)

                    # Update byte-based postfix periodically (every 100 rows or at end)
                    if rows_written % 100 == 0 or rows_written == limit:
                        try:
                            bytes_read = raw.tell()  # bytes consumed from the underlying file
                        except Exception:
                            bytes_read = 0
                        pct = f"{(bytes_read / file_size * 100):.1f}%" if file_size else "NA"
                        pbar.set_postfix(rows=rows_written, bytes=f"{bytes_read/1e6:.1f}/{(file_size or 1)/1e6:.1f} MB", pct=pct)

                    if rows_written >= limit:
                        break

                # If file ended before reaching limit, finalize the bar at the real total
                if rows_written < limit:
                    pbar.total = rows_written
                    pbar.refresh()

    finally:
        raw.close()

    return str(output_path)


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def _detect_encoding(path: Path, sample_bytes: int = 2_000_000) -> str:
    """
    Detect a reasonable text encoding for CSV reading.
    Priority:
      1) BOM-based detection (UTF-8-SIG, UTF-16 LE/BE, UTF-32 LE/BE)
      2) Try 'utf-8' strict
      3) Try 'cp1252'
      4) Fallback 'latin-1' (lossless single-byte)

    Returns an encoding string suitable for open(..., encoding=...).
    """
    with open(path, "rb") as fb:
        raw = fb.read(sample_bytes)

    # BOM checks
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if raw.startswith(b"\xff\xfe\x00\x00"):
        return "utf-32-le"
    if raw.startswith(b"\x00\x00\xfe\xff"):
        return "utf-32-be"
    if raw.startswith(b"\xff\xfe"):
        return "utf-16-le"
    if raw.startswith(b"\xfe\xff"):
        return "utf-16-be"

    # Try utf-8 strict
    try:
        raw.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        pass

    # Try cp1252
    try:
        raw.decode("cp1252")
        return "cp1252"
    except UnicodeDecodeError:
        pass

    # Last resort: latin-1 (never fails to decode 0-255)
    return "latin-1"


def _sniff_dialect(path: Path, encoding: str, sample_bytes: int = 2_000_000) -> csv.Dialect:
    """
    Sniff CSV dialect (delimiter/quoting) from a sample of the file.
    Fallback to csv.excel if sniffing fails.
    """
    try:
        with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
            sample = f.read(sample_bytes)
            return csv.Sniffer().sniff(sample)
    except Exception:
        return csv.excel


# ------------------------------------------------------------
# Main (uses the THREE parameters defined at the top)
# ------------------------------------------------------------

if __name__ == "__main__":
    out = create_csv_preview(INPUT_CSV_PATH, OUTPUT_DIR, N_ROWS)
    print(f"Preview written: {out}")
