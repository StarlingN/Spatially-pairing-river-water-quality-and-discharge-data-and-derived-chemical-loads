#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Safely copy a large CSV, then replace specific erroneous strings in ONLY the target columns.
- Streams row-by-row (memory safe)
- Shows tqdm progress for copy + processing
- Preserves delimiter/quote as much as practical; uses a SAFE writer to avoid 'need to escape' errors
- Robust error handling and atomic write

Requirements:
    pip install tqdm

Usage:
    python process_1_fixing.py
    # Optional:
    #   --src <path>       # source CSV
    #   --dst <path>       # destination CSV (fixed copy)
    #   --skip-copy        # if you already created the fixed copy and only want replacements
"""

import argparse
import csv
import os
import sys
import time
import tempfile
from typing import Tuple

from tqdm import tqdm

# --------------------------
# CONFIG (pre-filled for you)
# --------------------------
SRC_PATH_DEFAULT = r"E:\publications\clawave_1\data\datastream\process_7_datastream_with_poi_v1_0_WSC_matched\observations_2000_2025_all_clipped_unified_crs_renamed+catchments+streams_matchups.csv"
DST_PATH_DEFAULT = r"E:\publications\clawave_1\data\datastream\process_7_datastream_with_poi_v1_0_WSC_matched\observations_2000_2025_all_clipped_unified_crs_renamed+catchments+streams_matchups_fixed.csv"

TARGET_COLUMNS = [
    "datastream_catchment_Obs_NM",
    "datastream_stream_Obs_NM",
    "poi_v1_0_WSC_Obs_NM",
    "poi_v1_0_WSC_catchment_Obs_NM",
    "poi_v1_0_WSC_stream_Obs_NM",
]

BAD_VALUES = {"23.53747846082304", "23.537478460823"}
REPLACEMENT = "05OJ017"

# Encodings to try in order (reading & writing with the first that works)
CANDIDATE_ENCODINGS = ["utf-8", "utf-8-sig", "cp1252", "latin1"]


def human_size(num: int) -> str:
    for unit in ["B","KB","MB","GB","TB"]:
        if num < 1024.0:
            return f"{num:3.1f}{unit}"
        num /= 1024.0
    return f"{num:.1f}PB"


def copy_file_with_tqdm(src: str, dst: str, chunk_size: int = 16 * 1024 * 1024) -> None:
    """Copy a file in binary mode with a progress bar."""
    if not os.path.exists(src):
        raise FileNotFoundError(f"Source not found: {src}")

    os.makedirs(os.path.dirname(dst), exist_ok=True)

    total = os.path.getsize(src)
    desc = f"Copying {os.path.basename(src)} → {os.path.basename(dst)}"
    with open(src, "rb") as fsrc, open(dst, "wb") as fdst, tqdm(
        total=total, unit="B", unit_scale=True, unit_divisor=1024, desc=desc, dynamic_ncols=True
    ) as pbar:
        while True:
            buf = fsrc.read(chunk_size)
            if not buf:
                break
            fdst.write(buf)
            pbar.update(len(buf))


def try_sniff_dialect(path: str, encoding: str) -> csv.Dialect:
    """Sniff a CSV dialect from a sample; fall back to a simple dialect."""
    with open(path, "r", encoding=encoding, newline="") as f:
        sample = f.read(1024 * 1024)  # 1MB sample
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
            return dialect
        except Exception:
            class SimpleDialect(csv.Dialect):
                delimiter = ","
                quotechar = '"'
                escapechar = None
                doublequote = True
                skipinitialspace = False
                lineterminator = "\r\n"
                quoting = csv.QUOTE_MINIMAL
            return SimpleDialect()


def try_open_with_encodings_for_sniff(path: str) -> Tuple[str, csv.Dialect]:
    """
    Try multiple encodings until we can sniff a dialect successfully.
    Returns (encoding, dialect).
    """
    last_err = None
    for enc in CANDIDATE_ENCODINGS:
        try:
            dialect = try_sniff_dialect(path, enc)
            return enc, dialect
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Could not read/sniff CSV with candidate encodings {CANDIDATE_ENCODINGS}. Last error: {last_err}")


def count_rows(path: str) -> int:
    """
    Quick line counter for progress. Note: CSVs with embedded newlines in quoted
    fields can make this an overestimate. It's only used for the progress bar.
    """
    total_lines = 0
    with open(path, "rb") as f:
        buf_size = 8 * 1024 * 1024
        while True:
            b = f.read(buf_size)
            if not b:
                break
            total_lines += b.count(b"\n")
    return max(0, total_lines - 1)  # minus header line


def make_safe_writer_kwargs_from_dialect(dialect: csv.Dialect) -> dict:
    """
    Create writer kwargs from a (possibly unsafe) dialect.
    If the dialect declares QUOTE_NONE, switch to QUOTE_MINIMAL to avoid
    'need to escape, but no escapechar set' errors when writing.
    """
    # Base kwargs from sniffed dialect
    quoting = getattr(dialect, "quoting", csv.QUOTE_MINIMAL)
    if quoting == csv.QUOTE_NONE:
        # Force quoting instead of escaping
        quoting = csv.QUOTE_MINIMAL

    writer_kwargs = dict(
        delimiter=getattr(dialect, "delimiter", ",") or ",",
        quotechar=getattr(dialect, "quotechar", '"') or '"',
        doublequote=True,  # safer for most CSV consumers
        lineterminator=getattr(dialect, "lineterminator", "\r\n") or "\r\n",
        quoting=quoting,
    )

    # If the original dialect had an escapechar and quoting isn't minimal/full,
    # keep it; otherwise it's not necessary.
    esc = getattr(dialect, "escapechar", None)
    if esc:
        writer_kwargs["escapechar"] = esc

    return writer_kwargs


def process_file_in_place(
    path: str,
    dialect: csv.Dialect,
    encoding: str,
    target_cols: list,
    bad_values: set,
    replacement: str,
) -> Tuple[int, dict, int]:
    """
    Read 'path' CSV and write to a temp file; replace only in target columns;
    then atomically replace original 'path'.

    Returns:
        total_rows_processed, replacements_by_column (dict), rows_corrected (int)
    """
    total_rows_est = count_rows(path)

    # Prepare temp file in the same directory for atomic replace
    dirpath = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp", dir=dirpath, text=True)
    os.close(fd)  # We'll reopen with the proper encoding

    replacements = {c: 0 for c in target_cols}
    processed = 0
    rows_corrected = 0

    reader_header = None

    try:
        with open(path, "r", encoding=encoding, newline="") as fin, \
             open(tmp_path, "w", encoding=encoding, newline="") as fout:

            # Use sniffed dialect for reading (header parsed automatically)
            reader = csv.DictReader(fin, dialect=dialect)
            reader_header = reader.fieldnames or []
            if reader_header and reader_header[0].startswith("\ufeff"):
                reader_header[0] = reader_header[0].lstrip("\ufeff")
                # Make DictReader use the cleaned header keys
                reader.fieldnames = reader_header

            # Warn about missing targets
            missing = [c for c in target_cols if c not in reader_header]
            if missing:
                print(f"[WARN] Missing columns (will be skipped): {missing}")

            work_cols = [c for c in target_cols if c in reader_header]

            # SAFE writer kwargs to avoid 'need to escape' errors
            writer_kwargs = make_safe_writer_kwargs_from_dialect(dialect)
            writer = csv.DictWriter(fout, fieldnames=reader_header, extrasaction="ignore", **writer_kwargs)
            writer.writeheader()

            with tqdm(total=total_rows_est or None, unit="rows", desc="Processing rows", dynamic_ncols=True) as pbar:
                for row in reader:
                    if row is None:
                        break

                    any_replaced = False
                    # ---- Existing replacement logic (unchanged) ----
                    for col in work_cols:
                        val = row.get(col, None)
                        if val is not None:
                            if val.strip() in bad_values:
                                row[col] = replacement
                                replacements[col] += 1
                                any_replaced = True

                    if any_replaced:
                        rows_corrected += 1

                    # ---- NEW: drop row if ANY target col has > 7 chars (after replacements) ----
                    drop_row = False
                    for col in work_cols:
                        v = row.get(col, None)
                        if v is None:
                            continue
                        if len(v.strip()) > 7:
                            drop_row = True
                            break
                    if drop_row:
                        # Skip writing this row entirely
                        # (Progress bar behavior left unchanged intentionally)
                        continue
                    # ------------------------------------------------

                    writer.writerow(row)
                    processed += 1
                    if total_rows_est:
                        pbar.update(1)

        # Atomic replace: overwrite the original (copied) file with the temp
        os.replace(tmp_path, path)
        return processed, replacements, rows_corrected

    except Exception as e:
        # Clean up temp file on failure
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        raise e


def main():
    parser = argparse.ArgumentParser(description="Copy a CSV and fix specific bad values in selected columns, streaming with tqdm.")
    parser.add_argument("--src", default=SRC_PATH_DEFAULT, help="Source CSV path (original, untouched).")
    parser.add_argument("--dst", default=DST_PATH_DEFAULT, help="Destination CSV path (the 'fixed' copy).")
    parser.add_argument("--skip-copy", action="store_true", help="Skip the initial byte-for-byte copy step (operate on existing dst).")
    args = parser.parse_args()

    src = args.src
    dst = args.dst

    start = time.time()

    # 1) Make a plain copy first (so original remains untouched)
    if not args.skip_copy:
        try:
            if os.path.abspath(src) == os.path.abspath(dst):
                print("[ERROR] Destination path must differ from source path.")
                sys.exit(2)
            if os.path.exists(dst):
                print(f"[INFO] Destination already exists and will be overwritten: {dst}")
            copy_file_with_tqdm(src, dst)
        except PermissionError as e:
            print(f"[ERROR] Permission denied during copy: {e}")
            sys.exit(3)
        except FileNotFoundError as e:
            print(f"[ERROR] {e}")
            sys.exit(4)
        except OSError as e:
            print(f"[ERROR] OS error during copy: {e}")
            sys.exit(5)

    # 2) Detect encoding + dialect from the *copied* file
    try:
        encoding, dialect = try_open_with_encodings_for_sniff(dst)
        quoting_name = {
            csv.QUOTE_MINIMAL: "QUOTE_MINIMAL",
            csv.QUOTE_ALL: "QUOTE_ALL",
            csv.QUOTE_NONE: "QUOTE_NONE",
            csv.QUOTE_NONNUMERIC: "QUOTE_NONNUMERIC",
        }.get(getattr(dialect, "quoting", csv.QUOTE_MINIMAL), str(getattr(dialect, "quoting", "?")))
        print(f"[INFO] Using encoding='{encoding}', delimiter='{dialect.delimiter}', quotechar='{dialect.quotechar}', quoting={quoting_name}, escapechar='{getattr(dialect,'escapechar',None)}'")
    except Exception as e:
        print(f"[ERROR] Failed to read destination file for sniffing: {e}")
        sys.exit(6)

    # 3) Replace in place (on the fixed copy only)
    try:
        total_rows, repl, rows_corrected = process_file_in_place(
            path=dst,
            dialect=dialect,
            encoding=encoding,
            target_cols=TARGET_COLUMNS,
            bad_values=BAD_VALUES,
            replacement=REPLACEMENT,
        )
    except PermissionError as e:
        print(f"[ERROR] Permission denied while processing: {e}")
        sys.exit(7)
    except OSError as e:
        print(f"[ERROR] OS error while processing: {e}")
        sys.exit(8)
    except Exception as e:
        print(f"[ERROR] Unexpected error while processing: {e}")
        sys.exit(9)

    elapsed = time.time() - start
    print("\n========== SUMMARY ==========")
    print(f"Processed rows: {total_rows:,}")
    total_replacements = sum(repl.values())
    print(f"Total replacements (all columns): {total_replacements:,}")
    print(f"Rows corrected (≥1 replacement in the row): {rows_corrected:,}")
    for col in TARGET_COLUMNS:
        print(f"  {col}: {repl.get(col, 0):,}")
    try:
        size = os.path.getsize(dst)
        print(f"Output file: {dst} ({human_size(size)})")
    except Exception:
        print(f"Output file: {dst}")
    print(f"Elapsed time: {elapsed:0.1f} s")
    print("Done.")


if __name__ == "__main__":
    main()
