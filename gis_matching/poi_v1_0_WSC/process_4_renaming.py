#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rename headers in a CSV by prefixing 'poi_v1_0_WSC_' while preserving all data as-is.

- Streams the file to avoid memory blowups on large CSVs.
- Copies the file byte-for-byte except the very first line (header).
- Detects original newline and preserves it.
- Uses tqdm to report progress over remaining bytes.
- Avoids double-prefixing if headers are already prefixed.
"""

import os
import sys
import io
import csv
import hashlib
import logging
from pathlib import Path
from typing import Tuple, List

# ---- Configuration (edit paths if needed) ----
INPUT_PATH  = r"E:\publications\clawave_1\poi_v1_0_WSC\process_1_arcgis_clipped\poi_v1_0_WSC_clipped.csv"
OUTPUT_PATH = r"E:\publications\clawave_1\poi_v1_0_WSC\process_4_renamed\poi_v1_0_WSC_clipped_renamed.csv"
PREFIX      = "poi_v1_0_WSC_"
CHUNK_SIZE  = 8 * 1024 * 1024  # 8 MB chunked copy
# ----------------------------------------------

# tqdm import with soft fallback
try:
    from tqdm.auto import tqdm
except Exception:
    def tqdm(iterable=None, total=None, desc=None, unit=None):
        # Minimal fallback if tqdm is not available
        class _Dummy:
            def __init__(self, it): self.it = it
            def __iter__(self): return iter(self.it)
            def update(self, n): pass
            def close(self): pass
        return _Dummy(iterable if iterable is not None else [])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def _decode_header_line(header_bytes: bytes) -> Tuple[str, str, str]:
    """
    Try a few common encodings to decode the header *content* (without newline).
    Returns: (header_text, encoding_used, newline_bytes_as_text)
    """
    # Separate newline bytes so we can preserve them
    newline = b""
    if header_bytes.endswith(b"\r\n"):
        header_body = header_bytes[:-2]
        newline = b"\r\n"
    elif header_bytes.endswith(b"\n"):
        header_body = header_bytes[:-1]
        newline = b"\n"
    elif header_bytes.endswith(b"\r"):
        header_body = header_bytes[:-1]
        newline = b"\r"
    else:
        header_body = header_bytes  # no explicit newline captured

    encodings_to_try = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
    last_err = None
    for enc in encodings_to_try:
        try:
            text = header_body.decode(enc)
            return text, enc, newline.decode("latin-1", errors="ignore")
        except Exception as e:
            last_err = e
            continue

    raise UnicodeDecodeError(
        "unknown", header_body, 0, len(header_body),
        f"Failed to decode header with tried encodings; last error: {last_err}"
    )

def _detect_delimiter(header_text: str) -> str:
    """
    Heuristic delimiter detection for the header line.
    Falls back to comma.
    """
    candidates = [",", ";", "\t", "|"]
    counts = {c: header_text.count(c) for c in candidates}
    # Choose the delimiter with the highest count; default comma
    delim = max(counts, key=counts.get)
    return delim if counts[delim] > 0 else ","

def _split_fields(header_text: str, delimiter: str) -> List[str]:
    """
    Split header fields safely using csv.reader with a fixed delimiter.
    This handles quoted headers correctly if present.
    """
    reader = csv.reader([header_text], delimiter=delimiter)
    return next(reader)

def _already_prefixed(fields: List[str], prefix: str) -> bool:
    return all(f.startswith(prefix) for f in fields)

def _prefix_fields(fields: List[str], prefix: str) -> List[str]:
    """
    Prefix fields while preserving any leading/trailing spaces within the field text.
    Also removes BOM if present on the very first field *before* prefixing.
    """
    new_fields = []
    for i, f in enumerate(fields):
        # Remove BOM only at the start of the first field if present
        if i == 0 and f and f[0] == "\ufeff":
            f = f.lstrip("\ufeff")
        new_fields.append(prefix + f)
    return new_fields

def _reconstruct_header(new_fields: List[str], delimiter: str, original_had_quotes: bool, quotechar: str = '"') -> str:
    """
    Recreate a single-line header string. If the original header contained quotes around
    fields (heuristically), we quote every field to preserve the look.
    """
    if original_had_quotes:
        escaped = [f'{quotechar}{f.replace(quotechar, quotechar*2)}{quotechar}' for f in new_fields]
        return delimiter.join(escaped)
    else:
        return delimiter.join(new_fields)

def _header_had_quotes(raw_header_text: str, delimiter: str, quotechar: str = '"') -> bool:
    """
    Heuristically check if the original header looked like quoted fields, e.g.:
    "Id","SubId","Obs_NM",...
    """
    stripped = raw_header_text.strip()
    if not stripped:
        return False
    # Simple pattern: starts with quote and contains quote-delimiter-quote sequences
    if stripped.startswith(quotechar) and (quotechar + delimiter + quotechar) in stripped:
        return True
    return False

def rename_headers_streaming(input_path: str, output_path: str, prefix: str, chunk_size: int = CHUNK_SIZE) -> None:
    in_path = Path(input_path)
    out_path = Path(output_path)
    out_dir  = out_path.parent

    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    out_dir.mkdir(parents=True, exist_ok=True)

    # Open input in binary to preserve exact bytes for data rows
    with open(in_path, "rb") as fin:
        file_size = in_path.stat().st_size
        logging.info(f"Input file size: {file_size:,} bytes")

        # Read exactly the first line (header), including its newline bytes
        header_bytes = fin.readline()
        if not header_bytes:
            raise ValueError("The input file appears to be empty; no header found.")

        # Keep a copy of the raw header (without newline) for quoting heuristic
        # We'll decode header for detection; we also capture original newline
        header_text, encoding_used, newline_text = _decode_header_line(header_bytes)
        delimiter = _detect_delimiter(header_text)
        origin_quoted = _header_had_quotes(header_text, delimiter, quotechar='"')

        # Parse fields with csv.reader (handles quotes if any)
        fields = _split_fields(header_text, delimiter)

        # Safety: avoid double-prefixing
        if _already_prefixed(fields, prefix):
            logging.info("Headers already prefixed; will copy file as-is to the output path.")
            # Copy raw file bytes with a progress bar
            temp_path = out_path.with_suffix(out_path.suffix + ".tmp")
            with open(temp_path, "wb") as fout:
                fin.seek(0)  # rewind and copy whole file
                remaining = file_size
                pbar = tqdm(total=remaining, unit="B", desc="Copying",)
                while True:
                    chunk = fin.read(chunk_size)
                    if not chunk:
                        break
                    fout.write(chunk)
                    pbar.update(len(chunk))
                pbar.close()
            # Atomic replace
            temp_path.replace(out_path)
            logging.info(f"Output written (copied unchanged): {out_path}")
            return

        # Build new header line
        new_fields = _prefix_fields(fields, prefix)

        # Detect duplicate names post-rename (very unlikely, but good to guard)
        if len(set(new_fields)) != len(new_fields):
            dups = [n for n in new_fields if new_fields.count(n) > 1]
            raise ValueError(f"Duplicate column names after prefixing: {sorted(set(dups))}")

        new_header_text = _reconstruct_header(new_fields, delimiter, origin_quoted, quotechar='"')
        new_header_bytes = new_header_text.encode(encoding_used, errors="strict") + newline_text.encode("latin-1", errors="ignore")

        # Prepare temp output for atomic move
        temp_path = out_path.with_suffix(out_path.suffix + ".tmp")
        logging.info(f"Writing to temp file: {temp_path}")

        with open(temp_path, "wb") as fout:
            # Write the new header
            fout.write(new_header_bytes)

            # Copy the rest of the file byte-for-byte with progress
            remaining_bytes = file_size - len(header_bytes)
            pbar = tqdm(total=remaining_bytes, unit="B", desc="Copying data")
            copied = 0
            while True:
                chunk = fin.read(chunk_size)
                if not chunk:
                    break
                fout.write(chunk)
                copied += len(chunk)
                pbar.update(len(chunk))
            pbar.close()

        # Atomic replace the final file
        temp_path.replace(out_path)
        logging.info(f"Output written: {out_path}")
        logging.info(f"Header encoding: {encoding_used} | Delimiter: {repr(delimiter)} | Quoted: {origin_quoted}")

        # Quick sanity check: read back the first line of the output
        with open(out_path, "rb") as fcheck:
            out_header = fcheck.readline()
        out_header_text, _, _ = _decode_header_line(out_header)
        # Ensure it starts with the expected prefix
        check_fields = _split_fields(out_header_text, delimiter)
        if not _already_prefixed(check_fields, prefix):
            raise RuntimeError("Post-write verification failed: headers not properly prefixed.")

def main():
    try:
        logging.info("Starting header renaming...")
        logging.info(f"Input:  {INPUT_PATH}")
        logging.info(f"Output: {OUTPUT_PATH}")
        rename_headers_streaming(INPUT_PATH, OUTPUT_PATH, PREFIX, CHUNK_SIZE)
        logging.info("Done.")
    except Exception as e:
        logging.exception(f"Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
