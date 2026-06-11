#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Make a 4-column CSV from a very large input, renaming headers and deduplicating
by those 4 columns (exact string match). Output columns ONLY:

    MonitoringLocationLatitude
    MonitoringLocationLongitude
    MonitoringLocationHorizontalCoordinateReferenceSystem
    StationNum

Key features:
- Detects encoding and newline style; keeps your original delimiter.
- Robust CSV parsing (sniffs a large sample); writer is forced to safe quoting.
- Dedupes using a disk-backed SQLite "seen set" keyed by a 128-bit hash of the 4 strings.
- Streams row-by-row with tqdm; atomic write to destination.
"""

from __future__ import annotations
import os
import sys
import csv
import logging
import sqlite3
import hashlib
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Tuple, List, Dict, Any
from tqdm import tqdm

# --- USER PATHS ---
INPUT_CSV  = r"E:\publications\clawave_1\data\datastream\process_7_datastream_with_poi_v1_0_WSC_matched\observations_2000_2025_all_clipped_unified_crs_renamed+catchments+streams_matchups_fixed.csv"
OUTPUT_CSV = r"E:\publications\clawave_1\data\datastream\process_7_datastream_with_poi_v1_0_WSC_matched\canada_datastream_hydat_matchups.csv"

# --- INPUT column name options (accept either old or already-renamed) ---
KEY_PAIRS_IN = [
    ("datastream_MonitoringLocationLatitude", "MonitoringLocationLatitude"),
    ("datastream_MonitoringLocationLongitude", "MonitoringLocationLongitude"),
    ("datastream_MonitoringLocationHorizontalCoordinateReferenceSystem",
     "MonitoringLocationHorizontalCoordinateReferenceSystem"),
    ("poi_v1_0_WSC_Obs_NM", "StationNum"),
]

# --- OUTPUT columns (exact order & names) ---
HEADERS_OUT = [
    "MonitoringLocationLatitude",
    "MonitoringLocationLongitude",
    "MonitoringLocationHorizontalCoordinateReferenceSystem",
    "StationNum",
]

# --- CONFIG ---
LOG_LEVEL = logging.INFO
OVERWRITE = True
COMMIT_EVERY = 50000
SNIFF_SAMPLE_BYTES = 1_000_000  # header + some data


# --------------------- Helpers: header/newline/encoding --------------------- #

def _detect_newline_and_header(src_f) -> Tuple[bytes, bytes]:
    """Read bytes until the first '\n'. Return (header_core_without_newline, newline_bytes)."""
    header = bytearray()
    while True:
        b = src_f.read(1)
        if not b:
            break
        header += b
        if b == b'\n':
            break
    if not header:
        raise ValueError("Input file appears to be empty (no header found).")

    if header.endswith(b'\r\n'):
        newline = b'\r\n'
        core = header[:-2]
    elif header.endswith(b'\n'):
        newline = b'\n'
        core = header[:-1]
    else:
        newline = b''
        core = bytes(header)
    return bytes(core), newline


def _detect_encoding(header_core: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            header_core.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("latin-1", b"", 0, 1, "Failed to decode header")


# --------------------- Dialect sniffing & params --------------------- #

def _sniff_dialect_from_sample(input_csv: Path, encoding: str) -> csv.Dialect:
    """Sniff delimiter/quoting using up to SNIFF_SAMPLE_BYTES of text."""
    sniffer = csv.Sniffer()
    with open(input_csv, "r", encoding=encoding, newline="") as fin:
        sample = fin.read(SNIFF_SAMPLE_BYTES)
    try:
        return sniffer.sniff(sample, delimiters=[",", ";", "\t", "|"])
    except Exception:
        class _Fallback(csv.Dialect):
            delimiter = ","
            quotechar = '"'
            escapechar = None
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        return _Fallback


def _params_from_dialect(d: csv.Dialect) -> Dict[str, Any]:
    """Turn a dialect into a param dict; fill sensible defaults."""
    return {
        "delimiter": getattr(d, "delimiter", ",") or ",",
        "quotechar": getattr(d, "quotechar", '"') or '"',
        "escapechar": getattr(d, "escapechar", None) or None,
        "doublequote": getattr(d, "doublequote", True),
        "skipinitialspace": getattr(d, "skipinitialspace", False),
        "quoting": getattr(d, "quoting", csv.QUOTE_MINIMAL),
    }


# --------------------- Header parsing & index resolution --------------------- #

def _parse_header_fields(header_text: str, params: Dict[str, Any]) -> List[str]:
    # Ensure reader can handle quotes even if sniffed as none
    reader_params = params.copy()
    if reader_params["quotechar"] in (None, ""):
        reader_params["quotechar"] = '"'
    if reader_params["quoting"] == csv.QUOTE_NONE:
        reader_params["quoting"] = csv.QUOTE_MINIMAL

    reader = csv.reader([header_text],
                        delimiter=reader_params["delimiter"],
                        quotechar=reader_params["quotechar"],
                        escapechar=reader_params["escapechar"],
                        doublequote=reader_params["doublequote"],
                        skipinitialspace=reader_params["skipinitialspace"],
                        quoting=reader_params["quoting"])
    row = next(reader, None)
    if row is None:
        raise ValueError("Failed to parse header fields.")
    return row


def _resolve_key_indices(header_fields_in: List[str]) -> List[int]:
    """Return indices of the 4 key columns from the INPUT header (original or renamed forms)."""
    indices = []
    for old_name, new_name in KEY_PAIRS_IN:
        if old_name in header_fields_in and new_name in header_fields_in:
            raise ValueError(f"Both '{old_name}' and '{new_name}' exist in header. Ambiguous input.")
        if old_name in header_fields_in:
            indices.append(header_fields_in.index(old_name))
        elif new_name in header_fields_in:
            indices.append(header_fields_in.index(new_name))
        else:
            raise KeyError(f"Required column not found: '{old_name}' (or '{new_name}').")
    return indices


# --------------------- Dedupe machinery (SQLite + hash) --------------------- #

def _hash_key(values: List[str]) -> bytes:
    """Compact 128-bit hash of the key tuple using BLAKE2b (exact strings, no trimming)."""
    h = hashlib.blake2b(digest_size=16)
    for i, v in enumerate(values):
        if i:
            h.update(b"\x1f")  # unit separator
        h.update(v.encode("utf-8", errors="surrogatepass"))
    return h.digest()


def _sqlite_seen_db(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    cur = con.cursor()
    # Pragmas tuned for speed
    cur.execute("PRAGMA journal_mode=OFF;")
    cur.execute("PRAGMA synchronous=0;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA mmap_size=30000000000;")
    cur.execute("PRAGMA cache_size=-200000;")
    cur.execute("CREATE TABLE IF NOT EXISTS seen (h BLOB PRIMARY KEY);")
    con.commit()
    return con


# --------------------- Main processing --------------------- #

def process_file(input_csv: Path, output_csv: Path, overwrite: bool = True) -> None:
    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")

    if output_csv.exists() and not overwrite:
        raise FileExistsError(
            f"Output file already exists and OVERWRITE={overwrite}: {output_csv}"
        )

    # 1) Detect header/newline/encoding
    with open(input_csv, "rb") as src_bytes:
        header_core, newline_bytes = _detect_newline_and_header(src_bytes)
        encoding = _detect_encoding(header_core)
        header_text = header_core.decode(encoding)

    newline_str = newline_bytes.decode("ascii", errors="ignore") or "\n"

    # 2) Sniff dialect from a LARGE text sample
    dialect = _sniff_dialect_from_sample(input_csv, encoding)
    params = _params_from_dialect(dialect)

    # 3) Parse input header; resolve indices of the four source columns
    header_fields_in = _parse_header_fields(header_text, params)
    key_indices = _resolve_key_indices(header_fields_in)

    # 4) Prepare temp output & temp SQLite DB
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(dir=str(output_csv.parent), delete=False, suffix=".csv") as tmp_out:
        tmp_out_path = Path(tmp_out.name)
    with NamedTemporaryFile(dir=str(output_csv.parent), delete=False, suffix=".sqlite") as tmp_db:
        tmp_db_path = Path(tmp_db.name)

    con = None
    try:
        con = _sqlite_seen_db(tmp_db_path)
        cur = con.cursor()

        # Reader: robust against weird quoting
        reader_params = params.copy()
        if reader_params["quotechar"] in (None, ""):
            reader_params["quotechar"] = '"'
        if reader_params["quoting"] == csv.QUOTE_NONE:
            reader_params["quoting"] = csv.QUOTE_MINIMAL

        # Writer: ALWAYS safe quoting to avoid "need to escape" errors
        writer_params = {
            "delimiter": params["delimiter"],
            "quotechar": '"',
            "escapechar": None,
            "doublequote": True,
            "quoting": csv.QUOTE_MINIMAL,
        }

        # 5) Stream: read, extract ONLY 4 columns, dedupe, write
        with open(input_csv, "r", encoding=encoding, newline="") as fin, \
             open(tmp_out_path, "w", encoding=encoding, newline="") as fout:

            reader = csv.reader(
                fin,
                delimiter=reader_params["delimiter"],
                quotechar=reader_params["quotechar"],
                escapechar=reader_params["escapechar"],
                doublequote=reader_params["doublequote"],
                skipinitialspace=reader_params["skipinitialspace"],
                quoting=reader_params["quoting"],
            )
            writer = csv.writer(
                fout,
                delimiter=writer_params["delimiter"],
                quotechar=writer_params["quotechar"],
                escapechar=writer_params["escapechar"],
                doublequote=writer_params["doublequote"],
                lineterminator=newline_str,
                quoting=writer_params["quoting"],
            )

            # Write output header: exactly the four desired names
            writer.writerow(HEADERS_OUT)

            # Skip input header
            try:
                next(reader)
            except StopIteration:
                pass

            rows_in = 0
            rows_out = 0
            pending = 0

            with tqdm(unit="rows", desc="Processing rows", miniters=1000) as pbar:
                for row in reader:
                    rows_in += 1

                    # Pad ragged rows so key indices are safe
                    if len(row) <= max(key_indices):
                        row = row + [""] * (max(key_indices) + 1 - len(row))

                    # Extract ONLY our 4 columns (in same order as HEADERS_OUT)
                    vals = [row[idx] for idx in key_indices]

                    # Deduplicate
                    h = _hash_key(vals)
                    try:
                        cur.execute("INSERT OR IGNORE INTO seen(h) VALUES (?)", (h,))
                        inserted = (cur.rowcount == 1)
                    except sqlite3.Error:
                        con.rollback()
                        cur.execute("INSERT OR IGNORE INTO seen(h) VALUES (?)", (h,))
                        inserted = (cur.rowcount == 1)

                    pending += 1
                    if pending >= COMMIT_EVERY:
                        con.commit()
                        pending = 0

                    if inserted:
                        writer.writerow(vals)
                        rows_out += 1

                    pbar.update(1)
                    pbar.set_postfix({"kept": rows_out, "seen": rows_in}, refresh=False)

                if pending:
                    con.commit()

            logging.info("Rows read: %d | Rows written (unique): %d", rows_in, rows_out)

        # 6) Atomic replace
        os.replace(str(tmp_out_path), str(output_csv))
        logging.info("Wrote output: %s", output_csv)

    except Exception:
        # Cleanup temp output on failure
        try:
            if 'tmp_out_path' in locals() and tmp_out_path.exists():
                tmp_out_path.unlink(missing_ok=True)
        finally:
            raise
    finally:
        # Close and remove temp DB
        if con is not None:
            con.close()
        if 'tmp_db_path' in locals() and tmp_db_path.exists():
            try:
                tmp_db_path.unlink(missing_ok=True)
            except Exception:
                logging.warning("Could not remove temp DB: %s", tmp_db_path)


def main():
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    inp = Path(INPUT_CSV)
    outp = Path(OUTPUT_CSV)
    logging.info("Input:  %s", inp)
    logging.info("Output: %s", outp)
    try:
        process_file(inp, outp, overwrite=OVERWRITE)
        logging.info("Done.")
    except Exception as e:
        logging.error("Failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
