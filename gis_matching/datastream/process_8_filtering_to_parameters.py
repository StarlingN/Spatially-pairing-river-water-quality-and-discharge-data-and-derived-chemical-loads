# -*- coding: utf-8 -*-
"""
Filter a very large Datastream CSV into parameter-specific subset CSVs (one pass).

- Streams through the input in chunks (no big memory use).
- Preserves ALL columns exactly as in the input.
- Exact-match filtering on the "datastream_CharacteristicName" column.
- Writes one CSV per parameter with safe Windows-friendly filenames (provided map).
- Live tqdm progress with processed row counts and top matches.
- Robust engine fallbacks for older pandas (C/Python engine; on_bad_lines handling).
"""

from __future__ import annotations

import os
import sys
import csv
import gc
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd
from tqdm import tqdm


# =========================
# ===== USER SETTINGS =====
# =========================

INPUT_CSV = Path(r"E:\publications\clawave_1\datastream\process_7_datastream_with_poi_v1_0_WSC_matched\observations_2000_2025_all_clipped_unified_crs_renamed+catchments+streams_matchups_fixed.csv")
OUTPUT_DIR = Path(r"E:\publications\clawave_1\datastream\process_8_filtered_to_parameters")
PARAM_COL = "datastream_CharacteristicName"

# Tune chunk size for your RAM/IO; 250k–1M rows is common.
CHUNK_SIZE = 250_000

# With older pandas, on_bad_lines only works with engine="python". This script auto-falls back.
ON_BAD_LINES = "warn"  # "error" | "warn" | "skip"

# If True, delete any existing per-parameter CSVs before writing.
OVERWRITE_OUTPUTS = True

# Set to the known encoding if needed (e.g., "utf-8", "latin-1"); None lets pandas decide/try BOMs.
CSV_ENCODING: str | None = None


# ====================================================================================
# Mapping of exact parameter label (in the CSV) -> safe output filename (no extension)
# ====================================================================================
PARAM_MAP: Dict[str, str] = {
    "Total Phosphorus, mixed forms": "total_phosphorus_mixed_forms",
    "Organic carbon": "organic_carbon",
    "Total Nitrogen, mixed forms": "total_nitrogen_mixed_forms",
    "Inorganic nitrogen (nitrate and nitrite)": "inorganic_nitrogen_nitrate_and_nitrite",
    "Nitrite": "nitrite",
    "Orthophosphate": "orthophosphate",
    "Kjeldahl nitrogen": "kjeldahl_nitrogen",
    "Ammonia": "ammonia",
    "Nitrate": "nitrate",
    "Chloride": "chloride",
    "Magnesium": "magnesium",
    "Total hardness": "total_hardness",
    "Temperature, water": "temperature_water",
    "pH": "ph",
    "Alkalinity, total": "alkalinity_total",
}


# =========================
# ====== UTILITIES ========
# =========================

def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def existing_file_has_content(path: Path) -> bool:
    try:
        return path.exists() and path.stat().st_size > 0
    except OSError:
        return False


def remove_existing_outputs(output_dir: Path, filenames: List[str]) -> None:
    if not OVERWRITE_OUTPUTS:
        return
    for name in filenames:
        try:
            (output_dir / f"{name}.csv").unlink(missing_ok=True)
        except Exception as e:
            print(f"[WARN] Could not delete existing file {name}.csv: {e}", flush=True)


def _find_actual_param_col(header_cols: List[str], desired: str) -> str:
    """
    Returns the actual column name to use for filtering:
      1) exact match first,
      2) else first column whose .strip() equals desired,
      3) else raises KeyError.
    """
    if desired in header_cols:
        return desired
    stripped_map = {c.strip(): c for c in header_cols}
    if desired in stripped_map:
        return stripped_map[desired]
    raise KeyError(
        f"Required column '{desired}' not found. "
        f"Available columns (first 12): {header_cols[:12]}{'...' if len(header_cols) > 12 else ''}"
    )


def validate_inputs(input_csv: Path, desired_param_col: str) -> Tuple[List[str], str]:
    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")
    # Read only header
    try:
        header_df = pd.read_csv(
            input_csv,
            nrows=0,
            encoding=CSV_ENCODING or None,
            engine="c",  # fast path if possible
        )
    except Exception:
        header_df = pd.read_csv(
            input_csv,
            nrows=0,
            encoding=CSV_ENCODING or None,
            engine="python",
        )
    cols = list(header_df.columns)
    actual_param_col = _find_actual_param_col(cols, desired_param_col)
    return cols, actual_param_col


def safe_open_mode_for(path: Path, header_written: bool) -> Tuple[str, bool]:
    if existing_file_has_content(path) or header_written:
        return "a", False
    return "w", True


def make_reader(
    input_csv: Path,
    chunksize: int,
    actual_param_col: str,
    encoding: str | None,
    on_bad_lines: str,
):
    """
    Create a chunked CSV reader with graceful fallbacks across pandas versions:
      1) engine='c' with on_bad_lines (may raise TypeError on older versions)
      2) engine='c' without on_bad_lines
      3) engine='python' with on_bad_lines
    """
    base_kwargs = dict(
        chunksize=chunksize,
        encoding=encoding or None,
        low_memory=False,
        dtype={actual_param_col: "object"},
        # memory_map generally helps but can be flaky on some filesystems; keep default.
    )

    # Try C engine with on_bad_lines
    try:
        return pd.read_csv(
            input_csv,
            engine="c",
            on_bad_lines=on_bad_lines,  # may not be supported on older pandas C-engine
            **base_kwargs,
        )
    except TypeError:
        # Older pandas where on_bad_lines not accepted by C-engine
        pass
    except Exception:
        # Other C-engine issues with on_bad_lines; continue to next fallback
        pass

    # Try C engine without on_bad_lines
    try:
        return pd.read_csv(
            input_csv,
            engine="c",
            **base_kwargs,
        )
    except Exception:
        pass

    # Fallback: Python engine with on_bad_lines (widely supported)
    return pd.read_csv(
        input_csv,
        engine="python",
        on_bad_lines=on_bad_lines,
        **base_kwargs,
    )


# =========================
# ===== MAIN ROUTINE ======
# =========================

def main() -> None:
    print("\n=== Datastream Parameter Filter ===", flush=True)
    print(f"Input : {INPUT_CSV}", flush=True)
    print(f"Output: {OUTPUT_DIR}", flush=True)
    print(f"Column: {PARAM_COL}", flush=True)
    print(f"Params: {len(PARAM_MAP)} targets\n", flush=True)

    ensure_directory(OUTPUT_DIR)

    # Validate and resolve the exact column name as it appears in the file
    all_columns, actual_param_col = validate_inputs(INPUT_CSV, PARAM_COL)

    # Prepare outputs
    remove_existing_outputs(OUTPUT_DIR, list(PARAM_MAP.values()))

    # Track header writes and counts
    header_written = {label: existing_file_has_content(OUTPUT_DIR / f"{stem}.csv")
                      for label, stem in PARAM_MAP.items()}
    match_counts = {label: 0 for label in PARAM_MAP.keys()}
    processed_rows = 0
    bad_chunks = 0

    try:
        reader = make_reader(
            INPUT_CSV,
            chunksize=CHUNK_SIZE,
            actual_param_col=actual_param_col,
            encoding=CSV_ENCODING,
            on_bad_lines=ON_BAD_LINES,
        )

        pbar = tqdm(unit="rows", dynamic_ncols=True, desc="Processing", leave=True)

        for chunk_idx, df in enumerate(reader, start=1):
            try:
                # Defensive check
                if actual_param_col not in df.columns:
                    raise KeyError(
                        f"Chunk {chunk_idx} missing required column '{actual_param_col}'."
                    )

                n_rows = len(df)
                processed_rows += n_rows

                # Single pass filter
                mask = df[actual_param_col].isin(PARAM_MAP.keys())
                if mask.any():
                    df_sub = df.loc[mask]

                    # Write per-parameter groups
                    for param_value, group in df_sub.groupby(actual_param_col):
                        out_stem = PARAM_MAP[param_value]
                        out_path = OUTPUT_DIR / f"{out_stem}.csv"
                        mode, write_header = safe_open_mode_for(
                            out_path, header_written[param_value]
                        )
                        group.to_csv(
                            out_path,
                            mode=mode,
                            header=write_header,
                            index=False,
                            encoding="utf-8",
                            quoting=csv.QUOTE_MINIMAL,
                            lineterminator="\n",  # <-- fixed: correct pandas kw
                        )
                        header_written[param_value] = True
                        match_counts[param_value] += len(group)

                # Update progress
                top3 = sorted(match_counts.items(), key=lambda kv: kv[1], reverse=True)[:3]
                postfix = {k: v for k, v in top3}
                postfix["rows"] = processed_rows
                pbar.update(n_rows)
                pbar.set_postfix(postfix)

                # Free memory
                del df
                if mask is not None:
                    del mask
                if 'df_sub' in locals():
                    del df_sub
                gc.collect()

            except Exception as chunk_err:
                bad_chunks += 1
                tqdm.write(f"[WARN] Error in chunk {chunk_idx}: {chunk_err}")

        # Close progress bar
        pbar.close()

        # Close reader if available
        try:
            reader.close()
        except Exception:
            pass

    except pd.errors.EmptyDataError:
        print("[ERROR] Input CSV appears empty or unreadable.", flush=True)
        sys.exit(2)
    except FileNotFoundError as e:
        print(f"[ERROR] {e}", flush=True)
        sys.exit(2)
    except PermissionError as e:
        print(f"[ERROR] Permission issue: {e}", flush=True)
        sys.exit(2)
    except Exception as e:
        print(f"[ERROR] Unhandled exception: {e}", flush=True)
        sys.exit(2)

    # =========================
    # ===== FINAL SUMMARY =====
    # =========================
    print("\n=== Summary ===", flush=True)
    print(f"Total rows processed: {processed_rows}", flush=True)
    if bad_chunks:
        print(f"Chunks with errors:   {bad_chunks}", flush=True)
    print("Matches per parameter:", flush=True)
    for label, count in match_counts.items():
        print(f"  - {label}: {count}", flush=True)

    # Machine-readable summary
    try:
        summary_path = OUTPUT_DIR / "_filter_summary.tsv"
        with summary_path.open("w", encoding="utf-8", newline="") as f:
            f.write("parameter_label\toutput_file\trows_written\n")
            for label, stem in PARAM_MAP.items():
                f.write(f"{label}\t{stem}.csv\t{match_counts[label]}\n")
        print(f"\nWrote summary: {summary_path}", flush=True)
    except Exception as e:
        print(f"[WARN] Could not write summary file: {e}", flush=True)


if __name__ == "__main__":
    main()
