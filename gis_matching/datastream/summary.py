import pandas as pd
import os
from pathlib import Path

# --- Helper function for fast row counting without loading into RAM ---
def count_rows_fast(filepath):
    if not os.path.exists(filepath):
        return "File not found"
    with open(filepath, 'rb') as f:
        # Count newline characters; subtract 1 for the header
        return sum(1 for _ in f) - 1

def safe_unique_count(filepath, col_options):
    if not os.path.exists(filepath):
        return "File not found"
    
    # Read just the header first to see which column name actually exists
    header = pd.read_csv(filepath, nrows=0).columns.tolist()
    target_col = next((col for col in col_options if col in header), None)
    
    if target_col:
        # Load ONLY that specific column into memory
        df = pd.read_csv(filepath, usecols=[target_col], dtype=str)
        return df[target_col].nunique()
    return f"Columns {col_options} not found"

print("="*50)
print("EXTRACTING METHODOLOGY STATISTICS...")
print("="*50)

# 1. INITIAL DATASTREAM PULL
file_raw = r"E:\publications\clawave_1\data\datastream\process_0_downloaded\observations_2000_2025_all.csv"
print(f"\n[1] Initial DataStream Pull (2000-2025):")
print(f"    Total raw records: {count_rows_fast(file_raw):,}")

# 2. POST-CLIPPING (CANADIAN BOUNDING BOX)
file_clipped = r"E:\publications\clawave_1\data\datastream\process_1_clipped\observations_2000_2025_all_clipped.csv"
print(f"\n[2] Post-Clipping (Canadian Extent):")
print(f"    Total records retained: {count_rows_fast(file_clipped):,}")

# 3. CRS UNIFICATION & MERGE
file_merged = r"E:\publications\clawave_1\data\datastream\process_4_merged_unified_crs+renamed\observations_2000_2025_all_clipped_unified_crs_renamed.csv"
print(f"\n[3] Post-CRS Unification:")
print(f"    Total records: {count_rows_fast(file_merged):,}")

# 4. POST-MATCHING (THE CORE SPATIAL ALGORITHM)
file_matched = r"E:\publications\clawave_1\data\datastream\process_7_datastream_with_poi_v1_0_WSC_matched\observations_2000_2025_all_clipped_unified_crs_renamed+catchments+streams_matchups_fixed.csv"
print(f"\n[4] Post-Spatial Matching (DataStream to HYDAT):")
print(f"    Total matched records: {count_rows_fast(file_matched):,}")
# Try to count unique HYDAT stations from the matched dataset
print(f"    Unique HYDAT pairs (WSC Obs NM): {safe_unique_count(file_matched, ['poi_v1_0_WSC_Obs_NM', 'StationNum'])}")

# 5. RHBN FILTERING
file_rhbn = r"E:\publications\clawave_1\data\datastream\process_9_added_rhbn\canada_datastream_hydat_matchups+RHBN.csv"
print(f"\n[5] RHBN Subset Integration:")
if os.path.exists(file_rhbn):
    df_rhbn = pd.read_csv(file_rhbn, dtype=str)
    rhbn_count = df_rhbn[df_rhbn['rhbn_listed'] == '1']['StationNum'].nunique()
    non_rhbn_count = df_rhbn[df_rhbn['rhbn_listed'] == '0']['StationNum'].nunique()
    print(f"    Unique HYDAT stations in lookup: {df_rhbn['StationNum'].nunique():,}")
    print(f"    Listed in RHBN: {rhbn_count:,}")
    print(f"    Not in RHBN: {non_rhbn_count:,}")
else:
    print("    File not found.")

# 6. WRTDS TEMPORAL FILTERING (FINAL CLEANED)
dir_final = Path(r"E:\publications\clawave_1\data\datastream\process_11_cleaned")
print(f"\n[6] Post-WRTDS Temporal Filtering (Final Outputs):")
total_final_records = 0
total_final_stations = set()

if dir_final.exists():
    for csv_file in dir_final.glob("*.csv"):
        # Skip the split_by_station folder if it's caught in the glob
        if not csv_file.is_file(): continue 
        
        # Count rows
        rows = count_rows_fast(csv_file)
        total_final_records += (rows if isinstance(rows, int) else 0)
        
        # Get unique stations for this parameter
        header = pd.read_csv(csv_file, nrows=0).columns.tolist()
        target_col = next((c for c in ['StationNum', 'poi_v1_0_WSC_Obs_NM'] if c in header), None)
        
        if target_col:
            df_param = pd.read_csv(csv_file, usecols=[target_col], dtype=str)
            unique_stations = df_param[target_col].nunique()
            total_final_stations.update(df_param[target_col].dropna().unique().tolist())
            print(f"    - {csv_file.stem}: {rows:,} records across {unique_stations:,} unique stations")
            
    print(f"\n    GRAND TOTAL FINAL RECORDS: {total_final_records:,}")
    print(f"    GRAND TOTAL UNIQUE STATIONS ACROSS ALL PARAMS: {len(total_final_stations):,}")
else:
    print("    Directory not found.")

print("="*50)
print("DONE!")