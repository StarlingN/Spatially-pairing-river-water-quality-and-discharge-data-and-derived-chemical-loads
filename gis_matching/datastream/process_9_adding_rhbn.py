# -*- coding: utf-8 -*-
"""
Add an 'rhbn_listed' flag (1/0) to canada_datastream_hydat_matchups.csv
based on whether its StationNum appears in rhbn.csv.
"""

from pathlib import Path
import pandas as pd

# ---- Paths ----
MATCHUPS_CSV = Path(r"E:\publications\clawave_1\data\datastream\process_7_datastream_with_poi_v1_0_WSC_matched\canada_datastream_hydat_matchups.csv")
RHBN_CSV     = Path(r"E:\publications\clawave_1\data\datastream\process_9_added_rhbn\rhbn.csv")
OUTPUT_CSV   = Path(r"E:\publications\clawave_1\data\datastream\process_9_added_rhbn\canada_datastream_hydat_matchups+RHBN.csv")

# Ensure output directory exists
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# ---- Read CSVs (preserve leading zeros by using string dtype) ----
# Using pandas' StringDtype keeps NA values as <NA> rather than "nan" strings.
read_kwargs = {"dtype": {"StationNum": "string"}}

df = pd.read_csv(MATCHUPS_CSV, **read_kwargs)
rhbn = pd.read_csv(RHBN_CSV, **read_kwargs)

# ---- Basic validation ----
for name, frame in [("canada_datastream_hydat_matchups.csv", df),
                    ("rhbn.csv", rhbn)]:
    if "StationNum" not in frame.columns:
        raise KeyError(f"'StationNum' column not found in {name}")

# ---- Clean StationNum (strip whitespace) ----
df["StationNum"] = df["StationNum"].str.strip()
rhbn["StationNum"] = rhbn["StationNum"].str.strip()

# ---- Build lookup set from RHBN ----
rhbn_set = set(rhbn["StationNum"].dropna().unique())

# ---- Flag presence: 1 if StationNum in RHBN, else 0 ----
df["rhbn_listed"] = df["StationNum"].isin(rhbn_set).astype(int)

# ---- Save ----
df.to_csv(OUTPUT_CSV, index=False)

# (Optional) quick sanity printout
print(f"Rows in matchups: {len(df)}")
print(f"Unique RHBN stations: {len(rhbn_set)}")
print(f"Flag counts:\n{df['rhbn_listed'].value_counts(dropna=False).to_string()}")
print(f"Saved: {OUTPUT_CSV}")
