import pandas as pd

# Path to your CSV file
file_path = r"E:\publications\clawave_1\data\datastream\process_1_clipped\observations_2000_2025_all_clipped.csv"

# Read the CSV, parsing the ActivityStartDate as dates
df = pd.read_csv(file_path, parse_dates=["ActivityStartDate"])

# 1) Number of rows
num_rows = len(df)

# 2) Range of ActivityStartDate
min_date = df["ActivityStartDate"].min()
max_date = df["ActivityStartDate"].max()

print(f"Number of rows: {num_rows}")
print(f"ActivityStartDate range: {min_date.date()} to {max_date.date()}")
