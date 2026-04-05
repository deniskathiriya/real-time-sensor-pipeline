import pandas as pd
from utils import convert_to_long_format

# Load raw dataset
df = pd.read_csv("sample_data/iot_telemetry_data.csv")

print("Original columns:")
print(df.columns)

print("\nOriginal sample data:")
print(df.head())

file_name = "iot_telemetry_data.csv"

# Convert only first 5 rows for testing
long_df = convert_to_long_format(df.head(5), file_name)

print("\nTransformed long-format data:")
print(long_df)

print("\nTransformed columns:")
print(long_df.columns)

print("\nTotal transformed rows:")
print(len(long_df))