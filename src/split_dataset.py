import pandas as pd
import os

input_file = "sample_data/iot_telemetry_data.csv"
output_folder = "test data files"
chunk_size = 1000

os.makedirs(output_folder, exist_ok=True)

df = pd.read_csv(input_file)

for i in range(0, len(df), chunk_size):
    chunk = df.iloc[i:i + chunk_size]
    file_name = f"sensor_batch_{i // chunk_size + 1}.csv"
    output_path = os.path.join(output_folder, file_name)
    chunk.to_csv(output_path, index=False)

print("Dataset split completed.")