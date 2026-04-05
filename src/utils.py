import pandas as pd
import os
import shutil

# numeric sensor columns and their units
NUMERIC_SENSOR_COLUMNS = {
    "co": "ppm",
    "humidity": "%",
    "lpg": "ppm",
    "smoke": "ppm",
    "temp": "C"
}


def convert_unix_timestamp(ts_value):
    """
    Convert Unix timestamp to pandas datetime.
    """
    return pd.to_datetime(ts_value, unit="s", errors="coerce")


def convert_to_long_format(df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """
    Convert the raw wide-format dataset into the standard long-format schema.
    """
    records = []

    for _, row in df.iterrows():
        sensor_id = row["device"]
        event_timestamp = convert_unix_timestamp(row["ts"])

        for sensor_col, unit in NUMERIC_SENSOR_COLUMNS.items():
            sensor_type = "temperature" if sensor_col == "temp" else sensor_col

            records.append({
                "sensor_id": sensor_id,
                "sensor_type": sensor_type,
                "reading_value": row[sensor_col],
                "reading_unit": unit,
                "event_timestamp": event_timestamp,
                "source_name": "kaggle_iot_sensor",
                "location": "unknown",
                "file_name": file_name
            })

    return pd.DataFrame(records)

def move_file(source_path: str, destination_folder: str):
    os.makedirs(destination_folder, exist_ok=True)
    file_name = os.path.basename(source_path)
    destination_path = os.path.join(destination_folder, file_name)
    shutil.move(source_path, destination_path)
    return destination_path