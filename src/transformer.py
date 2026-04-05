import pandas as pd

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


def transform_dataframe(df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """
    Transform raw wide-format IoT dataset into standard long-format schema.

    Output columns:
        sensor_id
        sensor_type
        reading_value
        reading_unit
        event_timestamp
        source_name
        location
        file_name
        ingestion_time
        processed_time
    """
    records = []
    processed_time = pd.Timestamp.now()

    for _, row in df.iterrows():
        sensor_id = row["device"]
        event_timestamp = convert_unix_timestamp(row["ts"])

        for sensor_col, unit in NUMERIC_SENSOR_COLUMNS.items():
            sensor_type = "temperature" if sensor_col == "temp" else sensor_col

            records.append({
                "sensor_id": sensor_id,
                "sensor_type": sensor_type,
                "reading_value": float(row[sensor_col]),
                "reading_unit": unit,
                "event_timestamp": event_timestamp,
                "source_name": "kaggle_iot_sensor",
                "location": "unknown",
                "file_name": file_name,
                "ingestion_time": processed_time,
                "processed_time": processed_time
            })

    transformed_df = pd.DataFrame(records)
    return transformed_df