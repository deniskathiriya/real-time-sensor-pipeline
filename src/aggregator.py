import pandas as pd


def aggregate_sensor_data(transformed_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate transformed sensor data by:
    - file_name
    - source_name
    - sensor_type

    Metrics:
    - min_value
    - max_value
    - avg_value
    - stddev_value
    - record_count
    """
    if transformed_df.empty:
        return pd.DataFrame(columns=[
            "file_name",
            "source_name",
            "sensor_type",
            "min_value",
            "max_value",
            "avg_value",
            "stddev_value",
            "record_count",
            "aggregation_time"
        ])

    grouped = (
        transformed_df
        .groupby(["file_name", "source_name", "sensor_type"])["reading_value"]
        .agg(["min", "max", "mean", "std", "count"])
        .reset_index()
    )

    grouped = grouped.rename(columns={
        "min": "min_value",
        "max": "max_value",
        "mean": "avg_value",
        "std": "stddev_value",
        "count": "record_count"
    })

    grouped["stddev_value"] = grouped["stddev_value"].fillna(0.0)
    grouped["aggregation_time"] = pd.Timestamp.now()

    return grouped