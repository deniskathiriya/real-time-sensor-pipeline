import pandas as pd


class ValidationError(Exception):
    pass


REQUIRED_COLUMNS = ["ts", "device", "co", "humidity", "lpg", "smoke", "temp"]

VALUE_RANGES = {
    "temp": (-50, 50),
    "humidity": (0, 100),
    "co": (0, 100),
    "lpg": (0, 100),
    "smoke": (0, 100),
}


def validate_required_columns(df: pd.DataFrame):
    missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValidationError(f"Missing required columns: {missing_cols}")


def validate_no_nulls(df: pd.DataFrame):
    null_counts = df[REQUIRED_COLUMNS].isnull().sum()
    invalid_cols = null_counts[null_counts > 0]

    if not invalid_cols.empty:
        raise ValidationError(
            f"Null values found in required columns: {invalid_cols.to_dict()}"
        )


def validate_timestamps(df: pd.DataFrame):
    parsed_ts = pd.to_numeric(df["ts"], errors="coerce")
    invalid_count = parsed_ts.isnull().sum()

    if invalid_count > 0:
        raise ValidationError(f"Invalid timestamps found: {invalid_count} rows")


def validate_numeric_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert numeric columns safely and return a cleaned copy
    for downstream range validation.
    """
    df = df.copy()
    numeric_cols = ["co", "humidity", "lpg", "smoke", "temp"]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        invalid_count = df[col].isnull().sum()

        if invalid_count > 0:
            raise ValidationError(
                f"Invalid numeric values found in '{col}': {invalid_count} rows"
            )

    return df


def validate_value_ranges(df: pd.DataFrame):
    for col, (min_val, max_val) in VALUE_RANGES.items():
        out_of_range_mask = (df[col] < min_val) | (df[col] > max_val)
        out_of_range_count = out_of_range_mask.sum()

        if out_of_range_count > 0:
            bad_values = df.loc[out_of_range_mask, col].head(5).tolist()
            raise ValidationError(
                f"Out-of-range values found in '{col}': {out_of_range_count} rows "
                f"(allowed range: {min_val} to {max_val}). Sample bad values: {bad_values}"
            )


def validate_dataframe(df: pd.DataFrame):
    validate_required_columns(df)
    validate_no_nulls(df)
    validate_timestamps(df)

    # important: use numeric-converted dataframe for range validation
    df_numeric = validate_numeric_types(df)
    validate_value_ranges(df_numeric)

    return True