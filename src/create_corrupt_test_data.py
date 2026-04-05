import random
from pathlib import Path

import pandas as pd


INPUT_FOLDER = "transfer_corrupt_data"
OUTPUT_FOLDER = "corrupt_test_data"
FILE_PATTERN = "*.csv"

RANDOM_SEED = 42
NULL_CORRUPTION_RATE = 0.03
TYPE_CORRUPTION_RATE = 0.03
RANGE_CORRUPTION_RATE = 0.03

KEY_FIELDS = ["device", "ts", "co", "humidity", "lpg", "smoke", "temp"]
NUMERIC_FIELDS = ["co", "humidity", "lpg", "smoke", "temp"]


random.seed(RANDOM_SEED)


def ensure_output_folder(folder_path: str) -> None:
    Path(folder_path).mkdir(parents=True, exist_ok=True)


def pick_random_indices(df: pd.DataFrame, fraction: float) -> list[int]:
    if df.empty or fraction <= 0:
        return []

    count = max(1, int(len(df) * fraction))
    count = min(count, len(df))
    return random.sample(list(df.index), count)


def corrupt_null_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    indices = pick_random_indices(df, NULL_CORRUPTION_RATE)

    for idx in indices:
        col = random.choice(KEY_FIELDS)
        df.at[idx, col] = None

    return df


def corrupt_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Introduce invalid data types by first converting target columns to object.
    """
    df = df.copy()

    target_cols = ["ts"] + NUMERIC_FIELDS
    for col in target_cols:
        df[col] = df[col].astype("object")

    indices = pick_random_indices(df, TYPE_CORRUPTION_RATE)

    for idx in indices:
        col = random.choice(target_cols)

        if col == "ts":
            df.at[idx, col] = random.choice(["invalid_ts", "not_a_timestamp", "abc123"])
        else:
            df.at[idx, col] = random.choice(["bad_value", "xyz", "NaN_text"])

    return df


def corrupt_value_ranges(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    indices = pick_random_indices(df, RANGE_CORRUPTION_RATE)

    for idx in indices:
        col = random.choice(NUMERIC_FIELDS)

        if col == "temp":
            df.at[idx, col] = random.choice([-200, 120, 999])
        elif col == "humidity":
            df.at[idx, col] = random.choice([-20, 150, 300])
        elif col == "co":
            df.at[idx, col] = random.choice([-1, 500, 9999])
        elif col == "lpg":
            df.at[idx, col] = random.choice([-5, 500, 9999])
        elif col == "smoke":
            df.at[idx, col] = random.choice([-3, 500, 9999])

    return df


def add_corruption_summary(original_df: pd.DataFrame, corrupted_df: pd.DataFrame) -> dict:
    changed_cells = (original_df.astype(str) != corrupted_df.astype(str)).sum().sum()

    return {
        "original_rows": len(original_df),
        "corrupted_rows": len(corrupted_df),
        "changed_cells": int(changed_cells),
    }


def corrupt_file(input_path: Path, output_folder: str) -> None:
    print(f"Processing: {input_path.name}")

    df = pd.read_csv(input_path)
    original_df = df.copy()

    # Choose ONE corruption type for this file
    corruption_type = random.choice(["null", "type", "range"])

    print(f"Applying corruption type: {corruption_type}")

    if corruption_type == "null":
        df = corrupt_null_values(df)

    elif corruption_type == "type":
        df = corrupt_data_types(df)

    elif corruption_type == "range":
        df = corrupt_value_ranges(df)

    # Save with clear naming
    output_path = Path(output_folder) / f"{corruption_type}_corrupt_{input_path.name}"
    df.to_csv(output_path, index=False)

    summary = add_corruption_summary(original_df, df)

    print(
        f"Saved: {output_path.name} | "
        f"type={corruption_type} | "
        f"rows={summary['corrupted_rows']} | "
        f"changed_cells={summary['changed_cells']}"
    )

def main() -> None:
    input_folder = Path(INPUT_FOLDER)
    output_folder = Path(OUTPUT_FOLDER)

    if not input_folder.exists():
        raise FileNotFoundError(f"Input folder not found: {input_folder.resolve()}")

    ensure_output_folder(str(output_folder))

    csv_files = list(input_folder.glob(FILE_PATTERN))

    if not csv_files:
        print(f"No CSV files found in: {input_folder.resolve()}")
        return

    for csv_file in csv_files:
        try:
            corrupt_file(csv_file, str(output_folder))
        except Exception as e:
            print(f"Failed to process {csv_file.name}: {e}")

    print(f"\nDone. Corrupted test files are in: {output_folder.resolve()}")


if __name__ == "__main__":
    main()