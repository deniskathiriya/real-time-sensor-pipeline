import os
import pandas as pd

from validator import validate_dataframe, ValidationError
from transformer import transform_dataframe
from aggregator import aggregate_sensor_data
from utils import move_file
from db import (
    insert_raw_data,
    insert_aggregates,
    log_file_status,
    file_already_processed
)
from config import logger, PROCESSED_DIR, QUARANTINE_DIR, FAILED_DIR
from retry import retry_on_exception


@retry_on_exception(
    max_attempts=3,
    delay_seconds=2,
    backoff_factor=2,
    allowed_exceptions=(Exception,)
)
def read_csv_with_retry(file_path: str):
    return pd.read_csv(file_path)


def process_file(file_path: str):
    file_name = os.path.basename(file_path)

    try:
        logger.info(f"Processing file: {file_name}")

        # Duplicate check should not block processing if DB is unavailable
        try:
            if file_already_processed(file_name):
                logger.info(f"Skipping already processed file: {file_name}")
                return None, None
        except Exception as e:
            logger.error(
                f"Could not verify duplicate status for {file_name}: {e}. "
                f"Continuing processing."
            )

        # IN_PROGRESS logging should also not block processing
        try:
            log_file_status(file_name, "IN_PROGRESS")
        except Exception as e:
            logger.error(f"Could not log IN_PROGRESS for {file_name}: {e}")

        # Step 1: Read CSV
        df = read_csv_with_retry(file_path)
        logger.info(f"Read file successfully: {file_name} | rows={len(df)}")

        # Step 2: Validate
        validate_dataframe(df)
        logger.info(f"Validation passed: {file_name}")

        # Step 3: Transform
        transformed_df = transform_dataframe(df, file_name)
        logger.info(f"Transformation completed: {file_name} | rows={len(transformed_df)}")

        # Step 4: Aggregate
        aggregated_df = aggregate_sensor_data(transformed_df)
        logger.info(f"Aggregation completed: {file_name} | aggregate_rows={len(aggregated_df)}")

        # Step 5: Raw insert
        insert_raw_data(transformed_df)
        logger.info(f"Inserted raw data into DB: {file_name}")

        # Step 6: Aggregate insert
        insert_aggregates(aggregated_df)
        logger.info(f"Inserted aggregate data into DB: {file_name}")

        # Step 7: SUCCESS log should not block moving file
        try:
            log_file_status(file_name, "SUCCESS")
        except Exception as e:
            logger.error(f"Could not log SUCCESS for {file_name}: {e}")

        # Step 8: Move to processed
        move_file(file_path, PROCESSED_DIR)
        logger.info(f"Moved file to processed/: {file_name}")

        return transformed_df, aggregated_df

    except ValidationError as e:
        logger.error(f"Validation failed for {file_name}: {e}")

        try:
            log_file_status(file_name, "FAILED_VALIDATION", str(e))
        except Exception as log_error:
            logger.error(f"Could not log validation failure for {file_name}: {log_error}")

        try:
            move_file(file_path, QUARANTINE_DIR)
            logger.info(f"Moved invalid file to quarantine/: {file_name}")
        except Exception as move_error:
            logger.error(f"Could not move invalid file {file_name} to quarantine: {move_error}")

        return None, None

    except Exception as e:
        logger.error(f"System failure while processing {file_name}: {e}")

        try:
            log_file_status(file_name, "FAILED_SYSTEM", str(e))
        except Exception as log_error:
            logger.error(f"Could not log system failure for {file_name}: {log_error}")

        try:
            move_file(file_path, FAILED_DIR)
            logger.info(f"Moved system-failed file to failed/: {file_name}")
        except Exception as move_error:
            logger.error(f"Could not move failed file {file_name} to failed/: {move_error}")

        return None, None