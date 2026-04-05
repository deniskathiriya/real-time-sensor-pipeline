import os
import time

from processor import process_file
from config import logger, DATA_DIR
from db import file_already_processed


POLL_INTERVAL = 5  # seconds


def start_polling_watcher():
    """
    Continuously scan the data folder for new CSV files every few seconds.

    If DB is unavailable, still attempt processing so the processor can
    handle retries and move failed files to the failed/ folder.
    """
    logger.info(f"Starting polling watcher (interval={POLL_INTERVAL}s) on folder: {DATA_DIR}")

    while True:
        try:
            current_files = os.listdir(DATA_DIR)

            for file_name in current_files:
                file_path = os.path.join(DATA_DIR, file_name)

                if not file_name.lower().endswith(".csv"):
                    continue

                if not os.path.isfile(file_path):
                    continue

                should_process = True

                # Try duplicate check, but do not block processing if DB is down
                try:
                    if file_already_processed(file_name):
                        logger.info(f"Skipping already processed file: {file_name}")
                        should_process = False
                except Exception as e:
                    logger.error(
                        f"Could not check DB status for {file_name}: {e}. "
                        f"Will still attempt processing."
                    )

                if not should_process:
                    continue

                logger.info(f"New file detected: {file_name}")

                try:
                    process_file(file_path)
                except Exception as e:
                    logger.error(f"Error processing file {file_name}: {e}")

            time.sleep(POLL_INTERVAL)

        except Exception as e:
            logger.error(f"Watcher error: {e}")
            time.sleep(POLL_INTERVAL)