from db import test_connection
from watcher import start_polling_watcher
from config import FAILED_DIR, logger, DATA_DIR, PROCESSED_DIR, QUARANTINE_DIR, LOG_DIR

import os


def ensure_directories():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    os.makedirs(FAILED_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)


if __name__ == "__main__":
    logger.info("Starting polling-based sensor pipeline...")

    ensure_directories()

    # Try DB connection, but do not crash the whole pipeline if DB is unavailable
    try:
        test_connection()
        logger.info("Initial database connection check passed.")
    except Exception as e:
        logger.error(f"Initial database connection failed: {e}")
        logger.info("Pipeline will continue running. Database operations will retry during file processing.")


    # Start polling watcher
    start_polling_watcher()