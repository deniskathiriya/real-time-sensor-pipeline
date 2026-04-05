import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from retry import retry_on_exception

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(DATABASE_URL)


@retry_on_exception(
    max_attempts=3,
    delay_seconds=2,
    backoff_factor=2,
    allowed_exceptions=(SQLAlchemyError,)
)
def test_connection():
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        print("Database connection successful:", result.scalar())


@retry_on_exception(
    max_attempts=3,
    delay_seconds=2,
    backoff_factor=2,
    allowed_exceptions=(SQLAlchemyError,)
)
def insert_raw_data(df):
    df.to_sql("raw_sensor_data", engine, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into raw_sensor_data")


@retry_on_exception(
    max_attempts=3,
    delay_seconds=2,
    backoff_factor=2,
    allowed_exceptions=(SQLAlchemyError,)
)
def insert_aggregates(df):
    df.to_sql("sensor_aggregates", engine, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into sensor_aggregates")


@retry_on_exception(
    max_attempts=3,
    delay_seconds=2,
    backoff_factor=2,
    allowed_exceptions=(SQLAlchemyError,)
)
def log_file_status(file_name, status, error_message=None):
    query = text("""
        INSERT INTO pipeline_file_log (file_name, status, error_message)
        VALUES (:file_name, :status, :error_message)
        ON CONFLICT (file_name)
        DO UPDATE SET
            status = EXCLUDED.status,
            error_message = EXCLUDED.error_message,
            completed_time = CURRENT_TIMESTAMP
    """)

    with engine.begin() as connection:
        connection.execute(
            query,
            {
                "file_name": file_name,
                "status": status,
                "error_message": error_message
            }
        )

    print(f"Logged file status: {file_name} -> {status}")


@retry_on_exception(
    max_attempts=3,
    delay_seconds=2,
    backoff_factor=2,
    allowed_exceptions=(SQLAlchemyError,)
)
def file_already_processed(file_name):
    query = text("""
        SELECT COUNT(*)
        FROM pipeline_file_log
        WHERE file_name = :file_name
          AND status = 'SUCCESS'
    """)

    with engine.connect() as connection:
        result = connection.execute(query, {"file_name": file_name})
        count = result.scalar()
        return count > 0