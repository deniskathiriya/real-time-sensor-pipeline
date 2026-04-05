CREATE TABLE IF NOT EXISTS raw_sensor_data (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(100) NOT NULL,
    sensor_type VARCHAR(100) NOT NULL,
    reading_value DOUBLE PRECISION NOT NULL,
    reading_unit VARCHAR(50),
    event_timestamp TIMESTAMP NOT NULL,
    source_name VARCHAR(255),
    location VARCHAR(255),
    file_name VARCHAR(255) NOT NULL,
    ingestion_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_sensor_id
ON raw_sensor_data(sensor_id);

CREATE INDEX IF NOT EXISTS idx_raw_sensor_type
ON raw_sensor_data(sensor_type);

CREATE INDEX IF NOT EXISTS idx_raw_event_timestamp
ON raw_sensor_data(event_timestamp);

CREATE INDEX IF NOT EXISTS idx_raw_file_name
ON raw_sensor_data(file_name);


CREATE TABLE IF NOT EXISTS sensor_aggregates (
    id BIGSERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    source_name VARCHAR(255),
    sensor_type VARCHAR(100) NOT NULL,
    min_value DOUBLE PRECISION NOT NULL,
    max_value DOUBLE PRECISION NOT NULL,
    avg_value DOUBLE PRECISION NOT NULL,
    stddev_value DOUBLE PRECISION NOT NULL,
    record_count INT NOT NULL,
    aggregation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agg_sensor_type
ON sensor_aggregates(sensor_type);

CREATE INDEX IF NOT EXISTS idx_agg_file_name
ON sensor_aggregates(file_name);


CREATE TABLE IF NOT EXISTS pipeline_file_log (
    id BIGSERIAL PRIMARY KEY,
    file_name VARCHAR(255) UNIQUE NOT NULL,
    status VARCHAR(50) NOT NULL,
    error_message TEXT,
    detected_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_time TIMESTAMP
);