

# 🚀 Real-Time Sensor Data Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue)  
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue)  
![Status](https://img.shields.io/badge/Status-Active-success)  
![License](https://img.shields.io/badge/License-MIT-green)

----------

## 📌 Overview

This project implements a **real-time data pipeline** that continuously monitors a folder for incoming CSV files, validates the data, transforms it into a standardized format, computes aggregate metrics, and stores both raw and processed data in PostgreSQL.

The system simulates a **production-grade sensor ingestion pipeline**, focusing on:

-   data integrity
-   automation
-   fault tolerance
-   scalability
    

----------

## 🧠 Key Features

✔ Continuous folder monitoring (every 5 seconds)  
✔ Data validation (schema, nulls, types, ranges)  
✔ Automatic quarantine of invalid files  
✔ Wide → Long transformation  
✔ Aggregation (min, max, avg, stddev)  
✔ PostgreSQL storage (raw + aggregates)  
✔ Metadata tagging (file, timestamp, source)  
✔ Logging & file tracking  
✔ Retry mechanism with exponential backoff  
✔ Fault-tolerant design (pipeline never crashes)

----------

## 🏗️ Architecture

```text
        CSV Files
           ↓
     Folder Watcher (Polling - 5s)
           ↓
       Validation
      /         \
 Invalid       Valid
   ↓             ↓
Quarantine   Transformation
                  ↓
            Aggregation
                  ↓
              PostgreSQL
                  ↓
               Logging

```

----------

## 📂 Project Structure

```text
DataEngineeringTaskBosch/
│
├── data/                  # Incoming files
├── processed/             # Successfully processed
├── quarantine/            # Invalid files
├── failed/                # System failure files
├── sample_data/           # Main dataset
├── logs/                  # Pipeline logs
├── corrupt_test_data/     # Corrupted test files
├── transfer_corrupt_data/ # file to transfer into corrupt files
├── test data files/       # main dataset to transfer 400+ files
│
├── sql/
│   └── schema.sql       # Database schema
│
├── src/
│   ├── aggregator.py
│   ├── config.py
│   ├── create_corrupt_test_data.py
│   ├── db.py
│   ├── main.py
│   ├── processor.py
│   ├── transformer.py
│   ├── utils.py
│   ├── validator.py
│   └── watcher.py
│
├── .env
├── README.md
└── requirements.txt

```

----------

## 📊 Dataset

IoT sensor dataset from Kaggle.

**Columns used:**

-   `ts` → timestamp
    
-   `device` → sensor ID
    
-   `co`, `humidity`, `lpg`, `smoke`, `temp` → sensor readings
    

To simulate real-world scenarios, corrupted datasets were generated with:

-   missing values
    
-   invalid types
    
-   out-of-range values
    

----------

## 🔄 Pipeline Flow

1.  CSV file placed in `data/`
    
2.  Watcher detects file (every 5 seconds)
    
3.  Data validation performed
    
4.  If invalid → moved to `quarantine/`
    
5.  If valid:
    
    -   Transform data
        
    -   Compute aggregates
        
    -   Store in PostgreSQL
        
    -   Move file to `processed/`
        

----------

## ✅ Validation Rules

-   Required columns must exist
    
-   No null values in key fields
    
-   Numeric fields must be valid numbers
    
-   Value ranges enforced:
    
    -   Temperature: **-50 to 50°C**
        
    -   Humidity: **0 to 100%**
        

Invalid files are moved to `quarantine/` with error logs.

----------

## 🔧 Transformation

### Input (Wide Format)

```
ts, device, co, humidity, lpg, smoke, temp

```

### Output (Long Format)

-   sensor_id
    
-   sensor_type
    
-   reading_value
    
-   reading_unit
    
-   event_timestamp
    
-   file_name
    
-   source_name
    
-   processed_time
    

Each input row expands into multiple rows (one per sensor type).

----------

## 📈 Aggregations

For each file and sensor type:

-   Minimum
    
-   Maximum
    
-   Average
    
-   Standard Deviation
    
-   Count
    

----------

## 🗄️ Database Schema

### Tables

-   `raw_sensor_data` → transformed sensor data
    
-   `sensor_aggregates` → aggregated metrics
    
-   `pipeline_file_log` → file tracking
    

### Design Features

-   Normalized schema
    
-   Indexed columns (sensor_id, timestamp)
    
-   Optimized for querying and scalability
    

----------

## ⚙️ Setup & Run

### 1. Create Virtual Environment

```bash
python -m venv venv
venv\Scripts\activate

```

### 2. Install Dependencies

```bash
pip install -r requirements.txt

```

### 3. Configure Database

Create `.env` file:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=sensor_pipeline
DB_USER=postgres
DB_PASSWORD=your_password

```

### 4. Create Database Tables

Run:

```sql
sql/schema.sql

```

### 5. Run Pipeline

```bash
python src/main.py

```

----------

## 🧪 Testing

### ✅ Valid File

-   Moves to `processed/`
    
-   Data inserted into DB
    

### ❌ Invalid File

-   Moves to `quarantine/`
    
-   Error logged
    

### ⚠️ System Failure (DB Down)

-   Retry mechanism triggered
    
-   File moved to `failed/`
    
-   Pipeline continues running
    

----------

## 🧨 Generate Corrupted Data

```bash
python src/create_corrupt_test_data.py

```

----------

## 📝 Logs

Location:

```text
logs/pipeline.log

```

Tracks:

-   file processing
    
-   validation errors
    
-   retries
    
-   database operations
    

----------

## 🔍 Example Queries

```sql
SELECT * FROM raw_sensor_data LIMIT 10;

SELECT * FROM sensor_aggregates LIMIT 10;

SELECT * FROM pipeline_file_log ORDER BY id DESC;

```

----------

## 🛡️ Fault Tolerance

-   Retry mechanism with exponential backoff
    
-   Logging of all failures
    
-   Separation of failure types:
    
    -   `quarantine/` → data issues
        
    -   `failed/` → system failures
        
-   Pipeline continues running even if DB is unavailable
    

----------

## 🔁 Recovery Strategy

-   Failed files stored in `failed/`
    
-   Can be reprocessed by moving back to `data/`
    
-   Ensures no data loss
    

----------

## 📈 Scalability

Current system:

-   Local execution (pandas + PostgreSQL)
    

Production scaling:

-   Apache Kafka / PubSub (event-driven ingestion)
    
-   Apache Spark (distributed processing)
    
-   AWS S3 / Cloud Storage
    
-   Apache Airflow (workflow orchestration)
    
-   Cloud databases (BigQuery, Snowflake)
    

----------

## ⚠️ Limitations

-   Single-node processing
    
-   Polling-based ingestion
    
-   Manual reprocessing of failed files
    

----------

## 🔮 Future Improvements

-   Automatic retry of failed files
    
-   Docker & Kubernetes deployment
    
-   Real-time dashboards
    
-   Alerting system
    
-   Streaming architecture
    

----------

## ⚖️ Data Ethics

-   Uses publicly available dataset
    
-   No personal or sensitive data
    
-   Complies with data usage policies
    

----------

## 👨‍💻 Author

Denisbhai Kathiriya

----------

## ⭐ Summary

A modular, fault-tolerant, real-time data pipeline featuring:

-   automated ingestion
    
-   validation and transformation
    
-   aggregation and storage
    
-   retry and recovery mechanisms
    
-   scalability-ready architecture
    

----------


