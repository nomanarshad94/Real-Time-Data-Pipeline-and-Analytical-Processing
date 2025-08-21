# Advanced Real-Time Data Pipeline and Analytical Processing

A scalable real-time data pipeline that monitors a directory for incoming sensor data, validates and transforms it, computes aggregated metrics, and stores the results in a relational database for further analysis.  
This project was developed as part of a **
task for Bosch** to demonstrate capabilities in data engineering, scalability, and analytical processing.

---

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/database-PostgreSQL-316192.svg)](https://www.postgresql.org/)
[![License: Apache License 2.0](https://img.shields.io/badge/license-Apache%20License%202.0-blue)](https://opensource.org/license/apache-2-0)
---

## 📌 Objective
- Monitor a directory for incoming sensor data in CSV format.  
- Validate and transform raw sensor data to ensure quality and consistency.  
- Quarantine invalid data and maintain detailed error logs.  
- Compute aggregated metrics (min, max, average, standard deviation) for each sensor type.  
- Store both raw and aggregated data in a relational database (MySQL/PostgreSQL).  
- Ensure scalability, fault tolerance, and automation for continuous processing.  

---


## ⚙️ Task Description & Features

### 1. Data Ingestion & Monitoring
- Monitors a `data/` folder for new CSV files every 5–10 seconds.  
- Automatically triggers processing on detection of new files.  

### 2. Data Source
- **IoT Environmental Dataset**: Uses Kaggle's IoT-based environmental dataset  
- **Data Schema**: 
  ```
  timestamp, location_id, temperature_celsius, humidity_percent, 
  air_quality_index, noise_level_db, lighting_lux, crowd_density, 
  stress_level, sleep_hours, mood_score, mental_health_status
  ```
- **Data Types**: Environmental metrics and mental health indicators
- **Location-based**: Data organized by location_id for spatial analysis
- **Real-world Simulation**: Random missing or corrupt data points are introduced to mimic real-world issues.

### 3. Data Validation & Transformation
- **Validation checks**:  
  - No nulls in key fields (`sensor_id`, `timestamp`, `reading`).  
  - Correct data types (numeric for readings, valid datetime format).  
  - Acceptable ranges for sensor values (e.g., temperature: -50°C to 50°C).  
- Invalid records moved to a `quarantine/` folder with error logs.  
- Valid data is normalized and standardized (units, formats).  

### 4. Data Analysis
- Aggregates metrics per sensor type:  
  - Minimum, Maximum, Average, Standard Deviation.  
- Appends metadata such as data source, timestamp, and file name.  

### 5. Database Storage
- **Relational Database Schema** (MySQL/PostgreSQL):  
  - `raw_data` table: Stores validated sensor readings.  
  - `aggregates` table: Stores computed metrics.  
- Optimized for querying with indexes and normalized schema design.  

### 6. Scalability
- Designed for future scalability with distributed frameworks:  
  - Apache Kafka, Spark, or Flink for high-throughput streaming.  
  - Cloud-native services like AWS Lambda, Kinesis, or Google Pub/Sub.  
- Horizontal scaling strategy outlined in documentation.  

### 7. Automation & Fault Tolerance
- Continuous pipeline execution without manual intervention.  
- Fault tolerance with logging, retries, and recovery for failed operations.  
- Ensures no data loss even in case of outages.  

---

## 🛠️ Tech Stack
- **Language:** Python  
- **Libraries:** Pandas, SQLAlchemy, Watchdog (file monitoring), Logging  
- **Database:** MySQL (preferred) or PostgreSQL  
- **Optional for Scale:** Apache Kafka, Apache Spark, Airflow, Docker  

---


## Steps to create a real-time data processing pipeline project structure (brainstorming)

1. Set a project directory structure for a real-time data processing pipeline.
2. Include directories for data, quarantine, processed files, logs, source code, tests, and configuration.
3. Create data_connector to ingest raw data from kaggle and store it in data directory


## 📂 Project Structure
```plaintext
real-time-pipeline/
├── data/                           # Monitored raw data folder
├── quarantine/                     # Invalid files
├── processed/                      # Archive processed files
├── logs/                           # Error and process logs
├── src/
│   └──  utills/    
│       └── filesystem_helper.py    # filesystem helper functions
│   ├── monitor.py                  # File monitoring
│   ├── validator.py                # Data validation
│   ├── transformer.py              # Data transformation
│   ├── analyzer.py                 # Statistical analysis
│   ├── database.py                 # Database operations
│   └── config.py                   # Configuration
├── tests/
├── requirements.txt                # Python dependencies
├── LICENSE                         # License file
├── NOTICE                          # Notice file related to License
└── README.md                       # Project documentation
```


---

## 🚀 Setup & Usage

1. **Clone Repository**
   ```bash
   git clone https://github.com/nomanarshad94/Real-Time-Data-Pipeline-and-Analytical-Processing.git
   cd Real-Time-Data-Pipeline-and-Analytical-Processing
    ```
2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure Database**
   ```bash
   # PostgreSQL setup
   createdb iot_sensor_db
   ```
    - Update src/config.py with database credentials and monitored directory path.

4. **Run the Pipeline**
   ```bash
   # Process single file
   python src/main.py --file path/to/data.csv
   
   # Start continuous monitoring
   python src/main.py

   # Check pipeline status
   python src/main.py --status
   ```

## 🔧 Configuration

The pipeline is configured through `src/config.py` and environment variables:

```python
# Key Configuration Parameters
DATA_DIR = "data/"                    # Input directory
QUARANTINE_DIR = "quarantine/"        # Invalid files
PROCESSED_DIR = "processed/"          # Processed files archive
LOGS_DIR = "logs/"                    # Log files

# Validation Ranges (IoT Environmental Data)
TEMP_MIN = -50.0                     # Minimum temperature (°C)
TEMP_MAX = 60.0                      # Maximum temperature (°C)
HUMIDITY_MIN = 0.0                   # Minimum humidity (%)
HUMIDITY_MAX = 100.0                 # Maximum humidity (%)
AIR_QUALITY_MIN = 0                  # Minimum AQI
AIR_QUALITY_MAX = 500                # Maximum AQI
STRESS_LEVEL_MIN = 0                 # Minimum stress level
STRESS_LEVEL_MAX = 100               # Maximum stress level
SLEEP_HOURS_MIN = 0.0                # Minimum sleep hours
SLEEP_HOURS_MAX = 24.0               # Maximum sleep hours
MOOD_SCORE_MIN = 0                   # Minimum mood score
MOOD_SCORE_MAX = 5                   # Maximum mood score

# Pipeline Settings
MAX_RETRIES = 3                      # Failed operation retries
RETRY_DELAY = 5                      # Retry delay (seconds)
POLLING_INTERVAL = 10                # File monitoring interval
```

## 📊 Example SQL Queries for Data Analysis

### Raw Environmental Data Analysis
```sql
-- Daily record counts by location
SELECT DATE(timestamp) AS day, location_id, COUNT(*) as record_count
FROM raw_sensor_data
GROUP BY day, location_id 
ORDER BY day, location_id;
```

```sql
-- Average environmental conditions by location
SELECT location_id,
    AVG(temperature_celsius) AS avg_temp,
    AVG(humidity_percent) AS avg_humidity,
    AVG(air_quality_index) AS avg_aqi,
    AVG(noise_level_db) AS avg_noise,
    AVG(lighting_lux) AS avg_lighting
FROM raw_sensor_data
GROUP BY location_id;
```

```sql
-- Mental health indicators by location
SELECT location_id,
    AVG(stress_level) AS avg_stress,
    AVG(sleep_hours) AS avg_sleep,
    AVG(mood_score) AS avg_mood,
    SUM(CASE WHEN mental_health_status = 1 THEN 1 ELSE 0 END) AS mental_health_issues,
    COUNT(*) AS total_records
FROM raw_sensor_data
GROUP BY location_id;
```

```sql
-- Environmental conditions affecting mental health
SELECT 
    CASE 
        WHEN air_quality_index < 50 THEN 'Good'
        WHEN air_quality_index < 100 THEN 'Moderate'
        WHEN air_quality_index < 150 THEN 'Unhealthy for Sensitive'
        ELSE 'Unhealthy'
    END AS air_quality_category,
    AVG(stress_level) AS avg_stress,
    AVG(mood_score) AS avg_mood,
    COUNT(*) as record_count
FROM raw_sensor_data
GROUP BY air_quality_category;
```

### Aggregated Metrics Analysis
```sql
-- Latest aggregated metrics by location
SELECT am.location_id, am.metric_name, 
    am.min_value, am.max_value, am.avg_value, am.std_value
FROM aggregated_metrics am
INNER JOIN (
    SELECT location_id, metric_name, MAX(timestamp) as latest_time
    FROM aggregated_metrics
    GROUP BY location_id, metric_name
) latest ON am.location_id = latest.location_id 
    AND am.metric_name = latest.metric_name 
    AND am.timestamp = latest.latest_time;
```

### Time-based Analysis
```sql
-- Hourly patterns of environmental and mental health data
SELECT 
    EXTRACT(HOUR FROM timestamp) AS hour_of_day,
    AVG(temperature_celsius) AS avg_temp,
    AVG(stress_level) AS avg_stress,
    AVG(mood_score) AS avg_mood,
    COUNT(*) AS record_count
FROM raw_sensor_data
GROUP BY hour_of_day
ORDER BY hour_of_day;
```
  
## 🚀 Scalability Architecture

### Current Implementation Limitations
- Single-threaded file processing
- Local file system monitoring
- Single database connection
- Memory-based data processing

### Horizontal Scaling Strategy

#### 1. Distributed Message Streaming
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Kafka     │    │   Kafka     │    │   Kafka     │
│  Producer   │───▶│   Cluster   │───▶│  Consumer   │
│             │    │             │    │   Groups    │
└─────────────┘    └─────────────┘    └─────────────┘
```
- **Apache Kafka**: Handle millions of files/day with distributed partitioning
- **Topics**: Separate topics for raw data, validated data, and aggregated metrics
- **Partitioning**: Location-based partitioning for parallel processing
- **Consumer Groups**: Multiple processing nodes for high throughput

#### 2. Distributed Processing Framework
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  PySpark    │    │  PySpark    │    │  PySpark    │
│ Streaming   │    │ Streaming   │    │ Streaming   │
│  Worker 1   │    │  Worker 2   │    │  Worker N   │
└─────────────┘    └─────────────┘    └─────────────┘
```
- **PySpark Streaming**: Real-time processing with micro-batching
- **Structured Streaming**: Fault-tolerant stream processing
- **Auto-scaling**: Dynamic resource allocation based on load
- **State Management**: Exactly-once processing guarantees

#### 3. Cloud-Native Architecture
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   AWS S3    │    │   Lambda    │    │  RDS/Aurora │
│   Events    │───▶│ Functions   │───▶│  Database   │
│             │    │             │    │   Cluster   │
└─────────────┘    └─────────────┘    └─────────────┘
```
- **AWS Lambda**: Serverless processing with automatic scaling
- **S3 Event Triggers**: Process files as they arrive
- **SQS/SNS**: Reliable message queuing and notifications with dead letter queue
- **RDS Aurora**: Auto-scaling database with read replicas

### Performance Optimizations

#### Data Processing
- **Parallel Processing**: Multi-threading for CPU-intensive operations
- **Vectorized Operations**: NumPy/Pandas optimizations
- **Memory Management**: Chunked processing for large files
- **Caching**: Redis for frequently accessed aggregations

#### Database Optimizations
- **Read Replicas**: Separate read/write workloads
- **Partitioning**: Time-based table partitioning
- **Connection Pooling**: Efficient database connection management
- **Materialized Views**: Pre-computed aggregations

#### Monitoring & Observability
- **Prometheus/Grafana**: Real-time metrics and alerting
- **Distributed Tracing**: Jaeger for end-to-end visibility
- **Log Aggregation**: ELK stack for centralized logging
- **Health Checks**: Automated failure detection and recovery

### Target Performance Metrics
- **Throughput**: 1M+ files per day
- **Latency**: < 10 seconds end-to-end processing
- **Availability**: 99.9% uptime
- **Scalability**: Linear scaling with added resources

## 📈 Future Enhancements
- **Real-time Dashboard**: Streamlit/Grafana for live monitoring
- **Machine Learning**: Predictive analytics for mental health trends
- **API Gateway**: RESTful APIs for external integrations
- **Data Lake**: Apache Iceberg for historical data analytics
- **Edge Computing**: IoT edge processing for reduced latency

## 📝 License
This project is licensed under the MIT License. See the LICENSE file for details.

## 👤 Author
**Noman Arshad** <br>
[LinkedIn](https://www.linkedin.com/in/noman-arshad) | [GitHub](https://github.com/nomanarshad94/)