-- =================================================================
-- Real-Time IoT Data Pipeline - Database Schema
-- PostgreSQL Schema for Raw Data and Aggregated Metrics
-- =================================================================
-- Drop existing tables if they exist (for fresh setup)
DROP TABLE IF EXISTS aggregated_metrics CASCADE;
DROP TABLE IF EXISTS raw_sensor_data CASCADE;

-- =================================================================
-- TABLE 1: raw_sensor_data
-- Purpose: Store validated IoT environmental and mental health data
-- =================================================================

CREATE TABLE raw_sensor_data (
    -- Primary Key
    id                    SERIAL PRIMARY KEY,
    
    -- Core IoT Identifiers
    location_id           VARCHAR(50) NOT NULL,
    timestamp            TIMESTAMP NOT NULL,
    
    -- Environmental Metrics
    temperature_celsius   REAL,
    humidity_percent      REAL,
    air_quality_index     INTEGER,
    noise_level_db        REAL,
    lighting_lux          REAL,
    crowd_density         INTEGER,
    
    -- Mental Health Indicators
    stress_level          INTEGER,
    sleep_hours           REAL,
    mood_score            REAL,
    mental_health_status  INTEGER,  -- 0: Normal, 1: Issue
    
    -- Processing Metadata
    file_name             VARCHAR(255) NOT NULL,
    data_source           VARCHAR(100) NOT NULL,
    processed_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =================================================================
-- TABLE 2: aggregated_metrics
-- Purpose: Store computed statistical metrics per location and metric type
-- =================================================================

CREATE TABLE aggregated_metrics (
    -- Primary Key
    id                    SERIAL PRIMARY KEY,
    
    -- Grouping Identifiers
    location_id           VARCHAR(50) NOT NULL,
    metric_name           VARCHAR(50) NOT NULL,  -- temperature_celsius, humidity_percent, etc.
    
    -- Statistical Metrics
    min_value             REAL,
    max_value             REAL,
    avg_value             REAL,
    std_value             REAL,
    count                 INTEGER,
    
    -- Processing Metadata
    data_source           VARCHAR(100) NOT NULL,
    file_name             VARCHAR(255) NOT NULL,
    timestamp            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =================================================================
-- INDEXES for Query Performance Optimization
-- Essential indexes only - optimized for common query patterns
-- =================================================================

-- Raw Data Indexes
CREATE INDEX idx_raw_location_id ON raw_sensor_data(location_id);
CREATE INDEX idx_raw_timestamp ON raw_sensor_data(timestamp);

-- Aggregated Data Indexes  
CREATE INDEX idx_agg_location_metric ON aggregated_metrics(location_id, metric_name);

-- =================================================================
-- SAMPLE QUERIES for Data Analysis
-- =================================================================

-- Query 1: Latest environmental conditions by location
/*
SELECT 
    location_id,
    AVG(temperature_celsius) as avg_temp,
    AVG(humidity_percent) as avg_humidity,
    AVG(air_quality_index) as avg_aqi,
    AVG(stress_level) as avg_stress,
    COUNT(*) as record_count
FROM raw_sensor_data 
WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY location_id
ORDER BY avg_stress DESC;
*/

-- Query 2: Mental health correlation with environmental factors
/*
SELECT 
    CASE 
        WHEN air_quality_index < 50 THEN 'Good'
        WHEN air_quality_index < 100 THEN 'Moderate'
        WHEN air_quality_index < 150 THEN 'Unhealthy for Sensitive'
        ELSE 'Unhealthy'
    END as air_quality_category,
    AVG(stress_level) as avg_stress,
    AVG(mood_score) as avg_mood,
    SUM(CASE WHEN mental_health_status = 1 THEN 1 ELSE 0 END) as health_issues,
    COUNT(*) as total_records
FROM raw_sensor_data
GROUP BY air_quality_category
ORDER BY avg_stress DESC;
*/

-- Query 3: Aggregated metrics summary
/*
SELECT 
    am.location_id,
    am.metric_name,
    am.min_value,
    am.max_value,
    am.avg_value,
    am.std_value,
    am.timestamp
FROM aggregated_metrics am
INNER JOIN (
    SELECT location_id, metric_name, MAX(timestamp) as latest_time
    FROM aggregated_metrics
    GROUP BY location_id, metric_name
) latest ON am.location_id = latest.location_id 
    AND am.metric_name = latest.metric_name 
    AND am.timestamp = latest.latest_time
ORDER BY am.location_id, am.metric_name;
*/


-- =================================================================
-- SCHEMA VALIDATION and STATISTICS
-- =================================================================

-- Verify schema creation
/*
SELECT table_name, column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_schema = 'public' 
AND table_name IN ('raw_sensor_data', 'aggregated_metrics')
ORDER BY table_name, ordinal_position;
*/

-- Check index creation
/*
SELECT schemaname, tablename, indexname, indexdef 
FROM pg_indexes 
WHERE tablename IN ('raw_sensor_data', 'aggregated_metrics')
ORDER BY tablename, indexname;
*/

-- =================================================================
-- END OF SCHEMA
-- =================================================================