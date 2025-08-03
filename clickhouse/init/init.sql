-- Create database
CREATE DATABASE IF NOT EXISTS coal_mining_dw;
USE coal_mining_dw;

-- Bronze Layer Tables
CREATE TABLE IF NOT EXISTS bronze_production_logs (
    log_id Int32,
    date Date,
    mine_id Int32,
    shift String,
    tons_extracted Float64,
    quality_grade Float64,
    data_quality_flag String,
    ingestion_timestamp DateTime DEFAULT now(),
    source String DEFAULT 'mysql'
) ENGINE = MergeTree()
ORDER BY (date, mine_id, shift);

CREATE TABLE IF NOT EXISTS bronze_equipment_sensors (
    timestamp DateTime,
    equipment_id String,
    status String,
    fuel_consumption Float64,
    maintenance_alert Bool,
    ingestion_timestamp DateTime DEFAULT now(),
    source String DEFAULT 'csv'
) ENGINE = MergeTree()
ORDER BY (timestamp, equipment_id);

CREATE TABLE IF NOT EXISTS bronze_weather_data (
    date Date,
    latitude Float64,
    longitude Float64,
    temperature_mean Float64,
    precipitation_sum Float64,
    ingestion_timestamp DateTime DEFAULT now(),
    source String DEFAULT 'api'
) ENGINE = MergeTree()
ORDER BY date;

-- Silver Layer Tables
CREATE TABLE IF NOT EXISTS silver_production_daily (
    date Date,
    mine_id Int32,
    total_tons_extracted Float64,
    avg_quality_grade Float64,
    shifts_count Int32,
    quality_score String,
    anomaly_flag Bool,
    processed_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (date, mine_id);

CREATE TABLE IF NOT EXISTS silver_equipment_metrics (
    date Date,
    equipment_id String,
    total_operational_hours Float64,
    utilization_percentage Float64,
    avg_fuel_consumption Float64,
    maintenance_alerts_count Int32,
    fuel_efficiency Float64,
    processed_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (date, equipment_id);

CREATE TABLE IF NOT EXISTS silver_weather_clean (
    date Date,
    temperature_mean Float64,
    precipitation_sum Float64,
    weather_category String,
    is_rainy_day Bool,
    processed_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY date;

-- Gold Layer Tables
CREATE TABLE IF NOT EXISTS gold_daily_production_metrics (
    date Date,
    total_production_daily Float64,
    average_quality_grade Float64,
    equipment_utilization Float64,
    fuel_efficiency Float64,
    weather_impact_score Float64,
    operational_mines_count Int32,
    total_equipment_count Int32,
    quality_performance String,
    production_trend String,
    processed_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY date;

CREATE TABLE IF NOT EXISTS gold_mine_performance (
    date Date,
    mine_id Int32,
    mine_name String,
    production_tons Float64,
    quality_grade Float64,
    performance_rank Int32,
    efficiency_score Float64,
    weather_correlation Float64,
    processed_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (date, mine_id);

-- Data Quality Tracking
CREATE TABLE IF NOT EXISTS data_quality_log (
    check_timestamp DateTime DEFAULT now(),
    layer String,
    table_name String,
    check_type String,
    status String,
    error_count Int32,
    total_records Int32,
    error_details String
) ENGINE = MergeTree()
ORDER BY check_timestamp;