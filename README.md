# Data Pipeline

Coal mining data processing pipeline with Airflow orchestration.

## Setup

```bash
docker compose up -d
```

Access Airflow at http://localhost:8080 (admin/admin)

## Structure

- `scripts/` - Data processing scripts
- `airflow/dags/` - Airflow DAG definitions  
- `data/` - Sample data files
- `clickhouse/` - Database init scripts

## Pipeline

1. **Bronze** - Raw data ingestion (every 6 hours)
2. **Silver** - Data cleaning and transformation
3. **Gold** - Business metrics and analytics

## Services

- Airflow: localhost:8080
- MySQL: localhost:3306
- ClickHouse: localhost:8123

## Usage

Enable DAGs in Airflow UI:
- bronze_layer_pipeline
- silver_layer_pipeline  
- gold_layer_pipeline

Stop: `docker compose down`