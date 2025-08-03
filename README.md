# Coal Mining Data Pipeline

A data engineering pipeline for processing coal mining sensor data using a medallion architecture (Bronze → Silver → Gold).

## Quick Start

1. **Start the environment:**
   ```bash
   docker compose up -d
   ```

2. **Run the pipeline:**
   ```bash
   # Full pipeline
   ./run_scripts.sh full
   
   # Individual layers
   ./run_scripts.sh bronze-all
   ./run_scripts.sh silver-all  
   ./run_scripts.sh gold-all
   ```

3. **Check status:**
   ```bash
   ./run_scripts.sh status
   ```

## Pipeline Components

### Bronze Layer (Raw Data)
- `bronze_equipment.py` - Equipment sensor data from CSV
- `bronze_production.py` - Production logs from MySQL
- `bronze_weather.py` - Weather data from API

### Silver Layer (Cleaned Data)  
- `silver_equipment.py` - Validated equipment metrics
- `silver_production.py` - Processed production data
- `silver_weather.py` - Cleaned weather data

### Gold Layer (Analytics Ready)
- `gold_daily_metrics.py` - Daily aggregated metrics
- `gold_mine_performance.py` - Mine performance analytics

## Environment Variables

Set these in docker-compose.yml or .env:

```bash
MYSQL_HOST=mysql
MYSQL_DATABASE=coal_mining
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_DATABASE=coal_mining_dw
```

## Data Sources

- **Equipment sensors:** `data/equipment_sensors.csv`
- **Production logs:** MySQL database
- **Weather data:** Open-Meteo API

## Troubleshooting

- Check container logs: `docker compose logs [service]`
- Verify connections: `./run_scripts.sh status`
- View pipeline logs: `docker exec etl-runner ls /app/logs/`