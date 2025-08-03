#!/bin/bash

export AIRFLOW_UID=$(id -u)

docker compose up -d

echo "Starting services..."
echo "Airflow UI: http://localhost:8080 (admin/admin)"
echo "MySQL: localhost:3306"
echo "ClickHouse: localhost:8123"