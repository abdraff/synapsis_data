#!/bin/bash

# Individual script runners for each layer
# These scripts can be run independently for testing or selective execution

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

# Create logs directory if it doesn't exist
mkdir -p ./logs

print_status $BLUE "=== Coal Mining Data Pipeline - Individual Script Runners ==="

# Bronze Layer Scripts
print_status $YELLOW "\n🥉 BRONZE LAYER SCRIPTS"

run_bronze_production() {
    print_status $BLUE "Running Bronze Production Pipeline..."
    docker exec etl-runner python3 /app/scripts/bronze_production.py
    if [ $? -eq 0 ]; then
        print_status $GREEN "✅ Bronze Production completed successfully"
    else
        print_status $RED "❌ Bronze Production failed"
        return 1
    fi
}

run_bronze_equipment() {
    print_status $BLUE "Running Bronze Equipment Pipeline..."
    docker exec etl-runner python3 /app/scripts/bronze_equipment.py
    if [ $? -eq 0 ]; then
        print_status $GREEN "✅ Bronze Equipment completed successfully"
    else
        print_status $RED "❌ Bronze Equipment failed"
        return 1
    fi
}

run_bronze_weather() {
    print_status $BLUE "Running Bronze Weather Pipeline..."
    docker exec etl-runner python3 /app/scripts/bronze_weather.py
    if [ $? -eq 0 ]; then
        print_status $GREEN "✅ Bronze Weather completed successfully"
    else
        print_status $RED "❌ Bronze Weather failed"
        return 1
    fi
}

# Silver Layer Scripts
print_status $YELLOW "\n🥈 SILVER LAYER SCRIPTS"

run_silver_production() {
    print_status $BLUE "Running Silver Production Pipeline..."
    docker exec etl-runner python3 /app/scripts/silver_production.py
    if [ $? -eq 0 ]; then
        print_status $GREEN "✅ Silver Production completed successfully"
    else
        print_status $RED "❌ Silver Production failed"
        return 1
    fi
}

run_silver_equipment() {
    print_status $BLUE "Running Silver Equipment Pipeline..."
    docker exec etl-runner python3 /app/scripts/silver_equipment.py
    if [ $? -eq 0 ]; then
        print_status $GREEN "✅ Silver Equipment completed successfully"
    else
        print_status $RED "❌ Silver Equipment failed"
        return 1
    fi
}

run_silver_weather() {
    print_status $BLUE "Running Silver Weather Pipeline..."
    docker exec etl-runner python3 /app/scripts/silver_weather.py
    if [ $? -eq 0 ]; then
        print_status $GREEN "✅ Silver Weather completed successfully"
    else
        print_status $RED "❌ Silver Weather failed"
        return 1
    fi
}

# Gold Layer Scripts
print_status $YELLOW "\n🥇 GOLD LAYER SCRIPTS"

run_gold_daily_metrics() {
    print_status $BLUE "Running Gold Daily Metrics Pipeline..."
    docker exec etl-runner python3 /app/scripts/gold_daily_metrics.py
    if [ $? -eq 0 ]; then
        print_status $GREEN "✅ Gold Daily Metrics completed successfully"
    else
        print_status $RED "❌ Gold Daily Metrics failed"
        return 1
    fi
}

run_gold_mine_performance() {
    print_status $BLUE "Running Gold Mine Performance Pipeline..."
    docker exec etl-runner python3 /app/scripts/gold_mine_performance.py
    if [ $? -eq 0 ]; then
        print_status $GREEN "✅ Gold Mine Performance completed successfully"
    else
        print_status $RED "❌ Gold Mine Performance failed"
        return 1
    fi
}

# Complete Layer Functions
run_bronze_layer() {
    print_status $YELLOW "\n🥉 Running Complete Bronze Layer..."
    run_bronze_production && run_bronze_equipment && run_bronze_weather
    if [ $? -eq 0 ]; then
        print_status $GREEN "✅ Bronze Layer completed successfully"
    else
        print_status $RED "❌ Bronze Layer failed"
        return 1
    fi
}

run_silver_layer() {
    print_status $YELLOW "\n🥈 Running Complete Silver Layer..."
    run_silver_production && run_silver_equipment && run_silver_weather
    if [ $? -eq 0 ]; then
        print_status $GREEN "✅ Silver Layer completed successfully"
    else
        print_status $RED "❌ Silver Layer failed"
        return 1
    fi
}

run_gold_layer() {
    print_status $YELLOW "\n🥇 Running Complete Gold Layer..."
    run_gold_daily_metrics && run_gold_mine_performance
    if [ $? -eq 0 ]; then
        print_status $GREEN "✅ Gold Layer completed successfully"
    else
        print_status $RED "❌ Gold Layer failed"
        return 1
    fi
}

# Full Pipeline
run_full_pipeline() {
    print_status $YELLOW "\n🚀 Running Full Pipeline..."
    docker exec etl-runner python3 /app/scripts/run_pipeline.py
    if [ $? -eq 0 ]; then
        print_status $GREEN "✅ Full Pipeline completed successfully"
    else
        print_status $RED "❌ Full Pipeline failed"
        return 1
    fi
}

# Usage function
show_usage() {
    print_status $BLUE "\nUsage: $0 [OPTION]"
    print_status $BLUE "\nAvailable options:"
    print_status $BLUE "  Bronze Layer:"
    print_status $BLUE "    bronze-production    Run bronze production pipeline"
    print_status $BLUE "    bronze-equipment     Run bronze equipment pipeline"
    print_status $BLUE "    bronze-weather       Run bronze weather pipeline"
    print_status $BLUE "    bronze-all          Run complete bronze layer"
    print_status $BLUE ""
    print_status $BLUE "  Silver Layer:"
    print_status $BLUE "    silver-production    Run silver production pipeline"
    print_status $BLUE "    silver-equipment     Run silver equipment pipeline"
    print_status $BLUE "    silver-weather       Run silver weather pipeline"
    print_status $BLUE "    silver-all          Run complete silver layer"
    print_status $BLUE ""
    print_status $BLUE "  Gold Layer:"
    print_status $BLUE "    gold-daily          Run gold daily metrics pipeline"
    print_status $BLUE "    gold-mines          Run gold mine performance pipeline"
    print_status $BLUE "    gold-all            Run complete gold layer"
    print_status $BLUE ""
    print_status $BLUE "  Full Pipeline:"
    print_status $BLUE "    full                Run complete medallion pipeline"
    print_status $BLUE "    all                 Same as 'full'"
    print_status $BLUE ""
    print_status $BLUE "  Utilities:"
    print_status $BLUE "    help                Show this help message"
    print_status $BLUE "    status              Check pipeline status"
}

# Status check function
check_status() {
    print_status $BLUE "\n📊 Checking Pipeline Status..."
    
    # Check if containers are running
    if docker ps | grep -q "mysql-db"; then
        print_status $GREEN "✅ MySQL container is running"
    else
        print_status $RED "❌ MySQL container is not running"
    fi
    
    if docker ps | grep -q "clickhouse-db"; then
        print_status $GREEN "✅ ClickHouse container is running"
    else
        print_status $RED "❌ ClickHouse container is not running"
    fi
    
    if docker ps | grep -q "etl-runner"; then
        print_status $GREEN "✅ ETL Runner container is running"
    else
        print_status $RED "❌ ETL Runner container is not running"
    fi
    
    # Check if log files exist
    if [ -d "/app/logs" ] || docker exec etl-runner ls /app/logs > /dev/null 2>&1; then
        print_status $GREEN "✅ Log directory exists"
        print_status $BLUE "Recent log files:"
        docker exec etl-runner ls -la /app/logs/ 2>/dev/null || echo "  No log files found"
    else
        print_status $YELLOW "⚠️  Log directory not found"
    fi
}

# Main script logic
case "${1:-help}" in
    "bronze-production")
        run_bronze_production
        ;;
    "bronze-equipment")
        run_bronze_equipment
        ;;
    "bronze-weather")
        run_bronze_weather
        ;;
    "bronze-all")
        run_bronze_layer
        ;;
    "silver-production")
        run_silver_production
        ;;
    "silver-equipment")
        run_silver_equipment
        ;;
    "silver-weather")
        run_silver_weather
        ;;
    "silver-all")
        run_silver_layer
        ;;
    "gold-daily")
        run_gold_daily_metrics
        ;;
    "gold-mines")
        run_gold_mine_performance
        ;;
    "gold-all")
        run_gold_layer
        ;;
    "full"|"all")
        run_full_pipeline
        ;;
    "status")
        check_status
        ;;
    "help"|*)
        show_usage
        ;;
esac