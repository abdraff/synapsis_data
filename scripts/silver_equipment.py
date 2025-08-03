#!/usr/bin/env python3

import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
from config import get_config
from database_utils import ClickHouseConnector

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/app/logs/silver_equipment.log'),
            logging.StreamHandler()
        ]
    )

def extract_bronze_equipment(ch_conn: ClickHouseConnector, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    
    # Default to sample data range if no dates provided
    if not end_date:
        end_date = '2025-06-30'
    if not start_date:
        start_date = '2025-06-01'
    
    query = f"""
    SELECT 
        DATE(timestamp) as date,
        equipment_id,
        status,
        fuel_consumption,
        maintenance_alert
    FROM coal_mining_dw.bronze_equipment_sensors
    WHERE DATE(timestamp) BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date, equipment_id
    """
    
    logging.info(f"Extracting bronze equipment data from {start_date} to {end_date}")
    result = ch_conn.execute_query(query)
    
    # Convert to DataFrame
    columns = ['date', 'equipment_id', 'status', 'fuel_consumption', 'maintenance_alert']
    df = pd.DataFrame(result, columns=columns)
    logging.info(f"Extracted {len(df)} bronze equipment records")
    return df

def transform_equipment_data(df: pd.DataFrame) -> pd.DataFrame:
    
    if df.empty:
        return df
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    # Filter only active equipment
    df_active = df[df['status'] == 'active'].copy()
    
    # Count records per day per equipment (as proxy for operating hours)
    df_active['operating_hours'] = 1  # Each record represents 1 hour of operation
    
    # Aggregate by date and equipment_id
    daily_agg = df_active.groupby(['date', 'equipment_id']).agg({
        'fuel_consumption': 'sum',
        'operating_hours': 'sum',
        'maintenance_alert': 'sum'  # Count maintenance alerts
    }).reset_index()
    
    # Rename columns to match silver table schema
    daily_agg = daily_agg.rename(columns={
        'operating_hours': 'total_operational_hours',
        'fuel_consumption': 'avg_fuel_consumption',
        'maintenance_alert': 'maintenance_alerts_count'
    })
    
    # Calculate utilization (assuming 24 hours max per day)
    daily_agg['utilization_percentage'] = (daily_agg['total_operational_hours'] / 24.0 * 100).clip(0, 100)
    
    # Calculate fuel efficiency (hours per liter)
    daily_agg['fuel_efficiency'] = daily_agg['total_operational_hours'] / daily_agg['avg_fuel_consumption'].replace(0, 1)
    
    # Add processing timestamp
    daily_agg['processed_timestamp'] = datetime.now()
    
    logging.info(f"Transformed to {len(daily_agg)} daily equipment records")
    return daily_agg

def validate_silver_data(df: pd.DataFrame) -> pd.DataFrame:
    
    # Check for negative fuel consumption
    negative_fuel = df[df['avg_fuel_consumption'] < 0]
    if len(negative_fuel) > 0:
        logging.warning(f"Found {len(negative_fuel)} records with negative fuel consumption")
    
    # Check utilization bounds
    invalid_utilization = df[(df['utilization_percentage'] < 0) | (df['utilization_percentage'] > 100)]
    if len(invalid_utilization) > 0:
        logging.warning(f"Found {len(invalid_utilization)} records with invalid utilization rate")
    
    # Log summary statistics
    logging.info(f"Equipment metrics summary:")
    logging.info(f"  Average daily fuel consumption: {df['avg_fuel_consumption'].mean():.2f}")
    logging.info(f"  Average utilization rate: {df['utilization_percentage'].mean():.2f}%")
    logging.info(f"  Equipment with maintenance alerts: {df['maintenance_alerts_count'].sum()}")
    
    logging.info(f"Validated {len(df)} silver equipment records")
    return df

def load_to_silver(ch_conn: ClickHouseConnector, df: pd.DataFrame):
    
    # Clear existing data for the date range
    if not df.empty:
        min_date = df['date'].min().strftime('%Y-%m-%d')
        max_date = df['date'].max().strftime('%Y-%m-%d')
        
        delete_query = f"""
        ALTER TABLE silver_equipment_metrics 
        DELETE WHERE date BETWEEN '{min_date}' AND '{max_date}'
        """
        ch_conn.execute_command(delete_query)
        logging.info(f"Cleared existing silver equipment data from {min_date} to {max_date}")
    
    ch_conn.insert_dataframe('silver_equipment_metrics', df)
    logging.info(f"Loaded {len(df)} transformed equipment records to silver layer")

def main():
    setup_logging()
    config = get_config()
    
    try:
        ch_conn = ClickHouseConnector(config.clickhouse)
        bronze_df = extract_bronze_equipment(ch_conn)
        
        if bronze_df.empty:
            logging.warning("No bronze equipment data found")
            return
        
        silver_df = transform_equipment_data(bronze_df)
        validated_df = validate_silver_data(silver_df)
        load_to_silver(ch_conn, validated_df)
        
        logging.info("Silver equipment pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Silver equipment pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()