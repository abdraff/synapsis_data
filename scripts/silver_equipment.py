#!/usr/bin/env python3
"""
Silver Layer - Equipment Data Transformation
Transforms bronze equipment sensor data into daily equipment metrics
"""

import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
from config import get_config
from database_utils import ClickHouseConnector

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/app/logs/silver_equipment.log'),
            logging.StreamHandler()
        ]
    )

def extract_bronze_equipment(clickhouse_conn: ClickHouseConnector, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Extract equipment data from bronze layer"""
    
    # Default to last 30 days if no dates provided
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    query = f"""
    SELECT 
        toDate(timestamp) as date,
        equipment_id,
        status,
        fuel_consumption,
        maintenance_alert
    FROM bronze_equipment_sensors
    WHERE toDate(timestamp) BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date, equipment_id, timestamp
    """
    
    logging.info(f"Extracting bronze equipment data from {start_date} to {end_date}")
    result = clickhouse_conn.execute_query(query)
    
    # Convert to DataFrame
    df = pd.DataFrame(result, columns=['date', 'equipment_id', 'status', 'fuel_consumption', 'maintenance_alert'])
    logging.info(f"Extracted {len(df)} bronze equipment records")
    return df

def transform_equipment_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform and aggregate equipment data"""
    
    # Data cleaning
    df_clean = df.copy()
    
    # Handle missing equipment IDs (fill with 'UNKNOWN')
    df_clean['equipment_id'] = df_clean['equipment_id'].fillna('UNKNOWN')
    
    # Handle negative fuel consumption (replace with 0)
    df_clean.loc[df_clean['fuel_consumption'] < 0, 'fuel_consumption'] = 0
    
    # Convert maintenance_alert to boolean if needed
    if df_clean['maintenance_alert'].dtype == 'object':
        df_clean['maintenance_alert'] = df_clean['maintenance_alert'].astype(bool)
    
    # Calculate operational hours (assume each record represents 1 hour for active status)
    df_clean['operational_hours'] = (df_clean['status'] == 'active').astype(int)
    
    # Aggregate by date and equipment_id
    daily_equipment = df_clean.groupby(['date', 'equipment_id']).agg({
        'operational_hours': 'sum',
        'fuel_consumption': 'mean',
        'maintenance_alert': 'sum',
        'status': 'count'  # Total records per day
    }).reset_index()
    
    daily_equipment.columns = [
        'date', 'equipment_id', 'total_operational_hours', 
        'avg_fuel_consumption', 'maintenance_alerts_count', 'total_records'
    ]
    
    # Calculate utilization percentage (operational hours / total records * 100)
    daily_equipment['utilization_percentage'] = (
        daily_equipment['total_operational_hours'] / daily_equipment['total_records'] * 100
    ).round(2)
    
    # Ensure utilization is between 0 and 100
    daily_equipment['utilization_percentage'] = daily_equipment['utilization_percentage'].clip(0, 100)
    
    # Calculate fuel efficiency (if we had production data per equipment, we'd use tons/fuel)
    # For now, we'll use inverse of fuel consumption as efficiency indicator
    daily_equipment['fuel_efficiency'] = (
        1 / (daily_equipment['avg_fuel_consumption'] + 0.001)  # Add small value to avoid division by zero
    ).round(4)
    
    # Add processing timestamp
    daily_equipment['processed_timestamp'] = datetime.now()
    
    # Drop the helper column
    daily_equipment = daily_equipment.drop(columns=['total_records'])
    
    logging.info(f"Transformed to {len(daily_equipment)} daily equipment records")
    return daily_equipment

def validate_silver_equipment_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate transformed equipment data"""
    
    # Check for invalid utilization percentages
    invalid_utilization = df[(df['utilization_percentage'] < 0) | (df['utilization_percentage'] > 100)]
    if len(invalid_utilization) > 0:
        logging.error(f"Found {len(invalid_utilization)} records with invalid utilization percentage")
    
    # Check for negative operational hours
    negative_hours = df[df['total_operational_hours'] < 0]
    if len(negative_hours) > 0:
        logging.error(f"Found {len(negative_hours)} records with negative operational hours")
    
    # Check for extreme fuel consumption values
    extreme_fuel = df[df['avg_fuel_consumption'] > 1000]  # Assuming 1000 is unrealistic
    if len(extreme_fuel) > 0:
        logging.warning(f"Found {len(extreme_fuel)} records with extreme fuel consumption")
    
    # Check for missing equipment IDs
    missing_equipment = df[df['equipment_id'].isna() | (df['equipment_id'] == '')]
    if len(missing_equipment) > 0:
        logging.warning(f"Found {len(missing_equipment)} records with missing equipment ID")
    
    logging.info(f"Validated {len(df)} silver equipment records")
    return df

def load_to_silver(clickhouse_conn: ClickHouseConnector, df: pd.DataFrame):
    """Load transformed equipment data to silver layer"""
    
    # Clear existing data for the date range
    if not df.empty:
        min_date = df['date'].min()
        max_date = df['date'].max()
        
        delete_query = f"""
        ALTER TABLE silver_equipment_metrics 
        DELETE WHERE date BETWEEN '{min_date}' AND '{max_date}'
        """
        clickhouse_conn.execute_command(delete_query)
        logging.info(f"Cleared existing silver equipment data from {min_date} to {max_date}")
    
    # Insert new data
    clickhouse_conn.insert_dataframe('silver_equipment_metrics', df)
    logging.info(f"Loaded {len(df)} transformed equipment records to silver layer")

def main():
    """Main execution function"""
    setup_logging()
    config = get_config()
    
    try:
        # Initialize ClickHouse connection
        clickhouse_conn = ClickHouseConnector(config.clickhouse)
        
        # Extract data from bronze layer
        bronze_df = extract_bronze_equipment(clickhouse_conn)
        
        if bronze_df.empty:
            logging.warning("No bronze equipment data found")
            return
        
        # Transform data
        silver_df = transform_equipment_data(bronze_df)
        
        # Validate transformed data
        validated_df = validate_silver_equipment_data(silver_df)
        
        # Load to silver layer
        load_to_silver(clickhouse_conn, validated_df)
        
        logging.info("Silver equipment pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Silver equipment pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()