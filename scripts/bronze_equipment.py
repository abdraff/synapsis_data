#!/usr/bin/env python3
"""
Bronze Layer - Equipment Sensors Data Ingestion
Extracts equipment sensor data from CSV and loads into ClickHouse bronze layer
"""

import sys
import logging
import pandas as pd
from datetime import datetime
from config import get_config
from database_utils import ClickHouseConnector

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/app/logs/bronze_equipment.log'),
            logging.StreamHandler()
        ]
    )

def extract_equipment_data(csv_path: str) -> pd.DataFrame:
    """Extract equipment sensor data from CSV"""
    
    logging.info(f"Reading equipment data from {csv_path}")
    
    try:
        # Read CSV with proper data types
        df = pd.read_csv(csv_path)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Convert maintenance_alert to boolean
        df['maintenance_alert'] = df['maintenance_alert'].astype(bool)
        
        logging.info(f"Extracted {len(df)} equipment sensor records")
        return df
        
    except Exception as e:
        logging.error(f"Failed to read CSV file: {e}")
        raise

def validate_equipment_data(df: pd.DataFrame) -> pd.DataFrame:
    """Basic validation and data quality checks"""
    initial_count = len(df)
    
    # Check for missing values
    missing_equipment_id = df[df['equipment_id'].isna()]
    if len(missing_equipment_id) > 0:
        logging.warning(f"Found {len(missing_equipment_id)} records with missing equipment_id")
    
    # Check for invalid status values
    valid_statuses = ['active', 'inactive', 'maintenance']
    invalid_status = df[~df['status'].isin(valid_statuses)]
    if len(invalid_status) > 0:
        logging.warning(f"Found {len(invalid_status)} records with invalid status")
    
    # Check for negative fuel consumption
    negative_fuel = df[df['fuel_consumption'] < 0]
    if len(negative_fuel) > 0:
        logging.warning(f"Found {len(negative_fuel)} records with negative fuel_consumption")
    
    # Add validation flags
    df['data_quality_flag'] = 'valid'
    df.loc[df['equipment_id'].isna(), 'data_quality_flag'] = 'missing_equipment_id'
    df.loc[~df['status'].isin(valid_statuses), 'data_quality_flag'] = 'invalid_status'
    df.loc[df['fuel_consumption'] < 0, 'data_quality_flag'] = 'negative_fuel'
    
    logging.info(f"Validated {initial_count} equipment sensor records")
    return df

def load_to_bronze(clickhouse_conn: ClickHouseConnector, df: pd.DataFrame):
    """Load equipment data to bronze layer"""
    
    # Add metadata columns
    df['ingestion_timestamp'] = datetime.now()
    df['source'] = 'csv'
    
    # Clear existing data for the timestamp range
    if not df.empty:
        min_timestamp = df['timestamp'].min()
        max_timestamp = df['timestamp'].max()
        
        delete_query = f"""
        ALTER TABLE bronze_equipment_sensors 
        DELETE WHERE timestamp BETWEEN '{min_timestamp}' AND '{max_timestamp}'
        """
        clickhouse_conn.execute_command(delete_query)
        logging.info(f"Cleared existing data from {min_timestamp} to {max_timestamp}")
    
    # Prepare DataFrame for insertion (remove validation flag for bronze table)
    df_insert = df.drop(columns=['data_quality_flag'])
    
    # Insert new data
    clickhouse_conn.insert_dataframe('bronze_equipment_sensors', df_insert)
    logging.info(f"Loaded {len(df_insert)} equipment sensor records to bronze layer")

def main():
    """Main execution function"""
    setup_logging()
    config = get_config()
    
    try:
        # Initialize ClickHouse connection
        clickhouse_conn = ClickHouseConnector(config.clickhouse)
        
        # Extract data
        equipment_df = extract_equipment_data(config.csv_file_path)
        
        if equipment_df.empty:
            logging.warning("No equipment data found")
            return
        
        # Validate data
        validated_df = validate_equipment_data(equipment_df)
        
        # Load to bronze layer
        load_to_bronze(clickhouse_conn, validated_df)
        
        logging.info("Bronze equipment pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Bronze equipment pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()