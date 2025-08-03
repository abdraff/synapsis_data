#!/usr/bin/env python3
"""
Bronze Layer - Production Data Ingestion
Extracts production data from MySQL and loads into ClickHouse bronze layer
"""

import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
import argparse  # Add this import
from config import get_config
from database_utils import MySQLConnector, ClickHouseConnector

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/app/logs/bronze_production.log'),
            logging.StreamHandler()
        ]
    )

def extract_production_data(mysql_conn: MySQLConnector, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Extract production data from MySQL"""
    
    # Default to last 30 days if no dates provided
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    query = f"""
    SELECT 
        pl.log_id,
        pl.date,
        pl.mine_id,
        pl.shift,
        pl.tons_extracted,
        pl.quality_grade
    FROM production_logs pl
    WHERE pl.date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY pl.date, pl.mine_id, pl.shift
    """
    
    logging.info(f"Extracting production data from {start_date} to {end_date}")
    return mysql_conn.execute_query(query)

def validate_production_data(df: pd.DataFrame) -> pd.DataFrame:
    """Basic validation and data quality checks"""
    initial_count = len(df)
    
    # Log data quality issues
    negative_tons = df[df['tons_extracted'] < 0]
    if len(negative_tons) > 0:
        logging.warning(f"Found {len(negative_tons)} records with negative tons_extracted")
    
    missing_quality = df[df['quality_grade'].isna()]
    if len(missing_quality) > 0:
        logging.warning(f"Found {len(missing_quality)} records with missing quality_grade")
    
    # Add validation flags but keep all data for bronze layer
    df['data_quality_flag'] = 'valid'
    df.loc[df['tons_extracted'] < 0, 'data_quality_flag'] = 'negative_extraction'
    df.loc[df['quality_grade'].isna(), 'data_quality_flag'] = 'missing_quality'
    
    logging.info(f"Validated {initial_count} production records")
    return df

def load_to_bronze(clickhouse_conn: ClickHouseConnector, df: pd.DataFrame):
    """Load production data to bronze layer"""
    
    # Add metadata columns
    df['ingestion_timestamp'] = datetime.now()
    df['source'] = 'mysql'
    
    # Clear existing data for the date range
    if not df.empty:
        min_date = df['date'].min()
        max_date = df['date'].max()
        
        delete_query = f"""
        ALTER TABLE bronze_production_logs 
        DELETE WHERE date BETWEEN '{min_date}' AND '{max_date}'
        """
        clickhouse_conn.execute_command(delete_query)
        logging.info(f"Cleared existing data from {min_date} to {max_date}")
    
    # Insert new data
    clickhouse_conn.insert_dataframe('bronze_production_logs', df)
    logging.info(f"Loaded {len(df)} production records to bronze layer")

def main():
    """Main execution function"""
    setup_logging()

    # Add argument parsing
    parser = argparse.ArgumentParser(description='Bronze Production Data Ingestion')
    parser.add_argument('--start_date', help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end_date', help='End date in YYYY-MM-DD format')
    args = parser.parse_args()

    config = get_config()
    
    try:
        # Initialize connections
        mysql_conn = MySQLConnector(config.mysql)
        clickhouse_conn = ClickHouseConnector(config.clickhouse)
        
        # Extract data using provided dates or default
        production_df = extract_production_data(mysql_conn, args.start_date, args.end_date)
        
        if production_df.empty:
            logging.warning("No production data found")
            return
        
        # Validate data
        validated_df = validate_production_data(production_df)
        
        # Load to bronze layer
        load_to_bronze(clickhouse_conn, validated_df)
        
        logging.info("Bronze production pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Bronze production pipeline failed: {e}")
        sys.exit(1)
    finally:
        mysql_conn.close()

if __name__ == "__main__":
    main()