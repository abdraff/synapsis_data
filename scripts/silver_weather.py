#!/usr/bin/env python3
"""
Silver Layer - Weather Data Transformation
Transforms bronze weather data into clean, categorized weather metrics
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
            logging.FileHandler('/app/logs/silver_weather.log'),
            logging.StreamHandler()
        ]
    )

def extract_bronze_weather(clickhouse_conn: ClickHouseConnector, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Extract weather data from bronze layer"""
    
    # Default to last 30 days if no dates provided
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    query = f"""
    SELECT 
        date,
        temperature_mean,
        precipitation_sum
    FROM bronze_weather_data
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date
    """
    
    logging.info(f"Extracting bronze weather data from {start_date} to {end_date}")
    result = clickhouse_conn.execute_query(query)
    
    # Convert to DataFrame
    df = pd.DataFrame(result, columns=['date', 'temperature_mean', 'precipitation_sum'])
    logging.info(f"Extracted {len(df)} bronze weather records")
    return df

def transform_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform and clean weather data"""
    
    df_clean = df.copy()
    
    # Handle missing values - forward fill for temperature, 0 for precipitation
    df_clean['temperature_mean'] = df_clean['temperature_mean'].fillna(method='ffill')
    df_clean['precipitation_sum'] = df_clean['precipitation_sum'].fillna(0)
    
    # Handle extreme values
    # Cap temperature at reasonable ranges for tropical climate
    df_clean['temperature_mean'] = df_clean['temperature_mean'].clip(10, 50)
    
    # Ensure precipitation is not negative
    df_clean['precipitation_sum'] = df_clean['precipitation_sum'].clip(lower=0)
    
    # Add weather categorization
    def categorize_weather(temp, precip):
        """Categorize weather based on temperature and precipitation"""
        if precip > 10:
            return 'Heavy Rain'
        elif precip > 2:
            return 'Light Rain'
        elif temp > 30:
            return 'Hot'
        elif temp > 25:
            return 'Warm'
        else:
            return 'Cool'
    
    df_clean['weather_category'] = df_clean.apply(
        lambda row: categorize_weather(row['temperature_mean'], row['precipitation_sum']), 
        axis=1
    )
    
    # Add rainy day flag (>2mm precipitation typically indicates rain)
    df_clean['is_rainy_day'] = df_clean['precipitation_sum'] > 2.0
    
    # Add processing timestamp
    df_clean['processed_timestamp'] = datetime.now()
    
    logging.info(f"Transformed {len(df_clean)} weather records")
    return df_clean

def validate_silver_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate transformed weather data"""
    
    # Check for missing values after cleaning
    missing_temp = df[df['temperature_mean'].isna()]
    if len(missing_temp) > 0:
        logging.warning(f"Found {len(missing_temp)} records with missing temperature after cleaning")
    
    missing_precip = df[df['precipitation_sum'].isna()]
    if len(missing_precip) > 0:
        logging.warning(f"Found {len(missing_precip)} records with missing precipitation after cleaning")
    
    # Check for unrealistic values after cleaning
    extreme_temp = df[(df['temperature_mean'] < 10) | (df['temperature_mean'] > 50)]
    if len(extreme_temp) > 0:
        logging.warning(f"Found {len(extreme_temp)} records with extreme temperatures after cleaning")
    
    negative_precip = df[df['precipitation_sum'] < 0]
    if len(negative_precip) > 0:
        logging.error(f"Found {len(negative_precip)} records with negative precipitation after cleaning")
    
    # Check weather category distribution
    category_counts = df['weather_category'].value_counts()
    logging.info(f"Weather category distribution: {category_counts.to_dict()}")
    
    rainy_days = df['is_rainy_day'].sum()
    total_days = len(df)
    logging.info(f"Rainy days: {rainy_days}/{total_days} ({rainy_days/total_days*100:.1f}%)")
    
    logging.info(f"Validated {len(df)} silver weather records")
    return df

def load_to_silver(clickhouse_conn: ClickHouseConnector, df: pd.DataFrame):
    """Load transformed weather data to silver layer"""
    
    # Clear existing data for the date range
    if not df.empty:
        min_date = df['date'].min()
        max_date = df['date'].max()
        
        delete_query = f"""
        ALTER TABLE silver_weather_clean 
        DELETE WHERE date BETWEEN '{min_date}' AND '{max_date}'
        """
        clickhouse_conn.execute_command(delete_query)
        logging.info(f"Cleared existing silver weather data from {min_date} to {max_date}")
    
    # Insert new data
    clickhouse_conn.insert_dataframe('silver_weather_clean', df)
    logging.info(f"Loaded {len(df)} transformed weather records to silver layer")

def main():
    """Main execution function"""
    setup_logging()
    config = get_config()
    
    try:
        # Initialize ClickHouse connection
        clickhouse_conn = ClickHouseConnector(config.clickhouse)
        
        # Extract data from bronze layer
        bronze_df = extract_bronze_weather(clickhouse_conn)
        
        if bronze_df.empty:
            logging.warning("No bronze weather data found")
            return
        
        # Transform data
        silver_df = transform_weather_data(bronze_df)
        
        # Validate transformed data
        validated_df = validate_silver_weather_data(silver_df)
        
        # Load to silver layer
        load_to_silver(clickhouse_conn, validated_df)
        
        logging.info("Silver weather pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Silver weather pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()