#!/usr/bin/env python3
"""
Bronze Layer - Weather Data Ingestion
Extracts weather data from Open-Meteo API and loads into ClickHouse bronze layer
"""

import sys
import logging
import pandas as pd
import requests
from datetime import datetime, timedelta
from config import get_config
from database_utils import ClickHouseConnector

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/app/logs/bronze_weather.log'),
            logging.StreamHandler()
        ]
    )

def extract_weather_data(api_config, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Extract weather data from Open-Meteo API"""
    
    # Default to last 30 days if no dates provided
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    url = api_config.base_url
    params = {
        'latitude': api_config.latitude,
        'longitude': api_config.longitude,
        'daily': 'temperature_2m_mean,precipitation_sum',
        'timezone': api_config.timezone,
        'start_date': start_date,
        'end_date': end_date
    }
    
    logging.info(f"Extracting weather data from {start_date} to {end_date}")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Convert to DataFrame
        daily_data = data['daily']
        df = pd.DataFrame({
            'date': pd.to_datetime(daily_data['time']),
            'latitude': data['latitude'],
            'longitude': data['longitude'],
            'temperature_mean': daily_data['temperature_2m_mean'],
            'precipitation_sum': daily_data['precipitation_sum']
        })
        
        logging.info(f"Extracted {len(df)} weather records")
        return df
        
    except requests.RequestException as e:
        logging.error(f"Failed to fetch weather data: {e}")
        raise
    except KeyError as e:
        logging.error(f"Unexpected API response format: {e}")
        raise

def validate_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    """Basic validation and data quality checks"""
    initial_count = len(df)
    
    # Check for missing values
    missing_temp = df[df['temperature_mean'].isna()]
    if len(missing_temp) > 0:
        logging.warning(f"Found {len(missing_temp)} records with missing temperature")
    
    missing_precip = df[df['precipitation_sum'].isna()]
    if len(missing_precip) > 0:
        logging.warning(f"Found {len(missing_precip)} records with missing precipitation")
    
    # Check for unrealistic values
    extreme_temp = df[(df['temperature_mean'] < -50) | (df['temperature_mean'] > 60)]
    if len(extreme_temp) > 0:
        logging.warning(f"Found {len(extreme_temp)} records with extreme temperatures")
    
    negative_precip = df[df['precipitation_sum'] < 0]
    if len(negative_precip) > 0:
        logging.warning(f"Found {len(negative_precip)} records with negative precipitation")
    
    # Add validation flags
    df['data_quality_flag'] = 'valid'
    df.loc[df['temperature_mean'].isna(), 'data_quality_flag'] = 'missing_temperature'
    df.loc[df['precipitation_sum'].isna(), 'data_quality_flag'] = 'missing_precipitation'
    df.loc[(df['temperature_mean'] < -50) | (df['temperature_mean'] > 60), 'data_quality_flag'] = 'extreme_temperature'
    df.loc[df['precipitation_sum'] < 0, 'data_quality_flag'] = 'negative_precipitation'
    
    logging.info(f"Validated {initial_count} weather records")
    return df

def load_to_bronze(clickhouse_conn: ClickHouseConnector, df: pd.DataFrame):
    """Load weather data to bronze layer"""
    
    # Add metadata columns
    df['ingestion_timestamp'] = datetime.now()
    df['source'] = 'api'
    
    # Clear existing data for the date range
    if not df.empty:
        min_date = df['date'].min().strftime('%Y-%m-%d')
        max_date = df['date'].max().strftime('%Y-%m-%d')
        
        delete_query = f"""
        ALTER TABLE bronze_weather_data 
        DELETE WHERE date BETWEEN '{min_date}' AND '{max_date}'
        """
        clickhouse_conn.execute_command(delete_query)
        logging.info(f"Cleared existing data from {min_date} to {max_date}")
    
    # Prepare DataFrame for insertion (remove validation flag for bronze table)
    df_insert = df.drop(columns=['data_quality_flag'])
    df_insert['date'] = df_insert['date'].dt.date  # Convert to date only
    
    # Insert new data
    clickhouse_conn.insert_dataframe('bronze_weather_data', df_insert)
    logging.info(f"Loaded {len(df_insert)} weather records to bronze layer")

def main():
    """Main execution function"""
    setup_logging()
    config = get_config()
    
    try:
        # Initialize ClickHouse connection
        clickhouse_conn = ClickHouseConnector(config.clickhouse)
        
        # Extract data
        weather_df = extract_weather_data(config.weather_api)
        
        if weather_df.empty:
            logging.warning("No weather data found")
            return
        
        # Validate data
        validated_df = validate_weather_data(weather_df)
        
        # Load to bronze layer
        load_to_bronze(clickhouse_conn, validated_df)
        
        logging.info("Bronze weather pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Bronze weather pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()