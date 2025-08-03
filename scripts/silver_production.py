#!/usr/bin/env python3
"""
Silver Layer - Production Data Transformation
Transforms bronze production data into clean, aggregated daily metrics
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
            logging.FileHandler('/app/logs/silver_production.log'),
            logging.StreamHandler()
        ]
    )

def extract_bronze_production(clickhouse_conn: ClickHouseConnector, start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """Extract production data from bronze layer"""
    
    # Default to last 30 days if no dates provided
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    query = f"""
    SELECT 
        date,
        mine_id,
        shift,
        tons_extracted,
        quality_grade
    FROM coal_mining_dw.bronze_production_logs
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date, mine_id, shift
    """
    
    logging.info(f"Extracting bronze production data from {start_date} to {end_date}")
    result = clickhouse_conn.execute_query(query)
    
    # Convert to DataFrame
    df = pd.DataFrame(result, columns=['date', 'mine_id', 'shift', 'tons_extracted', 'quality_grade'])
    logging.info(f"Extracted {len(df)} bronze production records")
    return df

def transform_production_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform and aggregate production data"""
    
    # Data cleaning - handle negative tons_extracted (mark as anomaly but replace with 0)
    df_clean = df.copy()
    anomaly_mask = df_clean['tons_extracted'] < 0
    df_clean.loc[anomaly_mask, 'tons_extracted'] = 0
    
    # Aggregate by date and mine_id
    daily_agg = df_clean.groupby(['date', 'mine_id']).agg({
        'tons_extracted': 'sum',
        'quality_grade': 'mean',
        'shift': 'count'
    }).reset_index()
    
    daily_agg.columns = ['date', 'mine_id', 'total_tons_extracted', 'avg_quality_grade', 'shifts_count']
    
    # Add quality score categorization
    def categorize_quality(grade):
        if pd.isna(grade):
            return 'Unknown'
        elif grade < 3.0:
            return 'Poor'
        elif grade < 4.0:
            return 'Fair'
        elif grade < 5.0:
            return 'Good'
        else:
            return 'Excellent'
    
    daily_agg['quality_score'] = daily_agg['avg_quality_grade'].apply(categorize_quality)
    
    # Flag anomalies (days with any negative extraction records)
    anomaly_dates = df[anomaly_mask].groupby(['date', 'mine_id']).size().reset_index(name='anomaly_count')
    daily_agg = daily_agg.merge(anomaly_dates, on=['date', 'mine_id'], how='left')
    daily_agg['anomaly_flag'] = daily_agg['anomaly_count'].notna()
    daily_agg = daily_agg.drop(columns=['anomaly_count'])
    
    # Add processing timestamp
    daily_agg['processed_timestamp'] = datetime.now()
    
    logging.info(f"Transformed to {len(daily_agg)} daily production records")
    return daily_agg

def validate_silver_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate transformed data"""
    
    # Check for negative totals (should not happen after cleaning)
    negative_totals = df[df['total_tons_extracted'] < 0]
    if len(negative_totals) > 0:
        logging.error(f"Found {len(negative_totals)} records with negative total extraction after cleaning")
    
    # Check for missing quality grades
    missing_quality = df[df['avg_quality_grade'].isna()]
    if len(missing_quality) > 0:
        logging.warning(f"Found {len(missing_quality)} records with missing average quality grade")
    
    # Check for unrealistic quality grades
    extreme_quality = df[(df['avg_quality_grade'] < 0) | (df['avg_quality_grade'] > 10)]
    if len(extreme_quality) > 0:
        logging.warning(f"Found {len(extreme_quality)} records with extreme quality grades")
    
    logging.info(f"Validated {len(df)} silver production records")
    return df

def load_to_silver(clickhouse_conn: ClickHouseConnector, df: pd.DataFrame):
    """Load transformed data to silver layer"""
    
    # Clear existing data for the date range
    if not df.empty:
        min_date = df['date'].min()
        max_date = df['date'].max()
        
        delete_query = f"""
        ALTER TABLE silver_production_daily 
        DELETE WHERE date BETWEEN '{min_date}' AND '{max_date}'
        """
        clickhouse_conn.execute_command(delete_query)
        logging.info(f"Cleared existing silver data from {min_date} to {max_date}")
    
    # Insert new data
    clickhouse_conn.insert_dataframe('silver_production_daily', df)
    logging.info(f"Loaded {len(df)} transformed production records to silver layer")

def main():
    """Main execution function"""
    setup_logging()
    config = get_config()
    
    try:
        # Initialize ClickHouse connection
        clickhouse_conn = ClickHouseConnector(config.clickhouse)
        
        # Extract data from bronze layer
        bronze_df = extract_bronze_production(clickhouse_conn)
        
        if bronze_df.empty:
            logging.warning("No bronze production data found")
            return
        
        # Transform data
        silver_df = transform_production_data(bronze_df)
        
        # Validate transformed data
        validated_df = validate_silver_data(silver_df)
        
        # Load to silver layer
        load_to_silver(clickhouse_conn, validated_df)
        
        logging.info("Silver production pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Silver production pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()