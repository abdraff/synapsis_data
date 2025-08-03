#!/usr/bin/env python3
"""
Gold Layer - Mine Performance Analysis
Creates mine-specific performance metrics and rankings
"""

import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from config import get_config
from database_utils import ClickHouseConnector

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/app/logs/gold_mine_performance.log'),
            logging.StreamHandler()
        ]
    )

def extract_silver_data(clickhouse_conn: ClickHouseConnector, start_date: str = None, end_date: str = None) -> tuple:
    """Extract mine production and weather data from silver layers"""
    
    # Default to last 30 days if no dates provided
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    # Extract production data by mine
    production_query = f"""
    SELECT 
        date,
        mine_id,
        total_tons_extracted,
        avg_quality_grade,
        anomaly_flag
    FROM silver_production_daily
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date, mine_id
    """
    
    # Extract weather data for correlation analysis
    weather_query = f"""
    SELECT 
        date,
        precipitation_sum,
        is_rainy_day
    FROM silver_weather_clean
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date
    """
    
    logging.info(f"Extracting silver data for mine performance from {start_date} to {end_date}")
    
    production_result = clickhouse_conn.execute_query(production_query)
    weather_result = clickhouse_conn.execute_query(weather_query)
    
    # Convert to DataFrames
    production_df = pd.DataFrame(production_result, 
                                columns=['date', 'mine_id', 'total_tons_extracted', 'avg_quality_grade', 'anomaly_flag'])
    weather_df = pd.DataFrame(weather_result, 
                             columns=['date', 'precipitation_sum', 'is_rainy_day'])
    
    logging.info(f"Extracted {len(production_df)} production records, {len(weather_df)} weather records")
    return production_df, weather_df

def get_mine_names(clickhouse_conn: ClickHouseConnector) -> dict:
    """Get mine names from the source system (if available)"""
    
    # This would typically come from a dimension table
    # For now, we'll create a simple mapping
    mine_names = {
        1: 'Bukit Bara',
        2: 'Gunung Hitam',
        3: 'Sumber Jaya'
    }
    
    logging.info(f"Using mine name mapping: {mine_names}")
    return mine_names

def calculate_mine_performance(production_df: pd.DataFrame, weather_df: pd.DataFrame, mine_names: dict) -> pd.DataFrame:
    """Calculate comprehensive mine performance metrics"""
    
    if production_df.empty:
        logging.warning("No production data available for mine performance calculation")
        return pd.DataFrame()
    
    # Merge production with weather data
    df_merged = production_df.merge(weather_df, on='date', how='left')
    
    # Calculate daily mine performance
    mine_performance = []
    
    for date in df_merged['date'].unique():
        daily_data = df_merged[df_merged['date'] == date]
        
        for mine_id in daily_data['mine_id'].unique():
            mine_data = daily_data[daily_data['mine_id'] == mine_id]
            
            # Aggregate mine data for the day (in case of multiple records)
            production_tons = mine_data['total_tons_extracted'].sum()
            quality_grade = mine_data['avg_quality_grade'].mean()
            has_anomaly = mine_data['anomaly_flag'].any()
            precipitation = mine_data['precipitation_sum'].iloc[0] if not mine_data.empty else 0
            is_rainy = mine_data['is_rainy_day'].iloc[0] if not mine_data.empty else False
            
            mine_performance.append({
                'date': date,
                'mine_id': mine_id,
                'mine_name': mine_names.get(mine_id, f'Mine_{mine_id}'),
                'production_tons': production_tons,
                'quality_grade': quality_grade,
                'has_anomaly': has_anomaly,
                'precipitation': precipitation,
                'is_rainy': is_rainy
            })
    
    if not mine_performance:
        logging.warning("No mine performance data calculated")
        return pd.DataFrame()
    
    df_performance = pd.DataFrame(mine_performance)
    
    # Calculate efficiency score (combination of production and quality)
    df_performance['efficiency_score'] = calculate_efficiency_score(df_performance)
    
    # Calculate weather correlation for each mine
    df_performance = add_weather_correlation(df_performance)
    
    # Calculate daily performance rankings
    df_performance = add_performance_rankings(df_performance)
    
    # Add processing timestamp
    df_performance['processed_timestamp'] = datetime.now()
    
    logging.info(f"Calculated performance metrics for {len(df_performance)} mine-days")
    return df_performance

def calculate_efficiency_score(df: pd.DataFrame) -> pd.Series:
    """Calculate efficiency score based on production and quality"""
    
    # Normalize production and quality to 0-100 scale
    if df['production_tons'].max() > 0:
        prod_normalized = (df['production_tons'] / df['production_tons'].max()) * 100
    else:
        prod_normalized = pd.Series([0] * len(df))
    
    if df['quality_grade'].max() > 0:
        qual_normalized = (df['quality_grade'] / 6.0) * 100  # Assuming max quality is 6
    else:
        qual_normalized = pd.Series([0] * len(df))
    
    # Weight production more heavily than quality (70:30)
    efficiency_score = (prod_normalized * 0.7 + qual_normalized * 0.3)
    
    # Penalize for anomalies
    efficiency_score = np.where(df['has_anomaly'], efficiency_score * 0.8, efficiency_score)
    
    return efficiency_score.round(2)

def add_weather_correlation(df: pd.DataFrame) -> pd.DataFrame:
    """Add weather correlation analysis for each mine"""
    
    correlations = []
    
    for mine_id in df['mine_id'].unique():
        mine_data = df[df['mine_id'] == mine_id].copy()
        
        if len(mine_data) > 1:
            # Calculate correlation between precipitation and production
            corr_coef = mine_data['precipitation'].corr(mine_data['production_tons'])
            if pd.isna(corr_coef):
                corr_coef = 0.0
        else:
            corr_coef = 0.0
        
        # Create a mapping for all dates for this mine
        mine_correlations = pd.Series([corr_coef] * len(mine_data), index=mine_data.index)
        correlations.extend(mine_correlations.tolist())
    
    df['weather_correlation'] = correlations
    return df

def add_performance_rankings(df: pd.DataFrame) -> pd.DataFrame:
    """Add daily performance rankings"""
    
    # Calculate rankings for each date
    rankings = []
    
    for date in df['date'].unique():
        daily_data = df[df['date'] == date].copy()
        
        # Rank by efficiency score (1 = best)
        daily_data['rank'] = daily_data['efficiency_score'].rank(method='dense', ascending=False)
        rankings.extend(daily_data['rank'].tolist())
    
    df['performance_rank'] = [int(r) for r in rankings]
    return df

def validate_mine_performance_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate mine performance data"""
    
    # Check for negative production
    negative_production = df[df['production_tons'] < 0]
    if len(negative_production) > 0:
        logging.error(f"Found {len(negative_production)} records with negative production")
    
    # Check for invalid quality grades
    invalid_quality = df[(df['quality_grade'] < 0) | (df['quality_grade'] > 10)]
    if len(invalid_quality) > 0:
        logging.warning(f"Found {len(invalid_quality)} records with invalid quality grades")
    
    # Check efficiency score bounds
    invalid_efficiency = df[(df['efficiency_score'] < 0) | (df['efficiency_score'] > 100)]
    if len(invalid_efficiency) > 0:
        logging.warning(f"Found {len(invalid_efficiency)} records with invalid efficiency scores")
    
    # Log performance statistics by mine
    for mine_id in df['mine_id'].unique():
        mine_data = df[df['mine_id'] == mine_id]
        mine_name = mine_data['mine_name'].iloc[0]
        avg_production = mine_data['production_tons'].mean()
        avg_quality = mine_data['quality_grade'].mean()
        avg_efficiency = mine_data['efficiency_score'].mean()
        
        logging.info(f"Mine {mine_name} (ID: {mine_id}):")
        logging.info(f"  Average daily production: {avg_production:.2f} tons")
        logging.info(f"  Average quality grade: {avg_quality:.2f}")
        logging.info(f"  Average efficiency score: {avg_efficiency:.2f}")
    
    return df

def load_to_gold(clickhouse_conn: ClickHouseConnector, df: pd.DataFrame):
    """Load mine performance data to gold layer"""
    
    # Clear existing data for the date range
    if not df.empty:
        min_date = df['date'].min()
        max_date = df['date'].max()
        
        delete_query = f"""
        ALTER TABLE gold_mine_performance 
        DELETE WHERE date BETWEEN '{min_date}' AND '{max_date}'
        """
        clickhouse_conn.execute_command(delete_query)
        logging.info(f"Cleared existing gold mine performance data from {min_date} to {max_date}")
    
    # Select and reorder columns for the gold table
    gold_columns = [
        'date', 'mine_id', 'mine_name', 'production_tons', 'quality_grade',
        'performance_rank', 'efficiency_score', 'weather_correlation', 'processed_timestamp'
    ]
    
    df_gold = df[gold_columns].copy()
    
    # Insert new data
    clickhouse_conn.insert_dataframe('gold_mine_performance', df_gold)
    logging.info(f"Loaded {len(df_gold)} mine performance records to gold layer")

def main():
    """Main execution function"""
    setup_logging()
    config = get_config()
    
    try:
        # Initialize ClickHouse connection
        clickhouse_conn = ClickHouseConnector(config.clickhouse)
        
        # Get mine names
        mine_names = get_mine_names(clickhouse_conn)
        
        # Extract data from silver layers
        production_df, weather_df = extract_silver_data(clickhouse_conn)
        
        # Calculate mine performance metrics
        mine_performance_df = calculate_mine_performance(production_df, weather_df, mine_names)
        
        if mine_performance_df.empty:
            logging.warning("No mine performance metrics calculated")
            return
        
        # Validate data
        validated_df = validate_mine_performance_data(mine_performance_df)
        
        # Load to gold layer
        load_to_gold(clickhouse_conn, validated_df)
        
        logging.info("Gold mine performance pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Gold mine performance pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()