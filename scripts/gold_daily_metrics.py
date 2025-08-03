#!/usr/bin/env python3
"""
Gold Layer - Daily Production Metrics
Creates business-ready daily production metrics combining all data sources
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
            logging.FileHandler('/app/logs/gold_daily_metrics.log'),
            logging.StreamHandler()
        ]
    )

def extract_silver_data(clickhouse_conn: ClickHouseConnector, start_date: str = None, end_date: str = None) -> tuple:
    """Extract data from all silver layer tables"""
    
    # Default to last 30 days if no dates provided
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    # Extract production data
    production_query = f"""
    SELECT 
        date,
        mine_id,
        total_tons_extracted,
        avg_quality_grade,
        anomaly_flag
    FROM silver_production_daily
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    
    # Extract equipment data
    equipment_query = f"""
    SELECT 
        date,
        equipment_id,
        total_operational_hours,
        utilization_percentage,
        avg_fuel_consumption,
        fuel_efficiency
    FROM silver_equipment_metrics
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    
    # Extract weather data
    weather_query = f"""
    SELECT 
        date,
        temperature_mean,
        precipitation_sum,
        weather_category,
        is_rainy_day
    FROM silver_weather_clean
    WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    
    logging.info(f"Extracting silver data from {start_date} to {end_date}")
    
    production_result = clickhouse_conn.execute_query(production_query)
    equipment_result = clickhouse_conn.execute_query(equipment_query)
    weather_result = clickhouse_conn.execute_query(weather_query)
    
    # Convert to DataFrames
    production_df = pd.DataFrame(production_result, 
                                columns=['date', 'mine_id', 'total_tons_extracted', 'avg_quality_grade', 'anomaly_flag'])
    equipment_df = pd.DataFrame(equipment_result, 
                               columns=['date', 'equipment_id', 'total_operational_hours', 'utilization_percentage', 
                                       'avg_fuel_consumption', 'fuel_efficiency'])
    weather_df = pd.DataFrame(weather_result, 
                             columns=['date', 'temperature_mean', 'precipitation_sum', 'weather_category', 'is_rainy_day'])
    
    logging.info(f"Extracted {len(production_df)} production, {len(equipment_df)} equipment, {len(weather_df)} weather records")
    return production_df, equipment_df, weather_df

def calculate_daily_metrics(production_df: pd.DataFrame, equipment_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate comprehensive daily production metrics"""
    
    # Get unique dates from all sources
    all_dates = set()
    if not production_df.empty:
        all_dates.update(production_df['date'].unique())
    if not equipment_df.empty:
        all_dates.update(equipment_df['date'].unique())
    if not weather_df.empty:
        all_dates.update(weather_df['date'].unique())
    
    if not all_dates:
        logging.warning("No dates found in any silver layer data")
        return pd.DataFrame()
    
    # Create base DataFrame with all dates
    daily_metrics = pd.DataFrame({'date': sorted(list(all_dates))})
    
    # Calculate production metrics
    if not production_df.empty:
        daily_production = production_df.groupby('date').agg({
            'total_tons_extracted': 'sum',
            'avg_quality_grade': 'mean',
            'mine_id': 'nunique',
            'anomaly_flag': 'any'
        }).reset_index()
        
        daily_production.columns = ['date', 'total_production_daily', 'average_quality_grade', 'operational_mines_count', 'has_anomaly']
        daily_metrics = daily_metrics.merge(daily_production, on='date', how='left')
    else:
        daily_metrics['total_production_daily'] = 0
        daily_metrics['average_quality_grade'] = np.nan
        daily_metrics['operational_mines_count'] = 0
        daily_metrics['has_anomaly'] = False
    
    # Calculate equipment metrics
    if not equipment_df.empty:
        daily_equipment = equipment_df.groupby('date').agg({
            'utilization_percentage': 'mean',
            'fuel_efficiency': 'mean',
            'equipment_id': 'nunique'
        }).reset_index()
        
        daily_equipment.columns = ['date', 'equipment_utilization', 'fuel_efficiency', 'total_equipment_count']
        daily_metrics = daily_metrics.merge(daily_equipment, on='date', how='left')
    else:
        daily_metrics['equipment_utilization'] = 0
        daily_metrics['fuel_efficiency'] = 0
        daily_metrics['total_equipment_count'] = 0
    
    # Add weather data
    if not weather_df.empty:
        daily_metrics = daily_metrics.merge(weather_df[['date', 'precipitation_sum', 'is_rainy_day']], on='date', how='left')
    else:
        daily_metrics['precipitation_sum'] = 0
        daily_metrics['is_rainy_day'] = False
    
    # Calculate weather impact score
    daily_metrics['weather_impact_score'] = calculate_weather_impact(daily_metrics)
    
    # Add performance categories
    daily_metrics['quality_performance'] = daily_metrics['average_quality_grade'].apply(categorize_quality_performance)
    daily_metrics['production_trend'] = calculate_production_trend(daily_metrics)
    
    # Fill missing values
    daily_metrics = daily_metrics.fillna({
        'total_production_daily': 0,
        'average_quality_grade': 0,
        'equipment_utilization': 0,
        'fuel_efficiency': 0,
        'weather_impact_score': 0,
        'operational_mines_count': 0,
        'total_equipment_count': 0,
        'has_anomaly': False,
        'precipitation_sum': 0,
        'is_rainy_day': False
    })
    
    # Add processing timestamp
    daily_metrics['processed_timestamp'] = datetime.now()
    
    logging.info(f"Calculated daily metrics for {len(daily_metrics)} days")
    return daily_metrics

def calculate_weather_impact(df: pd.DataFrame) -> pd.Series:
    """Calculate weather impact score on production"""
    
    # Simple scoring: negative impact for rainy days, neutral otherwise
    weather_impact = np.where(df['is_rainy_day'], -1, 0)
    
    # Additional impact based on precipitation amount
    precip_impact = np.where(df['precipitation_sum'] > 10, -2, 
                            np.where(df['precipitation_sum'] > 5, -1, 0))
    
    # Combine impacts and normalize to 0-10 scale
    combined_impact = weather_impact + precip_impact
    normalized_impact = np.clip((combined_impact + 3) * 10 / 6, 0, 10)
    
    return normalized_impact

def categorize_quality_performance(quality_grade):
    """Categorize quality performance"""
    if pd.isna(quality_grade):
        return 'No Data'
    elif quality_grade < 3.0:
        return 'Poor'
    elif quality_grade < 4.0:
        return 'Fair'
    elif quality_grade < 5.0:
        return 'Good'
    else:
        return 'Excellent'

def calculate_production_trend(df: pd.DataFrame) -> pd.Series:
    """Calculate production trend indicators"""
    
    # Simple 3-day moving average trend
    if len(df) < 3:
        return pd.Series(['Stable'] * len(df), index=df.index)
    
    df_sorted = df.sort_values('date')
    production_ma = df_sorted['total_production_daily'].rolling(window=3, min_periods=1).mean()
    
    trends = []
    for i in range(len(df_sorted)):
        if i < 2:
            trends.append('Stable')
        else:
            current = production_ma.iloc[i]
            previous = production_ma.iloc[i-1]
            if current > previous * 1.1:
                trends.append('Increasing')
            elif current < previous * 0.9:
                trends.append('Decreasing')
            else:
                trends.append('Stable')
    
    return pd.Series(trends, index=df_sorted.index)

def validate_gold_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate gold layer data"""
    
    # Check for negative production
    negative_production = df[df['total_production_daily'] < 0]
    if len(negative_production) > 0:
        logging.error(f"Found {len(negative_production)} records with negative production")
    
    # Check utilization percentage bounds
    invalid_utilization = df[(df['equipment_utilization'] < 0) | (df['equipment_utilization'] > 100)]
    if len(invalid_utilization) > 0:
        logging.warning(f"Found {len(invalid_utilization)} records with invalid utilization percentage")
    
    # Check weather impact score bounds
    invalid_weather_score = df[(df['weather_impact_score'] < 0) | (df['weather_impact_score'] > 10)]
    if len(invalid_weather_score) > 0:
        logging.warning(f"Found {len(invalid_weather_score)} records with invalid weather impact score")
    
    # Log summary statistics
    logging.info(f"Daily metrics summary:")
    logging.info(f"  Average daily production: {df['total_production_daily'].mean():.2f} tons")
    logging.info(f"  Average quality grade: {df['average_quality_grade'].mean():.2f}")
    logging.info(f"  Average equipment utilization: {df['equipment_utilization'].mean():.2f}%")
    logging.info(f"  Days with anomalies: {df['has_anomaly'].sum()}")
    
    return df

def load_to_gold(clickhouse_conn: ClickHouseConnector, df: pd.DataFrame):
    """Load daily metrics to gold layer"""
    
    # Clear existing data for the date range
    if not df.empty:
        min_date = df['date'].min()
        max_date = df['date'].max()
        
        delete_query = f"""
        ALTER TABLE gold_daily_production_metrics 
        DELETE WHERE date BETWEEN '{min_date}' AND '{max_date}'
        """
        clickhouse_conn.execute_command(delete_query)
        logging.info(f"Cleared existing gold data from {min_date} to {max_date}")
    
    # Select and reorder columns for the gold table
    gold_columns = [
        'date', 'total_production_daily', 'average_quality_grade', 
        'equipment_utilization', 'fuel_efficiency', 'weather_impact_score',
        'operational_mines_count', 'total_equipment_count', 
        'quality_performance', 'production_trend', 'processed_timestamp'
    ]
    
    df_gold = df[gold_columns].copy()
    
    # Insert new data
    clickhouse_conn.insert_dataframe('gold_daily_production_metrics', df_gold)
    logging.info(f"Loaded {len(df_gold)} daily metrics to gold layer")

def main():
    """Main execution function"""
    setup_logging()
    config = get_config()
    
    try:
        # Initialize ClickHouse connection
        clickhouse_conn = ClickHouseConnector(config.clickhouse)
        
        # Extract data from silver layers
        production_df, equipment_df, weather_df = extract_silver_data(clickhouse_conn)
        
        # Calculate daily metrics
        daily_metrics_df = calculate_daily_metrics(production_df, equipment_df, weather_df)
        
        if daily_metrics_df.empty:
            logging.warning("No daily metrics calculated")
            return
        
        # Validate gold data
        validated_df = validate_gold_data(daily_metrics_df)
        
        # Load to gold layer
        load_to_gold(clickhouse_conn, validated_df)
        
        logging.info("Gold daily metrics pipeline completed successfully")
        
    except Exception as e:
        logging.error(f"Gold daily metrics pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()