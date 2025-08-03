#!/usr/bin/env python3
"""
Coal Mining Data Pipeline Orchestration
Runs the complete medallion architecture pipeline
"""

import sys
import os
import logging
import subprocess
import time
from datetime import datetime
from config import get_config

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('/app/logs/pipeline_orchestration.log'),
            logging.StreamHandler()
        ]
    )

def run_script(script_path: str, layer: str, source: str) -> bool:
    """Run a pipeline script and return success status"""
    
    script_name = f"{layer}_{source}.py"
    full_path = os.path.join('/app/scripts', script_name)
    
    logging.info(f"Starting {layer} layer - {source} pipeline")
    start_time = time.time()
    
    try:
        result = subprocess.run([
            'python3', full_path
        ], 
        capture_output=True, 
        text=True, 
        timeout=600  # 10 minute timeout
        )
        
        end_time = time.time()
        duration = end_time - start_time
        
        if result.returncode == 0:
            logging.info(f"✅ {layer} layer - {source} completed successfully in {duration:.2f}s")
            return True
        else:
            logging.error(f"❌ {layer} layer - {source} failed with return code {result.returncode}")
            logging.error(f"STDOUT: {result.stdout}")
            logging.error(f"STDERR: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logging.error(f"❌ {layer} layer - {source} timed out after 10 minutes")
        return False
    except Exception as e:
        logging.error(f"❌ {layer} layer - {source} failed with exception: {e}")
        return False

def run_bronze_layer() -> bool:
    """Run all bronze layer pipelines"""
    
    logging.info("🥉 Starting Bronze Layer Processing")
    
    bronze_scripts = [
        ('bronze', 'production'),
        ('bronze', 'equipment'),
        ('bronze', 'weather')
    ]
    
    success_count = 0
    
    for layer, source in bronze_scripts:
        if run_script('', layer, source):
            success_count += 1
        time.sleep(2)  # Small delay between scripts
    
    if success_count == len(bronze_scripts):
        logging.info("🥉 Bronze Layer completed successfully")
        return True
    else:
        logging.error(f"🥉 Bronze Layer completed with {len(bronze_scripts) - success_count} failures")
        return False

def run_silver_layer() -> bool:
    """Run all silver layer pipelines"""
    
    logging.info("🥈 Starting Silver Layer Processing")
    
    silver_scripts = [
        ('silver', 'production'),
        ('silver', 'equipment'),
        ('silver', 'weather')
    ]
    
    success_count = 0
    
    for layer, source in silver_scripts:
        if run_script('', layer, source):
            success_count += 1
        time.sleep(2)  # Small delay between scripts
    
    if success_count == len(silver_scripts):
        logging.info("🥈 Silver Layer completed successfully")
        return True
    else:
        logging.error(f"🥈 Silver Layer completed with {len(silver_scripts) - success_count} failures")
        return False

def run_gold_layer() -> bool:
    """Run all gold layer pipelines"""
    
    logging.info("🥇 Starting Gold Layer Processing")
    
    gold_scripts = [
        ('gold', 'daily_metrics'),
        ('gold', 'mine_performance')
    ]
    
    success_count = 0
    
    for layer, source in gold_scripts:
        if run_script('', layer, source):
            success_count += 1
        time.sleep(2)  # Small delay between scripts
    
    if success_count == len(gold_scripts):
        logging.info("🥇 Gold Layer completed successfully")
        return True
    else:
        logging.error(f"🥇 Gold Layer completed with {len(gold_scripts) - success_count} failures")
        return False

def check_prerequisites() -> bool:
    """Check if all prerequisites are met"""
    
    logging.info("🔍 Checking prerequisites")
    
    # Check if required directories exist
    required_dirs = ['/app/scripts', '/app/data', '/app/logs']
    for dir_path in required_dirs:
        if not os.path.exists(dir_path):
            logging.error(f"Required directory does not exist: {dir_path}")
            return False
    
    # Check if required files exist
    required_scripts = [
        'bronze_production.py',
        'bronze_equipment.py', 
        'bronze_weather.py',
        'silver_production.py',
        'silver_equipment.py',
        'silver_weather.py',
        'gold_daily_metrics.py',
        'gold_mine_performance.py'
    ]
    
    for script in required_scripts:
        script_path = os.path.join('/app/scripts', script)
        if not os.path.exists(script_path):
            logging.error(f"Required script does not exist: {script_path}")
            return False
    
    # Check if CSV file exists
    csv_path = '/app/data/equipment_sensors.csv'
    if not os.path.exists(csv_path):
        logging.warning(f"CSV file does not exist: {csv_path}")
        # This is a warning, not a failure, as the file might be mounted differently
    
    logging.info("✅ Prerequisites check completed")
    return True

def create_pipeline_summary(bronze_success: bool, silver_success: bool, gold_success: bool, total_duration: float):
    """Create and log pipeline execution summary"""
    
    logging.info("📊 PIPELINE EXECUTION SUMMARY")
    logging.info("=" * 50)
    logging.info(f"Pipeline executed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"Total execution time: {total_duration:.2f} seconds")
    logging.info("")
    logging.info("Layer Results:")
    logging.info(f"  🥉 Bronze Layer: {'✅ SUCCESS' if bronze_success else '❌ FAILED'}")
    logging.info(f"  🥈 Silver Layer: {'✅ SUCCESS' if silver_success else '❌ FAILED'}")
    logging.info(f"  🥇 Gold Layer:   {'✅ SUCCESS' if gold_success else '❌ FAILED'}")
    logging.info("")
    
    overall_success = bronze_success and silver_success and gold_success
    logging.info(f"Overall Pipeline Status: {'✅ SUCCESS' if overall_success else '❌ FAILED'}")
    logging.info("=" * 50)

def main():
    """Main pipeline orchestration function"""
    
    setup_logging()
    
    pipeline_start_time = time.time()
    
    logging.info("🚀 Starting Coal Mining Data Pipeline")
    logging.info("=" * 50)
    
    # Check prerequisites
    if not check_prerequisites():
        logging.error("❌ Prerequisites check failed. Aborting pipeline.")
        sys.exit(1)
    
    # Initialize success flags
    bronze_success = False
    silver_success = False
    gold_success = False
    
    try:
        # Run Bronze Layer
        bronze_success = run_bronze_layer()
        
        # Run Silver Layer (only if Bronze succeeded)
        if bronze_success:
            silver_success = run_silver_layer()
        else:
            logging.error("🥈 Skipping Silver Layer due to Bronze Layer failures")
        
        # Run Gold Layer (only if Silver succeeded)
        if silver_success:
            gold_success = run_gold_layer()
        else:
            logging.error("🥇 Skipping Gold Layer due to Silver Layer failures")
        
    except KeyboardInterrupt:
        logging.warning("⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"❌ Pipeline failed with unexpected error: {e}")
        sys.exit(1)
    finally:
        pipeline_end_time = time.time()
        total_duration = pipeline_end_time - pipeline_start_time
        
        # Create summary
        create_pipeline_summary(bronze_success, silver_success, gold_success, total_duration)
        
        # Exit with appropriate code
        if bronze_success and silver_success and gold_success:
            logging.info("🎉 Pipeline completed successfully!")
            sys.exit(0)
        else:
            logging.error("💥 Pipeline completed with failures!")
            sys.exit(1)

if __name__ == "__main__":
    main()