import sys
import subprocess
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

sys.path.append('/app/scripts')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bronze_layer_pipeline',
    default_args=default_args,
    description='Bronze layer data ingestion',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    max_active_runs=1,
)

def run_bronze_script(script_name: str, **context):
    script_path = f'/app/scripts/{script_name}'
    result = subprocess.run(['python3', script_path], 
                          capture_output=True, text=True, 
                          timeout=600, cwd='/app')
    if result.returncode != 0:
        raise Exception(f"Script {script_name} failed: {result.stderr}")
    return result.stdout

check_prerequisites = BashOperator(
    task_id='check_prerequisites',
    bash_command='ls /app/scripts/bronze_*.py',
    dag=dag,
)

bronze_production = PythonOperator(
    task_id='bronze_production',
    python_callable=run_bronze_script,
    op_kwargs={'script_name': 'bronze_production.py'},
    dag=dag,
)

bronze_equipment = PythonOperator(
    task_id='bronze_equipment',
    python_callable=run_bronze_script,
    op_kwargs={'script_name': 'bronze_equipment.py'},
    dag=dag,
)

bronze_weather = PythonOperator(
    task_id='bronze_weather',
    python_callable=run_bronze_script,
    op_kwargs={'script_name': 'bronze_weather.py'},
    dag=dag,
)

validate_bronze = BashOperator(
    task_id='validate_bronze',
    bash_command='echo "Bronze layer completed"',
    dag=dag,
)

check_prerequisites >> [bronze_production, bronze_equipment, bronze_weather] >> validate_bronze