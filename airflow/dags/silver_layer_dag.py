import sys
import subprocess
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
    'silver_layer_pipeline',
    default_args=default_args,
    description='Silver layer data transformation',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    max_active_runs=1,
)

def run_silver_script(script_name: str, **context):
    script_path = f'/app/scripts/{script_name}'
    result = subprocess.run(['python3', script_path], 
                          capture_output=True, text=True, 
                          timeout=900, cwd='/app')
    if result.returncode != 0:
        raise Exception(f"Script {script_name} failed: {result.stderr}")
    return result.stdout

wait_for_bronze = ExternalTaskSensor(
    task_id='wait_for_bronze',
    external_dag_id='bronze_layer_pipeline',
    external_task_id='validate_bronze',
    timeout=3600,
    poke_interval=300,
    dag=dag,
)

check_bronze_data = BashOperator(
    task_id='check_bronze_data',
    bash_command='echo "Bronze data available"',
    dag=dag,
)

silver_production = PythonOperator(
    task_id='silver_production',
    python_callable=run_silver_script,
    op_kwargs={'script_name': 'silver_production.py'},
    dag=dag,
)

silver_equipment = PythonOperator(
    task_id='silver_equipment',
    python_callable=run_silver_script,
    op_kwargs={'script_name': 'silver_equipment.py'},
    dag=dag,
)

silver_weather = PythonOperator(
    task_id='silver_weather',
    python_callable=run_silver_script,
    op_kwargs={'script_name': 'silver_weather.py'},
    dag=dag,
)

validate_silver = BashOperator(
    task_id='validate_silver',
    bash_command='echo "Silver layer completed"',
    dag=dag,
)

wait_for_bronze >> check_bronze_data >> [silver_production, silver_equipment, silver_weather] >> validate_silver