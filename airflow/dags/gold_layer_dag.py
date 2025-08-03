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
    'gold_layer_pipeline',
    default_args=default_args,
    description='Gold layer analytics',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    max_active_runs=1,
)

def run_gold_script(script_name: str, **context):
    script_path = f'/app/scripts/{script_name}'
    result = subprocess.run(['python3', script_path], 
                          capture_output=True, text=True, 
                          timeout=1200, cwd='/app')
    if result.returncode != 0:
        raise Exception(f"Script {script_name} failed: {result.stderr}")
    return result.stdout

wait_for_silver = ExternalTaskSensor(
    task_id='wait_for_silver',
    external_dag_id='silver_layer_pipeline',
    external_task_id='validate_silver',
    timeout=3600,
    poke_interval=300,
    dag=dag,
)

check_silver_data = BashOperator(
    task_id='check_silver_data',
    bash_command='echo "Silver data available"',
    dag=dag,
)

gold_daily_metrics = PythonOperator(
    task_id='gold_daily_metrics',
    python_callable=run_gold_script,
    op_kwargs={'script_name': 'gold_daily_metrics.py'},
    dag=dag,
)

gold_mine_performance = PythonOperator(
    task_id='gold_mine_performance',
    python_callable=run_gold_script,
    op_kwargs={'script_name': 'gold_mine_performance.py'},
    dag=dag,
)

validate_gold = BashOperator(
    task_id='validate_gold',
    bash_command='echo "Gold layer completed"',
    dag=dag,
)

wait_for_silver >> check_silver_data >> [gold_daily_metrics, gold_mine_performance] >> validate_gold