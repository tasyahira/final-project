from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retry_delay': timedelta(minutes=5),
}

# Paths to your Python scripts
src_to_stg = '/Users/tasyahira/Documents/bootcamp-ds/final-project/source/etl_src_to_stg.py'
stg_to_dwh = '/Users/tasyahira/Documents/bootcamp-ds/final-project/source/etl_stg_to_dwh.py'

def run_src_to_stg():
    os.system(f'python {src_to_stg}')

def run_stg_to_dwh():
    os.system(f'python {stg_to_dwh}')

with DAG('etl_covid_daily', default_args=default_args, schedule_interval='@daily') as dag:
    src_to_stg_task = PythonOperator(
        task_id='etl_src_to_stg',
        python_callable=run_src_to_stg
    )

    stg_to_dwh_task = PythonOperator(
        task_id='etl_stg_to_dwh',
        python_callable=run_stg_to_dwh
    )

    src_to_stg_task >> stg_to_dwh_task
