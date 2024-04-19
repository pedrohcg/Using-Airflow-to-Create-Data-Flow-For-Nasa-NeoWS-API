import pyodbc
import datetime as dt
from airflow import DAG
from pipeline import exec
from datetime import datetime
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Pedro',
    'depends_on_past': False,
    'start_date': '2024-01-01',
    'email': ['email@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('dag_pipeline_nasa',
          default_args = default_args,
          description = 'Pipeline ETL using Nasa API',
          schedule_interval = timedelta(minutes=60)
)

execute_etl = PythonOperator(task_id = 'etl_python_script',
                             python_callable = exec,
                             dag = dag
)

execute_etl