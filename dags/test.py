from airflow.operators import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

import csv
import os

args = {
    'owner': 'dbergz',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 25),
    'email': ['dylan.bergey@hpfc.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id = 'csv_query',
    default_args = args,
    schedule_interval = None,
    tags=['CSV_TESTING'],
    )

def get_csv(**kwargs):
    dir = r'C:/Users/dylan.bergey/Downloads/'
    file = 'test_csv.csv'
    path = os.path.join(r"C:\Users\Downloads", file)
    for file in os.listdir(dir):
        print (file)

getting_csv = PythonOperator(
    task_id = 'printing_csv_rows',
    python_callable=get_csv,
    dag = dag,
    )

getting_csv
