"""Airflow is optional: business logic stays in Go, this DAG invokes its API."""
import json
import os
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from airflow import DAG
from airflow.operators.python import PythonOperator


def train():
    request = Request(
        os.environ['RECOGO_URL'] + '/admin/train',
        data=b'{}',
        headers={'Content-Type': 'application/json',
                 'Authorization': 'Bearer ' + os.environ['ADMIN_TOKEN']},
        method='POST',
    )
    with urlopen(request, timeout=120) as response:
        result = json.load(response)
    if not result['accepted']:
        raise RuntimeError('Quality gate rejected candidate: ' + result['reason'])
    return result['version']


with DAG('recogo_retrain', start_date=datetime(2025, 1, 1), schedule='0 * * * *',
         catchup=False, max_active_runs=1, is_paused_upon_creation=True,
         default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)}) as dag:
    PythonOperator(task_id='train_validate_publish_go_model', python_callable=train)
