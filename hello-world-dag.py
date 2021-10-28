from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import requests
import json

def print_hello():
    url = 'http://bkf-onboarding.backoffice.svc.cluster.local:8090/bmais-backoffice/v1/onboardings/job-pendencias-rfb/iniciar'
    body = {'name': 'Maryja'}
    headers = {'idRequisicao': '77777'}
    r = requests.post(url, data=json.dumps(body), headers=headers)
  
args = {
    'owner': 'Lucas Baiao & Sette',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 22, 15, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(dag_id="hello_world", default_args=args, schedule_interval='5 * * * *', catchup=False, concurrency=1, max_active_runs=1)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
