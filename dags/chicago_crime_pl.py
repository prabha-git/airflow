from chicago_crime_etl.extract import Extract
from chicago_crime_etl.transformation import Transformation
from chicago_crime_etl.load_to_datalake import LoadToDataLake 

from airflow import DAG
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.bash_operator import BashOperator

import datetime as dt

default_args = {
    'retries' : 2,
    'retry_delay' : dt.timedelta(minutes=1),
    'email_on_retry' : False,
    'email_on_failure' : False,
}

dag = DAG(
    'my_first_dag',
    default_args=default_args,
    start_date=dt.datetime(2022,2,20),
    schedule_interval='*/5 * * * *',
    catchup=True
)


t1 = BashOperator(
    task_id = 'write_bash_to_text',
    bash_command='echo "Test Text {{ run_id }}" >> ~/prabha.txt',
    dag=dag
)

t2 = BashOperator(
    task_id='count_line',
    bash_command='wc -l ~/prabha.txt >> ~/prabha_count.txt',
    dag=dag
)

t1 >> t2
