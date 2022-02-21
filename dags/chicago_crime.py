from chicago_crime_etl.extract import Extract
from chicago_crime_etl.transformation import Transformation
from chicago_crime_etl.load_to_datalake import LoadToDataLake 

from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.bash_operator import BashOperator

import datetime as dt


def run_etl(ds=None):
	extract=Extract(dt.datetime.strptime(ds,'%Y-%m-%d'))
	df = extract.execute_extraction()
	transform = Transformation(df)
	transformed_df = transform.convert_datatype()
	load = LoadToDataLake('af_data_lake','crime_data')
	load.execute_load(transformed_df)

default_args = {
    'retries' : 0,
    'retry_delay' : dt.timedelta(minutes=1),
    'email_on_retry' : False,
    'email_on_failure' : False,
}

dag = DAG(
    'chicago_crime_pipeline1',
    default_args=default_args,
    start_date=dt.datetime(2022,2,11),
    schedule_interval='0 0 * * *',
    catchup=True
)


t1 = PythonOperator(
    task_id = 'run_etl',
    python_callable=run_etl,
    dag=dag
)

t2 = BashOperator(
    task_id='count_line',
    bash_command='wc -l ~/prabha.txt >> ~/prabha_count.txt',
    dag=dag
)

t1 >> t2
