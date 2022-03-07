from chicago_crime_etl.extract import Extract
from chicago_crime_etl.transformation import Transformation
from chicago_crime_etl.load_to_datalake import LoadToDataLake 

from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

import datetime as dt
import pendulum


local_tz = pendulum.timezone('America/Chicago')

def run_etl(ds=None):
    extract=Extract(dt.datetime.strptime(ds,'%Y-%m-%d'))
    df = extract.execute_extraction()
    if df:
        transform = Transformation(df)
        transformed_df = transform.convert_datatype_datalake()
        load = LoadToDataLake('af_data_lake','crime_data')
        load.execute_load(transformed_df)

default_args = {
    'retries' : 0,
    'retry_delay' : dt.timedelta(minutes=1),
    'email_on_retry' : False,
    'email_on_failure' : False,
    'email': ['prabhakaran.mails@gmail.com']
}

dag = DAG(
    'chicago_crime_pipeline5',
    default_args=default_args,
    start_date=dt.datetime(2022,3,1,tzinfo=local_tz),
    schedule_interval='0 0 * * *',
    catchup=True
)


t1 = PythonOperator(
    task_id = 'run_etl',
    python_callable=run_etl,
    dag=dag
)

t2 = BigQueryCheckOperator(
    task_id="check_etl_completion",
    sql=f"SELECT COUNT(*) FROM airflow-341215.af_data_lake.crime_data",
    use_legacy_sql=False
)


email = EmailOperator(
        task_id='send_email',
        to='prabhakaran.mails@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Airflow Pipeline Complete !!! t</h3> """,
        dag=dag
)


t1 >> t2 >> email
