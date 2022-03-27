from chicago_crime_etl.extract import Extract
from chicago_crime_etl.transformation import Transformation
from chicago_crime_etl.load_to_datalake import LoadToDataLake 

from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

import datetime as dt
import pendulum

from airflow.models import Variable


local_tz = pendulum.timezone('America/Chicago')

def run_etl(**kwargs):
    start_time = kwargs.get('templates_dict').get('start_time',None)
    end_time = kwargs.get('templates_dict').get('end_time',None)
    extract=Extract(start_time,end_time)
    df = extract.execute_extraction()
    if not df.empty:
        transform = Transformation(df)
        transformed_df = transform.convert_datatype_datalake()
        load = LoadToDataLake('af_data_lake','crime_data')
        load.execute_load(transformed_df)

default_args = {
    'retries' : 0,
    'retry_delay' : dt.timedelta(minutes=1),
    'email_on_retry' : True,
    'email_on_failure' :True,
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
    templates_dict = {'start_time': "{{ prev_execution_date.strftime('%Y-%m-%dT%H:%M:%S') }}", 'end_time': "{{ execution_date.strftime('%Y-%m-%dT%H:%M:%S') }}"},
    provide_context=True,
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

warehouse_load = BigQueryOperator(
    task_id  = 'load_to_wh',
    sql = 'warehouse_load.sql',
    create_disposition = False,
    write_disposition = False,
    schema_update_options=False
)


t1 >> t2 >> [email,warehouse_load]
