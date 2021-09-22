from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

dag_args = {
    'owner': 'prabha.arivalagan',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 22),
    'email': ['prabhakaran.mails@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)}

dag = DAG(
    dag_id='record_count',
    start_date=datetime(2021, 9, 22),
    default_args=dag_args,
    end_date=None,
    schedule_interval='0 9 * * *')

t1 = BigQueryCheckOperator(task_id='check_data_exists',
                           sql="""
                            SELECT count(*) > 0 
                            FROM `chicago-crime-284514.crime_dataset.chicago_crime1`
                        """,
                           use_legacy_sql=False)

t2 = BigQueryOperator(
    task_id='count_record',
    use_legacy_sql=False,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    allow_large_results=True,
    bigquery_conn_id='bigquery_default',
    time_partitioning={
        "type": 'DAY'
        },
    sql='''
    #standardSQL
            select '{{ds}}' timestamp,count(*) from
    `chicago-crime-284514.crime_dataset.chicago_crime1`

    ''',
    destination_dataset_table='chicago-crime-284514:crime_dataset.record_count_test',
    dag=dag)

t1 >> t2