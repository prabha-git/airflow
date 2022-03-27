from xmlrpc.client import Boolean
import pandas as pd
import datetime as dt
from google.cloud import bigquery

class LoadToDataLake:
    

    def __init__(self, dataset,table) -> None:
        self.columns =  ['id','case_number','date','block','iucr','primary_type','description','location_description','arrest','domestic','beat','district','ward','community_area','fbi_code','x_coordinate',
           'y_coordinate','year','updated_on','latitude','longitude','location','db_updated_on']

        self.bq_client = bigquery.Client()
        # dataset_ref = bq_client.dataset(dataset)
        # table_ref=dataset_ref.table(table)
        # table = bq_client.get_table(table_ref)
        self.table_id = 'airflow-341215.af_data_lake.crime_data'
        self.job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        

    def execute_load(self,df_to_insert) -> None:
        job = self.bq_client.load_table_from_dataframe(df_to_insert,self.table_id,self.job_config)
        print(f"job results is {job.result()}")


class LoadToDataWarehouse:

    def __init__(self):
        self.bq_client = bigquery.Client()
        sql_file = open("../warehouse_load.sql",'r')
        self.sql = sql_file.read()
        sql_file.close()

    def execute_load(self) -> None:
        result = self.bq_client.query(self.query).result()




