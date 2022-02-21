from xmlrpc.client import Boolean
import pandas as pd
import datetime as dt
from google.cloud import bigquery

class LoadToDataLake:
    

    def __init__(self, dataset,table) -> None:
        self.columns =  ['id','case_number','date','block','iucr','primary_type','description','location_description','arrest','domestic','beat','district','ward','community_area','fbi_code','x_coordinate',
           'y_coordinate','year','updated_on','latitude','longitude','location']

        bq_client = bigquery.Client()
        dataset_ref = bq_client.dataset(dataset)
        table_ref=dataset_ref.table(table)
        table = bq_client.get_table(table_ref)
        

    def execute_load(self,df_to_insert) -> Boolean:
        errors = self.bq_client.insert_rows(self.table,df_to_insert)
        print(errors)
        assert errors == []

