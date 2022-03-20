import pandas as pd
import logging
import numpy as np
import json
import datetime as dt
from datetime import timezone

class Transformation:
    def __init__(self, data : pd.DataFrame) -> None:
        self.data=data

    def convert_datatype_datalake(self) -> pd.DataFrame:
        logging.info('Starting the datatype conversion')
        # Data type conversion
        date_columns = ['updated_on','date']
        for col in date_columns:
            self.data[col] = self.data[col].astype('datetime64[ns]')
        
        float_columns = ['x_coordinate','y_coordinate','latitude','longitude']
        for col in float_columns:
            self.data[col] = self.data[col].astype('float')
        

        int_columns = ['id','year']
        for col in int_columns:
            self.data[col] = self.data[col].astype('int64')
        
        bool_columns = ['arrest','domestic']
        for col in bool_columns:
            self.data[col] = self.data[col].astype('bool')
        
        self.data['location'].fillna("",inplace=True)
    
        self.data['location'] = self.data['location'].apply(lambda x: self.dict_clean(x))


        

        self.data['db_updated_on'] = dt.datetime.now()
        self.data['db_updated_on'] = self.data['db_updated_on'].astype('datetime64[ns]')
        
        self.data = self.data.replace({np.nan: None})
        return self.data
    
    def dict_clean(self,x):
        if not x:
            return None
        print(x)
        for k,v in x.items():
            if k == 'latitude' or k=='longitude':
                x[k] = float(x[k])
            if k == 'human_address':
                x[k]={}
        return x
