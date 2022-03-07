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
        
        #float_columns = ['x_coordinate','y_coordinate','latitude','longitude']
        float_columns=[]
        for col in float_columns:
            self.data[col] = self.data[col].astype('float')
        

        #int_columns = ['id','year']
        int_columns = []
        for col in int_columns:
            self.data[col] = self.data[col].astype('int64')
        
        bool_columns = ['arrest','domestic']
        for col in bool_columns:
            self.data[col] = self.data[col].astype('bool')
        
    
        self.data['location'] = self.data['location'].apply(lambda x: json.dumps(x))


        self.data = self.data.replace({np.nan: None})

        self.data['db_updated_on'] = dt.datetime.now()
        self.data['db_updated_on'] = self.data['db_updated_on'].astype('datetime64[ns]')

        print(self.data['db_updated_on'])
        return self.data