import pandas as pd
import logging
import numpy as np
import json
import datetime as dt
from datetime import timezone

class Transformation:
    def __init__(self, data : pd.DataFrame) -> None:
        self.data=data
        #self.data['updated_on'] = dt.datetime.now(timezone.utc)

    def convert_datatype(self) -> pd.DataFrame:
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
        
    
        #self.data['location'] = self.data['location'].apply(lambda x: json.loads(x))
        self.data['location'] = self.data['location'].apply(lambda x:x['latitude'].astyple('float'))
	print(self.data['location'][0])
        print(type(self.data['location'][0]))
        self.data = self.data.replace({np.nan: None})

        return self.data
