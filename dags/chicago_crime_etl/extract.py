import pandas as pd
import datetime as dt
from sodapy import Socrata
from airflow.hooks.base_hook import BaseHook

class Extract:
    chicago_crime_portal = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"

    def __init__(self, start_time , end_time ) -> None:
        self.start_time = start_time
        self.end_time = end_time
        self.client = Socrata("data.cityofchicago.org", app_token=BaseHook.get_connection("CITY_OF_CHICAGO_APP_TOKEN").password)

        # Get all the updates in the last week.
        self.updated_on_filter = "updated_on >= '"+ start_time  +"' and updated_on < '"+ end_time +"'"

    def execute_extraction(self) -> pd.DataFrame:

        crimes = self.client.get_all("ijzp-q8t2",where = self.updated_on_filter)
        crime_df = pd.DataFrame.from_records(crimes)
        print(crime_df.head(5))
        print(crime_df.columns)
        return crime_df

if __name__=="__main__":
    extract = Extract(dt.datetime.now()+dt.timedelta(days=-3))
    data = extract.execute_extraction()
    print(data)
