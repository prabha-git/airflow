import pandas as pd
import datetime as dt
from sodapy import Socrata

class Extract:
    chicago_crime_portal = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"

    def __init__(self, date : dt.date) -> None:
        self.date = date.replace(second=0, microsecond=0, minute=0, hour=0)
        self.client = Socrata("data.cityofchicago.org", app_token="Ttz4HIh52J3g53HKTYKMNxu4M")

        # Get all the updates in the last week.
        self.updated_on_filter = "date_trunc_ymd(updated_on) >= '"+ dt.datetime.strftime(self.date,'%Y-%m-%d') +"'"

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