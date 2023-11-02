import os
import pandas as pd
import requests
from sqlalchemy import create_engine
import time

# Function to fetch data and store in Postgres
def fetch_and_store_data():
    # Fetching data from API
    response = requests.get("https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_status.json")
    data = response.json()

    df_station_status = pd.DataFrame(data['data']['stations'])
    # Add a column for the current timestamp
    df_station_status['data_retrieved'] = time.time()

    response = requests.get("https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json")
    data = response.json()

    df_station_info = pd.DataFrame(data['data']['stations'])
    df_station_info['data_retrieved'] = time.time()

    response = requests.get("https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/free_bike_status.json")
    data = response.json()

    df_bike_status = pd.DataFrame(data['data']['bikes'])
    df_bike_status['data_retrieved'] = time.time()

    # Establish connection to the Postgres database
    engine = create_engine(os.getenv("db_connection_url"))

    # Store data in Postgres
    df_station_status.to_sql('station_status', engine, if_exists='append', index=False)
    df_station_info.to_sql('station_info', engine, if_exists='append', index=False)
    df_bike_status.to_sql('bike_status', engine, if_exists='append', index=False)

    # Close the database connection
    engine.dispose()

fetch_and_store_data()