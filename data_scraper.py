import os
import pandas as pd
import requests
from sqlalchemy import create_engine
import time

# Function to convert non-numeric columns to strings
def convert_non_numeric_to_string(df):
    for col in df.columns:
        if df[col].dtype not in [int, float]:
            df[col] = df[col].astype(str)
    return df

# Function to fetch data and store in Postgres
def fetch_and_store_data():
    # Establish connection to the Postgres database
    engine = create_engine(os.getenv("db_connection_url"))

    # Set the time data is being retrieved
    data_retrieved = time.time()

    data_urls = {
        "station_status": "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_status.json",
        "station_info": "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json",
        "bike_status": "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/free_bike_status.json"
    }

    for table_name, url in data_urls.items():
        response = requests.get(url)
        data = response.json()
        if table_name == 'bike_status':
            df = pd.DataFrame(data['data']['bikes'])
        else:
            df = pd.DataFrame(data['data']['stations'])

        # Add a column for the current timestamp
        df['data_retrieved'] = data_retrieved
        df = convert_non_numeric_to_string(df)

        df.to_sql(table_name, engine, if_exists='append', index=False)

    # Close the database connection
    engine.dispose()

fetch_and_store_data()
