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
    
    response = requests.get("https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_status.json")
    data = response.json()
    df = pd.DataFrame(data['data']['stations'])

    # Add a column for the current timestamp
    df['data_retrieved'] = time.time()
    df = convert_non_numeric_to_string(df)

    df.to_sql('station_status', engine, if_exists='append', index=False)

    response = requests.get("https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json")
    data = response.json()
    df = pd.DataFrame(data['data']['stations'])
    
    # Add a column for the current timestamp
    df['data_retrieved'] = time.time()
    df = convert_non_numeric_to_string(df)

    df.to_sql('station_info', engine, if_exists='append', index=False)

    response = requests.get("https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/free_bike_status.json")
    data = response.json()
    df = pd.DataFrame(data['data']['bikes'])

    # Add a column for the current timestamp
    df['data_retrieved'] = time.time()
    df = convert_non_numeric_to_string(df)

    df.to_sql('bike_status', engine, if_exists='append', index=False)

    # Close the database connection
    engine.dispose()

fetch_and_store_data()