import os
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from datetime import datetime

# Function to convert non-numeric columns to strings
def convert_non_numeric_to_string(df):
    for col in df.columns:
        if df[col].dtype not in [int, float]:
            df[col] = df[col].astype(str)
    return df

# Function to convert binary columns to boolean
def convert_binary_to_boolean(df, cols):
    if set(cols).issubset(set(df.columns)):
        for col in cols:
            df[col] = df[col].astype('bool')
    return df

# Function to fetch data and store in Postgres
def fetch_and_store_data():
    # Establish connection to the Postgres database
    engine = create_engine(os.getenv("db_connection_url"))

    # Set the time data is being retrieved
    data_retrieved_utc = datetime.utcnow()

    data_urls = {
        "station_status": "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_status.json",
        "station_info": "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_information.json",
    }

    for table_name, url in data_urls.items():
        response = requests.get(url)
        data = response.json()

        if table_name == 'station_status':
            cols = [
                'station_id',
                'is_renting',
                'is_returning',
                'is_installed',
                'num_bikes_disabled',
                'num_docks_disabled',
                'num_bikes_available',
                'num_ebikes_available',
                'num_docks_available',
                'last_reported',
                'data_retrieved_ts'
            ]
        else:
            cols = [
                'station_id',
                'name',
                'lat',
                'lon',
                'capacity',
                'data_retrieved_ts'
            ]

        # Extract relevant columns and add a column for the current timestamp
        df = pd.DataFrame(data['data']['stations'])
        df = convert_non_numeric_to_string(df)
        df['data_retrieved_ts'] = pd.to_datetime(data_retrieved_utc, utc=True)
        df = df[cols]

        # Convert columns that are binary to boolean
        df = convert_binary_to_boolean(df, ['is_renting', 'is_returning', 'is_installed'])

        if table_name == 'station_status':
            df['last_reported'] = pd.to_datetime(df['last_reported'], unit='s', utc=True)

        # Generate a temporary table with the new data
        temp_table_name = f"temp_{table_name}"
        df.to_sql(temp_table_name, engine, if_exists='replace', index=False)

        # Use SQL to perform the merge
        merge_sql = f"""
            MERGE INTO {table_name} AS prod
            USING {temp_table_name} AS temp
            ON prod.station_id = temp.station_id
            AND {' AND '.join(f"prod.{col} = temp.{col}" for col in cols if col != 'station_id')}

            WHEN MATCHED THEN UPDATE SET
                data_retrieved_ts = temp.data_retrieved_ts
            
            WHEN NOT MATCHED THEN INSERT VALUES (
                {', '.join(cols)}
            );
        """

        conn = engine.connect()
        
        # Run the merge
        conn.execute(text(merge_sql))

        # Drop the temporary table
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table_name}"))

    # Close the database connection
    engine.dispose()

fetch_and_store_data()