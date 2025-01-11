import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os


def ingest_data(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.db
    table_name = params.table_name
    url = params.source_url

    # download source data and save it as source.csv in the same directory as this file
    source_name = 'source.csv'
    os.system(f"wget {url} -O {source_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    iter_df = pd.read_csv(source_name, chunksize=10, iterator=True)
    
    df_100k = next(iter_df)

    df_100k["tpep_pickup_datetime"] = pd.to_datetime(df_100k["tpep_pickup_datetime"])
    df_100k["tpep_dropoff_datetime"] = pd.to_datetime(df_100k["tpep_dropoff_datetime"])

    df_100k.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    # now insert the first 100k records of the dataframe
    df_100k.to_sql(name=table_name, con=engine, if_exists='append') # if the table already exists, append this record to it

    try:
        chunk_count = 0  
        for df_100k in iter_df:
            t_start = time()

            # Ensure datetime conversion only if needed
            if not pd.api.types.is_datetime64_any_dtype(df_100k["tpep_pickup_datetime"]):
                df_100k["tpep_pickup_datetime"] = pd.to_datetime(df_100k["tpep_pickup_datetime"])
            if not pd.api.types.is_datetime64_any_dtype(df_100k["tpep_dropoff_datetime"]):
                df_100k["tpep_dropoff_datetime"] = pd.to_datetime(df_100k["tpep_dropoff_datetime"])

            # Insert into database
            df_100k.to_sql(name=table_name, con=engine, if_exists='append', index=False)

            t_end = time()
            chunk_count += 1
            print(f"Inserted chunk {chunk_count}. Took {round(t_end - t_start, 4)} seconds.")

    except StopIteration:
        print("All data processed.")
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        print(f"Total chunks processed: {chunk_count}")



'''
db_params = {
    'host': 'localhost', # local machine
    'database': 'ny_taxi',
    'user': 'postgres',
    'password': '358',
    'port': '5433'  # localhost port 
}
Since data source is in our local directory, we will use python to access it on the web browser from our local 
comouter and download it from there:
> python -m http.server
source_url = http://localhost:8000/ny_taxi_source_data/yellow_head_100.csv
Check the IP address and the of the localhost in the url above with it
This is because using localhost is the local host of the container itself so it will not know how to 
find that of the computer
> ipconfig
source_url = http://machine_ipaddress:8000/ny_taxi_source_data/simple_data/yellow_head_100.csv

destination_table_name = yellow_taxi_2015_01_head
'''
if __name__ == "__main__":
    parser = argparse.ArgumentParser("Ingest CSV source data to Postgres.")

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database on postgres')
    parser.add_argument('--table_name', help='table in postgres we will ingest source data to')
    parser.add_argument('--source_url', help='url of the source csv data')

    args = parser.parse_args()
    ingest_data(args)






