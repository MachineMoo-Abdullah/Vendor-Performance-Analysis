import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time


import importlib
importlib.reload(logging)

logging.basicConfig(
    filename='/content/drive/MyDrive/Project/logs/ingestion_db.log',
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode='a'
)

engine = create_engine('sqlite:///drive/MyDrive/Project/inventory.db')

def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def load_raw_data():
    start = time.time()
    for file in os.listdir('drive/MyDrive/Project/data/'):
        if '.csv' in file:
            df = pd.read_csv(f'drive/MyDrive/Project/data/{file}')
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)
            print(df.shape)

    end = time.time()
    total_time = (end - start) / 60
    logging.info('-------Ingestion Complete------')
    print(total_time)
    logging.info(f'Total Time taken: {total_time} minutes')

if __name__ == '__main__':
    load_raw_data()
