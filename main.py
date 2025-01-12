import os
import yaml
from datetime import datetime, timedelta
import random
import string
import pytz
from elastic_client import ElasticSearchClient
from helper import (
    rename_columns_in_dataframe, 
    load_mapping_from_json, 
    convert_integer_dataframe, 
    fillna_df, 
    format_date_columns,
    documents_to_csv
)
import pandas as pd

CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
es_client = ElasticSearchClient()

def main():
    response = es_client.smart_search(index_name="expenses")
    CONFIG_PATH = os.path.join(CURRENT_DIRECTORY, 'output.csv')
    documents_to_csv(response.get("documents", {}), CONFIG_PATH)
    
def process_df_and_ingest_es():
    CONFIG_PATH = os.path.join(CURRENT_DIRECTORY, 'config.yaml')
    with open(CONFIG_PATH, 'r', encoding="utf-8") as fr:
        CONFIG = yaml.safe_load(fr)

    FILE_PATH = os.path.join(CURRENT_DIRECTORY, 'expense_2023.xlsx')
    df = pd.read_excel(FILE_PATH)
    print(df.info())
    df = df.apply(lambda x: x.astype(str).str.strip() if x.dtype == "object" else x)
    
    FILE_PATH = os.path.join(CURRENT_DIRECTORY, 'rename_columns.json')
    df = rename_columns_in_dataframe(df=df, rename_file=FILE_PATH)

    print('reading dates')
    # Convert the 'Date' column to datetime
    df['date'] = pd.to_datetime(df['date'])
    # Change the year to 2023
    df['date'] = df['date'].apply(lambda x: x.replace(year=2023))
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df['time'] = pd.to_datetime(df['time'])
    df['time'] = df['time'].apply(lambda t: t.strftime('%H:%M:%S'))
    df['datetime'] = pd.to_datetime(df['date'] + ' ' + df['time'])
    df['datetime'] = df['datetime'].dt.tz_localize(pytz.timezone('Asia/Singapore'))
    print(df['datetime'].head(20))
    # .astimezone(pytz.utc)  # Add UTC+8 timezone
    # df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    print('changed the dates')

    df['currency'] = 'SGD'
    df['amount'] = (df['amount'] * 100).astype(int)
    df['amount_to_display'] = (df['amount'] / 100).apply(lambda x: f"{x:.2f}")

    df = convert_integer_dataframe(df=df, integer_columns=CONFIG['integer_columns'])
    df = fillna_df(df=df, integer_columns=CONFIG['integer_columns'])
    df = format_date_columns(df=df, date_columns=CONFIG['date_columns'])


    FILE_PATH = os.path.join(CURRENT_DIRECTORY, 'output.csv')
    df.drop(['date', 'time'], axis=1, inplace=True)
    # df.to_csv(FILE_PATH)
    # print(df.head(5))


    es_client = ElasticSearchClient()

    FILE_PATH = os.path.join(CURRENT_DIRECTORY, 'mapping.json')
    mappings = load_mapping_from_json(FILE_PATH)

    index_settings = {
        "settings": {
            "number_of_shards":1,
            "number_of_replicas": 1
        },
        "mappings": mappings.get("mappings", {})
    }
    es_client.delete_index(index_name="expenses_2023")
    es_client.delete_index(index_name="expenses_2024")
    es_client.create_index(index_name="expenses_2023", settings=index_settings)

    es_client.create_alias(index_name="expenses_2023", alias_name="expenses")
    
    es_client.bulk_index_from_df(df, index_name="expenses_2023")


if __name__ == "__main__":
    process_df_and_ingest_es()
    # main()
