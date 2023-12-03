from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import os
import glob
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

@dag(
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
    doc_md=__doc__,
)
def data_clean_pipeline():
    load_data_folder = './../data'
    filename_path_save = './lab1_output.csv'

    @task
    def load_data(path_folder: str):
        files = glob.glob(os.path.join(path_folder, '*.csv'))
        return pd.concat((pd.read_csv(file) for file in files), ignore_index=True)

    @task
    def preprocessing(df: pd.DataFrame):
        df = df.dropna(subset=['designation', 'region_1'])
        df['price'].fillna(0.0, inplace=True)
        return df

    @task
    def save_data(df: pd.DataFrame, filename_path_save: str):
        df.to_csv(filename_path_save, index=False)

    @task
    def index_to_elasticsearch(df: pd.DataFrame):
        es = Elasticsearch(['localhost'], port=19200)
        data_json = df.to_dict(orient='records')
        actions = [
            {'_index': 'my_index', '_type': 'my_type', '_id': i, '_source': doc}
            for i, doc in enumerate(data_json)
        ]
        bulk(es, actions)

    df = load_data(load_data_folder)
    df_preprocessed = preprocessing(df)
    save_data(df_preprocessed, filename_path_save)
    index_to_elasticsearch(df_preprocessed)

pipeline_dag = data_clean_pipeline()