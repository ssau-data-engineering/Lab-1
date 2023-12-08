from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import uuid

default_args = {
    'owner': 'mariashaina',
    'start_date': datetime(2023, 12, 5),
}

dag = DAG(
    'airflow_lab1',
    default_args=default_args,
    catchup=False,
)

def extract_transform_load_data():
    chunks = [pd.read_csv(f"/opt/airflow/data/chunk{i}.csv") for i in range(26)]
    result_dataframe = pd.concat(chunks)

    result_dataframe = result_dataframe.dropna(subset=['designation', 'region_1'])
    result_dataframe['price'] = result_dataframe['price'].fillna(0)
    result_dataframe = result_dataframe.drop(['id'], axis=1)

    result_dataframe.to_csv('/opt/airflow/data/data.csv', index=False)

etl_task = PythonOperator(
    task_id='etl_task',
    python_callable=extract_transform_load_data,
    dag=dag
)

def load_chunck_data_to_elastic():
    elastic_connection = Elasticsearch("http://elasticsearch-kibana:9200")

    data_from_file = pd.read_csv('/opt/airflow/data/data.csv')
    
    mod_data_from_file = data_from_file.fillna('')
    for i, current_row in mod_data_from_file.iterrows():
        id_documenta = str(uuid.uuid4())

        row_documenta = {key: current_row[key] for key in ["country", "description", "designation", "points", "price", "province", "region_1", "taster_name", "taster_twitter_handle", "title", "variety", "winery"]}

        elastic_connection.index(index="dag_wines", id=id_documenta, body=row_documenta)
        print(row_documenta)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_chunck_data_to_elastic,
    dag=dag
)

etl_task >> load_task
