from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from datetime import datetime
import pandas as pd
import numpy as np

with DAG('basic_etl_dag',

         schedule_interval=None,

         start_date=datetime(2023, 28, 11),

         catchup=False) as dag:

    def read_data():
        result = pd.DataFrame()
        for i in range(26):
            result = pd.concat([result, pd.read_csv(f"/opt/airflow/data/chunk{i}.csv")])
        return result
    
    def transform_data():
     
        result = read_data()
        result = result[(result['designation'].str.len() > 0)]
        result = result[(result['region_1'].str.len() > 0)]
        result['price'] = result['price'].replace(np.nan, 0)
        result = result.drop(['id'], axis=1)
        result.to_csv('/opt/airflow/data/data.csv', index=False)
    
    def load_to_elastic():
     patch = Elasticsearch("http://elasticsearch-kibana:9200")
     data = pd.read_csv(f"/opt/airflow/data/data.csv")

     for i, row in data.iterrows():
        doc = {
        "country": row["country"],
        "description": row["description"],
        "designation": row["designation"],
        "points": row["points"],
        "price": row["price"],
        "province": row["province"],
        "region_1": row["region_1"],
        "region_2": row["region_1"],
        "taster_name": row["taster_name"],
        "taster_twitter_handle": row["taster_twitter_handle"],
        "title": row["title"],
        "variety": row["variety"],
        "winery": row["winery"],
     }

     if i < data.shape[0] - 1: 
        patch.index(index="wines", id=i, document=doc)

    transform_data = PythonOperator(

         task_id='transform_data',

         python_callable=transform_data,

         dag=dag)

    load_to_elastic = PythonOperator(

         task_id='load_to_elastic',

         python_callable=load_to_elastic,

         dag=dag)

     #task order
    transform_data >> load_to_elastic

