from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from datetime import datetime
import pandas as pd
import numpy as np

with DAG('basic_etl_dag',

         schedule_interval=None,

         start_date=datetime(2023, 10, 7),

         catchup=False) as dag:

    def transform_data():
     res = pd.DataFrame()
     for i in range(26):
         res = pd.concat([res, pd.read_csv(f"/opt/airflow/data/chunk{i}.csv")])

     res = res[(res['designation'].str.len() > 0) & (res['region_1'].str.len() > 0)]
     res['price'] = res['price'].replace(np.nan, 0)
     res = res.drop(['id'], axis=1)
     res.to_csv('/opt/airflow/data/data.csv', index=False)

    transform_task = PythonOperator(

         task_id='transform_task',

         python_callable=transform_data,

         dag=dag)

    transform_task
    
    def load_to_elastic():
     es = Elasticsearch("http://elasticsearch-kibana:9200")
     input = pd.read_csv(f"/opt/airflow/data/data.csv")

     for i, row in input.iterrows():
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

     if i < input.shape[0] - 1: 
        es.index(index="wines", id=i, document=doc)

    load_to_elastic_task = PythonOperator(

         task_id='load_to_elastic_task',

         python_callable=load_to_elastic,

         dag=dag)

    load_to_elastic_task

     #task order
    transform_task >> load_to_elastic_task