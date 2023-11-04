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

    def read_and_transf_data():
     data = pd.DataFrame()
     for i in range(26):
         data = pd.concat([data, pd.read_csv(f"/opt/airflow/data/chunk{i}.csv")])

     data = data[(data['designation'].str.len() > 0) & (data['region_1'].str.len() > 0)]
     data['price'] = data['price'].replace(np.nan, 0)
     data = data.drop(['id'], axis=1)
     data.to_csv('/opt/airflow/data/data.csv', index=False)

    read_and_transf_task = PythonOperator(

         task_id='read_and_transf_task',

         python_callable=read_and_transf_data,

         dag=dag)
    
    def load_to_elastic():
     es = Elasticsearch("http://elasticsearch-kibana:9200")
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
        es.index(index="wines", id=i, document=doc)

    load_to_elastic_task = PythonOperator(

         task_id='load_to_elastic_task',

         python_callable=load_to_elastic,

         dag=dag)

    read_and_transf_task >> load_to_elastic_task