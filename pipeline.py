from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from datetime import datetime
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

with DAG('DAG_Airflow',

         schedule_interval=None,

         start_date=datetime(2023, 11, 30),

         catchup=False) as dag:

    def read_data():
        result = pd.DataFrame()
        for i in range(26):
            result = pd.concat([result, pd.read_csv(f"/opt/airflow/data/chunk{i}.csv")])
        return result
    
    def transform_data():
     
        result = read_data()
        result = result[result['region_1'].notnull()]
        result = result[result['designation'].notnull()]        
        result['price'] = result['price'].replace(np.nan, 0)
        result = result.drop(['id'], axis=1)
        result.to_csv('/opt/airflow/data/data.csv', index=False)
    
    def load_to_elastic():
        patch = Elasticsearch("http://elasticsearch-kibana:9200")
        data = pd.read_csv(f"/opt/airflow/data/data.csv")

        for i, row in data.iterrows():
            doc = {
            "_index": 'lab1',
            "_type": "_doc",
            "_source": data,
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

    transform_data >> load_to_elastic