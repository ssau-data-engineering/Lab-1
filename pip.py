from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from datetime import datetime
import pandas as pd
import numpy as np

with DAG('DAG_Airflow',
         schedule_interval=None,
         start_date=datetime(2023, 12, 1),
         catchup=False) as dag:

    def transform_data():
        result = pd.DataFrame()
        for i in range(26):
            result = pd.concat([result, pd.read_csv(f"/opt/airflow/data/chunk{i}.csv")])
        result = result[result['region_1'].notnull()]
        result = result[result['designation'].notnull()]
        result['price'] = result['price'].replace(np.nan, 0)
        result = result.drop(['id'], axis=1)
        result.to_csv('/opt/airflow/data/data.csv', index=False)

    def load_data():
        elastic = Elasticsearch("http://elasticsearch-kibana:9200")
        data = pd.read_csv('/opt/airflow/data/data.csv')
        data = data.fillna('')
        for i, row in data.iterrows():
            doc = {
                "country": row["country"],
                "description": row["description"],
                "designation": row["designation"],
                "points": row["points"],
                "price": row["price"],
                "province": row["province"],
                "region_1": row["region_1"],
                "taster_name": row["taster_name"],
                "taster_twitter_handle": row["taster_twitter_handle"],
                "title": row["title"],
                "variety": row["variety"],
                "winery": row["winery"],
            }

            elastic.index(index="wines", id=i, body=doc)

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag
        )

    transform_data >> load_data