from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import uuid 

default_args = {
    'owner': 'black_viking',
    'start_date': datetime(2023, 12, 1),
}

dag = DAG(
    'airline_dagestan',
    default_args=default_args,
    catchup=False,
)

def extract_transform_load_data():
    result_dataframe = pd.DataFrame()
    for i in range(26):
        current_chunck = pd.read_csv(f"/opt/airflow/data/chunk{i}.csv")
        result_dataframe = pd.concat([result_dataframe, current_chunck])

    result_dataframe = result_dataframe[
        (~(result_dataframe['designation'].isnull()))
        &
        (~(result_dataframe['region_1'].isnull()))
    ]
    result_dataframe['price'] = result_dataframe['price'].replace(np.nan, 0)
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
    final_data_from_file = mod_data_from_file.iterrows()

    for i, current_row in final_data_from_file:
        id_documenta = str(uuid.uuid4())

        row_documenta = {
            "country": current_row["country"],
            "description": current_row["description"],
            "designation": current_row["designation"],
            "points": current_row["points"],
            "price": current_row["price"],
            "province": current_row["province"],
            "region_1": current_row["region_1"],
            "taster_name": current_row["taster_name"],
            "taster_twitter_handle": current_row["taster_twitter_handle"],
            "title": current_row["title"],
            "variety": current_row["variety"],
            "winery": current_row["winery"],
        }

        elastic_connection.index(
            index="dag_wines",
            id=id_documenta,
            body=row_documenta)

        print(row_documenta)


load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_chunck_data_to_elastic,
    dag=dag
)

etl_task >> load_task