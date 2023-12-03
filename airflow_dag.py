from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import uuid 

default_args = {
    'owner': 'vlados',
    'start_date': datetime(2023, 12, 2),
}

dag = DAG(
    'airflow_sender',
    default_args=default_args,
    description='This DAG will send some data to Elasticsearch',
    catchup=False,
)

#С помощью data_download загрузим датафрейм, с которым будем работать  
def data_download():
    dag_dataframe = pd.DataFrame()
    for i in range(26):
        chunk = pd.read_csv(f"/opt/airflow/data/chunk{i}.csv")
        dag_dataframe = pd.concat([dag_dataframe, chunk])
    return dag_dataframe



def data_processing():
    dag_dataframe = data_download()
    dag_dataframe = dag_dataframe[
        (~(dag_dataframe['designation'].isnull()))
        &
        (~(dag_dataframe['region_1'].isnull()))
    ]
    dag_dataframe['price'] = dag_dataframe['price'].replace(np.nan, 0.0)
    dag_dataframe = dag_dataframe.drop(['id'], axis=1)

    dag_dataframe.to_csv('/opt/airflow/data/data.csv', index=False)

processing_data = PythonOperator(
    task_id='processing_data',
    python_callable=data_processing,
    dag=dag
)


def data_to_elastic_send():
    connect_to_elastic = Elasticsearch("http://elasticsearch-kibana:9200")

    upload_data = pd.read_csv('/opt/airflow/data/data.csv')
    
    
    improved_data = upload_data.fillna('')

    for i, row in improved_data.iterrows():
            # Generate a unique ID for each document
            doc_id = str(uuid.uuid4())

            # Handle NaN values
            row = row.fillna('')  # Replace NaN with empty string

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

            # Задаём индексы
            connect_to_elastic.index(index="wines", id=doc_id, body=doc)

            # Print the document instead of indexing
            print(doc)


send_data_to_elastic = PythonOperator(
    task_id='send_data_to_elastic',
    python_callable=data_to_elastic_send,
    dag=dag
)

# Set task dependencies
processing_data >> send_data_to_elastic

