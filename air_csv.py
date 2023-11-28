from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import uuid 

default_args = {
    'owner': 'bogdann',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'elasticsearch_dag_nb',
    default_args=default_args,
    description='DAG for sending data to Elasticsearch',
    schedule_interval=None,  # You may adjust this as needed
    catchup=False,
)

def transform_data():
    res = pd.DataFrame()
    for i in range(26):
        res = pd.concat([res, pd.read_csv(f"/opt/airflow/data/chunk{i}.csv")])

    res = res[(res['designation'].str.len() > 0) & (res['region_1'].str.len() > 0)]
    res['price'] = res['price'].replace(np.nan, 0)
    res = res.drop(['id'], axis=1)

    # Save DataFrame to a CSV file (you may adjust the file path)
    res.to_csv('/opt/airflow/data/data.csv', index=False)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    dag=dag
)

def load_to_elastic():
    es = Elasticsearch("http://elasticsearch-kibana:9200")

    # Read DataFrame from the CSV file
    input = pd.read_csv('/opt/airflow/data/data.csv')

    for i, row in input.iterrows():
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

        # Index the document in Elasticsearch with the generated ID
        es.index(index="wines", id=doc_id, body=doc)

        # Print the document instead of indexing
        print(doc)

load_to_elastic_task = PythonOperator(
    task_id='load_to_elastic_task',
    python_callable=load_to_elastic,
    dag=dag
)

# Set task dependencies
transform_task >> load_to_elastic_task

if __name__ == "__main__":
    dag.cli()
