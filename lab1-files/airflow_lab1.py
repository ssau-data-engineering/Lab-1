import pendulum
from airflow.decorators import dag, task


default_args = {
    "depends_on_past": True,  # Следущее выполнение задачи зависит от успеха предыдущей
}

@dag(
    'airflow_lab1',
    schedule=None,  # DAG будет запускаться вручную
    default_args=default_args,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def dag_1():
    output_path = '/opt/airflow/data/lab_1/output/'  

    @task
    def read_csvfile(input_path: str):
        import pandas as pd
        import os

        dataframes = []
        for filename in os.listdir(input_path):
            if filename.endswith(".csv"):
                file_path = os.path.join(input_path, filename)
                dataframes.append(pd.read_csv(file_path))

        df = pd.concat(dataframes, ignore_index=True)
        return df


    @task
    def data_processing(df):
        result_dataframe = df.dropna(subset=['designation', 'region_1']).copy()
        result_dataframe['price'] = result_dataframe['price'].fillna(0.0)
        return result_dataframe
        

    @task
    def save_to_dick(result_dataframe, save_path: str):
        import os
        result_dataframe.to_csv(os.path.join(save_path, "data.csv"))
        return result_dataframe


    @task
    def save_to_elasticsearch(result_dataframe):
        from elasticsearch import Elasticsearch
        from elasticsearch.helpers import bulk
        import pandas as pd

        es_client = Elasticsearch("http://elasticsearch-kibana:9200")

        def convert_null_to_none(dict):
            return {key: None if pd.isna(value) else value for key, value in dict.items()}
        

        es_object = (
            {
                '_index': 'airflow1',
                '_type': '_doc', #document tipe 
                '_id': i,
                '_source': convert_null_to_none(entry.to_dict()),
            }
            for i, entry in result_dataframe.iterrows()
        )
        bulk(es_client, es_object)

    result_file = data_processing(read_csvfile(input_path = '/opt/airflow/data/lab_1/input/'))
    save_to_dick(result_file, output_path)
    save_to_elasticsearch(result_file)

dag_1()

if __name__ == "__main__":
     dag_1().test()

