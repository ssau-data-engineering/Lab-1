from airflow.decorators import dag, task
from pendulum import datetime
import logging


default_args = {"start_date": datetime(2023, 1, 1)}

@dag(dag_id="lab1",
    schedule="@daily",
    catchup=False, 
    default_args=default_args,)
def lab1_dag():
    @task
    def read_data(path: str = "/opt/airflow/data"):
        import pandas as pd
        import os
        
        #logger = logging.getLogger("airflow.task")
        
        splits = []
        
        files = [os.path.join(path, it) for it in os.listdir(path)]
        
        for file in files:
            splits.append(pd.read_csv(file, index_col=0))
            
        df = pd.concat(splits)
        return df

    @task
    def filter_data(df):
        return df[df["designation"].notna() & df["region_1"].notna()].copy()

    @task
    def fill_nan_price(df):
        tdf = df.copy()
        tdf["price"] = tdf["price"].fillna(0.0)
        return tdf

    @task
    def save_to_disk(df, path = "/opt/airflow/results"):
        import os
        df.to_csv(os.path.join(path, "result.csv"))
        return df

    @task
    def save_to_elastic(df) -> ...:
        from elasticsearch import Elasticsearch, helpers
        import pandas as pd
        
        client = Elasticsearch(
            "http://elasticsearch-kibana:9200/"
        )

        def replace_np_nan(dict):
            return {k: None if pd.isna(v) else v
                    for k, v in dict.items()}
        gen = (
                {
                    "_index": "airflow-wine",
                    "_type" : "_doc",
                    "_id"   : ind,
                    "_source": replace_np_nan(entry.to_dict()),
                } for ind, entry in df.iterrows()
            )
        
        helpers.bulk(client, gen)

    df = fill_nan_price(filter_data(read_data()))
    save_to_disk(df)
    save_to_elastic(df)

lab1_dag()

if __name__ == "__main__":
    lab1_dag().test()