
import datetime
import pendulum

from airflow import DAG
from airflow.decorators import task, dag

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def Lab_1_dag():
    @task.virtualenv(task_id="virtualenv_python", requirements=["pandas"], system_site_packages=False)
    def callable_virtualenv():

        import pandas as pd
        
        df = pd.read_csv('/opt/airflow/data/lab_1/input/chunk0.csv', encoding='utf-8')

        for i in range(1,26,1):
            df_ = pd.read_csv(f'/opt/airflow/data/lab_1/input/chunk{i}.csv', encoding='utf-8')
            df = pd.concat([df,df_])

        df = df.dropna(subset=['region_1','designation'])
        df = df.fillna(value={'price': 0.0})    

        df.to_csv('/opt/airflow/data/lab_1/output/output.csv', encoding='utf-8', index=False)

    virtualenv_task = callable_virtualenv()

this_is_my_dag = Lab_1_dag()