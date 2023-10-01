from airflow.decorators import task, dag
from airflow.operators.python_operator import PythonOperator

from datetime import datetime

import pandas as pd
import numpy as np
import os
import glob

from lightgbm import LGBMClassifier

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

@dag(
	start_date=datetime(2021, 1, 1),
	schedule_interval=None,
	catchup=False,
	doc_md=__doc__,
)
def data_clean_pipeline():
	filename_path_save = './lab1_output.csv'
	load_data_folder = './../data'
	
	@task
	def load_data(path_folder: str):
		return pd.concat(
			(
				pd.read_csv(single_file) 
				for single_file in glob.glob(os.path.join(path_folder, '*.csv'))
			), 
			ignore_index=True
		)	

	@task
	def preprocessing(df: pd.DateFrame):
		df = df[df['designation'].notnull()]
		df = df[df['region_1'].notnull()]
		df.loc[df["price"].isnull(), "price"] = 0.0

		return df

	@task
	def save_data(df: pd.DateFrame, filename_path_save: str):
		df.to_csv(filename_path_save)

	es = Elasticsearch(['localhost'], port=19200)

	df = load_data(load_data_folder)
	pd_preprocessed = preprocessing(df)
	save_data(pd_preprocessed, filename_path_save)

	data_json = pd_preprocessed.to_json(orient='records')
	actions = [
	    {
	        '_index': 'my_index',
	        '_type': 'my_type',
	        '_id': i,
	        '_source': doc
	    }
	    for i, doc in enumerate(data)
	]
	bulk(es, actions)

pipeline_dag = data_clean_pipeline()
