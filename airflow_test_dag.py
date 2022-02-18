#!/usr/bin/env python
# coding: utf-8

# # Try a very simple DAG 

# In[ ]:


import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd


# Very simple function to read from csv and write to json

# In[ ]:


def csv_to_json():
    df = pd.read_csv('/home/parallels/local/data-engineering-practice/data1.csv')
    
    for i, r in df.iterrows():
        print(r['name'])
    
    df.to_json('/home/parallels/local/data-engineering-practice/fromAirflow.json', orient='records')


# In[ ]:


csv_to_json()


# In[ ]:


default_args = {
    'owner': 'test',
    'start_date': dt.datetime(2022, 2, 18),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}


# In[ ]:


with DAG('csvToJsonDAG',
        default_args = default_args,
        schedule_interval = '0 * * * *') as dag:
    start_task = BashOperator(task_id = 'start_print',
                             bash_command = 'echo "Reading csv now .."')
    
    csv_to_json_task = PythonOperator(task_id = 'csv_to_json',
                                 python_callable=csv_to_json)
    
    start_task >> csv_to_json_task

