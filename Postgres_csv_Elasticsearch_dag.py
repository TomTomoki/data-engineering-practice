#!/usr/bin/env python
# coding: utf-8

# In[2]:


import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch


# In[3]:


default_args = {
    'owner': 'tom_tomoki',
    'start_date': dt.datetime(2022, 3, 11),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}


# In[10]:


def queryPostgres():
    conn_string = "dbname='dataengineering' host='localhost' user='USERNAME' password='PASSWORD'"
    
    conn = db.connect(conn_string)
    
    df = pd.read_sql("select * from test1", conn)
    df.to_csv("source_data.csv")
    print("===Data Extracted===")


# In[12]:


def insertElasticsearch():
    es = Elasticsearch('https://localhost:9200',
                       http_auth=('USERNAME', 'PASSWORD'),
                       ca_certs='PATH_TO_.crt')
    
    df = pd.read_csv("source_data.csv")
    
    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(index="users", body=doc)
        print(res)


# In[7]:


with DAG('Dag1',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5)) as dag:

    getData = PythonOperator(task_id='GetDataFromPostgreSQL',
                            python_callable=queryPostgres)
    
    insertData = PythonOperator(task_id='InsertDataIntoElasticsearch',
                               python_callable=insertElasticsearch)
    
    getData >> insertData

