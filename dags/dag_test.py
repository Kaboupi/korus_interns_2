import os
import sys
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text as sql_text
from loguru import logger
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
proj_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(proj_path)

PG_HOOK_SOURCES = PostgresHook(postgres_conn_id='korus_internship_sources')
engine_sources = create_engine(PG_HOOK_SOURCES.get_uri().rsplit('?')[0])
conn = engine_sources.connect()
logger.info('Connected successfully!')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_select',
    default_args=default_args,
    description='A simple DAG that selects with SQL query and saves to .csv and .json',
    schedule_interval=timedelta(days=1),
)

def select(**context):
    df = pd.read_sql(
        '''
        SELECT
            c.category_name,
            p.name_short,
            SUM(CASE WHEN available_quantity <> '' THEN available_quantity::NUMERIC(10,2) ELSE 0 END) as sum_quantity
        FROM sources.stock s
        INNER JOIN sources.product p ON s.product_id = p.product_id
        INNER JOIN sources.category c ON c.category_id = p.category_id
        WHERE cost_per_item <> ''
        GROUP BY c.category_name, p.name_short
        HAVING SUM(CASE WHEN available_quantity <> '' THEN available_quantity::NUMERIC(10,2) ELSE 0 END) >= 0
        ORDER BY sum_quantity DESC
        LIMIT 100;
        ''', conn)
    print(df)
    context['ti'].xcom_push(key='dataframe', value=df.to_dict())
    logger.info('Dataframe logger.infoed!')
    
    
def save_to_csv(**context):
    df = pd.DataFrame(context['ti'].xcom_pull(key='dataframe'))
    df.to_csv(f'{proj_path}/output/data.csv')
    logger.info('Dataframe successfully saved to .csv!')
    

def save_to_json(**context):
    df = pd.DataFrame(context['ti'].xcom_pull(key='dataframe'))
    df.to_json(f'{proj_path}/output/data.json', force_ascii=False)
    logger.info('Dataframe successfully saved to .json!')


start_task = EmptyOperator(
    task_id='start_task',
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end_task',
    dag=dag,
)

task_select = PythonOperator(
    task_id='select_query',
    python_callable=select,
    provide_context=True,
    dag=dag,
)

task_save_csv = PythonOperator(
    task_id='save_csv',
    python_callable=save_to_csv,
    provide_context=True,
    dag=dag,
)

task_save_json = PythonOperator(
    task_id='save_json',
    python_callable=save_to_json,
    provide_context=True,
    dag=dag,
)

start_task >> task_select >> [task_save_csv, task_save_json] >> end_task
