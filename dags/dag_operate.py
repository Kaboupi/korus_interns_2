import os
import sys
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from loguru import logger
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
proj_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(proj_path)
from crud.etl_functions import *

PG_HOOK_SOURCES = PostgresHook(postgres_conn_id='korus_internship_sources')
engine_sources = create_engine(PG_HOOK_SOURCES.get_uri().rsplit('?')[0])
conn_sources = engine_sources.connect()

PG_HOOK_INTERNS = PostgresHook(postgres_conn_id='korus_internship_2_db')
engine_interns = create_engine(PG_HOOK_INTERNS.get_uri().rsplit('?')[0])
conn_interns = engine_interns.connect()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'run_etl_processes',
    default_args=default_args,
    description='ETL процесс по обработке таблиц из схемы sources в схему dds',
    schedule_interval=timedelta(weeks=1),
)

# BRAND
def func_brand_transform(**context):
    df_brand = pd.read_sql('SELECT * FROM sources.brand', con=conn_sources, index_col='brand_id')
    df_brand, df_brand_error = get_brands(df_brand)
    
    context['ti'].xcom_push(key='df_brand_t', value=df_brand.to_dict())
    context['ti'].xcom_push(key='df_brand_e', value=df_brand_error.to_dict())

def func_brand_dds(**context):
    df_brand = pd.DataFrame(context['ti'].xcom_pull(key='df_brand_t'))
    df_brand.index = df_brand.index.rename('brand_id')
    df_brand.to_sql(schema='dds', name='brand', con=conn_interns, if_exists='append')

def func_brand_error(**context):
    df_brand_error = pd.DataFrame(context['ti'].xcom_pull(key='df_brand_e'))
    df_brand_error.index = df_brand_error.index.rename('brand_id')
    df_brand_error.to_sql(schema='error', name='brand', con=conn_interns, if_exists='append')

task_transform_brand = PythonOperator(
    task_id='transform_brand',
    python_callable=func_brand_transform,
    dag=dag,
)

task_load_brand_dds = PythonOperator(
    task_id='brand_to_dds',
    python_callable=func_brand_dds,
    dag=dag,
)

task_load_brand_error = PythonOperator(
    task_id='brand_to_error',
    python_callable=func_brand_error,
    dag=dag
)

task_end_brand = EmptyOperator(
    task_id='task_end_brand',
    dag=dag,
)


# CATEGORY
def func_cat_transform(**context):
    df_cat = pd.read_sql('SELECT * FROM sources.category', conn_sources, index_col='category_id')
    df_brand = pd.read_sql('SELECT * FROM sources.brand', conn_sources, index_col='brand_id')
    df_cat, df_cat_error = get_cats(df_cat, df_brand)
    
    context['ti'].xcom_push(key='df_cat_t', value=df_cat.to_dict())
    context['ti'].xcom_push(key='df_cat_e', value=df_cat_error.to_dict())

def func_cat_dds(**context):
    df_cat = pd.DataFrame(context['ti'].xcom_pull(key='df_cat_t'))
    df_cat.index = df_cat.index.rename('category_id')
    
    df_cat.to_sql(schema='dds', name='category', con=conn_interns, if_exists='append')

def func_cat_error(**context):
    df_cat_error = pd.DataFrame(context['ti'].xcom_pull(key='df_cat_e'))
    df_cat_error.index = df_cat_error.index.rename('category_id')
    
    df_cat_error.to_sql(schema='error', name='category', con=conn_interns, if_exists='append')

task_transform_category = PythonOperator(
    task_id='transform_category',
    python_callable=func_cat_transform,
    dag=dag,
)

task_load_category_dds = PythonOperator(
    task_id='category_to_dds',
    python_callable=func_cat_dds,
    dag=dag,
)

task_load_category_error = PythonOperator(
    task_id='category_to_error',
    python_callable=func_cat_error,
    dag=dag
)

task_end_category = EmptyOperator(
    task_id='task_end_category',
    dag=dag,
)


# PRODUCT
def func_stores_dds(**context):
    df_product = pd.read_sql('SELECT * FROM sources.product', conn_sources, index_col='product_id')
    df_cat = pd.read_sql('SELECT * FROM dds.category', conn_interns, index_col='category_id')
    df_product, df_product_error = get_products(df_product, df_cat)

    context['ti'].xcom_push(key='df_product_t', value=df_product.to_dict())
    context['ti'].xcom_push(key='df_product_e', value=df_product_error.to_dict())

def func_product_dds(**context):
    df_product = pd.DataFrame(context['ti'].xcom_pull(key='df_product_t'))
    df_product.index = df_product.index.rename('product_id')
    df_product.to_sql(schema='dds', name='product', con=conn_interns, if_exists='append')

def func_product_error(**context):
    df_product_error = pd.DataFrame(context['ti'].xcom_pull(key='df_product_e'))
    df_product_error.index = df_product_error.index.rename('product_id')
    df_product_error.to_sql(schema='error', name='product', con=conn_interns, if_exists='append')

task_transform_product = PythonOperator(
    task_id='transform_product',
    python_callable=func_stores_dds,
    dag=dag,
)

task_load_product_dds = PythonOperator(
    task_id='product_to_dds',
    python_callable=func_product_dds,
    dag=dag,
)

task_load_product_error = PythonOperator(
    task_id='product_to_error',
    python_callable=func_product_error,
    dag=dag
)

task_end_product = EmptyOperator(
    task_id='task_end_product',
    dag=dag,
)


# STORES
def func_stores_dds():
    df_stores = pd.read_csv(f'{proj_path}/src/stores.csv', encoding='cp1251', delimiter=';', index_col='pos')
    df_stores.to_sql(schema='dds', name='stores', con=conn_interns, if_exists='append')

task_load_stores_dds = PythonOperator(
    task_id='stores_to_dds',
    python_callable=func_stores_dds,
    dag=dag,
)


# TRANSACTION
def func_transaction_transform(**context):
    df_trans = pd.read_csv(f'{proj_path}/src/transaction.csv', index_col=['transaction_id', 'product_id', 'pos'])
    df_product = pd.read_sql('SELECT * FROM dds.product', con=conn_interns, index_col='product_id')

    df_trans, df_trans_error = get_transactions(df_trans, df_product)
    print(df_trans)
    context['ti'].xcom_push(key='df_transaction_t', value=df_trans.reset_index().to_dict())
    context['ti'].xcom_push(key='df_transaction_e', value=df_trans_error.reset_index().to_dict())

def func_transaction_dds(**context):
    df_trans = pd.DataFrame(context['ti'].xcom_pull(key='df_transaction_t')).set_index(['transaction_id', 'product_id', 'pos'])
    df_trans.to_sql(schema='dds', name='transaction', con=conn_interns, if_exists='append')
    print(df_trans)

def func_transaction_error(**context):
    df_trans_error = pd.DataFrame(context['ti'].xcom_pull(key='df_transaction_e')).set_index(['transaction_id', 'product_id', 'pos'])
    df_trans_error.to_sql(schema='error', name='transaction', con=conn_interns, if_exists='append')

task_transform_transaction = PythonOperator(
    task_id='transform_transaction',
    python_callable=func_transaction_transform,
    dag=dag,
)

task_load_transaction_dds = PythonOperator(
    task_id='transaction_to_dds',
    python_callable=func_transaction_dds,
    dag=dag,
)

task_load_transaction_error = PythonOperator(
    task_id='transaction_to_error',
    python_callable=func_transaction_error,
    dag=dag
)

task_end_transaction = EmptyOperator(
    task_id='task_end_transaction',
    dag=dag,
)


# STOCK
def func_stock_transform(**context):
    df_stock = pd.read_sql('SELECT * FROM sources.stock', con=conn_sources, index_col=['available_on', 'product_id', 'pos'])
    df_product = pd.read_sql('SELECT * FROM dds.product', con=conn_interns, index_col='product_id')

    df_stock, df_stock_error = get_stocks(df_stock, df_product)
    
    context['ti'].xcom_push(key='df_stock_t', value=df_stock.reset_index().to_dict())
    context['ti'].xcom_push(key='df_stock_e', value=df_stock_error.reset_index().to_dict())

def func_stock_dds(**context):
    df_stock = pd.DataFrame(context['ti'].xcom_pull(key='df_stock_t')).set_index(['available_on', 'product_id', 'pos'])
    df_stock.to_sql(schema='dds', name='stock', con=conn_interns, if_exists='append')

def func_stock_error(**context):
    df_stock_error = pd.DataFrame(context['ti'].xcom_pull(key='df_stock_e')).set_index(['available_on', 'product_id', 'pos'])
    df_stock_error.to_sql(schema='error', name='stock', con=conn_interns, if_exists='append')

task_transform_stock = PythonOperator(
    task_id='transform_stock',
    python_callable=func_stock_transform,
    dag=dag,
)

task_load_stock_dds = PythonOperator(
    task_id='stock_to_dds',
    python_callable=func_stock_dds,
    dag=dag,
)

task_load_stock_error = PythonOperator(
    task_id='stock_to_error',
    python_callable=func_stock_error,
    dag=dag
)

task_end_stock = EmptyOperator(
    task_id='task_end_stock',
    dag=dag,
)


# STEPS
task_start = EmptyOperator(
    task_id='start_task',
    dag=dag,
)

task_truncate_dds = PostgresOperator(
    task_id='truncate_schema_dds',
    postgres_conn_id='korus_internship_2_db',
    sql="""
        TRUNCATE TABLE dds.stock CASCADE;
        TRUNCATE TABLE dds.transaction CASCADE;
        TRUNCATE TABLE dds.product CASCADE;
        TRUNCATE TABLE dds.category CASCADE;
        TRUNCATE TABLE dds.brand CASCADE;
        TRUNCATE TABLE dds.stores CASCADE;
        """,
    dag=dag,
)

task_truncate_error = PostgresOperator(
    task_id='truncate_schema_error',
    postgres_conn_id='korus_internship_2_db',
    sql="""
        TRUNCATE TABLE error.transaction;
        TRUNCATE TABLE error.stock;
        TRUNCATE TABLE error.stores;
        TRUNCATE TABLE error.product;
        TRUNCATE TABLE error.brand;
        TRUNCATE TABLE error.category;
        """,
    dag=dag,
)

task_start_ETL = EmptyOperator(
    task_id='start_ETL',
    dag=dag
)

task_stores_product_end = EmptyOperator(
    task_id='stores_product_end',
    dag=dag,
)

task_end = EmptyOperator(
    task_id='end_task',
    dag=dag,
)


task_start >> [task_truncate_dds, task_truncate_error] >> task_start_ETL >> task_transform_brand >> [task_load_brand_dds, task_load_brand_error] >> task_end_brand
task_start >> [task_truncate_dds, task_truncate_error] >> task_start_ETL >> task_transform_category >> [task_load_category_dds, task_load_category_error] >> task_end_category

[task_end_brand, task_end_category] >> task_transform_product >> [task_load_product_dds, task_load_product_error] >> task_end_product >> task_stores_product_end

task_start >> task_load_stores_dds >> task_stores_product_end 

task_stores_product_end >> task_transform_transaction >> [task_load_transaction_dds, task_load_transaction_error] >> task_end_transaction >> task_end
task_stores_product_end >> task_transform_stock >> [task_load_stock_dds, task_load_stock_error] >> task_end_stock >> task_end
