import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
proj_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


PG_HOOK_SOURCES = PostgresHook(postgres_conn_id='korus_internship_sources')
PG_HOOK_INTERNS = PostgresHook(postgres_conn_id='korus_internship_2_db')

os.environ['CONN_SOURCES'] = PG_HOOK_SOURCES.get_uri().rsplit('?')[0]
os.environ['CONN_INTERNS'] = PG_HOOK_INTERNS.get_uri().rsplit('?')[0]


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'run_etl_tasks',
    default_args=default_args,
    description='ETL процесс по обработке таблиц из схемы sources в схему dds',
    template_searchpath=f'{proj_path}/sql/',
    schedule_interval=timedelta(weeks=4),
)

task_truncate = PostgresOperator(
    task_id='truncate_dds_tables',
    postgres_conn_id='korus_internship_2_db',
    sql='dds_truncate.sql',
    dag=dag
)

trans_brand = BashOperator(
    task_id='transform_brand',
    bash_command=f'python {proj_path}/crud/dds_brand.py',
    dag=dag
)

trans_category = BashOperator(
    task_id='transform_category',
    bash_command=f'python {proj_path}/crud/dds_category.py',
    dag=dag
)

trans_product = BashOperator(
    task_id='transform_product',
    bash_command=f'python {proj_path}/crud/dds_product.py',
    dag=dag
)

trans_stores = BashOperator(
    task_id='transform_stores',
    bash_command=f'python {proj_path}/crud/dds_stores.py',
    dag=dag
)

trans_transaction = BashOperator(
    task_id='transform_transaction',
    bash_command=f'python {proj_path}/crud/dds_transaction.py',
    dag=dag
)

trans_stock = BashOperator(
    task_id='transform_stock',
    bash_command=f'python {proj_path}/crud/dds_stock.py',
    dag=dag
)

end_task = EmptyOperator(
    task_id='end_ETL',
    dag=dag
)

task_truncate >> [trans_brand, trans_category, trans_stores] >> trans_product >> [trans_stock, trans_transaction] >> end_task
 