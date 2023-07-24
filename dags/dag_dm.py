import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
proj_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    dag_id='DM',
    default_args=default_args,
    description='dds -> dm',
    template_searchpath=f'{proj_path}/sql/',
    schedule_interval='0 6 * * *',
)

task_truncate_dm = PostgresOperator(
    task_id='truncate_dm',
    postgres_conn_id='korus_internship_2_db',
    sql='dm_truncate.sql',
    dag=dag
)

task_load = PostgresOperator(
    task_id='load_into_dm',
    postgres_conn_id='korus_internship_2_db',
    sql='dm_load.sql',
    dag=dag
)

# task_load
task_truncate_dm >> task_load