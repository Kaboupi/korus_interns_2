import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.empty import EmptyOperator
proj_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 22),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_ddl_layers',
    default_args=default_args,
    description='Создание схем и сущностей в БД',
    template_searchpath=f'{proj_path}/sql/',
    schedule_interval=None,
)

start_task = EmptyOperator(
    task_id='start_task',
    dag=dag,
)

create_schema = PostgresOperator(
    task_id='create_schema',
    postgres_conn_id='korus_internship_2_db',
    sql='schema_create.sql',
    dag=dag
)

create_dds_tables = PostgresOperator(
    task_id='create_dds_tables',
    postgres_conn_id='korus_internship_2_db',
    sql='dds_create.sql',
    dag=dag
)

create_error_tables = PostgresOperator(
    task_id='create_error_tables',
    postgres_conn_id='korus_internship_2_db',
    sql='error_create.sql',
    dag=dag
)

end_task = EmptyOperator(
    task_id='end_task',
    dag=dag
)


start_task >> create_schema >> [create_dds_tables, create_error_tables] >> end_task
