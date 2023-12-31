import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.empty import EmptyOperator


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
    'DDL_create',
    default_args=default_args,
    description='Создание схем и сущностей в БД',
    template_searchpath='/opt/airflow/sql/',
    schedule_interval=None,
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

create_dm_tables = PostgresOperator(
    task_id='create_dm_tables',
    postgres_conn_id='korus_internship_2_db',
    sql='dm_create.sql',
    dag=dag
)

create_ref_tables = PostgresOperator(
    task_id='create_reference_table',
    postgres_conn_id='korus_internship_2_db',
    sql='ref_info_create.sql',
    dag=dag
)

trigger_DDS = TriggerDagRunOperator(
    task_id='trigger_DDS',
    trigger_dag_id='DDS',
    dag=dag
)


create_schema >> create_ref_tables >> [create_dds_tables, create_dm_tables, create_error_tables] >> trigger_DDS
