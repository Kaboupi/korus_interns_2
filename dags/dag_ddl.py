from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.empty import EmptyOperator


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
    'DDL_load',
    default_args=default_args,
    description='Create DDL statement',
    schedule_interval=timedelta(days=1),
)

start_task = EmptyOperator(
    task_id='start_task',
    dag=dag,
)

create_ddl_tables = PostgresOperator(
    task_id='create_ddl_tables',
    postgres_conn_id='korus_internship_2_db',
    sql="""
        CREATE TABLE IF NOT EXISTS dds.brand(
            brand_id INTEGER PRIMARY KEY,
            brand VARCHAR(255)
        );

        CREATE TABLE IF NOT EXISTS dds.category(
            category_id VARCHAR(150) PRIMARY KEY,
            category_name VARCHAR(255)
        );

        CREATE TABLE IF NOT EXISTS dds.product(
            product_id INTEGER PRIMARY KEY,
            name_short VARCHAR(255),
            category_id VARCHAR(150) REFERENCES dds.category(category_id),
            pricing_line_id INTEGER,
            brand_id INTEGER REFERENCES dds.brand(brand_id)
        );

        CREATE TABLE IF NOT EXISTS dds.transaction(
            transaction_id VARCHAR(150),
            product_id INTEGER REFERENCES dds.product(product_id),
            recorded_on TIMESTAMP,
            quantity INTEGER,
            price NUMERIC(10,2),
            price_full NUMERIC(10,2),
            order_type_id VARCHAR(150),
            PRIMARY KEY (transaction_id, product_id)
        );

        CREATE TABLE IF NOT EXISTS dds.stores(
            pos VARCHAR(150) PRIMARY KEY,
            pos_name VARCHAR(255)    
        );

        CREATE TABLE IF NOT EXISTS dds.stock(
            available_on DATE,
            product_id INTEGER REFERENCES dds.product(product_id),
            pos VARCHAR(150) REFERENCES dds.stores(pos),
            available_quantity NUMERIC(10,5),
            cost_per_item NUMERIC(10,2),
            PRIMARY KEY(available_on, product_id, pos)
        );
    """,
    dag=dag
)

end_task = EmptyOperator(
    task_id='end_task',
    dag=dag
)


start_task >> create_ddl_tables >> end_task
