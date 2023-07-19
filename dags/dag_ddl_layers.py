from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'create_ddl_layers',
    default_args=default_args,
    description='Создание схем и сущностей в БД',
    schedule_interval=timedelta(days=1),
)

start_task = EmptyOperator(
    task_id='start_task',
    dag=dag,
)

create_schemas = PostgresOperator(
    task_id='create_schemas',
    postgres_conn_id='korus_internship_2_db',
    sql="""
        DROP SCHEMA IF EXISTS dds CASCADE;
        DROP SCHEMA IF EXISTS dm CASCADE;
        DROP SCHEMA IF EXISTS error CASCADE;
        
        CREATE SCHEMA IF NOT EXISTS dds;
        CREATE SCHEMA IF NOT EXISTS dm;
        CREATE SCHEMA IF NOT EXISTS error;
    """,
    dag=dag
)

create_dds_tables = PostgresOperator(
    task_id='create_dds_tables',
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

        CREATE TABLE IF NOT EXISTS dds.stores(
            pos VARCHAR(150) PRIMARY KEY,
            pos_name VARCHAR(255)    
        );

        CREATE TABLE IF NOT EXISTS dds.transaction(
            transaction_id VARCHAR(150),
            product_id INTEGER REFERENCES dds.product(product_id),
            pos VARCHAR(150) REFERENCES dds.stores(pos),
            recorded_on TIMESTAMP,
            quantity INTEGER,
            price NUMERIC(10,2),
            price_full NUMERIC(10,2),
            order_type_id VARCHAR(150),
            PRIMARY KEY (transaction_id, product_id, pos)
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

create_error_tables = PostgresOperator(
    task_id='create_error_tables',
    postgres_conn_id='korus_internship_2_db',
    sql="""        
        CREATE TABLE IF NOT EXISTS error.brand(
            brand_id VARCHAR(50),
            brand VARCHAR(50),
            brand_error_type VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS error.category(
            category_id VARCHAR(50),
            category_name VARCHAR(50),
            category_error_type VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS error.product(
            product_id VARCHAR(50),
            name_short VARCHAR(50),
            category_id VARCHAR(50),
            pricing_line_id VARCHAR(50),
            brand_id VARCHAR(50),
            product_error_type VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS error.transaction(
            transaction_id VARCHAR(50),
            product_id VARCHAR(50),
            pos VARCHAR(50),
            recorded_on VARCHAR(50),
            quantity VARCHAR(50),
            price VARCHAR(50),
            price_full VARCHAR(50),
            order_type_id VARCHAR(50),
            transaction_error_type VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS error.stores(
            pos VARCHAR(50),
            pos_name VARCHAR(50),
            stores_error_type VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS error.stock(
            available_on VARCHAR(50),
            product_id VARCHAR(50),
            pos VARCHAR(50),
            available_quantity VARCHAR(50),
            cost_per_item VARCHAR(50),
            stock_error_type VARCHAR(50)
        );
        
        CREATE TABLE IF NOT EXISTS error.error_types(
            error_id VARCHAR(50) PRIMARY KEY,
            error_name VARCHAR(255)
        )
    """,
    dag=dag
)

fill_error_types = PostgresOperator(
    task_id='fill_error_types',
    postgres_conn_id='korus_internship_2_db',
    sql="""
        TRUNCATE TABLE error.error_types;
        
        INSERT INTO error.error_types
        VALUES ('misc', 'Прочие'),
               ('dup_idx', 'Дубликат-индекс'),
               ('dup_val', 'Дубликат-значение'),
               ('inc_val', 'Некорректное значение'),
               ('inc_vlim', 'Некорректное значение (ограничение 40 символов)'),
               ('empty', 'Пустое значение'),
               ('missing', 'Отсутствует связь между таблицами из-за нехватки ключей');
    """,
    dag=dag
)

end_task = EmptyOperator(
    task_id='end_task',
    dag=dag
)


start_task >> create_schemas >> create_dds_tables >> end_task
start_task >> create_schemas >> create_error_tables >> fill_error_types >> end_task
