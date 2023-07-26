import os
import pandas as pd
from loguru import logger


if __name__ == '__main__':
    df_stores = pd.read_csv('/opt/airflow/src/stores.csv', encoding='cp1251', delimiter=';', index_col='pos')
    df_stores.to_sql(schema='dds', name='stores', con=os.environ['CONN_INTERNS'], if_exists='append')
