import os
import pandas as pd
from loguru import logger
proj_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


if __name__ == '__main__':
    df_stores = pd.read_csv(f'{proj_path}/src/stores.csv', encoding='cp1251', delimiter=';', index_col='pos')
    df_stores.to_sql(schema='dds', name='stores', con=os.environ['CONN_INTERNS'], if_exists='append')
