import os
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine


def get_brands(df: pd.DataFrame) -> set:
    # Удалили дубликаты
    df.drop_duplicates(ignore_index=False, keep='first', inplace=True)
    
    # Убрали некорректные значения (перепутаны местами + 'NO BRAND')
    df_error = df.loc[['TEVA', '1000']]
    df_error['brand_error_type'] = 'misc'
    
    # Убрали категорию 'не используем'
    df.drop(axis=0, index='2989', inplace=True)
    
    # Убрали ошибки
    df.drop(axis=1, index=df_error.index, inplace=True)
    
    return df, df_error


if __name__ == '__main__':
    df_brand = pd.read_sql('SELECT * FROM sources.brand', con=os.environ['CONN_SOURCES'], index_col='brand_id')
    
    df_brand, df_brand_error = get_brands(df_brand)
    
    df_brand.to_sql(schema='dds', name='brand', con=os.environ['CONN_INTERNS'], if_exists='append')
    df_brand_error.to_sql(schema='error', name='brand', con=os.environ['CONN_INTERNS'], if_exists='append')
