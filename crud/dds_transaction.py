import os
import pandas as pd
from loguru import logger


def get_transactions(df_trans: pd.DataFrame, df_product: pd.DataFrame) -> set:
    # Убрали дубликаты
    df_trans = df_trans.reset_index()
    df_trans.drop_duplicates(ignore_index=False, keep='first', inplace=True)
    
    # Убрали пустые значения
    df_trans_error = df_trans[df_trans['pos'].isna()]
    df_trans = df_trans[~df_trans['pos'].isna()]
    df_trans_error['transaction_error_type'] = 'empty'
    
    # Пофиксили зависимости
    df_trans['product_id'] = df_trans['product_id'].astype(int)
    df_trans_error = df_trans_error.append(df_trans[~df_trans['product_id'].isin(df_product.index)])
    df_trans = df_trans[df_trans['product_id'].isin(df_product.index)]
    df_trans_error['transaction_error_type'].fillna('missing', inplace=True)
    
    # Вернули индексацию
    df_trans = df_trans.reset_index(drop=True).set_index(['transaction_id', 'product_id', 'pos'])
    df_trans_error = df_trans_error.reset_index(drop=True).set_index(['transaction_id', 'product_id', 'pos'])
    
    return df_trans, df_trans_error


if __name__ == '__main__':
    df_trans = pd.read_csv('/opt/airflow/src/transaction.csv', index_col=['transaction_id', 'product_id', 'pos'])
    df_product = pd.read_sql('SELECT * FROM dds.product', con=os.environ['CONN_INTERNS'], index_col='product_id')

    df_trans, df_trans_error = get_transactions(df_trans, df_product)
    
    df_trans.to_sql(schema='dds', name='transaction', con=os.environ['CONN_INTERNS'], if_exists='append')
    df_trans_error.to_sql(schema='error', name='transaction', con=os.environ['CONN_INTERNS'], if_exists='append')
