import os
import pandas as pd
from loguru import logger


def get_products(df_product: pd.DataFrame, df_cat: pd.DataFrame) -> set:
    # Убрали дубликаты
    df_product.drop_duplicates(ignore_index=False, keep='first', inplace=True)
    
    # Заполнили пустые pricing_line_id
    df_product['pricing_line_id'] = df_product.index.to_series()
    
    # Отфильтровали некоррентое name_short (40 символов)
    df_err = df_product[df_product['name_short'].apply(lambda x: len(x)) == 40]
    df_err['product_error_type'] = 'inc_vlim'
    
    # Убрали brand 'NO BRAND'
    df_err = df_err.append(df_product[df_product['brand_id'] == '1000'])
    df_err['product_error_type'].fillna('inc_val', inplace=True)
    
    # Некорректное name_short
    df_err = df_err.append(df_product[df_product['name_short'].str.match(r'([a-zа-я0-9])')].sort_index().loc['24597873':])
    df_err['product_error_type'].fillna('inc_val', inplace=True)
    
    # Отфильтровали дубликаты (одинаковые id, разные значения)
    df_err = df_err.append(df_product[(df_product.duplicated(subset='name_short', keep=False) == True) & (df_product['name_short'].apply(lambda x: len(x)) != 40)].sort_values(by='name_short'))
    df_err['product_error_type'].fillna('dup_val', inplace=True)
    
    # Удалили всё, кроме ограничения в 40
    df_product.drop(axis=1, index=df_err[df_err['product_error_type'] != 'inc_vlim'].index, inplace=True)
    
    # Пофиксили зависимости
    df_product = df_product[df_product['category_id'].isin(df_cat.index)]
    
    return df_product, df_err


if __name__ == '__main__':
    df_product = pd.read_sql('SELECT * FROM sources.product', os.environ['CONN_SOURCES'], index_col='product_id')
    df_cat = pd.read_sql('SELECT * FROM dds.category', os.environ['CONN_INTERNS'], index_col='category_id')
    
    df_product, df_product_error = get_products(df_product, df_cat)
    
    df_product.to_sql(schema='dds', name='product', con=os.environ['CONN_INTERNS'], if_exists='append')
    df_product_error.to_sql(schema='error', name='product', con=os.environ['CONN_INTERNS'], if_exists='append')