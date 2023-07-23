import os
import pandas as pd
from loguru import logger
from xlrd import xldate_as_datetime

def get_stocks(df_stock: pd.DataFrame, df_product: pd.DataFrame) -> set:
    # Функция для конвертации времени
    def excel_date_to_datetime(num: int):
        return xldate_as_datetime(num, 0).date().isoformat()
   
    # Убрали дубликаты
    df_stock = df_stock.reset_index()
    df_stock.drop_duplicates(ignore_index=False, keep='first', inplace=True)
    
    # Конвертировали тип Excel date в datetime
    df_stock['available_on'] = df_stock['available_on'].astype(int).apply(excel_date_to_datetime)
    df_stock['available_on'] = pd.to_datetime(df_stock['available_on'], format='%Y-%m-%d').astype(str)
    
    # Убрали пропуски
    df_stock_error = df_stock.loc[(df_stock == '').any(axis=1)]
    df_stock_error['stock_error_type'] = 'empty'
    
    # Убрали отрицательное available_quantity
    df_stock_error = df_stock_error.append(df_stock[df_stock['available_quantity'] <= '0'])
    df_stock_error['stock_error_type'].fillna('inc_val', inplace=True)
    
    # Отфильтровали pos
    df_stock_error = df_stock_error.append(df_stock[~df_stock['pos'].str.match(r'(^Магазин [0-9]$|^Магазин 10$)')])
    df_stock_error['stock_error_type'].fillna('inc_val', inplace=True)
    
    # Очистили ошибки и их дубликаты
    df_stock_error.drop_duplicates(ignore_index=False, keep='first', inplace=True)
    df_stock = df_stock.set_index(['available_on', 'product_id', 'pos'])
    df_stock_error = df_stock_error.set_index(['available_on', 'product_id', 'pos'])
    df_stock.drop(axis=0, index=df_stock_error.index, inplace=True)
    
    # Пофиксили зависимости
    df_stock = df_stock.reset_index()
    df_stock_error = df_stock_error.reset_index()
    df_stock = df_stock[df_stock['product_id'].astype(int).isin(df_product.index)]
    df_stock_error = df_stock_error.append(df_stock[~df_stock['product_id'].astype(int).isin(df_product.index)])
    df_stock_error['stock_error_type'].fillna('missing', inplace=True)
    
    # Убрали дубликаты (индекс) + вернули индексацию
    df_stock = df_stock.set_index(['available_on', 'product_id', 'pos'])
    df_stock['available_quantity'] = df_stock['available_quantity'].astype(float).astype(int)
    df_stock = df_stock.groupby(level=['available_on', 'product_id', 'pos']).agg({
        'available_quantity': 'sum',
        'cost_per_item': 'first'
    })
    df_stock_error = df_stock_error.set_index(['available_on', 'product_id', 'pos'])
    
    return df_stock, df_stock_error


if __name__ == '__main__':
    df_stock = pd.read_sql('SELECT * FROM sources.stock', con=os.environ['CONN_SOURCES'], index_col=['available_on', 'product_id', 'pos'])
    df_product = pd.read_sql('SELECT * FROM dds.product', con=os.environ['CONN_INTERNS'], index_col='product_id')

    df_stock, df_stock_error = get_stocks(df_stock, df_product)

    df_stock.to_sql(schema='dds', name='stock', con=os.environ['CONN_INTERNS'], if_exists='append')
    df_stock_error.to_sql(schema='error', name='stock', con=os.environ['CONN_INTERNS'], if_exists='append')
