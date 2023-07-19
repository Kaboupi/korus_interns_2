import numpy as np
import pandas as pd
from loguru import logger
from xlrd import xldate_as_datetime

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


def get_cats(df_cat: pd.DataFrame, df_brand: pd.DataFrame) -> set:
    # Убрали дубликаты
    df_cat.drop_duplicates(ignore_index=False, keep='first', inplace=True)
    
    # Убрали некорректные названия категорий
    df_cat.drop(axis=1, index=df_cat[df_cat['category_name'].str.contains(r'([?!<>])') == True].index, inplace=True)
    
    # Убрали попадания имя бренда в категорию (англоязычные)
    df_cat_error = df_cat[~df_cat['category_name'].str.match(r'.*[а-яА-я].')]
    df_cat_error['category_error_type'] = 'misc'
    
    # Убрали попадания имя бренда в категорию (все остальные)
    df_cat['cat_lower'] = df_cat['category_name'].str.lower()
    df_brand['name_lower'] = df_brand['brand'].str.lower()
    dup_idx = pd.Index(df_cat.reset_index().set_index('cat_lower').join(df_brand.set_index('name_lower'), how="inner")['category_id'].values)
    
    # Удалили остатки предыдущей операции
    df_cat.drop('cat_lower', axis=1, inplace=True)
    df_brand.drop('name_lower', axis=1, inplace=True)
    df_cat_error = df_cat_error.append(df_cat.loc[dup_idx])
    df_cat_error['category_error_type'] = df_cat_error['category_error_type'].fillna('misc')
    # Категория "НЕ ИСПОЛЬЗУЕМ"
    df_cat.drop(axis=1, index=df_cat[df_cat['category_name'] == 'НЕ ИСПОЛЬЗУЕМ'].index, inplace=True)
    
    # Убрали значения 'Значение{цифра}' (работа вручную)
    inc_val_idx = pd.Index(['PC7', '4E4', 'PC51', 'PC59', 'PC64', 'PC65', 'PC75', 'PC76', 'PC77', 'M11', '43B', 
                            '459', '445', '46B', '446', '431', '442', 'C12', '4A1', '4A2', '46C', 'PC41'])
    df_cat_error = df_cat_error.append(df_cat.loc[inc_val_idx])
    df_cat_error['category_error_type'] = df_cat_error['category_error_type'].fillna('inc_val')
    
    # Убрали ошибки
    df_cat.drop(axis=1, index=df_cat_error.index, inplace=True)
    df_cat_error.drop_duplicates(ignore_index=False, inplace=True)
    
    return df_cat, df_cat_error


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
