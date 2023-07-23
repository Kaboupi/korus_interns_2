import os
import pandas as pd
from loguru import logger


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
    df_cat_error.index = df_cat_error.index.rename('category_id')
    
    return df_cat, df_cat_error


if __name__ == '__main__':
    df_cat = pd.read_sql('SELECT * FROM sources.category', os.environ['CONN_SOURCES'], index_col='category_id')
    df_brand = pd.read_sql('SELECT * FROM sources.brand', os.environ['CONN_SOURCES'], index_col='brand_id')
    
    df_cat, df_cat_error = get_cats(df_cat, df_brand)

    df_cat.to_sql(schema='dds', name='category', con=os.environ['CONN_INTERNS'], if_exists='append')
    df_cat_error.to_sql(schema='error', name='category', con=os.environ['CONN_INTERNS'], if_exists='append')
