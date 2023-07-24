# КОРУС Консалтинг. Задания для разработчика 2, 3
> Стажёр-разработчик Георгий Новожилов \
> Системный аналитик Валентина Сорокина
> Команда 2

## 1. Структура
[**./crud/**](crud/) - Python-скрипты для обработки данных из таблиц \
[**./sql/**](sql/) - SQL-скрипты для создания / загрузки / обработки \
[**./dags/dag_ddl.py**](dags/dag_ddl.py) - Создание схем и сущностей в БД **internship_2_sources** \
[**./dags/dag_dds.py**](dags/dag_dds.py) - Обработка и выгрузка данных из **internship_sources.source** в **internship_2_db.dds** \
[**./dags/dag_dm.py**](dags/dag_dm.py) - Агрегация данных в слой-витрину **dm.finalmart**.

Ниже представлена структура репозитория:
```
├── crud
│   ├── __init__.py
│   ├── dds_brand.py
│   ├── dds_category.py
│   ├── dds_product.py
│   ├── dds_stock.py
│   ├── dds_stores.py
│   └── dds_transaction.py
├── dags
│   ├── __init__.py
│   ├── dag_ddl.py
│   ├── dag_dds.py
│   └── dag_dm.py
├── sql
│   ├── dds_create.sql
│   ├── dds_truncate.sql
│   ├── dm_create.sql
│   ├── dm_load.sql
│   ├── dm_truncate.sql
│   ├── error_create.sql
│   ├── error_truncate.sql
│   └── schema_create.sql
├── README.md
└── requirements.txt
```

## 2. Запуск
Запуск сервиса Apache Airflow 2.6.3 происходил из нативно установленной версии. 
Инструкция к нативной установке указана в [официальной документации Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start.html)
Требуемые зависимости для корректной работы сервиса указаны в файле [requirements.txt](requirements.txt).

Настройка окружения (если модуль apache-airflow установлен в одно из виртуальных окружений, то предварительно активируйте его)
```bash
pip install -Ur requirements.txt
```
Для корректкой работы сервиса Apache Airflow не обязательно переходить в директорию с папкой, т.к. в проекте добавлены абсолютные пути в `proj_path`.
```bash
airflow standalone
```
