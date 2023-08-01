# Стажировка КОРУС Консалтинг, команда 2
> Стажёр-разработчик Георгий Новожилов \
> Системный аналитик Валентина Сорокина

## 1. Слои данных
В проекте используются 4 слоя данных: **DDS**, **DM**, **REF_INFO** и **ERROR**
### `DDS`
В слое **DDS** содержатся таблицы **brand**, **category**, **product**, **stores**, **stock** и **transaction**. В эти таблицы поступают обработанные данные из одноимённых таблиц схемы **sources** БД **internship_sources**. Скрипты обработки и загрузки находятся в директории [**crud/**](crud/)
### `DM`
В слое **DM** хранится единственная таблица **finalmart** - это витрина, содержащая в себе значения для последующей визуализации. Содержит объединённые значения из всех таблиц слоя DDS. Скрипт загрузки данных находится в [**dm_load.sql**](sql/dm_load.sql)
### `ERROR`
В слое **ERROR** содержатся одноимённые таблицы из **DDS** с дополнительными полями, которые отображают найденные ошибки при обработке данных. Данные, которые не могут быть изменены вручную или слишком объёмные для удаления из БД помещаются в этот слой. 
### `REF_INFO`
В слое **REF_INFO** хранится таблица **error_types** - нормативно-справочная информация для схемы **error**
<br>
Категории ошибок **res_info.error_types**:
|error_id|error_type|
|---|---|
|misc|Прочие|
|dup_idx|Дубликат-индекс|
|dup_val|Дубликат-значение|
|inc_val|Некорректное значение|
|inc_vlim|Некорректное значение (ограничение 40 символов)|
|empty|Пустое значение|
|missing|Отсутствует связь сежду таблицами из-за нехватки ключей|

## 2. Структура
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
│   ├── ref_info_create.sql
│   └── schema_create.sql
├── .env
├── docker-compose-airflow.yaml
├── Dockerfile
├── README.md
└── requirements.txt
```

> Примечание: для корректной работы сервиса требуется создать папку **src/** в корневой директории проекта, поместив туда *.csv* файлы с транзакциями и названиями магазинов - **transaction.csv** и **stores.csv** соответственно

## 3. Запуск сервиса Apache Airflow
Запуск сервиса Apache Airflow 2.6.3 происходит при помощи **docker compose**. Официальная инструкция к установке указана в [официальной документации по установке Docker](https://docs.docker.com/desktop/install/windows-install/).

1. Скопируйте репозиторий на свою систему:
```bash
git clone https://github.com/Kaboupi/korus_interns_2.git
```
2. Перейдите в директорию с проектом:
```bash
cd korus_interns_2
```
3. Создайте папки для запуска **Apache Airflow**:
```bash
mkdir config logs plugins
```
4. (Только при первом запуске!) Инициализируйте базу данных в **Apache Airflow**:
```bash
docker compose -f docker-compose-airflow.yaml up airflow-init
```
5. После иниализации запустите контейнер:
```bash
docker compose -f docker-compose-airflow.yaml up -d
```
6. В интерфейсе https://localhost:8080/ по пути **Admin -> Connections** укажите соединения к БД

## 4. Запуск сервиса Apache Superset
Официальная инструкция к установке указана в [официальной документации к установке Apache Superset при помощи Docker Compose](https://superset.apache.org/docs/installation/installing-superset-using-docker-compose).

1. Перейдите в папку с проектом **korus_interns_2**:
```bash
cd korus_interns_2
```
2. Скопируйте репозиторий Apache Superset на свою систему:
```bash
git clone https://github.com/apache/superset.git
```
3. Перейдите в директорию **superset**:
```bash
cd superset
```
4. Запустите контейнер:
```bash
docker compose -f docker-compose-non-dev.yml up -d
```
5. Перейдите в интерфейс **Apache Superset** по адресу https://localhost:8088/ и установите подключение с базой данных
