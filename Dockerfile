FROM apache/airflow:2.6.3
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt