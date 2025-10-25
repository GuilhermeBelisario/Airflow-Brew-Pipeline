FROM apache/airflow:2.9.1

USER airflow

COPY prod_requirements.txt .

RUN pip install -r prod_requirements.txt


COPY config/ /opt/airflow/config/
COPY dags/ /opt/airflow/dags/