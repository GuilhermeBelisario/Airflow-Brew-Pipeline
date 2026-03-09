FROM apache/airflow:3.0.0

USER airflow

COPY prod_requirements.txt .

RUN pip install -r prod_requirements.txt


COPY config/ /opt/airflow/config/
COPY jars/ /opt/airflow/jars