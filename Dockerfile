FROM apache/airflow:2.9.1

WORKDIR /app

COPY prod_requirements.txt ./

RUN pip install -r prod_requirements.txt

COPY dags/ .