FROM apache/airflow:2.9.1

WORKDIR /app

USER root
RUN apt-get update \
    && apt-get install -y default-jdk \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME="/usr/lib/jvm/default-java"
ENV SPARK_HOME="/usr/local/spark"
ENV PATH="$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin"

COPY prod_requirements.txt ./

RUN pip install -r prod_requirements.txt

COPY dags/ .