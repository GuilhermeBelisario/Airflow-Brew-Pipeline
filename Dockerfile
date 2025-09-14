FROM apache/airflow:2.9.1

WORKDIR /app

USER root
RUN apt-get update && apt-get install -y wget
RUN wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
RUN tar -xvzf spark-3.5.1-bin-hadoop3.tgz
RUN mv spark-3.5.1-bin-hadoop3 /opt/spark
RUN apt-get install -y openjdk-11-jdk
RUN apt-get install -y libhadoop1
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME="/usr/lib/jvm/default-java"
ENV SPARK_HOME="/opt/spark"
ENV PATH="$PATH:$SPARK_HOME/bin"

COPY prod_requirements.txt ./

RUN pip install -r prod_requirements.txt

COPY dags/ .