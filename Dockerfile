FROM apache/airflow:2.9.1

WORKDIR /app

USER root

# Instala dependências, JDK e Spark em uma única camada
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget **openjdk-17-jdk-headless** && \
    wget -q https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.1-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Volta para o usuário airflow
USER airflow

# Configura variáveis de ambiente
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV HADOOP_HOME=/opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=${PATH}:${SPARK_HOME}/bin


# Instala dependências Python
COPY prod_requirements.txt .
RUN pip install --no-cache-dir -r prod_requirements.txt

# Copia os arquivos JAR necessários
COPY jars/ /opt/jars/
COPY dags/ .