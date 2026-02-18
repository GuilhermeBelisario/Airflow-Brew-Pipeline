#!/bin/bash

echo "-- Carregando as variaveis de ambiente dos arquivos .env --"
set -a
    source config/postgredb.env
    source config/airflow.env
    source config/.env
set +a

sudo chown -R 50000:0 ./logs  
echo "-- Baixando os JARs do Spark (se necessario) --"
mkdir -p jars

download_if_missing() {
    if [ ! -f "$2" ]; then
        echo "Baixando $2..."
        curl -L -o "$2" "$1"
    else
        echo "$2 ja existe, pulando..."
    fi
}

download_if_missing https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.1/hadoop-azure-3.3.1.jar jars/hadoop-azure-3.3.1.jar
download_if_missing https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar jars/azure-storage-8.6.6.jar
download_if_missing https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure-datalake/3.3.1/hadoop-azure-datalake-3.3.1.jar jars/hadoop-azure-datalake-3.3.1.jar
download_if_missing https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar jars/delta-core_2.12-2.4.0.jar
download_if_missing https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar jars/postgresql-42.6.0.jar

echo "-- Apagando os volumes e containers do Docker --"
docker-compose down --volumes

echo "-- Iniciando os containers do Docker --"
docker-compose up -d --build --remove-orphans