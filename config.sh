#!/bin/bash

echo "-- Carregando as variaveis de ambiente dos arquivos .env --"
set -a && source config/postgredb.env && source config/airflow.env && set +a

sudo chown -R 50000:0 ./logs  

echo "-- Apagando os volumes e containers do Docker --"
docker-compose down --volumes

echo "-- Iniciando os containers do Docker --"
docker-compose up -d --build