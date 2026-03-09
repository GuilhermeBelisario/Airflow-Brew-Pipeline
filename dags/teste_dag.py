from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pyspark.sql.functions as F


def teste_spark_local():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("teste-bronze-local")
        .config("spark.driver.host", "airflow-scheduler")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", "7078")
        .config("spark.blockManager.port", "7079")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "1")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .getOrCreate()
    )

    print("=" * 50)
    print(f"Spark versão: {spark.version}")
    print(f"Spark Master: {spark.sparkContext.master}")
    print("=" * 50)

    # Simula dados JSON que viriam do Azure (BreweryDB)
    dados_fake = [
        {"id": "1", "name": "Brewery A", "brewery_type": "micro",  "city": "São Paulo"},
        {"id": "2", "name": "Brewery B", "brewery_type": "large",  "city": "Rio de Janeiro"},
        {"id": "3", "name": "Brewery C", "brewery_type": "nano",   "city": "Curitiba"},
        {"id": "4", "name": "Brewery D", "brewery_type": "brewpub","city": "Belo Horizonte"},
    ]

    df = spark.createDataFrame(dados_fake)

    # Mesmas transformações do pipeline real
    df = (
        df.withColumn("data_de_processamento", F.current_timestamp())
        .withColumn("origem_do_dado",           F.lit("Landing Zone - BreweryDB API"))
        .withColumn("formato_na_origem",         F.lit("JSON"))
        .withColumn("pipeline_vinculado",        F.lit("BreweryDB API - ETL"))
        .withColumn("nome_do_arquivo_original",  F.lit("brewery-extrated"))
    )

    print("SCHEMA:")
    df.printSchema()
    df.show(truncate=False)

    output_path = "/tmp/bronze-teste-output"

    # Escrita em Parquet com partitionBy — igual ao pipeline real
    df.write.format("parquet").mode("overwrite").partitionBy(
        "data_de_processamento"
    ).save(output_path)

    # Valida lendo de volta
    df_lido = spark.read.parquet(output_path)
    total = df_lido.count()

    print(f"\nArquivos gravados com sucesso! Total de linhas: {total}")
    df_lido.show(truncate=False)

    assert total == len(dados_fake), f"Esperado {len(dados_fake)} linhas, mas encontrou {total}"

    spark.stop()
    print("Teste finalizado com sucesso!")


with DAG(
    dag_id="dag_teste_spark_local",
    description="Testa conexão e escrita Parquet no Spark sem Azure",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # só roda manualmente
    catchup=False,
    tags=["teste", "spark"],
) as dag:

    task_teste = PythonOperator(
        task_id="teste_spark_bronze",
        python_callable=teste_spark_local,
    )