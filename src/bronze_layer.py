from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def escrevendo_dados_na_bronze(
    spark: SparkSession,
    container_landing: str,
    container_bronze: str,
    storage_account_name,
) -> str:

    if not all([container_landing, container_bronze]):
        raise ValueError("Parâmetros de conexão não podem ser nulos")

    df = spark.read.json(
        f"abfss://{container_landing}@{storage_account_name}.dfs.core.windows.net/*.json"
    )

    df = (
        df.withColumn("data_de_processamento", F.current_timestamp())
        .withColumn("origem_do_dado", F.lit("Landing Zone - BreweryDB API"))
        .withColumn("formato_na_origem", F.lit("JSON"))
        .withColumn("pipeline_vinculado", F.lit("BreweryDB API - ETL"))
        .withColumn("nome_do_arquivo_original", F.lit("brewery-extrated"))
    )

    print(f"""SCHEMA:{df.printSchema()}""")

    if df is not None:
        try:
            df.write.format("parquet").mode("overwrite").partitionBy(
                "data_de_processamento"
            ).save(
                f"abfss://{container_bronze}@{storage_account_name}.dfs.core.windows.net/brewery-bronze-extracted"
            )
            return print("Landing Zone para Bronze Layer finalizada!")
        except:
            return print("Falha ao tentar salvar o arquivo")

    else:
        return print("Falha ao tentar ler o arquivo")
