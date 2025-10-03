from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

def escrevendo_dados_na_bronze(spark, container_landing: str, container_bronze: str, storage_account_name):

    if not all([container_landing, container_bronze]):
        raise ValueError("Parâmetros de conexão não podem ser nulos")
    
    df = spark.read.json(f"abfss://{container_landing}@{storage_account_name}.dfs.core.windows.net/*.json")
    
    df = (df
        .withColumn('data_de_processamento', current_timestamp())
        .withColumn('origem_do_dado', lit('Landing Zone - BreweryDB API'))
        .withColumn('formato_na_origem', lit('JSON'))
        .withColumn('pipeline_vinculado', lit('BreweryDB API - ETL'))
        .withColumn('nome_do_arquivo_original', lit("brewery-extrated"))
    )

    print(f"""SCHEMA:{df.printSchema()}""")

    df.printSchema()
    if df is not None:  
        try:
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("data_de_processamento") \
                .save(f"abfss://{container_bronze}@{storage_account_name}.dfs.core.windows.net/brewery-bronze-extracted")
            return print("Landing Zone para Bronze Layer finalizada!")
        except:
            return print('Falha ao tentar salvar o arquivo')
            
    else:
        return print("Falha ao tentar ler o arquivo")

    