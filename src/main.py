from bronze_layer import escrevendo_dados_na_bronze
from silver_layer import transformar_dados
from gold_layer import criar_tabelas_para_consumidores
from gold_layer import gravar_tabelas_no_banco
from pyspark.sql import SparkSession

import os
from dotenv import load_dotenv

load_dotenv()


#Config do JDBC
jdbc_url = "jdbc:postgresql://localhost:55432/brewerydb"
connection_properties = {
    "user": "lofrey",
    "password": "lofrey",
    "driver": "org.postgresql.Driver"
    }


#Variaveis de ambiente:
container_landing = os.getenv("CONTAINER_LANDING")
container_bronze = os.getenv("CONTAINER_BRONZE")
container_silver = os.getenv("CONTAINER_SILVER")
container_gold = os.getenv("CONTAINER_GOLD")
access_key = os.getenv("AZURE_ACCESS_KEY")

#Abrindo Spark
spark = SparkSession.builder \
    .appName("SparkELT") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-azure:3.3.1,"
            "com.microsoft.azure:azure-storage:8.6.6,"
            "org.apache.hadoop:hadoop-azure-datalake:3.3.1,"
            "io.delta:delta-core_2.12:2.4.0,"
            "org.postgresql:postgresql:42.6.0") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

#Config do Azure
spark.conf.set("fs.azure.account.auth.type.lofrey.dfs.core.windows.net", "SharedKey")
spark.conf.set("fs.azure.account.key.lofrey.dfs.core.windows.net", access_key)


if __name__ == '__main__':
    
    try:
        escrevendo_dados_na_bronze(spark, container_landing,container_bronze)

        transformar_dados(spark, container_silver,container_bronze)

        df_list = criar_tabelas_para_consumidores(container_silver,spark, container_gold)
        
        tabelas = [
            ('dim_brewery_type', df_list[0]),
            ('dim_brewery', df_list[1]),
            ('dim_location', df_list[2]),
            ('fact_brewery_operations', df_list[3])
        ]
        
        for tabela, df in tabelas:
            gravar_tabelas_no_banco(
                df=df,
                table_name=tabela,
                jdbc_url=jdbc_url,
                connection_properties=connection_properties
            )
    except:
        print("Erro ao tentar executar o proceesso")