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
    "user": os.getenv("POSTGRE_USER"),
    "password": os.getenv("POSTGRE_PASSWORD"),
    "driver": "org.postgresql.Driver"
    }


#Variaveis de ambiente:
container_landing = os.getenv("CONTAINER_LANDING")
container_bronze = os.getenv("CONTAINER_BRONZE")
container_silver = os.getenv("CONTAINER_SILVER")
container_gold = os.getenv("CONTAINER_GOLD")
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
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
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "SharedKey")
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key)


if __name__ == '__main__':
    
    print('Iniciando a transição para camada bronze...')
    escrevendo_dados_na_bronze(spark, container_landing,container_bronze,storage_account_name)
    print('Concluida!')

    print('Iniciando a camada bronze...')
    transformar_dados(spark, container_silver,container_bronze,storage_account_name)
    print('Concluida!')
    
    print('Iniciando a camada Silver...')
    dfs_dict = criar_tabelas_para_consumidores(container_silver,spark, container_gold,storage_account_name)
    print('Concluida!')

    print('Iniciando a camada gold...')     
    for nome, df in dfs_dict.values():
        gravar_tabelas_no_banco(
            df=df,
            table_name=nome,
            jdbc_url=jdbc_url,
            connection_properties=connection_properties
        )
    print(f'Tabelas {list(dfs_dict.keys())} gravadas.')
