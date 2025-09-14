# Import das funções do Pipeline
from bronze_layer import escrevendo_dados_na_bronze
from silver_layer import transformar_dados
from gold_layer import criar_tabelas_para_consumidores
from gold_layer import gravar_tabelas_no_banco

# Import de uma ferramenta para gerar log
from utils import timing_decorator

# Import das lib's do projeto
from airflow.models.dag import dag
from airflow.decorators import task
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from datetime import datetime
import os


load_dotenv('/home/lofrey/workplace/Airflow-Brewery-API/Airflow-Brew-Pipeline/config/.env')

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
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
access_key = os.getenv("AZURE_ACCESS_KEY")

def criar_spark():
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
    return spark

print("Iniciando o Pipeline...")
@dag(
    dag_id="brewery_elt_pipeline",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 9, 10),
    catchup=False,
    tags=["elt", "spark", "brewery"]
)
def brewery_elt_pipeline():

    @task(task_id="transicao_para_camada_bronze")
    @timing_decorator
    def task_camada_landing():

        spark = criar_spark()
        escrevendo_dados_na_bronze(spark, container_landing,container_bronze,storage_account_name)
        
    @task(task_id="transicao_para_camada_silver")
    @timing_decorator
    def task_camada_bronze():

        spark = criar_spark()
        transformar_dados(spark, container_silver,container_bronze,storage_account_name)

        
    @task(task_id="transicao_para_camada_gold")
    @timing_decorator
    def task_camada_silver():
        
        spark = criar_spark()
        dfs_dict = criar_tabelas_para_consumidores(container_silver,spark,storage_account_name)
        return dfs_dict

    @task(task_id="escrevendo_no_postgres")
    @timing_decorator
    def task_camada_gold(dfs_dict):

        for nome, df in dfs_dict.values():
            gravar_tabelas_no_banco(
                df=df,
                table_name=nome,
                jdbc_url=jdbc_url,
                connection_properties=connection_properties
            )
        print(f'Tabelas {list(dfs_dict.keys())} gravadas.')


    landing_task = task_camada_landing()
    bronze_task = task_camada_bronze()
    silver_task = task_camada_silver()
    gold_task = task_camada_gold(silver_task)

    landing_task >> bronze_task >> silver_task >> gold_task

brewery_elt_pipeline()