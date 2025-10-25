# Import das funções do Pipeline
from bronze_layer import escrevendo_dados_na_bronze
from silver_layer import transformar_dados
from gold_layer import criar_tabelas_para_consumidores
from gold_layer import gravar_tabelas_no_banco

# Import de uma ferramenta para gerar log
from utils import timing_decorator
from utils import criar_spark

# Import das lib's do projeto
from airflow.models.dag import dag
from airflow.decorators import task
from dotenv import load_dotenv
from datetime import datetime
import os


load_dotenv('/opt/airflow/config/.env')

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


spark = criar_spark(storage_account_name,access_key)

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
        print("Iniciando a tarefa de transição para a camada Bronze...")
        escrevendo_dados_na_bronze(spark, container_landing,container_bronze,storage_account_name)
        
    @task(task_id="transicao_para_camada_silver")
    @timing_decorator
    def task_camada_bronze():
        print("Iniciando a tarefa de transição para a camada Silver...")
        transformar_dados(spark, container_silver,container_bronze,storage_account_name)

        
    @task(task_id="transicao_para_camada_gold")
    @timing_decorator
    def task_camada_silver():
        print("Iniciando a tarefa de transição para a camada Gold...")
        dfs_dict = criar_tabelas_para_consumidores(container_silver,spark,storage_account_name)
        return dfs_dict

    @task(task_id="escrevendo_no_postgres")
    @timing_decorator
    def task_camada_gold(dfs_dict):
        print("Iniciando a tarefa de escrita no Postgres...")
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