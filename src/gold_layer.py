from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, regexp_replace, substring, col



def criar_tabelas_para_consumidores(container_silver: str,connection_azure: str, container_gold: str):
    pass

