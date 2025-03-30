from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, regexp_replace, substring, col
import os
import shutil


def baixar_arquivos_container_bronze_e_transformar(container_silver: str,connection_azure: str, container_bronze: str):
    
    if not all([connection_azure, container_silver, container_bronze]):
        raise ValueError("Parâmetros de conexão não podem ser nulos")
    
    conexao = BlobServiceClient.from_connection_string(connection_azure)
    conexao_container_silver = conexao.get_container_client(container=container_bronze)
    lista_de_arquivos = [blob.name for blob in conexao_container_silver.list_blobs()]

    if not lista_de_arquivos:
        print('Nenhum arquivo encontrado no container de landing.')
        return

    spark = SparkSession.builder \
                    .appName("ETLSpark") \
                    .getOrCreate()
    
    for item in lista_de_arquivos:

        #Baixando o arquivo do zero!  
        caminho_do_arquivo_baixado = os.path.join('data',f'{item}.json')
        os.makedirs(os.path.dirname(caminho_do_arquivo_baixado), exist_ok=True)
        cliente_blob_arquivo = conexao.get_blob_client(container=container_bronze, blob=item)

        with open(caminho_do_arquivo_baixado, "wb") as arquivo:
            download_da_azure = cliente_blob_arquivo.download_blob()
            arquivo.write(download_da_azure.readall())

        # Transformação/Leitura
        df = spark.read.json(caminho_do_arquivo_baixado)
        df = df.withColumn(
        "telefone_arrumado",concat(
            lit("+1 "),  
            substring(regexp_replace(col("telefone"), "[^0-9]", ""), 1, 3),  
            lit("-"),  # Hífen
            substring(regexp_replace(col("telefone"), "[^0-9]", ""), 4, 3), 
            lit("-"),  # Hífen
            substring(regexp_replace(col("telefone"), "[^0-9]", ""), 7, 4)
                )
            )  
                