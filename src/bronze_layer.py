from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import os
from datetime import datetime
import shutil

def adicionado_os_arquivos_da_para_processar(container_landing: str,connection_azure: str, container_bronze: str):

    if not all([connection_azure, container_landing, container_bronze]):
        raise ValueError("Parâmetros de conexão não podem ser nulos")
    
    conexao = BlobServiceClient.from_connection_string(connection_azure)
    conexao_container_landing = conexao.get_container_client(container=container_landing)
    lista_de_arquivos = [blob.name for blob in conexao_container_landing.list_blobs()]

    if not lista_de_arquivos:
        print('Nenhum arquivo encontrado no container de landing.')
        return

    spark = SparkSession.builder \
                    .appName("ReadJSONExample") \
                    .getOrCreate()
    
    for item in lista_de_arquivos:

        #Baixando o arquivo do zero!
        
        caminho_do_arquivo_baixado = os.path.join('data',f'{item}.json')
        os.makedirs(os.path.dirname(caminho_do_arquivo_baixado), exist_ok=True)
        cliente_blob_arquivo = conexao.get_blob_client(container=container_landing, blob=item)

        with open(caminho_do_arquivo_baixado, "wb") as arquivo:
            download_da_azure = cliente_blob_arquivo.download_blob()
            arquivo.write(download_da_azure.readall())

        # Leitura e adição de meta dados
        df = spark.read.json(caminho_do_arquivo_baixado)
        
        df = (df
            .withColumn('data_de_processamento', current_timestamp())
            .withColumn('origem_do_dado', lit('Landing Zone - BreweryDB API'))
            .withColumn('formato_na_origem', lit('JSON'))
            .withColumn('pipeline_vinculado', lit('BreweryDB API - ETL'))
            .withColumn('nome_do_arquivo_original', lit(item))
        )

        print(f"""DataFrame criado para o arquivo: {item}
        SCHEMA:
            """)
        df.printSchema()

        temp_dir = os.path.abspath(f"temp_{item.replace('/', '_')}.json")
        df.coalesce(1).write.json(temp_dir, mode="overwrite")
        arquivos_processados = [f for f in os.listdir(temp_dir) if f.endswith('.json')]

        if not arquivos_processados:
            raise FileNotFoundError(f"Nenhum arquivo JSON encontrado em {temp_dir}")
     
        caminho_arquivos_processados = os.path.join(temp_dir, arquivos_processados[0])
        destino_final = os.path.join('data', f"processed_{item}.json")
        
        if os.path.exists(destino_final):
            os.remove(destino_final)  # Remove o arquivo processado anterior, se existir

        shutil.move(caminho_arquivos_processados, destino_final)
        print(f"Arquivo movido para: {destino_final}")

        try: 
            #Reescrevendo o dado na bronze
            conexao_container_bronze = conexao.get_blob_client(container=container_bronze, blob=f"processed_{item}.json")
            with open(destino_final,'rb') as data:
                conexao_container_bronze.upload_blob(data, overwrite=False)

            shutil.rmtree(temp_dir)
            print(f"Diretório temporário removido: {temp_dir}")

        except Exception as e:
            print(f"Erro ao processar o arquivo {item}: {str(e)}")

        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

        try:

            for nome_arquivo in os.listdir('data'):
                caminho_pasta_para_limpar = os.path.join('data', nome_arquivo)
                if os.path.isfile(caminho_pasta_para_limpar) or os.path.islink(caminho_pasta_para_limpar):
                    os.unlink(caminho_pasta_para_limpar)
                elif os.path.isdir(caminho_pasta_para_limpar):
                    shutil.rmtree(caminho_pasta_para_limpar)

        except FileNotFoundError as fnfe:
            print(f"Erro: Arquivo não encontrado. Detalhes: {fnfe}")
        except PermissionError as pe:
            print(f"Erro: Permissão negada. Detalhes: {pe}")
        except Exception as e:
            print(f"Erro inesperado ao manipular arquivos. Detalhes: {e}")
                

    return print("Landing Zone para Bronze Layer finalizada!")