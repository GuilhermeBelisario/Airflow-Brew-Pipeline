from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, regexp_replace, substring, col, to_date, when,concat_ws,year,month
from datetime import datetime
import os



def baixar_arquivos_container_bronze_e_transformar_os_dados(connection_azure: str, container_bronze: str):
    
    if not all([connection_azure, container_bronze]):
        raise ValueError("Parâmetros de conexão não podem ser nulos")
    
    conexao = BlobServiceClient.from_connection_string(connection_azure)
    conexao_container_bronze = conexao.get_container_client(container=container_bronze)
    lista_de_arquivos = [blob.name for blob in conexao_container_bronze.list_blobs()]

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

        df = df.withColumnRenamed('stat_province', 'state') \
            .withColumnRenamed('postal_code', 'zip_code') \
            .withColumn(
                "fixed_phone",
                concat(
                    lit("+1 "),  
                    substring(regexp_replace(col("phone"), "[^0-9]", ""), 1, 3),  
                    lit("-"),
                    substring(regexp_replace(col("phone"), "[^0-9]", ""), 4, 3), 
                    lit("-"),
                    substring(regexp_replace(col("phone"), "[^0-9]", ""), 7, 4)
                )
            ) \
            .withColumn('created_at', to_date(col('created_at'), 'yyyy-MM-dd')) \
            .withColumn('updated_at', to_date(col('updated_at'), 'yyyy-MM-dd')) \
            .withColumn('latitude', col('latitude').cast('float')) \
            .withColumn('longitude', col('longitude').cast('float')) \
            .withColumn(
                'has_location',
                when(col('longitude').isNull() | col('latitude').isNull(), lit(0)).otherwise(lit(1))
            ) \
            .withColumn(
                'full_address',
                concat_ws(', ',  # Usa concat_ws para evitar erro de vírgula e nulos
                    lit('St.'),
                    col('address_1'),
                    col('city'),
                    col('state'),
                    col('zip_code'),
                    col('country')
                )
            ) \
            .withColumn(
                'is_phone_missing',
                when(col('phone').isNull(), lit(1)).otherwise(lit(0))
            ) \
            .withColumn('created_year', year(col('created_at'))) \
            .withColumn('created_month', month(col('created_at')))
        
        #Salvando novo arquivo local
        today_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_file_path = os.path.join('data',f'trusted_{today_date}.parquet')
        df.write.mode("overwrite").parquet(new_file_path)

        if os.path.exist(new_file_path):
            return print(f'Arquivo Parquet salvo com sucesso!')
        else:
            return print('Ocorreu algum erro ao salvar o arquivo')
        
def salvar_novo_arquivo_no_container_silver(connection_azure: str, container_silver: str):
    
    if not all([connection_azure, container_silver]):
        raise ValueError("Parâmetros de conexão não podem ser nulos")
    
    path_parquet_files = 'data/'

    # Lista arquivos que terminam com .parquet
    arquivos_parquet = [f for f in os.listdir(path_parquet_files) if f.endswith('.parquet')]


    if arquivos_parquet:
    
        conexao = BlobServiceClient.from_connection_string(connection_azure)
        
        try: 
            #Reescrevendo o dado na bronze
            for item in path_parquet_files:
                file_name = os.path.join('data/',item)
                conexao_container_bronze = conexao.get_blob_client(container=container_silver, blob=item.name())
                with open(file_name,'rb') as data:
                    conexao_container_bronze.upload_blob(data, overwrite=False)

        except Exception as e:
            print(f"Erro ao processar o arquivo {item}:   {e}")

    else:
        return print('Não encontrei nenhum arquivos parquet no diretório.')