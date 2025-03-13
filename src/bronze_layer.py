from azure.storage.blob import BlobServiceClient
import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

def adicionado_os_arquivos_da_para_processar(container_landing: str,connection_azure: str):

    conexao = BlobServiceClient.from_connection_string(connection_azure)
    conexao_container = conexao.get_container_client(container=container_landing)

    lista_de_arquivos = []
    for i in conexao_container.list_blobs():
        if i.astype.str.endswitch(".json"):
            lista_de_arquivos.append(i)
        else:
            print(f'O arquivo {i} está em um formato diferente do esperado.')

    if lista_de_arquivos is not None:
        return lista_de_arquivos