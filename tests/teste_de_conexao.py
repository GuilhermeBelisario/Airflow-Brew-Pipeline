from azure.storage.blob import BlobServiceClient
import pytest
import os

container = os.getenv("CONTAINER_LANDING")
conexao_azure = os.getenv("AZURE_STORAGE_CONNECTION")

@pytest.fixture
def conexao_azure():

    chave_de_conexao = os.getenv("AZURE_STORAGE_CONNECTION")
    if not chave_de_conexao:
        pytest.fail("Variavel AZURE_STORAGE_CONNECTION não definida.")
    return chave_de_conexao

@pytest.fixture
def container():

    nome_do_container = os.getenv("CONTAINER_LANDING")
    if not nome_do_container:
        pytest.fail("Container 'CONTAINER_LANDING' não definido.")
    return nome_do_container

def teste_da_chave_de_conexao_azure(conexao_azure):

    if conexao_azure is None:
        pytest.fail("Chave de conexão vazia: AZURE_STORAGE_CONNECTION.")
    else:
        assert conexao_azure is not None, print(f'{conexao_azure} não está fazia!')

def teste_se_container_existe_no_azure(container, conexao_azure):
    
    if conexao_azure is None:
        pytest.fail("Chave vazia: AZURE_STORAGE_CONNECTION.")
    try:
        blob_service_client = BlobServiceClient.from_chave_de_conexao(conexao_azure)
        container_client = blob_service_client.get_container_client(container)
        container_exists = container_client.exists()

        assert container_exists is not None, print(f'Container: {container} existe!')
    except:
        pytest.fail("Erro ao tentar conectar!")