import pytest
import os
from dotenv import load_dotenv

load_dotenv()

@pytest.fixture
def conexao_azure():

    chave_de_conexao = os.getenv("AZURE_ACCESS_KEY")
    if not chave_de_conexao:
        pytest.fail("Variavel AZURE_ACCESS_KEY não definida.")
    return chave_de_conexao

@pytest.fixture
def container():

    container_landing = os.getenv("CONTAINER_LANDING")
    container_bronze = os.getenv("CONTAINER_BRONZE")
    container_silver = os.getenv("CONTAINER_SILVER")

    if not all([container_landing, container_bronze,container_silver]):
        pytest.fail("Um ou mais containers não foi definido.")
    return container_landing,container_bronze,container_silver