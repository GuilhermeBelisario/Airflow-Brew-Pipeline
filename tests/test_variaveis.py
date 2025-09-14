import pytest
import os
from dotenv import load_dotenv

load_dotenv('/home/lofrey/workplace/Airflow-Brewery-API/Airflow-Brew-Pipeline/config/.env')

# Fixtures dos testes

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

@pytest.fixture
def chaves_postgres():
    
    postgres_user = os.getenv("POSTGRE_USER")
    postres_password = os.getenv("POSTGRE_PASSWORD")

    if not all([postgres_user, postres_password]):
        pytest.fail("Um ou mais containers não foi definido.")
    return postgres_user,postres_password

#Testes

def test_validar_conexao_com_azure(conexao_azure):
    """
    Este teste RECEBE 'conexao_azure' como um argumento.
    """
    print(f"\n   Dentro do teste, recebemos a chave: {conexao_azure[:5]}...")
    assert conexao_azure is not None
    assert isinstance(conexao_azure, str)

def test_validar_credenciais_postgres(chaves_postgres):
    """
    Este teste para 'chaves_postgres' verifica se existe user e password do banco.
    """
    usuario, senha = chaves_postgres # Desempacotando a tupla retornada
    
    print(f"\n   Dentro do teste, recebemos o usuário: {usuario}")
    assert usuario is not None
    assert senha is not None

def test_valida_nome_container(container):
    """
    Este testa se a função container tem retorno dos nomes dos containers que serão usados
    """
    landing, bronze, silver = container
    assert [landing, bronze, silver] is not None
