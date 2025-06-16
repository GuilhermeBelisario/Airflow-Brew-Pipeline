from pyspark.sql import SparkSession
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import os
import pytest

load_dotenv()

access_key = os.getenv("AZURE_ACCESS_KEY")

spark = SparkSession.builder \
        .appName("tester") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("fs.azure.account.key.lofrey.dfs.core.windows.net", access_key) \
        .getOrCreate()

@pytest.fixture(scope="session")
def blob_service_client():

    connect_str = os.getenv("AZURE_ACCESS_KEY")

    if not connect_str:
        pytest.skip("A variável de ambiente AZURE_ACCESS_KEY não foi definida.")

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    return blob_service_client

@pytest.mark.parametrize("container_name", [
    "landingzone",
    "bronze",
    "silver"])
def test_container_foi_instanciado(blob_service_client, container_name):
    "Verifica se os contêineres essenciais existem no Azure Blob Storage."
    container_client = blob_service_client.get_container_client(container_name)
    assert container_client.exists(), f"O contêiner '{container_name}' deveria existir, mas não foi encontrado."

def test_leitura_landing_zone ():

    container_landing = os.getenv("CONTAINER_LANDING")
    df = spark.read.json(f"abfss://{container_landing}@lofrey.dfs.core.windows.net/*.json")
    if df is None:
        pytest.fail(f'Falha no teste de leitura dos arquivos.')
    else:
        assert df is not None