from bronze_layer import adicionado_os_arquivos_da_para_processar
import os
from dotenv import load_dotenv

load_dotenv()

container_landing = os.getenv("CONTAINER_LANDING")
connection_azure = os.getenv("AZURE_STORAGE_CONNECTION")
container_bronze = os.getenv("CONTAINER_BRONZE")


os.environ["PYTHONIOENCODING"] = "UTF-8"


adicionado_os_arquivos_da_para_processar(container_landing,connection_azure,container_bronze)