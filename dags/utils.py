import time
import logging
from functools import wraps
from pyspark.sql import SparkSession

def timing_decorator(func):
    """
    Decorator para medir o tempo de execução das função e registrar nos logs.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__name__)
        
        start_time = time.time()
        logger.info(f"Iniciando a execução da função '{func.__name__}'...")
        
        try:
            result = func(*args, **kwargs)
            
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"Execução da função '{func.__name__}' concluída em {duration:.2f} segundos.")
            
            return result
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logger.error(f"Erro na função '{func.__name__}' após {duration:.2f} segundos: {str(e)}")
            raise
            
    return wrapper


def criar_spark(storage_account_name, access_key, app_name="BreweryELT"):
    """
    Cria e retorna uma instância do SparkSession.
    Inclui pacotes JARs necessários (Azure, Delta, Postgres) e otimizações de memória/timeout
    para garantir estabilidade em ambientes Airflow/Docker com recursos limitados.
    
    Args:
        storage_account_name (str): Nome da conta de storage do Azure
        access_key (str): Chave de acesso do Azure Storage
        app_name (str): Nome da aplicação Spark
        
    Returns:
        SparkSession: Sessão Spark configurada
    """
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Criando SparkSession '{app_name}' conectada ao cluster spark-master:7077")
        
        spark = SparkSession.builder \
            .master("spark://spark-master:7077") \
            .appName(app_name) \
            .config("spark.jars", "/opt/jars/hadoop-azure-3.3.1.jar,"
                                  "/opt/jars/azure-storage-8.6.6.jar,"
                                  "/opt/jars/hadoop-azure-datalake-3.3.1.jar,"
                                  "/opt/jars/delta-core_2.12-2.4.0.jar,"
                                  "/opt/jars/postgresql-42.6.0.jar") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.executor.cores", "2") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.network.timeout", "600s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .getOrCreate()

        logger.info("SparkSession criada com sucesso!")
        
        # Configuração de Acesso ao Azure Data Lake (Shared Key)
        logger.info(f"Configurando acesso ao Azure Storage: {storage_account_name}")
        
        spark.conf.set(
            f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", 
            "SharedKey"
        )
        spark.conf.set(
            f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", 
            access_key
        )
        
        logger.info("Configuração do Azure Storage concluída!")
        
        return spark
        
    except Exception as e:
        logger.error(f"Erro ao criar SparkSession: {str(e)}")
        raise