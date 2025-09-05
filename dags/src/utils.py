import time
import logging
from functools import wraps

def timing_decorator(func):
    """
    Decorator para medir o tempo de execução das função e registrar nos logs.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Captura o logger da função para registrar mensagens
        logger = logging.getLogger(func.__name__)
        
        start_time = time.time()
        logger.info(f"Iniciando a execução da função '{func.__name__}'...")
        
        # Executa a função original
        result = func(*args, **kwargs)
        
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Execução da função '{func.__name__}' concluída em {duration:.2f} segundos.")
        
        return result
    return wrapper