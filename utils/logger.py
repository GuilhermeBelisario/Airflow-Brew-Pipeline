import logging
from datetime import datetime
import os
import pandas as pd

class LoggerReport:

    def __init__(self, message: str, level: str):
        self.message = message
        self.level = level

    def criar_log(self, path: str):
        # Garante que a pasta existe
        log_dir = os.path.join(path, "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        file_path = os.path.join(log_dir, f"brewery_{datetime.today().strftime('%Y-%m-%d')}.log")
        
        # Formato limpo e consistente
        logging.basicConfig(
            level=logging.INFO,
            filename=file_path,
            filemode="a",
            format="%(asctime)s | %(levelname)s | Records: %(message)s"
        )

        
    def capturar_valor_medio_volumetria(self, log_files: list):
    
        import re
        
        # Regex procura por 'Records: ' seguido de dígitos
        pattern = r"Records:\s+(\d+)"
        data = []
        
        for file_path in log_files:
            with open(file_path, 'r') as f:
                for line in f:
                    match = re.search(pattern, line)
                    if match:
                        # Captura o grupo (dígitos)
                        data.append({'records': int(match.group(1))})
        
        df = pd.DataFrame(data)
        if not df.empty:
            return df['records'].mean()
        return 0


    def avaliacao_volumetrica(self, volume_atual: int):

        valor = capturar_valor_medio_volumetria()

        if volume_atual <= 0:
            logging.warning("DEU MERDAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")   


        if volume_atual > valor:
            logging.warning("")



    def salvar_log(self, path: str, logs: dict):
        pass

