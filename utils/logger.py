import logging
from datetime import datetime
import os
import json


class LoggerReport:

    def __init__(self, message: str, level: str):
        self.message = message
        self.level = level
        self.traceback = None

    def calculo_de_nulos(self):
        pass

    def avaliacao_volumetrica(self):
        pass

    def _resultado_log(self, name_file: str, level: str):

        file_path = os.getcwd()
        file_path = os.path.normpath(file_path)
        file_path = os.path.join(file_path, "logs", f"brewery_{datetime.today().strftime('%Y-%m-%d')}.log")
        
        logging.basicConfig(
            level=logging.WARNING,
            filename=file_path,
            filemode="a",
            format="%(asctime)s | -- | %(levelname)s | -- | %(message)s")

        return logging.warning(f"Arquivo: {name_file}")

    def salvar_log(self, path: str, logs: dict):
        pass

