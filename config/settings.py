import os
from pathlib import Path
from typing import List
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
load_dotenv()

class Settings:
    """Configurações centralizadas do sistema"""
    
    # Paths do projeto
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    LOGS_DIR = BASE_DIR / "logs"
    
    # Banco de dados
    DATABASE_PATH = os.getenv("DATABASE_PATH", "data/trading_system.db")
    
    # Configurações de coleta
    SYMBOLS: List[str] = os.getenv("SYMBOLS", "AAPL,GOOGL").split(",")
    DEFAULT_INTERVAL = os.getenv("DEFAULT_INTERVAL", "1d")
    HISTORY_PERIOD = os.getenv("HISTORY_PERIOD", "30d")
    
    # Logs
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    DEBUG = os.getenv("DEBUG", "False").lower() == "true"
    
    # Rate limiting (para evitar bloqueios)
    REQUEST_DELAY = 1.0  # segundos entre requests
    MAX_RETRIES = 3
    
    @classmethod
    def create_directories(cls):
        """Cria diretórios necessários se não existirem"""
        cls.DATA_DIR.mkdir(exist_ok=True)
        cls.LOGS_DIR.mkdir(exist_ok=True)

settings = Settings()