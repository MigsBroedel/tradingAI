import os
from pathlib import Path
from typing import List
from dotenv import load_dotenv

load_dotenv()

class Settings:
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    LOGS_DIR = BASE_DIR / "logs"
    
    # Banco de dados
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://neondb_owner:...")

    # Configurações de coleta de mercado
    SYMBOLS: List[str] = os.getenv("SYMBOLS", "AAPL,GOOGL").split(",")
    DEFAULT_INTERVAL = os.getenv("DEFAULT_INTERVAL", "1d")
    HISTORY_PERIOD = os.getenv("HISTORY_PERIOD", "30d")
    
    # NewsAPI
    NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
    NEWS_SOURCES = os.getenv("NEWS_SOURCES", "reuters,bloomberg").split(",")
    NEWS_LANGUAGE = "pt" if any(s.endswith(".SA") for s in SYMBOLS) else "en"
    NEWS_PERIOD_HOURS = 24

    # Fundamentals APIs
    FMP_API_KEY = os.getenv("FMP_API_KEY")
    ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
    COLLECT_FUNDAMENTALS = os.getenv("COLLECT_FUNDAMENTALS", "True").lower() == "true"
    FUNDAMENTALS_UPDATE_FREQUENCY = os.getenv("FUNDAMENTALS_UPDATE_FREQUENCY", "daily")

    # Logs
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    DEBUG = os.getenv("DEBUG", "False").lower() == "true"
    
    # Rate limiting
    REQUEST_DELAY = 1.0
    MAX_RETRIES = 3
    
    @classmethod
    def create_directories(cls):
        cls.DATA_DIR.mkdir(exist_ok=True)
        cls.LOGS_DIR.mkdir(exist_ok=True)

settings = Settings()