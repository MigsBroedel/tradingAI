from abc import ABC, abstractmethod
from typing import Any, Dict, List
import time
from utils.logger import app_logger
from config.settings import settings

class BaseCollector(ABC):
    """Classe base abstrata para todos os coletores de dados"""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = app_logger
        self.request_delay = settings.REQUEST_DELAY
        self.max_retries = settings.MAX_RETRIES
    
    @abstractmethod
    def collect(self, *args, **kwargs) -> Any:
        """Método principal de coleta - deve ser implementado pelas subclasses"""
        pass
    
    def _rate_limit(self):
        """Aplica rate limiting entre requests"""
        if self.request_delay > 0:
            time.sleep(self.request_delay)
    
    def _retry_on_failure(self, func, *args, **kwargs):
        """Executa função com retry automático em caso de falha"""
        for attempt in range(self.max_retries):
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt  # Backoff exponencial
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"All {self.max_retries} attempts failed for {func.__name__}")
                    raise e
    
    def validate_data(self, data: Any) -> bool:
        """Validação básica dos dados coletados"""
        if data is None:
            self.logger.error("Data is None")
            return False
        return True