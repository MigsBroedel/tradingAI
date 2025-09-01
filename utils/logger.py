import sys
from pathlib import Path
from loguru import logger
from config.settings import settings

def setup_logger():
    """Configura sistema de logs estruturado"""
    
    # Remove handler padrão
    logger.remove()
    
    # Console output com cores
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
               "<level>{level: <8}</level> | "
               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
               "<level>{message}</level>",
        level=settings.LOG_LEVEL,
        colorize=True
    )
    
    # Arquivo de log com rotação
    logger.add(
        settings.LOGS_DIR / "trading_system_{time:YYYY-MM-DD}.log",
        rotation="00:00",  # Nova arquivo a cada dia
        retention="30 days",  # Manter 30 dias
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
        level=settings.LOG_LEVEL
    )
    
    return logger

# Logger global para uso em todo projeto
app_logger = setup_logger()