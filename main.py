# main.py

from datetime import datetime
from config.settings import settings
from utils.logger import app_logger
from data_collectors.market_data import MarketDataCollector
from data_collectors.news_collector import NewsCollector  # ← novo
from storage.database_postgres import DatabaseManager
import sys

def main():
    app_logger.info("="*60)
    app_logger.info("TRADING SYSTEM - INICIANDO")
    app_logger.info("="*60)
    
    settings.create_directories()
    
    try:
        db = DatabaseManager()
        db.create_news_table()  # Garante que tabela news existe

        # === Coleta de dados de mercado ===
        market_collector = MarketDataCollector()
        results = market_collector.collect_multiple(
            symbols=settings.SYMBOLS,
            period=settings.HISTORY_PERIOD,
            interval=settings.DEFAULT_INTERVAL
        )

        # === Coleta de notícias ===
        news_collector = NewsCollector()
        news_collector.process_and_store()

        # === Estatísticas ===
        stats = db.get_stats()
        app_logger.info(f"\nESTATÍSTICAS DO BANCO:")
        app_logger.info(f"Total de registros: {stats['total_records']}")
        app_logger.info(f"Símbolos únicos: {stats['unique_symbols']}")
        app_logger.info(f"Último update: {stats['last_update']}")

        # Mostra 3 notícias recentes
        latest_news = db.get_latest_news(limit=3)
        if not latest_news.empty:
            app_logger.info(f"\nNOTÍCIAS RECENTES:")
            for _, row in latest_news.iterrows():
                app_logger.info(f"[{row['sentiment_label']}] {row['title']} ({row['source']})")

        app_logger.info("="*60)
        app_logger.info("TRADING SYSTEM - FINALIZADO COM SUCESSO")
        app_logger.info("="*60)
        
    except Exception as e:
        app_logger.error(f"Erro crítico no sistema: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()