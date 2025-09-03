from datetime import datetime
from config.settings import settings
from utils.logger import app_logger
from data_collectors.market_data import MarketDataCollector
from data_collectors.news_collector import NewsCollector
from data_collectors.fundamentals_collector import FundamentalsCollector  # ← novo
from storage.database_postgres import DatabaseManager
import sys

def main():
    app_logger.info("="*60)
    app_logger.info("TRADING SYSTEM - FASE 4 - INICIANDO")
    app_logger.info("="*60)
    
    settings.create_directories()
    
    try:
        db = DatabaseManager()
        
        # Criar todas as tabelas necessárias
        db.create_news_table()
        db.create_fundamentals_tables()  # ← novo

        # === Coleta de dados de mercado ===
        market_collector = MarketDataCollector()
        market_results = market_collector.collect_multiple(
            symbols=settings.SYMBOLS,
            period=settings.HISTORY_PERIOD,
            interval=settings.DEFAULT_INTERVAL
        )

        # === Coleta de notícias ===
        news_collector = NewsCollector()
        news_collector.process_and_store()

        # === Coleta de dados fundamentalistas ===
        if settings.COLLECT_FUNDAMENTALS:
            fundamentals_collector = FundamentalsCollector()
            
            # Filtra apenas símbolos US (FMP não suporta .SA)
            us_symbols = [s for s in settings.SYMBOLS if not s.endswith('.SA')]
            
            if us_symbols:
                app_logger.info(f"Coletando fundamentals para: {us_symbols}")
                fundamentals_results = fundamentals_collector.collect_multiple_fundamentals(us_symbols)
                
                # Mostra resultados
                app_logger.info("\nRESULTADOS FUNDAMENTALS:")
                for symbol, results in fundamentals_results.items():
                    successful = sum(1 for success in results.values() if success)
                    app_logger.info(f"{symbol}: {successful}/{len(results)} coletados")
            else:
                app_logger.info("Nenhum símbolo US encontrado para coleta de fundamentals")

        # === Estatísticas finais ===
        stats = db.get_stats()
        app_logger.info(f"\nESTATÍSTICAS DO SISTEMA:")
        app_logger.info(f"📊 Dados de mercado: {stats['total_records']} registros")
        app_logger.info(f"🏢 Símbolos únicos: {stats['unique_symbols']}")
        app_logger.info(f"⏰ Último update: {stats['last_update']}")

        # Mostra resumo de fundamentals para primeira empresa US
        us_symbols = [s for s in settings.SYMBOLS if not s.endswith('.SA')]
        if us_symbols and settings.COLLECT_FUNDAMENTALS:
            first_symbol = us_symbols[0]
            fundamentals_summary = db.get_company_fundamentals_summary(first_symbol)
            
            if not fundamentals_summary.empty:
                row = fundamentals_summary.iloc[0]
                app_logger.info(f"\n📈 RESUMO FUNDAMENTALS - {first_symbol}:")
                app_logger.info(f"Empresa: {row.get('company_name', 'N/A')}")
                app_logger.info(f"Setor: {row.get('sector', 'N/A')}")
                app_logger.info(f"Market Cap: ${row.get('market_cap', 0):,.0f}")
                app_logger.info(f"P/E Ratio: {row.get('pe_ratio', 'N/A')}")
                app_logger.info(f"ROE: {row.get('roe', 'N/A')}%")

        # Mostra 3 notícias recentes
        latest_news = db.get_latest_news(limit=3)
        if not latest_news.empty:
            app_logger.info(f"\n📰 NOTÍCIAS RECENTES:")
            for _, row in latest_news.iterrows():
                app_logger.info(f"[{row['sentiment_label']}] {row['title'][:80]}...")

        app_logger.info("="*60)
        app_logger.info("🚀 TRADING SYSTEM - FASE 4 COMPLETA!")
        app_logger.info("="*60)
        
    except Exception as e:
        app_logger.error(f"Erro crítico no sistema: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()