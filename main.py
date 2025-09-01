#!/usr/bin/env python3
"""
Trading System - Entry Point
Sistema de coleta de dados para trading automatizado
"""

import sys
from datetime import datetime
from config.settings import settings
from utils.logger import app_logger
from data_collectors.market_data import MarketDataCollector
from storage.database import DatabaseManager

def main():
    """Função principal do sistema"""
    app_logger.info("="*60)
    app_logger.info("TRADING SYSTEM - INICIANDO")
    app_logger.info("="*60)
    
    # Cria diretórios necessários
    settings.create_directories()
    
    try:
        # Inicializa coletor de dados de mercado
        market_collector = MarketDataCollector()
        
        # Coleta dados para os símbolos configurados
        app_logger.info(f"Symbols to collect: {settings.SYMBOLS}")
        app_logger.info(f"Interval: {settings.DEFAULT_INTERVAL}")
        app_logger.info(f"Period: {settings.HISTORY_PERIOD}")
        
        results = market_collector.collect_multiple(
            symbols=settings.SYMBOLS,
            period=settings.HISTORY_PERIOD,
            interval=settings.DEFAULT_INTERVAL
        )
        
        # Mostra resultados
        app_logger.info("\nRESULTADOS DA COLETA:")
        for symbol, success in results.items():
            status = "✓ SUCCESS" if success else "✗ FAILED"
            app_logger.info(f"{symbol}: {status}")
        
        # Estatísticas do banco
        db = DatabaseManager()
        stats = db.get_stats()
        
        app_logger.info("\nESTATÍSTICAS DO BANCO:")
        app_logger.info(f"Total de registros: {stats['total_records']}")
        app_logger.info(f"Símbolos únicos: {stats['unique_symbols']}")
        app_logger.info(f"Último update: {stats['last_update']}")
        
        # Exemplo de consulta
        if settings.SYMBOLS:
            first_symbol = settings.SYMBOLS[0].strip()
            sample_data = db.get_market_data(first_symbol, settings.DEFAULT_INTERVAL, limit=5)
            
            if not sample_data.empty:
                app_logger.info(f"\nÚLTIMOS 5 REGISTROS - {first_symbol}:")
                app_logger.info(f"\n{sample_data.to_string()}")
        
        app_logger.info("="*60)
        app_logger.info("TRADING SYSTEM - FINALIZADO COM SUCESSO")
        app_logger.info("="*60)
        
    except Exception as e:
        app_logger.error(f"Erro crítico no sistema: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()