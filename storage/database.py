import sqlite3
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import pandas as pd
from utils.logger import app_logger
from config.settings import settings

class DatabaseManager:
    """Gerenciador do banco de dados SQLite"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or settings.DATABASE_PATH
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._create_tables()
        app_logger.info(f"Database initialized at: {self.db_path}")
    
    def _create_tables(self):
        """Cria tabelas se não existirem"""
        with sqlite3.connect(self.db_path) as conn:
            # Tabela de dados de preços
            conn.execute("""
                CREATE TABLE IF NOT EXISTS market_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    datetime TEXT NOT NULL,
                    interval_type TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, datetime, interval_type)
                )
            """)
            
            # Índices para performance
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_market_symbol_datetime 
                ON market_data(symbol, datetime)
            """)
            
            conn.commit()
            app_logger.info("Database tables created/verified")
    
    def save_market_data(self, df: pd.DataFrame, symbol: str, interval: str) -> int:
        """Salva dados de mercado no banco"""
        if df.empty:
            app_logger.warning(f"Empty dataframe for {symbol}")
            return 0
        
        # Prepara dados para inserção
        records = []
        for index, row in df.iterrows():
            records.append({
                'symbol': symbol,
                'datetime': index.strftime('%Y-%m-%d %H:%M:%S'),
                'interval_type': interval,
                'open': row['Open'],
                'high': row['High'],
                'low': row['Low'],
                'close': row['Close'],
                'volume': row['Volume']
            })
        
        # Insere no banco (ignora duplicatas)
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            inserted = 0
            
            for record in records:
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO market_data 
                        (symbol, datetime, interval_type, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record['symbol'], record['datetime'], record['interval_type'],
                        record['open'], record['high'], record['low'], 
                        record['close'], record['volume']
                    ))
                    if cursor.rowcount > 0:
                        inserted += 1
                except sqlite3.Error as e:
                    app_logger.error(f"Error inserting record for {symbol}: {e}")
            
            conn.commit()
            
        app_logger.info(f"Inserted {inserted} new records for {symbol}")
        return inserted
    
    def get_market_data(self, symbol: str, interval: str, limit: int = 100) -> pd.DataFrame:
        """Recupera dados de mercado do banco"""
        query = """
            SELECT datetime, open, high, low, close, volume
            FROM market_data 
            WHERE symbol = ? AND interval_type = ?
            ORDER BY datetime DESC
            LIMIT ?
        """
        
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(query, conn, params=(symbol, interval, limit))
            
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            
        return df
    
    def get_stats(self) -> Dict[str, Any]:
        """Estatísticas do banco de dados"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Total de registros
            cursor.execute("SELECT COUNT(*) FROM market_data")
            total_records = cursor.fetchone()[0]
            
            # Símbolos únicos
            cursor.execute("SELECT COUNT(DISTINCT symbol) FROM market_data")
            unique_symbols = cursor.fetchone()[0]
            
            # Último update
            cursor.execute("SELECT MAX(created_at) FROM market_data")
            last_update = cursor.fetchone()[0]
            
        return {
            'total_records': total_records,
            'unique_symbols': unique_symbols,
            'last_update': last_update
        }