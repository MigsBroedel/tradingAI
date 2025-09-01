import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from config.settings import settings
from utils.logger import app_logger

Base = declarative_base()

class MarketData(Base):
    __tablename__ = 'market_data'
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    interval = Column(String, nullable=False)
    sma = Column(Float, nullable=True)
    rsi = Column(Float, nullable=True)

    def __repr__(self):
        return f"<MarketData(symbol='{self.symbol}', timestamp='{self.timestamp}', close='{self.close}')>"

class DatabaseManager:
    """Gerencia a conexão e operações com o banco de dados"""
    
    def __init__(self):
        self.engine = create_engine(settings.DATABASE_URL)
        Base.metadata.create_all(self.engine)  # Cria as tabelas se não existirem
        self.Session = sessionmaker(bind=self.engine)
        self.logger = app_logger
    
    def get_connection(self):
        """
        Retorna uma conexão crua do SQLAlchemy (para uso com pandas ou psycopg2-style)
        """
        return self.engine.connect()

    def save_market_data(self, df: pd.DataFrame, symbol: str, interval: str) -> int:
        """
        Salva dados de mercado no banco de dados.
        Atualiza registros existentes e insere novos.
        """
        if df.empty:
            self.logger.warning(f"No data to save for {symbol}")
            return 0

        session = self.Session()
        records_inserted = 0
        
        try:
            for index, row in df.iterrows():
                # Converte o índice (timestamp) para datetime
                timestamp = pd.to_datetime(index)
                
                # Converte tipos NumPy para tipos Python nativos
                open_val = float(row.get('Open')) if pd.notna(row.get('Open')) else None
                high_val = float(row.get('High')) if pd.notna(row.get('High')) else None
                low_val = float(row.get('Low')) if pd.notna(row.get('Low')) else None
                close_val = float(row.get('Close')) if pd.notna(row.get('Close')) else None
                volume_val = int(row.get('Volume')) if pd.notna(row.get('Volume')) else None
                sma_val = float(row.get('SMA')) if pd.notna(row.get('SMA')) else None
                rsi_val = float(row.get('RSI')) if pd.notna(row.get('RSI')) else None

                # Verifica se o registro já existe
                existing_record = session.query(MarketData).filter_by(
                    symbol=symbol,
                    timestamp=timestamp,
                    interval=interval
                ).first()

                if existing_record:
                    # Atualiza o registro existente
                    existing_record.open = open_val
                    existing_record.high = high_val
                    existing_record.low = low_val
                    existing_record.close = close_val
                    existing_record.volume = volume_val
                    existing_record.sma = sma_val
                    existing_record.rsi = rsi_val
                    self.logger.debug(f"Updated record for {symbol} at {timestamp}")
                else:
                    # Insere um novo registro
                    new_record = MarketData(
                        symbol=symbol,
                        timestamp=timestamp,
                        open=open_val,
                        high=high_val,
                        low=low_val,
                        close=close_val,
                        volume=volume_val,
                        interval=interval,
                        sma = sma_val,
                        rsi = rsi_val
                    )
                    session.add(new_record)
                    records_inserted += 1
                    self.logger.debug(f"Inserted new record for {symbol} at {timestamp}")
            
            session.commit()
            self.logger.info(f"Successfully saved {records_inserted} new records for {symbol} to PostgreSQL.")
            return records_inserted
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error saving data for {symbol}: {e}")
            return 0
        finally:
            session.close()

    def get_market_data(self, symbol: str, interval: str, limit: int = 5) -> pd.DataFrame:
        """
        Retorna os últimos 'limit' registros de dados de mercado para um símbolo e intervalo.
        """
        session = self.Session()
        try:
            data = session.query(MarketData).filter_by(
                symbol=symbol,
                interval=interval
            ).order_by(MarketData.timestamp.desc()).limit(limit).all()
            
            if not data:
                return pd.DataFrame()

            df = pd.DataFrame([
                {
                    'timestamp': d.timestamp,
                    'Open': d.open,
                    'High': d.high,
                    'Low': d.low,
                    'Close': d.close,
                    'Volume': d.volume,
                    'SMA': d.sma,
                    'RSI': d.rsi
                } for d in data
            ])
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            return df
        except Exception as e:
            self.logger.error(f"Error retrieving data for {symbol}: {e}")
            return pd.DataFrame()
        finally:
            session.close()

    def get_stats(self) -> dict:
        """
        Retorna estatísticas básicas do banco de dados.
        """
        session = self.Session()
        stats = {
            'total_records': 0,
            'unique_symbols': 0,
            'last_update': 'N/A'
        }
        try:
            stats['total_records'] = session.query(MarketData).count()
            stats['unique_symbols'] = session.query(MarketData.symbol).distinct().count()
            
            last_record = session.query(MarketData).order_by(MarketData.timestamp.desc()).first()
            if last_record:
                stats['last_update'] = last_record.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            return stats
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return stats
        finally:
            session.close()
    

    def create_news_table(self):
    
        create_table_query = """
        CREATE TABLE IF NOT EXISTS news (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT,
            url VARCHAR(500),
            source VARCHAR(100),
            published_at TIMESTAMP WITH TIME ZONE,
            sentiment_label VARCHAR(10) CHECK (sentiment_label IN ('positive', 'negative', 'neutral')),
            sentiment_score NUMERIC(4,3),
            symbols TEXT[],
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """
        index1 = "CREATE INDEX IF NOT EXISTS idx_news_symbols ON news USING GIN(symbols);"
        index2 = "CREATE INDEX IF NOT EXISTS idx_news_published ON news(published_at);"

        with self.get_connection() as conn:
            conn.execute(text(create_table_query))
            conn.execute(text(index1))
            conn.execute(text(index2))
            conn.commit()
        app_logger.info("Tabela 'news' verificada/criada.")

    def insert_news(self, title: str, content: str, url: str, source: str,
                    published_at: str, sentiment_label: str, sentiment_score: float,
                    symbols: list):
        """Insere uma notícia no banco"""
        query = """
        INSERT INTO news (title, content, url, source, published_at,
                        sentiment_label, sentiment_score, symbols)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.get_connection() as conn:
            conn.execute(text(query), (
                title, content, url, source, published_at,
                sentiment_label, sentiment_score, symbols
            ))
            conn.commit()

    def get_latest_news(self, symbol: str = None, limit: int = 10):
        if symbol:
            query = """
            SELECT * FROM news 
            WHERE %s = ANY(symbols)
            ORDER BY published_at DESC 
            LIMIT %s
            """
            params = (symbol, limit)
        else:
            query = "SELECT * FROM news ORDER BY published_at DESC LIMIT %s"
            params = (limit,)

        with self.get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=params)
        return df

