# storage/database_postgres.py

import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, text

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from config.settings import settings
from utils.logger import app_logger
import psycopg2  # Adicionado para uso direto

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
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.logger = app_logger

    def get_connection(self):
        """Retorna uma conexão do SQLAlchemy (para pandas)"""
        return self.engine.connect()

    def _get_psycopg2_connection(self):

        from sqlalchemy import make_url
        url = make_url(settings.DATABASE_URL)
        return psycopg2.connect(
            host=url.host,
            port=url.port,
            database=url.database,
            user=url.username,
            password=url.password,
            sslmode=url.query.get('sslmode', 'require')
        )

    # --- DADOS DE MERCADO (mantido com SQLAlchemy ORM) ---
    def save_market_data(self, df: pd.DataFrame, symbol: str, interval: str) -> int:
        if df.empty:
            self.logger.warning(f"No data to save for {symbol}")
            return 0

        session = self.Session()
        records_inserted = 0
        
        try:
            for index, row in df.iterrows():
                timestamp = pd.to_datetime(index)
                open_val = float(row.get('Open')) if pd.notna(row.get('Open')) else None
                high_val = float(row.get('High')) if pd.notna(row.get('High')) else None
                low_val = float(row.get('Low')) if pd.notna(row.get('Low')) else None
                close_val = float(row.get('Close')) if pd.notna(row.get('Close')) else None
                volume_val = int(row.get('Volume')) if pd.notna(row.get('Volume')) else None
                sma_val = float(row.get('SMA')) if pd.notna(row.get('SMA')) else None
                rsi_val = float(row.get('RSI')) if pd.notna(row.get('RSI')) else None

                existing_record = session.query(MarketData).filter_by(
                    symbol=symbol, timestamp=timestamp, interval=interval
                ).first()

                if existing_record:
                    existing_record.open = open_val
                    existing_record.high = high_val
                    existing_record.low = low_val
                    existing_record.close = close_val
                    existing_record.volume = volume_val
                    existing_record.sma = sma_val
                    existing_record.rsi = rsi_val
                else:
                    new_record = MarketData(
                        symbol=symbol, timestamp=timestamp,
                        open=open_val, high=high_val, low=low_val, close=close_val,
                        volume=volume_val, interval=interval, sma=sma_val, rsi=rsi_val
                    )
                    session.add(new_record)
                    records_inserted += 1
            
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
        session = self.Session()
        try:
            data = session.query(MarketData).filter_by(
                symbol=symbol, interval=interval
            ).order_by(MarketData.timestamp.desc()).limit(limit).all()
            
            if not data:
                return pd.DataFrame()

            df = pd.DataFrame([
                {'timestamp': d.timestamp, 'Open': d.open, 'High': d.high, 'Low': d.low,
                 'Close': d.close, 'Volume': d.volume, 'SMA': d.sma, 'RSI': d.rsi}
                for d in data
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
        session = self.Session()
        stats = {'total_records': 0, 'unique_symbols': 0, 'last_update': 'N/A'}
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

    # --- NOTÍCIAS (corrigido para psycopg2 puro) ---
    def create_news_table(self):
        """Cria tabela de notícias com GIN index para arrays"""
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

        conn = self._get_psycopg2_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(create_table_query)
                cursor.execute(index1)
                cursor.execute(index2)
            conn.commit()
            app_logger.info("Tabela 'news' verificada/criada.")
        except Exception as e:
            conn.rollback()
            app_logger.error(f"Erro ao criar tabela news: {e}")
        finally:
            conn.close()

    def insert_news(self, title: str, content: str, url: str, source: str,
                    published_at: str, sentiment_label: str, sentiment_score: float,
                    symbols: list):
        """Insere notícia com suporte a array PostgreSQL"""
        query = """
        INSERT INTO news (title, content, url, source, published_at,
                          sentiment_label, sentiment_score, symbols)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        # Limpa e converte para lista segura
        clean_symbols = [str(s).strip() for s in symbols if s] if symbols else None

        conn = self._get_psycopg2_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (
                    title, content, url, source, published_at,
                    sentiment_label, sentiment_score, clean_symbols
                ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            app_logger.error(f"Erro ao salvar notícia: {e}")
        finally:
            conn.close()

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

    # --- FUNDAMENTALS (mantido com SQLAlchemy text) ---
    def create_fundamentals_tables(self):
        # (mantenha exatamente como está, funciona bem)
        # ... (seu código atual de criação de tabelas)
        pass

    def save_company_profile(self, symbol: str, company_name: str, sector: str, 
                       industry: str, description: str, website: str, 
                       market_cap: int, employees: int, country: str, 
                       currency: str, exchange: str):
        
        query = """
        INSERT INTO companies (symbol, company_name, sector, industry, description, 
                            website, market_cap, employees, country, currency, exchange, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (symbol) 
        DO UPDATE SET 
            company_name = EXCLUDED.company_name,
            sector = EXCLUDED.sector,
            industry = EXCLUDED.industry,
            description = EXCLUDED.description,
            website = EXCLUDED.website,
            market_cap = EXCLUDED.market_cap,
            employees = EXCLUDED.employees,
            country = EXCLUDED.country,
            currency = EXCLUDED.currency,
            exchange = EXCLUDED.exchange,
            updated_at = NOW()
        """
        
        conn = self._get_psycopg2_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (
                    symbol, company_name, sector, industry, description, website,
                    market_cap, employees, country, currency, exchange
                ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Erro ao salvar perfil: {e}")
        finally:
            conn.close()



    def save_income_statement(self, symbol: str, date: str, period: str,
                     revenue: float, cost_of_revenue: float, gross_profit: float,
                     operating_expenses: float, operating_income: float,
                     net_income: float, eps: float, ebitda: float):
        
        query = """
        INSERT INTO income_statements (symbol, date, period, revenue, cost_of_revenue,
                                    gross_profit, operating_expenses, operating_income,
                                    net_income, eps, ebitda)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date, period) DO NOTHING
        """
        
        conn = self._get_psycopg2_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (
                    symbol, date, period, revenue, cost_of_revenue, gross_profit,
                    operating_expenses, operating_income, net_income, eps, ebitda
                ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Erro ao salvar DRE: {e}")
        finally:
            conn.close()

    def save_balance_sheet(self, symbol: str, date: str, period: str, total_assets: int,
                     total_liabilities: int, total_equity: int, cash: int,
                     total_debt: int, working_capital: int):
        
        query = """
        INSERT INTO balance_sheets (symbol, date, period, total_assets, total_liabilities,
                                total_equity, cash, total_debt, working_capital)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date, period) DO NOTHING
        """
        
        conn = self._get_psycopg2_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, (
                    symbol, date, period, total_assets, total_liabilities, 
                    total_equity, cash, total_debt, working_capital
                ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Erro ao salvar balanço: {e}")
        finally:
            conn.close()

    def save_cash_flow(self, symbol: str, date: str, period: str, operating_cash_flow: int,
                      investing_cash_flow: int, financing_cash_flow: int, 
                      free_cash_flow: int, capex: int):
        """Salva fluxo de caixa"""
        query = """
        INSERT INTO cash_flows (symbol, date, period, operating_cash_flow,
                              investing_cash_flow, financing_cash_flow, free_cash_flow, capex)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date, period) DO NOTHING
        """
        
        with self.get_connection() as conn:
            conn.execute(text(query), (
                symbol, date, period, operating_cash_flow, investing_cash_flow,
                financing_cash_flow, free_cash_flow, capex
            ))
            conn.commit()

    def save_financial_ratios(self, symbol: str, date: str, period: str, pe_ratio: float,
                            pb_ratio: float, ps_ratio: float, roe: float, roa: float,
                            roi: float, debt_to_equity: float, current_ratio: float,
                            quick_ratio: float, gross_margin: float, operating_margin: float,
                            net_margin: float):
        """Salva ratios financeiros"""
        query = """
        INSERT INTO financial_ratios (symbol, date, period, pe_ratio, pb_ratio, ps_ratio,
                                    roe, roa, roi, debt_to_equity, current_ratio, quick_ratio,
                                    gross_margin, operating_margin, net_margin)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date, period) DO NOTHING
        """
        
        with self.get_connection() as conn:
            conn.execute(text(query), (
                symbol, date, period, pe_ratio, pb_ratio, ps_ratio, roe, roa, roi,
                debt_to_equity, current_ratio, quick_ratio, gross_margin, 
                operating_margin, net_margin
            ))
            conn.commit()

    def save_earnings_calendar(self, symbol: str, date: str, eps_estimate: float,
                             eps_actual: float, revenue_estimate: int, revenue_actual: int,
                             time: str):
        """Salva evento de earnings"""
        query = """
        INSERT INTO earnings_calendar (symbol, date, eps_estimate, eps_actual,
                                     revenue_estimate, revenue_actual, time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, date) DO NOTHING
        """
        
        with self.get_connection() as conn:
            conn.execute(text(query), (
                symbol, date, eps_estimate, eps_actual, revenue_estimate,
                revenue_actual, time
            ))
            conn.commit()

    def get_company_fundamentals_summary(self, symbol: str):
        """Retorna resumo dos fundamentals de uma empresa"""
        query = """
        SELECT 
            c.company_name, c.sector, c.industry, c.market_cap,
            i.revenue, i.net_income, i.eps,
            r.pe_ratio, r.pb_ratio, r.roe, r.debt_to_equity
        FROM companies c
        LEFT JOIN income_statements i ON c.symbol = i.symbol 
            AND i.date = (SELECT MAX(date) FROM income_statements WHERE symbol = c.symbol)
        LEFT JOIN financial_ratios r ON c.symbol = r.symbol 
            AND r.date = (SELECT MAX(date) FROM financial_ratios WHERE symbol = c.symbol)
        WHERE c.symbol = %s
        """
        
        with self.get_connection() as conn:
            df = pd.read_sql_query(query, conn, params=(symbol,))
        
        return df

