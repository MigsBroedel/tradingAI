import yfinance as yf
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta
from .base_collector import BaseCollector
from storage.database import DatabaseManager

class MarketDataCollector(BaseCollector):
    """Coletor de dados de preços usando Yahoo Finance"""
    
    def __init__(self):
        super().__init__("MarketDataCollector")
        self.db = DatabaseManager()
    
    def collect(self, symbol: str, period: str = "30d", interval: str = "1d") -> bool:
        """
        Coleta dados de preços para um símbolo
        
        Args:
            symbol: Símbolo da ação (ex: AAPL)
            period: Período histórico (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
            interval: Intervalo (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
        
        Returns:
            bool: True se sucesso, False se falha
        """
        self.logger.info(f"Collecting data for {symbol} - period: {period}, interval: {interval}")
        
        try:
            # Coleta dados com retry
            data = self._retry_on_failure(self._fetch_data, symbol, period, interval)
            
            if data.empty:
                self.logger.warning(f"No data returned for {symbol}")
                return False
            
            # Valida dados
            if not self._validate_market_data(data, symbol):
                return False
            
            # Salva no banco
            records_inserted = self.db.save_market_data(data, symbol, interval)
            
            self.logger.info(f"Successfully collected {len(data)} records for {symbol}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to collect data for {symbol}: {e}")
            return False
    
    def _fetch_data(self, symbol: str, period: str, interval: str) -> pd.DataFrame:
        """Busca dados no Yahoo Finance"""
        self._rate_limit()
        
        ticker = yf.Ticker(symbol)
        data = ticker.history(period=period, interval=interval)
        
        return data
    
    def _validate_market_data(self, data: pd.DataFrame, symbol: str) -> bool:
        """Valida dados de mercado coletados"""
        if not super().validate_data(data):
            return False
        
        # Verifica colunas obrigatórias
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            self.logger.error(f"Missing columns for {symbol}: {missing_columns}")
            return False
        
        # Verifica valores negativos ou zero em preços
        price_columns = ['Open', 'High', 'Low', 'Close']
        for col in price_columns:
            if (data[col] <= 0).any():
                self.logger.warning(f"Found non-positive prices in {col} for {symbol}")
        
        # Verifica se High >= Low
        if (data['High'] < data['Low']).any():
            self.logger.error(f"Found High < Low for {symbol}")
            return False
        
        # Verifica outliers simples (variação > 50% em um dia)
        if len(data) > 1:
            daily_change = data['Close'].pct_change().abs()
            outliers = daily_change > 0.5
            if outliers.any():
                self.logger.warning(f"Found potential outliers for {symbol}: {outliers.sum()} records")
        
        self.logger.debug(f"Data validation passed for {symbol}")
        return True
    
    def collect_multiple(self, symbols: list, period: str = "30d", interval: str = "1d") -> dict[str, bool]:
        """Coleta dados para múltiplos símbolos"""
        results = {}
        
        self.logger.info(f"Starting collection for {len(symbols)} symbols")
        
        for symbol in symbols:
            symbol = symbol.strip().upper()
            results[symbol] = self.collect(symbol, period, interval)
            
            # Pequena pausa entre símbolos
            self._rate_limit()
        
        successful = sum(1 for success in results.values() if success)
        self.logger.info(f"Collection completed: {successful}/{len(symbols)} successful")
        
        return results