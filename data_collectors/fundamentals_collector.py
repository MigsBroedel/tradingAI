# data_collectors/fundamentals_collector.py

import requests
import yfinance as yf
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, text
from typing import Dict, List
from storage.database_postgres import DatabaseManager
from config.settings import settings
from utils.logger import app_logger
import time

class FundamentalsCollector:
    """Coletor de dados fundamentalistas usando Alpha Vantage e yfinance (gratuitos)"""

    def __init__(self):
        self.db = DatabaseManager()
        self.av_base_url = "https://www.alphavantage.co/query"
        self.logger = app_logger

    def collect_company_profile(self, symbol: str) -> bool:
        """Coleta perfil da empresa usando Alpha Vantage"""
        if not settings.ALPHA_VANTAGE_API_KEY:
            self.logger.warning("ALPHA_VANTAGE_API_KEY não configurada")
            return False

        try:
            params = {
                "function": "OVERVIEW",
                "symbol": symbol,
                "apikey": settings.ALPHA_VANTAGE_API_KEY
            }

            self.logger.info(f"🔍 Buscando perfil de {symbol} via Alpha Vantage")
            response = requests.get(self.av_base_url, params=params, timeout=10)

            if response.status_code != 200:
                self.logger.error(f"Erro HTTP {response.status_code} ao buscar perfil de {symbol}")
                return False

            data = response.json()

            print(data)

            if "Note" in data:
                self.logger.error(f"Alpha Vantage: limite de requisições atingido (aguarde 1 min)")
                return False

            if "Error Message" in data:
                self.logger.error(f"Erro na API: {data['Error Message']}")
                return False

            if "Symbol" not in data:
                self.logger.warning(f"Dados não encontrados para {symbol}")
                return False

            # Salva no banco
            self.db.save_company_profile(
                symbol=data["Symbol"],
                company_name=data.get("Name", ""),
                sector=data.get("Sector", ""),
                industry=data.get("Industry", ""),
                description=data.get("Description", ""),
                website=data.get("Website", ""),
                market_cap=float(data.get("MarketCapitalization", 0)),
                employees=int(data.get("FullTimeEmployees", 0)),
                country=data.get("Country", ""),
                currency=data.get("Currency", ""),
                exchange=data.get("Exchange", "")
            )

            self.logger.info(f"✅ Perfil coletado para {symbol}: {data['Name']}")
            return True

        except Exception as e:
            self.logger.error(f"❌ Erro ao coletar perfil de {symbol}: {e}")
            return False

    def collect_financial_statements(self, symbol: str) -> bool:
        """Coleta DRE, Balanço e Fluxo de Caixa via yfinance"""
        try:
            ticker = yf.Ticker(symbol)
            results = 0

            # Demonstração de Resultados
            income_stmt = ticker.income_stmt
            if income_stmt is not None and not income_stmt.empty:
                latest = income_stmt.iloc[:, 0]  # Último ano
                self.db.save_income_statement(
                    symbol=symbol,
                    date=str(latest.name)[:10],
                    period="FY",
                    revenue=latest.get("Total Revenue", 0),
                    cost_of_revenue=latest.get("Cost of Revenue", 0),
                    gross_profit=latest.get("Gross Profit", 0),
                    operating_expenses=latest.get("Operating Expenses", 0),  # ✅ adicionado
                    operating_income=latest.get("Operating Income", 0),
                    net_income=latest.get("Net Income", 0),
                    eps=latest.get("Diluted EPS", 0),
                    ebitda=latest.get("EBITDA", 0)
                )
                results += 1

            # Balanço Patrimonial
            balance_sheet = ticker.balance_sheet
            if balance_sheet is not None and not balance_sheet.empty:
                latest = balance_sheet.iloc[:, 0]
                self.db.save_balance_sheet(
                    symbol=symbol,
                    date=str(latest.name)[:10],
                    period="FY",
                    total_assets=latest.get("Total Assets", 0),
                    total_liabilities=latest.get("Total Liabilities Net Minority Interest", 0),
                    total_equity=latest.get("Stockholders Equity", 0),
                    cash=latest.get("Cash And Cash Equivalents", 0),
                    total_debt=latest.get("Net Debt", 0),
                    working_capital=latest.get("Working Capital", 0)
                )
                results += 1

            # Fluxo de Caixa
            cash_flow = ticker.cashflow
            if cash_flow is not None and not cash_flow.empty:
                latest = cash_flow.iloc[:, 0]
                self.db.save_cash_flow(
                    symbol=symbol,
                    date=str(latest.name)[:10],
                    period="FY",
                    operating_cash_flow=latest.get("Operating Cash Flow", 0),
                    investing_cash_flow=latest.get("Investing Cash Flow", 0),
                    financing_cash_flow=latest.get("Financing Cash Flow", 0),
                    free_cash_flow=latest.get("Free Cash Flow", 0),
                    capex=latest.get("Capital Expenditure", 0)
                )
                results += 1

            if results > 0:
                self.logger.info(f"📊 {results}/3 demonstrativos salvos para {symbol}")
            else:
                self.logger.warning(f"Nenhum demonstrativo encontrado para {symbol}")

            return results > 0

        except Exception as e:
            self.logger.error(f"❌ Erro ao coletar demonstrativos de {symbol}: {e}")
            return False

    def collect_key_ratios(self, symbol: str) -> bool:
        """Coleta ratios via Alpha Vantage"""
        if not settings.ALPHA_VANTAGE_API_KEY:
            return False

        try:
            
            params = {
                "function": "OVERVIEW",
                "symbol": symbol,
                "apikey": settings.ALPHA_VANTAGE_API_KEY
            }

            response = requests.get(self.av_base_url, params=params, timeout=10)
            if response.status_code != 200:
                return False

            data = response.json()
            if "Symbol" not in data:
                return False

            self.db.save_financial_ratios(
                symbol=symbol,
                date=datetime.now().strftime("%Y-%m-%d"),
                period="TTM",
                pe_ratio=float(data.get("PE_RATIO", 0)),
                pb_ratio=float(data.get("PB_RATIO", 0)),
                ps_ratio=float(data.get("PS_RATIO", 0)),
                roe=float(data.get("RETURN_ON_EQUITY_TTM", 0)),
                roa=float(data.get("RETURN_ON_ASSETS_TTM", 0)),
                roi=float(data.get("RETURN_ON_INVESTMENT", 0)),
                debt_to_equity=float(data.get("DEBT_TO_EQUITY", 0)),
                current_ratio=float(data.get("CURRENT_RATIO", 0)),
                quick_ratio=float(data.get("QUICK_RATIO", 0)),
                gross_margin=float(data.get("GROSS_MARGIN_TTM", 0)),
                operating_margin=float(data.get("OPERATING_MARGIN_TTM", 0)),
                net_margin=float(data.get("NET_PROFIT_MARGIN_TTM", 0))
            )

            self.logger.info(f"📈 Ratios salvos para {symbol}")
            return True

        except Exception as e:
            self.logger.error(f"❌ Erro ao coletar ratios de {symbol}: {e}")
            return False

    def collect_all_fundamentals(self, symbol: str) -> Dict[str, bool]:
        """Coleta todos os fundamentos para um símbolo"""
        self.logger.info(f"📥 Iniciando coleta de fundamentals para {symbol}")

        results = {}

        results['profile'] = self.collect_company_profile(symbol)
        time.sleep(1)

        results['statements'] = self.collect_financial_statements(symbol)
        time.sleep(1)

        results['ratios'] = self.collect_key_ratios(symbol)

        successful = sum(1 for v in results.values() if v)
        total = len(results)
        self.logger.info(f"✅ Fundamentals para {symbol}: {successful}/{total} coletados")

        return results

    def collect_multiple_fundamentals(self, symbols: List[str]) -> Dict[str, Dict[str, bool]]:
        """Coleta fundamentals para múltiplos símbolos"""
        all_results = {}
        valid_symbols = [s.strip().upper() for s in symbols if s.strip()]

        self.logger.info(f"🚀 Iniciando coleta de fundamentals para {len(valid_symbols)} símbolos")

        for symbol in valid_symbols:
            if symbol.endswith('.SA'):
                self.logger.warning(f"⚠️ Pulando {symbol} - yfinance/AV têm limitações com ações brasileiras")
                continue

            try:
                all_results[symbol] = self.collect_all_fundamentals(symbol)
            except Exception as e:
                self.logger.error(f"❌ Falha ao coletar {symbol}: {e}")
                all_results[symbol] = {"success": False}

            time.sleep(1.5)  # respeitar rate limit

        return all_results