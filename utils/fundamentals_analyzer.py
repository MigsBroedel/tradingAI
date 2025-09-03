import pandas as pd
from typing import Dict, List, Tuple, Optional
from utils.logger import app_logger

class FundamentalsAnalyzer:
    """Analisador de dados fundamentalistas para scoring de empresas"""
    
    def __init__(self, db_manager):
        self.db = db_manager
        self.logger = app_logger
    
    def calculate_financial_health_score(self, symbol: str) -> Tuple[float, Dict]:
        """
        Calcula score de saúde financeira (0-100)
        Baseado em múltiplos indicadores fundamentalistas
        """
        try:
            # Busca dados mais recentes
            summary = self.db.get_company_fundamentals_summary(symbol)
            
            if summary.empty:
                self.logger.warning(f"Nenhum dado fundamental encontrado para {symbol}")
                return 0.0, {}
            
            row = summary.iloc[0]
            scores = {}
            
            # 1. Profitabilidade (25 pontos)
            roe = row.get('roe')
            if roe and roe > 0:
                if roe > 20:
                    scores['profitability'] = 25
                elif roe > 15:
                    scores['profitability'] = 20
                elif roe > 10:
                    scores['profitability'] = 15
                elif roe > 5:
                    scores['profitability'] = 10
                else:
                    scores['profitability'] = 5
            else:
                scores['profitability'] = 0
            
            # 2. Valuation (25 pontos)
            pe_ratio = row.get('pe_ratio')
            if pe_ratio and pe_ratio > 0:
                if pe_ratio < 15:
                    scores['valuation'] = 25
                elif pe_ratio < 20:
                    scores['valuation'] = 20
                elif pe_ratio < 25:
                    scores['valuation'] = 15
                elif pe_ratio < 35:
                    scores['valuation'] = 10
                else:
                    scores['valuation'] = 5
            else:
                scores['valuation'] = 0
            
            # 3. Endividamento (25 pontos)
            debt_to_equity = row.get('debt_to_equity')
            if debt_to_equity is not None:
                if debt_to_equity < 0.3:
                    scores['debt'] = 25
                elif debt_to_equity < 0.5:
                    scores['debt'] = 20
                elif debt_to_equity < 1.0:
                    scores['debt'] = 15
                elif debt_to_equity < 2.0:
                    scores['debt'] = 10
                else:
                    scores['debt'] = 5
            else:
                scores['debt'] = 0
            
            # 4. Crescimento (25 pontos) - baseado em receita
            revenue = row.get('revenue')
            if revenue and revenue > 0:
                # Simplificado: empresas com receita > 1B ganham pontos
                if revenue > 50_000_000_000:  # >50B
                    scores['growth'] = 25
                elif revenue > 10_000_000_000:  # >10B
                    scores['growth'] = 20
                elif revenue > 1_000_000_000:   # >1B
                    scores['growth'] = 15
                elif revenue > 100_000_000:     # >100M
                    scores['growth'] = 10
                else:
                    scores['growth'] = 5
            else:
                scores['growth'] = 0
            
            total_score = sum(scores.values())
            
            self.logger.info(f"Score financeiro para {symbol}: {total_score}/100")
            return total_score, scores
            
        except Exception as e:
            self.logger.error(f"Erro ao calcular score para {symbol}: {e}")
            return 0.0, {}
    
    def rank_companies_by_fundamentals(self, symbols: List[str]) -> pd.DataFrame:
        """Rankeia empresas por fundamentals"""
        results = []
        
        for symbol in symbols:
            if symbol.endswith('.SA'):
                continue  # Pula ações brasileiras
                
            score, breakdown = self.calculate_financial_health_score(symbol)
            
            if score > 0:
                results.append({
                    'symbol': symbol,
                    'total_score': score,
                    'profitability_score': breakdown.get('profitability', 0),
                    'valuation_score': breakdown.get('valuation', 0),
                    'debt_score': breakdown.get('debt', 0),
                    'growth_score': breakdown.get('growth', 0)
                })
        
        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values('total_score', ascending=False)
        
        return df
    
    def get_undervalued_stocks(self, symbols: List[str], pe_threshold: float = 15) -> List[str]:
        """Encontra ações potencialmente subvalorizadas"""
        undervalued = []
        
        for symbol in symbols:
            if symbol.endswith('.SA'):
                continue
                
            try:
                summary = self.db.get_company_fundamentals_summary(symbol)
                if not summary.empty:
                    pe_ratio = summary.iloc[0].get('pe_ratio')
                    roe = summary.iloc[0].get('roe', 0)
                    
                    # Critérios: P/E baixo E ROE > 10%
                    if pe_ratio and roe and pe_ratio < pe_threshold and roe > 10:
                        undervalued.append(symbol)
                        self.logger.info(f"{symbol} potencialmente subvalorizada: P/E={pe_ratio:.2f}, ROE={roe:.2f}%")
            
            except Exception as e:
                self.logger.error(f"Erro ao analisar {symbol}: {e}")
        
        return undervalued