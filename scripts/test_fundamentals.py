import os
import sys

# ✅ CORREÇÃO: Adiciona o diretório pai ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.database_postgres import DatabaseManager
from config.settings import settings
from utils.logger import app_logger
import pandas as pd

# Análise simples sem arquivo separado (para evitar problemas de import)
class SimpleFundamentalsAnalyzer:
    """Analisador simples de fundamentals integrado no script"""
    
    def __init__(self, db_manager):
        self.db = db_manager
    
    def calculate_financial_health_score(self, symbol: str):
        """Score simples baseado nos dados disponíveis"""
        try:
            summary = self.db.get_company_fundamentals_summary(symbol)
            
            if summary.empty:
                return 0.0, {}
            
            row = summary.iloc[0]
            scores = {}
            total_score = 0
            
            # ROE Score (0-25)
            roe = row.get('roe')
            if roe and roe > 0:
                if roe > 20:
                    scores['roe'] = 25
                elif roe > 15:
                    scores['roe'] = 20
                elif roe > 10:
                    scores['roe'] = 15
                else:
                    scores['roe'] = 10
            else:
                scores['roe'] = 0
            
            # P/E Score (0-25)
            pe = row.get('pe_ratio')
            if pe and pe > 0:
                if pe < 15:
                    scores['pe'] = 25
                elif pe < 25:
                    scores['pe'] = 15
                else:
                    scores['pe'] = 5
            else:
                scores['pe'] = 0
            
            # Debt Score (0-25)
            debt_equity = row.get('debt_to_equity')
            if debt_equity is not None:
                if debt_equity < 0.5:
                    scores['debt'] = 25
                elif debt_equity < 1.0:
                    scores['debt'] = 15
                else:
                    scores['debt'] = 5
            else:
                scores['debt'] = 0
            
            # Revenue Score (0-25)
            revenue = row.get('revenue')
            if revenue and revenue > 0:
                if revenue > 10_000_000_000:  # >10B
                    scores['revenue'] = 25
                elif revenue > 1_000_000_000:  # >1B
                    scores['revenue'] = 15
                else:
                    scores['revenue'] = 10
            else:
                scores['revenue'] = 0
            
            total_score = sum(scores.values())
            return total_score, scores
            
        except Exception as e:
            app_logger.error(f"Erro ao calcular score para {symbol}: {e}")
            return 0.0, {}

def main():
    """Testa coleta e análise de fundamentals"""
    
    print("🔍 TESTE DE DADOS FUNDAMENTALISTAS")
    print("=" * 50)
    
    try:
        db = DatabaseManager()
        analyzer = SimpleFundamentalsAnalyzer(db)
        
        # Filtra símbolos US
        us_symbols = [s for s in settings.SYMBOLS if not s.endswith('.SA')]
        
        if not us_symbols:
            print("❌ Nenhum símbolo US configurado")
            print("💡 Configure símbolos como AAPL,MSFT,GOOGL no arquivo .env")
            return
        
        print(f"🎯 Analisando: {us_symbols}\n")
        
        # 1. Verifica se tabelas existem
        print("📋 VERIFICANDO TABELAS...")
        try:
            db.create_fundamentals_tables()
            print("✅ Tabelas de fundamentals verificadas")
        except Exception as e:
            print(f"❌ Erro nas tabelas: {e}")
            return
        
        # 2. Mostra dados para cada símbolo
        companies_found = 0
        
        for symbol in us_symbols:
            print(f"\n📊 DADOS DE {symbol}:")
            print("-" * 40)
            
            try:
                summary = db.get_company_fundamentals_summary(symbol)
                
                if summary.empty:
                    print("❌ Nenhum dado encontrado")
                    print("💡 Execute 'python main.py' primeiro para coletar dados")
                    continue
                    
                companies_found += 1
                row = summary.iloc[0]
                
                print(f"📈 Empresa: {row.get('company_name', 'N/A')}")
                print(f"🏭 Setor: {row.get('sector', 'N/A')}")
                print(f"💰 Market Cap: ${row.get('market_cap', 0):,.0f}")
                print(f"📊 Receita: ${row.get('revenue', 0):,.0f}")
                print(f"💵 Lucro: ${row.get('net_income', 0):,.0f}")
                print(f"📈 EPS: ${row.get('eps', 0):.2f}")
                print(f"⚖️  P/E: {row.get('pe_ratio', 'N/A')}")
                print(f"🔄 ROE: {row.get('roe', 'N/A')}%")
                print(f"💳 Debt/Equity: {row.get('debt_to_equity', 'N/A')}")
                
                # Score simples
                score, breakdown = analyzer.calculate_financial_health_score(symbol)
                print(f"🏆 Score Financeiro: {score}/100")
                
            except Exception as e:
                print(f"❌ Erro ao analisar {symbol}: {e}")
        
        # 3. Resumo final
        print(f"\n📈 RESUMO:")
        print("=" * 30)
        print(f"✅ Empresas com dados: {companies_found}/{len(us_symbols)}")
        
        if companies_found == 0:
            print("\n💡 PRÓXIMOS PASSOS:")
            print("1. Configure API keys no arquivo .env:")
            print("   FMP_API_KEY=sua_chave_aqui")
            print("2. Execute: python main.py")
            print("3. Execute novamente este teste")
        else:
            print("🎉 Dados fundamentalistas funcionando corretamente!")
            
    except Exception as e:
        print(f"💥 Erro no teste: {e}")
        print("\n🔧 DICAS DE SOLUÇÃO:")
        print("1. Verifique se o banco PostgreSQL está rodando")
        print("2. Verifique as configurações no .env")
        print("3. Execute 'python main.py' primeiro")

if __name__ == "__main__":
    main()