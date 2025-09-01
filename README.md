"""
# Trading System - MVP Fase 1

Sistema de coleta automatizada de dados financeiros para trading algorítmico.

## Funcionalidades Atuais
- ✅ Coleta de dados de preços (OHLCV) via Yahoo Finance
- ✅ Armazenamento em SQLite com prevenção de duplicatas
- ✅ Sistema de logs estruturado
- ✅ Configurações centralizadas via arquivo .env
- ✅ Validação de dados coletados
- ✅ Rate limiting e retry automático

## Setup Rápido

```bash
# 1. Clonar/baixar o projeto
git clone <repo> ou extrair zip

# 2. Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

# 3. Instalar dependências
pip install -r requirements.txt

# 4. Configurar variáveis (opcional)
cp .env.example .env
# Editar .env conforme necessário

# 5. Executar
python main.py
```

## Estrutura do Projeto

```
trading-system/
├── config/
│   └── settings.py          # Configurações centralizadas
├── data_collectors/
│   ├── base_collector.py    # Classe abstrata base
│   └── market_data.py       # Coletor Yahoo Finance
├── storage/
│   └── database.py          # Gerenciador SQLite
├── utils/
│   └── logger.py            # Sistema de logs
├── data/                    # Banco de dados (criado automaticamente)
├── logs/                    # Arquivos de log (criado automaticamente)
├── .env                     # Configurações (criar)
├── requirements.txt         # Dependências Python
├── main.py                 # Script principal
└── README.md               # Este arquivo
```

## Configurações (.env)

```bash
DEBUG=True
LOG_LEVEL=INFO
SYMBOLS=AAPL,GOOGL,MSFT,TSLA,NVDA
DEFAULT_INTERVAL=1d
HISTORY_PERIOD=30d
```

## Uso

```bash
# Execução simples
python main.py

# Com debug
DEBUG=True python main.py

# Símbolos específicos
SYMBOLS=AAPL,TSLA python main.py
```

## Próximas Fases

- Fase 2: Múltiplos intervalos e PostgreSQL
- Fase 3: Coleta de notícias com análise de sentimento  
- Fase 4: Dados fundamentalistas
- Fase 5: Order book em tempo real
- Fase 6: Features para machine learning

## Monitoramento

Os logs ficam em:
- Console: Output colorido em tempo real
- Arquivo: `logs/trading_system_YYYY-MM-DD.log`

## Troubleshooting

**Erro de conexão**: Verifique internet e proxy
**Símbolo não encontrado**: Verifique se existe no Yahoo Finance
**Banco corrompido**: Delete `data/trading_system.db` para recriar

## Contato

Para dúvidas ou sugestões, verifique os logs primeiro!
"""