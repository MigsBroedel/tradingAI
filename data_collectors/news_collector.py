# data_collectors/news_collector.py
import requests
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict
from config.settings import settings
from analysis.sentiment_analyzer import analyze_sentiment
from storage.database_postgres import DatabaseManager
from utils.logger import app_logger
from urllib.parse import urljoin, urlencode
from sqlalchemy import text

class NewsCollector:
    def __init__(self):
        self.db = DatabaseManager()
        self.session = requests.Session()
        self.session.headers.update({"X-api-Key": settings.NEWSAPI_KEY})

    def _build_query(self) -> str:
        keywords = []

        for symbol in settings.SYMBOLS:
            if symbol.endswith(".SA"):
                nome = {
                    "PETR4.SA": "Petrobras",
                    "VALE3.SA": "Vale",
                    "ITUB4.SA": "Itaú",
                    "BBDC4.SA": "Bradesco",
                    "ABEV3.SA": "Ambev"
                }.get(symbol, symbol.split('.')[0])
                keywords.extend([nome, nome.replace(" ", "")])
            else:
                # Para US
                nome = {
                    "AAPL": "Apple",
                    "GOOGL": "Google",
                    "MSFT": "Microsoft",
                    "TSLA": "Tesla"
                }.get(symbol, symbol)
                keywords.append(nome)

        # Adiciona termos gerais do setor
        sector_terms = ["bolsa", "ação", "ações", "investimento", "Ibovespa"]  # opcional
        all_terms = keywords + sector_terms
        return " OR ".join([f'"{term}"' for term in all_terms])

    def _is_relevant(self, title: str, content: str) -> bool:
        """Verifica se notícia é relevante"""
        text = (title + " " + (content or "")).lower()
        return any(s.split('.')[0].lower() in text for s in settings.SYMBOLS)

    def _extract_symbols(self, text: str) -> List[str]:
        """Extrai símbolos mencionados no texto"""
        found = []
        text_lower = text.lower()
        for symbol in settings.SYMBOLS:
            ticker = symbol.split('.')[0].lower()
            if ticker in text_lower:
                found.append(symbol)
        return list(set(found))

    def fetch_news(self) -> List[Dict]:
        if not settings.NEWSAPI_KEY:
            app_logger.warning("NewsAPI key não configurada.")
            return []

        # ✅ URL limpa
        url = "https://newsapi.org/v2/everything"  # Sem espaços!

        from_time = datetime.now(timezone.utc) - timedelta(hours=settings.NEWS_PERIOD_HOURS)

        params = {
            "q": "Petrobras OR investimento OR bolsa",  # ou use self._build_query()
            "sortBy": "publishedAt",
            "language": settings.NEWS_LANGUAGE,
            "pageSize": 100,
            "apiKey": settings.NEWSAPI_KEY  
        }

        # ✅ Log da URL completa
        full_url = f"{url}?{urlencode(params)}"
        app_logger.info(f"🔍 URL da API: {full_url}")

        try:
            response = self.session.get(url, params=params, timeout=10)
            app_logger.info(f"📡 Status da API: {response.status_code}")

            if response.status_code != 200:
                app_logger.error(f"❌ Erro da API: {response.status_code} - {response.text}")
                return []

            data = response.json()
            articles = data.get("articles", [])
            total = data.get("totalResults", 0)

            app_logger.info(f"📥 {len(articles)} artigos retornados (total disponível: {total})")
            return articles

        except Exception as e:
            app_logger.error(f"🚨 Erro ao buscar notícias: {e}")
            return []

    def process_and_store(self):
        articles = self.fetch_news()
        inserted = 0

        for item in articles:
            title = item.get("title") or ""
            content = item.get("content") or item.get("description") or ""
            source_data = item.get("source") or {}
            source_name = source_data.get("name", "Unknown")
            url = item.get("url") or ""
            published_at = item.get("publishedAt", "").replace("Z", "+00:00")

            if not title:  # Pula se não tiver título
                continue

            if not self._is_relevant(title, content):
                continue

            symbols = self._extract_symbols(title + " " + content)
            sentiment_label, sentiment_score = analyze_sentiment(content)

            try:
                self.db.insert_news(
                    title=title,
                    content=content,
                    url=url,
                    source=source_name,
                    published_at=published_at,
                    sentiment_label=sentiment_label,
                    sentiment_score=sentiment_score,
                    symbols=symbols  # lista de strings → PostgreSQL TEXT[]
                )
                inserted += 1
            except Exception as e:
                app_logger.error(f"Erro ao salvar notícia: {e}")

        app_logger.info(f"{inserted} notícias relevantes inseridas no banco.")

    
