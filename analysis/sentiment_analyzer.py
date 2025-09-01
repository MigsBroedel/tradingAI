# analysis/sentiment_analyzer.py
from textblob import TextBlob

def analyze_sentiment(text: str):
    """
    Analisa o sentimento de um texto.
    Retorna: (label: str, score: float)
    """
    if not text or not text.strip():
        return "neutral", 0.0

    blob = TextBlob(text)
    polarity = blob.sentiment.polarity  # -1 a 1
    if polarity > 0.1:
        label = "positive"
    elif polarity < -0.1:
        label = "negative"
    else:
        label = "neutral"

    return label, round(polarity, 3)