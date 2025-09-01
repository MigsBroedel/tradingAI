import pandas as pd

def calculate_sma(data: pd.DataFrame, window: int) -> pd.Series:
    """
    Calcula a Média Móvel Simples (SMA).
    
    Args:
        data: DataFrame com os dados de mercado, deve conter uma coluna 'Close'.
        window: Período da média móvel.
        
    Returns:
        pd.Series: Série com os valores da SMA.
    """
    if 'Close' not in data.columns:
        raise ValueError("DataFrame must contain a 'Close' column for SMA calculation.")
    return data['Close'].rolling(window=window).mean()

def calculate_rsi(data: pd.DataFrame, window: int) -> pd.Series:
    """
    Calcula o Índice de Força Relativa (RSI).
    
    Args:
        data: DataFrame com os dados de mercado, deve conter uma coluna 'Close'.
        window: Período do RSI.
        
    Returns:
        pd.Series: Série com os valores do RSI.
    """
    if 'Close' not in data.columns:
        raise ValueError("DataFrame must contain a 'Close' column for RSI calculation.")

    delta = data['Close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = (-delta).where(delta < 0, 0)

    avg_gain = gain.ewm(com=window - 1, min_periods=window).mean()
    avg_loss = loss.ewm(com=window - 1, min_periods=window).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


