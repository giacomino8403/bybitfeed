from pybit.unified_trading import HTTP
import pandas as pd
import ta
import os

# --- Config ---
API_KEY = os.getenv("APIKEY")
API_SECRET = os.getenv("APISECRET")
SYMBOLS = ["ETHUSDT", "XRPUSDT"]
TIMEFRAMES = ["15", "60"]  # minuti
CANDLES = 200

# --- Connessione ---
session = HTTP(
    api_key=API_KEY,
    api_secret=API_SECRET,
    testnet=False
)

def get_klines(symbol, interval):
    data = session.get_kline(
        category="linear",
        symbol=symbol,
        interval=interval,
        limit=CANDLES
    )["result"]["list"]
    df = pd.DataFrame(data, columns=[
        "timestamp", "open", "high", "low", "close", "volume", "_", "_", "_", "_", "_", "_"
    ])
    df["close"] = df["close"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df = df.iloc[::-1].reset_index(drop=True)
    return df

def calc_indicators(df):
    df["EMA20"] = ta.trend.EMAIndicator(df["close"], 20).ema_indicator()
    df["EMA50"] = ta.trend.EMAIndicator(df["close"], 50).ema_indicator()
    df["EMA200"] = ta.trend.EMAIndicator(df["close"], 200).ema_indicator()
    df["RSI"] = ta.momentum.RSIIndicator(df["close"], 14).rsi()
    macd = ta.trend.MACD(df["close"])
    df["MACD"] = macd.macd()
    df["MACD_signal"] = macd.macd_signal()
    df["OBV"] = ta.volume.OnBalanceVolumeIndicator(df["close"], df["volume"]).on_balance_volume()
    df["ATR"] = ta.volatility.AverageTrueRange(
        high=df["high"].astype(float),
        low=df["low"].astype(float),
        close=df["close"].astype(float),
        window=14
    ).average_true_range()
    return df

def main():
    for symbol in SYMBOLS:
        for tf in TIMEFRAMES:
            df = get_klines(symbol, tf)
            df = calc_indicators(df)
            latest = df.iloc[-1]
            print(f"{symbol} {tf}m:", latest.to_dict())
            # TODO: integrare logica alert TP/SL/divergenze

if __name__ == "__main__":
    main()
