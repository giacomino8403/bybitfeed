import os, time, json
import pandas as pd
import ccxt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta

# -------- Config da ENV --------
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", "Foglio1")  # cambia se serve
TZ = timezone(timedelta(hours=2))  # Europa/Roma (CEST). In inverno metti +1.

SYMBOLS = ["ETH/USDT", "XRP/USDT"]
TIMEFRAMES = ["15m", "1h"]
CANDLES = 200

# -------- Google Sheets auth (creds.json da secret) --------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
# Il workflow crea creds.json prima di eseguire
creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
ws = sh.worksheet(SHEET_NAME)

def connect(exchange_id):
    ex = getattr(ccxt, exchange_id)(
        {"enableRateLimit": True, "options": {"defaultType": "spot"}}
    )
    ex.load_markets()
    return ex

bybit = connect("bybit")
binance = connect("binance")
EXCHS = [bybit, binance]

def fetch_ohlcv_safe(symbol, timeframe, limit):
    last_err = None
    for ex in EXCHS:
        try:
            data = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if data:
                return ex.id, data
        except Exception as e:
            last_err = e
            print(f"[{ex.id}] fallback {symbol} {timeframe}: {e}")
            time.sleep(0.6)
    raise RuntimeError(f"Nessun exchange disponibile per {symbol} {timeframe}. Ultimo errore: {last_err}")

def to_df(data):
    df = pd.DataFrame(data, columns=["ts","open","high","low","close","volume"])
    df["datetime"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df[["datetime","open","high","low","close","volume"]]

def add_indicators(df):
    import ta
    out = df.copy()
    out["EMA20"]  = ta.trend.EMAIndicator(out["close"], 20).ema_indicator()
    out["EMA50"]  = ta.trend.EMAIndicator(out["close"], 50).ema_indicator()
    out["EMA200"] = ta.trend.EMAIndicator(out["close"], 200).ema_indicator()
    out["RSI"]    = ta.momentum.RSIIndicator(out["close"], 14).rsi()
    macd = ta.trend.MACD(out["close"])
    out["MACD"]        = macd.macd()
    out["MACD_signal"] = macd.macd_signal()
    return out

def ensure_header():
    vals = ws.get_all_values()
    if len(vals) == 0:
        header = [["timestamp_utc","timestamp_it","exchange","symbol","timeframe",
                   "close","ema20","ema50","ema200","rsi","macd","macd_signal"]]
        ws.append_rows(header)

def one_run():
    rows = []
    now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
    now_it  = now_utc.astimezone(TZ)
    for symbol in SYMBOLS:
        for tf in TIMEFRAMES:
            ex_id, ohlcv = fetch_ohlcv_safe(symbol, tf, CANDLES)
            df = add_indicators(to_df(ohlcv))
            last = df.iloc[-1]
            rows.append([
                now_utc.isoformat(),
                now_it.strftime("%Y-%m-%d %H:%M:%S"),
                ex_id, symbol, tf,
                float(last["close"]),
                float(last["EMA20"]),
                float(last["EMA50"]),
                float(last["EMA200"]),
                float(last["RSI"]),
                float(last["MACD"]),
                float(last["MACD_signal"]),
            ])
            print(f"OK {symbol} {tf} via {ex_id}: close={last['close']:.6f} RSI={last['RSI']:.2f}")
    ensure_header()
    ws.append_rows(rows)
    print("Dati aggiunti su Google Sheets ✔️")

if __name__ == "__main__":
    one_run()
