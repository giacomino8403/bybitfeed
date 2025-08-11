import os
import time
import pandas as pd
import ccxt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME     = os.getenv("SHEET_NAME", "Foglio1")

SYMBOLS    = ["ETH/USDT", "XRP/USDT"]
TIMEFRAMES = ["15m", "1h"]
CANDLES    = 200

TZ_ITALY = ZoneInfo("Europe/Rome")

# ---- Google Sheets auth ----
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
ws = sh.worksheet(SHEET_NAME)

def connect_exchange(name: str):
    try:
        ex = getattr(ccxt, name)({
            "enableRateLimit": True,
            "options": {"defaultType": "spot"}
        })
        ex.load_markets()
        print(f"{name} pronto.")
        return ex
    except Exception as e:
        print(f"{name} non disponibile: {e}")
        return None

# Ordine di fallback: bybit -> binance -> kraken -> bitstamp
EXCHS = []
for name in ["bybit", "binance", "kraken", "bitstamp"]:
    ex = connect_exchange(name)
    if ex: EXCHS.append(ex)

if not EXCHS:
    raise RuntimeError("Nessun exchange disponibile (bybit/binance/kraken/bitstamp).")

def fetch_ohlcv_safe(symbol, timeframe, limit):
    last_err = None
    for ex in EXCHS:
        try:
            # Alcuni exchange non hanno USDT ma USD: mappa se serve
            sym = symbol
            if ex.id in ("kraken", "bitstamp"):
                # Kraken/Bitstamp spesso usano USD anziché USDT
                base, quote = symbol.split("/")
                if quote == "USDT":
                    sym = f"{base}/USD" if f"{base}/USD" in ex.markets else symbol
            data = ex.fetch_ohlcv(sym, timeframe=timeframe, limit=limit)
            if data: 
                return ex.id, data, sym
        except Exception as e:
            last_err = e
            print(f"[{ex.id}] errore su {symbol}({sym}) {timeframe}, provo il prossimo: {e}")
            time.sleep(0.6)
    raise RuntimeError(f"Nessun exchange ha risposto per {symbol} {timeframe}. Ultimo errore: {last_err}")

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
    if len(ws.get_all_values()) == 0:
        header = [[
            "timestamp_utc","timestamp_italy","exchange","symbol_used","symbol_requested","timeframe",
            "close","ema20","ema50","ema200","rsi","macd","macd_signal"
        ]]
        ws.append_rows(header)

def one_run():
    rows = []
    now_utc = datetime.now(timezone.utc)
    now_it  = now_utc.astimezone(TZ_ITALY)

    for symbol in SYMBOLS:
        for tf in TIMEFRAMES:
            ex_id, ohlcv, sym_used = fetch_ohlcv_safe(symbol, tf, CANDLES)
            df  = add_indicators(to_df(ohlcv))
            last = df.iloc[-1]
            rows.append([
                now_utc.isoformat(),
                now_it.strftime("%Y-%m-%d %H:%M:%S"),
                ex_id, sym_used, symbol, tf,
                float(last["close"]),
                float(last["EMA20"]),
                float(last["EMA50"]),
                float(last["EMA200"]),
                float(last["RSI"]),
                float(last["MACD"]),
                float(last["MACD_signal"]),
            ])
            print(f"OK {symbol} ({sym_used}) {tf} via {ex_id}: close={last['close']:.6f} RSI={last['RSI']:.2f}")

    ensure_header()
    ws.append_rows(rows)
    print("Dati aggiunti su Google Sheets ✔️")

if __name__ == "__main__":
    one_run()
