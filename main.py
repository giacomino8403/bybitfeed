import os, time
import pandas as pd
import ccxt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# ========= CONFIG =========
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME     = os.getenv("SHEET_NAME", "Foglio1")

# core + high/mid cap (puoi aggiungere)
SYMBOLS    = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
              "ADA/USDT", "DOGE/USDT", "MATIC/USDT", "LINK/USDT",
              "AVAX/USDT", "DOT/USDT", "ATOM/USDT", "LTC/USDT", "OP/USDT", "ARB/USDT"]
TIMEFRAMES = ["15m", "1h", "4h", "1d"]   # scalping/intraday/swing/macro
CANDLES    = 300

TZ_ITALY = ZoneInfo("Europe/Rome")

# Per abilitare altri exchange quando lanci da PC/Colab:
# es. EXCH_ENABLE="kraken,binance,bybit"
EXCH_ENABLE = os.getenv("EXCH_ENABLE", "kraken")

# ========= GOOGLE SHEETS =========
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
ws = sh.worksheet(SHEET_NAME)

def ensure_header():
    if not ws.get_all_values():
        header = [[
            "timestamp_utc","timestamp_italy","exchange",
            "symbol_requested","symbol_used","timeframe","close",
            "ema20","ema50","ema200",
            "rsi","stoch_k","stoch_d",
            "macd","macd_signal",
            "adx","atr","bb_pos","vol_spike",
            "ema_trend","bb_breakout",
            "score","signal"
        ]]
        ws.append_rows(header)

# ========= EXCHANGES =========
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

EXCHS = []
for name in [n.strip() for n in EXCH_ENABLE.split(",") if n.strip()]:
    ex = connect_exchange(name)
    if ex: EXCHS.append(ex)
if not EXCHS:
    raise RuntimeError("Nessun exchange disponibile.")

# ========= DATA & INDICATORS =========
def map_symbol_for_exchange(ex, symbol):
    """Alcuni exchange (Kraken/Bitstamp) hanno USD al posto di USDT."""
    if ex.id in ("kraken", "bitstamp"):
        base, quote = symbol.split("/")
        if quote == "USDT":
            s_usd = f"{base}/USD"
            if s_usd in ex.markets:
                return s_usd
    return symbol

def fetch_ohlcv_safe(symbol, timeframe, limit):
    last_err = None
    for ex in EXCHS:
        try:
            sym = map_symbol_for_exchange(ex, symbol)
            data = ex.fetch_ohlcv(sym, timeframe=timeframe, limit=limit)
            if data:
                return ex.id, sym, data
        except Exception as e:
            last_err = e
            print(f"[{ex.id}] errore {symbol}({timeframe}): {e}")
            time.sleep(0.5)
    raise RuntimeError(f"Falliti tutti gli exchange per {symbol} {timeframe}: {last_err}")

def to_df(data):
    df = pd.DataFrame(data, columns=["ts","open","high","low","close","volume"])
    df["datetime"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df[["datetime","open","high","low","close","volume"]]

def add_indicators(df):
    import ta
    out = df.copy()

    # Trend
    out["EMA20"]  = ta.trend.EMAIndicator(out["close"], 20).ema_indicator()
    out["EMA50"]  = ta.trend.EMAIndicator(out["close"], 50).ema_indicator()
    out["EMA200"] = ta.trend.EMAIndicator(out["close"], 200).ema_indicator()
    macd          = ta.trend.MACD(out["close"])
    out["MACD"]   = macd.macd()
    out["MACDsig"]= macd.macd_signal()
    out["ADX"]    = ta.trend.ADXIndicator(out["high"], out["low"], out["close"]).adx()

    # Momentum
    out["RSI"] = ta.momentum.RSIIndicator(out["close"], 14).rsi()
    stoch      = ta.momentum.StochasticOscillator(out["high"], out["low"], out["close"])
    out["STO_K"] = stoch.stoch()
    out["STO_D"] = stoch.stoch_signal()

    # Volatilità
    bb = ta.volatility.BollingerBands(out["close"], window=20, window_dev=2)
    out["BB_up"] = bb.bollinger_hband()
    out["BB_dn"] = bb.bollinger_lband()
    out["ATR"]   = ta.volatility.AverageTrueRange(out["high"], out["low"], out["close"]).average_true_range()

    # Posizione vs bande (0=low, 0.5=middle, 1=high)
    out["BB_pos"] = (out["close"] - out["BB_dn"]) / (out["BB_up"] - out["BB_dn"])

    # Volume spike (rolling 20)
    vol_ma = out["volume"].rolling(20).mean()
    out["VOL_spike"] = out["volume"] > (vol_ma * 2.0)

    return out

# ========= SIGNAL ENGINE =========
def compute_signal(row):
    score = 0
    notes = []

    # Trend bias
    if row["EMA20"] > row["EMA50"] > row["EMA200"]:
        score += 2; notes.append("ema_bull")
    elif row["EMA20"] < row["EMA50"] < row["EMA200"]:
        score -= 2; notes.append("ema_bear")

    # MACD
    if row["MACD"] > row["MACDsig"]: score += 1
    else:                              score -= 1

    # RSI zones
    if row["RSI"] < 30:  score += 1; notes.append("rsi_ovsold")
    if row["RSI"] > 70:  score -= 1; notes.append("rsi_ovbought")

    # ADX (forza trend)
    if row["ADX"] > 25:
        score += 1 if row["EMA20"] > row["EMA50"] else -1

    # Bollinger breakout
    bb_breakout = 0
    if row["close"] > row["BB_up"]:  score += 1; bb_breakout = 1
    if row["close"] < row["BB_dn"]:  score -= 1; bb_breakout = -1

    # Volume spike
    if bool(row["VOL_spike"]): score += 1

    # Segnale finale
    if score >= 3: signal = "BUY"
    elif score <= -3: signal = "SELL"
    else: signal = "NEUTRAL"

    ema_trend = "bull" if row["EMA20"] > row["EMA50"] > row["EMA200"] else ("bear" if row["EMA20"] < row["EMA50"] < row["EMA200"] else "mix")
    return score, signal, ema_trend, bb_breakout

# ========= RUN =========
def one_run():
    ensure_header()
    now_utc = datetime.now(timezone.utc)
    now_it  = now_utc.astimezone(TZ_ITALY)

    batch = []
    for symbol in SYMBOLS:
        for tf in TIMEFRAMES:
            ex_id, sym_used, ohlcv = fetch_ohlcv_safe(symbol, tf, CANDLES)
            df  = to_df(ohlcv)
            ind = add_indicators(df).iloc[-1]

            score, signal, ema_trend, bb_breakout = compute_signal(ind)

            batch.append([
                now_utc.isoformat(),
                now_it.strftime("%Y-%m-%d %H:%M:%S"),
                ex_id,
                symbol, sym_used, tf,
                float(ind["close"]),
                float(ind["EMA20"]), float(ind["EMA50"]), float(ind["EMA200"]),
                float(ind["RSI"]), float(ind["STO_K"]), float(ind["STO_D"]),
                float(ind["MACD"]), float(ind["MACDsig"]),
                float(ind["ADX"]), float(ind["ATR"]),
                float(ind["BB_pos"]),
                bool(ind["VOL_spike"]),
                ema_trend,
                int(bb_breakout),
                int(score), signal
            ])
            print(f"{symbol} {tf} via {ex_id}: signal={signal} score={score}")

    ws.append_rows(batch)
    print("Aggiornamento scritto su Google Sheets ✔️")

if __name__ == "__main__":
    one_run()
