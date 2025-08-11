import os, time, math
import pandas as pd
import ccxt
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# ========= CONFIG =========
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SHEET_NAME     = os.getenv("SHEET_NAME", "Foglio1")
STATUS_SHEET   = os.getenv("STATUS_SHEET", "Status")

SYMBOLS = [
    "BTC/USDT","ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT",
    "ADA/USDT","DOGE/USDT","POL/USDT","LINK/USDT",
    "AVAX/USDT","DOT/USDT","ATOM/USDT","LTC/USDT","OP/USDT","LDC/USDT","ARB/USDT"
]
TIMEFRAMES = ["15m","1h","4h","1d"]
CANDLES    = 300

# Su GitHub lascia “kraken”. Su PC/Colab puoi fare EXCH_ENABLE="kraken,binance,bybit"
EXCH_ENABLE = os.getenv("EXCH_ENABLE", "kraken")
TZ_ITALY = ZoneInfo("Europe/Rome")

# ========= GOOGLE SHEETS =========
scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
ws = sh.worksheet(SHEET_NAME)

def get_or_create_status_ws():
    try:
        return sh.worksheet(STATUS_SHEET)
    except gspread.exceptions.WorksheetNotFound:
        s = sh.add_worksheet(title=STATUS_SHEET, rows=2000, cols=3)
        s.append_row(["timestamp_utc","component","message"])
        return s

status_ws = get_or_create_status_ws()

def ensure_header():
    if not ws.get_all_values():
        ws.append_rows([[
            "timestamp_utc","timestamp_italy","exchange",
            "symbol_requested","symbol_used","timeframe","close",
            "ema20","ema50","ema200",
            "rsi","stoch_k","stoch_d",
            "macd","macd_signal",
            "adx","atr","bb_pos","vol_spike",
            "ema_trend","bb_breakout",
            "score","signal"
        ]])

def log_issue(component: str, msg: str):
    now = datetime.now(timezone.utc).isoformat()
    print(f"[WARN] {component}: {msg}")
    try:
        status_ws.append_row([now, component, msg])
    except Exception as e:
        print(f"[WARN] status append failed: {e}")

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
        log_issue("exchange", f"{name} non disponibile: {e}")
        return None

EXCHS = []
for name in [n.strip() for n in EXCH_ENABLE.split(",") if n.strip()]:
    ex = connect_exchange(name)
    if ex: EXCHS.append(ex)
if not EXCHS:
    log_issue("startup","nessun exchange disponibile")
    raise RuntimeError("Nessun exchange disponibile.")

# ========= DATA & INDICATORS =========
def map_symbol_for_exchange(ex, symbol):
    # Kraken/Bitstamp spesso hanno USD al posto di USDT
    if ex.id in ("kraken","bitstamp"):
        base, quote = symbol.split("/")
        if quote == "USDT":
            s_usd = f"{base}/USD"
            if s_usd in ex.markets:
                return s_usd
    return symbol

def fetch_ohlcv_safe(symbol, timeframe, limit):
    """Ritorna (ex_id, sym_used, data) oppure (None, None, None) e logga."""
    for ex in EXCHS:
        sym = map_symbol_for_exchange(ex, symbol)
        if sym not in ex.markets:
            log_issue(ex.id, f"symbol non presente: {sym} (richiesto {symbol})")
            continue
        try:
            data = ex.fetch_ohlcv(sym, timeframe=timeframe, limit=limit)
            if data:
                return ex.id, sym, data
            else:
                log_issue(ex.id, f"nessun dato OHLCV per {sym} {timeframe}")
        except Exception as e:
            log_issue(ex.id, f"errore fetch {sym} {timeframe}: {e}")
            time.sleep(0.3)
    log_issue("fetch", f"falliti tutti gli exchange per {symbol} {timeframe}")
    return None, None, None

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

    # Posizione vs bande (0..1) con protezione divisione per 0
    den = (out["BB_up"] - out["BB_dn"])
    den = den.replace(0, pd.NA)
    out["BB_pos"] = (out["close"] - out["BB_dn"]) / den

    # Volume spike (rolling 20)
    vol_ma = out["volume"].rolling(20).mean()
    out["VOL_spike"] = out["volume"] > (vol_ma * 2.0)

    return out

# ========= SANITIZATION =========
def clean_val(v):
    # Converte NaN/Inf in None e arrotonda i float
    if isinstance(v, float):
        if math.isfinite(v):
            return round(v, 6)
        return None
    if isinstance(v, (int, str, bool)):
        return v
    if pd.isna(v):
        return None
    return None

# ========= SIGNAL ENGINE (robusto) =========
def compute_signal(row):
    # valori puliti/neutralizzati
    ema20 = clean_val(row.get("EMA20"))
    ema50 = clean_val(row.get("EMA50"))
    ema200 = clean_val(row.get("EMA200"))
    macd = clean_val(row.get("MACD"))
    macd_signal = clean_val(row.get("MACDsig"))
    rsi = clean_val(row.get("RSI"))
    adx = clean_val(row.get("ADX"))
    bb_up = clean_val(row.get("BB_up"))
    bb_dn = clean_val(row.get("BB_dn"))
    close = clean_val(row.get("close"))
    vol_spike = bool(row.get("VOL_spike")) if not pd.isna(row.get("VOL_spike")) else False

    score = 0

    # Trend bias
    if None not in (ema20, ema50, ema200):
        if ema20 > ema50 > ema200:
            score += 2
        elif ema20 < ema50 < ema200:
            score -= 2

    # MACD
    if None not in (macd, macd_signal):
        score += 1 if macd > macd_signal else -1

    # RSI zones
    if rsi is not None:
        if rsi < 30: score += 1
        if rsi > 70: score -= 1

    # ADX
    if adx is not None and ema20 is not None and ema50 is not None:
        if adx > 25:
            score += 1 if ema20 > ema50 else -1

    # Bollinger breakout
    bb_breakout = 0
    if None not in (bb_up, bb_dn, close):
        if close > bb_up:
            score += 1
            bb_breakout = 1
        if close < bb_dn:
            score -= 1
            bb_breakout = -1

    # Volume spike
    if vol_spike:
        score += 1

    # Segnale finale
    if score >= 3: signal = "BUY"
    elif score <= -3: signal = "SELL"
    else: signal = "NEUTRAL"

    ema_trend = None
    if None not in (ema20, ema50, ema200):
        ema_trend = "bull" if ema20 > ema50 > ema200 else \
                    ("bear" if ema20 < ema50 < ema200 else "mix")

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
            if ohlcv is None:
                continue  # salta ma continua

            df  = to_df(ohlcv)
            ind = add_indicators(df).iloc[-1]
            ind = ind.fillna(value=pd.NA)

            score, signal, ema_trend, bb_breakout = compute_signal(ind)

            row = [
                now_utc.isoformat(),
                now_it.strftime("%Y-%m-%d %H:%M:%S"),
                ex_id, symbol, sym_used, tf,
                ind["close"], ind["EMA20"], ind["EMA50"], ind["EMA200"],
                ind["RSI"], ind["STO_K"], ind["STO_D"],
                ind["MACD"], ind["MACDsig"],
                ind["ADX"], ind["ATR"], ind["BB_pos"], ind["VOL_spike"],
                ema_trend, bb_breakout, score, signal
            ]
            batch.append([clean_val(v) for v in row])
            print(f"{symbol} {tf} via {ex_id}: signal={signal} score={score}")

    if batch:
        ws.append_rows(batch, value_input_option="RAW")
        print(f"Aggiornamento scritto su Google Sheets ✔️ ({len(batch)} righe)")
    else:
        log_issue("writer","nessuna riga scritta (tutte le combinazioni fallite)")

if __name__ == "__main__":
    one_run()
