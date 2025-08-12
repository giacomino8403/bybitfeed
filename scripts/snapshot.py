# scripts/snapshot.py
import os, json, math
from datetime import datetime, timezone
import pandas as pd

CSV_URL = os.environ.get("CSV_URL")  # Secret con link pub?output=csv
OUT_DIR = "docs"
OUT_SNAPSHOT = os.path.join(OUT_DIR, "snapshot.json")
OUT_CHANGES  = os.path.join(OUT_DIR, "changes.json")
NOJEKYLL     = os.path.join(OUT_DIR, ".nojekyll")

# moltiplicatori ATR per TP/SL (scalping/intraday/swing/macro)
ATR_MUL = {
    "15m": (1.0, 1.0, 2.0),   # (SL, TP1, TP2)
    "1h" : (1.2, 1.2, 2.4),
    "4h" : (1.5, 1.5, 3.0),
    "1d" : (2.0, 2.0, 4.0),
}

NUM_COLS = [
    "close","ema20","ema50","ema200","rsi","stoch_k","stoch_d",
    "macd","macd_signal","adx","atr","bb_pos","bb_breakout","score"
]

RENAME = {  # mappa nomi colonne dal CSV al modello standard
    "timestamp_utc":"timestamp_utc",
    "exchange":"exchange",
    "symbol":"symbol_requested",
    "symbol_requested":"symbol_requested",
    "symbol_used":"symbol_used",
    "timeframe":"timeframe",
    "close":"close",
    "ema20":"ema20",
    "ema50":"ema50",
    "ema200":"ema200",
    "rsi":"rsi",
    "stoch_k":"stoch_k",
    "stoch_d":"stoch_d",
    "macd":"macd",
    "macd_signal":"macd_signal",
    "adx":"adx",
    "atr":"atr",
    "bb_pos":"bb_pos",
    "vol_spike":"vol_spike",
    "ema_trend":"ema_trend",
    "bb_breakout":"bb_breakout",
    "score":"score",
    "signal":"signal",
}

def load_prev(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def clean_float(x, ndigits=6):
    try:
        v = float(x)
        if math.isfinite(v):
            return round(v, ndigits)
    except Exception:
        pass
    return None

def compute_extras(item: dict):
    close  = clean_float(item.get("close"))
    ema200 = clean_float(item.get("ema200"))
    atr    = clean_float(item.get("atr"))
    tf     = str(item.get("timeframe") or "")
    sl = tp1 = tp2 = None

    # distanza % da EMA200
    if close is not None and ema200 not in (None, 0):
        item["pct_from_ema200"] = round((close/ema200 - 1)*100, 3)
    else:
        item["pct_from_ema200"] = None

    # TP/SL da ATR
    (k_sl, k_tp1, k_tp2) = ATR_MUL.get(tf, (1.0, 1.0, 2.0))
    if close is not None and atr is not None:
        sl  = close - k_sl  * atr
        tp1 = close + k_tp1 * atr
        tp2 = close + k_tp2 * atr

    item["sl"]  = clean_float(sl)
    item["tp1"] = clean_float(tp1)
    item["tp2"] = clean_float(tp2)
    return item

def main():
    if not CSV_URL:
        raise SystemExit("Missing CSV_URL env var")

    # 1) Leggi CSV pubblico
    df = pd.read_csv(CSV_URL)
    df = df.rename(columns=RENAME)

    # 2) Normalizza tipi
    df["timestamp_utc"] = pd.to_datetime(df.get("timestamp_utc"), errors="coerce", utc=True)
    for c in NUM_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "vol_spike" in df.columns:
        # normalizza booleani
        df["vol_spike"] = df["vol_spike"].astype(str).str.upper().isin(["TRUE","1","T","Y","YES"])

    # 3) Pulisci e ordina
    df = df.dropna(subset=["timestamp_utc","timeframe"])
    # preferisci symbol_used se presente, altrimenti requested
    if "symbol_used" in df.columns and df["symbol_used"].notna().any():
        df["symbol_key"] = df["symbol_used"].fillna(df.get("symbol_requested"))
    else:
        df["symbol_key"] = df.get("symbol_requested", df.get("symbol", ""))
    df = df.sort_values("timestamp_utc")

    # 4) Ultima riga per (symbol_key, timeframe)
    last = df.groupby(["symbol_key","timeframe"], as_index=False).tail(1).copy()

    # 5) Costruisci snapshot
    snapshot = {
        "_meta": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "google-sheets-csv",
            "rows": int(len(last))
        },
        "items": {}
    }

    for _, r in last.iterrows():
        row = r.to_dict()
        item = {
            "timestamp_utc": row.get("timestamp_utc").isoformat() if pd.notna(row.get("timestamp_utc")) else None,
            "exchange": row.get("exchange"),
            "symbol_requested": row.get("symbol_requested") or row.get("symbol"),
            "symbol_used": row.get("symbol_key"),
            "timeframe": row.get("timeframe"),
            "close": clean_float(row.get("close")),
            "ema20": clean_float(row.get("ema20")),
            "ema50": clean_float(row.get("ema50")),
            "ema200": clean_float(row.get("ema200")),
            "rsi": clean_float(row.get("rsi")),
            "stoch_k": clean_float(row.get("stoch_k")),
            "stoch_d": clean_float(row.get("stoch_d")),
            "macd": clean_float(row.get("macd")),
            "macd_signal": clean_float(row.get("macd_signal")),
            "adx": clean_float(row.get("adx")),
            "atr": clean_float(row.get("atr")),
            "bb_pos": clean_float(row.get("bb_pos")),
            "vol_spike": bool(row.get("vol_spike")),
            "ema_trend": row.get("ema_trend"),
            "bb_breakout": int(row.get("bb_breakout")) if pd.notna(row.get("bb_breakout")) else 0,
            "score": int(row.get("score")) if pd.notna(row.get("score")) else None,
            "signal": row.get("signal"),
        }
        item = compute_extras(item)
        key = f"{item['symbol_used']}|{item['timeframe']}"
        snapshot["items"][key] = item

    # 6) Changes vs snapshot precedente
    prev = load_prev(OUT_SNAPSHOT)
    prev_items = prev.get("items", {}) if isinstance(prev, dict) else {}
    changes = []
    for k, cur in snapshot["items"].items():
        old = prev_items.get(k)
        if not old:
            changes.append({"key":k, "type":"new", "to":{"signal":cur.get("signal"), "score":cur.get("score")}})
        else:
            if cur.get("signal") != old.get("signal") or cur.get("score") != old.get("score"):
                changes.append({
                    "key":k, "type":"update",
                    "from":{"signal": old.get("signal"), "score": old.get("score")},
                    "to":{"signal": cur.get("signal"), "score": cur.get("score")}
                })

    changes_doc = {
        "_meta": {
            "generated_at_utc": snapshot["_meta"]["generated_at_utc"],
            "count": len(changes)
        },
        "changes": changes
    }

    # 7) Salva file + .nojekyll
    os.makedirs(OUT_DIR, exist_ok=True)
    try:
        open(NOJEKYLL, "a").close()  # disattiva Jekyll
    except Exception:
        pass

    with open(OUT_SNAPSHOT, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    with open(OUT_CHANGES, "w", encoding="utf-8") as f:
        json.dump(changes_doc, f, ensure_ascii=False, indent=2)

    # 8) Log riassunto
    print(f"[snapshot] items: {len(snapshot['items'])}")
    print(f"[changes] count: {len(changes)}")
    for c in changes[:12]:
        print(f"- {c['key']}: {c['type']} {c.get('from')} -> {c.get('to')}")

if __name__ == "__main__":
    main()
