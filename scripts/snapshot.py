# scripts/snapshot.py (robusto agli header)
import os, json, math
from datetime import datetime, timezone
import pandas as pd

CSV_URL = os.environ.get("CSV_URL")
OUT_DIR = "docs"
OUT_SNAPSHOT = os.path.join(OUT_DIR, "snapshot.json")
OUT_CHANGES  = os.path.join(OUT_DIR, "changes.json")
NOJEKYLL     = os.path.join(OUT_DIR, ".nojekyll")

ATR_MUL = {"15m": (1.0,1.0,2.0), "1h": (1.2,1.2,2.4), "4h": (1.5,1.5,3.0), "1d": (2.0,2.0,4.0)}

# possibili alias per colonne
CAND_TS = ["timestamp_utc","timestamp","time_utc","time","datetime","date"]
CAND_TF = ["timeframe","tf","interval","granularity"]
RENAME_BASE = {
  "exchange":"exchange","symbol":"symbol_requested","symbol_requested":"symbol_requested",
  "symbol_used":"symbol_used","close":"close","ema20":"ema20","ema50":"ema50","ema200":"ema200",
  "rsi":"rsi","stoch_k":"stoch_k","stoch_d":"stoch_d","macd":"macd","macd_signal":"macd_signal",
  "adx":"adx","atr":"atr","bb_pos":"bb_pos","vol_spike":"vol_spike","ema_trend":"ema_trend",
  "bb_breakout":"bb_breakout","score":"score","signal":"signal"
}
NUM_COLS = ["close","ema20","ema50","ema200","rsi","stoch_k","stoch_d","macd","macd_signal","adx","atr","bb_pos","bb_breakout","score"]

def load_prev(path):
    try:
        with open(path,"r") as f: return json.load(f)
    except Exception:
        return {}

def clean_float(x, nd=6):
    try:
        v=float(x)
        if math.isfinite(v): return round(v, nd)
    except Exception: pass
    return None

def find_col(cols, candidates):
    s = {c.lower().strip(): c for c in cols}
    for cand in candidates:
        if cand in s: return s[cand]
    # prova anche senza underscore/spazi
    s2 = {"".join(k.replace(" ","").split("_")).lower(): k for k in cols}
    for cand in candidates:
        k = "".join(cand.split("_")).lower()
        if k in s2: return s2[k]
    return None

def compute_extras(item):
    close  = clean_float(item.get("close"))
    ema200 = clean_float(item.get("ema200"))
    atr    = clean_float(item.get("atr"))
    tf     = str(item.get("timeframe") or "")
    if close is not None and ema200 not in (None,0):
        item["pct_from_ema200"] = round((close/ema200 - 1)*100, 3)
    else:
        item["pct_from_ema200"] = None
    sl=tp1=tp2=None
    k_sl,k_tp1,k_tp2 = ATR_MUL.get(tf,(1.0,1.0,2.0))
    if close is not None and atr is not None:
        sl  = close - k_sl*atr
        tp1 = close + k_tp1*atr
        tp2 = close + k_tp2*atr
    item["sl"]=clean_float(sl); item["tp1"]=clean_float(tp1); item["tp2"]=clean_float(tp2)
    return item

def main():
    if not CSV_URL: raise SystemExit("Missing CSV_URL")

    df = pd.read_csv(CSV_URL)
    # normalizza header base
    ren = {}
    for col in df.columns:
        k = col.strip()
        lk = k.lower()
        if lk in RENAME_BASE: ren[col] = RENAME_BASE[lk]
        else: ren[col] = k  # lascia com'è
    df = df.rename(columns=ren)

    # autodetect timestamp & timeframe
    cols_lower = [c.lower().strip() for c in df.columns]
    ts_col = find_col(cols_lower, CAND_TS)
    tf_col = find_col(cols_lower, CAND_TF)
    if not ts_col:
        raise SystemExit(f"Timestamp column not found. Tried: {CAND_TS}. Headers: {list(df.columns)}")
    if not tf_col:
        raise SystemExit(f"Timeframe column not found. Tried: {CAND_TF}. Headers: {list(df.columns)}")

    # parse tipi
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    for c in NUM_COLS:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    if "vol_spike" in df.columns:
        df["vol_spike"] = df["vol_spike"].astype(str).str.upper().isin(["TRUE","1","T","Y","YES"])

    # pulizia
    df = df.dropna(subset=[ts_col, tf_col]).sort_values(ts_col)

    # usa symbol_used se c'è, altrimenti requested/symbol
    sym_used_col = "symbol_used" if "symbol_used" in df.columns else ("symbol_requested" if "symbol_requested" in df.columns else "symbol")
    df["symbol_key"] = df[sym_used_col]

    # ultima riga per (symbol_key, timeframe)
    last = df.groupby(["symbol_key", tf_col], as_index=False).tail(1).copy()

    snapshot = {"_meta":{"generated_at_utc": datetime.now(timezone.utc).isoformat(),
                         "source":"google-sheets-csv","rows": int(len(last))},
                "items":{}}

    for _, r in last.iterrows():
        row = r.to_dict()
        item = {
            "timestamp_utc": row.get(ts_col).isoformat() if pd.notna(row.get(ts_col)) else None,
            "exchange": row.get("exchange"),
            "symbol_requested": row.get("symbol_requested") or row.get("symbol"),
            "symbol_used": row.get("symbol_key"),
            "timeframe": row.get(tf_col),
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
            "vol_spike": bool(row.get("vol_spike")) if "vol_spike" in row else False,
            "ema_trend": row.get("ema_trend"),
            "bb_breakout": int(row.get("bb_breakout")) if pd.notna(row.get("bb_breakout")) else 0,
            "score": int(row.get("score")) if pd.notna(row.get("score")) else None,
            "signal": row.get("signal"),
        }
        item = compute_extras(item)
        key = f"{item['symbol_used']}|{item['timeframe']}"
        snapshot["items"][key] = item

    # changes vs precedente
    prev = load_prev(OUT_SNAPSHOT)
    prev_items = prev.get("items", {}) if isinstance(prev, dict) else {}
    changes = []
    for k, cur in snapshot["items"].items():
        old = prev_items.get(k)
        if not old:
            changes.append({"key":k,"type":"new","to":{"signal":cur.get("signal"),"score":cur.get("score")}})
        elif cur.get("signal") != old.get("signal") or cur.get("score") != old.get("score"):
            changes.append({"key":k,"type":"update",
                            "from":{"signal":old.get("signal"),"score":old.get("score")},
                            "to":{"signal":cur.get("signal"),"score":cur.get("score")}})

    # salva + .nojekyll
    os.makedirs(OUT_DIR, exist_ok=True)
    open(NOJEKYLL,"a").close()
    with open(OUT_SNAPSHOT,"w",encoding="utf-8") as f: json.dump(snapshot,f,ensure_ascii=False,indent=2)
    with open(OUT_CHANGES,"w",encoding="utf-8") as f: json.dump({"_meta":{"generated_at_utc":snapshot["_meta"]["generated_at_utc"],"count":len(changes)},"changes":changes},f,ensure_ascii=False,indent=2)

    print(f"[snapshot] items: {len(snapshot['items'])}")
    print(f"[changes] count: {len(changes)}")
    for c in changes[:12]:
        print(f"- {c['key']}: {c['type']} {c.get('from')} -> {c.get('to')}")

if __name__ == "__main__":
    main()
