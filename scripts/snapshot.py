# scripts/snapshot.py
import os, json, sys, time
from datetime import datetime, timezone
import pandas as pd

CSV_URL = os.environ["CSV_URL"]  # metti il link pub?output=csv nei Secrets
OUT_SNAPSHOT = "docs/snapshot.json"
OUT_CHANGES  = "docs/changes.json"

def load_prev(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def normalize_key(row):
    # chiave univoca per coppia+tf (uso symbol_used se presente, altrimenti requested)
    sym_used = row.get("symbol_used") or row.get("symbol") or row.get("symbol_requested")
    tf = row.get("timeframe")
    return f"{sym_used}|{tf}"

def compute_extras(row):
    out = dict(row)
    try:
        close = float(row["close"])
        ema200 = float(row.get("ema200", "nan"))
        if pd.notna(ema200) and ema200 != 0:
            out["pct_from_ema200"] = round((close/ema200 - 1)*100, 3)
    except Exception:
        out["pct_from_ema200"] = None
    return out

def main():
    # 1) leggi CSV pubblico
    df = pd.read_csv(CSV_URL)

    # mappa intestazioni comuni (adatta se servisse)
    df = df.rename(columns={
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
    })

    # 2) parse timestamp e ordina
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], errors="coerce", utc=True)
    df = df.dropna(subset=["timestamp_utc","timeframe"])
    df = df.sort_values("timestamp_utc")

    # 3) prendi l'ultima riga per (symbol_used or requested, timeframe)
    #    creo una colonna symbol_key preferendo symbol_used se presente
    df["symbol_key"] = df["symbol_used"].fillna(df["symbol_requested"])
    last = df.groupby(["symbol_key","timeframe"], as_index=False).tail(1).copy()

    # 4) costruisci snapshot dict
    snapshot = {
        "_meta": {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source": "google-sheets-csv",
            "rows": len(last)
        },
        "items": {}
    }

    for _, r in last.iterrows():
        row = {k: (None if pd.isna(v) else v) for k,v in r.to_dict().items()}
        # allinea nomi chiave standard
        item = {
            "timestamp_utc": row.get("timestamp_utc"),
            "exchange": row.get("exchange"),
            "symbol_requested": row.get("symbol_requested"),
            "symbol_used": row.get("symbol_key"),
            "timeframe": row.get("timeframe"),
            "close": row.get("close"),
            "ema20": row.get("ema20"),
            "ema50": row.get("ema50"),
            "ema200": row.get("ema200"),
            "rsi": row.get("rsi"),
            "stoch_k": row.get("stoch_k"),
            "stoch_d": row.get("stoch_d"),
            "macd": row.get("macd"),
            "macd_signal": row.get("macd_signal"),
            "adx": row.get("adx"),
            "atr": row.get("atr"),
            "bb_pos": row.get("bb_pos"),
            "vol_spike": row.get("vol_spike"),
            "ema_trend": row.get("ema_trend"),
            "bb_breakout": row.get("bb_breakout"),
            "score": row.get("score"),
            "signal": row.get("signal"),
        }
        item = compute_extras(item)
        key = normalize_key(item)
        snapshot["items"][key] = item

    # 5) changes vs precedente
    prev = load_prev(OUT_SNAPSHOT)
    prev_items = prev.get("items", {}) if isinstance(prev, dict) else {}
    changes = []
    for k, cur in snapshot["items"].items():
        old = prev_items.get(k)
        if not old:
            changes.append({"key":k, "type":"new", "from":None, "to":cur.get("signal")})
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

    # 6) salva file
    os.makedirs("docs", exist_ok=True)
    with open(OUT_SNAPSHOT, "w") as f: json.dump(snapshot, f, ensure_ascii=False, indent=2)
    with open(OUT_CHANGES, "w") as f:  json.dump(changes_doc, f, ensure_ascii=False, indent=2)

    # stampa un riepilogo per i log
    print(f"[snapshot] items: {len(snapshot['items'])}")
    print(f"[changes] count: {len(changes)}")
    for c in changes[:10]:
        print(f"- {c['key']}: {c['type']} {c.get('from')} -> {c.get('to')}")

if __name__ == "__main__":
    main()
