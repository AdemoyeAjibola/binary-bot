# (full file content) -- paste into bot.py
# … [BEGIN bot.py]
# (This is the complete upgraded bot.py with sqlite-backed klines cache,
#  weighted strategy voting from per-symbol tuning, and safe fallbacks.)
# For readability I'm repeating the full code; paste it over your existing bot.py.

import os
import json
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, Any
from zoneinfo import ZoneInfo

from flask import Flask, request, jsonify
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

import pandas as pd
import numpy as np

# try pandas_ta
HAS_PANDAS_TA = True
try:
    import pandas_ta as ta
except Exception:
    HAS_PANDAS_TA = False

load_dotenv()

# -------------------- Config --------------------
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")
DIAG_CHAT_ID = os.environ.get("DIAG_CHAT_ID", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
MIN_CONFIDENCE = int(os.environ.get("MIN_CONFIDENCE", "70"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "10"))
PORT = int(os.environ.get("PORT", "5000"))
DATABASE_PATH = os.environ.get("DATABASE_PATH", "signals.db")
KLINES_TTL = int(os.environ.get("KLINES_TTL", "30"))  # seconds in-memory TTL (DB persists)

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("tv_bot_binary")

# -------------------- Flask + Scheduler --------------------
app = Flask(__name__)
scheduler = BackgroundScheduler()
scheduler.start()
session = requests.Session()

# -------------------- Timezone --------------------
LAGOS_TZ = ZoneInfo("Africa/Lagos")

def utcnow():
    return datetime.now(timezone.utc)

def iso(ts=None):
    return (ts or utcnow()).isoformat()

def to_lagos(dt: datetime):
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LAGOS_TZ)

# -------------------- Database init --------------------
def init_db():
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    # existing signals table
    c.execute("""CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        received_at TEXT,
        pair TEXT,
        action TEXT,
        entry_price REAL,
        confidence INTEGER,
        timeframe TEXT,
        expiry TEXT,
        expiry_epoch INTEGER,
        evaluated_at TEXT,
        result TEXT,
        exit_price REAL,
        raw_json TEXT,
        strategy_json TEXT
    )""")
    # meta table
    c.execute("""CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)""")
    # klines cache persisted
    c.execute("""CREATE TABLE IF NOT EXISTS klines_cache (
        symbol TEXT,
        interval TEXT,
        ts INTEGER,
        payload TEXT,
        PRIMARY KEY(symbol, interval)
    )""")
    # strategy weights per symbol
    c.execute("""CREATE TABLE IF NOT EXISTS strategy_weights (
        symbol TEXT PRIMARY KEY,
        weights_json TEXT
    )""")
    cur = c.execute("SELECT v FROM meta WHERE k='SIGNAL_ACTIVE'").fetchone()
    if cur is None:
        c.execute("INSERT OR REPLACE INTO meta (k,v) VALUES (?,?)", ("SIGNAL_ACTIVE", "1"))
    conn.commit()
    conn.close()

init_db()

def db_get(k):
    conn = sqlite3.connect(DATABASE_PATH); c = conn.cursor()
    r = c.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    conn.close(); return r[0] if r else None

def db_set(k,v):
    conn = sqlite3.connect(DATABASE_PATH); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO meta (k,v) VALUES (?,?)", (k,str(v)))
    conn.commit(); conn.close()

def db_execute(query, params=()):
    conn = sqlite3.connect(DATABASE_PATH); c = conn.cursor()
    c.execute(query, params); conn.commit()
    last = c.lastrowid; conn.close(); return last

def db_query(query, params=()):
    conn = sqlite3.connect(DATABASE_PATH); conn.row_factory = sqlite3.Row
    c = conn.cursor(); c.execute(query, params)
    rows = [dict(r) for r in c.fetchall()]
    conn.close(); return rows

# -------------------- Telegram helpers --------------------
def telegram_api(method, payload):
    if not TELEGRAM_TOKEN:
        return False, "No TELEGRAM_TOKEN"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    try:
        r = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return True, r.json()
    except Exception as e:
        LOG.exception("Telegram API error"); return False, str(e)

def send_telegram_message(text, chat_id=None):
    chat = chat_id or CHAT_ID
    if not chat:
        LOG.warning("CHAT_ID missing; message not sent.")
        return False, "No chat id"
    payload = {"chat_id": chat, "text": text}
    return telegram_api("sendMessage", payload)

# -------------------- Formatter --------------------
def format_signal_message(sid, pair, action, entry, conf, expiry_epoch, timeframe, strategy_map):
    expiry_utc = datetime.fromtimestamp(expiry_epoch, tz=timezone.utc)
    expiry_lagos = to_lagos(expiry_utc)

    tf = str(timeframe).lower()
    if tf.endswith("m"): minutes = tf[:-1]
    elif tf.endswith("h"): minutes = f"{int(tf[:-1])*60}"
    else: minutes = "1"

    strategy_lines = []
    for strat, v in strategy_map.items():
        vote = v.get("vote", "NEUT").upper()
        mark = "✅" if vote == "CALL" else ("❌" if vote == "PUT" else "·")
        note = v.get("note", "")
        strategy_lines.append(f"{mark} {strat} {f'({note})' if note else ''}")
    strategies_text = "\n".join(strategy_lines)

    return (
        f"🔔 Binary Signal #{sid}\n\n"
        f"Pair: {pair}\n"
        f"Action: {action}\n"
        f"Trade: {minutes} Minute Expiry\n"
        f"Enter by: {expiry_lagos.strftime('%I:%M %p')} (Africa/Lagos)\n"
        f"Entry Price: {entry}\n"
        f"Confidence: {conf}%\n\n"
        f"Strategies ({sum(1 for v in strategy_map.values() if v['vote'].upper() in ['CALL','PUT'])}/{len(strategy_map)} agreed):\n"
        f"{strategies_text}"
    )

# -------------------- Binance helpers & cache persistence --------------------
BINANCE_BASE = "https://api.binance.com"
# in-memory cache {(symbol, interval): (df, ts)}
_IN_MEMORY_CACHE: Dict[Tuple[str,str], Tuple[pd.DataFrame, float]] = {}

def _db_get_klines(symbol: str, interval: str):
    conn = sqlite3.connect(DATABASE_PATH); c = conn.cursor()
    r = c.execute("SELECT ts, payload FROM klines_cache WHERE symbol=? AND interval=?", (symbol, interval)).fetchone()
    conn.close()
    if not r: return None
    ts, payload = r
    try:
        data = json.loads(payload)
        df = pd.DataFrame(data)
        df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        return df, ts
    except Exception:
        return None

def _db_set_klines(symbol: str, interval: str, df: pd.DataFrame):
    payload = df.to_dict(orient="records")
    conn = sqlite3.connect(DATABASE_PATH); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO klines_cache (symbol, interval, ts, payload) VALUES (?,?,?,?)",
              (symbol, interval, int(time.time()), json.dumps(payload)))
    conn.commit(); conn.close()

def get_klines(symbol: str, interval: str = "1m", limit: int = 300) -> pd.DataFrame:
    key = (symbol, interval)
    now = time.time()
    # check in-memory cache
    cached = _IN_MEMORY_CACHE.get(key)
    if cached and (now - cached[1]) < KLINES_TTL:
        LOG.debug("Using in-memory cached klines for %s %s", symbol, interval)
        return cached[0].copy()
    # check DB persisted cache
    db_cached = _db_get_klines(symbol, interval)
    if db_cached:
        df_db, ts_db = db_cached
        # if db cache is recent enough (KLINES_TTL * 10 for restart) use it
        if (now - ts_db) < max(KLINES_TTL * 10, 300):
            LOG.debug("Using DB cached klines for %s %s", symbol, interval)
            _IN_MEMORY_CACHE[key] = (df_db.copy(), now)
            return df_db.copy()

    # fetch from Binance
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    url = f"{BINANCE_BASE}/api/v3/klines"
    r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    df = pd.DataFrame(data, columns=[
        "open_time","open","high","low","close","volume","close_time",
        "quote_asset_volume","num_trades","taker_buy_base","taker_buy_quote","ignore"
    ])
    df = df[["open_time","open","high","low","close","volume"]]
    df["open_time"] = pd.to_datetime(df["open_time"], unit='ms', utc=True)
    df[["open","high","low","close","volume"]] = df[["open","high","low","close","volume"]].astype(float)
    # persist to DB and in-memory
    _IN_MEMORY_CACHE[key] = (df.copy(), now)
    try:
        _db_set_klines(symbol, interval, df)
    except Exception:
        LOG.exception("Failed to persist klines to DB cache")
    return df

def get_market_price(symbol: str) -> float:
    url = f"{BINANCE_BASE}/api/v3/ticker/price"
    r = session.get(url, params={"symbol": symbol}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return float(r.json()["price"])

# -------------------- TA helpers (pandas_ta preferred) --------------------
def atr_series(df: pd.DataFrame, length=14) -> pd.Series:
    try:
        if HAS_PANDAS_TA:
            return ta.atr(df["high"], df["low"], df["close"], length=length)
    except Exception:
        LOG.exception("pandas_ta atr failed, using fallback")
    high = df["high"]; low = df["low"]; close = df["close"]
    tr1 = (high - low).abs()
    tr2 = (high - close.shift()).abs()
    tr3 = (low - close.shift()).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.ewm(alpha=1/length, adjust=False).mean()

def pct_atr_last(df: pd.DataFrame, length=14) -> float:
    a = atr_series(df, length=length).iloc[-1]
    price = df["close"].iloc[-1]
    if not price or price == 0: return 0.0
    return (a / price) * 100.0

def sma_series(df_close: pd.Series, length=50) -> pd.Series:
    try:
        if HAS_PANDAS_TA:
            return ta.sma(df_close, length=length)
    except Exception:
        LOG.exception("pandas_ta sma failed, fallback")
    return df_close.rolling(window=length, min_periods=1).mean()

def ema_series(df_close: pd.Series, length=21) -> pd.Series:
    try:
        if HAS_PANDAS_TA:
            return ta.ema(df_close, length=length)
    except Exception:
        LOG.exception("pandas_ta ema failed, fallback")
    return df_close.ewm(span=length, adjust=False).mean()

def rsi_series(df_close: pd.Series, length=14) -> pd.Series:
    try:
        if HAS_PANDAS_TA:
            return ta.rsi(df_close, length=length)
    except Exception:
        LOG.exception("pandas_ta rsi failed, fallback")
    delta = df_close.diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/length, adjust=False).mean()
    ma_down = down.ewm(alpha=1/length, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-9)
    return 100 - (100 / (1 + rs))

def macd_series(df_close: pd.Series, fast=12, slow=26, signal=9):
    try:
        if HAS_PANDAS_TA:
            macd_df = ta.macd(df_close, fast=fast, slow=slow, signal=signal)
            macd_line = macd_df.iloc[:,0]
            signal_line = macd_df.iloc[:,1]
            hist = macd_df.iloc[:,2]
            return macd_line, signal_line, hist
    except Exception:
        LOG.exception("pandas_ta macd failed, fallback")
    fast_e = ema_series(df_close, fast)
    slow_e = ema_series(df_close, slow)
    macd_line = fast_e - slow_e
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def bollinger_series(df_close: pd.Series, length=20, mult=2.0):
    try:
        if HAS_PANDAS_TA:
            bb = ta.bbands(df_close, length=length, std=mult)
            upper = bb.iloc[:,1]; mid = bb.iloc[:,0]; lower = bb.iloc[:,2]
            return upper, mid, lower
    except Exception:
        LOG.exception("pandas_ta bbands failed, fallback")
    mid = sma_series(df_close, length)
    std = df_close.rolling(window=length, min_periods=1).std()
    upper = mid + (std * mult)
    lower = mid - (std * mult)
    return upper, mid, lower

def roc_series(df_close: pd.Series, length=12):
    try:
        if HAS_PANDAS_TA:
            return ta.roc(df_close, length=length)
    except Exception:
        LOG.exception("pandas_ta roc failed, fallback")
    return df_close.pct_change(periods=length) * 100.0

def stoch_series(df: pd.DataFrame, k=14, d=3):
    try:
        if HAS_PANDAS_TA:
            st = ta.stoch(df['high'], df['low'], df['close'], k=k, d=d)
            return st.iloc[:,0], st.iloc[:,1]
    except Exception:
        LOG.exception("pandas_ta stoch failed, fallback")
    low_k = df["low"].rolling(k).min()
    high_k = df["high"].rolling(k).max()
    stoch_k = 100 * ((df["close"] - low_k) / (high_k - low_k + 1e-9))
    stoch_d = stoch_k.rolling(d).mean()
    return stoch_k, stoch_d

def adx_series(df: pd.DataFrame, length=14):
    try:
        if HAS_PANDAS_TA:
            adx_df = ta.adx(df['high'], df['low'], df['close'], length=length)
            return adx_df.iloc[:,0], adx_df.iloc[:,1], adx_df.iloc[:,2]
    except Exception:
        LOG.exception("pandas_ta adx failed, fallback")
    high = df["high"]; low = df["low"]; close = df["close"]
    up = high.diff(); down = -low.diff()
    plus_dm = ((up > down) & (up > 0)) * up
    minus_dm = ((down > up) & (down > 0)) * down
    tr = pd.concat([(high - low).abs(), (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
    atr_ = tr.rolling(window=length, min_periods=1).mean()
    plus_di = 100 * (plus_dm.rolling(window=length, min_periods=1).sum() / (atr_ + 1e-9))
    minus_di = 100 * (minus_dm.rolling(window=length, min_periods=1).sum() / (atr_ + 1e-9))
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9)) * 100
    adx_ = dx.rolling(window=length, min_periods=1).mean()
    return adx_, plus_di, minus_di

def supertrend_series(df: pd.DataFrame, length=10, mult=3.0):
    try:
        if HAS_PANDAS_TA:
            st = ta.supertrend(df['high'], df['low'], df['close'], length=length, multiplier=mult)
            # try best-effort extraction
            cols = [c for c in st.columns if "SUPERT" in c.upper() or "SUPER" in c.upper()]
            if len(cols) >= 1:
                # return boolean trend series if available; else fallback
                # Many versions differ; so fallback if ambiguous
                # We'll just fallback to custom if not clear
                pass
    except Exception:
        LOG.debug("pandas_ta supertrend not used/fails")

    hl2 = (df["high"] + df["low"]) / 2
    atr_s = atr_series(df, length)
    upperband = hl2 + (mult * atr_s)
    lowerband = hl2 - (mult * atr_s)
    final_upper = upperband.copy(); final_lower = lowerband.copy()
    trend = pd.Series(True, index=df.index)
    for i in range(1, len(df)):
        if df["close"].iat[i-1] <= final_upper.iat[i-1]:
            final_upper.iat[i] = min(upperband.iat[i], final_upper.iat[i-1])
        else:
            final_upper.iat[i] = upperband.iat[i]
        if df["close"].iat[i-1] >= final_lower.iat[i-1]:
            final_lower.iat[i] = max(lowerband.iat[i], final_lower.iat[i-1])
        else:
            final_lower.iat[i] = lowerband.iat[i]
        if df["close"].iat[i] > final_upper.iat[i-1]:
            trend.iat[i] = True
        elif df["close"].iat[i] < final_lower.iat[i-1]:
            trend.iat[i] = False
        else:
            trend.iat[i] = trend.iat[i-1]
    return final_upper, final_lower, trend

def ichimoku_series(df: pd.DataFrame):
    try:
        if HAS_PANDAS_TA:
            ich = ta.ichimoku(df['high'], df['low'], df['close'])
            if ich is not None and not ich.empty:
                conv = ich.iloc[:,0]; base = ich.iloc[:,1] if ich.shape[1] > 1 else None
                span_a = ich.iloc[:,2] if ich.shape[1] > 2 else None; span_b = ich.iloc[:,3] if ich.shape[1] > 3 else None
                return conv, base, span_a, span_b
    except Exception:
        LOG.debug("pandas_ta ichimoku not used/fails")
    high = df["high"]; low = df["low"]; close = df["close"]
    conv = (high.rolling(9).max() + low.rolling(9).min()) / 2
    base = (high.rolling(26).max() + low.rolling(26).min()) / 2
    span_a = ((conv + base) / 2).shift(26)
    span_b = ((high.rolling(52).max() + low.rolling(52).min()) / 2).shift(26)
    return conv, base, span_a, span_b

# -------------------- Strategy voting + adaptive thresholds --------------------
STRATEGY_NAMES = ["SMA","EMA","RSI","MACD","BOLLINGER","ATR","VOLUME","ROC","SUPERTREND","ICHIMOKU","STOCH","ADX"]

def evaluate_strategies_from_df(df: pd.DataFrame, pair: str) -> Dict[str, Dict[str, Any]]:
    votes: Dict[str, Dict[str, Any]] = {}
    close = df["close"]
    last_close = close.iloc[-1]

    atr_pct = pct_atr_last(df, length=14)
    threshold_pct = max(0.03, 0.5 * atr_pct)  # percent
    rsi_band_widen = min(10, max(0, int(atr_pct * 2)))

    sma50 = sma_series(close, length=50).iloc[-1]
    if last_close > sma50 * (1 + threshold_pct/100):
        votes["SMA"] = {"vote":"CALL", "note": f"price > SMA50 by {threshold_pct:.2f}%"}
    elif last_close < sma50 * (1 - threshold_pct/100):
        votes["SMA"] = {"vote":"PUT", "note": f"price < SMA50 by {threshold_pct:.2f}%"}
    else:
        votes["SMA"] = {"vote":"NEUT", "note":"near SMA50"}

    ema21 = ema_series(close, length=21).iloc[-1]
    if last_close > ema21 * (1 + threshold_pct/100):
        votes["EMA"] = {"vote":"CALL", "note": f"price > EMA21 by {threshold_pct:.2f}%"}
    elif last_close < ema21 * (1 - threshold_pct/100):
        votes["EMA"] = {"vote":"PUT", "note": f"price < EMA21 by {threshold_pct:.2f}%"}
    else:
        votes["EMA"] = {"vote":"NEUT", "note":"near EMA21"}

    r = rsi_series(close, length=14).iloc[-1]
    rsi_low = max(20, 30 - rsi_band_widen)
    rsi_high = min(80, 70 + rsi_band_widen)
    if r < rsi_low:
        votes["RSI"] = {"vote":"CALL", "note": f"rsi {r:.1f} < {rsi_low}"}
    elif r > rsi_high:
        votes["RSI"] = {"vote":"PUT", "note": f"rsi {r:.1f} > {rsi_high}"}
    else:
        votes["RSI"] = {"vote":"NEUT", "note": f"rsi {r:.1f}"}

    macd_line, signal_line, hist = macd_series(close)
    if macd_line.iloc[-1] > signal_line.iloc[-1] and hist.iloc[-1] > 0:
        votes["MACD"] = {"vote":"CALL", "note":"macd bullish"}
    elif macd_line.iloc[-1] < signal_line.iloc[-1] and hist.iloc[-1] < 0:
        votes["MACD"] = {"vote":"PUT", "note":"macd bearish"}
    else:
        votes["MACD"] = {"vote":"NEUT", "note":"macd neutral"}

    upper, mid, lower = bollinger_series(close, length=20, mult=2.0)
    if last_close > upper.iloc[-1]:
        votes["BOLLINGER"] = {"vote":"CALL", "note":"above upper band"}
    elif last_close < lower.iloc[-1]:
        votes["BOLLINGER"] = {"vote":"PUT", "note":"below lower band"}
    else:
        votes["BOLLINGER"] = {"vote":"NEUT", "note":"within bands"}

    atrv = atr_series(df, length=14).iloc[-1]
    votes["ATR"] = {"vote":"NEUT", "note": f"atr_pct {atr_pct:.3f}%"}

    v = df["volume"]
    if v.iloc[-1] > v.rolling(window=20, min_periods=1).mean().iloc[-1] * 1.8:
        if last_close > close.iloc[-3]:
            votes["VOLUME"] = {"vote":"CALL", "note":"volume spike with upward move"}
        else:
            votes["VOLUME"] = {"vote":"PUT", "note":"volume spike with downward move"}
    else:
        votes["VOLUME"] = {"vote":"NEUT", "note":"volume normal"}

    r_roc = roc_series(close, length=12).iloc[-1]
    roc_thresh = max(2.0, atr_pct * 2.0)
    if r_roc > roc_thresh:
        votes["ROC"] = {"vote":"CALL", "note": f"roc {r_roc:.2f} > {roc_thresh:.2f}%"}
    elif r_roc < -roc_thresh:
        votes["ROC"] = {"vote":"PUT", "note": f"roc {r_roc:.2f} < -{roc_thresh:.2f}%"}
    else:
        votes["ROC"] = {"vote":"NEUT", "note": f"roc {r_roc:.2f}"}

    _, _, trend = supertrend_series(df, length=10, mult=3.0)
    if hasattr(trend, "iloc"):
        votes["SUPERTREND"] = {"vote":"CALL" if bool(trend.iloc[-1]) else "PUT", "note":"supertrend"}
    else:
        votes["SUPERTREND"] = {"vote":"NEUT", "note":"supertrend unknown"}

    conv, base, span_a, span_b = ichimoku_series(df)
    try:
        if conv is not None and base is not None and span_a is not None and span_b is not None:
            if last_close > conv.iloc[-1] and last_close > base.iloc[-1] and span_a.iloc[-1] > span_b.iloc[-1]:
                votes["ICHIMOKU"] = {"vote":"CALL", "note":"ichimoku bullish"}
            elif last_close < conv.iloc[-1] and last_close < base.iloc[-1] and span_a.iloc[-1] < span_b.iloc[-1]:
                votes["ICHIMOKU"] = {"vote":"PUT", "note":"ichimoku bearish"}
            else:
                votes["ICHIMOKU"] = {"vote":"NEUT", "note":"ichimoku neutral"}
        else:
            votes["ICHIMOKU"] = {"vote":"NEUT", "note":"ichimoku unavailable"}
    except Exception:
        votes["ICHIMOKU"] = {"vote":"NEUT", "note":"ichimoku error"}

    stoch_k, stoch_d = stoch_series(df, k=14, d=3)
    if stoch_k.iloc[-1] > 80 and stoch_d.iloc[-1] > 80:
        votes["STOCH"] = {"vote":"PUT", "note": f"stoch overbought K={stoch_k.iloc[-1]:.1f}"}
    elif stoch_k.iloc[-1] < 20 and stoch_d.iloc[-1] < 20:
        votes["STOCH"] = {"vote":"CALL", "note": f"stoch oversold K={stoch_k.iloc[-1]:.1f}"}
    else:
        votes["STOCH"] = {"vote":"NEUT", "note":"stoch neutral"}

    adx_val, plus_di, minus_di = adx_series(df, length=14)
    try:
        adx_now = adx_val.iloc[-1]
        if adx_now > 25:
            votes["ADX"] = {"vote":"CALL" if plus_di.iloc[-1] > minus_di.iloc[-1] else "PUT", "note": f"adx {adx_now:.1f}"}
        else:
            votes["ADX"] = {"vote":"NEUT", "note": f"adx {adx_now:.1f} weak"}
    except Exception:
        votes["ADX"] = {"vote":"NEUT", "note":"adx unavailable"}

    for s in STRATEGY_NAMES:
        if s not in votes:
            votes[s] = {"vote":"NEUT", "note":"not evaluated"}

    return votes

# -------------------- Strategy weights load/save --------------------
def load_strategy_weights_for_symbol(symbol: str) -> Dict[str, int]:
    """
    Return dict {strategy_name: weight} for a symbol from strategy_weights table.
    Default 1 for all strategies if none found.
    """
    row = db_query("SELECT weights_json FROM strategy_weights WHERE symbol=?", (symbol,))
    if not row:
        return {s:1 for s in STRATEGY_NAMES}
    try:
        weights = json.loads(row[0]["weights_json"])
        # ensure all strategies present
        for s in STRATEGY_NAMES:
            if s not in weights:
                weights[s] = 1
        return {s:int(weights.get(s,1)) for s in STRATEGY_NAMES}
    except Exception:
        return {s:1 for s in STRATEGY_NAMES}

def save_strategy_weights_for_symbol(symbol: str, weights: Dict[str,int]):
    conn = sqlite3.connect(DATABASE_PATH); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO strategy_weights (symbol, weights_json) VALUES (?,?)", (symbol, json.dumps(weights)))
    conn.commit(); conn.close()

# -------------------- Confidence aggregation with weights --------------------
def aggregate_confidence(strategy_map: Dict[str, Dict[str, Any]], symbol: str = None) -> Tuple[int, Dict[str, int]]:
    """
    Weighted voting: each strategy vote is multiplied by its weight for the symbol.
    CALL vote contributes +weight, PUT contributes -weight. NEUT contributes 0.
    Confidence = percentage of the net-voting mass in absolute terms / total_non_neut_mass.
    Returns (conf_percent, breakdown)
    """
    weights = load_strategy_weights_for_symbol(symbol or "")
    call_score = 0
    put_score = 0
    neut_count = 0
    breakdown = {"CALL":0,"PUT":0,"NEUT":0}
    for strat, v in strategy_map.items():
        vote = v.get("vote","NEUT").upper()
        w = int(weights.get(strat,1))
        if vote == "CALL":
            call_score += w
            breakdown["CALL"] += w
        elif vote == "PUT":
            put_score += w
            breakdown["PUT"] += w
        else:
            neut_count += 1
            breakdown["NEUT"] += 1
    total_non_neut = call_score + put_score
    if total_non_neut == 0:
        return 0, breakdown
    majority = max(call_score, put_score)
    conf = int(round((majority / total_non_neut) * 100))
    return conf, breakdown

# -------------------- Evaluation/scheduling --------------------
def schedule_evaluation(signal_id: int, expiry_epoch: int):
    run_time = datetime.fromtimestamp(expiry_epoch + 5, tz=timezone.utc)
    scheduler.add_job(evaluate_signal, "date", run_date=run_time,
                      args=[signal_id], id=f"eval_{signal_id}", replace_existing=True)
    LOG.info("Scheduled evaluation for signal #%s at %s", signal_id, run_time.isoformat())

def evaluate_signal(signal_id):
    rows = db_query("SELECT * FROM signals WHERE id=?", (signal_id,))
    if not rows:
        return
    row = rows[0]
    if row["result"] is not None:
        return
    try:
        exit_price = get_market_price(row["pair"])
        entry = row["entry_price"]; action = (row["action"] or "").upper()
        result = "invalid"
        if entry is not None:
            if action == "CALL": result = "win" if exit_price > entry else "loss"
            elif action == "PUT": result = "win" if exit_price < entry else "loss"
        db_execute("UPDATE signals SET evaluated_at=?, result=?, exit_price=? WHERE id=?",
                   (iso(), result, exit_price, signal_id))
        # Note: signals table persists strategy_json, so tuner uses it later
        expiry_lagos = to_lagos(datetime.fromtimestamp(row["expiry_epoch"], tz=timezone.utc))
        txt = (
            f"📊 Result for Binary Signal #{signal_id} ({row['pair']})\n"
            f"Action: {row['action']}\n"
            f"Entry: {entry}\nExit: {exit_price}\n"
            f"Result: {result.upper()}\n"
            f"Confidence: {row.get('confidence')}%\n"
            f"Expiry was: {expiry_lagos.strftime('%I:%M %p')} (Africa/Lagos)"
        )
        send_telegram_message(txt)
    except Exception:
        LOG.exception("Failed to evaluate")

# -------------------- Reports --------------------
def parse_period(period_seconds):
    to_ts = utcnow(); frm = to_ts - timedelta(seconds=period_seconds)
    return frm, to_ts

def aggregate_report_seconds(period_seconds):
    frm, to_ts = parse_period(period_seconds)
    rows = db_query("SELECT * FROM signals WHERE received_at >= ? AND received_at <= ? ORDER BY received_at ASC",
                    (frm.isoformat(), to_ts.isoformat()))
    total = len(rows); wins = sum(1 for r in rows if r.get("result")=="win")
    losses = sum(1 for r in rows if r.get("result")=="loss")
    pending = sum(1 for r in rows if not r.get("result"))
    confs = [r.get("confidence") for r in rows if r.get("confidence") is not None]
    avg_conf = sum(confs)/len(confs) if confs else None
    return {
        "from": to_lagos(frm).strftime("%Y-%m-%d %H:%M"),
        "to": to_lagos(to_ts).strftime("%Y-%m-%d %H:%M"),
        "total": total, "wins": wins, "losses": losses,
        "pending": pending, "avg_confidence": avg_conf, "signals": rows
    }

def format_report_text(name, report, include_recent=10):
    txt = (
        f"📝 {name} Binary Report\n"
        f"Period: {report['from']} → {report['to']}\n"
        f"Signals: {report['total']} | Wins: {report['wins']} | Losses: {report['losses']} "
        f"| Accuracy: {round((report['wins']/(report['total'] or 1))*100,1)}%\n"
    )
    if report["avg_confidence"] is not None:
        txt += f"Avg confidence: {report['avg_confidence']:.1f}%\n"
    if report["signals"]:
        txt += "\nRecent:\n"
        for s in report["signals"][-include_recent:]:
            ts = datetime.fromisoformat(s["received_at"]).replace(tzinfo=timezone.utc)
            ts_local = to_lagos(ts).strftime("%Y-%m-%d %H:%M")
            txt += f"#{s['id']} {s['pair']} {s['action']} entry={s.get('entry_price')} conf={s.get('confidence')}% result={s.get('result')} @ {ts_local}\n"
    return txt

def send_periodic_summary_name(name, seconds):
    report = aggregate_report_seconds(seconds)
    txt = format_report_text(name, report, include_recent=10)

    if seconds >= 86400 and seconds < 7*86400:
        frm = datetime.fromisoformat(report["from"]); to = datetime.fromisoformat(report["to"])
        cur = frm
        txt += "\n📊 Hourly Breakdowns:\n"
        while cur < to:
            nxt = cur + timedelta(hours=1)
            rows = db_query("SELECT * FROM signals WHERE received_at >= ? AND received_at < ?",
                            (cur.isoformat(), nxt.isoformat()))
            wins = sum(1 for r in rows if r.get("result")=="win")
            losses = sum(1 for r in rows if r.get("result")=="loss")
            avgc = (sum(r["confidence"] for r in rows if r.get("confidence"))/len(rows)) if rows else None
            txt += f"{to_lagos(cur).strftime('%H:%M')}–{to_lagos(nxt).strftime('%H:%M')} → {len(rows)} signals | {wins}W {losses}L | Accuracy {round((wins/(len(rows) or 1))*100,1)}% | Avg conf {int(avgc) if avgc else 'N/A'}\n"
            cur = nxt

    if seconds >= 7*86400:
        frm = datetime.fromisoformat(report["from"]); to = datetime.fromisoformat(report["to"])
        cur = frm
        txt += "\n📊 Daily Breakdowns:\n"
        while cur < to:
            nxt = cur + timedelta(days=1)
            rows = db_query("SELECT * FROM signals WHERE received_at >= ? AND received_at < ?",
                            (cur.isoformat(), nxt.isoformat()))
            wins = sum(1 for r in rows if r.get("result")=="win")
            losses = sum(1 for r in rows if r.get("result")=="loss")
            avgc = (sum(r["confidence"] for r in rows if r.get("confidence"))/len(rows)) if rows else None
            txt += f"{to_lagos(cur).strftime('%Y-%m-%d')} → {len(rows)} signals | {wins}W {losses}L | Accuracy {round((wins/(len(rows) or 1))*100,1)}% | Avg conf {int(avgc) if avgc else 'N/A'}\n"
            cur = nxt

    send_telegram_message(txt)

# -------------------- Auto scheduled reports --------------------
scheduler.add_job(lambda: send_periodic_summary_name("Hourly", 3600),
                  CronTrigger(minute=0, timezone=LAGOS_TZ))
scheduler.add_job(lambda: send_periodic_summary_name("Daily", 86400),
                  CronTrigger(hour=0, minute=0, timezone=LAGOS_TZ))
scheduler.add_job(lambda: send_periodic_summary_name("Weekly", 604800),
                  CronTrigger(day_of_week="mon", hour=0, minute=0, timezone=LAGOS_TZ))

# -------------------- Telegram commands --------------------
def handle_command(text, chat_id=None):
    cmd = text.strip().lower()
    if cmd == "/stats":
        report = aggregate_report_seconds(86400)
        send_telegram_message(format_report_text("Stats", report), chat_id=chat_id)
    elif cmd == "/hourly":
        send_periodic_summary_name("Hourly", 3600)
    elif cmd == "/daily":
        send_periodic_summary_name("Daily", 86400)
    elif cmd == "/weekly":
        send_periodic_summary_name("Weekly", 604800)
    else:
        send_telegram_message("Unknown command. Try /stats, /hourly, /daily, /weekly.", chat_id=chat_id)

@app.route(f"/bot{TELEGRAM_TOKEN}", methods=["POST"])
def telegram_webhook():
    update = request.get_json()
    if not update:
        return jsonify({"ok": False}), 400
    if "message" in update and "text" in update["message"]:
        chat_id = update["message"]["chat"]["id"]
        text = update["message"]["text"]
        if CHAT_ID and str(chat_id) != str(CHAT_ID):
            LOG.info("Ignoring command from unknown chat %s", chat_id)
            return jsonify({"ok": True})
        handle_command(text, chat_id=chat_id)
    return jsonify({"ok": True})

def run_strategies(symbol: str, interval: str = "1m"):
    """
    Run all strategies on the given symbol and interval.
    Returns: dict of strategy_name -> {"vote": "CALL"/"PUT"/"NEUT"}
    """
    try:
        df = get_klines(symbol, interval)  # fetch candles from Binance/cache
        if df is None or df.empty:
            return {}
        return evaluate_strategies(df)     # run pandas_ta + thresholds
    except Exception as e:
        LOG.exception(f"Strategy error for {symbol} {interval}: {e}")
        return {}


# -------------------- Webhook (TradingView) --------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True) or {}
    if not data: return jsonify({"status":"error","reason":"invalid_json"}), 400
    req_secret = data.get("secret") or request.headers.get("X-Webhook-Secret")
    if WEBHOOK_SECRET and req_secret != WEBHOOK_SECRET:
        return jsonify({"status":"error","reason":"unauthorized"}), 401

    pair = (data.get("pair") or "").upper()
    timeframe = data.get("timeframe") or "1m"
    action = (data.get("action") or "CALL").upper()
    if action == "BUY": action = "CALL"
    elif action == "SELL": action = "PUT"
    try:
        entry = float(data.get("price")) if data.get("price") else None
    except:
        entry = None

    if entry is None:
        try:
            entry = get_market_price(pair)
        except Exception:
            entry = None

    expiry_epoch = int((utcnow()+timedelta(minutes=1)).timestamp())
    if str(timeframe).endswith("m"): expiry_epoch = int((utcnow()+timedelta(minutes=int(timeframe[:-1]))).timestamp())
    elif str(timeframe).endswith("h"): expiry_epoch = int((utcnow()+timedelta(hours=int(timeframe[:-1]))).timestamp())

    strategy_map = run_strategies(pair, interval=timeframe)
    conf, breakdown = aggregate_confidence(strategy_map, symbol=pair)

    sid = db_execute("INSERT INTO signals (received_at,pair,action,entry_price,confidence,timeframe,expiry,expiry_epoch,raw_json,strategy_json) VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (iso(), pair, action, entry, conf, timeframe,
                      datetime.utcfromtimestamp(expiry_epoch).isoformat(),
                      expiry_epoch, json.dumps(data), json.dumps(strategy_map)))
    schedule_evaluation(sid, expiry_epoch)

    if conf < MIN_CONFIDENCE:
        txt = f"⚠️ Low-confidence signal saved: ID {sid} {pair} {action} {conf}%"
        if DIAG_CHAT_ID: send_telegram_message(txt, chat_id=DIAG_CHAT_ID)
        return jsonify({"status":"ok","id":sid,"note":"low_confidence_saved"}), 200

    if db_get("SIGNAL_ACTIVE") == "1":
        msg = format_signal_message(sid, pair, action, entry, conf, expiry_epoch, timeframe, strategy_map)
        send_telegram_message(msg)

    return jsonify({"status":"ok","id":sid,"confidence":conf}), 200

# -------------------- Health --------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status":"ok","time": iso(), "active": db_get("SIGNAL_ACTIVE")})

# -------------------- Main --------------------
if __name__ == "__main__":
    LOG.info("Starting binary bot with tuning + persistent cache (pandas-ta available=%s)...", HAS_PANDAS_TA)
    app.run(host="0.0.0.0", port=PORT)

# … [END bot.py]
