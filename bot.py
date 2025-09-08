"""
Full bot.py - TradingView -> Telegram Binary Options Signal Bot (binary-only)
Includes:
 - Binance klines cache persisted to SQLite (JSON-safe)
 - 12 technical strategies (SMA, EMA, RSI, MACD, BOLLINGER, ATR, VOLUME, ROC, SUPERTREND, ICHIMOKU, STOCH, ADX)
 - pandas_ta used if available, fallback implementations when necessary
 - per-symbol strategy weights (for tuning), stored in DB table strategy_weights
 - safe Telegram sender with MarkdownV2 escaping to reduce 400 errors
 - scheduler jobs for hourly/daily/weekly reports (Lagos timezone for scheduling display)
 - all internal times stored/used in UTC; messages show both UTC and Africa/Lagos
"""

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

# Try to import pandas_ta if available
HAS_PANDAS_TA = True
try:
    import pandas_ta as ta
except Exception:
    HAS_PANDAS_TA = False

load_dotenv()

# -------------------- Config --------------------
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")  # numeric chat id
DIAG_CHAT_ID = os.environ.get("DIAG_CHAT_ID", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
MIN_CONFIDENCE = int(os.environ.get("MIN_CONFIDENCE", "70"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "10"))
PORT = int(os.environ.get("PORT", "5000"))
DATABASE_PATH = os.environ.get("DATABASE_PATH", "signals.db")
KLINES_TTL = int(os.environ.get("KLINES_TTL", "30"))  # seconds in-memory TTL

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("tv_bot_binary")

# -------------------- Flask + Scheduler --------------------
app = Flask(__name__)
scheduler = BackgroundScheduler()
scheduler.start()
session = requests.Session()

# -------------------- Timezone helpers --------------------
LAGOS_TZ = ZoneInfo("Africa/Lagos")

def utcnow():
    return datetime.now(timezone.utc)

def iso(ts=None):
    return (ts or utcnow()).isoformat()

def to_lagos_str(dt: datetime) -> str:
    """Return Lagos-localized string for a datetime (input must be tz-aware)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LAGOS_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

# -------------------- Database --------------------
def init_db():
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
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
    c.execute("""CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS klines_cache (
        symbol TEXT,
        interval TEXT,
        ts INTEGER,
        payload TEXT,
        PRIMARY KEY(symbol, interval)
    )""")
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

# -------------------- Telegram helpers (safe sender) --------------------
def escape_markdown_v2(text: str) -> str:
    # Escape all MarkdownV2 special characters
    # characters: _ * [ ] ( ) ~ ` > # + - = | { } . !
    replace_chars = r'_*[]()~`>#+-=|{}.!'
    s = text
    for ch in replace_chars:
        s = s.replace(ch, "\\" + ch)
    return s

def telegram_api(method, payload):
    if not TELEGRAM_TOKEN:
        LOG.warning("No TELEGRAM_TOKEN configured")
        return False, "No TELEGRAM_TOKEN"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    try:
        r = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return True, r.json()
    except Exception as e:
        # Log Telegram response body if available
        try:
            LOG.error("Telegram response: %s", getattr(e, "response").text if hasattr(e,"response") else str(e))
        except Exception:
            pass
        LOG.exception("Telegram API error"); return False, str(e)

def send_telegram_message(text, chat_id=None):
    chat = chat_id or CHAT_ID
    if not chat:
        LOG.warning("CHAT_ID missing; message not sent.")
        return False, "No chat id"
    safe_text = escape_markdown_v2(text)
    payload = {"chat_id": chat, "text": safe_text, "parse_mode": "MarkdownV2"}
    return telegram_api("sendMessage", payload)

# -------------------- Formatter (with Lagos) --------------------
def format_signal_message(sid, pair, action, entry, conf, expiry_epoch, timeframe, strategy_map):
    # Convert expiry to UTC and Lagos
    expiry_dt_utc = datetime.utcfromtimestamp(expiry_epoch).replace(tzinfo=timezone.utc)
    expiry_dt_lagos = expiry_dt_utc.astimezone(LAGOS_TZ)

    # Convert timeframe (e.g. "1m", "5m", "1h") to minutes
    tf = str(timeframe).lower()
    if tf.endswith("m"):
        minutes = tf[:-1]
    elif tf.endswith("h"):
        minutes = f"{int(tf[:-1]) * 60}"
    else:
        minutes = "1"

    # Strategy breakdown
    strategy_lines = []
    for strat, v in strategy_map.items():
        vote = v.get("vote", "NEUT").upper()
        mark = "✅" if vote == "CALL" else ("❌" if vote == "PUT" else "·")
        note = v.get("note", "")
        strategy_lines.append(f"{mark} {strat} {f'({note})' if note else ''}")
    strategies_text = "\n".join(strategy_lines)

    return (
        f"📊 *Binary Signal #{sid}*\n\n"
        f"Pair: {pair}\n"
        f"Action: {action}\n"
        f"Entry Price: {entry}\n"
        f"Confidence: {conf}%\n"
        f"Timeframe: {timeframe} ({minutes} min expiry)\n\n"
        f"Expiry (UTC): {expiry_dt_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
        f"Expiry (Lagos): {expiry_dt_lagos.strftime('%Y-%m-%d %H:%M:%S %Z')}\n\n"
        f"Strategies ({sum(1 for v in strategy_map.values() if v['vote'].upper() in ['CALL','PUT'])}/{len(strategy_map)} agreed):\n"
        f"{strategies_text}"
    )

def format_result_message(sid, row):
    # row is a dict from DB
    expiry_dt_utc = datetime.utcfromtimestamp(row["expiry_epoch"]).replace(tzinfo=timezone.utc)
    expiry_dt_lagos = expiry_dt_utc.astimezone(LAGOS_TZ)
    return (
        f"📊 Result for Binary Signal #{sid} ({row['pair']})\n"
        f"Action: {row['action']}\n"
        f"Entry: {row['entry_price']}\n"
        f"Exit: {row['exit_price']}\n"
        f"Result: {row['result'].upper()}\n"
        f"Confidence: {row.get('confidence')}%\n"
        f"Expiry (UTC): {expiry_dt_utc.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
        f"Expiry (Lagos): {expiry_dt_lagos.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
    )

# -------------------- Binance helpers & cache persistence --------------------
BINANCE_BASE = "https://api.binance.com"
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
        # parse open_time if present
        if "open_time" in df.columns:
            df["open_time"] = pd.to_datetime(df["open_time"], utc=True)
        return df, ts
    except Exception:
        return None

def _db_set_klines(symbol: str, interval: str, df: pd.DataFrame):
    try:
        # ensure open_time is a column and serialized as ISO strings
        payload = df.reset_index().copy()
        if "open_time" in payload.columns:
            payload["open_time"] = payload["open_time"].astype(str)
        # convert numeric types to native Python
        payload = payload.to_dict(orient="records")
        conn = sqlite3.connect(DATABASE_PATH); c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO klines_cache (symbol, interval, ts, payload) VALUES (?,?,?,?)",
                  (symbol, interval, int(time.time()), json.dumps(payload)))
        conn.commit(); conn.close()
    except Exception:
        LOG.exception("Failed to persist klines to DB cache")

def get_klines(symbol: str, interval: str = "1m", limit: int = 500) -> pd.DataFrame:
    key = (symbol, interval)
    now = time.time()
    cached = _IN_MEMORY_CACHE.get(key)
    if cached and (now - cached[1]) < KLINES_TTL:
        LOG.debug("Using in-memory cached klines for %s %s", symbol, interval)
        return cached[0].copy()
    db_cached = _db_get_klines(symbol, interval)
    if db_cached:
        df_db, ts_db = db_cached
        if (now - ts_db) < max(KLINES_TTL * 10, 300):
            LOG.debug("Using DB cached klines for %s %s", symbol, interval)
            _IN_MEMORY_CACHE[key] = (df_db.copy(), now)
            return df_db.copy()

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

# -------------------- TA helper functions --------------------
def atr_series(df: pd.DataFrame, length=14):
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

def sma_series(close: pd.Series, length=50):
    try:
        if HAS_PANDAS_TA:
            return ta.sma(close, length=length)
    except Exception:
        LOG.exception("pandas_ta sma failed, fallback")
    return close.rolling(window=length, min_periods=1).mean()

def ema_series(close: pd.Series, length=21):
    try:
        if HAS_PANDAS_TA:
            return ta.ema(close, length=length)
    except Exception:
        LOG.exception("pandas_ta ema failed, fallback")
    return close.ewm(span=length, adjust=False).mean()

def rsi_series(close: pd.Series, length=14):
    try:
        if HAS_PANDAS_TA:
            return ta.rsi(close, length=length)
    except Exception:
        LOG.exception("pandas_ta rsi failed, fallback")
    delta = close.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/length, adjust=False).mean()
    ma_down = down.ewm(alpha=1/length, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-9)
    return 100 - (100 / (1 + rs))

def macd_series(close: pd.Series, fast=12, slow=26, signal=9):
    try:
        if HAS_PANDAS_TA:
            macd_df = ta.macd(close, fast=fast, slow=slow, signal=signal)
            # pandas_ta returns columns names, convert robustly
            macd_line = macd_df.iloc[:,0]
            signal_line = macd_df.iloc[:,1]
            hist = macd_df.iloc[:,2]
            return macd_line, signal_line, hist
    except Exception:
        LOG.exception("pandas_ta macd failed, fallback")
    fast_e = ema_series(close, fast)
    slow_e = ema_series(close, slow)
    macd_line = fast_e - slow_e
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist

def bollinger_series(close: pd.Series, length=20, mult=2.0):
    try:
        if HAS_PANDAS_TA:
            bb = ta.bbands(close, length=length, std=mult)
            upper = bb.iloc[:,1]; mid = bb.iloc[:,0]; lower = bb.iloc[:,2]
            return upper, mid, lower
    except Exception:
        LOG.exception("pandas_ta bbands failed, fallback")
    mid = sma_series(close, length)
    std = close.rolling(window=length, min_periods=1).std()
    upper = mid + (std * mult)
    lower = mid - (std * mult)
    return upper, mid, lower

def roc_series(close: pd.Series, length=12):
    try:
        if HAS_PANDAS_TA:
            return ta.roc(close, length=length)
    except Exception:
        LOG.exception("pandas_ta roc failed, fallback")
    return close.pct_change(periods=length) * 100.0

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
    # simplified supertrend (returns boolean trend)
    hl2 = (df["high"] + df["low"]) / 2
    atr_ = atr_series(df, length=length)
    upperband = hl2 + (mult * atr_)
    lowerband = hl2 - (mult * atr_)
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
    high = df["high"]; low = df["low"]
    conv = (high.rolling(9).max() + low.rolling(9).min()) / 2
    base = (high.rolling(26).max() + low.rolling(26).min()) / 2
    span_a = ((conv + base) / 2).shift(26)
    span_b = ((high.rolling(52).max() + low.rolling(52).min()) / 2).shift(26)
    return conv, base, span_a, span_b

# -------------------- Strategy implementations (the 12 strategies) --------------------
STRATEGY_NAMES = ["SMA","EMA","RSI","MACD","BOLLINGER","ATR","VOLUME","ROC","SUPERTREND","ICHIMOKU","STOCH","ADX"]

def sma_strategy(df: pd.DataFrame):
    # SMA cross short/long
    close = df["close"]
    s = sma_series(close, length=9).iloc[-1]
    l = sma_series(close, length=21).iloc[-1]
    if s > l: return {"vote":"CALL","note":"SMA9>21"}
    if s < l: return {"vote":"PUT","note":"SMA9<21"}
    return {"vote":"NEUT"}

def ema_strategy(df: pd.DataFrame):
    close = df["close"]
    s = ema_series(close, length=9).iloc[-1]
    l = ema_series(close, length=21).iloc[-1]
    if s > l: return {"vote":"CALL","note":"EMA9>21"}
    if s < l: return {"vote":"PUT","note":"EMA9<21"}
    return {"vote":"NEUT"}

def rsi_strategy(df: pd.DataFrame):
    r = rsi_series(df["close"], length=14).iloc[-1]
    if r < 30: return {"vote":"CALL","note":f"RSI{r:.1f}<30"}
    if r > 70: return {"vote":"PUT","note":f"RSI{r:.1f}>70"}
    return {"vote":"NEUT","note":f"RSI{r:.1f}"}

def macd_strategy(df: pd.DataFrame):
    macd_line, signal_line, hist = macd_series(df["close"])
    if macd_line.iloc[-1] > signal_line.iloc[-1] and hist.iloc[-1] > 0: return {"vote":"CALL","note":"MACD>Signal"}
    if macd_line.iloc[-1] < signal_line.iloc[-1] and hist.iloc[-1] < 0: return {"vote":"PUT","note":"MACD<Signal"}
    return {"vote":"NEUT"}

def bollinger_strategy(df: pd.DataFrame):
    upper, mid, lower = bollinger_series(df["close"], length=20, mult=2.0)
    last = df["close"].iloc[-1]
    if last > upper.iloc[-1]: return {"vote":"CALL","note":"above upper"}
    if last < lower.iloc[-1]: return {"vote":"PUT","note":"below lower"}
    return {"vote":"NEUT"}

def atr_strategy(df: pd.DataFrame):
    # Use ATR to detect volatility; vote NEUT and include note
    pct = (atr_series(df, length=14).iloc[-1] / df["close"].iloc[-1]) * 100
    return {"vote":"NEUT","note":f"ATR%{pct:.3f}"}

def volume_strategy(df: pd.DataFrame):
    v = df["volume"]
    last = v.iloc[-1]
    avg = v.rolling(window=20, min_periods=1).mean().iloc[-1]
    if last > avg * 2:
        # direction by price change over last candles
        if df["close"].iloc[-1] > df["close"].iloc[-3]: return {"vote":"CALL","note":"vol spike up"}
        else: return {"vote":"PUT","note":"vol spike down"}
    return {"vote":"NEUT"}

def roc_strategy(df: pd.DataFrame):
    r = roc_series(df["close"], length=12).iloc[-1]
    if r > 2: return {"vote":"CALL","note":f"ROC{r:.2f}%"}
    if r < -2: return {"vote":"PUT","note":f"ROC{r:.2f}%"}
    return {"vote":"NEUT"}

def supertrend_strategy(df: pd.DataFrame):
    _, _, trend = supertrend_series(df, length=10, mult=3.0)
    if bool(trend.iloc[-1]): return {"vote":"CALL","note":"supertrend up"}
    return {"vote":"PUT","note":"supertrend down"} if not bool(trend.iloc[-1]) else {"vote":"NEUT"}

def ichimoku_strategy(df: pd.DataFrame):
    conv, base, span_a, span_b = ichimoku_series(df)
    try:
        if conv is not None and base is not None and span_a is not None and span_b is not None:
            last = df["close"].iloc[-1]
            if last > conv.iloc[-1] and last > base.iloc[-1] and span_a.iloc[-1] > span_b.iloc[-1]:
                return {"vote":"CALL","note":"ichimoku bullish"}
            if last < conv.iloc[-1] and last < base.iloc[-1] and span_a.iloc[-1] < span_b.iloc[-1]:
                return {"vote":"PUT","note":"ichimoku bearish"}
    except Exception:
        pass
    return {"vote":"NEUT"}

def stoch_strategy(df: pd.DataFrame):
    k, d = stoch_series(df, k=14, d=3)
    if k.iloc[-1] > 80 and d.iloc[-1] > 80: return {"vote":"PUT","note":"stoch overbought"}
    if k.iloc[-1] < 20 and d.iloc[-1] < 20: return {"vote":"CALL","note":"stoch oversold"}
    return {"vote":"NEUT"}

def adx_strategy(df: pd.DataFrame):
    adx_val, plus_di, minus_di = adx_series(df, length=14)
    try:
        adx_now = adx_val.iloc[-1]
        if adx_now > 25:
            if plus_di.iloc[-1] > minus_di.iloc[-1]:
                return {"vote":"CALL","note":f"ADX{adx_now:.1f} DMP>DMN"}
            else:
                return {"vote":"PUT","note":f"ADX{adx_now:.1f} DMN>DMP"}
    except Exception:
        pass
    return {"vote":"NEUT","note":f"ADX{(adx_val.iloc[-1] if not adx_val.empty else 0):.1f}"}

# -------------------- Evaluate strategies (aggregate into map) --------------------
def evaluate_strategies(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    # This creates a map using exact STRATEGY_NAMES order
    out = {}
    try:
        out["SMA"] = sma_strategy(df)
        out["EMA"] = ema_strategy(df)
        out["RSI"] = rsi_strategy(df)
        out["MACD"] = macd_strategy(df)
        out["BOLLINGER"] = bollinger_strategy(df)
        out["ATR"] = atr_strategy(df)
        out["VOLUME"] = volume_strategy(df)
        out["ROC"] = roc_strategy(df)
        out["SUPERTREND"] = supertrend_strategy(df)
        out["ICHIMOKU"] = ichimoku_strategy(df)
        out["STOCH"] = stoch_strategy(df)
        out["ADX"] = adx_strategy(df)
    except Exception:
        LOG.exception("Strategy evaluation failed")
        # Ensure all keys exist
        for s in STRATEGY_NAMES:
            if s not in out:
                out[s] = {"vote":"NEUT"}
    return out

# -------------------- Strategy weights load/save --------------------
def load_strategy_weights_for_symbol(symbol: str) -> Dict[str, int]:
    row = db_query("SELECT weights_json FROM strategy_weights WHERE symbol=?", (symbol,))
    if not row:
        return {s:1 for s in STRATEGY_NAMES}
    try:
        weights = json.loads(row[0]["weights_json"])
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

# -------------------- Confidence aggregation --------------------
def aggregate_confidence(strategy_map: Dict[str, Dict[str, Any]], symbol: str = None) -> Tuple[int, Dict[str,int]]:
    weights = load_strategy_weights_for_symbol(symbol or "")
    call_score = 0; put_score = 0
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
            breakdown["NEUT"] += 1
    total_non_neut = call_score + put_score
    if total_non_neut == 0:
        return 0, breakdown
    majority = max(call_score, put_score)
    conf = int(round((majority / total_non_neut) * 100))
    return conf, breakdown

# -------------------- run_strategies wrapper --------------------
def run_strategies(symbol: str, interval: str = "1m"):
    try:
        df = get_klines(symbol, interval)
        if df is None or df.empty:
            return {s:{"vote":"NEUT"} for s in STRATEGY_NAMES}
        return evaluate_strategies(df)
    except Exception as e:
        LOG.exception(f"Strategy error for {symbol} {interval}: {e}")
        return {s:{"vote":"NEUT"} for s in STRATEGY_NAMES}

# -------------------- Evaluation and scheduling --------------------
def schedule_evaluation(signal_id: int, expiry_epoch: int):
    run_time = datetime.fromtimestamp(expiry_epoch + 5, tz=timezone.utc)
    scheduler.add_job(evaluate_signal, "date", run_date=run_time, args=[signal_id], id=f"eval_{signal_id}", replace_existing=True)
    LOG.info("Scheduled evaluation for signal #%s at %s", signal_id, run_time.isoformat())

def evaluate_signal(signal_id):
    rows = db_query("SELECT * FROM signals WHERE id=?", (signal_id,))
    if not rows: return
    row = rows[0]
    if row["result"] is not None: return
    try:
        exit_price = get_market_price(row["pair"])
        entry = row["entry_price"]; action = (row["action"] or "").upper()
        result = "invalid"
        if entry is not None:
            if action == "CALL": result = "win" if exit_price > entry else "loss"
            elif action == "PUT": result = "win" if exit_price < entry else "loss"
        db_execute("UPDATE signals SET evaluated_at=?, result=?, exit_price=? WHERE id=?",
                   (iso(), result, exit_price, signal_id))
        txt = format_result_message(signal_id, db_query("SELECT * FROM signals WHERE id=?", (signal_id,))[0])
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
        "from": to_lagos_str(frm),
        "to": to_lagos_str(to_ts),
        "total": total, "wins": wins, "losses": losses,
        "pending": pending, "avg_confidence": avg_conf, "signals": rows
    }

def format_report_text(name, report, include_recent=10):
    txt = (
        f"📝 *{name} Binary Report*\n"
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
            ts_local = ts.astimezone(LAGOS_TZ).strftime("%Y-%m-%d %H:%M")
            txt += f"#{s['id']} {s['pair']} {s['action']} entry={s.get('entry_price')} conf={s.get('confidence')}% result={s.get('result')} @ {ts_local}\n"
    return txt

def send_periodic_summary_name(name, seconds):
    report = aggregate_report_seconds(seconds)
    txt = format_report_text(name, report, include_recent=10)

    # Hourly breakdowns for daily
    if seconds >= 86400 and seconds < 7*86400:
        # build hourly breakdown between report['from'] and report['to'] in Lagos-localized strings
        frm = datetime.fromisoformat(report["from"]).replace(tzinfo=timezone.utc)  # not strictly necessary
        # simpler: iterate last 24 hours
        txt += "\n📊 Hourly Breakdowns (Lagos):\n"
        now_lagos = utcnow().astimezone(LAGOS_TZ)
        for i in range(24):
            end = now_lagos - timedelta(hours=i)
            start = end - timedelta(hours=1)
            start_utc = start.astimezone(timezone.utc).isoformat()
            end_utc = end.astimezone(timezone.utc).isoformat()
            rows = db_query("SELECT * FROM signals WHERE received_at >= ? AND received_at < ?", (start_utc, end_utc))
            wins = sum(1 for r in rows if r.get("result")=="win")
            losses = sum(1 for r in rows if r.get("result")=="loss")
            txt += f"{start.strftime('%H:%M')}–{end.strftime('%H:%M')} → {len(rows)} signals | {wins}W {losses}L | Accuracy {round((wins/(len(rows) or 1))*100,1)}%\n"

    # Daily breakdowns for weekly
    if seconds >= 7*86400:
        txt += "\n📊 Daily Breakdowns (Lagos):\n"
        now_lagos = utcnow().astimezone(LAGOS_TZ)
        for i in range(7):
            end = now_lagos - timedelta(days=i)
            start = end - timedelta(days=1)
            start_utc = start.astimezone(timezone.utc).isoformat()
            end_utc = end.astimezone(timezone.utc).isoformat()
            rows = db_query("SELECT * FROM signals WHERE received_at >= ? AND received_at < ?", (start_utc, end_utc))
            wins = sum(1 for r in rows if r.get("result")=="win")
            losses = sum(1 for r in rows if r.get("result")=="loss")
            txt += f"{start.strftime('%Y-%m-%d')} → {len(rows)} signals | {wins}W {losses}L | Accuracy {round((wins/(len(rows) or 1))*100,1)}%\n"

    send_telegram_message(txt)

# -------------------- Auto scheduled reports (Lagos timezone scheduling) --------------------
scheduler.add_job(lambda: send_periodic_summary_name("Hourly", 3600),
                  CronTrigger(minute=0, timezone=LAGOS_TZ))
scheduler.add_job(lambda: send_periodic_summary_name("Daily", 86400),
                  CronTrigger(hour=0, minute=0, timezone=LAGOS_TZ))
scheduler.add_job(lambda: send_periodic_summary_name("Weekly", 7*86400),
                  CronTrigger(day_of_week="mon", hour=0, minute=0, timezone=LAGOS_TZ))

# -------------------- Telegram commands endpoint (optional) --------------------
def handle_command(text, chat_id=None):
    cmd = text.strip().lower()
    if cmd == "/stats":
        report = aggregate_report_seconds(86400)
        send_telegram_message(format_report_text("Stats", report))
    elif cmd == "/hourly":
        send_periodic_summary_name("Hourly", 3600)
    elif cmd == "/daily":
        send_periodic_summary_name("Daily", 86400)
    elif cmd == "/weekly":
        send_periodic_summary_name("Weekly", 7*86400)
    else:
        send_telegram_message("Unknown command. Try /stats, /hourly, /daily, /weekly.", chat_id=chat_id)

@app.route(f"/bot{TELEGRAM_TOKEN}", methods=["POST"])
def telegram_webhook():
    update = request.get_json(silent=True)
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
    try: entry = float(data.get("price")) if data.get("price") else None
    except: entry = None

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
    LOG.info("Starting binary bot with strategies + persistent cache (pandas-ta available=%s)...", HAS_PANDAS_TA)
    app.run(host="0.0.0.0", port=PORT)
