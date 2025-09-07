# bot.py - TradingView -> Telegram Binary Options Signal Bot (binary-only)
# Features:
# - Binary-only signals (CALL / PUT) with explicit expiry handling
# - 12-strategy ensemble and confidence aggregation tailored for binary signals
# - SQLite storage of signals with per-strategy votes/scores
# - Scheduled evaluation at expiry (WIN / LOSS) using Binance price (price compare)
# - Hourly / Daily / Weekly scheduled summaries with nested breakdowns and per-strategy leaderboard
# - Manual on-demand reports via Telegram /menu buttons or text commands
# - /stop and /start to toggle forwarding (signals still recorded)
# - /stats HTTP endpoint for JSON reports
# IMPORTANT: This implementation uses Binance public API for candles/prices to compute indicators.
# Make sure the symbol names you use in TradingView match Binance-style symbols (e.g., EURUSDT, BTCUSDT).
import os, json, math, logging, sqlite3, time
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.base import JobLookupError
from dotenv import load_dotenv

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
PRICE_API_PROVIDER = os.environ.get("PRICE_API_PROVIDER", "binance")

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("tv_bot_binary")

# -------------------- Flask + Scheduler --------------------
app = Flask(__name__)
scheduler = BackgroundScheduler()
scheduler.start()
session = requests.Session()

# -------------------- Utilities --------------------
def utcnow():
    return datetime.now(timezone.utc)

def iso(ts=None):
    return (ts or utcnow()).isoformat()

# -------------------- Database --------------------
def init_db():
    conn = sqlite3.connect(DATABASE_PATH, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        received_at TEXT,
        pair TEXT,
        action TEXT, -- CALL or PUT
        entry_price REAL,
        confidence INTEGER,
        timeframe TEXT,
        expiry TEXT,
        expiry_epoch INTEGER,
        evaluated_at TEXT,
        result TEXT, -- 'win' or 'loss' or 'invalid' or NULL
        exit_price REAL,
        raw_json TEXT,
        strategy_json TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)""")
    cur = c.execute("SELECT v FROM meta WHERE k='SIGNAL_ACTIVE'").fetchone()
    if cur is None:
        c.execute("INSERT OR REPLACE INTO meta (k,v) VALUES (?,?)", ("SIGNAL_ACTIVE", "1"))
    conn.commit()
    conn.close()

def db_get(k):
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    r = c.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    conn.close()
    return r[0] if r else None

def db_set(k,v):
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO meta (k,v) VALUES (?,?)", (k,str(v)))
    conn.commit()
    conn.close()

def db_execute(query, params=()):
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute(query, params)
    conn.commit()
    last = c.lastrowid
    conn.close()
    return last

def db_query(query, params=()):
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(query, params)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows

init_db()

# -------------------- Telegram helpers --------------------
def telegram_api(method, payload):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    try:
        r = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return True, r.json()
    except Exception as e:
        LOG.exception("Telegram API error")
        return False, str(e)

def send_telegram_message(text, chat_id=None, reply_markup=None):
    chat = chat_id or CHAT_ID
    payload = {"chat_id": chat, "text": text}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return telegram_api("sendMessage", payload)

# -------------------- Price/data helpers (Binance public API) --------------------
def binance_symbol(sym):
    s = sym.replace("/", "").upper()
    if s.endswith("USD") and not s.endswith("USDT"):
        s = s + "T"
    return s

def get_binance_klines(symbol, interval="1m", limit=200):
    s = binance_symbol(symbol)
    url = f"https://api.binance.com/api/v3/klines?symbol={s}&interval={interval}&limit={limit}"
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    candles = []
    for item in data:
        candles.append({
            "t": int(item[0])//1000,
            "open": float(item[1]),
            "high": float(item[2]),
            "low": float(item[3]),
            "close": float(item[4]),
            "volume": float(item[5])
        })
    return candles

def get_market_price(symbol):
    s = binance_symbol(symbol)
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={s}"
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    return float(data["price"])

# -------------------- Indicator implementations --------------------
def sma(values, period):
    if len(values) < period: return None
    return sum(values[-period:]) / period

def ema(values, period):
    if len(values) < period: return None
    k = 2/(period+1)
    ema_val = values[0]
    for v in values[1:]:
        ema_val = v*k + ema_val*(1-k)
    return ema_val

def rsi(values, period=14):
    if len(values) < period+1: return None
    gains = 0.0; losses = 0.0
    for i in range(-period,0):
        diff = values[i] - values[i-1]
        if diff>0: gains += diff
        else: losses += abs(diff)
    if gains+losses==0: return 50.0
    rs = (gains/period)/(losses/period) if losses!=0 else 9999.0
    return 100 - (100/(1+rs))

def macd(values, fast=12, slow=26, signal=9):
    if len(values) < slow+signal: return None, None, None
    ema_fast = ema(values[-(slow+fast):], fast)
    ema_slow = ema(values[-(slow+fast):], slow)
    macd_line = (ema_fast or 0) - (ema_slow or 0)
    macd_series = []
    for i in range(len(values)-slow, len(values)):
        subset = values[:i+1]
        ef = ema(subset, fast)
        es = ema(subset, slow)
        macd_series.append((ef or 0) - (es or 0))
    sig = ema(macd_series, signal) if len(macd_series)>=signal else None
    hist = macd_line - (sig or 0)
    return macd_line, sig, hist

def bollinger(values, period=20, mult=2):
    if len(values) < period: return None, None, None
    window = values[-period:]
    mean = sum(window)/period
    variance = sum((x-mean)**2 for x in window)/period
    std = math.sqrt(variance)
    upper = mean + mult*std
    lower = mean - mult*std
    return upper, lower, std

def atr(candles, period=14):
    if len(candles) < period+1: return None
    trs = []
    for i in range(1, len(candles)):
        high = candles[i]["high"]; low = candles[i]["low"]
        prev_close = candles[i-1]["close"]
        tr = max(high-low, abs(high-prev_close), abs(low-prev_close))
        trs.append(tr)
    return sum(trs[-period:])/period

def roc(values, period=12):
    if len(values) < period+1: return None
    prev = values[-period-1]
    cur = values[-1]
    if prev == 0: return None
    return ((cur - prev)/prev)*100

def stochastic_k(values_high, values_low, values_close, period=14):
    if len(values_close) < period: return None
    low_min = min(values_low[-period:])
    high_max = max(values_high[-period:])
    if high_max - low_min == 0: return None
    k = (values_close[-1] - low_min)/(high_max - low_min)*100
    return k

def adx(candles, period=14):
    if len(candles) < period+2: return None
    trs = []; plus_dm = []; minus_dm = []
    for i in range(1, len(candles)):
        up_move = candles[i]["high"] - candles[i-1]["high"]
        down_move = candles[i-1]["low"] - candles[i]["low"]
        plus = up_move if (up_move>down_move and up_move>0) else 0
        minus = down_move if (down_move>up_move and down_move>0) else 0
        plus_dm.append(plus); minus_dm.append(minus)
        tr = max(candles[i]["high"]-candles[i]["low"], abs(candles[i]["high"]-candles[i-1]["close"]), abs(candles[i]["low"]-candles[i-1]["close"]))
        trs.append(tr)
    atr_val = sum(trs[-period:])/period
    if atr_val==0: return None
    pdi = 100 * (sum(plus_dm[-period:])/period) / atr_val
    mdi = 100 * (sum(minus_dm[-period:])/period) / atr_val
    dx = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi+mdi)!=0 else 0
    return dx

def ichimoku(candles):
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    if len(candles) < 52: return None
    tenkan = (max(highs[-9:]) + min(lows[-9:]))/2
    kijun = (max(highs[-26:]) + min(lows[-26:]))/2
    span_b = (max(highs[-52:]) + min(lows[-52:]))/2
    return tenkan, kijun, span_b

def supertrend(candles, period=10, multiplier=3):
    if len(candles) < period+2: return None
    closes = [c["close"] for c in candles]
    atr_val = atr(candles, period)
    if atr_val is None: return None
    hl2 = [(c["high"]+c["low"])/2 for c in candles]
    final_upper = [(hl2[i] + multiplier*atr_val) for i in range(len(hl2))]
    final_lower = [(hl2[i] - multiplier*atr_val) for i in range(len(hl2))]
    if closes[-1] > final_upper[-1]:
        return "up", final_lower[-1]
    elif closes[-1] < final_lower[-1]:
        return "down", final_upper[-1]
    else:
        return "side", hl2[-1]

# -------------------- Strategy engine --------------------
STRATEGY_NAMES = ["SMA","EMA","RSI","MACD","BOLLINGER","ATR","VOLUME","ROC","SUPERTREND","ICHIMOKU","STOCH","ADX"]

def run_strategies(pair, interval="1m"):
    try:
        candles = get_binance_klines(pair, interval=interval, limit=200)
    except Exception as e:
        LOG.exception("Failed to fetch candles for %s: %s", pair, e)
        return {}
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    volumes = [c["volume"] for c in candles]

    res = {}
    # SMA
    sma_short = sma(closes, 50) or sma(closes, 20)
    sma_long = sma(closes, 200) or sma(closes, 100) or sma(closes, 50)
    if sma_short and sma_long:
        vote = "CALL" if sma_short > sma_long else "PUT"
        score = min(100, max(1, int(abs(sma_short - sma_long)/max(sma_long,1)*1000)))
    else:
        vote, score = "NEUT", 10
    res["SMA"] = {"vote":vote,"score":score}

    # EMA 9/21
    try:
        ema9 = ema(closes,9); ema21 = ema(closes,21)
        if ema9 and ema21:
            vote = "CALL" if ema9 > ema21 else "PUT"
            score = min(100, max(1, int(abs(ema9-ema21)/max(ema21,1)*1000)))
        else:
            vote, score = "NEUT", 8
    except:
        vote, score = "NEUT", 8
    res["EMA"] = {"vote":vote,"score":score}

    # RSI
    r = rsi(closes,14)
    if r is None:
        res["RSI"] = {"vote":"NEUT","score":10}
    else:
        if r < 30: res["RSI"] = {"vote":"CALL","score": int(min(100,(30-r)/30*100))}
        elif r > 70: res["RSI"] = {"vote":"PUT","score": int(min(100,(r-70)/30*100))}
        else: res["RSI"] = {"vote":"NEUT","score": int(abs(r-50)/50*20)}

    # MACD
    macd_line, macd_sig, macd_hist = macd(closes)
    if macd_hist is None:
        res["MACD"] = {"vote":"NEUT","score":10}
    else:
        vote = "CALL" if macd_hist>0 else "PUT"
        score = int(min(100, abs(macd_hist)/max(abs(macd_line) if macd_line else 1,1)*100))
        res["MACD"] = {"vote":vote,"score": max(5,score)}

    # Bollinger
    upper, lower, std = bollinger(closes,20,2)
    if upper is None:
        res["BOLLINGER"] = {"vote":"NEUT","score":8}
    else:
        c = closes[-1]
        if c > upper: res["BOLLINGER"] = {"vote":"CALL","score": int(min(100,(c-upper)/std*10))}
        elif c < lower: res["BOLLINGER"] = {"vote":"PUT","score": int(min(100,(lower-c)/std*10))}
        else: res["BOLLINGER"] = {"vote":"NEUT","score": int(max(1, (abs(c-(upper+lower)/2)/((upper-lower)/2))*10))}

    # ATR
    atr_val = atr(candles,14)
    if atr_val is None:
        res["ATR"] = {"vote":"NEUT","score":8}
    else:
        res["ATR"] = {"vote":"CALL","score": int(min(100, (atr_val / max(closes[-1],1))*10000))}

    # Volume spike
    avg_vol = sum(volumes[-50:])/min(50,len(volumes))
    v = volumes[-1]
    if avg_vol>0 and v>avg_vol*1.5:
        res["VOLUME"] = {"vote":"CALL","score": int(min(100,(v/avg_vol-1)*100))}
    else:
        res["VOLUME"] = {"vote":"NEUT","score": max(5,int(min(40,(v/avg_vol if avg_vol>0 else 0)*10)))}

    # ROC
    r_roc = roc(closes,12)
    if r_roc is None:
        res["ROC"] = {"vote":"NEUT","score":8}
    else:
        res["ROC"] = {"vote":"CALL" if r_roc>0 else "PUT","score": int(min(100, abs(r_roc)))}

    # SuperTrend
    st = supertrend(candles,10,3)
    if st:
        direction, level = st
        if direction=="up": res["SUPERTREND"] = {"vote":"CALL","score":60}
        elif direction=="down": res["SUPERTREND"] = {"vote":"PUT","score":60}
        else: res["SUPERTREND"] = {"vote":"NEUT","score":10}
    else:
        res["SUPERTREND"] = {"vote":"NEUT","score":8}

    # Ichimoku
    ichi = ichimoku(candles)
    if ichi:
        tenkan,kijun,spanb = ichi
        if tenkan>kijun: res["ICHIMOKU"] = {"vote":"CALL","score":50}
        else: res["ICHIMOKU"] = {"vote":"PUT","score":50}
    else:
        res["ICHIMOKU"] = {"vote":"NEUT","score":8}

    # Stochastic %K
    stoch = stochastic_k(highs,lows,closes,14)
    if stoch is None:
        res["STOCH"] = {"vote":"NEUT","score":8}
    else:
        if stoch < 20: res["STOCH"] = {"vote":"CALL","score": int(min(80,(20-stoch)/20*100))}
        elif stoch > 80: res["STOCH"] = {"vote":"PUT","score": int(min(80,(stoch-80)/20*100))}
        else: res["STOCH"] = {"vote":"NEUT","score": int(abs(stoch-50)/50*20)}

    # ADX
    adx_val = adx(candles,14)
    if adx_val is None:
        res["ADX"] = {"vote":"NEUT","score":8}
    else:
        res["ADX"] = {"vote":"CALL","score": int(min(100, adx_val))}

    return res

# -------------------- Confidence aggregation --------------------
def aggregate_confidence(strategy_map):
    total_signed = 0.0
    total_abs = 0.0
    for k,v in strategy_map.items():
        sc = float(v.get("score") or 0)
        vote = v.get("vote") or "NEUT"
        sign = 0
        if vote.upper()=="CALL": sign = 1
        elif vote.upper()=="PUT": sign = -1
        total_signed += sign * sc
        total_abs += sc
    if total_abs == 0: base_conf = 0.0
    else: base_conf = abs(total_signed) / total_abs
    counts = {"CALL":0,"PUT":0,"NEUT":0}
    for _,v in strategy_map.items():
        counts[v["vote"].upper()] = counts.get(v["vote"].upper(),0) + 1
    leading = max(counts, key=lambda k: counts[k])
    agree_bonus = (counts[leading] / max(1,len(strategy_map))) * 0.1
    conf = min(1.0, base_conf + agree_bonus)
    return int(round(conf*100)), counts

# -------------------- Scheduling and evaluation --------------------
def schedule_evaluation(signal_id, expiry_epoch):
    job_id = f"eval_{signal_id}"
    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass
    run_time = datetime.fromtimestamp(expiry_epoch, tz=timezone.utc)
    scheduler.add_job(func=lambda: evaluate_signal(signal_id), trigger="date", run_date=run_time, id=job_id)
    LOG.info("Scheduled evaluation %s at %s", job_id, run_time.isoformat())

def evaluate_signal(signal_id):
    LOG.info("Evaluating signal %s", signal_id)
    rows = db_query("SELECT * FROM signals WHERE id=?", (signal_id,))
    if not rows:
        LOG.warning("Signal %s not found", signal_id); return
    row = rows[0]
    if row["result"] is not None:
        LOG.info("Signal already evaluated"); return
    try:
        exit_price = get_market_price(row["pair"])
        entry = row["entry_price"]
        action = (row["action"] or "").upper()
        result = "invalid"
        if entry is None:
            result = "invalid"
        else:
            if action == "CALL":
                result = "win" if exit_price > entry else "loss"
            elif action == "PUT":
                result = "win" if exit_price < entry else "loss"
            else:
                result = "invalid"
        db_execute("UPDATE signals SET evaluated_at=?, result=?, exit_price=? WHERE id=?", (iso(), result, exit_price, signal_id))
        txt = f"📊 Result for Binary Signal #{signal_id} ({row['pair']})\nAction: {row['action']}\nEntry: {entry}\nExit: {exit_price}\nResult: {result.upper()}\nConfidence: {row.get('confidence')}%"
        send_telegram_message(txt)
    except Exception as e:
        LOG.exception("Failed to evaluate")
        db_execute("UPDATE signals SET evaluated_at=?, result=? WHERE id=?", (iso(), "invalid", signal_id))

# -------------------- Aggregation & Reports --------------------
def parse_period(period_seconds):
    to_ts = utcnow()
    frm = to_ts - timedelta(seconds=period_seconds)
    return frm, to_ts

def aggregate_report_seconds(period_seconds):
    frm, to_ts = parse_period(period_seconds)
    rows = db_query("SELECT * FROM signals WHERE received_at >= ? AND received_at <= ? ORDER BY received_at ASC", (frm.isoformat(), to_ts.isoformat()))
    total = len(rows)
    wins = sum(1 for r in rows if r.get("result")=="win")
    losses = sum(1 for r in rows if r.get("result")=="loss")
    pending = sum(1 for r in rows if not r.get("result"))
    avg_conf = None
    confs = [r.get("confidence") for r in rows if r.get("confidence") is not None]
    if confs: avg_conf = sum(confs)/len(confs)
    return {"from":frm.isoformat(),"to":to_ts.isoformat(),"total":total,"wins":wins,"losses":losses,"pending":pending,"avg_confidence":avg_conf,"signals":rows}

def format_report_text(name, report, include_recent=10):
    txt = f"📝 {name} Binary Report\nPeriod: {report['from']} → {report['to']}\nSignals: {report['total']} | Wins: {report['wins']} | Losses: {report['losses']} | Accuracy: {round((report['wins']/(report['total'] or 1))*100,1)}%\n"
    if report["avg_confidence"] is not None:
        txt += f"Avg confidence: {report['avg_confidence']:.1f}%\n"
    if report["signals"]:
        txt += "\nRecent:\n"
        for s in report["signals"][-include_recent:]:
            txt += f"#{s['id']} {s['pair']} {s['action']} entry={s.get('entry_price')} conf={s.get('confidence')}% result={s.get('result')}\n"
    return txt

def strategy_leaderboard(rows):
    stats = {k:{"W":0,"L":0,"T":0} for k in STRATEGY_NAMES}
    for r in rows:
        sj = r.get("strategy_json")
        if not sj: continue
        try:
            sm = json.loads(sj)
        except:
            continue
        result = r.get("result")
        for k,v in sm.items():
            vote = v.get("vote","NEUT").upper()
            if result=="win":
                if vote in ("CALL","PUT"): stats[k]["W"] += 1
            elif result=="loss":
                if vote in ("CALL","PUT"): stats[k]["L"] += 1
            else:
                stats[k]["T"] += 1
    return stats

def send_periodic_summary_name(name, seconds):
    report = aggregate_report_seconds(seconds)
    txt = format_report_text(name, report, include_recent=10)
    lb = strategy_leaderboard(report["signals"])
    txt += "\n🏆 Strategy Performance (period)\n"
    for k,v in lb.items():
        total = v["W"]+v["L"]
        pct = int((v["W"]/total*100) if total>0 else 0)
        txt += f"{k}: {v['W']}W / {v['L']}L ({pct}%)\n"
    # nested breakdowns
    if seconds >= 86400:  # daily -> hourly breakdowns
        frm = datetime.fromisoformat(report["from"])
        to = datetime.fromisoformat(report["to"])
        cur = frm
        txt += "\n📊 Hourly Breakdowns (Daily):\n"
        while cur < to:
            nxt = cur + timedelta(hours=1)
            rows = db_query("SELECT * FROM signals WHERE received_at >= ? AND received_at < ?", (cur.isoformat(), nxt.isoformat()))
            wins = sum(1 for r in rows if r.get("result")=="win")
            losses = sum(1 for r in rows if r.get("result")=="loss")
            avgc = None
            if rows:
                confs = [r.get("confidence") for r in rows if r.get("confidence") is not None]
                if confs: avgc = sum(confs)/len(confs)
            txt += f"{cur.strftime('%H:00')}–{nxt.strftime('%H:00')} → {len(rows)} signals | {wins}W {losses}L | Accuracy {round((wins/(len(rows) or 1))*100,1)}% | Avg conf {int(avgc) if avgc else 'N/A'}\n"
            cur = nxt
    elif seconds >= 7*86400:  # weekly -> daily breakdowns
        frm = datetime.fromisoformat(report["from"])
        to = datetime.fromisoformat(report["to"])
        cur = frm
        txt += "\n📊 Daily Breakdowns (Weekly):\n"
        while cur < to:
            nxt = cur + timedelta(days=1)
            rows = db_query("SELECT * FROM signals WHERE received_at >= ? AND received_at < ?", (cur.isoformat(), nxt.isoformat()))
            wins = sum(1 for r in rows if r.get("result")=="win")
            losses = sum(1 for r in rows if r.get("result")=="loss")
            avgc = None
            if rows:
                confs = [r.get("confidence") for r in rows if r.get("confidence") is not None]
                if confs: avgc = sum(confs)/len(confs)
            txt += f"{cur.date()} → {len(rows)} signals | {wins}W {losses}L | Accuracy {round((wins/(len(rows) or 1))*100,1)}% | Avg conf {int(avgc) if avgc else 'N/A'}\n"
            cur = nxt
    send_telegram_message(txt)

# schedule summaries
try:
    scheduler.add_job(lambda: send_periodic_summary_name("Hourly", 3600), 'cron', minute=5, id="hourly_summary")
    scheduler.add_job(lambda: send_periodic_summary_name("Daily", 86400), 'cron', hour=0, minute=10, id="daily_summary")
    scheduler.add_job(lambda: send_periodic_summary_name("Weekly", 7*86400), 'cron', day_of_week='mon', hour=0, minute=20, id="weekly_summary")
except Exception as e:
    LOG.warning("Failed to schedule some jobs: %s", e)

# -------------------- Webhook endpoints --------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status":"ok"}), 200

@app.route("/webhook", methods=["POST"])
def webhook():
    data = None
    if request.is_json:
        data = request.get_json(silent=True)
    else:
        try:
            data = json.loads(request.data.decode("utf-8") or "{}")
        except:
            data = None
    if not data:
        return jsonify({"status":"error","reason":"invalid_json"}), 400
    req_secret = data.get("secret") or request.headers.get("X-Webhook-Secret") or request.args.get("secret")
    if WEBHOOK_SECRET:
        if not req_secret or req_secret != WEBHOOK_SECRET:
            LOG.warning("Unauthorized webhook call"); return jsonify({"status":"error","reason":"unauthorized"}), 401
    pair = (data.get("pair") or data.get("symbol") or data.get("ticker") or "").upper()
    timeframe = data.get("timeframe") or data.get("tf") or "1m"
    action = (data.get("action") or data.get("signal") or "CALL").upper()
    if action not in ("CALL","PUT"):
        if action=="BUY": action="CALL"
        elif action=="SELL": action="PUT"
    entry = None
    try:
        entry = float(data.get("price")) if data.get("price") is not None else None
    except:
        entry = None
    expiry_epoch = None
    if data.get("expiry_epoch"):
        try:
            expiry_epoch = int(data.get("expiry_epoch"))
        except:
            expiry_epoch = None
    elif data.get("expiry"):
        try:
            expiry_dt = datetime.fromisoformat(data.get("expiry"))
            expiry_epoch = int(expiry_dt.replace(tzinfo=timezone.utc).timestamp())
        except:
            expiry_epoch = None
    if expiry_epoch is None:
        tf = str(timeframe).lower()
        if tf.endswith("m"):
            n = int(tf[:-1]); expiry_epoch = int((utcnow()+timedelta(minutes=n)).timestamp())
        elif tf.endswith("h"):
            n = int(tf[:-1]); expiry_epoch = int((utcnow()+timedelta(hours=n)).timestamp())
        elif tf.endswith("d"):
            n = int(tf[:-1]); expiry_epoch = int((utcnow()+timedelta(days=n)).timestamp())
        else:
            expiry_epoch = int((utcnow()+timedelta(minutes=1)).timestamp())
    strategy_map = run_strategies(pair, interval=timeframe)
    conf, counts = aggregate_confidence(strategy_map)
    try:
        tv_conf = data.get("confidence")
        if tv_conf is not None:
            tv_conf = int(float(tv_conf))
            conf = max(conf, tv_conf)
    except:
        pass
    sid = db_execute("INSERT INTO signals (received_at,pair,action,entry_price,confidence,timeframe,expiry,expiry_epoch,raw_json,strategy_json) VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (iso(), pair, action, entry, conf, timeframe, datetime.utcfromtimestamp(expiry_epoch).isoformat(), expiry_epoch, json.dumps(data), json.dumps(strategy_map)))
    LOG.info("Saved binary signal %s %s %s conf=%s", sid, pair, action, conf)
    schedule_evaluation(sid, expiry_epoch)
    active = db_get("SIGNAL_ACTIVE")
    forward = True if (active is None or active=="1") else False
    if conf < MIN_CONFIDENCE:
        txt = f"⚠️ Low-confidence binary signal (saved):\nID: {sid}\n{pair} {action}\nEntry: {entry}\nConfidence: {conf}%\nExpiry: {datetime.utcfromtimestamp(expiry_epoch).isoformat()}Z"
        if DIAG_CHAT_ID:
            send_telegram_message(txt, chat_id=DIAG_CHAT_ID)
        else:
            LOG.info(txt)
        return jsonify({"status":"ok","id":sid,"note":"low_confidence_saved"}), 200
    if forward:
        strat_summary = " ".join([f"{k}✓" if v["vote"].upper()=="CALL" else (f"{k}▼" if v["vote"].upper()=="PUT" else f"{k}·") for k,v in strategy_map.items()])
        msg = f"🔔 Binary Signal #{sid}\nPair: {pair}\nAction: {action}\nEntry: {entry}\nConfidence: {conf}%\nExpiry (UTC): {datetime.utcfromtimestamp(expiry_epoch).isoformat()}Z\nStrategies: {strat_summary}"
        send_telegram_message(msg)
    else:
        LOG.info("Signal not forwarded (forward=%s conf=%s)", forward, conf)
    return jsonify({"status":"ok","id":sid,"confidence":conf}), 200

# -------------------- Telegram webhook for commands and buttons --------------------
def report_to_text(name, seconds):
    report = aggregate_report_seconds(seconds)
    txt = format_report_text(name, report, include_recent=10)
    lb = strategy_leaderboard(report["signals"])
    txt += "\n🏆 Strategy Performance (period)\n"
    for k,v in lb.items():
        total = v["W"]+v["L"]
        pct = int((v["W"]/total*100) if total>0 else 0)
        txt += f"{k}: {v['W']}W / {v['L']}L ({pct}%)\n"
    return txt

@app.route("/telegram_webhook", methods=["POST"])
def telegram_webhook():
    update = request.get_json(silent=True)
    if not update: return jsonify({}), 200
    if "callback_query" in update:
        cb = update["callback_query"]; data = cb.get("data"); chat_id = cb["message"]["chat"]["id"]
        telegram_api("answerCallbackQuery", {"callback_query_id": cb["id"]})
        if data == "hourly": send_telegram_message(report_to_text("Hourly",3600), chat_id=chat_id)
        elif data == "daily": send_telegram_message(report_to_text("Daily",86400), chat_id=chat_id)
        elif data == "weekly": send_telegram_message(report_to_text("Weekly",7*86400), chat_id=chat_id)
        elif data == "stats": send_telegram_message(report_to_text("Stats (24h)",86400), chat_id=chat_id)
        elif data == "stop": db_set("SIGNAL_ACTIVE","0"); send_telegram_message("🚫 Signal forwarding stopped. Signals will still be recorded.", chat_id=chat_id)
        elif data == "start": db_set("SIGNAL_ACTIVE","1"); send_telegram_message("✅ Signal forwarding resumed.", chat_id=chat_id)
        return jsonify({}), 200
    msg = update.get("message") or {}; text = (msg.get("text") or "").strip(); chat_id = msg.get("chat",{}).get("id")
    if not text: return jsonify({}), 200
    t = text.lower().split(); cmd = t[0]
    if cmd == "/menu":
        keyboard = {"inline_keyboard":[[{"text":"📊 Hourly","callback_data":"hourly"},{"text":"📅 Daily","callback_data":"daily"}],[{"text":"📆 Weekly","callback_data":"weekly"},{"text":"📈 Stats 24h","callback_data":"stats"}],[{"text":"⏸ Stop Signals","callback_data":"stop"},{"text":"▶️ Start Signals","callback_data":"start"}]]}
        telegram_api("sendMessage", {"chat_id": chat_id, "text":"Choose a report or control signals:", "reply_markup": keyboard})
    elif cmd == "/hourly" or cmd=="/hour": send_telegram_message(report_to_text("Hourly",3600), chat_id=chat_id)
    elif cmd == "/daily": send_telegram_message(report_to_text("Daily",86400), chat_id=chat_id)
    elif cmd == "/weekly": send_telegram_message(report_to_text("Weekly",7*86400), chat_id=chat_id)
    elif cmd == "/stop": db_set("SIGNAL_ACTIVE","0"); send_telegram_message("🚫 Signal forwarding stopped. Signals will still be recorded.", chat_id=chat_id)
    elif cmd == "/start": db_set("SIGNAL_ACTIVE","1"); send_telegram_message("✅ Signal forwarding resumed.", chat_id=chat_id)
    elif cmd == "/stats":
        secs = 86400
        if len(t)>1:
            arg=t[1]
            if arg.endswith("h"): secs = int(arg[:-1])*3600
            elif arg.endswith("d"): secs = int(arg[:-1])*86400
            elif arg.endswith("w"): secs = int(arg[:-1])*7*86400
        send_telegram_message(report_to_text(f"Stats ({secs}s)",secs), chat_id=chat_id)
    return jsonify({}), 200

# -------------------- HTTP stats endpoint --------------------
@app.route("/stats", methods=["GET"])
def stats():
    try:
        s = int(request.args.get("seconds") or request.args.get("s") or 86400)
    except:
        s = 86400
    return jsonify(aggregate_report_seconds(s)), 200

# -------------------- Main --------------------
if __name__ == "__main__":
    LOG.info("Starting binary bot...")
    sa = db_get("SIGNAL_ACTIVE")
    global SIGNAL_ACTIVE
    if sa is None or sa=="1": SIGNAL_ACTIVE = True
    else: SIGNAL_ACTIVE = False
    app.run(host="0.0.0.0", port=PORT)
