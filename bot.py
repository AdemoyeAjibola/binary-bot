# bot.py - TradingView -> Telegram Binary Options Signal Bot (binary-only)

import os, json, math, logging, sqlite3
from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from zoneinfo import ZoneInfo  # for Lagos time

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

def to_lagos(dt: datetime):
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(LAGOS_TZ)

# -------------------- Database --------------------
def init_db():
    conn = sqlite3.connect(DATABASE_PATH, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
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
    cur = c.execute("SELECT v FROM meta WHERE k='SIGNAL_ACTIVE'").fetchone()
    if cur is None:
        c.execute("INSERT OR REPLACE INTO meta (k,v) VALUES (?,?)", ("SIGNAL_ACTIVE", "1"))
    conn.commit()
    conn.close()

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

init_db()

# -------------------- Telegram helpers --------------------
def telegram_api(method, payload):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    try:
        r = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return True, r.json()
    except Exception as e:
        LOG.exception("Telegram API error"); return False, str(e)

def send_telegram_message(text, chat_id=None, reply_markup=None):
    chat = chat_id or CHAT_ID
    payload = {"chat_id": chat, "text": text}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
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
        strategy_lines.append(f"{mark} {strat}")
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

# -------------------- Strategies, Confidence, Binance helpers --------------------
# (KEEP your existing strategy/indicator functions and aggregate_confidence here)

def strategy_leaderboard(rows):
    STRATEGY_NAMES = ["SMA","EMA","RSI","MACD","BOLLINGER","ATR","VOLUME","ROC","SUPERTREND","ICHIMOKU","STOCH","ADX"]
    stats = {k:{"W":0,"L":0,"T":0} for k in STRATEGY_NAMES}
    for r in rows:
        sj = r.get("strategy_json")
        if not sj: continue
        try: sm = json.loads(sj)
        except: continue
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

# -------------------- Evaluation --------------------
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
    except Exception: LOG.exception("Failed to evaluate")

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

    # Hourly breakdowns (for Daily report)
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

            # Strategy performance per hour
            if rows:
                lb = strategy_leaderboard(rows)
                txt += "   🏆 Strategy Performance (Hour):\n"
                for k,v in lb.items():
                    total = v["W"]+v["L"]
                    pct = int((v["W"]/total*100) if total>0 else 0)
                    txt += f"   {k}: {v['W']}W / {v['L']}L ({pct}%)\n"

            cur = nxt

    # Daily breakdowns (for Weekly report)
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

# -------------------- Webhook --------------------
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

    expiry_epoch = int((utcnow()+timedelta(minutes=1)).timestamp())
    if str(timeframe).endswith("m"): expiry_epoch = int((utcnow()+timedelta(minutes=int(timeframe[:-1]))).timestamp())

    strategy_map = run_strategies(pair, interval=timeframe)
    conf, _ = aggregate_confidence(strategy_map)

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

# -------------------- Main --------------------
if __name__ == "__main__":
    LOG.info("Starting binary bot...")
    app.run(host="0.0.0.0", port=PORT)
