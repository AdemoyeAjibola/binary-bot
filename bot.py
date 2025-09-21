# bot.py - TradingView -> Telegram Binary Options Signal Bot (binary-only)
# Features:
# - Binary-only signals (CALL / PUT) with explicit expiry handling
# - Uses TradingView Pine alerts for entry + expiry confirmation
# - SQLite storage of signals with per-strategy info
# - Scheduled evaluation at expiry (WIN / LOSS)
# - Hourly / Daily / Weekly summaries with strategy breakdowns
# - Manual reports via /menu buttons or commands (/hourly, /daily, /weekly, /stats)
# - /stop and /start toggle forwarding (still records in DB)
# - All times reported in Africa/Lagos timezone

import os, json, logging, sqlite3, uuid
from datetime import datetime, timedelta, timezone
import pytz
from flask import Flask, request, jsonify
import requests
from apscheduler.schedulers.background import BackgroundScheduler
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

# Timezone for reporting
TZ = pytz.timezone("Africa/Lagos")

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("tv_bot_binary")

# -------------------- Flask + Scheduler --------------------
app = Flask(__name__)
scheduler = BackgroundScheduler()
scheduler.start()
session = requests.Session()

# -------------------- Utilities --------------------
def now_lagos():
    return datetime.now(TZ)

def iso(ts=None):
    return (ts or datetime.now(timezone.utc)).isoformat()

def format_time(ts):
    return ts.astimezone(TZ).strftime("%I:%M:%S %p")

# -------------------- Database --------------------
def init_db():
    conn = sqlite3.connect(DATABASE_PATH, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS signals (
        id TEXT PRIMARY KEY,
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

def db_set_meta(k, v):
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO meta (k,v) VALUES (?,?)", (k, str(v)))
    conn.commit()
    conn.close()

def db_get_meta(k):
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    r = c.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
    conn.close()
    return r[0] if r else None

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

# -------------------- Report Helpers --------------------
def aggregate_report_seconds(period_seconds):
    to_ts = datetime.now(timezone.utc)
    frm = to_ts - timedelta(seconds=period_seconds)
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
    txt = f"📝 {name} Report\nSignals: {report['total']} | Wins: {report['wins']} | Losses: {report['losses']} | Accuracy: {round((report['wins']/(report['total'] or 1))*100,1)}%\n"
    if report["avg_confidence"] is not None:
        txt += f"Avg confidence: {report['avg_confidence']:.1f}%\n"
    if report["signals"]:
        txt += "\nRecent:\n"
        for s in report["signals"][-include_recent:]:
            exp = datetime.fromtimestamp(s["expiry_epoch"], tz=timezone.utc).astimezone(TZ).strftime("%I:%M %p")
            txt += f"#{s['id']} {s['pair']} {s['action']} conf={s.get('confidence')}% expiry={exp} result={s.get('result')}\n"
    return txt

def report_to_text(name, seconds):
    report = aggregate_report_seconds(seconds)
    return format_report_text(name, report, include_recent=10)

# -------------------- Webhook endpoints --------------------
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status":"ok"}), 200

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"status":"error","reason":"invalid_json"}), 400
    req_secret = data.get("secret")
    if WEBHOOK_SECRET and req_secret != WEBHOOK_SECRET:
        return jsonify({"status":"error","reason":"unauthorized"}), 401

    sig_id = data.get("id") or str(uuid.uuid4())
    pair = data.get("pair", "")
    action = data.get("action", "").upper()
    entry = float(data.get("price", 0))
    tf = data.get("timeframe", "1m")
    strategies = data.get("strategies", "")
    conf = int(data.get("confidence", 0))

    expiry_epoch = None
    if tf.endswith("m"):
        expiry_epoch = int((datetime.now(timezone.utc)+timedelta(minutes=int(tf[:-1]))).timestamp())
    elif tf.endswith("h"):
        expiry_epoch = int((datetime.now(timezone.utc)+timedelta(hours=int(tf[:-1]))).timestamp())

    db_execute("INSERT OR REPLACE INTO signals (id,received_at,pair,action,entry_price,confidence,timeframe,expiry,expiry_epoch,raw_json,strategy_json) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (sig_id, iso(), pair, action, entry, conf, tf, datetime.utcfromtimestamp(expiry_epoch).isoformat(), expiry_epoch, json.dumps(data), strategies))

    # Message
    entry_time = now_lagos() + timedelta(minutes=1)
    expiry_time = entry_time + timedelta(minutes=int(tf[:-1]))
    msg = f"🆔 {sig_id}\n💷 {pair} ({tf})\n💎 {tf} Expiry\n🕐 Enter by {format_time(entry_time)}\n⏰ Expires at {format_time(expiry_time)}\n{'🟢 CALL' if action=='CALL' else '🔴 PUT'}\nStrategies: {strategies}\nConfidence: {conf}%"
    send_telegram_message(msg)
    return jsonify({"status":"ok","id":sig_id}), 200

# -------------------- Telegram webhook --------------------
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
        elif data == "stop": db_set_meta("SIGNAL_ACTIVE","0"); send_telegram_message("🚫 Signal forwarding stopped.", chat_id=chat_id)
        elif data == "start": db_set_meta("SIGNAL_ACTIVE","1"); send_telegram_message("✅ Signal forwarding resumed.", chat_id=chat_id)
        return jsonify({}), 200
    msg = update.get("message") or {}; text = (msg.get("text") or "").strip().lower(); chat_id = msg.get("chat",{}).get("id")
    if not text: return jsonify({}), 200
    if text == "/menu":
        keyboard = {"inline_keyboard":[
            [{"text":"📊 Hourly","callback_data":"hourly"},{"text":"📅 Daily","callback_data":"daily"}],
            [{"text":"📆 Weekly","callback_data":"weekly"},{"text":"📈 Stats 24h","callback_data":"stats"}],
            [{"text":"⏸ Stop","callback_data":"stop"},{"text":"▶️ Start","callback_data":"start"}]
        ]}
        telegram_api("sendMessage", {"chat_id": chat_id, "text":"Choose:", "reply_markup": keyboard})
    elif text == "/hourly": send_telegram_message(report_to_text("Hourly",3600), chat_id=chat_id)
    elif text == "/daily": send_telegram_message(report_to_text("Daily",86400), chat_id=chat_id)
    elif text == "/weekly": send_telegram_message(report_to_text("Weekly",7*86400), chat_id=chat_id)
    elif text == "/stats": send_telegram_message(report_to_text("Stats (24h)",86400), chat_id=chat_id)
    elif text == "/stop": db_set_meta("SIGNAL_ACTIVE","0"); send_telegram_message("🚫 Signal forwarding stopped.", chat_id=chat_id)
    elif text == "/start": db_set_meta("SIGNAL_ACTIVE","1"); send_telegram_message("✅ Signal forwarding resumed.", chat_id=chat_id)
    return jsonify({}), 200

# -------------------- Main --------------------
if __name__ == "__main__":
    LOG.info("Starting TradingView Binary Bot...")
    app.run(host="0.0.0.0", port=PORT)
